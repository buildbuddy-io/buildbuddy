package api

import (
	"context"
	"flag"
	"net/http"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/bytestream"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"

	api_common "github.com/buildbuddy-io/buildbuddy/server/api/common"
	api_config "github.com/buildbuddy-io/buildbuddy/server/api/config"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

var (
	enableCacheDeleteAPI = flag.Bool("enable_cache_delete_api", false, "If true, enable access to cache delete API.")
)

type APIServer struct {
	env environment.Env
}

func Register(env environment.Env) error {
	if api_config.APIEnabled() {
		env.SetAPIService(NewAPIServer(env))
	}
	return nil
}

func NewAPIServer(env environment.Env) *APIServer {
	return &APIServer{
		env: env,
	}
}

func (s *APIServer) checkPreconditions(ctx context.Context) (interfaces.UserInfo, error) {
	authenticator := s.env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionErrorf("No authenticator configured")
	}
	return s.env.GetAuthenticator().AuthenticatedUser(ctx)
}

func (s *APIServer) authorizeWrites(ctx context.Context) error {
	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY)
	if err != nil {
		return err
	}
	if !canWrite {
		return status.PermissionDeniedError("You do not have permission to perform this action.")
	}
	return nil
}

func (s *APIServer) GetInvocation(ctx context.Context, req *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error) {
	user, err := s.checkPreconditions(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetSelector().GetInvocationId() == "" && req.GetSelector().GetCommitSha() == "" {
		return nil, status.InvalidArgumentErrorf("InvocationSelector must contain a valid invocation_id or commit_sha")
	}

	q := query_builder.NewQuery(`SELECT * FROM Invocations`)
	q = q.AddWhereClause(`group_id = ?`, user.GetGroupID())
	if req.GetSelector().GetInvocationId() != "" {
		q = q.AddWhereClause(`invocation_id = ?`, req.GetSelector().GetInvocationId())
	}
	if req.GetSelector().GetCommitSha() != "" {
		q = q.AddWhereClause(`commit_sha = ?`, req.GetSelector().GetCommitSha())
	}
	if err := perms.AddPermissionsCheckToQuery(ctx, s.env, q); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()

	rows, err := s.env.GetDBHandle().DB(ctx).Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}

	invocations := []*apipb.Invocation{}
	for rows.Next() {
		var ti tables.Invocation
		if err := s.env.GetDBHandle().DB(ctx).ScanRows(rows, &ti); err != nil {
			return nil, err
		}

		apiInvocation := &apipb.Invocation{
			Id: &apipb.Invocation_Id{
				InvocationId: ti.InvocationID,
			},
			Success:       ti.Success,
			User:          ti.User,
			DurationUsec:  ti.DurationUsec,
			Host:          ti.Host,
			Command:       ti.Command,
			Pattern:       ti.Pattern,
			ActionCount:   ti.ActionCount,
			CreatedAtUsec: ti.CreatedAtUsec,
			UpdatedAtUsec: ti.UpdatedAtUsec,
			RepoUrl:       ti.RepoURL,
			BranchName:    ti.BranchName,
			CommitSha:     ti.CommitSHA,
			Role:          ti.Role,
		}

		invocations = append(invocations, apiInvocation)
	}

	return &apipb.GetInvocationResponse{
		Invocation: invocations,
	}, nil
}

func (s *APIServer) redisCachedTarget(ctx context.Context, userInfo interfaces.UserInfo, iid, targetLabel string) (*apipb.Target, error) {
	if !api_config.CacheEnabled() || s.env.GetMetricsCollector() == nil {
		return nil, nil
	}

	if targetLabel == "" {
		return nil, nil
	}
	key := api_common.TargetLabelKey(userInfo.GetGroupID(), iid, targetLabel)
	blobs, err := s.env.GetMetricsCollector().GetAll(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(blobs) != 1 {
		return nil, nil
	}

	t := &apipb.Target{}
	if err := proto.Unmarshal([]byte(blobs[0]), t); err != nil {
		return nil, err
	}
	return t, nil
}

func (s *APIServer) GetTarget(ctx context.Context, req *apipb.GetTargetRequest) (*apipb.GetTargetResponse, error) {
	userInfo, err := s.checkPreconditions(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetSelector().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("TargetSelector must contain a valid invocation_id")
	}
	iid := req.GetSelector().GetInvocationId()

	rsp := &apipb.GetTargetResponse{
		Target: make([]*apipb.Target, 0),
	}

	cacheKey := req.GetSelector().GetLabel()
	// Target ID is equal to the target label, so either can be used as a cache key.
	if targetId := req.GetSelector().GetTargetId(); targetId != "" {
		cacheKey = targetId
	}

	cachedTarget, err := s.redisCachedTarget(ctx, userInfo, iid, cacheKey)
	if err != nil {
		log.Debugf("redisCachedTarget err: %s", err)
	} else if cachedTarget != nil {
		if targetMatchesTargetSelector(cachedTarget, req.GetSelector()) {
			rsp.Target = append(rsp.Target, cachedTarget)
		}
	}
	if len(rsp.Target) > 0 {
		return rsp, nil
	}

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetSelector().GetInvocationId())
	if err != nil {
		return nil, err
	}
	targetMap := api_common.TargetMapFromInvocation(inv)

	// Filter to only selected targets.
	targets := []*apipb.Target{}
	for _, target := range targetMap {
		if targetMatchesTargetSelector(target, req.GetSelector()) {
			targets = append(targets, target)
		}
	}

	return &apipb.GetTargetResponse{
		Target: targets,
	}, nil
}

func (s *APIServer) redisCachedActions(ctx context.Context, userInfo interfaces.UserInfo, iid, targetLabel string) ([]*apipb.Action, error) {
	if !api_config.CacheEnabled() || s.env.GetMetricsCollector() == nil {
		return nil, nil
	}

	if targetLabel == "" {
		return nil, nil
	}

	const limit = 100_000
	key := api_common.ActionLabelKey(userInfo.GetGroupID(), iid, targetLabel)
	serializedResults, err := s.env.GetMetricsCollector().ListRange(ctx, key, 0, limit-1)
	if err != nil {
		return nil, err
	}
	a := &apipb.Action{}
	actions := make([]*apipb.Action, 0)
	for _, serializedResult := range serializedResults {
		if err := proto.Unmarshal([]byte(serializedResult), a); err != nil {
			return nil, err
		}
		actions = append(actions, a)
	}
	return actions, nil
}

func (s *APIServer) GetAction(ctx context.Context, req *apipb.GetActionRequest) (*apipb.GetActionResponse, error) {
	userInfo, err := s.checkPreconditions(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetSelector().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("ActionSelector must contain a valid invocation_id")
	}
	iid := req.GetSelector().GetInvocationId()
	rsp := &apipb.GetActionResponse{
		Action: make([]*apipb.Action, 0),
	}

	cacheKey := req.GetSelector().GetTargetLabel()
	// Target ID is equal to the target label, so either can be used as a cache key.
	if targetId := req.GetSelector().GetTargetId(); targetId != "" {
		cacheKey = targetId
	}

	cachedActions, err := s.redisCachedActions(ctx, userInfo, iid, cacheKey)
	if err != nil {
		log.Debugf("redisCachedAction err: %s", err)
	}
	for _, action := range cachedActions {
		if action != nil && actionMatchesActionSelector(action, req.GetSelector()) {
			rsp.Action = append(rsp.Action, action)
		}
	}
	if len(rsp.Action) > 0 {
		return rsp, nil
	}

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, iid)
	if err != nil {
		return nil, err
	}

	for _, event := range inv.GetEvent() {
		action := &apipb.Action{
			Id: &apipb.Action_Id{
				InvocationId: inv.InvocationId,
			},
		}
		action = api_common.FillActionFromBuildEvent(event.BuildEvent, action)

		// Filter to only selected actions.
		if action != nil && actionMatchesActionSelector(action, req.GetSelector()) {
			action = api_common.FillActionOutputFilesFromBuildEvent(event.BuildEvent, action)
			rsp.Action = append(rsp.Action, action)
		}
	}

	return rsp, nil
}

func (s *APIServer) GetLog(ctx context.Context, req *apipb.GetLogRequest) (*apipb.GetLogResponse, error) {
	// No need for user here because user filters will be applied by LookupInvocation.
	if _, err := s.checkPreconditions(ctx); err != nil {
		return nil, err
	}

	if req.GetSelector().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("LogSelector must contain a valid invocation_id")
	}

	chunkReq := &elpb.GetEventLogChunkRequest{
		InvocationId: req.GetSelector().GetInvocationId(),
		ChunkId:      req.GetPageToken(),
	}

	resp, err := eventlog.GetEventLogChunk(ctx, s.env, chunkReq)
	if err != nil {
		return nil, err
	}

	return &apipb.GetLogResponse{
		Log: &apipb.Log{
			Contents: string(resp.GetBuffer()),
		},
		NextPageToken: resp.GetNextChunkId(),
	}, nil
}

func (s *APIServer) GetFile(req *apipb.GetFileRequest, server apipb.ApiService_GetFileServer) error {
	ctx := server.Context()
	if _, err := s.checkPreconditions(ctx); err != nil {
		return err
	}

	parsedURL, err := url.Parse(req.GetUri())
	if err != nil {
		return status.InvalidArgumentErrorf("Invalid URL")
	}

	return bytestream.StreamBytestreamFile(ctx, s.env, parsedURL, func(data []byte) {
		server.Send(&apipb.GetFileResponse{
			Data: data,
		})
	})
}

func (s *APIServer) DeleteFile(ctx context.Context, req *apipb.DeleteFileRequest) (*apipb.DeleteFileResponse, error) {
	if !*enableCacheDeleteAPI {
		return nil, status.PermissionDeniedError("DeleteFile API not enabled")
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	if _, err = s.checkPreconditions(ctx); err != nil {
		return nil, err
	}
	if err = s.authorizeWrites(ctx); err != nil {
		return nil, err
	}

	parsedURL, err := url.Parse(req.GetUri())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("Invalid URL")
	}
	urlStr := strings.TrimPrefix(parsedURL.RequestURI(), "/")

	var parsedResourceName *digest.ResourceName
	var cacheType resource.CacheType
	if digest.IsActionCacheResourceName(urlStr) {
		parsedResourceName, err = digest.ParseActionCacheResourceName(urlStr)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Invalid URL. Does not match expected actioncache URI pattern: %s", err)
		}
		cacheType = resource.CacheType_AC
	} else if digest.IsDownloadResourceName(urlStr) {
		parsedResourceName, err = digest.ParseDownloadResourceName(urlStr)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Invalid URL. Does not match expected CAS URI pattern: %s", err)
		}
		cacheType = resource.CacheType_CAS
	} else {
		return nil, status.InvalidArgumentErrorf("Invalid URL. Only actioncache and CAS URIs supported.")
	}

	cache, err := s.env.GetCache().WithIsolation(ctx, cacheType, parsedResourceName.GetInstanceName())
	if err != nil {
		return nil, err
	}

	err = cache.Delete(ctx, parsedResourceName.GetDigest())
	if err != nil && !status.IsNotFoundError(err) {
		return nil, err
	}

	return &apipb.DeleteFileResponse{}, nil
}

// Handle streaming http GetFile request since protolet doesn't handle streaming rpcs yet.
func (s *APIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, err := s.checkPreconditions(r.Context()); err != nil {
		http.Error(w, "Invalid API key", http.StatusUnauthorized)
		return
	}

	req := apipb.GetFileRequest{}
	protolet.ReadRequestToProto(r, &req)

	parsedURL, err := url.Parse(req.GetUri())
	if err != nil {
		http.Error(w, "Invalid URI", http.StatusBadRequest)
		return
	}

	err = bytestream.StreamBytestreamFile(r.Context(), s.env, parsedURL, func(data []byte) {
		w.Write(data)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Returns true if a selector has an empty target ID or matches the target's ID or tag
func targetMatchesTargetSelector(target *apipb.Target, selector *apipb.TargetSelector) bool {
	if selector.Label != "" {
		return selector.Label == target.Label
	}

	if selector.Tag != "" {
		for _, tag := range target.GetTag() {
			if tag == selector.Tag {
				return true
			}
		}
		return false
	}
	return selector.TargetId == "" || selector.TargetId == target.GetId().TargetId
}

// Returns true if a selector doesn't specify a particular id or matches the target's ID
func actionMatchesActionSelector(action *apipb.Action, selector *apipb.ActionSelector) bool {
	return (selector.TargetId == "" || selector.TargetId == action.GetId().TargetId) &&
		(selector.TargetLabel == "" || selector.TargetLabel == action.GetTargetLabel()) &&
		(selector.ConfigurationId == "" || selector.ConfigurationId == action.GetId().ConfigurationId) &&
		(selector.ActionId == "" || selector.ActionId == action.GetId().ActionId)
}
