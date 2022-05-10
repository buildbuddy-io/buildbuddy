package api

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/bytestream"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

var enableAPI = flag.Bool("api.enable_api", true, "Whether or not to enable the BuildBuddy API.")

// A prefix specifying which ID encoding scheme we're using.
// Don't change this unless you're changing the ID scheme, in which case you should probably check for this
// prefix and support this old ID scheme for some period of time during the migration.
const encodedIDPrefix = "id::v1::"

type APIServer struct {
	env environment.Env
}

func Register(env environment.Env) error {
	if *enableAPI {
		env.SetAPIService(NewAPIServer(env))
	}
	return nil
}

func NewAPIServer(env environment.Env) *APIServer {
	return &APIServer{
		env: env,
	}
}

func Enabled() bool {
	return *enableAPI
}

func (s *APIServer) checkPreconditions(ctx context.Context) (interfaces.UserInfo, error) {
	authenticator := s.env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionErrorf("No authenticator configured")
	}
	return s.env.GetAuthenticator().AuthenticatedUser(ctx)
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

func (s *APIServer) GetTarget(ctx context.Context, req *apipb.GetTargetRequest) (*apipb.GetTargetResponse, error) {
	if _, err := s.checkPreconditions(ctx); err != nil {
		return nil, err
	}

	if req.GetSelector().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("TargetSelector must contain a valid invocation_id")
	}

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetSelector().GetInvocationId())
	if err != nil {
		return nil, err
	}

	targetMap := targetMapFromInvocation(inv)

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

func (s *APIServer) GetAction(ctx context.Context, req *apipb.GetActionRequest) (*apipb.GetActionResponse, error) {
	if _, err := s.checkPreconditions(ctx); err != nil {
		return nil, err
	}

	if req.GetSelector().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("ActionSelector must contain a valid invocation_id")
	}

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetSelector().GetInvocationId())
	if err != nil {
		return nil, err
	}

	actions := []*apipb.Action{}
	for _, event := range inv.GetEvent() {
		action := &apipb.Action{
			Id: &apipb.Action_Id{
				InvocationId: inv.InvocationId,
			},
		}

		action = fillActionFromBuildEvent(action, event.BuildEvent)

		// Filter to only selected actions.
		if action != nil && actionMatchesActionSelector(action.GetId(), req.GetSelector()) {
			actions = append(actions, action)
		}
	}

	return &apipb.GetActionResponse{
		Action: actions,
	}, nil
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

func encodeID(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(encodedIDPrefix + id))
}

func testStatusToStatus(testStatus build_event_stream.TestStatus) cmnpb.Status {
	switch testStatus {
	case build_event_stream.TestStatus_PASSED:
		return cmnpb.Status_PASSED
	case build_event_stream.TestStatus_FLAKY:
		return cmnpb.Status_FLAKY
	case build_event_stream.TestStatus_TIMEOUT:
		return cmnpb.Status_TIMED_OUT
	case build_event_stream.TestStatus_FAILED:
		return cmnpb.Status_FAILED
	case build_event_stream.TestStatus_INCOMPLETE:
		return cmnpb.Status_INCOMPLETE
	case build_event_stream.TestStatus_REMOTE_FAILURE:
		return cmnpb.Status_TOOL_FAILED
	case build_event_stream.TestStatus_FAILED_TO_BUILD:
		return cmnpb.Status_FAILED_TO_BUILD
	case build_event_stream.TestStatus_TOOL_HALTED_BEFORE_TESTING:
		return cmnpb.Status_CANCELLED
	default:
		return cmnpb.Status_STATUS_UNSPECIFIED
	}
}

func targetMapFromInvocation(inv *invocation.Invocation) map[string]*apipb.Target {
	targetMap := make(map[string]*apipb.Target)
	for _, event := range inv.GetEvent() {
		switch p := event.BuildEvent.Payload.(type) {
		case *build_event_stream.BuildEvent_Configured:
			{
				ruleType := strings.Replace(p.Configured.TargetKind, " rule", "", -1)
				language := ""
				if components := strings.Split(p.Configured.TargetKind, "_"); len(components) > 1 {
					language = components[0]
				}
				label := event.GetBuildEvent().GetId().GetTargetConfigured().GetLabel()
				targetMap[label] = &apipb.Target{
					Id: &apipb.Target_Id{
						InvocationId: inv.InvocationId,
						TargetId:     encodeID(label),
					},
					Label:    label,
					Status:   cmnpb.Status_BUILDING,
					RuleType: ruleType,
					Language: language,
					Tag:      p.Configured.Tag,
				}
			}
		case *build_event_stream.BuildEvent_Completed:
			{
				target := targetMap[event.GetBuildEvent().GetId().GetTargetCompleted().GetLabel()]
				target.Status = cmnpb.Status_BUILT
			}
		case *build_event_stream.BuildEvent_TestSummary:
			{
				target := targetMap[event.GetBuildEvent().GetId().GetTestSummary().GetLabel()]
				target.Status = testStatusToStatus(p.TestSummary.OverallStatus)
				startTime := timeutil.GetTimeWithFallback(p.TestSummary.FirstStartTime, p.TestSummary.FirstStartTimeMillis)
				duration := timeutil.GetDurationWithFallback(p.TestSummary.TotalRunDuration, p.TestSummary.TotalRunDurationMillis)
				target.Timing = &cmnpb.Timing{
					StartTime: timestamppb.New(startTime),
					Duration:  durationpb.New(duration),
				}
			}
		}
	}
	return targetMap
}

func filesFromOutput(output []*build_event_stream.File) []*apipb.File {
	files := []*apipb.File{}
	for _, output := range output {
		uri := ""
		switch file := output.File.(type) {
		case *build_event_stream.File_Uri:
			uri = file.Uri
			// Contents files are not currently supported - only the file name will be appended without a uri.
		}
		f := &apipb.File{
			Name: output.Name,
			Uri:  uri,
		}
		if u, err := url.Parse(uri); err == nil {
			if r, err := digest.ParseDownloadResourceName(u.Path); err == nil {
				f.Hash = r.GetDigest().GetHash()
				f.SizeBytes = r.GetDigest().GetSizeBytes()
			}
		}
		files = append(files, f)
	}
	return files
}

func fillActionFromBuildEvent(action *apipb.Action, event *build_event_stream.BuildEvent) *apipb.Action {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Completed:
		{
			action.Id.TargetId = encodeID(event.GetId().GetTargetCompleted().GetLabel())
			action.Id.ConfigurationId = event.GetId().GetTargetCompleted().GetConfiguration().Id
			action.Id.ActionId = encodeID("build")
			action.File = filesFromOutput(p.Completed.ImportantOutput)
			return action
		}
	case *build_event_stream.BuildEvent_TestResult:
		{
			testResultID := event.GetId().GetTestResult()
			action.Id.TargetId = encodeID(event.GetId().GetTestResult().GetLabel())
			action.Id.ConfigurationId = event.GetId().GetTestResult().GetConfiguration().Id
			action.Id.ActionId = encodeID(fmt.Sprintf("test-S_%d-R_%d-A_%d", testResultID.Shard, testResultID.Run, testResultID.Attempt))
			action.File = filesFromOutput(p.TestResult.TestActionOutput)
			return action
		}
	}
	return nil
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
func actionMatchesActionSelector(id *apipb.Action_Id, selector *apipb.ActionSelector) bool {
	return (selector.TargetId == "" || selector.TargetId == id.TargetId) &&
		(selector.ConfigurationId == "" || selector.ConfigurationId == id.ConfigurationId) &&
		(selector.ActionId == "" || selector.ActionId == id.ActionId)
}
