package buildbuddy_server

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_index"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/remote_exec_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/directory_size"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/scorecard"
	"github.com/buildbuddy-io/buildbuddy/server/role_filter"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/target"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	bzpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_config"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	gcpb "github.com/buildbuddy-io/buildbuddy/proto/gcp"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	qpb "github.com/buildbuddy-io/buildbuddy/proto/quota"
	repb "github.com/buildbuddy-io/buildbuddy/proto/repo"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
	stpb "github.com/buildbuddy-io/buildbuddy/proto/stats"
	supb "github.com/buildbuddy-io/buildbuddy/proto/suggestion"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	zipb "github.com/buildbuddy-io/buildbuddy/proto/zip"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/server/remote_execution/config"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

var (
	disableCertConfig   = flag.Bool("app.disable_cert_config", false, "If true, the certificate based auth option will not be shown in the config widget.")
	paginateInvocations = flag.Bool("app.paginate_invocations", true, "If true, paginate invocations returned to the UI.")
)

const (
	bytestreamProtocolPrefix  = "bytestream://"
	actioncacheProtocolPrefix = "actioncache://"
)

type BuildBuddyServer struct {
	env        environment.Env
	sslService interfaces.SSLService
}

func Register(env *real_environment.RealEnv) error {
	buildBuddyServer, err := NewBuildBuddyServer(env, env.GetSSLService())
	if err != nil {
		return status.InternalErrorf("Error initializing BuildBuddyServer: %s", err)
	}
	env.SetBuildBuddyServer(buildBuddyServer)
	return nil
}

func NewBuildBuddyServer(env environment.Env, sslService interfaces.SSLService) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		env:        env,
		sslService: sslService,
	}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	if req.GetLookup().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("GetInvocationRequest must contain a valid invocation_id")
	}
	if *paginateInvocations {
		idx := event_index.New()
		callback := func(event *inpb.InvocationEvent) error {
			idx.Add(event)
			return nil
		}
		inv, err := build_event_handler.LookupInvocationWithCallback(
			ctx, s.env, req.GetLookup().GetInvocationId(), callback)
		if err != nil {
			return nil, err
		}
		idx.Finalize()
		tr, err := target.GetTarget(ctx, s.env, inv, idx, &trpb.GetTargetRequest{})
		if err != nil {
			return nil, err
		}
		inv.Event = idx.TopLevelEvents
		inv.TargetGroups = tr.TargetGroups
		inv.TargetConfiguredCount = idx.ConfiguredCount
		return &inpb.GetInvocationResponse{Invocation: []*inpb.Invocation{inv}}, nil
	}
	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationResponse{Invocation: []*inpb.Invocation{inv}}, nil
}

func (s *BuildBuddyServer) GetTarget(ctx context.Context, req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
	if req.GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("request is missing invocation_id field")
	}
	idx := event_index.New()
	callback := func(event *inpb.InvocationEvent) error {
		idx.Add(event)
		return nil
	}
	inv, err := build_event_handler.LookupInvocationWithCallback(
		ctx, s.env, req.GetInvocationId(), callback)
	if err != nil {
		return nil, err
	}
	idx.Finalize()
	return target.GetTarget(ctx, s.env, inv, idx, req)
}

func (s *BuildBuddyServer) SearchInvocation(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentErrorf("SearchInvocationRequest cannot be empty")
	}
	searcher := s.env.GetInvocationSearchService()
	if searcher == nil {
		return nil, fmt.Errorf("No searcher was configured")
	}
	if req.Query == nil {
		return nil, fmt.Errorf("A query must be provided")
	}
	return searcher.QueryInvocations(ctx, req)
}

func (s *BuildBuddyServer) UpdateInvocation(ctx context.Context, req *inpb.UpdateInvocationRequest) (*inpb.UpdateInvocationResponse, error) {
	auth := s.env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	authenticatedUser, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	db := s.env.GetInvocationDB()
	if err := db.UpdateInvocationACL(ctx, &authenticatedUser, req.GetInvocationId(), req.GetAcl()); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForInvocation(ctx, req.GetInvocationId(), alpb.Action_UPDATE, req)
	}
	return &inpb.UpdateInvocationResponse{}, nil
}

func (s *BuildBuddyServer) DeleteInvocation(ctx context.Context, req *inpb.DeleteInvocationRequest) (*inpb.DeleteInvocationResponse, error) {
	auth := s.env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	authenticatedUser, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	db := s.env.GetInvocationDB()
	if err := db.DeleteInvocationWithPermsCheck(ctx, &authenticatedUser, req.GetInvocationId()); err != nil {
		return nil, err
	}

	return &inpb.DeleteInvocationResponse{}, nil
}

func (s *BuildBuddyServer) GetZipManifest(ctx context.Context, req *zipb.GetZipManifestRequest) (*zipb.GetZipManifestResponse, error) {
	u, err := url.Parse(req.GetUri())
	if err != nil {
		return nil, err
	}
	man, err := s.env.GetPooledByteStreamClient().FetchBytestreamZipManifest(ctx, u)
	if err != nil {
		return nil, err
	}
	return &zipb.GetZipManifestResponse{Manifest: man}, nil
}

func (s *BuildBuddyServer) CancelExecutions(ctx context.Context, req *inpb.CancelExecutionsRequest) (*inpb.CancelExecutionsResponse, error) {
	res := s.env.GetRemoteExecutionService()
	if res == nil {
		return nil, status.FailedPreconditionError("Remote execution not enabled")
	}

	err := s.authorizeInvocationWrite(ctx, req.GetInvocationId())
	if err != nil {
		return nil, err
	}

	if err = res.Cancel(ctx, req.GetInvocationId()); err != nil {
		return nil, err
	}

	return &inpb.CancelExecutionsResponse{}, nil
}

func getGroupUrl(g *tables.Group) string {
	gURL := build_buddy_url.String()
	if g.URLIdentifier != nil && *g.URLIdentifier != "" {
		gURL = subdomain.ReplaceURLSubdomainForGroup(build_buddy_url.String(), g)
	}
	return gURL
}

func makeGroups(groupRoles []*tables.GroupRole) []*grpb.Group {
	r := make([]*grpb.Group, 0)
	for _, gr := range groupRoles {
		g := gr.Group
		urlIdentifier := ""
		if g.URLIdentifier != nil {
			urlIdentifier = *g.URLIdentifier
		}
		githubToken := ""
		if g.GithubToken != nil {
			githubToken = *g.GithubToken
		}
		r = append(r, &grpb.Group{
			Id:                                g.GroupID,
			Name:                              g.Name,
			Role:                              role.ToProto(role.Role(gr.Role)),
			OwnedDomain:                       g.OwnedDomain,
			GithubLinked:                      githubToken != "",
			UrlIdentifier:                     urlIdentifier,
			SharingEnabled:                    g.SharingEnabled,
			UserOwnedKeysEnabled:              g.UserOwnedKeysEnabled,
			BotSuggestionsEnabled:             g.BotSuggestionsEnabled,
			DeveloperOrgCreationEnabled:       g.DeveloperOrgCreationEnabled,
			UseGroupOwnedExecutors:            g.UseGroupOwnedExecutors != nil && *g.UseGroupOwnedExecutors,
			RestrictCleanWorkflowRunsToAdmins: g.RestrictCleanWorkflowRunsToAdmins,
			EnforceIpRules:                    g.EnforceIPRules,
			SuggestionPreference:              g.SuggestionPreference,
			Url:                               getGroupUrl(&gr.Group),
		})
	}
	return r
}

func (s *BuildBuddyServer) getGroupIDForSubdomain(ctx context.Context) (string, error) {
	sd := subdomain.Get(ctx)
	if sd == "" {
		return "", nil
	}

	userDB := s.env.GetUserDB()
	if userDB == nil {
		return "", status.UnimplementedError("Not Implemented")
	}

	g, err := userDB.GetGroupByURLIdentifier(ctx, sd)
	if err != nil {
		return "", err
	}
	return g.GroupID, nil
}

func (s *BuildBuddyServer) GetUser(ctx context.Context, req *uspb.GetUserRequest) (*uspb.GetUserResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}

	auth := s.env.GetAuthenticator()
	if auth == nil {
		return nil, status.InternalError("No auth configured on this BuildBuddy instance")
	}
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	tu, err := s.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu == nil {
		// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
		return nil, status.UnauthenticatedErrorf("User %q not found", req.GetUserId())
	}

	selectedGroupID := ""
	selectedGroupRole := role.None
	selectedGroupAccess := uspb.SelectedGroup_DENIED
	if g := selectedGroup(ctx, req.GetRequestContext().GetGroupId(), tu.Groups); g != nil {
		selectedGroupID = g.Group.GroupID
		selectedGroupRole = role.Role(g.Role)
		selectedGroupAccess = uspb.SelectedGroup_ALLOWED
		if irs := s.env.GetIPRulesService(); irs != nil {
			err := irs.AuthorizeGroup(ctx, g.Group.GroupID)
			if status.IsPermissionDeniedError(err) {
				selectedGroupAccess = uspb.SelectedGroup_DENIED_BY_IP_RULES
			} else if err != nil {
				return nil, err
			}
		}
	}
	allowedRPCs := role_filter.RoleIndependentRPCs()
	if selectedGroupRole&role.Admin > 0 {
		allowedRPCs = append(allowedRPCs, role_filter.GroupAdminOnlyRPCs()...)
	}
	if selectedGroupRole&(role.Admin|role.Developer) > 0 {
		allowedRPCs = append(allowedRPCs, role_filter.GroupDeveloperRPCs()...)
	}

	subdomainGroupID := ""
	if serverAdminGID := s.env.GetAuthenticator().AdminGroupID(); serverAdminGID != "" {
		for _, gr := range tu.Groups {
			if gr.Group.GroupID == serverAdminGID && gr.Role == uint32(role.Admin) {
				allowedRPCs = append(allowedRPCs, role_filter.ServerAdminOnlyRPCs()...)
				gid, err := s.getGroupIDForSubdomain(ctx)
				if err != nil && !status.IsNotFoundError(err) {
					return nil, err
				}
				subdomainGroupID = gid
				break
			}
		}
	}

	return &uspb.GetUserResponse{
		DisplayUser:     tu.ToProto(),
		UserGroup:       makeGroups(tu.Groups),
		SelectedGroupId: selectedGroupID,
		SelectedGroup: &uspb.SelectedGroup{
			GroupId: selectedGroupID,
			Access:  selectedGroupAccess,
		},
		AllowedRpc:       allowedRPCs,
		GithubLinked:     tu.GithubToken != "",
		SubdomainGroupId: subdomainGroupID,
		IsImpersonating:  u.IsImpersonating(),
	}, nil
}

func (s *BuildBuddyServer) CreateUser(ctx context.Context, req *uspb.CreateUserRequest) (*uspb.CreateUserResponse, error) {
	auth := s.env.GetAuthenticator()
	userDB := s.env.GetUserDB()
	if auth == nil || userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	tu := &tables.User{}
	if err := auth.FillUser(ctx, tu); err != nil {
		return nil, err
	}
	if err := userDB.InsertUser(ctx, tu); err != nil {
		return nil, err
	}
	return &uspb.CreateUserResponse{
		DisplayUser: tu.ToProto(),
	}, nil
}

func (s *BuildBuddyServer) GetGroup(ctx context.Context, req *grpb.GetGroupRequest) (*grpb.GetGroupResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}

	var group *tables.Group
	if req.GetGroupId() != "" {
		// Looking up by group ID is restricted to server admins.
		u, err := perms.AuthenticatedUser(ctx, s.env)
		if err != nil {
			return nil, err
		}
		adminGroupID := s.env.GetAuthenticator().AdminGroupID()
		if adminGroupID == "" {
			return nil, status.PermissionDeniedError("Access denied")
		}
		if err := authutil.AuthorizeGroupRole(u, adminGroupID, role.Admin); err != nil {
			return nil, err
		}
		g, err := userDB.GetGroupByID(ctx, req.GetGroupId())
		if err != nil {
			return nil, err
		}
		group = g
	} else {
		urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())
		if urlIdentifier == "" {
			if sd := subdomain.Get(ctx); sd != "" {
				urlIdentifier = sd
			} else {
				return nil, status.InvalidArgumentError("URL identifier is required.")
			}
		}

		g, err := userDB.GetGroupByURLIdentifier(ctx, urlIdentifier)
		if err != nil {
			return nil, err
		}
		group = g
	}
	return &grpb.GetGroupResponse{
		Id: group.GroupID,
		// NOTE: this RPC does not require authentication, so sensitive group
		// info should not be exposed here.
		Name:        group.Name,
		OwnedDomain: group.OwnedDomain,
		SsoEnabled:  group.SamlIdpMetadataUrl != nil && *group.SamlIdpMetadataUrl != "",
		Url:         getGroupUrl(group),
	}, nil
}

func (s *BuildBuddyServer) GetGroupUsers(ctx context.Context, req *grpb.GetGroupUsersRequest) (*grpb.GetGroupUsersResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	users, err := userDB.GetGroupUsers(ctx, req.GetGroupId(), req.GetGroupMembershipStatus())
	if err != nil {
		return nil, err
	}
	return &grpb.GetGroupUsersResponse{
		User: users,
	}, nil
}

func (s *BuildBuddyServer) UpdateGroupUsers(ctx context.Context, req *grpb.UpdateGroupUsersRequest) (*grpb.UpdateGroupUsersResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if err := userDB.UpdateGroupUsers(ctx, req.GetRequestContext().GetGroupId(), req.GetUpdate()); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, req.GetRequestContext().GetGroupId(), alpb.Action_UPDATE_MEMBERSHIP, req)
	}
	return &grpb.UpdateGroupUsersResponse{}, nil
}

func (s *BuildBuddyServer) CreateGroup(ctx context.Context, req *grpb.CreateGroupRequest) (*grpb.CreateGroupResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	user, err := userDB.GetUser(ctx)
	if err != nil {
		return nil, err
	}

	groupName := strings.TrimSpace(req.GetName())
	if len(groupName) == 0 {
		return nil, status.InvalidArgumentError("Group name cannot be empty")
	}

	groupOwnedDomain := ""
	if req.GetAutoPopulateFromOwnedDomain() {
		userEmailDomain := getEmailDomain(user.Email)
		groupOwnedDomain = userEmailDomain
	}

	useGroupOwnedExecutors := req.GetUseGroupOwnedExecutors()
	group := &tables.Group{
		UserID:                      user.UserID,
		Name:                        groupName,
		OwnedDomain:                 groupOwnedDomain,
		SharingEnabled:              req.GetSharingEnabled(),
		UserOwnedKeysEnabled:        req.GetUserOwnedKeysEnabled(),
		BotSuggestionsEnabled:       req.GetBotSuggestionsEnabled(),
		DeveloperOrgCreationEnabled: req.GetDeveloperOrgCreationEnabled(),
		UseGroupOwnedExecutors:      &useGroupOwnedExecutors,
	}
	urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())
	group.URLIdentifier = &urlIdentifier
	group.SuggestionPreference = grpb.SuggestionPreference_ENABLED

	groupID, err := userDB.CreateGroup(ctx, group)
	if err != nil {
		return nil, err
	}
	return &grpb.CreateGroupResponse{
		Id:  groupID,
		Url: getGroupUrl(group),
	}, nil
}

func (s *BuildBuddyServer) UpdateGroup(ctx context.Context, req *grpb.UpdateGroupRequest) (*grpb.UpdateGroupResponse, error) {
	auth := s.env.GetAuthenticator()
	userDB := s.env.GetUserDB()
	if auth == nil || userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	var group *tables.Group
	var err error
	urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())

	if group == nil {
		if req.GetRequestContext().GetGroupId() == "" {
			return nil, status.InvalidArgumentError("Missing organization identifier.")
		}
		group, err = userDB.GetGroupByID(ctx, req.GetRequestContext().GetGroupId())
		if err != nil {
			return nil, err
		}
	}
	group.Name = req.GetName()
	if urlIdentifier != "" {
		group.URLIdentifier = &urlIdentifier
	}
	if req.GetAutoPopulateFromOwnedDomain() {
		user, err := userDB.GetUser(ctx)
		if err != nil {
			return nil, err
		}
		group.OwnedDomain = getEmailDomain(user.Email)
	} else {
		group.OwnedDomain = ""
	}
	group.SharingEnabled = req.GetSharingEnabled()
	useGroupOwnedExecutors := req.GetUseGroupOwnedExecutors()
	group.UserOwnedKeysEnabled = req.GetUserOwnedKeysEnabled()
	group.BotSuggestionsEnabled = req.GetBotSuggestionsEnabled()
	group.DeveloperOrgCreationEnabled = req.GetDeveloperOrgCreationEnabled()
	group.UseGroupOwnedExecutors = &useGroupOwnedExecutors
	group.SuggestionPreference = req.GetSuggestionPreference()
	group.RestrictCleanWorkflowRunsToAdmins = req.GetRestrictCleanWorkflowRunsToAdmins()
	if group.SuggestionPreference == grpb.SuggestionPreference_UNKNOWN_SUGGESTION_PREFERENCE {
		group.SuggestionPreference = grpb.SuggestionPreference_ENABLED
	}
	if _, err := userDB.InsertOrUpdateGroup(ctx, group); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, req.GetRequestContext().GetGroupId(), alpb.Action_UPDATE, req)
	}
	return &grpb.UpdateGroupResponse{}, nil
}

func (s *BuildBuddyServer) JoinGroup(ctx context.Context, req *grpb.JoinGroupRequest) (*grpb.JoinGroupResponse, error) {
	auth := s.env.GetAuthenticator()
	userDB := s.env.GetUserDB()
	if auth == nil || userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if _, err := userDB.RequestToJoinGroup(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &grpb.JoinGroupResponse{}, nil
}

func (s *BuildBuddyServer) GetApiKeys(ctx context.Context, req *akpb.GetApiKeysRequest) (*akpb.GetApiKeysResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	tableKeys, err := authDB.GetAPIKeys(ctx, req.GetRequestContext().GetGroupId())
	if err != nil {
		return nil, err
	}
	rsp := &akpb.GetApiKeysResponse{
		ApiKey: make([]*akpb.ApiKey, 0, len(tableKeys)),
	}
	for _, k := range tableKeys {
		// API Key value must be retrieved via GetAPIKey API.
		rsp.ApiKey = append(rsp.ApiKey, &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Label:               k.Label,
			Capability:          capabilities.FromInt(k.Capabilities),
			VisibleToDevelopers: k.VisibleToDevelopers,
		})
	}
	return rsp, nil
}

func (s *BuildBuddyServer) GetApiKey(ctx context.Context, req *akpb.GetApiKeyRequest) (*akpb.GetApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	key, err := authDB.GetAPIKey(ctx, req.GetApiKeyId())
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_GROUP_API_KEY,
			Id:   req.GetApiKeyId(),
			Name: key.Label,
		}
		al.Log(ctx, rid, alpb.Action_ACCESS, req)
	}
	return &akpb.GetApiKeyResponse{
		ApiKey: &akpb.ApiKey{
			Id:                  key.APIKeyID,
			Value:               key.Value,
			Label:               key.Label,
			Capability:          capabilities.FromInt(key.Capabilities),
			VisibleToDevelopers: key.VisibleToDevelopers,
		},
	}, nil
}

func (s *BuildBuddyServer) CreateApiKey(ctx context.Context, req *akpb.CreateApiKeyRequest) (*akpb.CreateApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	k, err := authDB.CreateAPIKey(
		ctx, req.GetRequestContext().GetGroupId(), req.GetLabel(), req.GetCapability(),
		req.GetVisibleToDevelopers())
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_GROUP_API_KEY,
			Id:   k.APIKeyID,
			Name: k.Label,
		}
		al.Log(ctx, rid, alpb.Action_CREATE, req)
	}
	return &akpb.CreateApiKeyResponse{
		ApiKey: &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Value:               k.Value,
			Label:               k.Label,
			Capability:          capabilities.FromInt(k.Capabilities),
			VisibleToDevelopers: k.VisibleToDevelopers,
		},
	}, nil
}

func (s *BuildBuddyServer) authorizeInvocationWrite(ctx context.Context, invocationID string) error {
	auth := s.env.GetAuthenticator()
	if auth == nil {
		return status.UnimplementedError("Not Implemented")
	}
	authenticatedUser, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return err
	}

	in, err := s.env.GetInvocationDB().LookupInvocation(ctx, invocationID)
	if err != nil {
		return err
	}
	acl := perms.ToACLProto(&uidpb.UserId{Id: in.UserID}, in.GroupID, in.Perms)

	return perms.AuthorizeWrite(&authenticatedUser, acl)
}

func (s *BuildBuddyServer) UpdateApiKey(ctx context.Context, req *akpb.UpdateApiKeyRequest) (*akpb.UpdateApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	existingKey, err := authDB.GetAPIKey(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	tk := &tables.APIKey{
		APIKeyID:            req.GetId(),
		Label:               req.GetLabel(),
		Capabilities:        capabilities.ToInt(req.GetCapability()),
		VisibleToDevelopers: req.GetVisibleToDevelopers(),
	}
	if err := authDB.UpdateAPIKey(ctx, tk); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_GROUP_API_KEY,
			Id:   req.GetId(),
			Name: existingKey.Label,
		}
		al.Log(ctx, rid, alpb.Action_UPDATE, req)
	}
	return &akpb.UpdateApiKeyResponse{}, nil
}

func (s *BuildBuddyServer) DeleteApiKey(ctx context.Context, req *akpb.DeleteApiKeyRequest) (*akpb.DeleteApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	existingKey, err := authDB.GetAPIKey(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := authDB.DeleteAPIKey(ctx, req.GetId()); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_GROUP_API_KEY,
			Id:   req.GetId(),
			Name: existingKey.Label,
		}
		al.Log(ctx, rid, alpb.Action_DELETE, req)
	}
	return &akpb.DeleteApiKeyResponse{}, nil
}

func (s *BuildBuddyServer) CreateImpersonationApiKey(ctx context.Context, req *akpb.CreateImpersonationApiKeyRequest) (*akpb.CreateImpersonationApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	k, err := authDB.CreateImpersonationAPIKey(ctx, req.GetRequestContext().GetGroupId())
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, req.GetRequestContext().GetGroupId(), alpb.Action_CREATE_IMPERSONATION_API_KEY, req)
	}
	return &akpb.CreateImpersonationApiKeyResponse{
		ApiKey: &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Value:               k.Value,
			Label:               k.Label,
			Capability:          capabilities.FromInt(k.Capabilities),
			VisibleToDevelopers: k.VisibleToDevelopers,
			ExpiryUsec:          k.ExpiryUsec,
		},
	}, nil
}

func (s *BuildBuddyServer) GetUserApiKeys(ctx context.Context, req *akpb.GetApiKeysRequest) (*akpb.GetApiKeysResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil || !authDB.GetUserOwnedKeysEnabled() {
		return nil, status.UnimplementedError("Not Implemented")
	}
	groupID := req.GetRequestContext().GetGroupId()
	tableKeys, err := authDB.GetUserAPIKeys(ctx, groupID)
	if err != nil {
		return nil, err
	}
	rsp := &akpb.GetApiKeysResponse{
		ApiKey: make([]*akpb.ApiKey, 0, len(tableKeys)),
	}
	for _, k := range tableKeys {
		// API Key value must be retrieved via GetUserAPIKey API.
		rsp.ApiKey = append(rsp.ApiKey, &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Label:               k.Label,
			Capability:          capabilities.FromInt(k.Capabilities),
			VisibleToDevelopers: k.VisibleToDevelopers,
		})
	}
	return rsp, nil
}

func (s *BuildBuddyServer) GetUserApiKey(ctx context.Context, req *akpb.GetApiKeyRequest) (*akpb.GetApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	key, err := authDB.GetAPIKey(ctx, req.GetApiKeyId())
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_USER_API_KEY,
			Id:   req.GetApiKeyId(),
			Name: key.Label,
		}
		al.Log(ctx, rid, alpb.Action_ACCESS, req)
	}
	return &akpb.GetApiKeyResponse{
		ApiKey: &akpb.ApiKey{
			Id:                  key.APIKeyID,
			Value:               key.Value,
			Label:               key.Label,
			Capability:          capabilities.FromInt(key.Capabilities),
			VisibleToDevelopers: key.VisibleToDevelopers,
		},
	}, nil
}

func (s *BuildBuddyServer) CreateUserApiKey(ctx context.Context, req *akpb.CreateApiKeyRequest) (*akpb.CreateApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil || !authDB.GetUserOwnedKeysEnabled() {
		return nil, status.UnimplementedError("Not Implemented")
	}
	k, err := authDB.CreateUserAPIKey(ctx, req.GetRequestContext().GetGroupId(), req.GetLabel(), req.GetCapability())
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_USER_API_KEY,
			Id:   k.APIKeyID,
			Name: k.Label,
		}
		al.Log(ctx, rid, alpb.Action_CREATE, req)
	}
	return &akpb.CreateApiKeyResponse{
		ApiKey: &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Value:               k.Value,
			Label:               k.Label,
			Capability:          capabilities.FromInt(k.Capabilities),
			VisibleToDevelopers: k.VisibleToDevelopers,
		},
	}, nil

}

func (s *BuildBuddyServer) UpdateUserApiKey(ctx context.Context, req *akpb.UpdateApiKeyRequest) (*akpb.UpdateApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil || !authDB.GetUserOwnedKeysEnabled() {
		return nil, status.UnimplementedError("Not Implemented")
	}
	existingKey, err := authDB.GetAPIKey(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	updates := &tables.APIKey{
		APIKeyID:            req.GetId(),
		Label:               req.GetLabel(),
		Capabilities:        capabilities.ToInt(req.GetCapability()),
		VisibleToDevelopers: req.GetVisibleToDevelopers(),
	}
	if err := authDB.UpdateAPIKey(ctx, updates); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_USER_API_KEY,
			Id:   req.GetId(),
			Name: existingKey.Label,
		}
		al.Log(ctx, rid, alpb.Action_UPDATE, req)
	}
	return &akpb.UpdateApiKeyResponse{}, nil
}

func (s *BuildBuddyServer) DeleteUserApiKey(ctx context.Context, req *akpb.DeleteApiKeyRequest) (*akpb.DeleteApiKeyResponse, error) {
	authDB := s.env.GetAuthDB()
	if authDB == nil || !authDB.GetUserOwnedKeysEnabled() {
		return nil, status.UnimplementedError("Not Implemented")
	}
	existingKey, err := authDB.GetAPIKey(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := authDB.DeleteAPIKey(ctx, req.GetId()); err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_USER_API_KEY,
			Id:   req.GetId(),
			Name: existingKey.Label,
		}
		al.Log(ctx, rid, alpb.Action_DELETE, req)
	}
	return &akpb.DeleteApiKeyResponse{}, nil
}

func selectedGroup(ctx context.Context, preferredGroupID string, groupRoles []*tables.GroupRole) *tables.GroupRole {
	if sd := subdomain.Get(ctx); sd != "" {
		for _, gr := range groupRoles {
			if gr.Group.URLIdentifier != nil && *gr.Group.URLIdentifier == sd {
				return gr
			}
		}
		return nil
	}
	if preferredGroupID != "" {
		for _, gr := range groupRoles {
			if gr.Group.GroupID == preferredGroupID {
				return gr
			}
		}
	}
	for _, gr := range groupRoles {
		if gr.Group.URLIdentifier != nil && *gr.Group.URLIdentifier != "" {
			return gr
		}
	}
	for _, gr := range groupRoles {
		if gr.Group.OwnedDomain != "" {
			return gr
		}
	}
	if len(groupRoles) > 0 {
		return groupRoles[0]
	}
	return nil
}

func getEmailDomain(email string) string {
	chunks := strings.Split(email, "@")
	return chunks[len(chunks)-1]
}

func makeConfigOption(lifecycle, flagName, flagValue string) *bzpb.ConfigOption {
	return &bzpb.ConfigOption{
		Body:            fmt.Sprintf("%s --%s=%s", lifecycle, flagName, flagValue),
		OptionLifecycle: lifecycle,
		FlagName:        flagName,
		FlagValue:       flagValue,
	}
}

func assembleURL(host, scheme, port string) string {
	// Strip any existing port from host if we're setting a port.
	components := strings.Split(host, ":")
	if len(components) > 1 && port != "" {
		host = components[0]
	}

	url := scheme + "//" + host

	// Only append port if it's set and not 80.
	if port != "" && port != "80" {
		url = url + ":" + port
	}
	return url
}

func toProtoAPIKeys(tableKeys []*tables.APIKey) []*akpb.ApiKey {
	protoKeys := make([]*akpb.ApiKey, len(tableKeys))
	for i, k := range tableKeys {
		protoKeys[i] = &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Value:               k.Value,
			Label:               k.Label,
			VisibleToDevelopers: k.VisibleToDevelopers,
			UserOwned:           k.Perms&perms.OWNER_READ > 0,
		}
	}
	return protoKeys
}

func (s *BuildBuddyServer) getAPIKeysForAuthorizedGroup(ctx context.Context) ([]*akpb.ApiKey, error) {
	groupID := ""
	if reqCtx := requestcontext.ProtoRequestContextFromContext(ctx); reqCtx != nil {
		groupID = reqCtx.GetGroupId()
	}
	if groupID == "" {
		return []*akpb.ApiKey{}, nil
	}
	if err := perms.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}
	auth := s.env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}

	// List user-owned keys first.
	var userKeys []*tables.APIKey
	if authDB.GetUserOwnedKeysEnabled() {
		keys, err := authDB.GetUserAPIKeys(ctx, groupID)
		// PermissionDenied means the Group doesn't have user-owned API keys
		// enabled; ignore.
		if err != nil && !status.IsPermissionDeniedError(err) {
			return nil, err
		}
		userKeys = keys
	}
	// Then list group-owned keys
	groupKeys, err := authDB.GetAPIKeys(ctx, groupID)
	if err != nil {
		return nil, err
	}
	return toProtoAPIKeys(append(userKeys, groupKeys...)), nil
}

func (s *BuildBuddyServer) GetBazelConfig(ctx context.Context, req *bzpb.GetBazelConfigRequest) (*bzpb.GetBazelConfigResponse, error) {
	configOptions := make([]*bzpb.ConfigOption, 0)

	var g *tables.Group
	if subdomain.Enabled() {
		u, err := perms.AuthenticatedUser(ctx, s.env)
		if err != nil {
			return nil, err
		}
		g, err = s.env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
		if err != nil {
			return nil, err
		}
	}
	replaceSubdomain := func(url string) string {
		if g == nil {
			return url
		}
		return subdomain.ReplaceURLSubdomainForGroup(url, g)
	}

	// Use config urls if they're set and fall back to host & protocol from request if not.
	resultsURL := build_buddy_url.WithPath("/invocation/").String()
	if build_buddy_url.String() == "" {
		resultsURL = assembleURL(req.Host, req.Protocol, "")
		resultsURL += "/invocation/"
	}
	configOptions = append(configOptions, makeConfigOption("build", "bes_results_url", replaceSubdomain(resultsURL)))

	grpcPort := "1985"
	if p, err := flagutil.GetDereferencedValue[int]("grpc_port"); err == nil {
		grpcPort = strconv.Itoa(p)
	}
	eventsAPIURL := events_api_url.String()
	if eventsAPIURL == "" {
		eventsAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
	}
	groupAPIKeys, err := s.getAPIKeysForAuthorizedGroup(ctx)
	if err != nil {
		return nil, err
	}

	configOptions = append(configOptions, makeConfigOption("build", "bes_backend", replaceSubdomain(eventsAPIURL)))

	if s.env.GetCache() != nil {
		cacheAPIURL := cache_api_url.String()
		if cacheAPIURL == "" {
			cacheAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		configOptions = append(configOptions, makeConfigOption("build", "remote_cache", replaceSubdomain(cacheAPIURL)))
	}

	if remote_execution_config.RemoteExecutionEnabled() {
		remoteExecutionAPIURL := remote_exec_api_url.String()
		if remoteExecutionAPIURL == "" {
			remoteExecutionAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		configOptions = append(configOptions, makeConfigOption("build", "remote_executor", replaceSubdomain(remoteExecutionAPIURL)))
	}

	credentials := make([]*bzpb.Credentials, len(groupAPIKeys))
	for i, apiKey := range groupAPIKeys {
		// API Key value must be retrieved via GetAPIKey API.
		apiKey.Value = ""
		credentials[i] = &bzpb.Credentials{
			ApiKey: apiKey,
		}
	}

	if req.GetIncludeCertificate() && s.sslService.IsCertGenerationEnabled() && !*disableCertConfig {
		for _, c := range credentials {
			cert, key, err := s.sslService.GenerateCerts(c.ApiKey.Id)
			if err != nil {
				return nil, status.InternalError(fmt.Sprintf("Error generating cert: %+v", err))
			}
			c.Certificate = &bzpb.Certificate{
				Cert: cert,
				Key:  key,
			}
		}
	}

	return &bzpb.GetBazelConfigResponse{
		ConfigOption: configOptions,
		Credential:   credentials,
	}, nil
}

func (s *BuildBuddyServer) GetInvocationStat(ctx context.Context, req *inpb.GetInvocationStatRequest) (*inpb.GetInvocationStatResponse, error) {
	if iss := s.env.GetInvocationStatService(); iss != nil {
		return iss.GetInvocationStat(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetTrend(ctx context.Context, req *stpb.GetTrendRequest) (*stpb.GetTrendResponse, error) {
	if iss := s.env.GetInvocationStatService(); iss != nil {
		return iss.GetTrend(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetStatHeatmap(ctx context.Context, req *stpb.GetStatHeatmapRequest) (*stpb.GetStatHeatmapResponse, error) {
	if iss := s.env.GetInvocationStatService(); iss != nil {
		return iss.GetStatHeatmap(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetStatDrilldown(ctx context.Context, req *stpb.GetStatDrilldownRequest) (*stpb.GetStatDrilldownResponse, error) {
	if iss := s.env.GetInvocationStatService(); iss != nil {
		return iss.GetStatDrilldown(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetInvocationOwner(ctx context.Context, req *inpb.GetInvocationOwnerRequest) (*inpb.GetInvocationOwnerResponse, error) {
	gid, err := s.env.GetInvocationDB().LookupGroupIDFromInvocation(ctx, req.GetInvocationId())
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationOwnerResponse{GroupId: gid}, nil
}

func (s *BuildBuddyServer) GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error) {
	if es := s.env.GetExecutionService(); es != nil {
		return es.GetExecution(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetTreeDirectorySizes(ctx context.Context, req *capb.GetTreeDirectorySizesRequest) (*capb.GetTreeDirectorySizesResponse, error) {
	return directory_size.GetTreeDirectorySizes(ctx, s.env, req)
}

func (s *BuildBuddyServer) GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error) {
	if ss := s.env.GetSchedulerService(); ss != nil {
		res, err := ss.GetExecutionNodes(ctx, req)
		return res, err
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) SearchExecution(ctx context.Context, req *espb.SearchExecutionRequest) (*espb.SearchExecutionResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentErrorf("SearchExecutionRequest cannot be empty")
	}
	searcher := s.env.GetExecutionSearchService()
	if searcher == nil {
		return nil, fmt.Errorf("No searcher was configured")
	}
	return searcher.SearchExecutions(ctx, req)
}

func (s *BuildBuddyServer) GetTargetHistory(ctx context.Context, req *trpb.GetTargetHistoryRequest) (*trpb.GetTargetHistoryResponse, error) {
	return target.GetTargetHistory(ctx, s.env, req)
}

func (s *BuildBuddyServer) GetEventLogChunk(ctx context.Context, req *elpb.GetEventLogChunkRequest) (*elpb.GetEventLogChunkResponse, error) {
	resp, err := eventlog.GetEventLogChunk(ctx, s.env, req)
	if err != nil {
		log.Errorf("Encountered error getting event log chunk: %s\nRequest: %s", err, req)
	}
	return resp, err
}

func (s *BuildBuddyServer) CreateWorkflow(ctx context.Context, req *wfpb.CreateWorkflowRequest) (*wfpb.CreateWorkflowResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
		return wfs.CreateWorkflow(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) DeleteWorkflow(ctx context.Context, req *wfpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
		return wfs.DeleteWorkflow(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) GetWorkflows(ctx context.Context, req *wfpb.GetWorkflowsRequest) (*wfpb.GetWorkflowsResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
		return wfs.GetWorkflows(ctx)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) GetWorkflowHistory(ctx context.Context, req *wfpb.GetWorkflowHistoryRequest) (*wfpb.GetWorkflowHistoryResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
		return wfs.GetWorkflowHistory(ctx)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) ExecuteWorkflow(ctx context.Context, req *wfpb.ExecuteWorkflowRequest) (*wfpb.ExecuteWorkflowResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
		// Set the workflow ID if it's not on the request
		// Note: This will only work for workflows created with the github app integration and not the legacy approach
		if req.GetWorkflowId() == "" {
			auth := s.env.GetAuthenticator()
			if auth == nil {
				return nil, status.UnimplementedError("Not Implemented")
			}
			authenticatedUser, err := auth.AuthenticatedUser(ctx)
			if err != nil {
				return nil, err
			}
			if authenticatedUser.GetGroupID() == "" {
				return nil, status.InternalErrorf("authenticated user's group ID is empty")
			}

			req.WorkflowId = wfs.GetLegacyWorkflowIDForGitRepository(authenticatedUser.GetGroupID(), req.GetTargetRepoUrl())
		}
		if al := s.env.GetAuditLogger(); al != nil && req.GetClean() {
			al.LogForGroup(ctx, req.GetRequestContext().GetGroupId(), alpb.Action_EXECUTE_CLEAN_WORKFLOW, req)
		}
		return wfs.ExecuteWorkflow(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) GetRepos(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
		return wfs.GetRepos(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) UnlinkGitHubAccount(ctx context.Context, req *ghpb.UnlinkGitHubAccountRequest) (*ghpb.UnlinkGitHubAccountResponse, error) {
	udb := s.env.GetUserDB()
	if udb == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InternalErrorf("authenticated user's group ID is empty")
	}

	if req.UnlinkUserAccount {
		if err := udb.DeleteUserGitHubToken(ctx); err != nil {
			return nil, err
		}
		return &ghpb.UnlinkGitHubAccountResponse{}, nil
	}

	// Lookup linked token
	g, err := udb.GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}
	if g.GithubToken == nil || *g.GithubToken == "" {
		return nil, status.NotFoundError("no linked GitHub account was found")
	}

	res := &ghpb.UnlinkGitHubAccountResponse{}
	// Delete workflows linked with this GitHub token
	if ws := s.env.GetWorkflowService(); ws != nil {
		wfids, err := ws.GetLinkedWorkflows(ctx, *g.GithubToken)
		if err != nil {
			return nil, err
		}
		for _, id := range wfids {
			if _, err := ws.DeleteWorkflow(ctx, &wfpb.DeleteWorkflowRequest{Id: id}); err != nil {
				// TODO(bduffany): Treat only webhook unlink errors as warnings;
				// failing to delete the workflow from the DB should be fatal
				res.Warning = append(res.Warning, err.Error())
			}
		}
	}
	// Remove the token association from the group
	if err := udb.DeleteGroupGitHubToken(ctx, u.GetGroupID()); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *BuildBuddyServer) LinkGitHubAppInstallation(ctx context.Context, req *ghpb.LinkAppInstallationRequest) (*ghpb.LinkAppInstallationResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.LinkGitHubAppInstallation(ctx, req)
}
func (s *BuildBuddyServer) GetGitHubAppInstallations(ctx context.Context, req *ghpb.GetAppInstallationsRequest) (*ghpb.GetAppInstallationsResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGitHubAppInstallations(ctx, req)
}
func (s *BuildBuddyServer) UnlinkGitHubAppInstallation(ctx context.Context, req *ghpb.UnlinkAppInstallationRequest) (*ghpb.UnlinkAppInstallationResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.UnlinkGitHubAppInstallation(ctx, req)
}
func (s *BuildBuddyServer) GetAccessibleGitHubRepos(ctx context.Context, req *ghpb.GetAccessibleReposRequest) (*ghpb.GetAccessibleReposResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetAccessibleGitHubRepos(ctx, req)
}
func (s *BuildBuddyServer) GetLinkedGitHubRepos(ctx context.Context, req *ghpb.GetLinkedReposRequest) (*ghpb.GetLinkedReposResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetLinkedGitHubRepos(ctx)
}
func (s *BuildBuddyServer) LinkGitHubRepo(ctx context.Context, req *ghpb.LinkRepoRequest) (*ghpb.LinkRepoResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	rsp, err := a.LinkGitHubRepo(ctx, req)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, req.GetRequestContext().GroupId, alpb.Action_LINK_GITHUB_REPO, req)
	}
	return rsp, nil
}
func (s *BuildBuddyServer) UnlinkGitHubRepo(ctx context.Context, req *ghpb.UnlinkRepoRequest) (*ghpb.UnlinkRepoResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	rsp, err := a.UnlinkGitHubRepo(ctx, req)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, req.GetRequestContext().GroupId, alpb.Action_UNLINK_GITHUB_REPO, req)
	}
	return rsp, nil
}

func (s *BuildBuddyServer) Run(ctx context.Context, req *rnpb.RunRequest) (*rnpb.RunResponse, error) {
	if rs := s.env.GetRunnerService(); rs != nil {
		return rs.Run(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	if us := s.env.GetUsageService(); us != nil {
		return us.GetUsage(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetSuggestion(ctx context.Context, req *supb.GetSuggestionRequest) (*supb.GetSuggestionResponse, error) {
	if us := s.env.GetSuggestionService(); us != nil {
		return us.GetSuggestion(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetCacheMetadata(ctx context.Context, req *capb.GetCacheMetadataRequest) (*capb.GetCacheMetadataResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	resourceName := req.GetResourceName()
	metadata, err := s.env.GetCache().Metadata(ctx, resourceName)
	if err != nil {
		return nil, err
	}

	return &capb.GetCacheMetadataResponse{
		StoredSizeBytes: metadata.StoredSizeBytes,
		DigestSizeBytes: metadata.DigestSizeBytes,
		LastAccessUsec:  metadata.LastAccessTimeUsec,
		LastModifyUsec:  metadata.LastModifyTimeUsec,
	}, nil
}

func (s *BuildBuddyServer) GetCacheScoreCard(ctx context.Context, req *capb.GetCacheScoreCardRequest) (*capb.GetCacheScoreCardResponse, error) {
	return scorecard.GetCacheScoreCard(ctx, s.env, req)
}

func (s *BuildBuddyServer) GetNamespace(ctx context.Context, req *qpb.GetNamespaceRequest) (*qpb.GetNamespaceResponse, error) {
	if qm := s.env.GetQuotaManager(); qm != nil {
		return qm.GetNamespace(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) RemoveNamespace(ctx context.Context, req *qpb.RemoveNamespaceRequest) (*qpb.RemoveNamespaceResponse, error) {
	if qm := s.env.GetQuotaManager(); qm != nil {
		return qm.RemoveNamespace(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) ModifyNamespace(ctx context.Context, req *qpb.ModifyNamespaceRequest) (*qpb.ModifyNamespaceResponse, error) {
	if qm := s.env.GetQuotaManager(); qm != nil {
		return qm.ModifyNamespace(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) ApplyBucket(ctx context.Context, req *qpb.ApplyBucketRequest) (*qpb.ApplyBucketResponse, error) {
	if qm := s.env.GetQuotaManager(); qm != nil {
		return qm.ApplyBucket(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetPublicKey(ctx context.Context, req *skpb.GetPublicKeyRequest) (*skpb.GetPublicKeyResponse, error) {
	if secretService := s.env.GetSecretService(); secretService != nil {
		return secretService.GetPublicKey(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) ListSecrets(ctx context.Context, req *skpb.ListSecretsRequest) (*skpb.ListSecretsResponse, error) {
	if secretService := s.env.GetSecretService(); secretService != nil {
		return secretService.ListSecrets(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) UpdateSecret(ctx context.Context, req *skpb.UpdateSecretRequest) (*skpb.UpdateSecretResponse, error) {
	if secretService := s.env.GetSecretService(); secretService != nil {
		rsp, newSecret, err := secretService.UpdateSecret(ctx, req)
		if err != nil {
			return nil, err
		}
		if al := s.env.GetAuditLogger(); al != nil {
			action := alpb.Action_UPDATE
			if newSecret {
				action = alpb.Action_CREATE
			}
			req.GetSecret().Value = ""
			al.LogForSecret(ctx, req.GetSecret().GetName(), action, req)
		}
		return rsp, err
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) DeleteSecret(ctx context.Context, req *skpb.DeleteSecretRequest) (*skpb.DeleteSecretResponse, error) {
	if secretService := s.env.GetSecretService(); secretService != nil {
		return secretService.DeleteSecret(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

type bsLookup struct {
	URL      *url.URL
	Filename string
}

func getBestFilename(filename, blobname string) string {
	// First try to use the filename parameter
	parts := strings.Split(filename, "/")
	name := parts[len(parts)-1]
	if name != "" {
		return name
	}
	// Next try to extract a reasonable name from the blob name.
	parts = strings.Split(blobname, "/")
	if len(parts) == 4 {
		return parts[2]
	}
	// Finally, just return the blobname.
	return blobname
}

func parseByteStreamURL(bsURL, filename string) (*bsLookup, error) {
	if strings.HasPrefix(bsURL, bytestreamProtocolPrefix) || strings.HasPrefix(bsURL, actioncacheProtocolPrefix) {
		u, err := url.Parse(bsURL)
		if err != nil {
			return nil, err
		}
		return &bsLookup{
			URL:      u,
			Filename: getBestFilename(filename, u.RequestURI()),
		}, nil
	}
	return nil, fmt.Errorf("unparsable bytestream URL: '%s'", bsURL)
}

func (s *BuildBuddyServer) getAnyAPIKeyForInvocation(ctx context.Context, invocationID string) (*tables.APIKey, error) {
	// LookupInvocation implicitly checks the logged-in user's access to invocationID.
	in, err := s.env.GetInvocationDB().LookupInvocation(ctx, invocationID)
	if err != nil {
		return nil, err
	}
	authDB := s.env.GetAuthDB()
	if authDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	// We can use any API key because LookupInvocation above already confirmed authorization.
	groupKey, err := authDB.GetAPIKeyForInternalUseOnly(ctx, in.GroupID)
	if err != nil && !status.IsNotFoundError(err) {
		return nil, err
	}
	if groupKey != nil {
		return groupKey, nil
	}
	// If we couldn't find any group-level keys, look up user-level keys for
	// the authenticated user. This handles the edge case where an org
	// *only* has user-level keys.
	if !authDB.GetUserOwnedKeysEnabled() {
		return nil, status.NotFoundErrorf("the organization does not have any API keys configured")
	}
	apiKeys, err := authDB.GetUserAPIKeys(ctx, in.GroupID)
	if err != nil {
		return nil, err
	}
	if len(apiKeys) == 0 {
		return nil, status.NotFoundError("The group that owns this invocation doesn't have any API keys configured.")
	}
	return apiKeys[0], nil
}

// ServeHTTP handles requests for build logs and artifacts either by looking
// them up from our cache servers using the bytestream API or pulling them
// from blobstore.
func (s *BuildBuddyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	var code int
	var err error
	if params.Get("artifact") != "" {
		code, err = s.serveArtifact(r.Context(), w, params)
	} else if params.Get("bytestream_url") != "" {
		// bytestream request
		code, err = s.serveBytestream(r.Context(), w, params)
		if err != nil && code == http.StatusNotFound {
			// Fall back to blobstore if object is not in cache
			code, err = s.serveArtifact(r.Context(), w, params)
		}
	} else {
		code = http.StatusBadRequest
		err = status.FailedPreconditionError(`One of "artifact" or "bytestream_url" query param is required`)
	}
	if err != nil {
		http.Error(w, err.Error(), code)
	}
}

// serveArtifact handles requests that specify particular build artifacts
func (s *BuildBuddyServer) serveArtifact(ctx context.Context, w http.ResponseWriter, params url.Values) (int, error) {
	iid := params.Get("invocation_id")
	if iid == "" {
		return http.StatusBadRequest, status.FailedPreconditionError("Missing invocation_id param")
	}
	if _, err := s.env.GetInvocationDB().LookupInvocation(ctx, iid); err != nil {
		if status.IsPermissionDeniedError(err) {
			return http.StatusForbidden, status.PermissionDeniedErrorf("User does not have permissions to access invocation %s", iid)
		} else if status.IsNotFoundError(err) {
			return http.StatusNotFound, status.NotFoundErrorf("Invocation %s does not exist.", iid)
		} else {
			log.Errorf("Error looking up invocation %s for build log fetch: %s", iid, err)
			return http.StatusInternalServerError, status.InternalErrorf("Internal server error")
		}
	}
	switch artifact := params.Get("artifact"); artifact {
	case "raw_json":
		if err := s.serveRawEventJSON(ctx, w, iid); err != nil {
			return http.StatusInternalServerError, err
		}
		return http.StatusOK, nil
	case "buildlog":
		attempt, err := strconv.ParseUint(params.Get("attempt"), 10, 64)
		if err != nil {
			err = status.FailedPreconditionErrorf(
				"Attempt param '%s' is not parseable to uint64.",
				params.Get("attempt"),
			)
			return http.StatusBadRequest, err
		}
		c := chunkstore.New(
			s.env.GetBlobstore(),
			&chunkstore.ChunkstoreOptions{},
		)
		// Stream the file back to our client
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=invocation-%s.log", iid))
		w.Header().Set("Content-Type", "application/octet-stream")
		path := eventlog.GetEventLogPathFromInvocationIdAndAttempt(iid, attempt)
		if _, err = io.Copy(w, c.Reader(ctx, path)); err != nil {
			log.Warningf("Error serving invocation-%s.log: %s", iid, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	case "": // fallback for cache artifact
		lookup, err := parseByteStreamURL(params.Get("bytestream_url"), params.Get("filename"))
		if err != nil {
			return http.StatusBadRequest, status.FailedPreconditionErrorf("Could not parse bytestream_url '%s' for cache artifact.", params.Get("bytestream_url"))
		}
		b, err := s.env.GetBlobstore().ReadBlob(ctx, path.Join(iid, "artifacts", "cache", lookup.URL.Path))
		if err != nil {
			log.Warningf("Error serving timing profile '%s' for invocation %s: %s", lookup.Filename, iid, err)
			return http.StatusInternalServerError, status.InternalErrorf("Internal server error")
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", lookup.Filename))
		w.Header().Set("Content-Type", "application/octet-stream")
		if strings.HasSuffix(lookup.Filename, ".gz") {
			w.Header().Set("Content-Encoding", "gzip")
		}
		w.Write(b)
	default:
		return http.StatusBadRequest, status.FailedPreconditionErrorf("Unrecognized artifact \"%s\" requested.", params.Get("artifact"))
	}
	return http.StatusOK, nil
}

func (s *BuildBuddyServer) serveRawEventJSON(ctx context.Context, w http.ResponseWriter, iid string) (err error) {
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_raw.json", iid))
	w.Header().Set("Content-Type", "application/json")

	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	n := 0
	_, err = build_event_handler.LookupInvocationWithCallback(ctx, s.env, iid, func(event *inpb.InvocationEvent) error {
		prefix := "\n"
		if n > 0 {
			prefix = ",\n"
		}
		if _, err := io.WriteString(w, prefix); err != nil {
			return err
		}
		b, err := protojson.Marshal(event.GetBuildEvent())
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		n++
		return nil
	})
	if err != nil {
		return err
	}
	end := "\n]"
	if n == 0 {
		end = "]"
	}
	if _, err := io.WriteString(w, end); err != nil {
		return err
	}
	return err
}

// serveBytestream handles requests that specify bytestream URLs.
func (s *BuildBuddyServer) serveBytestream(ctx context.Context, w http.ResponseWriter, params url.Values) (int, error) {
	lookup, err := parseByteStreamURL(params.Get("bytestream_url"), params.Get("filename"))
	if err != nil {
		return http.StatusBadRequest, err
	}

	if lookup.URL.User == nil {
		// Note that this implicitly authorizes the logged-in user's access to the invocation.
		apiKey, _ := s.getAnyAPIKeyForInvocation(ctx, params.Get("invocation_id"))
		if apiKey != nil {
			ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey.Value)
		}
	}

	// TODO(siggisim): Figure out why this JWT is overriding authority auth and remove.
	ctx = context.WithValue(ctx, "x-buildbuddy-jwt", nil)

	var zipReference = params.Get("z")
	if len(zipReference) > 0 {
		b, err := base64.StdEncoding.DecodeString(zipReference)
		if err != nil {
			log.Warningf("Error downloading file: %s", err)
			return http.StatusBadRequest, status.FailedPreconditionErrorf("\"%s\" is an invalid base64 string.", zipReference)
		}
		entry := zipb.ManifestEntryFromVTPool()
		defer entry.ReturnToVTPool()
		if err := proto.Unmarshal(b, entry); err != nil {
			log.Warningf("Failed to unmarshal ManifestEntry: %s", err)
			return http.StatusBadRequest, status.FailedPreconditionErrorf("\"%s\" does not represent a valid ManifestEntry proto when base64 decoded.", zipReference)
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", entry.GetName()))
		// TODO(jdhollen): Parse output mime type from bazel-generated MANIFEST file.
		w.Header().Set("Content-Type", "application/octet-stream")
		err = s.env.GetPooledByteStreamClient().StreamSingleFileFromBytestreamZip(ctx, lookup.URL, entry, w)
		if err != nil {
			if status.IsInvalidArgumentError(err) {
				return http.StatusBadRequest, err
			}
			return http.StatusNotFound, status.NotFoundErrorf("File not found.")
		}
		return http.StatusOK, nil
	}

	// Stream the file back to our client
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", lookup.Filename))
	w.Header().Set("Content-Type", "application/octet-stream")

	err = s.env.GetPooledByteStreamClient().StreamBytestreamFile(ctx, lookup.URL, w)

	if err != nil {
		if status.IsInvalidArgumentError(err) {
			return http.StatusBadRequest, err
		}
		return http.StatusNotFound, status.NotFoundErrorf("File not found.")
	}
	return http.StatusOK, nil
}

func (s *BuildBuddyServer) SetEncryptionConfig(ctx context.Context, request *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	crypter := s.env.GetCrypter()
	if crypter == nil {
		return nil, status.UnimplementedError("Encryption not configured")
	}
	rsp, err := crypter.SetEncryptionConfig(ctx, request)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, request.GetRequestContext().GetGroupId(), alpb.Action_UPDATE_ENCRYPTION_CONFIG, request)
	}
	return rsp, nil
}

func (s *BuildBuddyServer) GetEncryptionConfig(ctx context.Context, request *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	crypter := s.env.GetCrypter()
	if crypter == nil {
		return nil, status.UnimplementedError("Encryption not configured")
	}
	return crypter.GetEncryptionConfig(ctx, request)
}

func (s *BuildBuddyServer) GetAuditLogs(ctx context.Context, request *alpb.GetAuditLogsRequest) (*alpb.GetAuditLogsResponse, error) {
	al := s.env.GetAuditLogger()
	if al == nil {
		return nil, status.UnimplementedError("Audit logger not configured")
	}
	return al.GetLogs(ctx, request)
}

func (s *BuildBuddyServer) CreateRepo(ctx context.Context, request *repb.CreateRepoRequest) (*repb.CreateRepoResponse, error) {
	gh := s.env.GetGitHubApp()
	if gh == nil {
		return nil, status.UnimplementedError("Github service not configured")
	}
	return gh.CreateRepo(ctx, request)
}

func (s *BuildBuddyServer) GetGithubUserInstallations(ctx context.Context, req *ghpb.GetGithubUserInstallationsRequest) (*ghpb.GetGithubUserInstallationsResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubUserInstallations(ctx, req)
}

func (s *BuildBuddyServer) GetGithubUser(ctx context.Context, req *ghpb.GetGithubUserRequest) (*ghpb.GetGithubUserResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubUser(ctx, req)
}

func (s *BuildBuddyServer) GetGithubRepo(ctx context.Context, req *ghpb.GetGithubRepoRequest) (*ghpb.GetGithubRepoResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubRepo(ctx, req)
}

func (s *BuildBuddyServer) GetGithubContent(ctx context.Context, req *ghpb.GetGithubContentRequest) (*ghpb.GetGithubContentResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubContent(ctx, req)
}

func (s *BuildBuddyServer) GetGithubTree(ctx context.Context, req *ghpb.GetGithubTreeRequest) (*ghpb.GetGithubTreeResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubTree(ctx, req)
}

func (s *BuildBuddyServer) CreateGithubTree(ctx context.Context, req *ghpb.CreateGithubTreeRequest) (*ghpb.CreateGithubTreeResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.CreateGithubTree(ctx, req)
}

func (s *BuildBuddyServer) GetGithubBlob(ctx context.Context, req *ghpb.GetGithubBlobRequest) (*ghpb.GetGithubBlobResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubBlob(ctx, req)
}

func (s *BuildBuddyServer) CreateGithubBlob(ctx context.Context, req *ghpb.CreateGithubBlobRequest) (*ghpb.CreateGithubBlobResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.CreateGithubBlob(ctx, req)
}

func (s *BuildBuddyServer) CreateGithubPull(ctx context.Context, req *ghpb.CreateGithubPullRequest) (*ghpb.CreateGithubPullResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.CreateGithubPull(ctx, req)
}

func (s *BuildBuddyServer) MergeGithubPull(ctx context.Context, req *ghpb.MergeGithubPullRequest) (*ghpb.MergeGithubPullResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.MergeGithubPull(ctx, req)
}

func (s *BuildBuddyServer) GetGithubCompare(ctx context.Context, req *ghpb.GetGithubCompareRequest) (*ghpb.GetGithubCompareResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubCompare(ctx, req)
}

func (s *BuildBuddyServer) GetGithubForks(ctx context.Context, req *ghpb.GetGithubForksRequest) (*ghpb.GetGithubForksResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubForks(ctx, req)
}

func (s *BuildBuddyServer) CreateGithubFork(ctx context.Context, req *ghpb.CreateGithubForkRequest) (*ghpb.CreateGithubForkResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.CreateGithubFork(ctx, req)
}

func (s *BuildBuddyServer) GetGithubCommits(ctx context.Context, req *ghpb.GetGithubCommitsRequest) (*ghpb.GetGithubCommitsResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubCommits(ctx, req)
}

func (s *BuildBuddyServer) CreateGithubCommit(ctx context.Context, req *ghpb.CreateGithubCommitRequest) (*ghpb.CreateGithubCommitResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.CreateGithubCommit(ctx, req)
}

func (s *BuildBuddyServer) UpdateGithubRef(ctx context.Context, req *ghpb.UpdateGithubRefRequest) (*ghpb.UpdateGithubRefResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.UpdateGithubRef(ctx, req)
}

func (s *BuildBuddyServer) CreateGithubRef(ctx context.Context, req *ghpb.CreateGithubRefRequest) (*ghpb.CreateGithubRefResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.CreateGithubRef(ctx, req)
}

func (s *BuildBuddyServer) GetGithubPullRequest(ctx context.Context, req *ghpb.GetGithubPullRequestRequest) (*ghpb.GetGithubPullRequestResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubPullRequest(ctx, req)
}

func (s *BuildBuddyServer) GetGithubPullRequestDetails(ctx context.Context, req *ghpb.GetGithubPullRequestDetailsRequest) (*ghpb.GetGithubPullRequestDetailsResponse, error) {
	a := s.env.GetGitHubApp()
	if a == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return a.GetGithubPullRequestDetails(ctx, req)
}

func (s *BuildBuddyServer) SetIPRulesConfig(ctx context.Context, request *irpb.SetRulesConfigRequest) (*irpb.SetRulesConfigResponse, error) {
	irs := s.env.GetIPRulesService()
	if irs == nil {
		return nil, status.UnimplementedError("IP rules not enabled")
	}
	rsp, err := irs.SetIPRuleConfig(ctx, request)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		al.LogForGroup(ctx, request.GetRequestContext().GetGroupId(), alpb.Action_UPDATE_IP_RULES_CONFIG, request)
	}
	return rsp, err
}

func (s *BuildBuddyServer) GetIPRulesConfig(ctx context.Context, request *irpb.GetRulesConfigRequest) (*irpb.GetRulesConfigResponse, error) {
	irs := s.env.GetIPRulesService()
	if irs == nil {
		return nil, status.UnimplementedError("IP rules not enabled")
	}
	rsp, err := irs.GetIPRuleConfig(ctx, request)
	if err != nil {
		return nil, err
	}
	return rsp, err
}

func (s *BuildBuddyServer) GetIPRules(ctx context.Context, request *irpb.GetRulesRequest) (*irpb.GetRulesResponse, error) {
	irs := s.env.GetIPRulesService()
	if irs == nil {
		return nil, status.UnimplementedError("IP rules not enabled")
	}
	return irs.GetRules(ctx, request)
}

func (s *BuildBuddyServer) AddIPRule(ctx context.Context, request *irpb.AddRuleRequest) (*irpb.AddRuleResponse, error) {
	irs := s.env.GetIPRulesService()
	if irs == nil {
		return nil, status.UnimplementedError("IP rules not enabled")
	}
	rsp, err := irs.AddRule(ctx, request)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_IP_RULE,
			Id:   rsp.GetRule().GetIpRuleId(),
			Name: request.GetRule().GetDescription(),
		}
		al.Log(ctx, rid, alpb.Action_CREATE, request)
	}
	return rsp, nil
}

func (s *BuildBuddyServer) UpdateIPRule(ctx context.Context, request *irpb.UpdateRuleRequest) (*irpb.UpdateRuleResponse, error) {
	irs := s.env.GetIPRulesService()
	if irs == nil {
		return nil, status.UnimplementedError("IP rules not enabled")
	}
	r, err := irs.GetRule(ctx, request.GetRequestContext().GetGroupId(), request.GetRule().GetIpRuleId())
	if err != nil {
		return nil, err
	}
	rsp, err := irs.UpdateRule(ctx, request)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_IP_RULE,
			Id:   request.GetRule().GetIpRuleId(),
			Name: r.Description,
		}
		al.Log(ctx, rid, alpb.Action_UPDATE, request)
	}
	return rsp, nil
}

func (s *BuildBuddyServer) DeleteIPRule(ctx context.Context, request *irpb.DeleteRuleRequest) (*irpb.DeleteRuleResponse, error) {
	irs := s.env.GetIPRulesService()
	if irs == nil {
		return nil, status.UnimplementedError("IP rules not enabled")
	}
	r, err := irs.GetRule(ctx, request.GetRequestContext().GetGroupId(), request.GetIpRuleId())
	if err != nil {
		return nil, err
	}
	rsp, err := irs.DeleteRule(ctx, request)
	if err != nil {
		return nil, err
	}
	if al := s.env.GetAuditLogger(); al != nil {
		rid := &alpb.ResourceID{
			Type: alpb.ResourceType_IP_RULE,
			Id:   request.GetIpRuleId(),
			Name: r.Description,
		}
		al.Log(ctx, rid, alpb.Action_DELETE, request)
	}
	return rsp, nil
}

func (s *BuildBuddyServer) GetGCPProject(ctx context.Context, request *gcpb.GetGCPProjectRequest) (*gcpb.GetGCPProjectResponse, error) {
	gcpService := s.env.GetGCPService()
	if gcpService == nil {
		return nil, status.FailedPreconditionError("GCP service not enabled")
	}

	return gcpService.GetGCPProject(ctx, request)
}
