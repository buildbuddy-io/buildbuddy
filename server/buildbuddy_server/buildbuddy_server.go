package buildbuddy_server

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/bytestream"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/remote_exec_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/scorecard"
	"github.com/buildbuddy-io/buildbuddy/server/role_filter"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/target"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bzpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_config"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	qpb "github.com/buildbuddy-io/buildbuddy/proto/quota"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

var (
	disableCertConfig = flag.Bool("app.disable_cert_config", false, "If true, the certificate based auth option will not be shown in the config widget.")
)

const (
	bytestreamProtocolPrefix  = "bytestream://"
	actioncacheProtocolPrefix = "actioncache://"
)

type BuildBuddyServer struct {
	env        environment.Env
	sslService interfaces.SSLService
}

func Register(env environment.Env) error {
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

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}

	return &inpb.GetInvocationResponse{Invocation: []*inpb.Invocation{inv}}, nil
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
			Id:                     g.GroupID,
			Name:                   g.Name,
			OwnedDomain:            g.OwnedDomain,
			GithubLinked:           githubToken != "",
			UrlIdentifier:          urlIdentifier,
			SharingEnabled:         g.SharingEnabled,
			UseGroupOwnedExecutors: g.UseGroupOwnedExecutors != nil && *g.UseGroupOwnedExecutors,
			SuggestionPreference:   g.SuggestionPreference,
		})
	}
	return r
}

func (s *BuildBuddyServer) GetUser(ctx context.Context, req *uspb.GetUserRequest) (*uspb.GetUserResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	return s.getUser(ctx, req, userDB.GetUser)
}

func (s *BuildBuddyServer) GetImpersonatedUser(ctx context.Context, req *uspb.GetUserRequest) (*uspb.GetUserResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	return s.getUser(ctx, req, userDB.GetImpersonatedUser)
}

type userLookup func(ctx context.Context) (*tables.User, error)

func (s *BuildBuddyServer) getUser(ctx context.Context, req *uspb.GetUserRequest, dbLookup userLookup) (*uspb.GetUserResponse, error) {
	tu, err := dbLookup(ctx)
	if err != nil {
		return nil, err
	}
	if tu == nil {
		// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
		return nil, status.UnauthenticatedError("User not found")
	}

	selectedGroupID := ""
	selectedGroupRole := role.None
	if g := selectedGroup(req.GetRequestContext().GetGroupId(), tu.Groups); g != nil {
		selectedGroupID = g.Group.GroupID
		selectedGroupRole = role.Role(g.Role)
	}
	allowedRPCs := role_filter.RoleIndependentRPCs
	if selectedGroupRole&role.Admin > 0 {
		allowedRPCs = append(allowedRPCs, role_filter.GroupAdminOnlyRPCs...)
	}
	if selectedGroupRole&(role.Admin|role.Developer) > 0 {
		allowedRPCs = append(allowedRPCs, role_filter.GroupDeveloperRPCs...)
	}
	if serverAdminGID := s.env.GetAuthenticator().AdminGroupID(); serverAdminGID != "" {
		for _, gr := range tu.Groups {
			if gr.Group.GroupID == serverAdminGID && gr.Role == uint32(role.Admin) {
				allowedRPCs = append(allowedRPCs, role_filter.ServerAdminOnlyRPCs...)
				break
			}
		}
	}
	return &uspb.GetUserResponse{
		DisplayUser:     tu.ToProto(),
		UserGroup:       makeGroups(tu.Groups),
		SelectedGroupId: selectedGroupID,
		AllowedRpc:      allowedRPCs,
		GithubToken:     tu.GithubToken,
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
	urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())
	if urlIdentifier == "" {
		return nil, status.InvalidArgumentError("URL identifier is required.")
	}
	group, err := userDB.GetGroupByURLIdentifier(ctx, urlIdentifier)
	if err != nil {
		return nil, err
	}
	return &grpb.GetGroupResponse{
		Id: group.GroupID,
		// NOTE: this RPC does not require authentication, so sensitive group
		// info should not be exposed here.
		Name:        group.Name,
		OwnedDomain: group.OwnedDomain,
		SsoEnabled:  group.SamlIdpMetadataUrl != nil && *group.SamlIdpMetadataUrl != "",
	}, nil
}

func (s *BuildBuddyServer) GetGroupUsers(ctx context.Context, req *grpb.GetGroupUsersRequest) (*grpb.GetGroupUsersResponse, error) {
	if err := perms.AuthorizeGroupAccess(ctx, s.env, req.GetGroupId()); err != nil {
		return nil, err
	}
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
	if err := perms.AuthorizeGroupAccess(ctx, s.env, req.GetGroupId()); err != nil {
		return nil, err
	}
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if err := userDB.UpdateGroupUsers(ctx, req.GetGroupId(), req.GetUpdate()); err != nil {
		return nil, err
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
		UserID:                 user.UserID,
		Name:                   groupName,
		OwnedDomain:            groupOwnedDomain,
		SharingEnabled:         req.GetSharingEnabled(),
		UseGroupOwnedExecutors: &useGroupOwnedExecutors,
	}
	urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())

	if urlIdentifier != "" {
		if existingGroup, err := userDB.GetGroupByURLIdentifier(ctx, urlIdentifier); existingGroup != nil {
			return nil, status.InvalidArgumentError("URL is already in use")
		} else if gstatus.Code(err) != gcodes.NotFound {
			return nil, err
		}
		group.URLIdentifier = &urlIdentifier
	}
	group.SuggestionPreference = grpb.SuggestionPreference_ENABLED

	groupID, err := userDB.InsertOrUpdateGroup(ctx, group)
	if err != nil {
		return nil, err
	}

	if err := userDB.AddUserToGroup(ctx, user.UserID, groupID); err != nil {
		return nil, err
	}
	return &grpb.CreateGroupResponse{
		Id: groupID,
	}, nil
}

func (s *BuildBuddyServer) UpdateGroup(ctx context.Context, req *grpb.UpdateGroupRequest) (*grpb.UpdateGroupResponse, error) {
	auth := s.env.GetAuthenticator()
	userDB := s.env.GetUserDB()
	if auth == nil || userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if err := perms.AuthorizeGroupAccess(ctx, s.env, req.GetId()); err != nil {
		return nil, err
	}
	var group *tables.Group
	var err error
	urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())

	if urlIdentifier != "" {
		if group, err = userDB.GetGroupByURLIdentifier(ctx, urlIdentifier); group != nil && group.GroupID != req.GetId() {
			return nil, status.InvalidArgumentError("URL is already in use")
		} else if err != nil && gstatus.Code(err) != gcodes.NotFound {
			return nil, err
		}
	}
	if group == nil {
		if req.GetId() == "" {
			return nil, status.InvalidArgumentError("Missing organization identifier.")
		}
		group, err = userDB.GetGroupByID(ctx, req.GetId())
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
	group.UseGroupOwnedExecutors = &useGroupOwnedExecutors
	group.SuggestionPreference = req.GetSuggestionPreference()
	if group.SuggestionPreference == grpb.SuggestionPreference_UNKNOWN_SUGGESTION_PREFERENCE {
		group.SuggestionPreference = grpb.SuggestionPreference_ENABLED
	}
	if _, err := userDB.InsertOrUpdateGroup(ctx, group); err != nil {
		return nil, err
	}
	return &grpb.UpdateGroupResponse{}, nil
}

func (s *BuildBuddyServer) JoinGroup(ctx context.Context, req *grpb.JoinGroupRequest) (*grpb.JoinGroupResponse, error) {
	auth := s.env.GetAuthenticator()
	userDB := s.env.GetUserDB()
	if auth == nil || userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	user, err := userDB.GetUser(ctx)
	if err != nil {
		return nil, err
	}
	group, err := userDB.GetGroupByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	// If the user's email matches the group's owned domain, they can be added
	// as a member immediately.
	if group.OwnedDomain != "" && group.OwnedDomain == getEmailDomain(user.Email) {
		if err := userDB.AddUserToGroup(ctx, user.UserID, req.GetId()); err != nil {
			return nil, err
		}
	} else {
		// Otherwise submit a request to join the group.
		if err := userDB.RequestToJoinGroup(ctx, user.UserID, req.GetId()); err != nil {
			return nil, err
		}
	}

	return &grpb.JoinGroupResponse{}, nil
}

func (s *BuildBuddyServer) GetApiKeys(ctx context.Context, req *akpb.GetApiKeysRequest) (*akpb.GetApiKeysResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	groupID := req.GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}
	tableKeys, err := userDB.GetAPIKeys(ctx, groupID, true /*checkVisibility*/)
	if err != nil {
		return nil, err
	}
	rsp := &akpb.GetApiKeysResponse{
		ApiKey: make([]*akpb.ApiKey, 0, len(tableKeys)),
	}
	for _, k := range tableKeys {
		rsp.ApiKey = append(rsp.ApiKey, &akpb.ApiKey{
			Id:                  k.APIKeyID,
			Value:               k.Value,
			Label:               k.Label,
			Capability:          capabilities.FromInt(k.Capabilities),
			VisibleToDevelopers: k.VisibleToDevelopers,
		})
	}
	return rsp, nil
}

func (s *BuildBuddyServer) CreateApiKey(ctx context.Context, req *akpb.CreateApiKeyRequest) (*akpb.CreateApiKeyResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	groupID := req.GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}
	k, err := userDB.CreateAPIKey(ctx, groupID, req.GetLabel(), req.GetCapability(), req.GetVisibleToDevelopers())
	if err != nil {
		return nil, err
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

func (s *BuildBuddyServer) authorizeAPIKeyWrite(ctx context.Context, apiKeyID string) error {
	if apiKeyID == "" {
		return status.InvalidArgumentError("API key ID is required")
	}
	user, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return err
	}
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return status.UnimplementedError("Not Implemented")
	}
	// Check that the user belongs to the group that owns the requested API key.
	key, err := userDB.GetAPIKey(ctx, apiKeyID)
	if err != nil {
		return err
	}
	acl := perms.ToACLProto( /* userID= */ nil, key.GroupID, key.Perms)
	return perms.AuthorizeWrite(&user, acl)
}

func (s *BuildBuddyServer) UpdateApiKey(ctx context.Context, req *akpb.UpdateApiKeyRequest) (*akpb.UpdateApiKeyResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if err := s.authorizeAPIKeyWrite(ctx, req.GetId()); err != nil {
		return nil, err
	}
	tk := &tables.APIKey{
		APIKeyID:            req.GetId(),
		Label:               req.GetLabel(),
		Capabilities:        capabilities.ToInt(req.GetCapability()),
		VisibleToDevelopers: req.GetVisibleToDevelopers(),
	}
	if err := userDB.UpdateAPIKey(ctx, tk); err != nil {
		return nil, err
	}
	return &akpb.UpdateApiKeyResponse{}, nil
}

func (s *BuildBuddyServer) DeleteApiKey(ctx context.Context, req *akpb.DeleteApiKeyRequest) (*akpb.DeleteApiKeyResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if err := s.authorizeAPIKeyWrite(ctx, req.GetId()); err != nil {
		return nil, err
	}
	if err := userDB.DeleteAPIKey(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &akpb.DeleteApiKeyResponse{}, nil
}

func selectedGroup(preferredGroupID string, groupRoles []*tables.GroupRole) *tables.GroupRole {
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
	authenticatedUser, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	for _, allowedGroupID := range authenticatedUser.GetAllowedGroups() {
		if allowedGroupID == groupID {
			tableKeys, err := userDB.GetAPIKeys(ctx, groupID, true /*checkVisibility*/)
			if err != nil {
				return nil, err
			}
			return toProtoAPIKeys(tableKeys), nil
		}
	}
	return nil, status.InternalError("Could not find the requested group ID. This should never happen.")
}

func (s *BuildBuddyServer) GetBazelConfig(ctx context.Context, req *bzpb.GetBazelConfigRequest) (*bzpb.GetBazelConfigResponse, error) {
	configOptions := make([]*bzpb.ConfigOption, 0)

	// Use config urls if they're set and fall back to host & protocol from request if not.
	resultsURL := build_buddy_url.WithPath("/invocation/").String()
	if build_buddy_url.String() == "" {
		resultsURL = assembleURL(req.Host, req.Protocol, "")
		resultsURL += "/invocation/"
	}
	configOptions = append(configOptions, makeConfigOption("build", "bes_results_url", resultsURL))

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

	configOptions = append(configOptions, makeConfigOption("build", "bes_backend", eventsAPIURL))

	if s.env.GetCache() != nil {
		cacheAPIURL := cache_api_url.String()
		if cacheAPIURL == "" {
			cacheAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		configOptions = append(configOptions, makeConfigOption("build", "remote_cache", cacheAPIURL))
	}

	if remote_execution_config.RemoteExecutionEnabled() {
		remoteExecutionAPIURL := remote_exec_api_url.String()
		if remoteExecutionAPIURL == "" {
			remoteExecutionAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		configOptions = append(configOptions, makeConfigOption("build", "remote_executor", remoteExecutionAPIURL))
	}

	credentials := make([]*bzpb.Credentials, len(groupAPIKeys))
	for i, apiKey := range groupAPIKeys {
		credentials[i] = &bzpb.Credentials{
			ApiKey: apiKey,
		}
	}

	if req.GetIncludeCertificate() && s.sslService.IsCertGenerationEnabled() && !*disableCertConfig {
		for _, c := range credentials {
			cert, key, err := s.sslService.GenerateCerts(c.ApiKey.Value)
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

func (s *BuildBuddyServer) GetTrend(ctx context.Context, req *inpb.GetTrendRequest) (*inpb.GetTrendResponse, error) {
	if iss := s.env.GetInvocationStatService(); iss != nil {
		return iss.GetTrend(ctx, req)
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

func (s *BuildBuddyServer) GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error) {
	if ss := s.env.GetSchedulerService(); ss != nil {
		res, err := ss.GetExecutionNodes(ctx, req)
		return res, err
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetTarget(ctx context.Context, req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
	return target.GetTarget(ctx, s.env, req)
}

func (s *BuildBuddyServer) GetEventLogChunk(ctx context.Context, req *elpb.GetEventLogChunkRequest) (*elpb.GetEventLogChunkResponse, error) {
	return eventlog.GetEventLogChunk(ctx, s.env, req)
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
		return wfs.GetWorkflows(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}
func (s *BuildBuddyServer) ExecuteWorkflow(ctx context.Context, req *wfpb.ExecuteWorkflowRequest) (*wfpb.ExecuteWorkflowResponse, error) {
	if wfs := s.env.GetWorkflowService(); wfs != nil {
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

func (s *BuildBuddyServer) GetCacheMetadata(ctx context.Context, req *capb.GetCacheMetadataRequest) (*capb.GetCacheMetadataResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	resourceName := req.GetResourceName()
	cacheType, err := ProtoCacheTypeToCacheType(resourceName.GetCacheType())
	if err != nil {
		return nil, err
	}
	cache, err := s.env.GetCache().WithIsolation(ctx, cacheType, resourceName.GetInstanceName())
	if err != nil {
		return nil, err
	}

	metadata, err := cache.Metadata(ctx, resourceName.GetDigest())
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}

	return &capb.GetCacheMetadataResponse{
		SizeBytes:      metadata.SizeBytes,
		LastAccessUsec: metadata.LastAccessTimeUsec,
		LastModifyUsec: metadata.LastModifyTimeUsec,
	}, nil
}

func ProtoCacheTypeToCacheType(cacheType resource.CacheType) (resource.CacheType, error) {
	switch cacheType {
	case resource.CacheType_AC:
		return resource.CacheType_AC, nil
	case resource.CacheType_CAS:
		return resource.CacheType_CAS, nil
	default:
		return resource.CacheType_UNKNOWN_CACHE_TYPE, status.InvalidArgumentErrorf("unknown cache type %v", cacheType)
	}
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
	in, err := s.env.GetInvocationDB().LookupInvocation(ctx, invocationID)
	if err != nil {
		return nil, err
	}
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	apiKeys, err := userDB.GetAPIKeys(ctx, in.GroupID, false /*checkVisibility*/)
	if err != nil {
		return nil, err
	}
	if len(apiKeys) == 0 {
		return nil, status.NotFoundError("The group that owns this invocation doesn't have any API keys configured.")
	}
	return apiKeys[0], nil
}

// Handle requests for build logs and artifacts by looking them up in from our
// cache servers using the bytestream API.
func (s *BuildBuddyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	lookup, err := parseByteStreamURL(params.Get("bytestream_url"), params.Get("filename"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if lookup.URL.User == nil {
		apiKey, _ := s.getAnyAPIKeyForInvocation(r.Context(), params.Get("invocation_id"))
		if apiKey != nil {
			lookup.URL.User = url.User(apiKey.Value)
		}
	}

	// Stream the file back to our client
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", lookup.Filename))
	w.Header().Set("Content-Type", "application/octet-stream")

	// TODO(siggisim): Figure out why this JWT is overriding authority auth and remove.
	ctx := context.WithValue(r.Context(), "x-buildbuddy-jwt", nil)

	err = bytestream.StreamBytestreamFile(ctx, s.env, lookup.URL, func(data []byte) {
		w.Write(data)
	})

	if err != nil {
		log.Warningf("Error downloading file: %s", err.Error())
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
}
