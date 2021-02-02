package buildbuddy_server

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/bytestream"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/target"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/workflow"
	"github.com/golang/protobuf/proto"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bzpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_config"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	bytestreamProtocolPrefix = "bytestream://"
)

type BuildBuddyServer struct {
	env        environment.Env
	sslService *ssl.SSLService
}

func NewBuildBuddyServer(env environment.Env, sslService *ssl.SSLService) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		env:        env,
		sslService: sslService,
	}, nil
}

func (s *BuildBuddyServer) getConfiguredAPIKey() string {
	if apiConfig := s.env.GetConfigurator().GetAPIConfig(); apiConfig != nil {
		return apiConfig.APIKey
	}
	return ""
}

func (s *BuildBuddyServer) redactAPIKeys(ctx context.Context, rsp *inpb.GetInvocationResponse) error {
	proto.DiscardUnknown(rsp)
	txt := proto.MarshalTextString(rsp)

	// NB: this implementation depends on the way we generate API keys
	// (20 alphanumeric characters).

	// Replace x-buildbuddy-api-key header.
	pat := regexp.MustCompile("x-buildbuddy-api-key=[[:alnum:]]{20}")
	txt = pat.ReplaceAllLiteralString(txt, "x-buildbuddy-api-key=<REDACTED>")

	// Replace sequences that look like API keys immediately followed by '@',
	// to account for patterns like "grpc://$API_KEY@app.buildbuddy.io"
	// or "bes_backend=$API_KEY@domain.com".

	// Here we match 20 alphanum chars occurring at the start of a line.
	pat = regexp.MustCompile("^[[:alnum:]]{20}@")
	txt = pat.ReplaceAllLiteralString(txt, "<REDACTED>@")
	// Here we match 20 alphanum chars anywhere in the line, preceded by a non-
	// alphanum char (to ensure the match is exactly 20 alphanum chars long).
	pat = regexp.MustCompile("([^[:alnum:]])[[:alnum:]]{20}@")
	txt = pat.ReplaceAllString(txt, "$1<REDACTED>@")

	// Replace the literal API key in the configuration, which does not need
	// to conform to the way we generate API keys.
	configuredKey := s.getConfiguredAPIKey()
	if configuredKey != "" {
		txt = strings.ReplaceAll(txt, configuredKey, "<REDACTED>")
	}

	return proto.UnmarshalText(txt, rsp)
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	if req.GetLookup().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("GetInvocationRequest must contain a valid invocation_id")
	}

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}

	rsp := &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{
			inv,
		},
	}
	if err := s.redactAPIKeys(ctx, rsp); err != nil {
		return nil, err
	}
	return rsp, nil
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

func makeGroups(grps []*tables.Group) []*grpb.Group {
	r := make([]*grpb.Group, 0)
	for _, g := range grps {
		urlIdentifier := ""
		if g.URLIdentifier != nil {
			urlIdentifier = *g.URLIdentifier
		}
		r = append(r, &grpb.Group{
			Id:             g.GroupID,
			Name:           g.Name,
			OwnedDomain:    g.OwnedDomain,
			GithubLinked:   g.GithubToken != "",
			UrlIdentifier:  urlIdentifier,
			SharingEnabled: g.SharingEnabled,
		})
	}
	return r
}

func (s *BuildBuddyServer) GetUser(ctx context.Context, req *uspb.GetUserRequest) (*uspb.GetUserResponse, error) {
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	tu, err := userDB.GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu == nil {
		return nil, status.UnauthenticatedError("User not found")
	}
	return &uspb.GetUserResponse{
		DisplayUser: tu.ToProto(),
		UserGroup:   makeGroups(tu.Groups),
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

	group := &tables.Group{
		UserID:         user.UserID,
		Name:           groupName,
		OwnedDomain:    groupOwnedDomain,
		SharingEnabled: req.GetSharingEnabled(),
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
	tableKeys, err := userDB.GetAPIKeys(ctx, groupID)
	if err != nil {
		return nil, err
	}
	rsp := &akpb.GetApiKeysResponse{
		ApiKey: make([]*akpb.ApiKey, 0, len(tableKeys)),
	}
	for _, k := range tableKeys {
		rsp.ApiKey = append(rsp.ApiKey, &akpb.ApiKey{
			Id:    k.APIKeyID,
			Value: k.Value,
			Label: k.Label,
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
	k, err := userDB.CreateAPIKey(ctx, groupID, req.GetLabel())
	if err != nil {
		return nil, err
	}
	return &akpb.CreateApiKeyResponse{
		ApiKey: &akpb.ApiKey{
			Id:    k.APIKeyID,
			Value: k.Value,
			Label: k.Label,
		},
	}, nil
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
		APIKeyID: req.GetId(),
		Label:    req.GetLabel(),
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
			Id:    k.APIKeyID,
			Value: k.Value,
			Label: k.Label,
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
			tableKeys, err := userDB.GetAPIKeys(ctx, groupID)
			if err != nil {
				return nil, err
			}
			return toProtoAPIKeys(tableKeys), nil
		}
	}
	return nil, status.InternalError("Could not find the requested group ID. This should never happen.")
}

func getIntFlag(flagName string, defaultVal string) string {
	f := flag.Lookup(flagName)
	if f == nil {
		return defaultVal
	}
	return f.Value.String()
}

func (s *BuildBuddyServer) GetBazelConfig(ctx context.Context, req *bzpb.GetBazelConfigRequest) (*bzpb.GetBazelConfigResponse, error) {
	configOptions := make([]*bzpb.ConfigOption, 0)

	// Use config urls if they're set and fall back to host & protocol from request if not.
	resultsURL := s.env.GetConfigurator().GetAppBuildBuddyURL()
	if resultsURL == "" {
		resultsURL = assembleURL(req.Host, req.Protocol, "")
	}
	configOptions = append(configOptions, makeConfigOption("build", "bes_results_url", resultsURL+"/invocation/"))

	grpcPort := getIntFlag("grpc_port", "1985")
	eventsAPIURL := s.env.GetConfigurator().GetAppEventsAPIURL()
	if eventsAPIURL == "" {
		eventsAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
	}
	groupAPIKeys, err := s.getAPIKeysForAuthorizedGroup(ctx)
	if err != nil {
		return nil, err
	}

	configOptions = append(configOptions, makeConfigOption("build", "bes_backend", eventsAPIURL))

	if s.env.GetCache() != nil {
		cacheAPIURL := s.env.GetConfigurator().GetAppCacheAPIURL()
		if cacheAPIURL == "" {
			cacheAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		configOptions = append(configOptions, makeConfigOption("build", "remote_cache", cacheAPIURL))
	}

	if s.env.GetConfigurator().GetRemoteExecutionConfig() != nil {
		remoteExecutionAPIURL := s.env.GetConfigurator().GetAppRemoteExecutionAPIURL()
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

	if req.GetIncludeCertificate() && s.sslService.IsCertGenerationEnabled() {
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

func (s *BuildBuddyServer) GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error) {
	if es := s.env.GetExecutionService(); es != nil {
		return es.GetExecution(ctx, req)
	}
	return nil, status.UnimplementedError("Not implemented")
}

func (s *BuildBuddyServer) GetTarget(ctx context.Context, req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
	return target.GetTarget(ctx, s.env, req)
}

func (s *BuildBuddyServer) CreateWorkflow(ctx context.Context, req *trpb.CreateWorkflowRequest) (*wfpb.CreateWorkflowResponse, error) {
	return workflow.CreateWorkflow(ctx, s.env, req)
}
func (s *BuildBuddyServer) DeleteWorkflow(ctx context.Context, req *trpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error) {
	return workflow.DeleteWorkflow(ctx, s.env, req)
}
func (s *BuildBuddyServer) GetWorkflows(ctx context.Context, req *trpb.GetWorkflowsRequest) (*wfpb.GetWorkflowsResponse, error) {
	return workflow.GetWorkflows(ctx, s.env, req)
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
	if strings.HasPrefix(bsURL, bytestreamProtocolPrefix) {
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
	apiKeys, err := userDB.GetAPIKeys(ctx, in.GroupID)
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
		log.Printf("Error downloading file: %s", err.Error())
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
}
