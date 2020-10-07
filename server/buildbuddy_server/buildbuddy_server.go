package buildbuddy_server

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/bytestream"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bzpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_config"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
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

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	if req.GetLookup().GetInvocationId() == "" {
		return nil, status.InvalidArgumentErrorf("GetInvocationRequest must contain a valid invocation_id")
	}

	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.GetLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{
			inv,
		},
	}, nil
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

func makeGroups(grps []*tables.Group) []*grpb.Group {
	r := make([]*grpb.Group, 0)
	for _, g := range grps {
		r = append(r, &grpb.Group{
			Id:            g.GroupID,
			Name:          g.Name,
			OwnedDomain:   g.OwnedDomain,
			GithubLinked:  g.GithubToken != "",
			UrlIdentifier: g.URLIdentifier,
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
	group := &tables.Group{
		URLIdentifier: req.GetUrlIdentifier(),
	}
	if err := userDB.FillGroup(ctx, group); err != nil {
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
	userDB := s.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if req.GetRequestContext() == nil || req.GetRequestContext().getGroupID() == "" {
		return nil, status.InvalidArgumentError("Missing group ID in request context.")
	}
	users, err := userDB.GetGroupUsers(ctx, req.GetRequestContext().getGroupID(), req.GetMembershipStatus())
	if err != nil {
		return nil, err
	}
	displayUsers := make([]*uidpb.DisplayUser, 0)
	return &grpb.GetGroupUsersResponse{
		Users: displayUsers
	}, nil
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

	urlIdentifier := strings.TrimSpace(req.GetUrlIdentifier())

	group := &tables.Group{
		UserID:        user.UserID,
		Name:          groupName,
		URLIdentifier: urlIdentifier,
		OwnedDomain:   groupOwnedDomain,
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
	group := &tables.Group{
		GroupID: req.GetId(),
	}
	if err := userDB.FillGroup(ctx, group); err != nil {
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

func getEmailDomain(email string) string {
	chunks := strings.Split(email, "@")
	return chunks[len(chunks)-1]
}

func (s *BuildBuddyServer) UpdateGroup(ctx context.Context, req *grpb.UpdateGroupRequest) (*grpb.UpdateGroupResponse, error) {
	return nil, status.UnimplementedError("Not Implemented")
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

func (s *BuildBuddyServer) getGroupAPIKey(ctx context.Context) string {
	groupID := ""
	if reqCtx := requestcontext.ProtoRequestContextFromContext(ctx); reqCtx != nil {
		groupID = reqCtx.GetGroupId()
	}

	if userDB := s.env.GetUserDB(); userDB != nil {
		if tu, _ := userDB.GetUser(ctx); tu != nil {
			if groupID != "" {
				for _, g := range tu.Groups {
					if g.GroupID == groupID {
						return g.APIKey
					}
				}
				// If group ID was provided explicitly, it would be unexpected behavior
				// if we used an API key from another group, so return empty string.
				return ""
			}
			for _, g := range tu.Groups {
				if g.OwnedDomain != "" && g.APIKey != "" {
					return g.APIKey
				}
			}
			// Still here? This user might have a self-owned group, let's check for that.
			for _, g := range tu.Groups {
				if g.GroupID == strings.Replace(tu.UserID, "US", "GR", 1) && g.APIKey != "" {
					return g.APIKey
				}
			}
			// Finally, fall back to any group with a WriteToken. This will be the
			// default group for on-prem use cases.
			for _, g := range tu.Groups {
				if g.APIKey != "" {
					return g.APIKey
				}
			}
		}
	}
	return ""
}

func insertPassword(rawURL, password string) string {
	if password == "" {
		return rawURL
	}
	return strings.Replace(rawURL, "://", "://"+password+"@", 1)
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
	groupAPIKey := s.getGroupAPIKey(ctx)
	eventsAPIURL = insertPassword(eventsAPIURL, groupAPIKey)
	configOptions = append(configOptions, makeConfigOption("build", "bes_backend", eventsAPIURL))

	if s.env.GetCache() != nil {
		cacheAPIURL := s.env.GetConfigurator().GetAppCacheAPIURL()
		if cacheAPIURL == "" {
			cacheAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		cacheAPIURL = insertPassword(cacheAPIURL, groupAPIKey)
		configOptions = append(configOptions, makeConfigOption("build", "remote_cache", cacheAPIURL))
	}

	if s.env.GetConfigurator().GetRemoteExecutionConfig() != nil {
		remoteExecutionAPIURL := s.env.GetConfigurator().GetAppRemoteExecutionAPIURL()
		if remoteExecutionAPIURL == "" {
			remoteExecutionAPIURL = assembleURL(req.Host, "grpc:", grpcPort)
		}
		remoteExecutionAPIURL = insertPassword(remoteExecutionAPIURL, groupAPIKey)
		configOptions = append(configOptions, makeConfigOption("build", "remote_executor", remoteExecutionAPIURL))
	}

	cerificate := &bzpb.Certificate{}
	if req.GetIncludeCertificate() && s.sslService.IsCertGenerationEnabled() && groupAPIKey != "" {
		cert, key, err := s.sslService.GenerateCerts(groupAPIKey)
		if err != nil {
			return nil, fmt.Errorf("Error generating cert: %+v", err)
		}
		cerificate = &bzpb.Certificate{
			Cert: cert,
			Key:  key,
		}
	}

	return &bzpb.GetBazelConfigResponse{
		ConfigOption: configOptions,
		Certificate:  cerificate,
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

// Handle requests for build logs and artifacts by looking them up in from our
// cache servers using the bytestream API.
func (s *BuildBuddyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	lookup, err := parseByteStreamURL(params.Get("bytestream_url"), params.Get("filename"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	group, err := s.env.GetInvocationDB().LookupGroupFromInvocation(r.Context(), params.Get("invocation_id"))
	if err == nil && group != nil && lookup.URL.User == nil {
		lookup.URL.User = url.User(group.APIKey)
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
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
}
