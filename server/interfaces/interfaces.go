package interfaces

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
)

// An interface representing the user info gleaned from an authorization header.
type UserToken interface {
	GetIssuer() string
	GetSubscriber() string

	// Returns a fq string usable as an ID for this issuer + subscriber.
	GetSubID() string
}

// An interface representing the user info gleaned from http basic auth, which is
// often set for GRPC requests.
type BasicAuthToken interface {
	GetUser() string
	GetPassword() string
}

type UserInfo interface {
	GetUserID() string
	GetGroupID() string
	GetAllowedGroups() []string
	IsAdmin() bool
	HasCapability(akpb.ApiKey_Capability) bool
	GetUseGroupOwnedExecutors() bool
}

// Authenticator constants
const (
	AuthContextUserErrorKey = "auth.error"
)

type Authenticator interface {
	// Redirect to configured authentication provider.
	Login(w http.ResponseWriter, r *http.Request)
	// Clear any logout state.
	Logout(w http.ResponseWriter, r *http.Request)
	// Handle a callback from authentication provider.
	Auth(w http.ResponseWriter, r *http.Request)

	// Called by the authentication handler to authenticate a request. If a
	// user was authenticated, a new context will be returned that contains
	// a UserToken.
	AuthenticateHTTPRequest(w http.ResponseWriter, r *http.Request) context.Context

	// Called by the authentication handler to authenticate a request. If a
	// user was authenticated, a new context will be returned that contains
	// a BasicAuthToken.
	AuthenticateGRPCRequest(ctx context.Context) context.Context

	// FillUser may be used to construct an initial tables.User object. It
	// is filled based on information from the authenticator's JWT.
	FillUser(ctx context.Context, user *tables.User) error

	// Returns the UserInfo extracted from any authorization headers
	// present in the request.
	AuthenticatedUser(ctx context.Context) (UserInfo, error)

	// Parses and returns a BuildBuddy API key from the given string.
	ParseAPIKeyFromString(string) string

	// Returns a context containing the given API key.
	AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context
}

type BuildEventChannel interface {
	MarkInvocationDisconnected(ctx context.Context, iid string) error
	FinalizeInvocation(iid string) error
	HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error
}

type BuildEventHandler interface {
	OpenChannel(ctx context.Context, iid string) BuildEventChannel
}

// A Blobstore must allow for reading, writing, and deleting blobs.
// Implementations should return "os"-compatible package type errors, for
// example, if a file does not exist on Read, the blobstore should return an
// "os.ErrNotExist" error.
type Blobstore interface {
	BlobExists(ctx context.Context, blobName string) (bool, error)
	ReadBlob(ctx context.Context, blobName string) ([]byte, error)
	WriteBlob(ctx context.Context, blobName string, data []byte) (int, error)
	DeleteBlob(ctx context.Context, blobName string) error
}

// Similar to a blobstore, a cache allows for reading and writing data, but
// additionally it is responsible for deleting data that is past TTL to keep to
// a manageable size.
// Similar to the Cache above, a digest cache allows for more intelligent
// storing of blob data based on its size.
type Cache interface {
	// Returns a new Cache that will store everything under the prefix
	// specified by "prefix". The prefix specified is concatenated onto the
	// currently set prefix -- so this is a relative operation, not an
	// absolute one.
	WithPrefix(prefix string) Cache

	// Normal cache-like operations.
	Contains(ctx context.Context, d *repb.Digest) (bool, error)
	ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error)
	Get(ctx context.Context, d *repb.Digest) ([]byte, error)
	GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error)
	Set(ctx context.Context, d *repb.Digest, data []byte) error
	SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error
	Delete(ctx context.Context, d *repb.Digest) error

	// Low level interface used for seeking and stream-writing.
	Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error)
	Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error)
}

type InvocationDB interface {
	// Invocations API
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) error
	UpdateInvocationACL(ctx context.Context, authenticatedUser *UserInfo, invocationID string, acl *aclpb.ACL) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
	DeleteInvocationWithPermsCheck(ctx context.Context, authenticatedUser *UserInfo, invocationID string) error
	FillCounts(ctx context.Context, log *telpb.TelemetryStat) error
}

type APIKeyGroup interface {
	GetCapabilities() int32
	GetGroupID() string
	GetUseGroupOwnedExecutors() bool
}

type AuthDB interface {
	InsertOrUpdateUserToken(ctx context.Context, subID string, token *tables.Token) error
	ReadToken(ctx context.Context, subID string) (*tables.Token, error)
	GetAPIKeyGroupFromAPIKey(apiKey string) (APIKeyGroup, error)
	GetAPIKeyGroupFromBasicAuth(login, pass string) (APIKeyGroup, error)
}

type UserDB interface {
	// User API
	InsertUser(ctx context.Context, u *tables.User) error
	// GetUser will return the registered user's information or
	// an error if no registered user was found. It requires that a
	// valid authenticator is present in the environment and will return
	// a UserToken given the provided context.
	GetUser(ctx context.Context) (*tables.User, error)
	DeleteUser(ctx context.Context, userID string) error
	FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error

	// Creates the DEFAULT group, for on-prem usage where there is only
	// one group and all users are implicitly a part of it.
	CreateDefaultGroup(ctx context.Context) error

	// Groups API
	InsertOrUpdateGroup(ctx context.Context, g *tables.Group) (string, error)
	GetGroupByID(ctx context.Context, groupID string) (*tables.Group, error)
	GetGroupByURLIdentifier(ctx context.Context, urlIdentifier string) (*tables.Group, error)
	GetAuthGroup(ctx context.Context) (*tables.Group, error)
	DeleteGroup(ctx context.Context, groupID string) error
	AddUserToGroup(ctx context.Context, userID string, groupID string) error
	RequestToJoinGroup(ctx context.Context, userID string, groupID string) error
	GetGroupUsers(ctx context.Context, groupID string, statuses []grpb.GroupMembershipStatus) ([]*grpb.GetGroupUsersResponse_GroupUser, error)
	UpdateGroupUsers(ctx context.Context, groupID string, updates []*grpb.UpdateGroupUsersRequest_Update) error

	// API Keys API
	GetAPIKey(ctx context.Context, apiKeyID string) (*tables.APIKey, error)
	GetAPIKeys(ctx context.Context, groupID string) ([]*tables.APIKey, error)
	CreateAPIKey(ctx context.Context, groupID string, label string, capabilities []akpb.ApiKey_Capability) (*tables.APIKey, error)
	UpdateAPIKey(ctx context.Context, key *tables.APIKey) error
	DeleteAPIKey(ctx context.Context, apiKeyID string) error
}

// A webhook can be called when a build is completed.
type Webhook interface {
	NotifyComplete(ctx context.Context, invocation *inpb.Invocation) error
}

// Allows aggregating invocation statistics.
type InvocationStatService interface {
	GetInvocationStat(ctx context.Context, req *inpb.GetInvocationStatRequest) (*inpb.GetInvocationStatResponse, error)
}

// Allows searching invocations.
type InvocationSearchService interface {
	IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error
	QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error)
}

type ApiService interface {
	apipb.ApiServiceServer
	http.Handler
}

type WorkflowService interface {
	CreateWorkflow(ctx context.Context, req *wfpb.CreateWorkflowRequest) (*wfpb.CreateWorkflowResponse, error)
	DeleteWorkflow(ctx context.Context, req *wfpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error)
	GetWorkflows(ctx context.Context, req *wfpb.GetWorkflowsRequest) (*wfpb.GetWorkflowsResponse, error)
	GetRepos(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error)
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

type SplashPrinter interface {
	PrintSplashScreen(port, grpcPort int)
}

type RemoteExecutionService interface {
	Dispatch(ctx context.Context, req *repb.ExecuteRequest) (string, error)
	Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error
	WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error
	PublishOperation(stream repb.Execution_PublishOperationServer) error
}

type FileCache interface {
	FastLinkFile(d *repb.Digest, outputPath string) bool
	AddFile(d *repb.Digest, existingFilePath string)
}

type SchedulerService interface {
	RegisterNode(stream scpb.Scheduler_RegisterNodeServer) error
	RegisterAndStreamWork(stream scpb.Scheduler_RegisterAndStreamWorkServer) error
	LeaseTask(stream scpb.Scheduler_LeaseTaskServer) error
	ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error)
	EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error)
	ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest) (*scpb.ReEnqueueTaskResponse, error)
	GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error)
}

type ExecutionService interface {
	GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error)
}

// CommandContainer provides an execution environment for commands.
type CommandContainer interface {
	// Run the given command within the container.
	Run(ctx context.Context, command *repb.Command, workingDir string) *CommandResult
}

// CommandResult captures the output and details of an executed command.
type CommandResult struct {
	// Error is populated only if the command was unable to be started, or if it was
	// started but never completed.
	//
	// In particular, if the command runs and returns a non-zero exit code (such as 1),
	// this is considered a successful execution, and this error will NOT be populated.
	//
	// In some cases, the command may have failed to start due to an issue unrelated
	// to the command itself. For example, the runner may execute the command in a
	// sandboxed environment but fail to create the sandbox. In these cases, the
	// Error field here should be populated with a gRPC error code indicating why the
	// command failed to start, and the ExitCode field should contain the exit code
	// from the sandboxing process, rather than the command itself.
	//
	// If the call to `exec.Cmd#Run` returned -1, meaning that the command was killed or
	// never exited, this field should be populated with a gRPC error code indicating the
	// reason, such as DEADLINE_EXCEEDED (if the command times out), UNAVAILABLE (if
	// there is a transient error that can be retried), or RESOURCE_EXHAUSTED (if the
	// command ran out of memory while executing).
	Error error
	// CommandDebugString indicates the command that was run, for debugging purposes only.
	CommandDebugString string
	// Stdout from the command. This may contain data even if there was an Error.
	Stdout []byte
	// Stderr from the command. This may contain data even if there was an Error.
	Stderr []byte

	// ExitCode is one of the following:
	// * The exit code returned by the executed command
	// * -1 if the process was killed or did not exit
	// * -2 (NoExitCode) if the exit code could not be determined because it returned
	//   an error other than exec.ExitError. This case typically means it failed to start.
	ExitCode int
}

type Subscriber interface {
	Close() error
	Chan() <-chan string
}

// A PubSub allows for sending messages between distributed (cross process,
// cross machine) processes. This may be implemented by a cloud-pubsub service,
// or something like redis.
type PubSub interface {
	Publish(ctx context.Context, channelName string, message string) error
	Subscribe(ctx context.Context, channelName string) Subscriber
}

// A MetricsCollector allows for storing ephemeral values globally.
//
// No guarantees are made about durability of MetricsCollectors -- they may be
// evicted from the backing store that maintains them (usually memcache or
// redis), so they should *not* be used in critical path code.
type MetricsCollector interface {
	IncrementCount(ctx context.Context, counterName string, n int64) (int64, error)
	ReadCount(ctx context.Context, counterName string) (int64, error)
}

// A RepoDownloader allows testing a git-repo to see if it's downloadable.
type RepoDownloader interface {
	TestRepoAccess(ctx context.Context, repoURL, username, accessToken string) error
}

type Checker interface {
	// Returns nil on success, error on failure. Returning an error will
	// indicate to the health checker that this service is unhealthy and
	// that the server is not ready to serve.
	Check(ctx context.Context) error
}
type CheckerFunc func(ctx context.Context) error

func (f CheckerFunc) Check(ctx context.Context) error {
	return f(ctx)
}

type HealthChecker interface {
	// Adds a healthcheck -- the server's readiness is dependend on all
	// registered heathchecks passing.
	AddHealthCheck(name string, hc Checker)

	// RegisterShutdownFunction registers a function that will be called
	// when the server shuts down. This can be used for finalizing any
	// short work and freeing up resources. A CheckerFunc may block
	// shutdown for up to ~30 seconds or so, at which point the server
	// will terminate un-gracefully.
	RegisterShutdownFunction(hc CheckerFunc)

	// WaitForGracefulShutdown should be called as the last thing in a
	// main function -- it will block forever until a server recieves a
	// shutdown signal.
	WaitForGracefulShutdown()

	// LivenessHandler returns "OK" as soon as the server is alive.
	LivenessHandler() http.Handler

	// ReadinessHandler returns "OK" when the server is ready to serve.
	// If a HealthCheck returns failure for some reason, the server will
	// stop returning OK and will instead return Service Unavailable error.
	ReadinessHandler() http.Handler
}
