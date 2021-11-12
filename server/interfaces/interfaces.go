package interfaces

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
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

// GroupMembership represents a user's membership within a group as well as
// their role within that group.
type GroupMembership struct {
	GroupID string    `json:"group_id"`
	Role    role.Role `json:"role"`
}

type UserInfo interface {
	GetUserID() string
	GetGroupID() string
	// GetAllowedGroups returns the IDs of the groups of which the user is a
	// member.
	// DEPRECATED: Use GetGroupMemberships instead.
	GetAllowedGroups() []string
	// GetGroupMemberships returns the user's group memberships.
	GetGroupMemberships() []*GroupMembership
	// IsAdmin returns whether this user is a global administrator, meaning
	// they can access data across groups. This is not to be confused with the
	// concept of group admin, which grants full access only within a single
	// group.
	IsAdmin() bool
	HasCapability(akpb.ApiKey_Capability) bool
	GetUseGroupOwnedExecutors() bool
}

// Authenticator constants
const (
	AuthContextUserErrorKey = "auth.error"

	// AuthAnonymousUser is the identifier for unauthenticated users in installations that allow anonymous users.
	AuthAnonymousUser = "ANON"
)

type Authenticator interface {
	// Redirect to configured authentication provider.
	Login(w http.ResponseWriter, r *http.Request)
	// Clear any logout state.
	Logout(w http.ResponseWriter, r *http.Request)
	// Handle a callback from authentication provider.
	Auth(w http.ResponseWriter, r *http.Request)

	// AuthenticatedHTTPContext authenticates the user using the credentials present in the HTTP request and creates a
	// child context that contains the results.
	//
	// This function is called automatically for every HTTP request via a filter and the new context is passed to
	// application code.
	//
	// Application code can retrieve the stored information by calling AuthenticatedUser.
	AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context

	// AuthenticatedGRPCContext authenticates the user using the credentials present in the gRPC metadata and creates a
	// child context that contains the result.
	//
	// This function is called automatically for every gRPC request via a filter and the new context is passed to
	// application code.
	//
	// Application code that retrieve the stored information by calling AuthenticatedUser.
	AuthenticatedGRPCContext(ctx context.Context) context.Context

	// AuthenticateGRPCRequest authenticates the user using the credentials present in the gRPC metadata and returns the
	// result.
	//
	// You should only use this function if you need fresh information (for example to re-validate credentials during a
	// long running operation). For all other cases it is better to use the information cached in the context
	// retrieved via AuthenticatedUser.
	AuthenticateGRPCRequest(ctx context.Context) (UserInfo, error)

	// FillUser may be used to construct an initial tables.User object. It
	// is filled based on information from the authenticator's JWT.
	FillUser(ctx context.Context, user *tables.User) error

	// AuthenticatedUser returns the UserInfo stored in the context.
	//
	// See AuthenticatedHTTPContext/AuthenticatedGRPCContext for a description of how the context is created.
	AuthenticatedUser(ctx context.Context) (UserInfo, error)

	// Parses and returns a BuildBuddy API key from the given string.
	ParseAPIKeyFromString(string) string

	// Returns a context containing the given API key.
	AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context

	// TrustedJWTFromAuthContext returns a JWT from the authenticated context,
	// or empty string if the context is not authenticated.
	TrustedJWTFromAuthContext(ctx context.Context) string

	// AuthContextFromTrustedJWT returns an authenticated context using a JWT
	// which has been previously authenticated.
	AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context
}

type BuildEventChannel interface {
	FinalizeInvocation(iid string) error
	HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error
	Close()
}

type BuildEventHandler interface {
	OpenChannel(ctx context.Context, iid string) BuildEventChannel
}

// A Blobstore must allow for reading, writing, and deleting blobs.
type Blobstore interface {
	BlobExists(ctx context.Context, blobName string) (bool, error)
	ReadBlob(ctx context.Context, blobName string) ([]byte, error)
	WriteBlob(ctx context.Context, blobName string, data []byte) (int, error)

	// DeleteBlob does not return an error if the blob does not exist; some
	// blobstores do not distinguish on return between deleting an existing blob
	// and calling delete on a non-existent blob, so this is the only way to
	// provide a consistent interface.
	DeleteBlob(ctx context.Context, blobName string) error
}

type CacheType int

const (
	UnknownCacheType CacheType = iota
	ActionCacheType
	CASCacheType
)

func (t CacheType) Prefix() string {
	switch t {
	case ActionCacheType:
		return "ac"
	case CASCacheType:
		return ""
	default:
		alert.UnexpectedEvent("unknown_cache_type", "type: %v", t)
		return "unknown"
	}
}

// Similar to a blobstore, a cache allows for reading and writing data, but
// additionally it is responsible for deleting data that is past TTL to keep to
// a manageable size.
// Similar to the Cache above, a digest cache allows for more intelligent
// storing of blob data based on its size.
type Cache interface {
	// WithIsolation returns a cache accessor that guarantees that data for a given cacheType and
	// remoteInstanceCombination is isolated from any other cacheType and remoteInstanceName combination.
	WithIsolation(ctx context.Context, cacheType CacheType, remoteInstanceName string) (Cache, error)

	// Normal cache-like operations.
	Contains(ctx context.Context, d *repb.Digest) (bool, error)
	ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error)
	FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error)
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
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) (bool, error)
	UpdateInvocationACL(ctx context.Context, authenticatedUser *UserInfo, invocationID string, acl *aclpb.ACL) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
	DeleteInvocationWithPermsCheck(ctx context.Context, authenticatedUser *UserInfo, invocationID string) error
	FillCounts(ctx context.Context, log *telpb.TelemetryStat) error
	SetNowFunc(now func() time.Time)
}

type APIKeyGroup interface {
	GetCapabilities() int32
	GetGroupID() string
	GetUseGroupOwnedExecutors() bool
}

type AuthDB interface {
	InsertOrUpdateUserToken(ctx context.Context, subID string, token *tables.Token) error
	ReadToken(ctx context.Context, subID string) (*tables.Token, error)
	GetAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (APIKeyGroup, error)
	GetAPIKeyGroupFromBasicAuth(ctx context.Context, login, pass string) (APIKeyGroup, error)
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
	GetTrend(ctx context.Context, req *inpb.GetTrendRequest) (*inpb.GetTrendResponse, error)
}

// Allows searching invocations.
type InvocationSearchService interface {
	IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error
	QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error)
}

type UsageService interface {
	GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error)
}

type UsageTracker interface {
	// Increment adds the given usage counts to the current collection period
	// for the authenticated group ID. It is safe for concurrent access.
	Increment(ctx context.Context, counts *tables.UsageCounts) error
}

type ApiService interface {
	apipb.ApiServiceServer
	http.Handler
}

type WorkflowService interface {
	CreateWorkflow(ctx context.Context, req *wfpb.CreateWorkflowRequest) (*wfpb.CreateWorkflowResponse, error)
	DeleteWorkflow(ctx context.Context, req *wfpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error)
	GetWorkflows(ctx context.Context, req *wfpb.GetWorkflowsRequest) (*wfpb.GetWorkflowsResponse, error)
	ExecuteWorkflow(ctx context.Context, req *wfpb.ExecuteWorkflowRequest) (*wfpb.ExecuteWorkflowResponse, error)
	GetRepos(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error)
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

type RunnerService interface {
	Run(ctx context.Context, req *rnpb.RunRequest) (*rnpb.RunResponse, error)
}

type GitProviders []GitProvider

type GitProvider interface {
	// MatchRepoURL returns whether a given repo URL should be handled by this
	// provider. If multiple providers match, the first one in the GitProviders
	// list is chosen to handle the repo URL.
	MatchRepoURL(u *url.URL) bool

	// MatchWebhookRequest returns whether a given webhook request was sent by
	// this git provider. If multiple providers match, the first one in the
	// GitProviders list is chosen to handle the webhook.
	MatchWebhookRequest(req *http.Request) bool

	// ParseWebhookData parses webhook data from the given HTTP request sent to
	// a webhook endpoint. It should only be called if MatchWebhookRequest returns
	// true.
	ParseWebhookData(req *http.Request) (*WebhookData, error)

	// RegisterWebhook registers the given webhook URL to listen for push and
	// pull request (also called "merge request") events.
	RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error)

	// UnregisterWebhook unregisters the webhook with the given ID from the repo.
	UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error

	// GetFileContents fetches a single file's contents from the repo. It returns
	// status.NotFoundError if the file does not exist.
	GetFileContents(ctx context.Context, accessToken, repoURL, filePath, ref string) ([]byte, error)

	// TODO(bduffany): CreateStatus, ListRepos
}

// WebhookData represents the data extracted from a Webhook event.
type WebhookData struct {
	// EventName is the canonical event name that this data was created from.
	EventName string

	// PushedRepoURL is the canonical URL of the repo containing the pushed branch.
	// For pull request events from forked repos, this is the URL of the forked repo.
	// For other events, this is the same as the TargetRepoURL.
	// Ex: "https://github.com/some-untrusted-user/acme-fork"
	PushedRepoURL string

	// PushedBranch is the name of the branch in the pushed repo that triggered
	// the event when pushed.
	// Ex: "my-cool-feature"
	PushedBranch string

	// SHA is the commit SHA of the branch that was pushed.
	SHA string

	// TargetRepoURL is the canonical URL of the repo containing the TargetBranch.
	// This should always match the canonicalized URL of the repo linked to the
	// workflow.
	// Ex: "https://github.com/acme-inc/acme"
	TargetRepoURL string

	// TargetBranch is the branch associated with the event that determines whether
	// actions should be triggered. For push events this is the branch that was
	// pushed to. For pull_request events this is the base branch into which the PR
	// branch is being merged.
	// Ex: "main"
	TargetBranch string

	// IsTrusted returns whether the committed code came from a trusted actor.
	// For example, this will be true for members of the organization that owns
	// the repo, and false for forked repositories sending pull requests to the
	// repo.
	IsTrusted bool
}

type SplashPrinter interface {
	PrintSplashScreen(port, grpcPort int)
}

type RemoteExecutionService interface {
	Dispatch(ctx context.Context, req *repb.ExecuteRequest) (string, error)
	Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error
	WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error
	PublishOperation(stream repb.Execution_PublishOperationServer) error
	MarkExecutionFailed(ctx context.Context, taskID string, reason error) error
}

type FileCache interface {
	FastLinkFile(f *repb.FileNode, outputPath string) bool
	AddFile(f *repb.FileNode, existingFilePath string)
	WaitForDirectoryScanToComplete()
}

type SchedulerService interface {
	RegisterAndStreamWork(stream scpb.Scheduler_RegisterAndStreamWorkServer) error
	LeaseTask(stream scpb.Scheduler_LeaseTaskServer) error
	ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error)
	EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error)
	ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest) (*scpb.ReEnqueueTaskResponse, error)
	GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error)
	GetGroupIDAndDefaultPoolForUser(ctx context.Context, os string) (string, string, error)
}

type ExecutionService interface {
	GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error)
}

type ExecutionNode interface {
	// GetExecutorID returns the ID for this execution node that uniquely identifies
	// it within a node pool.
	GetExecutorID() string
}

// TaskRouter decides which execution nodes should execute a task.
//
// Routing is namespaced by group ID (extracted from context) and remote instance
// name. Tasks with different namespaces are not guaranteed to be routed the same.
//
// It is the caller's responsibility to check whether any execution nodes
// passed via parameters are accessible by the authenticated group in the context.
type TaskRouter interface {
	// RankNodes returns a slice of the given nodes sorted in decreasing order of
	// their suitability for executing the given command. Nodes with equal
	// suitability are returned in random order (for load balancing purposes).
	//
	// If an error occurs, the input nodes should be returned in random order.
	RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []ExecutionNode) []ExecutionNode

	// MarkComplete notifies the router that the command has been completed by the
	// given executor instance. Subsequent calls to RankNodes may assign a higher
	// rank to nodes with the given instance ID, given similar commands.
	MarkComplete(ctx context.Context, cmd *repb.Command, remoteInstanceName, executorInstanceID string)
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
	IncrementCount(ctx context.Context, key, field string, n int64) error
	ReadCounts(ctx context.Context, key string) (map[string]int64, error)
	Delete(ctx context.Context, key string) error
}

// A KeyValStore allows for storing ephemeral values globally.
//
// No guarantees are made about durability of KeyValStores -- they may be
// evicted from the backing store that maintains them (usually memcache or
// redis), so they should *not* be used in critical path code.
type KeyValStore interface {
	Set(ctx context.Context, key string, val []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
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
	// AddHealthCheck adds a healthcheck -- the server's readiness is dependent on all
	// registered heathchecks passing.
	AddHealthCheck(name string, hc Checker)

	// RegisterShutdownFunction registers a function that will be called
	// when the server shuts down. This can be used for finalizing any
	// short work and freeing up resources. A CheckerFunc may block
	// shutdown for up to ~30 seconds or so, at which point the server
	// will terminate un-gracefully.
	RegisterShutdownFunction(hc CheckerFunc)

	// WaitForGracefulShutdown should be called as the last thing in a
	// main function -- it will block forever until a server receives a
	// shutdown signal.
	WaitForGracefulShutdown()

	// LivenessHandler returns "OK" as soon as the server is alive.
	LivenessHandler() http.Handler

	// ReadinessHandler returns "OK" when the server is ready to serve.
	// If a HealthCheck returns failure for some reason, the server will
	// stop returning OK and will instead return Service Unavailable error.
	ReadinessHandler() http.Handler

	// Shutdown initiates a shutdown of the server.
	// This is intended to be used by tests as normally shutdown is automatically initiated upon receipt of a SIGTERM
	// signal.
	Shutdown()
}

// Locates all XCode versions installed on the host system.
type XcodeLocator interface {
	// Returns the developer directory and SDKs for the given XCode version.
	DeveloperDirForVersion(version string) (string, error)

	// Returns true if the given SDK path is present in the given XCode version.
	IsSDKPathPresentForVersion(sdkPath, version string) bool
}

// LRU implements a Least Recently Used cache.
type LRU interface {
	// Inserts a value into the LRU. A boolean is returned that indicates
	// if the value was successfully added.
	Add(key, value interface{}) bool

	// Gets a value from the LRU, returns a boolean indicating if the value
	// was present.
	Get(key interface{}) (interface{}, bool)

	// Returns a boolean indicating if the value is present in the LRU.
	Contains(key interface{}) bool

	// Removes a value from the LRU, releasing resources associated with
	// that value. Returns a boolean indicating if the value was sucessfully
	// removed.
	Remove(key interface{}) bool

	// Purge Remove()s all items in the LRU.
	Purge()

	// Returns the total "size" of the LRU.
	Size() int64

	// Remove()s the oldest value in the LRU. (See Remove() above).
	RemoveOldest() (interface{}, bool)

	// Returns metrics about the status of the LRU.
	Metrics() string
}

// DistributedLock provides a way to serialize access to a resource, where the
// accessors may be running on different nodes.
//
// Implementations should expire the lock after an appropriate length of time to
// ensure that the lock will eventually be released even if this node goes down.
type DistributedLock interface {
	// Lock attempts to acquire the lock.
	//
	// Implementations may return ResourceExhaustedError if and only if the
	// lock is already held.
	Lock(ctx context.Context) error

	// Unlock releases the lock.
	Unlock(ctx context.Context) error
}
