package interfaces

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
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
}

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
	Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error)
	Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error)

	// Begin garbage collection and any other necessary background tasks.
	Start() error
	// Stop garbage collection etc.
	Stop() error
}

type InvocationDB interface {
	// Invocations API
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
	FillCounts(ctx context.Context, log *telpb.TelemetryStat) error
}

type AuthDB interface {
	InsertOrUpdateUserToken(ctx context.Context, subID string, token *tables.Token) error
	ReadToken(ctx context.Context, subID string) (*tables.Token, error)
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
	InsertOrUpdateGroup(ctx context.Context, g *tables.Group) error
	GetAuthGroup(ctx context.Context) (*tables.Group, error)
	DeleteGroup(ctx context.Context, groupID string) error
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

type SplashPrinter interface {
	PrintSplashScreen(port, grpcPort int)
}

type ExecutionService interface {
	Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error
	SyncExecute(ctx context.Context, req *repb.ExecuteRequest) (*repb.SyncExecuteResponse, error)
	WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error
	PublishOperation(stream repb.Execution_PublishOperationServer) error
}

type ExecutionRouterService interface {
	GetExecutionClient(ctx context.Context, req *repb.ExecuteRequest) (ExecutionClientConfig, error)
}

type ExecutionClientConfig interface {
	GetExecutionClient() repb.ExecutionClient
	DisableStreaming() bool
}

type FileCache interface {
	FastLinkFile(d *repb.Digest, outputPath string) bool
	AddFile(d *repb.Digest, existingFilePath string)
}

type SchedulerService interface {
	RegisterNode(stream scpb.Scheduler_RegisterNodeServer) error
	GetTask(ctx context.Context, req *scpb.GetTaskRequest) (*scpb.GetTaskResponse, error)
}
