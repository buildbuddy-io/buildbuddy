package interfaces

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"google.golang.org/genproto/googleapis/longrunning"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

	// Returns the UserToken extracted from any authorization headers
	// present in the request. This does not guarantee the user has been
	// registered -- it only indicates they were authenticated using some
	// auth provider.
	//
	// To check if a user is registered, use UserDB!
	GetUserToken(ctx context.Context) (UserToken, error)

	// Returns the APIKey extracted from any authorization headers
	// present in the request. This does not guarantee the api key is
	// valid -- it only indicates that an API key was present in the
	// request
	//
	// To check if the key is valid, use UserDB.GetAPIKeyAuthGroup!
	GetAPIKey(ctx context.Context) (string, error)

	// Returns the BasicAuthToken extracted from any user/password set on
	// the RPC request. This does not guarantee the user has been
	// registered -- it only indicates they were authenticated using the
	// BASIC auth provider.
	//
	// To check if a user or group registered, use UserDB!
	GetBasicAuthToken(ctx context.Context) (BasicAuthToken, error)

	// FillUser may be used to construct an initial tables.User object. It
	// is filled based on information from the authenticator's JWT.
	FillUser(ctx context.Context, user *tables.User) error
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
type Cache interface {
	// Normal cache-like operations.
	Contains(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, data []byte) error
	Delete(ctx context.Context, key string) error

	// Low level interface used for seeking and stream-writing.
	Reader(ctx context.Context, key string, offset, length int64) (io.Reader, error)
	Writer(ctx context.Context, key string) (io.WriteCloser, error)

	// Begin garbage collection and any other necessary background tasks.
	Start() error
	// Stop garbage collection etc.
	Stop() error
}

type InvocationDB interface {
	// Invocations API
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
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

	// Creates the DEFAULT group, for on-prem usage where there is only
	// one group and all users are implicitly a part of it.
	CreateDefaultGroup(ctx context.Context) error

	// Groups API
	InsertOrUpdateGroup(ctx context.Context, g *tables.Group) error
	GetBasicAuthGroup(ctx context.Context) (*tables.Group, error)
	GetAPIKeyAuthGroup(ctx context.Context) (*tables.Group, error)
	DeleteGroup(ctx context.Context, groupID string) error
}

type ExecutionDB interface {
	InsertOrUpdateExecution(ctx context.Context, executionID string, stage repb.ExecutionStage_Value, op *longrunning.Operation) error
	ReadExecution(ctx context.Context, executionID string) (*tables.Execution, error)
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

type ExecutionClientConfig interface {
	GetExecutionClient() repb.ExecutionClient
	GetMaxDuration() time.Duration
}
