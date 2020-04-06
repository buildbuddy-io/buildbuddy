package interfaces

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	inpb "proto/invocation"
)

// An interface representing the user info gleaned from an authorization header.
type UserToken interface {
	GetIssuer() string
	GetSubscriber() string

	// Returns a fq string usable as an ID for this issuer + subscriber.
	GetSubID() string
}

type Authenticator interface {
	// Redirect to configured authentication provider.
	Login(w http.ResponseWriter, r *http.Request)
	// Clear any logout state.
	Logout(w http.ResponseWriter, r *http.Request)
	// Handle a callback from authentication provider.
	Auth(w http.ResponseWriter, r *http.Request)

	// Called by the authentication handler to authenticate a request. An error should
	// only be returned when unexpected behavior occurs. In all other cases, the
	// request context should be returned unchanged with no error set.
	AuthenticateRequest(w http.ResponseWriter, r *http.Request) context.Context

	// Returns the UserToken extracted from any authorization headers
	// present in the request. This does not guarantee the user has been
	// registered -- it only indicates they were authenticated using some
	// auth provider.
	//
	// To check if a user is registered, use UserDB.GetUser!
	GetUserToken(ctx context.Context) (UserToken, error)

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

	// Low level interface used for seeking and stream-writing.
	BlobReader(ctx context.Context, blobName string, offset, length int64) (io.Reader, error)
	BlobWriter(ctx context.Context, blobName string) (io.WriteCloser, error)
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
	// Stop garbage collection etc.r
	Stop() error
}

type InvocationDB interface {
	// Invocations API
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
	RawQueryInvocations(ctx context.Context, sql string, values ...interface{}) ([]*tables.Invocation, error)
}

type CacheDB interface {
	// Cache Entry API
	InsertOrUpdateCacheEntry(ctx context.Context, c *tables.CacheEntry) error
	IncrementEntryReadCount(ctx context.Context, key string) error
	DeleteCacheEntry(ctx context.Context, key string) error
	SumCacheEntrySizes(ctx context.Context) (int64, error)
	RawQueryCacheEntries(ctx context.Context, sql string, values ...interface{}) ([]*tables.CacheEntry, error)
}

type AuthDB interface {
	InsertOrUpdateUserToken(ctx context.Context, subID string, token *tables.Token) error
	ReadToken(ctx context.Context, subID string) (*tables.Token, error)
}

type UserDB interface {
	// User API
	InsertOrUpdateUser(ctx context.Context, u *tables.User) error
	// GetUser *may* do a database read and will return the registered
	// user's information, or nil if no user was found. It requires
	// that a valid authenticator is present in the environment and will
	// return a UserToken given the provided context.
	GetUser(ctx context.Context, userID *string) (*tables.User, error)
	DeleteUser(ctx context.Context, userID string) error
}

type GroupDB interface {
	// Groups API
	InsertOrUpdateGroup(ctx context.Context, g *tables.Group) error
	GetGroup(ctx context.Context, groupID string) (*tables.User, error)
	GetGroupByDomain(ctx context.Context, domain string) (*tables.User, error)
	DeleteGroup(ctx context.Context, groupID string) error
}

// A searcher allows finding various objects given a query.
type Searcher interface {
	IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error
	QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error)
}

// A webhook can be called when a build is completed.
type Webhook interface {
	NotifyComplete(ctx context.Context, invocation *inpb.Invocation) error
}
