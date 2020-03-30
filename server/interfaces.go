package interfaces

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	inpb "proto/invocation"
)

// An interface representing a registered User object.
type User interface {
	ID() string
	Name() string
	Groups() []string
}

// An interface representing the user info gleaned from an authorization header.
type UserToken interface {
	GetIssuer() string
	GetSubscriberID() string
}

type Authenticator interface {
	// Called by the authentication handler to authenticate a request. An error should
	// only be returned when unexpected behavior occurs. In all other cases, the
	// request context should be returned unchanged with no error set.
	AuthenticateRequest(w http.ResponseWriter, r *http.Request) (context.Context, error)

	// Returns the UserToken extracted from any authorization headers
	// present in the requst. This does not guarantee the user has been
	// registered -- it only indicates they were authenticating using some
	// auth provider.
	//
	// To check if a user is registered, use GetUser!
	GetUserToken(ctx context.Context) UserToken

	// GetUser *may* do a database read and will return the registered
	// user's information, or nil of no user was found.
	GetUser(ctx context.Context) User
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

// A Database must allow for various object modification operations.
type Database interface {
	// INVOCATIONS API
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error

	// CACHE ENTRY API
	InsertOrUpdateCacheEntry(ctx context.Context, c *tables.CacheEntry) error
	IncrementEntryReadCount(ctx context.Context, key string) error
	DeleteCacheEntry(ctx context.Context, key string) error
	SumCacheEntrySizes(ctx context.Context) (int64, error)
	RawQueryCacheEntries(ctx context.Context, sql string, values ...interface{}) ([]*tables.CacheEntry, error)
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
