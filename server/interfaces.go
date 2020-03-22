package interfaces

import (
	"context"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	inpb "proto/invocation"
)

// A Blobstore must allow for reading, writing, and deleting blobs.
// Implementations should "os" package type errors, for example, if a file does
// not exist on Read, the blobstore should return an os.ErrNotExist error.
type Blobstore interface {
	BlobExists(ctx context.Context, blobName string) (bool, error)
	ReadBlob(ctx context.Context, blobName string) ([]byte, error)
	WriteBlob(ctx context.Context, blobName string, data []byte) error
	DeleteBlob(ctx context.Context, blobName string) error

	// Low level interface used for seeking and stream-writing.
	BlobReader(ctx context.Context, blobName string, offset, length int64) (io.Reader, error)
	BlobWriter(ctx context.Context, blobName string) (io.WriteCloser, error)
}

// A Database must allow for various object update operations.
// TODO(tylerw): These probably need to take a tables.Invocation instead.
// That's the only way to include stored data (like blob ID) in the database.
type Database interface {
	InsertOrUpdateInvocation(ctx context.Context, in *tables.Invocation) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
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
