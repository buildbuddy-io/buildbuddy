package interfaces

import (
	"context"
	"time"

	"github.com/tryflame/buildbuddy/server/tables"
	inpb "proto/invocation"
)

// A Blobstore must allow for reading, writing, and deleting blobs.
type Blobstore interface {
	ReadBlob(ctx context.Context, blobName string) ([]byte, error)
	WriteBlob(ctx context.Context, blobName string, data []byte) error
	DeleteBlob(ctx context.Context, blobName string) error
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
	FindInvocations(ctx context.Context, query *tables.Invocation) ([]*inpb.Invocation, error)
}
