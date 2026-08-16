// Package findmissing carries the "purpose" of a FindMissing call on the
// context.
//
// FindMissing is called from many code paths (atime updates, write dedupe,
// Contains checks, chunk validation, ...) and cache implementations attribute
// present/absent metrics to the originating path. Rather than threading the
// purpose through the interfaces.Cache.FindMissing signature, callers stamp it
// on the context with ContextWithPurpose and implementations read it back with
// PurposeFromContext.
package findmissing

import (
	"context"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type purposeContextKey struct{}

// ContextWithPurpose returns a context carrying the given FindMissing purpose.
// Cache implementations retrieve it via PurposeFromContext.
func ContextWithPurpose(ctx context.Context, purpose repb.FindMissingBlobsRequest_Purpose) context.Context {
	return context.WithValue(ctx, purposeContextKey{}, purpose)
}

// PurposeFromContext returns the FindMissing purpose stamped on the context, or
// UNKNOWN if none was set.
func PurposeFromContext(ctx context.Context) repb.FindMissingBlobsRequest_Purpose {
	if p, ok := ctx.Value(purposeContextKey{}).(repb.FindMissingBlobsRequest_Purpose); ok {
		return p
	}
	return repb.FindMissingBlobsRequest_UNKNOWN
}
