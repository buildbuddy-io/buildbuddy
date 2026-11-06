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

	"google.golang.org/grpc/metadata"

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

// RequireQuorumHeader requires that a quorum of replicas contain the required digest,
// before reporting a hit.
//
// If unset, a hit will reported as long as one replica contains the digest.
const RequireQuorumHeader = "x-buildbuddy-find-missing-require-quorum"

// WithQuorum sets the quorum header on an outgoing context.
func WithQuorum(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, RequireQuorumHeader, "true")
}

// RequiresQuorum reports whether the incoming context requires quorum checks.
func RequiresQuorum(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, RequireQuorumHeader)
	return len(values) > 0 && values[0] == "true"
}
