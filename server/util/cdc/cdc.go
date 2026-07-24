// Package cdc provides shared constants and helpers for content-defined
// chunking (CDC). It is intentionally kept small so lightweight packages
// (e.g. proxy_util) can reference the CDC header name without pulling in the
// full chunking package and its flag registrations.
package cdc

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"google.golang.org/grpc/metadata"
)

const (
	SpliceWithoutValidationExperiment = "splice-without-validation"

	// ChunkedHeaderName is the gRPC header attached to RPCs whose digests
	// or payloads are individual content-defined chunks, not whole blobs.
	ChunkedHeaderName = "build.bazel.remote.execution.v2.chunked"

	// ChunkedHeaderValue names the chunking function used to produce the chunk.
	ChunkedHeaderValue = "FAST_CDC_2020"

	// The internal prefix makes the server strip this header from callers that
	// do not present a trusted BuildBuddy client identity.
	SpliceWithoutValidationHeaderName = authutil.InternalHeaderPrefix + "splice-without-validation"
)

// IsChunked reports whether the incoming RPC context carries a non-empty
// chunked header, meaning the request refers to individual content-defined chunks.
func IsChunked(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, ChunkedHeaderName)
	return len(values) > 0 && values[0] != ""
}

// ContextWithChunked returns ctx with the chunked header added to the outgoing
// gRPC metadata.
func ContextWithChunked(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, ChunkedHeaderName, ChunkedHeaderValue)
}

func IsSpliceWithoutValidation(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, SpliceWithoutValidationHeaderName)
	return len(values) == 1 && values[0] == "true"
}

func ContextWithSpliceWithoutValidation(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, SpliceWithoutValidationHeaderName, "true")
}
