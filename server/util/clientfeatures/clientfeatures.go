// Package clientfeatures provides a simple way for clients to tell servers
// about features they support. This primarily exists to support behavior
// changes to clients that are deployed on-prem where we may need to change
// behaviors but only for clients that are known to have been upgraded to picup
// a feature.
package clientfeatures

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

const HeaderName = "x-buildbuddy-client-features"

const (
	// ByteStreamWriteAlreadyExists means the client's ByteStream.Write
	// handling treats a terminal ALREADY_EXISTS status as a successful early
	// completion of the upload, equivalent to a WriteResponse with
	// committed_size -1.
	ByteStreamWriteAlreadyExists = "bytestream-write-already-exists"
)

// Advertise returns a context whose outgoing gRPC metadata declares the given
// features. Features advertised by earlier calls on the same context are kept.
func Advertise(ctx context.Context, features ...string) context.Context {
	if len(features) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, HeaderName, strings.Join(features, ","))
}

// Supports reports whether the incoming request advertised the feature.
func Supports(ctx context.Context, feature string) bool {
	for _, value := range metadata.ValueFromIncomingContext(ctx, HeaderName) {
		for token := range strings.SplitSeq(value, ",") {
			token = strings.TrimSpace(token)
			if token != "" && token == feature {
				return true
			}
		}
	}
	return false
}
