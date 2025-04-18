package proxy_util

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"google.golang.org/grpc/metadata"
)

const (
	// If set to "true" in the context metadata, use the proxy's local cache as
	// the source of truth and skip remote reads and writes.
	// By default, the remote app cache is the source of truth.
	SkipRemoteKey = "proxy_skip_remote"
)

func SkipRemote(ctx context.Context) bool {
	md := metadata.ValueFromIncomingContext(ctx, SkipRemoteKey)
	return len(md) > 0 && md[0] == "true"
}

func SetSkipRemote(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, SkipRemoteKey, "true")
}

func RequestTypeLabelFromContext(ctx context.Context) string {
	if SkipRemote(ctx) {
		return metrics.LocalOnlyCacheProxyRequestLabel
	}
	return metrics.DefaultCacheProxyRequestLabel
}
