package proxy_util

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/cdc"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"google.golang.org/grpc/metadata"
)

const (
	// If set to "true" in the context metadata, use the proxy's local cache as
	// the source of truth and skip remote reads and writes.
	// By default, the remote app cache is the source of truth.
	SkipRemoteKey = "proxy_skip_remote"
)

var (
	// The gRPC headers that should be forwarded by the Cache Proxy.
	HeadersToPropagate = []string{
		authutil.APIKeyHeader,
		authutil.ContextTokenStringKey,
		usageutil.ClientHeaderName,
		usageutil.OriginHeaderName,
		bazel_request.RequestMetadataKey,
		cdc.ChunkedHeaderName,
		cdc.SpliceWithoutValidationHeaderName,
		// Forward the client's opt-in for local AC caching so it survives
		// proxy-to-proxy hops and every proxy in the chain honors it.
		platform.OverrideHeaderPrefix + platform.CacheProxyActionCacheTTLPropertyName,
	}
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
