package proxy_util

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/cdc"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/findmissing"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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
		findmissing.RequireQuorumHeader,
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

type skipRemoteAtimeUpdateContextKey struct{}

func SetSkipRemoteAtimeUpdate(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipRemoteAtimeUpdateContextKey{}, true)
}

func SkipRemoteAtimeUpdate(ctx context.Context) bool {
	v, _ := ctx.Value(skipRemoteAtimeUpdateContextKey{}).(bool)
	return v
}

func RequestTypeLabelFromContext(ctx context.Context) string {
	if SkipRemote(ctx) {
		return metrics.LocalOnlyCacheProxyRequestLabel
	}
	return metrics.DefaultCacheProxyRequestLabel
}

// NewHTTPForwarder returns a handler that reverse-proxies HTTP requests to
// target. Like the gRPC forwarder, it strips client-supplied internal headers
// and sets the resolved client IP, attested with a grpc-proxy client identity
// unless the caller already has its own.
func NewHTTPForwarder(env environment.Env, target *url.URL) http.Handler {
	cis := env.GetClientIdentityService()
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		for k := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), authutil.InternalHeaderPrefix) {
				delete(r.Header, k)
			}
		}
		if cis == nil {
			return
		}
		clientIP := clientip.Get(r.Context())
		if clientIP == "" {
			return
		}
		r.Header.Set(clientip.HeaderName, clientIP)
		if r.Header.Get(authutil.ClientIdentityHeaderName) != "" {
			return
		}
		header, err := cis.CachedIdentityHeader(&interfaces.ClientIdentity{
			Origin: interfaces.ClientIdentityInternalOrigin,
			Client: interfaces.ClientIdentityGRPCProxy,
		})
		if err != nil {
			log.CtxWarningf(r.Context(), "Could not create client identity header: %s", err)
			return
		}
		r.Header.Set(authutil.ClientIdentityHeaderName, header)
	}
	return interceptors.ClientIP(env, proxy)
}
