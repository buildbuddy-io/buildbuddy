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
// target, preserving the incoming Host header (which the target uses to resolve
// the subdomain).
//
// Like the gRPC forwarder, it attests the client's IP to the target: it strips
// all client-supplied internal and identity headers, then sets the resolved
// client IP and a grpc-proxy client identity, which the target requires before
// trusting the IP. If env has no client identity service, nothing is attested.
// If the caller already carries a client identity, it is forwarded unchanged
// and the client IP is not attested.
func NewHTTPForwarder(env environment.Env, target *url.URL) http.Handler {
	cis := env.GetClientIdentityService()
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Host = r.In.Host
			// Append to the incoming X-Forwarded-For, as the default director
			// does; Rewrite otherwise drops it.
			r.Out.Header["X-Forwarded-For"] = r.In.Header["X-Forwarded-For"]
			r.SetXForwarded()
			// Keep the proto and host the original client used (e.g. https
			// when TLS is terminated in front of this proxy); SetXForwarded
			// would derive them from this proxy's own request.
			for _, h := range []string{"X-Forwarded-Proto", "X-Forwarded-Host"} {
				if v, ok := r.In.Header[h]; ok {
					r.Out.Header[h] = v
				}
			}

			for k := range r.Out.Header {
				if strings.HasPrefix(strings.ToLower(k), authutil.InternalHeaderPrefix) {
					delete(r.Out.Header, k)
				}
			}
			// A caller carrying its own identity (e.g. an executor or workflow)
			// keeps it, as in the gRPC forwarder: the target authorizes on it,
			// and replacing it with grpc-proxy would lose that.
			if cis == nil || r.In.Header.Get(authutil.ClientIdentityHeaderName) != "" {
				return
			}
			clientIP := clientip.Get(r.In.Context())
			if clientIP == "" {
				return
			}
			header, err := cis.CachedIdentityHeader(&interfaces.ClientIdentity{
				Origin: interfaces.ClientIdentityInternalOrigin,
				Client: interfaces.ClientIdentityGRPCProxy,
			})
			if err != nil {
				// Without the identity the target ignores the IP anyway.
				log.CtxWarningf(r.In.Context(), "Could not create client identity header: %s", err)
				return
			}
			r.Out.Header.Set(clientip.HeaderName, clientIP)
			r.Out.Header.Set(authutil.ClientIdentityHeaderName, header)
		},
	}
	return interceptors.ClientIP(env, proxy)
}
