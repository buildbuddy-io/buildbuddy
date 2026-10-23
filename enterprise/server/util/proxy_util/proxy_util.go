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

// attestHTTPForwardingFlag enables the attested HTTP forwarder.
const attestHTTPForwardingFlag = "cache_proxy.attest_http_forwarding"

// NewHTTPForwarder returns a handler that reverse-proxies HTTP requests to
// target, keeping the incoming Host.
//
// Behavior depends on the cache_proxy.attest_http_forwarding experiment:
//
//   - Off (the default, and today's behavior): requests are forwarded
//     unchanged by httputil.NewSingleHostReverseProxy, which passes all client
//     headers through and appends the peer to X-Forwarded-For. Nothing is
//     attested. Requests the "on" path would treat differently are counted in
//     metrics.CacheProxyHTTPForwardingDiffs.
//   - On: the proxy builds each outgoing request itself, like the gRPC
//     forwarder. It rejects a request with an invalid client identity (403),
//     drops client-supplied internal and Forwarded headers, sets
//     X-Forwarded-For/-Proto/-Host itself, and attests the resolved client IP
//     with a grpc-proxy identity, unless the caller has a valid identity of
//     its own.
func NewHTTPForwarder(env environment.Env, target *url.URL) http.Handler {
	efp := env.GetExperimentFlagProvider()
	cis := env.GetClientIdentityService()
	legacy := httputil.NewSingleHostReverseProxy(target)
	attested := interceptors.ClientIP(env, &httputil.ReverseProxy{Rewrite: attestingRewrite(cis, target)})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identityErr := validateCallerIdentity(cis, r)
		if efp == nil || !efp.Boolean(r.Context(), attestHTTPForwardingFlag, false) {
			countForwardingDiffs(r, identityErr)
			legacy.ServeHTTP(w, r)
			return
		}
		if identityErr != nil {
			log.CtxInfof(r.Context(), "Rejecting forwarded HTTP request with invalid client identity: %s", identityErr)
			http.Error(w, "invalid client identity", http.StatusForbidden)
			return
		}
		attested.ServeHTTP(w, r)
	})
}

func attestingRewrite(cis interfaces.ClientIdentityService, target *url.URL) func(*httputil.ProxyRequest) {
	return func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		r.Out.Host = r.In.Host
		r.Out.Header["X-Forwarded-For"] = r.In.Header["X-Forwarded-For"]
		r.SetXForwarded()
		for k := range r.Out.Header {
			if strings.HasPrefix(strings.ToLower(k), authutil.InternalHeaderPrefix) {
				delete(r.Out.Header, k)
			}
		}
		if cis == nil {
			return
		}
		clientIP := clientip.Get(r.In.Context())
		if clientIP == "" {
			return
		}
		r.Out.Header.Set(clientip.HeaderName, clientIP)
		// The caller's own identity was validated before forwarding.
		if r.Out.Header.Get(authutil.ClientIdentityHeaderName) != "" {
			return
		}
		header, err := cis.CachedIdentityHeader(&interfaces.ClientIdentity{
			Origin: interfaces.ClientIdentityInternalOrigin,
			Client: interfaces.ClientIdentityGRPCProxy,
		})
		if err != nil {
			log.CtxWarningf(r.In.Context(), "Could not create client identity header: %s", err)
			return
		}
		r.Out.Header.Set(authutil.ClientIdentityHeaderName, header)
	}
}

// validateCallerIdentity validates the caller's client identity, if it sent
// one.
func validateCallerIdentity(cis interfaces.ClientIdentityService, r *http.Request) error {
	identities := r.Header.Values(authutil.ClientIdentityHeaderName)
	if cis == nil || len(identities) == 0 {
		return nil
	}
	ctx := metadata.NewIncomingContext(r.Context(), metadata.MD{authutil.ClientIdentityHeaderName: identities})
	_, err := cis.ValidateIncomingIdentity(ctx)
	return err
}

func countForwardingDiffs(r *http.Request, identityErr error) {
	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}
	for _, d := range []struct {
		header, reason, attestedValue string
	}{
		{"Forwarded", "forwarded", ""},
		{"X-Forwarded-Host", "x_forwarded_host", r.Host},
		{"X-Forwarded-Proto", "x_forwarded_proto", proto},
	} {
		if v := r.Header.Values(d.header); len(v) > 0 && (len(v) > 1 || v[0] != d.attestedValue) {
			metrics.CacheProxyHTTPForwardingDiffs.WithLabelValues(d.reason).Inc()
		}
	}
	if identityErr != nil {
		metrics.CacheProxyHTTPForwardingDiffs.WithLabelValues("invalid_client_identity").Inc()
	}
}
