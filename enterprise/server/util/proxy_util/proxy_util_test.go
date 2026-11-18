package proxy_util_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

const (
	forgedClientIP   = "6.6.6.6"
	forgedInternal   = "X-Buildbuddy-Internal-Other"
	testClientHost   = "acme.buildbuddy.io"
	testClientSocket = "127.0.0.1"
)

func newEnv(t *testing.T, withIdentity bool) *real_environment.RealEnv {
	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("test"))
	t.Cleanup(env.GetHealthChecker().Shutdown)
	if withIdentity {
		require.NoError(t, clientidentity.Register(env))
		require.NotNil(t, env.GetClientIdentityService())
	}
	return env
}

func serveForwarder(t *testing.T, env environment.Env, backend http.Handler) *httptest.Server {
	backendServer := httptest.NewServer(backend)
	t.Cleanup(backendServer.Close)
	return serveForwarderTo(t, env, backendServer.URL)
}

func serveForwarderTo(t *testing.T, env environment.Env, targetURL string) *httptest.Server {
	target, err := url.Parse(targetURL)
	require.NoError(t, err)
	proxyServer := httptest.NewServer(proxy_util.NewHTTPForwarder(env, target))
	t.Cleanup(proxyServer.Close)
	return proxyServer
}

// capture returns a backend handler that records the request it receives.
func capture(got **http.Request) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r
	})
}

// doRequest sends a request carrying forged internal headers to url, after
// applying mutate, and returns the response status code.
func doRequest(t *testing.T, url string, mutate func(*http.Request)) int {
	req, err := http.NewRequest("GET", url+"/some/path", nil)
	require.NoError(t, err)
	req.Host = testClientHost
	req.Header.Set(clientip.HeaderName, forgedClientIP)
	req.Header.Set(forgedInternal, "forged")
	mutate(req)
	rsp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	rsp.Body.Close()
	return rsp.StatusCode
}

func noChange(*http.Request) {}

func identityHeader(t *testing.T, env environment.Env, client string) string {
	header, err := env.GetClientIdentityService().CachedIdentityHeader(&interfaces.ClientIdentity{
		Origin: interfaces.ClientIdentityInternalOrigin,
		Client: client,
	})
	require.NoError(t, err)
	return header
}

func requireIdentity(t *testing.T, env environment.Env, header http.Header, client string) {
	cis := env.GetClientIdentityService()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{
		authutil.ClientIdentityHeaderName: header.Values(authutil.ClientIdentityHeaderName),
	})
	ctx, err := cis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)
	identity, err := cis.IdentityFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, client, identity.Client)
}

func TestHTTPForwarder_AttestsClientIP(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)

	var got *http.Request
	proxyServer := serveForwarder(t, env, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, noChange))

	require.NotNil(t, got)
	require.Equal(t, "/some/path", got.URL.Path)
	require.Equal(t, testClientHost, got.Host)
	require.Equal(t, []string{testClientSocket}, got.Header.Values(clientip.HeaderName))
	require.Empty(t, got.Header.Values(forgedInternal))
	requireIdentity(t, env, got.Header, interfaces.ClientIdentityGRPCProxy)
}

func TestHTTPForwarder_NoIdentityService(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	env := newEnv(t, false)

	var got *http.Request
	proxyServer := serveForwarder(t, env, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, noChange))

	require.NotNil(t, got)
	require.Equal(t, testClientHost, got.Host)
	require.Empty(t, got.Header.Values(clientip.HeaderName))
	require.Empty(t, got.Header.Values(forgedInternal))
	require.Empty(t, got.Header.Values(authutil.ClientIdentityHeaderName))
}

// TestHTTPForwarder_ClientIPThroughLoadBalancer reproduces the production path:
// the app sees the proxy's egress IP in X-Forwarded-For (second to last), but
// must resolve the original client IP from the proxy's attestation.
func TestHTTPForwarder_ClientIPThroughLoadBalancer(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "test-key")
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	require.NoError(t, clientip.Init())
	proxyEnv := newEnv(t, true)
	appEnv := newEnv(t, true)

	var gotIP string
	app := interceptors.WrapExternalHandler(appEnv, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = clientip.Get(r.Context())
	}))
	// Mimics GCLB, which appends the IP it saw (the proxy's) and its own.
	lb := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-Forwarded-For", r.Header.Get("X-Forwarded-For")+", 10.0.0.1, 10.0.0.2")
		app.ServeHTTP(w, r)
	})
	proxyServer := serveForwarder(t, proxyEnv, lb)
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, noChange))

	require.Equal(t, testClientSocket, gotIP)
}

func TestHTTPForwarder_SetsForwardingHeaders(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	env := newEnv(t, false)

	var got *http.Request
	proxyServer := serveForwarder(t, env, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.9.9.9")
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("X-Forwarded-Host", "evil.example.com")
		req.Header.Set("Forwarded", "for=203.0.113.7")
	}))

	require.NotNil(t, got)
	require.Equal(t, []string{"203.0.113.7, 10.9.9.9, " + testClientSocket}, got.Header.Values("X-Forwarded-For"))
	require.Equal(t, []string{"http"}, got.Header.Values("X-Forwarded-Proto"))
	require.Equal(t, []string{testClientHost}, got.Header.Values("X-Forwarded-Host"))
	require.Empty(t, got.Header.Values("Forwarded"))
}

// TestHTTPForwarder_ClientIPFromIncomingForwardedFor covers customer -> Envoy
// -> proxy -> app: the proxy trusts the XFF set by Envoy and attests the
// client IP in it, rather than the Envoy peer address.
func TestHTTPForwarder_ClientIPFromIncomingForwardedFor(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "test-key")
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	require.NoError(t, clientip.Init())
	proxyEnv := newEnv(t, true)

	var got *http.Request
	proxyServer := serveForwarder(t, proxyEnv, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.9.9.9")
	}))

	require.NotNil(t, got)
	require.Equal(t, []string{"203.0.113.7"}, got.Header.Values(clientip.HeaderName))
}

func TestHTTPForwarder_ValidCallerIdentityIsKept(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)
	executorHeader := identityHeader(t, env, interfaces.ClientIdentityExecutor)

	var got *http.Request
	proxyServer := serveForwarder(t, env, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set(authutil.ClientIdentityHeaderName, executorHeader)
	}))

	require.NotNil(t, got)
	require.Equal(t, []string{executorHeader}, got.Header.Values(authutil.ClientIdentityHeaderName))
	require.Equal(t, []string{testClientSocket}, got.Header.Values(clientip.HeaderName))
	require.Empty(t, got.Header.Values(forgedInternal))
}

func TestHTTPForwarder_InvalidCallerIdentityIsRejected(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "attacker-key")
	otherKeyHeader := identityHeader(t, newEnv(t, true), interfaces.ClientIdentityGRPCProxy)
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)

	for name, identity := range map[string]string{
		"junk":      "junk",
		"other key": otherKeyHeader,
	} {
		t.Run(name, func(t *testing.T) {
			var got *http.Request
			proxyServer := serveForwarder(t, env, capture(&got))
			require.Equal(t, http.StatusForbidden, doRequest(t, proxyServer.URL, func(req *http.Request) {
				req.Header.Set(authutil.ClientIdentityHeaderName, identity)
			}))
			require.Nil(t, got)
		})
	}
}

// A client can't use the Connection header to make the proxy drop its
// attestation after setting it.
func TestHTTPForwarder_ConnectionHeaderCannotDropAttestation(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)

	var got *http.Request
	proxyServer := serveForwarder(t, env, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("Connection", authutil.ClientIdentityHeaderName+", "+clientip.HeaderName)
	}))

	require.NotNil(t, got)
	require.Equal(t, []string{testClientSocket}, got.Header.Values(clientip.HeaderName))
	requireIdentity(t, env, got.Header, interfaces.ClientIdentityGRPCProxy)
}

// client -> forwarder A -> forwarder B -> app: the app resolves the original
// client IP, not a proxy's.
func TestHTTPForwarder_ClientIPThroughTwoProxyHops(t *testing.T) {
	flags.Set(t, "cache_proxy.attest_http_forwarding", true)
	flags.Set(t, "app.client_identity.key", "test-key")
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	require.NoError(t, clientip.Init())

	var gotIP string
	app := interceptors.WrapExternalHandler(newEnv(t, true), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = clientip.Get(r.Context())
	}))
	proxyB := serveForwarder(t, newEnv(t, true), app)
	proxyA := serveForwarderTo(t, newEnv(t, true), proxyB.URL)

	require.Equal(t, http.StatusOK, doRequest(t, proxyA.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.9.9.9")
	}))

	require.Equal(t, "203.0.113.7", gotIP)
}

// With the experiment off, requests are forwarded as before, and the ones the
// attested forwarder would treat differently are counted.
func TestHTTPForwarder_Legacy(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)

	diffs := func() map[string]float64 {
		m := map[string]float64{}
		for _, reason := range []string{"forwarded", "x_forwarded_host", "x_forwarded_proto", "invalid_client_identity"} {
			m[reason] = testutil.ToFloat64(metrics.CacheProxyHTTPForwardingDiffs.WithLabelValues(reason))
		}
		return m
	}
	before := diffs()

	var got *http.Request
	proxyServer := serveForwarder(t, env, capture(&got))
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("X-Forwarded-Host", "app.buildbuddy.io")
		req.Header.Set("Forwarded", "for=203.0.113.7")
		req.Header.Set(authutil.ClientIdentityHeaderName, "junk")
	}))

	require.NotNil(t, got)
	require.Equal(t, testClientHost, got.Host)
	require.Equal(t, []string{forgedClientIP}, got.Header.Values(clientip.HeaderName))
	require.Equal(t, []string{"junk"}, got.Header.Values(authutil.ClientIdentityHeaderName))
	require.Equal(t, []string{"https"}, got.Header.Values("X-Forwarded-Proto"))
	require.Equal(t, []string{"app.buildbuddy.io"}, got.Header.Values("X-Forwarded-Host"))
	require.Equal(t, []string{"for=203.0.113.7"}, got.Header.Values("Forwarded"))

	after := diffs()
	for reason := range before {
		require.Equal(t, before[reason]+1, after[reason], reason)
	}

	// Headers the attested forwarder would set the same way aren't counted.
	before = after
	require.Equal(t, http.StatusOK, doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-Proto", "http")
		req.Header.Set("X-Forwarded-Host", testClientHost)
		req.Header.Set(authutil.ClientIdentityHeaderName, identityHeader(t, env, interfaces.ClientIdentityExecutor))
	}))
	require.Equal(t, before, diffs())
}

// The app must not trust a client IP header alongside a grpc-proxy identity
// signed with another key.
func TestAppIgnoresClientIPWithIdentitySignedWithOtherKey(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "attacker-key")
	otherKeyHeader := identityHeader(t, newEnv(t, true), interfaces.ClientIdentityGRPCProxy)
	flags.Set(t, "app.client_identity.key", "test-key")
	appEnv := newEnv(t, true)
	validHeader := identityHeader(t, appEnv, interfaces.ClientIdentityGRPCProxy)

	var gotIP string
	app := httptest.NewServer(interceptors.WrapExternalHandler(appEnv, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = clientip.Get(r.Context())
	})))
	t.Cleanup(app.Close)

	for _, tc := range []struct {
		identity, expectedIP string
	}{
		{validHeader, forgedClientIP},
		{otherKeyHeader, testClientSocket},
	} {
		require.Equal(t, http.StatusOK, doRequest(t, app.URL, func(req *http.Request) {
			req.Header.Set(authutil.ClientIdentityHeaderName, tc.identity)
		}))
		require.Equal(t, tc.expectedIP, gotIP)
	}
}
