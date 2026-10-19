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
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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
	target, err := url.Parse(backendServer.URL)
	require.NoError(t, err)
	proxyServer := httptest.NewServer(proxy_util.NewHTTPForwarder(env, target))
	t.Cleanup(proxyServer.Close)
	return proxyServer
}

// doForgedRequest sends a request carrying forged internal headers through the
// forwarder.
func doForgedRequest(t *testing.T, proxyURL string) {
	doRequest(t, proxyURL, func(req *http.Request) {})
}

// doRequest sends a request carrying forged trust headers through the
// forwarder, after applying mutate.
func doRequest(t *testing.T, proxyURL string, mutate func(*http.Request)) {
	req, err := http.NewRequest("GET", proxyURL+"/some/path", nil)
	require.NoError(t, err)
	req.Host = testClientHost
	req.Header.Set(clientip.HeaderName, forgedClientIP)
	req.Header.Set(forgedInternal, "forged")
	mutate(req)
	rsp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	rsp.Body.Close()
	require.Equal(t, http.StatusOK, rsp.StatusCode)
}

func TestHTTPForwarder_AttestsClientIP(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)

	var got *http.Request
	proxyServer := serveForwarder(t, env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
	}))
	doForgedRequest(t, proxyServer.URL)

	require.NotNil(t, got)
	require.Equal(t, "/some/path", got.URL.Path)
	require.Equal(t, testClientHost, got.Host)
	require.Equal(t, []string{testClientSocket}, got.Header.Values(clientip.HeaderName))
	require.Empty(t, got.Header.Values(forgedInternal))

	// The identity must validate, and be the grpc-proxy identity.
	cis := env.GetClientIdentityService()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{
		authutil.ClientIdentityHeaderName: got.Header.Values(authutil.ClientIdentityHeaderName),
	})
	ctx, err := cis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)
	identity, err := cis.IdentityFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, interfaces.ClientIdentityGRPCProxy, identity.Client)
}

func TestHTTPForwarder_NoIdentityService(t *testing.T) {
	env := newEnv(t, false)

	var got *http.Request
	proxyServer := serveForwarder(t, env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
	}))
	doForgedRequest(t, proxyServer.URL)

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
	doForgedRequest(t, proxyServer.URL)

	require.Equal(t, testClientSocket, gotIP)
}

func TestHTTPForwarder_PreservesForwardedProtoAndHost(t *testing.T) {
	env := newEnv(t, false)

	var got http.Header
	proxyServer := serveForwarder(t, env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("X-Forwarded-Host", "app.buildbuddy.io")
	})

	require.Equal(t, []string{"https"}, got.Values("X-Forwarded-Proto"))
	require.Equal(t, []string{"app.buildbuddy.io"}, got.Values("X-Forwarded-Host"))
}

func TestHTTPForwarder_DerivesForwardedProtoAndHostWhenAbsent(t *testing.T) {
	env := newEnv(t, false)

	var got http.Header
	proxyServer := serveForwarder(t, env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	doForgedRequest(t, proxyServer.URL)

	require.Equal(t, []string{"http"}, got.Values("X-Forwarded-Proto"))
	require.Equal(t, []string{testClientHost}, got.Values("X-Forwarded-Host"))
}

func TestHTTPForwarder_KeepsClientSuppliedForwardedForChain(t *testing.T) {
	env := newEnv(t, false)

	var got http.Header
	proxyServer := serveForwarder(t, env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.9.9.9")
	})

	require.Equal(t, []string{"203.0.113.7, 10.9.9.9, " + testClientSocket}, got.Values("X-Forwarded-For"))
}

// TestHTTPForwarder_ClientIPFromIncomingForwardedFor covers customer -> Envoy
// -> proxy -> app: the proxy trusts the XFF set by Envoy and attests the
// client IP in it, rather than the Envoy peer address.
func TestHTTPForwarder_ClientIPFromIncomingForwardedFor(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "test-key")
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	require.NoError(t, clientip.Init())
	proxyEnv := newEnv(t, true)

	var got http.Header
	proxyServer := serveForwarder(t, proxyEnv, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.9.9.9")
	})

	require.Equal(t, []string{"203.0.113.7"}, got.Values(clientip.HeaderName))
}

func TestHTTPForwarder_CallerIdentityIsKept(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "test-key")
	env := newEnv(t, true)
	executorHeader, err := env.GetClientIdentityService().CachedIdentityHeader(&interfaces.ClientIdentity{
		Origin: interfaces.ClientIdentityInternalOrigin,
		Client: interfaces.ClientIdentityExecutor,
	})
	require.NoError(t, err)

	var got http.Header
	proxyServer := serveForwarder(t, env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set(authutil.ClientIdentityHeaderName, executorHeader)
	})

	require.Equal(t, []string{executorHeader}, got.Values(authutil.ClientIdentityHeaderName))
	// Internal headers are still stripped, and the resolved IP replaces the
	// forged one.
	require.Equal(t, []string{testClientSocket}, got.Values(clientip.HeaderName))
	require.Empty(t, got.Values(forgedInternal))
}

// A caller's identity must never make the app trust a client IP header.
func TestHTTPForwarder_CallerIdentityDoesNotAttestForgedClientIP(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "test-key")
	proxyEnv := newEnv(t, true)
	appEnv := newEnv(t, true)
	executorHeader, err := proxyEnv.GetClientIdentityService().CachedIdentityHeader(&interfaces.ClientIdentity{
		Origin: interfaces.ClientIdentityInternalOrigin,
		Client: interfaces.ClientIdentityExecutor,
	})
	require.NoError(t, err)

	var gotIP string
	app := interceptors.WrapExternalHandler(appEnv, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = clientip.Get(r.Context())
	}))
	proxyServer := serveForwarder(t, proxyEnv, app)
	doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set(authutil.ClientIdentityHeaderName, executorHeader)
	})

	require.Equal(t, testClientSocket, gotIP)
}

// client -> forwarder A -> forwarder B -> app: the app resolves the original
// client IP, not a proxy's.
func TestHTTPForwarder_ClientIPThroughTwoProxyHops(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "test-key")
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	require.NoError(t, clientip.Init())

	var gotIP string
	app := interceptors.WrapExternalHandler(newEnv(t, true), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = clientip.Get(r.Context())
	}))
	proxyB := serveForwarder(t, newEnv(t, true), app)
	proxyBURL, err := url.Parse(proxyB.URL)
	require.NoError(t, err)
	proxyA := httptest.NewServer(proxy_util.NewHTTPForwarder(newEnv(t, true), proxyBURL))
	t.Cleanup(proxyA.Close)

	doRequest(t, proxyA.URL, func(req *http.Request) {
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.9.9.9")
	})

	require.Equal(t, "203.0.113.7", gotIP)
}

// A grpc-proxy identity signed with another key must not make the app trust a
// forged client IP header.
func TestHTTPForwarder_IdentitySignedWithOtherKeyIsNotTrusted(t *testing.T) {
	flags.Set(t, "app.client_identity.key", "attacker-key")
	attackerEnv := newEnv(t, true)
	forgedIdentity, err := attackerEnv.GetClientIdentityService().CachedIdentityHeader(&interfaces.ClientIdentity{
		Origin: interfaces.ClientIdentityInternalOrigin,
		Client: interfaces.ClientIdentityGRPCProxy,
	})
	require.NoError(t, err)

	flags.Set(t, "app.client_identity.key", "test-key")
	proxyEnv := newEnv(t, true)
	appEnv := newEnv(t, true)
	var gotIP string
	app := interceptors.WrapExternalHandler(appEnv, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = clientip.Get(r.Context())
	}))
	proxyServer := serveForwarder(t, proxyEnv, app)
	doRequest(t, proxyServer.URL, func(req *http.Request) {
		req.Header.Set(authutil.ClientIdentityHeaderName, forgedIdentity)
	})

	require.NotEqual(t, forgedClientIP, gotIP)
	require.Equal(t, testClientSocket, gotIP)
}
