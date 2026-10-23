package interceptors

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestRedirectIfNotForwardedHTTPS(t *testing.T) {
	flags.Set(t, "ssl.upgrade_insecure", true)

	tests := []struct {
		name            string
		route           string
		expectedCode    int
		expectedHeaders http.Header
		setup           func(*http.Request)
	}{
		{
			name:            "https request",
			route:           "/foo",
			expectedCode:    http.StatusOK,
			expectedHeaders: http.Header{},
			setup: func(req *http.Request) {
				req.Header.Set("X-Forwarded-Proto", "https")
			},
		},
		{
			name:         "http request with X-Forwarded-Proto header",
			route:        "/foo",
			expectedCode: http.StatusMovedPermanently,
			expectedHeaders: http.Header{
				"Location": []string{"https://example.com/foo"},
			},
			setup: func(req *http.Request) {
				req.Header.Set("X-Forwarded-Proto", "http")
			},
		},
		{
			name:         "http request without X-Forwarded-Proto header",
			route:        "/foo",
			expectedCode: http.StatusMovedPermanently,
			expectedHeaders: http.Header{
				"Location": []string{"https://example.com/foo"},
			},
			setup: func(req *http.Request) {
				req.Header.Del("X-Forwarded-Proto")
			},
		},
		{
			name:            "healthcheck request without X-Forwarded-Proto header",
			route:           "/health",
			expectedCode:    http.StatusOK,
			expectedHeaders: http.Header{},
			setup: func(req *http.Request) {
				req.Header.Del("X-Forwarded-Proto")
				req.Header.Set("User-Agent", "GoogleHC/1.0")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.route, nil)
			if tt.setup != nil {
				tt.setup(req)
			}

			rr := httptest.NewRecorder()
			RedirectIfNotForwardedHTTPS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, r.Header)
			})).ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)

			for headerName, expectedValues := range tt.expectedHeaders {
				assert.Equal(t, expectedValues, rr.Header().Values(headerName))
			}
		})
	}
}

func TestBasicMIMETypeFromExtension(t *testing.T) {
	for _, tc := range []struct {
		ext      string
		expected string
	}{
		{".png", "image/png"},
		{".jpg", "image/jpeg"},
		{".jpeg", "image/jpeg"},
		{".webm", "video/webm"},
		{".svg", "application/octet-stream"},
		{".js", "application/octet-stream"},
		{".pdf", "application/octet-stream"},
	} {
		assert.Equal(t, tc.expected, BasicMIMETypeFromExtension(tc.ext))
	}
}

func TestAuthorizeSelectedGroupRole_AdaptsHTTPRPCPaths(t *testing.T) {
	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("test"))
	t.Cleanup(env.GetHealthChecker().Shutdown)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1")))

	for _, tc := range []struct {
		name         string
		route        string
		expectedRPC  string
		expectedCode int
		expectedBody string
	}{
		{
			name:         "buildbuddy service",
			route:        "/rpc/BuildBuddyService/SearchInvocation",
			expectedRPC:  buildBuddyServicePrefix + "SearchInvocation",
			expectedCode: http.StatusOK,
		},
		{
			name:         "api service",
			route:        "/api/v1/Run",
			expectedRPC:  apiServicePrefix + "Run",
			expectedCode: http.StatusOK,
		},
		{
			name:         "unsupported prefix",
			route:        "/rpc/UnknownService/SearchInvocation",
			expectedCode: http.StatusForbidden,
			expectedBody: "unsupported RPC path\n",
		},
		{
			name:         "unknown method",
			route:        "/api/v1/NoSuchMethod",
			expectedCode: http.StatusForbidden,
			expectedBody: "rpc error: code = PermissionDenied desc = permission denied\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			var gotRPCName string
			handler := Authenticate(env, parseProtoletRPCName(AuthorizeSelectedGroupRole(env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotRPCName, _ = rpcNameFromContext(r.Context())
				called = true
				w.WriteHeader(http.StatusOK)
			}))))

			req := httptest.NewRequest(http.MethodPost, tc.route, nil)
			req.Header.Set(authutil.APIKeyHeader, "USER1")
			rsp := httptest.NewRecorder()
			handler.ServeHTTP(rsp, req)

			require.Equal(t, tc.expectedCode, rsp.Code)
			if tc.expectedCode == http.StatusOK {
				require.True(t, called)
				require.Equal(t, tc.expectedRPC, gotRPCName)
				return
			}
			require.False(t, called)
			require.Equal(t, tc.expectedBody, rsp.Body.String())
		})
	}
}

func TestClientIP_XForwardedForHops(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hops   int
		xff    []string
		wantIP string
	}{
		{
			name:   "no X-Forwarded-For uses peer",
			hops:   1,
			wantIP: "10.0.0.1",
		},
		{
			name:   "uses last entry across multiple lines",
			hops:   1,
			xff:    []string{"9.9.9.9", "8.8.8.8"},
			wantIP: "8.8.8.8",
		},
		{
			name:   "counts entries across multiple lines",
			hops:   2,
			xff:    []string{"9.9.9.9, 7.7.7.7", "8.8.8.8"},
			wantIP: "7.7.7.7",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "auth.trust_xforwardedfor_header", true)
			flags.Set(t, "auth.xforwardedfor_trusted_hops", tc.hops)

			env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("test"))
			t.Cleanup(env.GetHealthChecker().Shutdown)
			var gotIP string
			handler := ClientIP(env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotIP = clientip.Get(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = "10.0.0.1:1234"
			for _, v := range tc.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			handler.ServeHTTP(httptest.NewRecorder(), req)

			assert.Equal(t, tc.wantIP, gotIP)
		})
	}
}

// fakeClientIdentityService treats identity header values as the client name,
// and rejects the value "invalid".
type fakeClientIdentityService struct {
	interfaces.ClientIdentityService
}

type validatedKey struct{}

func (fakeClientIdentityService) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	vals := metadata.ValueFromIncomingContext(ctx, authutil.ClientIdentityHeaderName)
	if len(vals) != 1 || vals[0] == "invalid" {
		return ctx, errors.New("invalid identity")
	}
	return context.WithValue(ctx, validatedKey{}, vals[0]), nil
}

func (fakeClientIdentityService) IdentityFromContext(ctx context.Context) (*interfaces.ClientIdentity, error) {
	client, ok := ctx.Value(validatedKey{}).(string)
	if !ok {
		return nil, errors.New("no identity")
	}
	return &interfaces.ClientIdentity{Client: client}, nil
}

func TestClientIP(t *testing.T) {
	require.NoError(t, clientip.Init())
	flags.Set(t, "auth.trust_xforwardedfor_header", true)

	for _, tc := range []struct {
		name     string
		cis      bool
		headers  map[string][]string
		expected string
	}{
		{
			name: "attested header with grpc-proxy identity is honored",
			cis:  true,
			headers: map[string][]string{
				clientip.HeaderName:               {"1.2.3.4"},
				authutil.ClientIdentityHeaderName: {interfaces.ClientIdentityGRPCProxy},
			},
			expected: "1.2.3.4",
		},
		{
			name: "attested header without identity is ignored",
			cis:  true,
			headers: map[string][]string{
				clientip.HeaderName: {"1.2.3.4"},
			},
			expected: "9.9.9.9",
		},
		{
			name: "attested header with invalid identity is ignored",
			cis:  true,
			headers: map[string][]string{
				clientip.HeaderName:               {"1.2.3.4"},
				authutil.ClientIdentityHeaderName: {"invalid"},
			},
			expected: "9.9.9.9",
		},
		{
			name: "attested header with non-proxy identity is ignored",
			cis:  true,
			headers: map[string][]string{
				clientip.HeaderName:               {"1.2.3.4"},
				authutil.ClientIdentityHeaderName: {interfaces.ClientIdentityExecutor},
			},
			expected: "9.9.9.9",
		},
		{
			name: "multiple attested headers are ignored",
			cis:  true,
			headers: map[string][]string{
				clientip.HeaderName:               {"1.2.3.4", "1.1.1.1"},
				authutil.ClientIdentityHeaderName: {interfaces.ClientIdentityGRPCProxy},
			},
			expected: "9.9.9.9",
		},
		{
			name: "attested header is ignored without a client identity service",
			cis:  false,
			headers: map[string][]string{
				clientip.HeaderName:               {"1.2.3.4"},
				authutil.ClientIdentityHeaderName: {interfaces.ClientIdentityGRPCProxy},
			},
			expected: "9.9.9.9",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("test"))
			t.Cleanup(env.GetHealthChecker().Shutdown)
			if tc.cis {
				env.SetClientIdentityService(fakeClientIdentityService{})
			}
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("X-Forwarded-For", "9.9.9.9, 5.5.5.5")
			for k, vs := range tc.headers {
				for _, v := range vs {
					req.Header.Add(k, v)
				}
			}
			var got string
			ClientIP(env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = clientip.Get(r.Context())
			})).ServeHTTP(httptest.NewRecorder(), req)
			require.Equal(t, tc.expected, got)
		})
	}
}
