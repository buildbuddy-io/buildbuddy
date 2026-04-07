package interceptors

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
