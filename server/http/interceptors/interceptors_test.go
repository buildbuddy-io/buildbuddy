package interceptors

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

func TestRecoverAndAlert_UnexpectedPanic(t *testing.T) {
	server := httptest.NewServer(RecoverAndAlert(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(fmt.Errorf("something went wrong"))
	})))
	defer server.Close()

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Contains(t, string(body), "A panic occurred")
}

// A handler that panics with http.ErrAbortHandler is deliberately abandoning a
// response it has already started writing, which is the only way to signal a
// failure once the status has been committed. The panic has to reach net/http
// for the connection to be closed, so it must not be swallowed and turned into
// an error message appended to the body.
func TestRecoverAndAlert_AbortHandlerPanicAbortsResponse(t *testing.T) {
	// Declare more content than we write, and write enough of it to force the
	// status line and headers out onto the connection.
	const contentLength = 16 * 1024
	partial := strings.Repeat("a", contentLength/2)
	server := httptest.NewServer(RecoverAndAlert(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(contentLength))
		w.WriteHeader(http.StatusOK)
		_, err := io.WriteString(w, partial)
		require.NoError(t, err)
		panic(http.ErrAbortHandler)
	})))
	defer server.Close()

	resp, err := http.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, readErr := io.ReadAll(resp.Body)
	require.Error(t, readErr, "aborted response should surface as a read error")
	require.NotContains(t, string(body), "A panic occurred")
	require.True(t, strings.HasPrefix(partial, string(body)), "response body should be a prefix of what the handler wrote")
}
