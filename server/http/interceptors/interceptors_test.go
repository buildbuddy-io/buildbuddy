package interceptors_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type fakeQuotaManager struct {
	interfaces.QuotaManager

	mu             sync.Mutex
	calls          []string
	denyNamespaces map[string]error
}

func (f *fakeQuotaManager) Allow(ctx context.Context, namespace string, quantity int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, namespace)
	return f.denyNamespaces[namespace]
}

func (f *fakeQuotaManager) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type fakeIPRulesEnforcer struct {
	interfaces.IPRulesEnforcer

	err error
}

func (f *fakeIPRulesEnforcer) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) (context.Context, error) {
	return ctx, f.err
}

type fakeProtoletAPI struct {
	apipb.UnimplementedApiServiceServer
	getInvocation func(context.Context, *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error)
}

func (f *fakeProtoletAPI) GetInvocation(ctx context.Context, req *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error) {
	if f.getInvocation != nil {
		return f.getInvocation(ctx, req)
	}
	return &apipb.GetInvocationResponse{}, nil
}

type fakeProtoletBuildBuddyService struct {
	bbspb.UnimplementedBuildBuddyServiceServer
	searchInvocation func(context.Context, *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error)
}

func (f *fakeProtoletBuildBuddyService) SearchInvocation(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	if f.searchInvocation != nil {
		return f.searchInvocation(ctx, req)
	}
	return &inpb.SearchInvocationResponse{}, nil
}

func newTestEnv(t *testing.T) *testenv.TestEnv {
	t.Helper()
	return testenv.GetTestEnv(t)
}

func prefixedProtoReader(t *testing.T, msg proto.Message) *bytes.Reader {
	t.Helper()
	payload, err := proto.Marshal(msg)
	require.NoError(t, err)

	body := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(body[1:5], uint32(len(payload)))
	copy(body[5:], payload)
	return bytes.NewReader(body)
}

func newDeveloperKeyAuthenticator(t *testing.T) *testauth.TestAuthenticator {
	t.Helper()
	return testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"developer-key": &testauth.TestUser{
			UserID:        "developer-key",
			GroupID:       "GROUP1",
			AllowedGroups: []string{"GROUP1"},
			GroupMemberships: []*interfaces.GroupMembership{{
				GroupID:      "GROUP1",
				Capabilities: []cappb.Capability{cappb.Capability_CACHE_WRITE},
			}},
			Capabilities: []cappb.Capability{cappb.Capability_CACHE_WRITE},
		},
	})
}

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
			interceptors.RedirectIfNotForwardedHTTPS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		assert.Equal(t, tc.expected, interceptors.BasicMIMETypeFromExtension(tc.ext))
	}
}

func TestWrapAuthenticatedExternalProtoletHandler_AppliesQuotaToAPIProtolets(t *testing.T) {
	const quotaKey = "rpc:/api.v1.ApiService/GetInvocation"

	for _, testCase := range []struct {
		name           string
		denyNamespaces map[string]error
		wantStatus     int
		wantCalled     bool
	}{
		{
			name:       "allowed",
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name: "quota exceeded",
			denyNamespaces: map[string]error{
				quotaKey: status.ResourceExhaustedError("quota exceeded"),
			},
			wantStatus: http.StatusTooManyRequests,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Set up an authenticated API-key request with quota management enabled.
			env := newTestEnv(t)
			env.SetAuthenticator(newDeveloperKeyAuthenticator(t))
			qm := &fakeQuotaManager{denyNamespaces: testCase.denyNamespaces}
			env.SetQuotaManager(qm)

			// Serve the API method through protolet so the request goes through
			// the same wrapper stack used by /api/v1 RPCs in production.
			called := false
			handlers, err := protolet.GenerateHTTPHandlers(interceptors.APIServicePathPrefix, "api.v1", &fakeProtoletAPI{
				getInvocation: func(ctx context.Context, req *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error) {
					called = true
					return &apipb.GetInvocationResponse{}, nil
				},
			}, grpc.NewServer())
			require.NoError(t, err)

			mux := http.NewServeMux()
			mux.Handle(interceptors.APIServicePathPrefix, interceptors.WrapAuthenticatedExternalProtoletHandler(env, handlers))

			req := httptest.NewRequest(http.MethodPost, interceptors.APIServicePathPrefix+"GetInvocation", strings.NewReader(`{"selector":{"invocationId":"inv-1"}}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(authutil.APIKeyHeader, "developer-key")

			// The wrapper should spend exactly one RPC quota unit, then either
			// call the API handler or stop with Too Many Requests.
			rsp := httptest.NewRecorder()
			mux.ServeHTTP(rsp, req)

			require.Equal(t, testCase.wantStatus, rsp.Code)
			require.Equal(t, testCase.wantCalled, called)
			require.Equal(t, []string{quotaKey}, qm.Calls())
		})
	}
}

func TestWrapAuthenticatedExternalProtoletHandler_AppliesQuotaToBuildBuddyServiceProtolets(t *testing.T) {
	const quotaKey = "rpc:/buildbuddy.service.BuildBuddyService/SearchInvocation"

	for _, testCase := range []struct {
		name           string
		denyNamespaces map[string]error
		wantStatus     int
		wantCalled     bool
	}{
		{
			name:       "allowed",
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name: "quota exceeded",
			denyNamespaces: map[string]error{
				quotaKey: status.ResourceExhaustedError("quota exceeded"),
			},
			wantStatus: http.StatusTooManyRequests,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Set up a logged-in BuildBuddyService request with quota management enabled.
			env := newTestEnv(t)
			env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1")))
			qm := &fakeQuotaManager{denyNamespaces: testCase.denyNamespaces}
			env.SetQuotaManager(qm)

			// Serve the UI-facing protolet route for BuildBuddyService RPCs.
			called := false
			handlers, err := protolet.GenerateHTTPHandlers(interceptors.BuildBuddyServicePathPrefix, "buildbuddy.service", &fakeProtoletBuildBuddyService{
				searchInvocation: func(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
					called = true
					return &inpb.SearchInvocationResponse{}, nil
				},
			}, grpc.NewServer())
			require.NoError(t, err)

			mux := http.NewServeMux()
			mux.Handle(interceptors.BuildBuddyServicePathPrefix, interceptors.WrapAuthenticatedExternalProtoletHandler(env, handlers))

			// The UI hits the protolet route `/rpc/BuildBuddyService/...`, even
			// though quota uses the full gRPC method name `/buildbuddy.service.BuildBuddyService/...`.
			req := httptest.NewRequest(http.MethodPost, interceptors.BuildBuddyServicePathPrefix+"SearchInvocation", strings.NewReader(`{}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(authutil.APIKeyHeader, "USER1")

			// The HTTP wrapper should charge the full gRPC method quota key,
			// and a quota denial should prevent the RPC handler from running.
			rsp := httptest.NewRecorder()
			mux.ServeHTTP(rsp, req)

			require.Equal(t, testCase.wantStatus, rsp.Code)
			require.Equal(t, testCase.wantCalled, called)
			require.Equal(t, []string{quotaKey}, qm.Calls())
		})
	}
}

func TestWrapAuthenticatedExternalProtoletHandler_SkipsHTTPQuotaForPrefixedProto(t *testing.T) {
	// Set up a protolet handler with quota management enabled.
	env := newTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1")))
	qm := &fakeQuotaManager{}
	env.SetQuotaManager(qm)

	// Register the fake BuildBuddyService on the backing gRPC server because
	// proto-prefixed requests are forwarded there by protolet.
	called := false
	service := &fakeProtoletBuildBuddyService{
		searchInvocation: func(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
			called = true
			return &inpb.SearchInvocationResponse{}, nil
		},
	}
	grpcServer := grpc.NewServer()
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, service)
	handlers, err := protolet.GenerateHTTPHandlers(interceptors.BuildBuddyServicePathPrefix, "buildbuddy.service.BuildBuddyService", service, grpcServer)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle(interceptors.BuildBuddyServicePathPrefix, interceptors.WrapAuthenticatedExternalProtoletHandler(env, handlers))

	// Proto-prefixed requests are handed off to the gRPC server, whose own
	// interceptors enforce quota. The HTTP wrapper should not charge quota too.
	req := httptest.NewRequest(http.MethodPost, interceptors.BuildBuddyServicePathPrefix+"SearchInvocation", prefixedProtoReader(t, &inpb.SearchInvocationRequest{}))
	req.Header.Set("Content-Type", protolet.PrefixedProtoContentType)
	req.Header.Set(authutil.APIKeyHeader, "USER1")

	rsp := httptest.NewRecorder()
	mux.ServeHTTP(rsp, req)

	// The request should still reach the service, but the outer HTTP quota
	// wrapper should not charge it a second time before the gRPC handoff.
	require.Equal(t, http.StatusOK, rsp.Code)
	require.True(t, called)
	require.Empty(t, qm.Calls())
}

func TestWrapAuthenticatedExternalAPIRPCHandler_AppliesQuota(t *testing.T) {
	const quotaKey = "rpc:/api.v1.ApiService/GetFile"

	for _, testCase := range []struct {
		name           string
		denyNamespaces map[string]error
		wantStatus     int
		wantCalled     bool
	}{
		{
			name:       "allowed",
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name: "quota exceeded",
			denyNamespaces: map[string]error{
				quotaKey: status.ResourceExhaustedError("quota exceeded"),
			},
			wantStatus: http.StatusTooManyRequests,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Set up an authenticated GetFile request with quota management enabled.
			env := newTestEnv(t)
			env.SetAuthenticator(newDeveloperKeyAuthenticator(t))
			qm := &fakeQuotaManager{denyNamespaces: testCase.denyNamespaces}
			env.SetQuotaManager(qm)

			// GetFile is not served by protolet, but it should still be treated
			// as the /api.v1.ApiService/GetFile RPC for quota accounting.
			called := false
			handler := interceptors.WrapAuthenticatedExternalAPIRPCHandler(env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodPost, "/api/v1/GetFile", nil)
			req.Header.Set(authutil.APIKeyHeader, "developer-key")

			rsp := httptest.NewRecorder()
			handler.ServeHTTP(rsp, req)

			// Allowed requests reach the handler; quota-denied requests stop at
			// the wrapper, and both paths spend exactly one GetFile quota unit.
			require.Equal(t, testCase.wantStatus, rsp.Code)
			require.Equal(t, testCase.wantCalled, called)
			require.Equal(t, []string{quotaKey}, qm.Calls())
		})
	}
}

func TestWrapAuthenticatedExternalAPIRPCHandler_DoesNotSpendQuotaWhenIPDenied(t *testing.T) {
	// Set up an authenticated GetFile request with both IP rules and quota enabled.
	env := newTestEnv(t)
	env.SetAuthenticator(newDeveloperKeyAuthenticator(t))
	qm := &fakeQuotaManager{}
	env.SetQuotaManager(qm)
	env.SetIPRulesEnforcer(&fakeIPRulesEnforcer{err: status.PermissionDeniedError("blocked")})

	// The IP rules enforcer rejects the request before it reaches GetFile.
	// Quota should not be charged for a request that IP rules already blocked.
	called := false
	handler := interceptors.WrapAuthenticatedExternalAPIRPCHandler(env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/GetFile", nil)
	req.Header.Set(authutil.APIKeyHeader, "developer-key")

	// Send the blocked request through the full GetFile wrapper.
	rsp := httptest.NewRecorder()
	handler.ServeHTTP(rsp, req)

	// The denial should come from IP authorization before GetFile is called or
	// quota is spent.
	require.Equal(t, http.StatusForbidden, rsp.Code)
	require.False(t, called)
	require.Empty(t, qm.Calls())
}

func TestWrapAuthenticatedExternalProtoletHandler_RejectsInvalidRPC(t *testing.T) {
	// Set up the wrapper around a fake ApiService. Only known /api/v1/* methods
	// are registered, so requests to other paths must be rejected before reaching
	// the inner handler.
	env := newTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1")))

	called := false
	handlers, err := protolet.GenerateHTTPHandlers(interceptors.APIServicePathPrefix, "api.v1", &fakeProtoletAPI{
		getInvocation: func(ctx context.Context, req *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error) {
			called = true
			return &apipb.GetInvocationResponse{}, nil
		},
	}, grpc.NewServer())
	require.NoError(t, err)

	handler := interceptors.WrapAuthenticatedExternalProtoletHandler(env, handlers)

	for _, testCase := range []struct {
		name string
		path string
	}{
		{
			name: "unsupported prefix",
			path: "/rpc/UnknownService/SearchInvocation",
		},
		{
			name: "unknown method",
			path: interceptors.APIServicePathPrefix + "NoSuchMethod",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testCase.path, strings.NewReader(`{}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(authutil.APIKeyHeader, "USER1")

			rsp := httptest.NewRecorder()
			handler.ServeHTTP(rsp, req)

			// The wrapper must reject invalid RPC paths with a 4xx status without
			// invoking the inner handler.
			require.GreaterOrEqual(t, rsp.Code, 400, "body: %s", rsp.Body.String())
			require.Less(t, rsp.Code, 500, "body: %s", rsp.Body.String())
			require.False(t, called)
		})
	}
}

func TestProtoletRPCNameFromHTTPPath(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		path    string
		wantRPC string
		wantOK  bool
	}{
		{
			name:    "buildbuddy service",
			path:    interceptors.BuildBuddyServicePathPrefix + "SearchInvocation",
			wantRPC: "/buildbuddy.service.BuildBuddyService/SearchInvocation",
			wantOK:  true,
		},
		{
			name:    "api service",
			path:    interceptors.APIServicePathPrefix + "Run",
			wantRPC: "/api.v1.ApiService/Run",
			wantOK:  true,
		},
		{
			name:   "unsupported prefix",
			path:   "/rpc/UnknownService/SearchInvocation",
			wantOK: false,
		},
		{
			name:    "unknown method",
			path:    interceptors.APIServicePathPrefix + "NoSuchMethod",
			wantRPC: "/api.v1.ApiService/NoSuchMethod",
			wantOK:  true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotRPC, gotOK := interceptors.ProtoletRPCNameFromHTTPPath(testCase.path)
			require.Equal(t, testCase.wantOK, gotOK)
			require.Equal(t, testCase.wantRPC, gotRPC)
		})
	}
}
