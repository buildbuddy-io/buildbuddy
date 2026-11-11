package grpc_forward

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func cleanup() {
	for _, pool := range backendConnectionPools {
		pool.Close()
	}
	// Reset to an empty (non-nil) map so a subsequent test that dials through
	// getConnectionPool doesn't panic assigning into a nil map.
	backendConnectionPools = map[int]*grpc_client.ClientConnPool{}
}

// TestForwarding_PropagatesClientHeadersToBackend tests that the unknown-RPC
// gRPC forwarder preserves client-supplied headers.
func TestForwarding_PropagatesClientHeadersToBackend(t *testing.T) {
	const clientHeader = "x-test-client-header"

	var mu sync.Mutex
	var gotMD metadata.MD
	backendHandler := func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		mu.Lock()
		gotMD = md.Copy()
		mu.Unlock()
		_ = stream.RecvMsg(&emptypb.Empty{})
		return stream.SendMsg(&emptypb.Empty{})
	}
	backendTarget := startBackend(t, backendHandler)
	flags.Set(t, "app.proxy_targets", []proxyRule{{Prefix: "", Target: backendTarget}})
	clientConn := startProxy(t, "")

	ctx := metadata.AppendToOutgoingContext(context.Background(), clientHeader, "test-value")
	err := clientConn.Invoke(ctx, "/test.TestService/TestMethod", &emptypb.Empty{}, &emptypb.Empty{})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"test-value"}, gotMD.Get(clientHeader),
		"the client-supplied header must be forwarded through the proxy to the backend")
}

func TestGetConnectionPool_DedupesConcurrentDialsForSameRule(t *testing.T) {
	t.Cleanup(cleanup)

	flags.Set(t, "app.proxy_targets", []proxyRule{{Prefix: "", Target: "remote.example.com"}})
	dial := func(_ string, _ int, _ ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
		// Sleep for a bit to increase the chance of race conditions.
		time.Sleep(10 * time.Millisecond)
		return &grpc_client.ClientConnPool{}, nil
	}

	pools := make([]*grpc_client.ClientConnPool, 10)
	var wg sync.WaitGroup
	for i := range pools {
		wg.Go(func() {
			p, err := getConnectionPool(dial, "/test.TestService/TestMethod")
			require.NotNil(t, p)
			require.NoError(t, err)
			pools[i] = p
		})
	}
	wg.Wait()

	require.NotNil(t, pools[0])
	for _, pool := range pools[1:] {
		require.Same(t, pools[0], pool)
	}
}

type dialCall struct {
	target string
	size   int
}

func poolForMethod(t *testing.T, dialer dialFn, method string) *grpc_client.ClientConnPool {
	pool, err := getConnectionPool(dialer, method)
	require.NoError(t, err)
	return pool
}

func TestGetConnectionPool_DistinctRulesWithSameTargetGetDistinctPools(t *testing.T) {
	t.Cleanup(cleanup)
	const target = "remote.example.com"
	flags.Set(t, "app.proxy_targets", []proxyRule{
		{Prefix: "/build.bazel.remote.execution.v2.Execution/", Target: target},
		{Prefix: "", Target: target},
	})
	dials := 0
	dialer := func(string, int, ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
		dials++
		return &grpc_client.ClientConnPool{}, nil
	}

	execute := poolForMethod(t, dialer, "/build.bazel.remote.execution.v2.Execution/Execute")
	bes := poolForMethod(t, dialer, "/google.devtools.build.v1.PublishBuildEvent/PublishLifecycleEvent")
	wait := poolForMethod(t, dialer, "/build.bazel.remote.execution.v2.Execution/WaitExecution")

	require.NotSame(t, execute, bes)
	require.Same(t, execute, wait)
	require.Equal(t, 2, dials)
}

func TestGetConnectionPool_UsesPerRulePoolSize(t *testing.T) {
	t.Cleanup(cleanup)
	flags.Set(t, "app.proxy_pool_size", 5)
	flags.Set(t, "app.proxy_targets", []proxyRule{
		{Prefix: "/a.A/", Target: "a.example.com", PoolSize: 3},
		{Prefix: "", Target: "b.example.com"},
	})
	var calls []dialCall
	dialer := func(target string, size int, _ ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
		calls = append(calls, dialCall{target, size})
		return &grpc_client.ClientConnPool{}, nil
	}

	poolForMethod(t, dialer, "/a.A/Foo")
	poolForMethod(t, dialer, "/b.B/Bar")

	require.Equal(t, []dialCall{{"a.example.com", 3}, {"b.example.com", 5}}, calls)
}

// A held stream saturating one rule's pool must not block RPCs on another rule.
func TestForwarding_SaturatedRulePoolDoesNotBlockOtherRules(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	backendHandler := func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		if method == "/a.A/Stream" {
			close(started)
			select {
			case <-release:
			case <-stream.Context().Done():
			}
		}
		return stream.SendMsg(&emptypb.Empty{})
	}
	backendTarget := startBackend(t, backendHandler, grpc.MaxConcurrentStreams(1))
	flags.Set(t, "app.proxy_targets", []proxyRule{
		{Prefix: "/a.A/", Target: backendTarget, PoolSize: 1},
		{Prefix: "", Target: backendTarget, PoolSize: 1},
	})
	clientConn := startProxy(t, "")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := clientConn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/a.A/Stream")
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&emptypb.Empty{}))
	require.NoError(t, stream.CloseSend())
	waitForStarted(ctx, t, started)

	err = clientConn.Invoke(ctx, "/b.B/Unary", &emptypb.Empty{}, &emptypb.Empty{})
	require.NoError(t, err)

	close(release)
	require.NoError(t, stream.RecvMsg(&emptypb.Empty{}))
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }

// startBackend starts a server that handles every method with backendHandler
// and returns its target.
func startBackend(t *testing.T, backendHandler grpc.StreamHandler, opts ...grpc.ServerOption) string {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	backend := grpc.NewServer(append(opts, grpc.UnknownServiceHandler(backendHandler))...)
	go func() { _ = backend.Serve(lis) }()
	t.Cleanup(backend.Stop)
	return fmt.Sprintf("grpc://localhost:%d", lis.Addr().(*net.TCPAddr).Port)
}

// startProxy starts a forwarding proxy for the app.proxy_targets already set
// and returns a client connected to it. If groupID is non-empty, the proxy
// attaches claims for that group to each stream, as the auth interceptor would.
func startProxy(t *testing.T, groupID string) *grpc_client.ClientConnPool {
	t.Cleanup(cleanup)
	fwdOpt := GetForwardingServerOption(real_environment.NewRealEnv(nil))
	require.NotNil(t, fwdOpt, "forwarding must be enabled when app.proxy_targets is set")
	opts := []grpc.ServerOption{fwdOpt}
	if groupID != "" {
		opts = append(opts, grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			ctx := claims.AuthContext(ss.Context(), &claims.Claims{GroupID: groupID})
			return h(srv, &ctxStream{ServerStream: ss, ctx: ctx})
		}))
	}
	proxyLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	proxy := grpc.NewServer(opts...)
	go func() { _ = proxy.Serve(proxyLis) }()
	t.Cleanup(proxy.Stop)

	conn, err := grpc_client.DialSimple(fmt.Sprintf("grpc://localhost:%d", proxyLis.Addr().(*net.TCPAddr).Port))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// startForwardingProxy starts a backend running backendHandler behind a proxy
// with a single catch-all rule.
func startForwardingProxy(t *testing.T, backendHandler grpc.StreamHandler, groupID string) *grpc_client.ClientConnPool {
	flags.Set(t, "app.proxy_targets", []proxyRule{{Prefix: "", Target: startBackend(t, backendHandler)}})
	return startProxy(t, groupID)
}

func waitForStarted(ctx context.Context, t *testing.T, started <-chan struct{}) {
	select {
	case <-started:
	case <-ctx.Done():
		require.FailNow(t, "backend never received the forwarded stream")
	}
}

func activeStreams(method, groupID string) float64 {
	return testutil.ToFloat64(metrics.GRPCForwardActiveStreams.WithLabelValues(method, groupID))
}

func requireActiveStreamsEventuallyZero(t *testing.T, method, groupID string) {
	require.Eventually(t, func() bool { return activeStreams(method, groupID) == 0 }, 30*time.Second, 10*time.Millisecond)
}

// Each test uses its own method name because the gauge is global.
func testActiveStreamsWhileBlocked(t *testing.T, method, serverGroupID, wantGroupID string) {
	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ any, stream grpc.ServerStream) error {
		if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		close(started)
		select {
		case <-release:
		case <-stream.Context().Done():
		}
		return stream.SendMsg(&emptypb.Empty{})
	}
	conn := startForwardingProxy(t, handler, serverGroupID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&emptypb.Empty{}))
	require.NoError(t, stream.CloseSend())
	waitForStarted(ctx, t, started)

	require.Equal(t, 1.0, activeStreams(method, wantGroupID))

	close(release)
	require.NoError(t, stream.RecvMsg(&emptypb.Empty{}))
	require.Equal(t, io.EOF, stream.RecvMsg(&emptypb.Empty{}))
	requireActiveStreamsEventuallyZero(t, method, wantGroupID)
}

func TestForwarding_ActiveStreamsGaugeTracksOpenStream(t *testing.T) {
	testActiveStreamsWhileBlocked(t, "/gauge.Test/Tracked", "GR123", "GR123")
}

func TestForwarding_ActiveStreamsGaugeUsesFallbackGroupWithoutClaims(t *testing.T) {
	testActiveStreamsWhileBlocked(t, "/gauge.Test/Unauthenticated", "", interfaces.AuthAnonymousUser)
}

func TestForwarding_ActiveStreamsGaugeReturnsToZeroOnError(t *testing.T) {
	const method = "/gauge.Test/Rejected"
	// An empty backend handler would succeed; use a handler that rejects.
	handler := func(any, grpc.ServerStream) error {
		return status.UnimplementedError("nope")
	}
	conn := startForwardingProxy(t, handler, "GR123")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := conn.Invoke(ctx, method, &emptypb.Empty{}, &emptypb.Empty{})
	require.Error(t, err)
	requireActiveStreamsEventuallyZero(t, method, "GR123")
}

// fakeIdentityService records the identity it was asked to sign and returns a
// canned header.
type fakeIdentityService struct {
	mu         sync.Mutex
	lastClient string
	header     string
}

func (f *fakeIdentityService) CachedIdentityHeader(si *interfaces.ClientIdentity) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastClient = si.Client
	return f.header, nil
}

func (f *fakeIdentityService) NewIdentityHeader(si *interfaces.ClientIdentity, _ time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastClient = si.Client
	return f.header, nil
}

func (f *fakeIdentityService) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeIdentityService) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeIdentityService) IdentityFromContext(context.Context) (*interfaces.ClientIdentity, error) {
	return nil, nil
}

func ctxWithResolvedClientIP(ip string) context.Context {
	return context.WithValue(context.Background(), clientip.ContextKey, ip)
}

func ctxWithResolvedSubdomain(sd string) context.Context {
	return subdomain.Context(context.Background(), sd)
}

func TestCtxForBackend(t *testing.T) {
	const clientIP = "1.2.3.4"
	const sd = "acme"
	const identity = "signed-grpc-proxy-header"

	t.Run("attaches client IP and identity", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx, err := ctxForBackend(ctxWithResolvedClientIP(clientIP), cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Equal(t, []string{identity}, md.Get(authutil.ClientIdentityHeaderName))
		// The proxy attests as the grpc-proxy identity.
		require.Equal(t, interfaces.ClientIdentityGRPCProxy, cis.lastClient)
	})

	t.Run("attaches subdomain and identity", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx, err := ctxForBackend(ctxWithResolvedSubdomain(sd), cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{sd}, md.Get(subdomain.HeaderName))
		// The subdomain alone is enough to warrant attesting as grpc-proxy;
		// the backend only honors the header from that identity.
		require.Equal(t, []string{identity}, md.Get(authutil.ClientIdentityHeaderName))
		require.Equal(t, interfaces.ClientIdentityGRPCProxy, cis.lastClient)
	})

	t.Run("nothing resolved attaches nothing", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx, err := ctxForBackend(context.Background(), cis)
		require.NoError(t, err)

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			require.Empty(t, md.Get(clientip.HeaderName))
			require.Empty(t, md.Get(subdomain.HeaderName))
			require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
		}
	})

	t.Run("client-supplied subdomain header is overwritten with the resolved subdomain", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		// Simulate a caller pre-setting the subdomain header to a subdomain
		// their API key would match, to slip past the backend's check.
		ctx := metadata.NewIncomingContext(ctxWithResolvedSubdomain(sd), metadata.Pairs(subdomain.HeaderName, "attacker"))
		ctx, err := ctxForBackend(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{sd}, md.Get(subdomain.HeaderName))
	})

	t.Run("client-supplied subdomain is stripped when none is resolved", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(subdomain.HeaderName, "attacker"))
		ctx, err := ctxForBackend(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Empty(t, md.Get(subdomain.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("client-supplied client IP header is overwritten with the resolved IP", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		// Simulate an attacker pre-setting the client-IP header to a spoofed value.
		ctx := metadata.NewIncomingContext(ctxWithResolvedClientIP(clientIP), metadata.Pairs(clientip.HeaderName, "9.9.9.9"))
		ctx, err := ctxForBackend(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		// The spoofed value is stripped and replaced with the proxy-resolved IP.
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Equal(t, []string{identity}, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("client-supplied client IP is stripped when no IP is resolved", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		// Spoofed header present, but the proxy resolved no client IP of its own.
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clientip.HeaderName, "9.9.9.9"))
		ctx, err := ctxForBackend(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Empty(t, md.Get(clientip.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("client-supplied internal headers are stripped, other headers are preserved", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx := metadata.NewIncomingContext(ctxWithResolvedClientIP(clientIP), metadata.Pairs(
			authutil.InternalHeaderPrefix+"future-signal", "attacker",
			"x-test-client-header", "keep-me",
		))
		ctx, err := ctxForBackend(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Empty(t, md.Get(authutil.InternalHeaderPrefix+"future-signal"))
		require.Equal(t, []string{"keep-me"}, md.Get("x-test-client-header"))
	})

	t.Run("nil identity service sets IP without identity", func(t *testing.T) {
		ctx, err := ctxForBackend(ctxWithResolvedClientIP(clientIP), nil)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("caller-supplied identity is preserved rather than overwritten", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		// A caller that already carries a signed identity (e.g. a workflow) must
		// keep it so the backend still authorizes on it (e.g. IP-rule bypass);
		// the proxy must not clobber it with its own grpc-proxy identity.
		const callerIdentity = "workflow-identity"
		ctx := metadata.NewIncomingContext(ctxWithResolvedClientIP(clientIP), metadata.Pairs(authutil.ClientIdentityHeaderName, callerIdentity))
		ctx, err := ctxForBackend(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Equal(t, []string{callerIdentity}, md.Get(authutil.ClientIdentityHeaderName))
		// The proxy did not mint a grpc-proxy identity for this request.
		require.Empty(t, cis.lastClient)
	})
}
