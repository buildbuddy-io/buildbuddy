package grpc_forward

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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
	backendConnectionPools = map[string]*grpc_client.ClientConnPool{}
}

// TestForwarding_PropagatesClientHeadersToBackend tests that the unknown-RPC
// gRPC forwarder preserves client-supplied headers.
func TestForwarding_PropagatesClientHeadersToBackend(t *testing.T) {
	t.Cleanup(cleanup)
	const clientHeader = "x-test-client-header"

	backendLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
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
	backend := grpc.NewServer(grpc.UnknownServiceHandler(backendHandler))
	go func() { _ = backend.Serve(backendLis) }()
	t.Cleanup(backend.Stop)
	backendTarget := fmt.Sprintf("grpc://localhost:%d", backendLis.Addr().(*net.TCPAddr).Port)

	flags.Set(t, "app.proxy_targets", []proxyPair{{Prefix: "", Target: backendTarget}})
	fwdOpt := GetForwardingServerOption()
	require.NotNil(t, fwdOpt, "forwarding must be enabled when app.proxy_targets is set")

	proxyLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	proxy := grpc.NewServer(fwdOpt)
	go func() { _ = proxy.Serve(proxyLis) }()
	t.Cleanup(proxy.Stop)
	proxyTarget := fmt.Sprintf("grpc://localhost:%d", proxyLis.Addr().(*net.TCPAddr).Port)

	clientConn, err := grpc_client.DialSimple(proxyTarget)
	require.NoError(t, err)
	t.Cleanup(func() { clientConn.Close() })

	ctx := metadata.AppendToOutgoingContext(context.Background(), clientHeader, "test-value")
	err = clientConn.Invoke(ctx, "/test.TestService/TestMethod", &emptypb.Empty{}, &emptypb.Empty{})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"test-value"}, gotMD.Get(clientHeader),
		"the client-supplied header must be forwarded through the proxy to the backend")
}

func TestGetConnectionPool_DedupesConcurrentDialsForSameTarget(t *testing.T) {
	t.Cleanup(cleanup)

	dial := func(_ string, _ ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
		// Sleep for a bit to increase the chance of race conditions.
		time.Sleep(10 * time.Millisecond)
		return &grpc_client.ClientConnPool{}, nil
	}

	pools := make([]*grpc_client.ClientConnPool, 10)
	var wg sync.WaitGroup
	for i := range pools {
		wg.Go(func() {
			p, err := getConnectionPool(dial, "remote.example.com")
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

func TestCtxWithClientIP(t *testing.T) {
	const clientIP = "1.2.3.4"
	const identity = "signed-grpc-proxy-header"

	t.Run("attaches client IP and identity", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx, err := ctxWithClientIP(ctxWithResolvedClientIP(clientIP), cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Equal(t, []string{identity}, md.Get(authutil.ClientIdentityHeaderName))
		// The proxy attests as the grpc-proxy identity.
		require.Equal(t, interfaces.ClientIdentityGRPCProxy, cis.lastClient)
	})

	t.Run("no client IP attaches nothing", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		ctx, err := ctxWithClientIP(context.Background(), cis)
		require.NoError(t, err)

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			require.Empty(t, md.Get(clientip.HeaderName))
			require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
		}
	})

	t.Run("client-supplied client IP header is overwritten with the resolved IP", func(t *testing.T) {
		cis := &fakeIdentityService{header: identity}
		// Simulate an attacker pre-setting the client-IP header to a spoofed value.
		ctx := metadata.AppendToOutgoingContext(ctxWithResolvedClientIP(clientIP), clientip.HeaderName, "9.9.9.9")
		ctx, err := ctxWithClientIP(ctx, cis)
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
		ctx := metadata.AppendToOutgoingContext(context.Background(), clientip.HeaderName, "9.9.9.9")
		ctx, err := ctxWithClientIP(ctx, cis)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Empty(t, md.Get(clientip.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("nil identity service sets IP without identity", func(t *testing.T) {
		ctx, err := ctxWithClientIP(ctxWithResolvedClientIP(clientIP), nil)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})
}
