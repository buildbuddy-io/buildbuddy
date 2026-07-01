package grpc_forward

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

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

// TestDirector_ForwardsIncomingMetadataToOutgoingContext tests that director()
// copies incoming gRPC metadata into the outgoing backend call context for
// forwarded unknown RPCs.
func TestDirector_ForwardsIncomingMetadataToOutgoingContext(t *testing.T) {
	t.Cleanup(cleanup)
	flags.Set(t, "app.proxy_targets", []proxyPair{
		{Prefix: "", Target: "grpc://localhost:1985"},
	})

	const header1 = "x-test-client-header-1"
	const header2 = "x-test-client-header-2"
	incoming := metadata.New(map[string]string{
		header1: "value-1",
		header2: "value-2",
	})
	ctx := metadata.NewIncomingContext(context.Background(), incoming)

	outCtx, cc, err := director(ctx, "/test.TestService/TestMethod")
	require.NoError(t, err)
	require.NotNil(t, cc)

	outgoing, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "director must attach the incoming metadata to the outgoing context")
	require.Equal(t, []string{"value-1"}, outgoing.Get(header1),
		"client-supplied headers must be forwarded to the backend for unknown RPCs")
	require.Equal(t, []string{"value-2"}, outgoing.Get(header2))
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
