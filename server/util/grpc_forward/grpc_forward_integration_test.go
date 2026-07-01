package grpc_forward_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_forward"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestForwarding_PropagatesClientHeadersToBackend drives the real forwarding
// handler end-to-end: a client calls an RPC the proxy does not serve locally,
// the proxy (via GetForwardingServerOption) transparently forwards it, and we
// assert the client's headers arrive at the backend over the wire. This is the
// wire-level counterpart to the unit test on director() and reproduces the
// incident's exact path (a workflow setting
// --remote_exec_header=...container-registry-password=... through the proxy).
func TestForwarding_PropagatesClientHeadersToBackend(t *testing.T) {
	const pwHeader = "x-buildbuddy-platform.container-registry-password"

	// Start a backend gRPC server that accepts any RPC via an unknown-service
	// handler and records the incoming metadata it observes.
	backendLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var gotMD metadata.MD
	backendHandler := func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		mu.Lock()
		gotMD = md.Copy()
		mu.Unlock()
		// Drain the request and send an empty reply so the unary call the
		// client makes below completes cleanly.
		_ = stream.RecvMsg(&emptypb.Empty{})
		return stream.SendMsg(&emptypb.Empty{})
	}
	backend := grpc.NewServer(grpc.UnknownServiceHandler(backendHandler))
	go func() { _ = backend.Serve(backendLis) }()
	t.Cleanup(backend.Stop)
	backendTarget := fmt.Sprintf("grpc://localhost:%d", backendLis.Addr().(*net.TCPAddr).Port)

	// Point the forwarder at the backend and build a proxy gRPC server that
	// uses the real forwarding handler.
	flags.Set(t, "app.proxy_targets", []grpc_forward.ProxyTarget{{Prefix: "", Target: backendTarget}})
	fwdOpt := grpc_forward.GetForwardingServerOption()
	require.NotNil(t, fwdOpt, "forwarding must be enabled when app.proxy_targets is set")

	proxyLis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	proxy := grpc.NewServer(fwdOpt)
	go func() { _ = proxy.Serve(proxyLis) }()
	t.Cleanup(proxy.Stop)
	proxyTarget := fmt.Sprintf("grpc://localhost:%d", proxyLis.Addr().(*net.TCPAddr).Port)

	// Call an RPC the proxy does not serve locally (Execute is an "unknown"
	// service to the proxy, so it hits the forwarding handler) carrying a
	// client-supplied auth header.
	clientConn, err := grpc_client.DialSimple(proxyTarget)
	require.NoError(t, err)
	t.Cleanup(func() { clientConn.Close() })

	ctx := metadata.AppendToOutgoingContext(context.Background(), pwHeader, "hunter2")
	err = clientConn.Invoke(ctx, "/build.bazel.remote.execution.v2.Execution/Execute", &emptypb.Empty{}, &emptypb.Empty{})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"hunter2"}, gotMD.Get(pwHeader),
		"the client-supplied container-registry-password header must be forwarded through the proxy to the backend")
}
