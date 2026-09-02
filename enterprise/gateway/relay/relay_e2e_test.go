package relay_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/relay"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/testgateway"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/relaywire"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

// setupRelayGateway creates a gateway running the relay service.
func setupRelayGateway(t testing.TB, ta *testauth.TestAuthenticator) *server.Gateway {
	t.Helper()
	return testgateway.Setup(t, ta, relay.New())
}

// startEchoServer starts a TCP server on localhost that echoes what it reads
// and then half-closes, standing in for a production service the gateway can
// reach. It returns its port.
func startEchoServer(t testing.TB) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				// io.Copy returns when the client half-closes, which is what
				// makes this a half-close test and not just an echo test.
				io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

// dialRelay opens a connection to the network's relay through the tunnel and
// performs the relay handshake for target.
func dialRelay(t testing.TB, peer testgateway.Peer, target string, port int) (net.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := peer.Net.DialContext(ctx, "tcp", fmt.Sprintf("[%s]:%d", peer.GatewayIP, relaywire.DefaultPort))
	require.NoError(t, err, "dialing the relay listener on the hub")
	if err := relaywire.Connect(conn, target, port); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func TestRelay_ConnectByName(t *testing.T) {
	echoPort := startEchoServer(t)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupRelayGateway(t, ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peer := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "")

	// "localhost" exercises the fact that the *gateway* resolves the name: the
	// peer's gVisor stack has no idea what localhost is.
	conn, err := dialRelay(t, peer, "localhost", echoPort)
	require.NoError(t, err)
	defer conn.Close()

	const want = "hello through the relay"
	_, err = io.WriteString(conn, want)
	require.NoError(t, err)

	// Half-close and read to EOF. If CloseWrite is not propagated across both
	// relay hops, the echo server never sees EOF and this read hangs.
	require.NoError(t, conn.(interface{ CloseWrite() error }).CloseWrite())
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	got, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

func TestRelay_PeerRemovalClosesConnection(t *testing.T) {
	// A client that vanishes (laptop asleep, network gone) never closes its
	// relay connection, and the gVisor side of the relay has no keepalive, so
	// the gateway removing the peer is the only signal the relay gets. Stand
	// in an upstream that never sends and never closes on its own: without
	// that signal, the relay would hold this connection open forever.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	upstreamDone := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer c.Close()
		_, err = c.Read(make([]byte, 1))
		upstreamDone <- err
	}()

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupRelayGateway(t, ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peer := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "")

	conn, err := dialRelay(t, peer, "localhost", ln.Addr().(*net.TCPAddr).Port)
	require.NoError(t, err)
	defer conn.Close()

	select {
	case err := <-upstreamDone:
		t.Fatalf("upstream connection ended before the peer was removed: %v", err)
	default:
	}

	// Drop the peer without closing the relay connection.
	peer.Disconnect()

	select {
	case err := <-upstreamDone:
		require.ErrorIs(t, err, io.EOF, "the relay should close its upstream connection")
	case <-time.After(30 * time.Second):
		t.Fatal("upstream connection was not closed after the peer was removed")
	}
}

func TestRelay_NotComposedMeansNoListener(t *testing.T) {
	// A gateway that does not compose the relay (the customer-facing shape)
	// must not have a relay listener at all.
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := testgateway.Setup(t, ta, server.DNSService())
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peer := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "")

	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := peer.Net.DialContext(dialCtx, "tcp", fmt.Sprintf("[%s]:%d", peer.GatewayIP, relaywire.DefaultPort))
	if err == nil {
		conn.Close()
		t.Fatal("expected no relay listener on a gateway that does not compose the relay")
	}
}

func TestRelay_TargetSuffixNotAllowed(t *testing.T) {
	flags.Set(t, "gateway.relay.allowed_target_suffixes", []string{"svc.cluster.local"})
	echoPort := startEchoServer(t)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupRelayGateway(t, ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peer := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "")

	_, err = dialRelay(t, peer, "localhost", echoPort)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "got %v", err)
	// The refusal names the target, so the developer knows what was judged
	// without reading gateway logs.
	require.Contains(t, status.Message(err), `"localhost"`)
}

func TestRelay_TargetRefusesConnection(t *testing.T) {
	// Bind and immediately close, so the port is (almost certainly) closed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupRelayGateway(t, ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peer := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "")

	_, err = dialRelay(t, peer, "localhost", deadPort)
	require.Error(t, err)
	require.True(t, status.IsUnavailableError(err), "got %v", err)
	require.Contains(t, status.Message(err), "refused", "the gateway's dial error reaches the client verbatim")
}
