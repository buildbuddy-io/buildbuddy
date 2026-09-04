package server_test

// End-to-end tests that bring up real userspace WireGuard tunnels and verify
// actual packet flow through the gateway.
//
// Each client (brought up by the testgateway package) runs a userspace
// WireGuard device backed by a gVisor network stack.  The gateway runs on
// localhost with a real UDP socket, so these tests exercise the full path:
//
//	gVisor TCP → WireGuard (client) → UDP → gateway WireGuard → muxTUN →
//	outbound queue → gateway WireGuard → UDP → WireGuard (peer) → gVisor TCP

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/testgateway"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

// TestEndToEnd_WatchReportsHandshake verifies that a Watch stream reports the
// watched peer's first completed WireGuard handshake. The fallback poll is
// set long so events can only arrive via push — the registration broadcast
// or the device-logger wake, whichever the handshake's timing exercises —
// never the poll.
func TestEndToEnd_WatchReportsHandshake(t *testing.T) {
	flags.Set(t, "gateway.watch_fallback_poll_interval", time.Minute)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := testgateway.Setup(t, ta, server.DNSService())

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	// Watch before the peer exists so the stream observes the full
	// lifecycle: registration first, then the handshake.
	sessionID := testgateway.NextSessionID()
	events, cancelWatch, _ := testgateway.StartWatch(t, gw, ctx, sessionID)
	defer cancelWatch()

	testgateway.RegisterAndConnectWithSessionID(t, gw, ctx, "net1", "peer-a", sessionID)

	// The peer's persistent keepalive triggers the handshake; the watch must
	// eventually report a peer with a handshake time.
	deadline := time.After(30 * time.Second)
	for {
		select {
		case rsp := <-events:
			if rsp.GetPeer() == nil {
				continue // heartbeat
			}
			require.Equal(t, sessionID, rsp.GetPeer().GetSessionId())
			if rsp.GetPeer().GetLastHandshakeTime() != nil {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for Watch to report a completed handshake")
		}
	}
}

// TestEndToEnd_PeersCanCommunicate verifies that two peers in the same network
// can exchange data over the WireGuard tunnel using direct IP addressing.
func TestEndToEnd_PeersCanCommunicate(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := testgateway.Setup(t, ta, server.DNSService())

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peerA := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "peer-a")
	peerB := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "peer-b")

	const port = 9999
	ln, err := peerA.Net.ListenTCP(&net.TCPAddr{Port: port})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	const want = "hello through WireGuard"

	// peer-b dials peer-a by tunnel IP. The first SYN triggers a WireGuard
	// handshake; allow up to 30 s for it to complete.
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connB, err := peerB.Net.DialContext(dialCtx, "tcp", fmt.Sprintf("[%s]:%d", peerA.Addr, port))
	require.NoError(t, err)
	defer connB.Close()

	go func() {
		fmt.Fprint(connB, want)
		connB.Close()
	}()

	connA, err := ln.Accept()
	require.NoError(t, err)
	defer connA.Close()

	got, err := io.ReadAll(connA)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

// TestEndToEnd_DNSResolution verifies that a peer can resolve another peer's
// registered name via the gateway's per-network DNS server and connect using
// that name.
func TestEndToEnd_DNSResolution(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := testgateway.Setup(t, ta, server.DNSService())

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peerA := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "myvm")
	peerB := testgateway.RegisterAndConnect(t, gw, ctx, "net1", "")

	const port = 9998
	ln, err := peerA.Net.ListenTCP(&net.TCPAddr{Port: port})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	const want = "hello by DNS name"

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Dial peer-a by DNS name. The gVisor stack sends a DNS query to the
	// gateway hub IP (configured as the resolver in CreateNetTUN), which
	// routes it to serveDNS via injectInbound.
	connB, err := peerB.Net.DialContext(dialCtx, "tcp", fmt.Sprintf("%s:%d", peerA.AssignedName, port))
	require.NoError(t, err)
	defer connB.Close()

	go func() {
		fmt.Fprint(connB, want)
		connB.Close()
	}()

	connA, err := ln.Accept()
	require.NoError(t, err)
	defer connA.Close()

	got, err := io.ReadAll(connA)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

// BenchmarkGatewayThroughput measures TCP throughput between two peers routed
// through the gateway. Each iteration sends a 64 KiB chunk from peer-b to
// peer-a and waits for peer-a to echo it back, so b.SetBytes reports
// one-way bytes per second.
//
// Run with:
//
//	bazel test //enterprise/gateway/server:server_test \
//	  --test_arg=-test.bench=BenchmarkGatewayThroughput \
//	  --test_arg=-test.benchtime=10s \
//	  --test_arg=-test.run='^$'
func BenchmarkGatewayThroughput(b *testing.B) {
	ta := testauth.NewTestAuthenticator(b, testauth.TestUsers("user1", "group1"))
	gw := testgateway.Setup(b, ta, server.DNSService())

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(b, err)

	peerA := testgateway.RegisterAndConnect(b, gw, ctx, "net1", "server")
	peerB := testgateway.RegisterAndConnect(b, gw, ctx, "net1", "client")

	const port = 9997
	ln, err := peerA.Net.ListenTCP(&net.TCPAddr{Port: port})
	require.NoError(b, err)
	b.Cleanup(func() { ln.Close() })

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connB, err := peerB.Net.DialContext(dialCtx, "tcp", fmt.Sprintf("[%s]:%d", peerA.Addr, port))
	require.NoError(b, err)
	b.Cleanup(func() { connB.Close() })

	connA, err := ln.Accept()
	require.NoError(b, err)
	b.Cleanup(func() { connA.Close() })

	const chunkSize = 64 * 1024
	b.SetBytes(chunkSize)

	// peer-a echoes every chunk it receives.
	go func() {
		buf := make([]byte, chunkSize)
		for {
			if _, err := io.ReadFull(connA, buf); err != nil {
				return
			}
			if _, err := connA.Write(buf); err != nil {
				return
			}
		}
	}()

	payload := make([]byte, chunkSize)
	recv := make([]byte, chunkSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := connB.Write(payload); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := io.ReadFull(connB, recv); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
	b.StopTimer()
}
