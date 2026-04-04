package server

// End-to-end tests that bring up real userspace WireGuard tunnels and verify
// actual packet flow through the gateway.
//
// Each client uses golang.zx2c4.com/wireguard/tun/netstack to run a userspace
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
	"net/netip"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/keys"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

// tunnelPeer holds the gVisor netstack and assigned tunnel address for a
// registered WireGuard peer.
type tunnelPeer struct {
	net          *netstack.Net
	addr         netip.Addr
	assignedName string
}

// registerAndConnect registers a new peer with the gateway and brings up a
// userspace WireGuard tunnel for it. The tunnel is closed when the test ends.
//
// persistent_keepalive_interval=1 is used so that the first outbound packet
// triggers an immediate WireGuard handshake rather than waiting for the
// gateway to initiate one.
func registerAndConnect(t testing.TB, gw *Gateway, ctx context.Context, networkName, peerName string) tunnelPeer {
	t.Helper()
	privKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)

	resp, err := gw.Register(ctx, &gwpb.RegisterRequest{
		NetworkName: networkName,
		PeerName:    peerName,
		PublicKey:   privKey.PublicKey().Hex(),
	})
	require.NoError(t, err)

	addr := netip.MustParseAddr(resp.GetAssignedIp())
	tunDev, tnet, err := netstack.CreateNetTUN(
		[]netip.Addr{addr},
		// Use the gateway's hub IP as the DNS resolver so peer names
		// registered with peer_name are resolvable as <name>.internal.
		[]netip.Addr{netip.MustParseAddr(resp.GetGatewayIp())},
		1420,
	)
	require.NoError(t, err)

	logger := &device.Logger{
		Verbosef: func(format string, args ...any) {},
		Errorf:   func(format string, args ...any) { t.Logf("wg: "+format, args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), logger)
	t.Cleanup(dev.Close)

	ipc := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=1\n",
		privKey.Hex(), resp.GetServerPublicKey(), resp.GetNetworkCidr(), resp.GetServerEndpoint(),
	)
	require.NoError(t, dev.IpcSet(ipc))
	require.NoError(t, dev.Up())

	return tunnelPeer{net: tnet, addr: addr, assignedName: resp.GetAssignedPeerName()}
}

// TestEndToEnd_PeersCanCommunicate verifies that two peers in the same network
// can exchange data over the WireGuard tunnel using direct IP addressing.
func TestEndToEnd_PeersCanCommunicate(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peerA := registerAndConnect(t, gw, ctx, "net1", "peer-a")
	peerB := registerAndConnect(t, gw, ctx, "net1", "peer-b")

	const port = 9999
	ln, err := peerA.net.ListenTCP(&net.TCPAddr{Port: port})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	const want = "hello through WireGuard"

	// peer-b dials peer-a by tunnel IP. The first SYN triggers a WireGuard
	// handshake; allow up to 30 s for it to complete.
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connB, err := peerB.net.DialContext(dialCtx, "tcp", fmt.Sprintf("[%s]:%d", peerA.addr, port))
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
// that name (<name>.internal).
func TestEndToEnd_DNSResolution(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	peerA := registerAndConnect(t, gw, ctx, "net1", "myvm")
	peerB := registerAndConnect(t, gw, ctx, "net1", "")

	const port = 9998
	ln, err := peerA.net.ListenTCP(&net.TCPAddr{Port: port})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	const want = "hello by DNS name"

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Dial peer-a by DNS name. The gVisor stack sends a DNS query to the
	// gateway hub IP (configured as the resolver in CreateNetTUN), which
	// routes it to serveDNS via injectInbound.
	connB, err := peerB.net.DialContext(dialCtx, "tcp", fmt.Sprintf("%s.internal:%d", peerA.assignedName, port))
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
	gw := setupGateway(b, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(b, err)

	peerA := registerAndConnect(b, gw, ctx, "net1", "server")
	peerB := registerAndConnect(b, gw, ctx, "net1", "client")

	const port = 9997
	ln, err := peerA.net.ListenTCP(&net.TCPAddr{Port: port})
	require.NoError(b, err)
	b.Cleanup(func() { ln.Close() })

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connB, err := peerB.net.DialContext(dialCtx, "tcp", fmt.Sprintf("[%s]:%d", peerA.addr, port))
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
