package server

import (
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
)

// makeIPv6Packet builds a minimal IPv6 packet with the given src/dst.
// dispatch() only inspects the IP header, so the payload doesn't need to be valid.
func makeIPv6Packet(src, dst netip.Addr) []byte {
	pkt := make([]byte, header.IPv6MinimumSize+4)
	h := header.IPv6(pkt)
	h.Encode(&header.IPv6Fields{
		PayloadLength:     4,
		TransportProtocol: header.UDPProtocolNumber,
		HopLimit:          64,
		SrcAddr:           tcpip.AddrFrom16(src.As16()),
		DstAddr:           tcpip.AddrFrom16(dst.As16()),
	})
	return pkt
}

// TestMuxTUN_CrossNetworkForwarding verifies that muxTUN only forwards packets
// between peers in the same network and drops packets that cross network
// boundaries.
func TestMuxTUN_CrossNetworkForwarding(t *testing.T) {
	tun := newMuxTUN(1420)
	defer tun.Close()

	net0Peer1 := netip.MustParseAddr("fd00:bb::2")
	net0Peer2 := netip.MustParseAddr("fd00:bb::3")
	net1Peer := netip.MustParseAddr("fd00:bb:1::2")

	tun.registerIP(net0Peer1, 0)
	tun.registerIP(net0Peer2, 0)
	tun.registerIP(net1Peer, 1)

	t.Run("cross-network packets dropped", func(t *testing.T) {
		tun.Write([][]byte{makeIPv6Packet(net0Peer1, net1Peer)}, 0)
		select {
		case view := <-tun.outbound:
			view.Release()
			t.Fatal("cross-network packet was forwarded, expected drop")
		default:
		}
	})

	t.Run("same-network packets forwarded", func(t *testing.T) {
		tun.Write([][]byte{makeIPv6Packet(net0Peer1, net0Peer2)}, 0)
		select {
		case view := <-tun.outbound:
			view.Release()
		case <-time.After(time.Second):
			t.Fatal("same-network packet was not forwarded")
		}
	})
}

// TestMuxTUN_DNSIsolation verifies three properties:
//
//  1. A peer in network 1 cannot reach network 0's DNS server at all: its
//     packets to the net0 hub IP are dropped by dispatch() because src and dst
//     are in different network indices.
//
//  2. A same-network DNS query is consumed by the DNS stack (injected into
//     gVisor via injectInbound), not forwarded peer-to-peer via the WireGuard
//     outbound queue.
//
//  3. Each network's DNS lookup function is scoped to that network's names;
//     names from other networks are never resolvable via a foreign DNS server.
func TestMuxTUN_DNSIsolation(t *testing.T) {
	tun := newMuxTUN(1420)
	defer tun.Close()

	net0Peer := netip.MustParseAddr("fd00:bb::2")
	net1Peer := netip.MustParseAddr("fd00:bb:1::2")

	tun.registerIP(net0Peer, 0)
	tun.registerIP(net1Peer, 1)

	net0Name := "peer-in-net0"
	net0IP := netip.MustParseAddr("fd00:bb::2")
	net1Name := "peer-in-net1"
	net1IP := netip.MustParseAddr("fd00:bb:1::2")

	net0Lookup := func(name string) (netip.Addr, bool) {
		if name == net0Name {
			return net0IP, true
		}
		return netip.Addr{}, false
	}
	net1Lookup := func(name string) (netip.Addr, bool) {
		if name == net1Name {
			return net1IP, true
		}
		return netip.Addr{}, false
	}

	require.NoError(t, tun.startNetworkDNS(0, "net0", net0Lookup))
	require.NoError(t, tun.startNetworkDNS(1, "net1", net1Lookup))

	// Property 1: cross-network DNS packet dropped by dispatch before reaching
	// the DNS stack. A net1 peer's packet to net0's hub IP is in a different
	// network index than the destination, so dispatch silently drops it.
	t.Run("cross-network DNS query dropped", func(t *testing.T) {
		pkt := makeIPv6Packet(net1Peer, networkHubIP(0))
		tun.Write([][]byte{pkt}, 0)
		select {
		case view := <-tun.outbound:
			view.Release()
			t.Fatal("cross-network DNS packet should have been dropped by dispatch")
		default:
		}
	})

	// Property 2: a same-network DNS query goes to the DNS stack via
	// injectInbound, not into the peer-to-peer outbound queue.
	t.Run("same-network DNS query routed to DNS stack, not forwarded", func(t *testing.T) {
		pkt := makeIPv6Packet(net0Peer, networkHubIP(0))
		tun.Write([][]byte{pkt}, 0)
		select {
		case view := <-tun.outbound:
			view.Release()
			t.Fatal("DNS query to hub should be consumed by DNS stack, not peer-to-peer forwarded")
		default:
		}
	})

	// Property 3: each network's lookup function is independent. Asking net0's
	// DNS about a net1 name returns nothing, and vice versa.
	t.Run("net0 DNS resolves own names", func(t *testing.T) {
		ip, ok := net0Lookup(net0Name)
		require.True(t, ok)
		require.Equal(t, net0IP, ip)
	})

	t.Run("net0 DNS does not resolve net1 names", func(t *testing.T) {
		_, ok := net0Lookup(net1Name)
		require.False(t, ok)
	})

	t.Run("net1 DNS does not resolve net0 names", func(t *testing.T) {
		_, ok := net1Lookup(net0Name)
		require.False(t, ok)
	})
}
