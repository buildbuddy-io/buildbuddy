package usernet

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testnetworking"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"golang.org/x/net/icmp"
	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
)

func TestContainerNetwork(t *testing.T) {
	testnetworking.Setup(t)
	// The test servers listen on the host's default IP, which may be private.
	flags.Set(t, "executor.task_allowed_private_ips", []string{"default"})
	ctx := context.Background()
	hostIP4, err := networking.DefaultIP(ctx)
	require.NoError(t, err)

	type family struct {
		// unreachable has no route on the host.
		host, gateway, blocked, unreachable        net.IP
		echoReply, dstUnreachable, portUnreachable uint8
	}
	families := []family{{
		hostIP4, netip.MustParsePrefix(containerGatewayCIDR).Addr().AsSlice(), net.ParseIP("10.0.0.1"), net.ParseIP("203.0.113.1"),
		uint8(header.ICMPv4EchoReply), uint8(header.ICMPv4DstUnreachable), uint8(header.ICMPv4PortUnreachable),
	}}
	if _, err := os.Stat("/proc/sys/net/ipv6"); err == nil {
		families = append(families, family{
			// The host may have no IPv6 address, so the test adds one.
			net.ParseIP("2001:db8::1"), gateway6.Addr().AsSlice(), net.ParseIP("fc00::1"), net.ParseIP("2001:db8:ffff::1"),
			uint8(header.ICMPv6EchoReply), uint8(header.ICMPv6DstUnreachable), uint8(header.ICMPv6PortUnreachable),
		})
	} else {
		t.Log("IPv6 is disabled on the host; skipping IPv6 cases.")
	}
	// Replacing tolerates leftovers from an interrupted run.
	lo, err := netlink.LinkByName("lo")
	require.NoError(t, err)
	for _, f := range families {
		bits := len(f.host) * 8
		if f.host.To4() == nil {
			addr := &netlink.Addr{IPNet: &net.IPNet{IP: f.host, Mask: net.CIDRMask(bits, bits)}}
			require.NoError(t, netlink.AddrReplace(lo, addr))
			t.Cleanup(func() { netlink.AddrDel(lo, addr) })
		}
		route := &netlink.Route{Dst: &net.IPNet{IP: f.unreachable, Mask: net.CIDRMask(bits, bits)}, Type: unix.RTN_UNREACHABLE}
		require.NoError(t, netlink.RouteReplace(route))
		t.Cleanup(func() { netlink.RouteDel(route) })
	}

	n, err := NewContainerNetwork(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Cleanup(ctx)) })

	// dial connects from inside the container's net namespace.
	dial := func(network, address string) (net.Conn, error) {
		var conn net.Conn
		err := inNamespace(n.netns, func() error {
			var err error
			conn, err = net.DialTimeout(network, address, 5*time.Second)
			return err
		})
		return conn, err
	}
	// ping sends an echo request from the container and returns the ICMP
	// messages that arrive within half a second, other than neighbor
	// discovery.
	ping := func(t *testing.T, dst net.IP) [][]byte {
		network, listenAddr, echo := "ip4:icmp", "0.0.0.0", uint8(header.ICMPv4Echo)
		if dst.To4() == nil {
			network, listenAddr, echo = "ip6:ipv6-icmp", "::", uint8(header.ICMPv6EchoRequest)
		}
		var conn *icmp.PacketConn
		require.NoError(t, inNamespace(n.netns, func() error {
			var err error
			conn, err = icmp.ListenPacket(network, listenAddr)
			return err
		}))
		defer conn.Close()
		_, err := conn.WriteTo(icmpEcho(echo, 1, 1, []byte("ping")), &net.IPAddr{IP: dst})
		require.NoError(t, err)
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var msgs [][]byte
		for {
			buf := make([]byte, 1500)
			size, _, err := conn.ReadFrom(buf)
			if err != nil {
				require.ErrorIs(t, err, os.ErrDeadlineExceeded)
				return msgs
			}
			if dst.To4() == nil && buf[0] >= uint8(header.ICMPv6RouterSolicit) {
				continue
			}
			msgs = append(msgs, buf[:size])
		}
	}
	// typeCodes returns the type and code of each ICMP message.
	typeCodes := func(msgs [][]byte) [][2]uint8 {
		var tc [][2]uint8
		for _, m := range msgs {
			tc = append(tc, [2]uint8{m[0], m[1]})
		}
		return tc
	}

	for _, f := range families {
		t.Run(f.host.String()+"/tcp", func(t *testing.T) {
			lis, err := net.Listen("tcp", net.JoinHostPort(f.host.String(), "0"))
			require.NoError(t, err)
			defer lis.Close()
			go func() {
				conn, err := lis.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				io.Copy(conn, conn)
			}()

			conn, err := dial("tcp", lis.Addr().String())
			require.NoError(t, err)
			defer conn.Close()
			msg := make([]byte, 1<<20)
			go conn.Write(msg)
			_, err = io.ReadFull(conn, make([]byte, len(msg)))
			require.NoError(t, err)
		})

		t.Run(f.host.String()+"/udp", func(t *testing.T) {
			pc, err := net.ListenPacket("udp", net.JoinHostPort(f.host.String(), "0"))
			require.NoError(t, err)
			defer pc.Close()
			go func() {
				buf := make([]byte, 1500)
				for {
					size, addr, err := pc.ReadFrom(buf)
					if err != nil {
						return
					}
					pc.WriteTo(buf[:size], addr)
				}
			}()

			conn, err := dial("udp", pc.LocalAddr().String())
			require.NoError(t, err)
			defer conn.Close()
			_, err = conn.Write([]byte("hello"))
			require.NoError(t, err)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 1500)
			size, err := conn.Read(buf)
			require.NoError(t, err)
			require.Equal(t, "hello", string(buf[:size]))
		})

		t.Run(f.host.String()+"/icmp", func(t *testing.T) {
			msgs := ping(t, f.host)
			require.Equal(t, [][2]uint8{{f.echoReply, 0}}, typeCodes(msgs))
			require.Equal(t, "ping", string(msgs[0][header.ICMPv4MinimumSize:]))
		})

		t.Run(f.host.String()+"/icmp over limit", func(t *testing.T) {
			for range maxPingsInFlight {
				n.pings <- struct{}{}
			}
			require.Empty(t, ping(t, f.host))
			for range maxPingsInFlight {
				<-n.pings
			}
			require.Equal(t, [][2]uint8{{f.echoReply, 0}}, typeCodes(ping(t, f.host)))
		})

		t.Run(f.gateway.String()+"/icmp to gateway", func(t *testing.T) {
			require.Equal(t, [][2]uint8{{f.echoReply, 0}}, typeCodes(ping(t, f.gateway)))
		})

		for _, dst := range []net.IP{f.blocked, f.unreachable} {
			t.Run(dst.String()+"/rejected", func(t *testing.T) {
				_, err := dial("tcp", net.JoinHostPort(dst.String(), "80"))
				require.ErrorIs(t, err, syscall.ECONNREFUSED)
				conn, err := dial("udp", net.JoinHostPort(dst.String(), "80"))
				require.NoError(t, err)
				defer conn.Close()
				_, err = conn.Write([]byte("hello"))
				require.NoError(t, err)
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, err = conn.Read(make([]byte, 1500))
				require.ErrorIs(t, err, syscall.ECONNREFUSED)
				require.Equal(t, [][2]uint8{{f.dstUnreachable, f.portUnreachable}}, typeCodes(ping(t, dst)))
			})
		}
	}

	stats, err := n.Stats(ctx)
	require.NoError(t, err)
	require.Greater(t, stats.GetBytesSent(), int64(1<<20))
	require.Greater(t, stats.GetBytesReceived(), int64(1<<20))
}

func TestIsAllowed(t *testing.T) {
	flags.Set(t, "executor.task_allowed_private_ips", []string{"10.1.2.3"})
	allowed, err := networking.TaskAllowedPrivateIPs(context.Background())
	require.NoError(t, err)
	n := &Network{externalNetwork: true, allowedPrefixes: allowed}
	local := &Network{externalNetwork: false}
	for _, tc := range []struct {
		addr    string
		allowed bool
	}{
		{"8.8.8.8", true},
		{"10.1.2.3", true},
		{"10.1.2.4", false},
		{"192.168.1.1", false},
		{"169.254.169.254", false},
		{"127.0.0.1", false},
		{"224.0.0.1", false},
		{"255.255.255.255", false},
		{"2001:4860:4860::8888", true},
		{"::ffff:10.1.2.3", true},
		{"::ffff:10.1.2.4", false},
		{"::1", false},
		{"fd20:ce::254", false},
		{"fe80::1", false},
		{"ff02::1", false},
		{"::a00:1", false},
		{"64:ff9b::a9fe:a9fe", false},
		{"64:ff9b:1::1", false},
		{"2001:0:4136:e378::1", false},
		{"2002:a00:1::1", false},
	} {
		addr := tcpip.AddrFromSlice(netip.MustParseAddr(tc.addr).AsSlice())
		require.Equal(t, tc.allowed, n.isAllowed(addr), tc.addr)
		require.False(t, local.isAllowed(addr), tc.addr)
	}
}

func TestCleanupClosesForwardedConnections(t *testing.T) {
	testnetworking.Setup(t)
	flags.Set(t, "executor.task_allowed_private_ips", []string{"default"})
	ctx := context.Background()
	hostIP, err := networking.DefaultIP(ctx)
	require.NoError(t, err)
	lis, err := net.Listen("tcp", net.JoinHostPort(hostIP.String(), "0"))
	require.NoError(t, err)
	defer lis.Close()

	n, err := NewContainerNetwork(ctx)
	require.NoError(t, err)
	var guest net.Conn
	require.NoError(t, inNamespace(n.netns, func() error {
		var err error
		guest, err = net.DialTimeout("tcp", lis.Addr().String(), 5*time.Second)
		return err
	}))
	defer guest.Close()
	remote, err := lis.Accept()
	require.NoError(t, err)
	defer remote.Close()

	require.NoError(t, n.Cleanup(ctx))
	remote.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = remote.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestCleanupStopsGoroutines(t *testing.T) {
	testnetworking.Setup(t)
	// fdbased only starts processor goroutines when GOMAXPROCS > 1.
	prev := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	ctx := context.Background()
	goroutines := func() int {
		var b strings.Builder
		pprof.Lookup("goroutine").WriteTo(&b, 2)
		n := 0
		for g := range strings.SplitSeq(b.String(), "\n\n") {
			if strings.Contains(g, "gvisor.dev/gvisor/") {
				n++
			}
		}
		return n
	}
	before := goroutines()
	for range 3 {
		n, err := NewContainerNetwork(ctx)
		require.NoError(t, err)
		require.NoError(t, n.Cleanup(ctx))
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.LessOrEqual(c, goroutines(), before, "goroutines running gVisor code")
	}, 5*time.Second, 10*time.Millisecond)
}

func TestOneWayUDPFlowStaysOpen(t *testing.T) {
	testnetworking.Setup(t)
	flags.Set(t, "executor.task_allowed_private_ips", []string{"default"})
	idleTimeout := udpIdleTimeout
	udpIdleTimeout = 500 * time.Millisecond
	t.Cleanup(func() { udpIdleTimeout = idleTimeout })
	ctx := context.Background()
	hostIP, err := networking.DefaultIP(ctx)
	require.NoError(t, err)
	pc, err := net.ListenPacket("udp", net.JoinHostPort(hostIP.String(), "0"))
	require.NoError(t, err)
	defer pc.Close()

	n, err := NewContainerNetwork(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Cleanup(ctx)) })
	var conn net.Conn
	require.NoError(t, inNamespace(n.netns, func() error {
		var err error
		conn, err = net.Dial("udp", pc.LocalAddr().String())
		return err
	}))
	defer conn.Close()

	// Send for three idle timeouts without any replies. Each packet should
	// arrive from the same host port, i.e. the flow isn't recreated.
	var sources []string
	for range 12 {
		_, err := conn.Write([]byte("ping"))
		require.NoError(t, err)
		pc.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, addr, err := pc.ReadFrom(make([]byte, 16))
		require.NoError(t, err)
		sources = append(sources, addr.String())
		time.Sleep(udpIdleTimeout / 4)
	}
	require.Len(t, slices.Compact(sources), 1)
}
