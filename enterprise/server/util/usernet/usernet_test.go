package usernet

import (
	"context"
	"io"
	"net"
	"net/netip"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testnetworking"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/icmp"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
)

func TestContainerNetwork(t *testing.T) {
	testnetworking.Setup(t)
	// The test servers listen on the host's default IP, which may be private.
	flags.Set(t, "executor.task_allowed_private_ips", []string{"default"})
	ctx := context.Background()
	hostIP, err := networking.DefaultIP(ctx)
	require.NoError(t, err)

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

	t.Run("tcp", func(t *testing.T) {
		lis, err := net.Listen("tcp", net.JoinHostPort(hostIP.String(), "0"))
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

	t.Run("udp", func(t *testing.T) {
		pc, err := net.ListenPacket("udp", net.JoinHostPort(hostIP.String(), "0"))
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

	t.Run("icmp", func(t *testing.T) {
		var conn *icmp.PacketConn
		require.NoError(t, inNamespace(n.netns, func() error {
			var err error
			conn, err = icmp.ListenPacket("ip4:icmp", "0.0.0.0")
			return err
		}))
		defer conn.Close()
		_, err := conn.WriteTo(icmpEcho(header.ICMPv4Echo, 1, 1, []byte("ping")), &net.IPAddr{IP: hostIP})
		require.NoError(t, err)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 1500)
		size, _, err := conn.ReadFrom(buf)
		require.NoError(t, err)
		reply := header.ICMPv4(buf[:size])
		require.Equal(t, header.ICMPv4EchoReply, reply.Type())
		require.Equal(t, "ping", string(reply.Payload()))
	})

	t.Run("blocked private IP", func(t *testing.T) {
		_, err := dial("tcp", "10.0.0.1:80")
		require.ErrorIs(t, err, syscall.ECONNREFUSED)
	})

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
	} {
		addr := tcpip.AddrFrom4(netip.MustParseAddr(tc.addr).As4())
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
