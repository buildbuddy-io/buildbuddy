package interceptor

// These tests drive the interceptor's real gVisor stack with hand-built TCP
// SYN packets through a fake TUN device, which is as close to the real thing as
// we can get without CAP_NET_ADMIN: the packet plumbing, the promiscuous-mode
// accept, the held SYN, and the RST-on-failure path are all the production
// code paths.

import (
	"context"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/fakeip"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/tun"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
)

// fakeTUN is an in-memory tun.Device: packets written to `in` are delivered to
// the interceptor, and packets the interceptor sends appear on `out`.
type fakeTUN struct {
	in     chan []byte
	out    chan []byte
	events chan tun.Event
	closed chan struct{}
	once   sync.Once
}

func newFakeTUN() *fakeTUN {
	f := &fakeTUN{
		in:     make(chan []byte, 64),
		out:    make(chan []byte, 64),
		events: make(chan tun.Event, 4),
		closed: make(chan struct{}),
	}
	f.events <- tun.EventUp
	return f
}

func (f *fakeTUN) Read(bufs [][]byte, sizes []int, offset int) (int, error) {
	select {
	case pkt := <-f.in:
		sizes[0] = copy(bufs[0][offset:], pkt)
		return 1, nil
	case <-f.closed:
		return 0, os.ErrClosed
	}
}

func (f *fakeTUN) Write(bufs [][]byte, offset int) (int, error) {
	for _, b := range bufs {
		pkt := make([]byte, len(b)-offset)
		copy(pkt, b[offset:])
		select {
		case f.out <- pkt:
		case <-f.closed:
			return 0, os.ErrClosed
		}
	}
	return len(bufs), nil
}

func (f *fakeTUN) Name() (string, error)    { return "faketun0", nil }
func (f *fakeTUN) File() *os.File           { return nil }
func (f *fakeTUN) Events() <-chan tun.Event { return f.events }
func (f *fakeTUN) MTU() (int, error)        { return 1400, nil }
func (f *fakeTUN) BatchSize() int           { return 1 }
func (f *fakeTUN) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

// synPacket builds an IPv4 TCP SYN.
func synPacket(t testing.TB, src, dst netip.Addr, srcPort, dstPort uint16) []byte {
	t.Helper()
	total := header.IPv4MinimumSize + header.TCPMinimumSize
	buf := make([]byte, total)

	ip := header.IPv4(buf)
	ip.Encode(&header.IPv4Fields{
		TotalLength: uint16(total),
		TTL:         64,
		Protocol:    uint8(header.TCPProtocolNumber),
		SrcAddr:     tcpip.AddrFromSlice(src.AsSlice()),
		DstAddr:     tcpip.AddrFromSlice(dst.AsSlice()),
	})
	ip.SetChecksum(^ip.CalculateChecksum())

	tcpHdr := header.TCP(buf[header.IPv4MinimumSize:])
	tcpHdr.Encode(&header.TCPFields{
		SrcPort:    srcPort,
		DstPort:    dstPort,
		SeqNum:     12345,
		DataOffset: header.TCPMinimumSize,
		Flags:      header.TCPFlagSyn,
		WindowSize: 65535,
	})
	xsum := header.PseudoHeaderChecksum(header.TCPProtocolNumber, ip.SourceAddress(), ip.DestinationAddress(), header.TCPMinimumSize)
	tcpHdr.SetChecksum(^tcpHdr.CalculateChecksum(xsum))
	return buf
}

// awaitReply waits for a TCP segment from the interceptor and returns its flags.
func awaitReply(t testing.TB, f *fakeTUN) header.TCPFlags {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case pkt := <-f.out:
			if len(pkt) < header.IPv4MinimumSize {
				continue
			}
			ip := header.IPv4(pkt)
			if ip.Protocol() != uint8(header.TCPProtocolNumber) {
				continue
			}
			return header.TCP(pkt[ip.HeaderLength():]).Flags()
		case <-deadline:
			t.Fatal("timed out waiting for a TCP reply from the interceptor")
			return 0
		}
	}
}

// recordingDialer captures what the interceptor asked for, and returns either a
// live connection or an error.
type recordingDialer struct {
	mu     sync.Mutex
	zone   tunnelconfig.Zone
	host   string
	port   int
	called chan struct{}

	conn net.Conn
	err  error
}

func (d *recordingDialer) Dial(ctx context.Context, zone tunnelconfig.Zone, host string, port int) (net.Conn, error) {
	d.mu.Lock()
	d.zone, d.host, d.port = zone, host, port
	d.mu.Unlock()
	close(d.called)
	return d.conn, d.err
}

func setup(t *testing.T, dialer *recordingDialer) (*fakeTUN, *fakeip.Table) {
	t.Helper()
	cfg := &tunnelconfig.Config{
		FakeCIDR: "198.18.0.0/16",
		Zones: []tunnelconfig.Zone{{
			Suffix:    "bar.bb.internal",
			Gateway:   "grpcs://gateway.example",
			RewriteTo: "svc.cluster.local",
		}},
	}
	table, err := fakeip.NewTable(netip.MustParsePrefix(cfg.FakeCIDR))
	require.NoError(t, err)

	f := newFakeTUN()
	i, err := New(f, cfg, table, dialer)
	require.NoError(t, err)
	i.Start()
	t.Cleanup(i.Close)
	return f, table
}

func TestInterceptor_DialsTheNameBehindTheFakeIP(t *testing.T) {
	// The upstream end of the relayed connection.
	upstream, _ := net.Pipe()
	t.Cleanup(func() { upstream.Close() })

	dialer := &recordingDialer{called: make(chan struct{}), conn: upstream}
	f, table := setup(t, dialer)

	const name = "otel-collector.monitor-dev.bar.bb.internal"
	fake, err := table.Lookup(name)
	require.NoError(t, err)

	f.in <- synPacket(t, netip.MustParseAddr("198.18.0.1"), fake, 40000, 4317)

	select {
	case <-dialer.called:
	case <-time.After(20 * time.Second):
		t.Fatal("interceptor never dialed")
	}

	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	// The gateway is asked for the cluster's own name, not the fake IP and not
	// the cluster-qualified alias the developer typed.
	require.Equal(t, "otel-collector.monitor-dev.svc.cluster.local", dialer.host)
	require.Equal(t, 4317, dialer.port)
	require.Equal(t, "grpcs://gateway.example", dialer.zone.Gateway)

	// Only once the dial succeeded does the client see a SYN-ACK.
	require.Equal(t, header.TCPFlagSyn|header.TCPFlagAck, awaitReply(t, f))
}

func TestInterceptor_ResetsWhenTheDialFails(t *testing.T) {
	dialer := &recordingDialer{called: make(chan struct{}), err: os.ErrDeadlineExceeded}
	f, table := setup(t, dialer)

	fake, err := table.Lookup("broken.bar.bb.internal")
	require.NoError(t, err)

	f.in <- synPacket(t, netip.MustParseAddr("198.18.0.1"), fake, 40001, 8080)

	select {
	case <-dialer.called:
	case <-time.After(20 * time.Second):
		t.Fatal("interceptor never dialed")
	}

	// A failure has to surface as "connection refused", not as a connection
	// that opens and then dies — tools retry the latter and confuse users.
	flags := awaitReply(t, f)
	require.True(t, flags&header.TCPFlagRst != 0, "expected RST, got flags %s", flags)
	require.True(t, flags&header.TCPFlagSyn == 0, "connection should never have been accepted")
}

func TestInterceptor_ResetsUnknownFakeAddresses(t *testing.T) {
	dialer := &recordingDialer{called: make(chan struct{})}
	f, _ := setup(t, dialer)

	// An address inside the fake range that was never handed out by DNS.
	f.in <- synPacket(t, netip.MustParseAddr("198.18.0.1"), netip.MustParseAddr("198.18.99.99"), 40002, 22)

	flags := awaitReply(t, f)
	require.True(t, flags&header.TCPFlagRst != 0, "expected RST, got flags %s", flags)

	select {
	case <-dialer.called:
		t.Fatal("should not have dialed for an address DNS never allocated")
	default:
	}
}
