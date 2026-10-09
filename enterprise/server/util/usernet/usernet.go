// Package usernet provides container and VM networking through a userspace
// (gVisor) network stack instead of host routes, NAT and iptables rules.
//
// Each network gets its own net namespace, where the stack attaches to a device
// through a tap device that a VMM opens, or one end of a veth pair whose other
// end is a container's eth0. The stack acts as the guest's gateway and forwards
// guest TCP and UDP flows and ICMP echo requests using sockets opened by the
// executor.
//
// This is heavily inspired by gvisor-tap-vsock. It isn't used directly because
// it pins a newer gVisor than we can build, and doesn't allow our private IP
// policy or TCP tuning:
// https://github.com/containers/gvisor-tap-vsock/tree/ad36eb20acfae43f5df9f0807201f5059073b881/pkg/services/forwarder
//
// N.B. Forwarding pings requires the executor's group to be in
// net.ipv4.ping_group_range.
//
// This is separate from the networking package so that the guest init binary,
// which depends on networking, doesn't link all of gVisor.
package usernet

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/vishvananda/netlink"
	"golang.org/x/net/icmp"
	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/checksum"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/tcpip/network/arp"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gicmp "gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
)

const (
	// ContainerIP is the address of a container's eth0. (This matches slirp4netns)
	ContainerIP          = "10.0.2.100"
	containerGatewayCIDR = "10.0.2.2/24"
	containerDevice      = "eth0"

	// stackDevice is the end of a container's veth pair that the gvisor netstack uses.
	stackDevice = "usernet0"

	nicID = 1
	mtu   = 1500

	// gsoMaxSize lets segmentation-offloaded frames through whole, which is
	// much cheaper than exchanging MTU-sized frames.
	gsoMaxSize = 65536

	// Socket buffers must hold bursts of offloaded frames from concurrent
	// flows; the ~208 KiB default holds only a few.
	socketBufferSize = 16 << 20
	tcpBufferSize    = 8 << 20
	relayBufferSize  = 1 << 20

	dialTimeout = 30 * time.Second
	pingTimeout = 5 * time.Second

	// maxPingsInFlight caps the ping sockets open for a guest; echo requests
	// over the limit are dropped.
	maxPingsInFlight = 128
)

// udpIdleTimeout is how long a UDP flow may go without traffic in either
// direction before it's closed. It's a var so that tests can shorten it; each
// Network copies it when created.
var udpIdleTimeout = time.Minute

// gatewayLinkAddress is the stack's MAC address.
var gatewayLinkAddress = tcpip.LinkAddress("\x02\x00\x00\x00\x00\x01")

// blockedPrefixes are unreachable unless allowed by
// --executor.task_allowed_private_ips.
var blockedPrefixes = func() []netip.Prefix {
	var prefixes []netip.Prefix
	for _, r := range append(slices.Clone(networking.PrivateIPRanges), "0.0.0.0/8", "127.0.0.0/8", "224.0.0.0/4", "255.255.255.255/32") {
		prefixes = append(prefixes, netip.MustParsePrefix(r))
	}
	return prefixes
}()

// Network is a net namespace whose traffic is served by a userspace stack.
type Network struct {
	netns *networking.Namespace
	fd    int
	stack *stack.Stack
	// cancel closes connections opened on behalf of the guest.
	cancel context.CancelFunc

	gateway         tcpip.Address
	externalNetwork bool
	allowedPrefixes []netip.Prefix
	pings           chan struct{}
	udpIdleTimeout  time.Duration
}

// NewVMNetwork creates a net namespace containing tapDeviceName for a VMM to
// open, and serves the VM's gateway at gatewayCIDR (e.g. "192.168.241.1/29").
// Without external networking, only the gateway is reachable.
func NewVMNetwork(ctx context.Context, tapDeviceName, gatewayCIDR string, enableExternalNetworking bool) (*Network, error) {
	return newNetwork(ctx, gatewayCIDR, enableExternalNetworking, tapDeviceName, func() error {
		tap := &netlink.Tuntap{Name: tapDeviceName, Mode: netlink.TUNTAP_MODE_TAP, Flags: netlink.TUNTAP_NO_PI}
		if err := netlink.LinkAdd(tap); err != nil {
			return err
		}
		// The tap is persistent; the VMM opens it by name.
		for _, f := range tap.Fds {
			f.Close()
		}
		return netlink.LinkSetUp(tap)
	})
}

// NewContainerNetwork creates a net namespace for a container to join, with an
// eth0 at ContainerIP and a default route through the stack.
func NewContainerNetwork(ctx context.Context) (*Network, error) {
	gateway := netip.MustParsePrefix(containerGatewayCIDR)
	return newNetwork(ctx, containerGatewayCIDR, true /*=enableExternalNetworking*/, stackDevice, func() error {
		veth := &netlink.Veth{Name: containerDevice, PeerName: stackDevice}
		if err := netlink.LinkAdd(veth); err != nil {
			return err
		}
		for _, name := range []string{"lo", stackDevice, containerDevice} {
			link, err := netlink.LinkByName(name)
			if err != nil {
				return err
			}
			if err := netlink.LinkSetUp(link); err != nil {
				return err
			}
		}
		eth0, err := netlink.LinkByName(containerDevice)
		if err != nil {
			return err
		}
		addr := &netlink.Addr{IPNet: &net.IPNet{IP: net.ParseIP(ContainerIP), Mask: net.CIDRMask(gateway.Bits(), 32)}}
		if err := netlink.AddrAdd(eth0, addr); err != nil {
			return err
		}
		return netlink.RouteAdd(&netlink.Route{LinkIndex: eth0.Attrs().Index, Gw: gateway.Addr().AsSlice()})
	})
}

// newNetwork runs createDevices in a new net namespace, then attaches the stack
// to device.
func newNetwork(ctx context.Context, gatewayCIDR string, enableExternalNetworking bool, device string, createDevices func() error) (_ *Network, err error) {
	gateway, err := netip.ParsePrefix(gatewayCIDR)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid gateway %q: %s", gatewayCIDR, err)
	}
	allowed, err := networking.TaskAllowedPrivateIPs(ctx)
	if err != nil {
		return nil, err
	}
	n := &Network{
		fd:              -1,
		gateway:         tcpip.AddrFrom4(gateway.Addr().As4()),
		externalNetwork: enableExternalNetworking,
		allowedPrefixes: allowed,
		pings:           make(chan struct{}, maxPingsInFlight),
		udpIdleTimeout:  udpIdleTimeout,
	}
	defer func() {
		if err != nil {
			n.Cleanup(ctx)
		}
	}()

	n.netns, err = networking.CreateUniqueNetNamespace(ctx)
	if err != nil {
		return nil, status.WrapError(err, "create net namespace")
	}
	err = inNamespace(n.netns, func() error {
		if err := createDevices(); err != nil {
			return status.UnavailableErrorf("create network devices: %s", err)
		}
		fd, err := openPacketSocket(device)
		if err != nil {
			return status.UnavailableErrorf("open packet socket: %s", err)
		}
		n.fd = fd
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := n.startStack(gateway.Bits()); err != nil {
		return nil, err
	}
	return n, nil
}

// startStack is based on gvisor-tap-vsock's createStack:
// https://github.com/containers/gvisor-tap-vsock/blob/ad36eb20acfae43f5df9f0807201f5059073b881/pkg/virtualnetwork/virtualnetwork.go#L122
func (n *Network) startStack(prefixLen int) error {
	n.stack = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, arp.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, gicmp.NewProtocol4},
	})
	sack := tcpip.TCPSACKEnabled(true)
	moderateReceiveBuffer := tcpip.TCPModerateReceiveBufferOption(true)
	// RACK mistakes ACKs that the stack processes late for losses, and its
	// spurious recoveries collapse throughput with concurrent flows.
	recovery := tcpip.TCPRecovery(0)
	for _, opt := range []tcpip.SettableTransportProtocolOption{
		&sack,
		&moderateReceiveBuffer,
		&recovery,
		&tcpip.TCPSendBufferSizeRangeOption{Min: 4 << 10, Default: 1 << 20, Max: tcpBufferSize},
		&tcpip.TCPReceiveBufferSizeRangeOption{Min: 4 << 10, Default: 1 << 20, Max: tcpBufferSize},
	} {
		if err := n.stack.SetTransportProtocolOption(tcp.ProtocolNumber, opt); err != nil {
			return status.InternalErrorf("set TCP option %T: %s", opt, err)
		}
	}

	// Handlers must be set before the NIC starts delivering packets.
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	tcpForwarder := tcp.NewForwarder(n.stack, 0 /*=rcvWnd*/, 1024 /*=maxInFlight*/, func(r *tcp.ForwarderRequest) {
		n.forwardTCP(ctx, r)
	})
	n.stack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpForwarder.HandlePacket)
	udpForwarder := udp.NewForwarder(n.stack, func(r *udp.ForwarderRequest) bool {
		n.forwardUDP(ctx, r)
		return true
	})
	n.stack.SetTransportProtocolHandler(udp.ProtocolNumber, func(id stack.TransportEndpointID, pkt *stack.PacketBuffer) bool {
		if !n.isAllowed(id.LocalAddress) {
			n.reject(pkt)
			return true
		}
		return udpForwarder.HandlePacket(id, pkt)
	})
	n.stack.SetTransportProtocolHandler(gicmp.ProtocolNumber4, func(id stack.TransportEndpointID, pkt *stack.PacketBuffer) bool {
		return n.forwardEcho(id, pkt)
	})

	ep, err := fdbased.New(&fdbased.Options{
		FDs:                []int{n.fd},
		MTU:                mtu,
		EthernetHeader:     true,
		Address:            gatewayLinkAddress,
		PacketDispatchMode: fdbased.RecvMMsg,
		// More processors don't improve throughput, and with RecvMMsg, fdbased
		// leaks the processor goroutines of a dispatcher it discards:
		// https://github.com/google/gvisor/blob/39ed1f5ac29cb9a2d99d41502de53b8f0e2d19b6/pkg/tcpip/link/fdbased/endpoint.go#L350-L398
		ProcessorsPerChannel: 1,
		GSOMaxSize:           gsoMaxSize,
		// Guests leave checksums partial on offloaded frames.
		RXChecksumOffload: true,
	})
	if err != nil {
		return status.InternalErrorf("create link endpoint: %s", err)
	}
	if err := n.stack.CreateNIC(nicID, ep); err != nil {
		return status.InternalErrorf("create NIC: %s", err)
	}
	addr := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: n.gateway, PrefixLen: prefixLen},
	}
	if err := n.stack.AddProtocolAddress(nicID, addr, stack.AddressProperties{}); err != nil {
		return status.InternalErrorf("add gateway address: %s", err)
	}
	// Accept packets for any destination, and reply as that destination.
	n.stack.SetPromiscuousMode(nicID, true)
	n.stack.SetSpoofing(nicID, true)
	n.stack.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: nicID}})
	return nil
}

func (n *Network) isAllowed(addr tcpip.Address) bool {
	if !n.externalNetwork {
		return false
	}
	ip := netip.AddrFrom4(addr.As4())
	for _, p := range n.allowedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

func (n *Network) forwardTCP(ctx context.Context, r *tcp.ForwarderRequest) {
	id := r.ID()
	if !n.isAllowed(id.LocalAddress) {
		r.Complete(true /*=sendReset*/)
		return
	}
	dst := net.JoinHostPort(id.LocalAddress.String(), strconv.Itoa(int(id.LocalPort)))
	remote, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", dst)
	if err != nil {
		r.Complete(true /*=sendReset*/)
		return
	}
	defer remote.Close()
	stop := context.AfterFunc(ctx, func() { remote.Close() })
	defer stop()
	var wq waiter.Queue
	ep, tcpErr := r.CreateEndpoint(&wq)
	r.Complete(false /*=sendReset*/)
	if tcpErr != nil {
		return
	}
	guest := gonet.NewTCPConn(&wq, ep)
	defer guest.Close()

	var wg sync.WaitGroup
	wg.Go(func() {
		copyBuffered(remote, guest)
		remote.(*net.TCPConn).CloseWrite()
	})
	copyBuffered(guest, remote)
	guest.CloseWrite()
	wg.Wait()
}

func (n *Network) forwardUDP(ctx context.Context, r *udp.ForwarderRequest) {
	id := r.ID()
	// Creating the endpoint registers the flow, so that its later packets are
	// delivered to it rather than to the forwarder.
	var wq waiter.Queue
	ep, tcpErr := r.CreateEndpoint(&wq)
	if tcpErr != nil {
		return
	}
	dst := net.JoinHostPort(id.LocalAddress.String(), strconv.Itoa(int(id.LocalPort)))
	// The forwarder runs on the stack's packet processing goroutine.
	go relayUDP(ctx, gonet.NewUDPConn(&wq, ep), dst, n.udpIdleTimeout)
}

func relayUDP(ctx context.Context, guest *gonet.UDPConn, dst string, idleTimeout time.Duration) {
	remote, err := (&net.Dialer{}).DialContext(ctx, "udp", dst)
	if err != nil {
		guest.Close()
		return
	}
	closeBoth := sync.OnceFunc(func() {
		guest.Close()
		remote.Close()
	})
	stop := context.AfterFunc(ctx, closeBoth)
	defer stop()
	// The flow is closed once neither direction has had traffic for
	// idleTimeout.
	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	relay := func(dst, src net.Conn) {
		defer closeBoth()
		buf := make([]byte, 65535)
		for {
			src.SetReadDeadline(time.Unix(0, lastActivity.Load()).Add(idleTimeout))
			size, err := src.Read(buf)
			if err != nil {
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() && time.Since(time.Unix(0, lastActivity.Load())) < idleTimeout {
					// The other direction had traffic since the deadline was set.
					continue
				}
				return
			}
			lastActivity.Store(time.Now().UnixNano())
			if _, err := dst.Write(buf[:size]); err != nil {
				return
			}
		}
	}
	go relay(remote, guest)
	relay(guest, remote)
}

// forwardEcho sends echo requests from the host with ping sockets and relays
// the replies.
func (n *Network) forwardEcho(id stack.TransportEndpointID, pkt *stack.PacketBuffer) bool {
	h := header.ICMPv4(pkt.TransportHeader().Slice())
	if len(h) < header.ICMPv4MinimumSize || h.Type() != header.ICMPv4Echo {
		return false
	}
	// Leave echo requests to the gateway unhandled so the stack answers them.
	if id.LocalAddress == n.gateway {
		return false
	}
	if !n.isAllowed(id.LocalAddress) {
		n.reject(pkt)
		return true
	}
	select {
	case n.pings <- struct{}{}:
	default:
		return true
	}
	ident, seq, data := h.Ident(), h.Sequence(), pkt.Data().AsRange().ToSlice()
	go func() {
		defer func() { <-n.pings }()
		conn, err := icmp.ListenPacket("udp4", "0.0.0.0")
		if err != nil {
			return
		}
		defer conn.Close()
		// The kernel replaces the ident, and delivers only this socket's replies.
		if _, err := conn.WriteTo(icmpEcho(header.ICMPv4Echo, 0, seq, data), &net.UDPAddr{IP: id.LocalAddress.AsSlice()}); err != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(pingTimeout))
		buf := make([]byte, 65535)
		for {
			size, _, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			reply := header.ICMPv4(buf[:size])
			if size >= header.ICMPv4MinimumSize && reply.Type() == header.ICMPv4EchoReply && reply.Sequence() == seq {
				n.writeICMP(id.LocalAddress, id.RemoteAddress, icmpEcho(header.ICMPv4EchoReply, ident, seq, reply.Payload()))
				return
			}
		}
	}()
	return true
}

// reject answers a packet to a blocked destination with ICMP port
// unreachable, like an iptables REJECT rule.
func (n *Network) reject(pkt *stack.PacketBuffer) {
	ip := header.IPv4(pkt.NetworkHeader().Slice())
	original := append(slices.Clone(pkt.NetworkHeader().Slice()), pkt.TransportHeader().Slice()...)
	original = original[:min(len(original), int(ip.HeaderLength())+8)]
	msg := make([]byte, header.ICMPv4MinimumSize+len(original))
	h := header.ICMPv4(msg)
	h.SetType(header.ICMPv4DstUnreachable)
	h.SetCode(header.ICMPv4PortUnreachable)
	copy(h.Payload(), original)
	h.SetChecksum(^checksum.Checksum(msg, 0))
	n.writeICMP(n.gateway, ip.SourceAddress(), msg)
}

func (n *Network) writeICMP(from, to tcpip.Address, msg []byte) {
	r, err := n.stack.FindRoute(nicID, from, to, ipv4.ProtocolNumber, false /*=multicastLoop*/)
	if err != nil {
		return
	}
	defer r.Release()
	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		ReserveHeaderBytes: int(r.MaxHeaderLength()),
		Payload:            buffer.MakeWithData(msg),
	})
	defer pkt.DecRef()
	pkt.TransportProtocolNumber = header.ICMPv4ProtocolNumber
	r.WritePacket(stack.NetworkHeaderParams{Protocol: header.ICMPv4ProtocolNumber, TTL: 64}, pkt)
}

// NamespacePath returns the path of the net namespace to run in.
func (n *Network) NamespacePath() string {
	return n.netns.Path()
}

// Stats returns the traffic sent and received by the guest.
func (n *Network) Stats(ctx context.Context) (*repb.NetworkStats, error) {
	s := n.stack.NICInfo()[nicID].Stats
	return &repb.NetworkStats{
		BytesReceived:   int64(s.Tx.Bytes.Value()),
		PacketsReceived: int64(s.Tx.Packets.Value()),
		BytesSent:       int64(s.Rx.Bytes.Value()),
		PacketsSent:     int64(s.Rx.Packets.Value()),
	}, nil
}

func (n *Network) Cleanup(ctx context.Context) error {
	if n.cancel != nil {
		n.cancel()
	}
	if n.stack != nil {
		n.stack.Close()
		n.stack.Wait()
	}
	if n.fd >= 0 {
		unix.Close(n.fd)
	}
	if n.netns != nil {
		return n.netns.Delete(ctx)
	}
	return nil
}

// inNamespace runs fn on a new OS thread in netns.
func inNamespace(netns *networking.Namespace, fn func() error) error {
	errCh := make(chan error, 1)
	go func() {
		// The thread isn't unlocked, so it exits with this goroutine instead of
		// being switched back.
		runtime.LockOSThread()
		target, err := os.Open(netns.Path())
		if err != nil {
			errCh <- err
			return
		}
		defer target.Close()
		if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
			errCh <- err
			return
		}
		errCh <- fn()
	}()
	return <-errCh
}

// openPacketSocket opens a raw packet socket bound to device, exchanging
// virtio-net headers so that offloaded frames pass through whole.
func openPacketSocket(device string) (_ int, err error) {
	iface, err := net.InterfaceByName(device)
	if err != nil {
		return -1, err
	}
	// The protocol is only set at bind time: a socket created with one is
	// hooked to every device, and rebinding it waits for an RCU grace period.
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	defer func() {
		if err != nil {
			unix.Close(fd)
		}
	}()
	for _, opt := range []struct{ level, name, value int }{
		{unix.SOL_PACKET, unix.PACKET_VNET_HDR, 1},
		{unix.SOL_SOCKET, unix.SO_SNDBUFFORCE, socketBufferSize},
		{unix.SOL_SOCKET, unix.SO_RCVBUFFORCE, socketBufferSize},
	} {
		if err := unix.SetsockoptInt(fd, opt.level, opt.name, opt.value); err != nil {
			return -1, err
		}
	}
	protocol := uint16(unix.ETH_P_ALL)<<8 | uint16(unix.ETH_P_ALL)>>8 // network byte order
	if err := unix.Bind(fd, &unix.SockaddrLinklayer{Protocol: protocol, Ifindex: iface.Index}); err != nil {
		return -1, err
	}
	return fd, nil
}

var relayBuffers = sync.Pool{New: func() any {
	buf := make([]byte, relayBufferSize)
	return &buf
}}

// copyBuffered copies src to dst with a large buffer. The wrappers hide
// ReaderFrom and WriterTo, whose own buffers are small.
func copyBuffered(dst io.Writer, src io.Reader) {
	buf := relayBuffers.Get().(*[]byte)
	defer relayBuffers.Put(buf)
	io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{src}, *buf)
}

func icmpEcho(typ header.ICMPv4Type, ident, seq uint16, data []byte) []byte {
	msg := make([]byte, header.ICMPv4MinimumSize+len(data))
	h := header.ICMPv4(msg)
	h.SetType(typ)
	h.SetIdent(ident)
	h.SetSequence(seq)
	copy(h.Payload(), data)
	h.SetChecksum(^checksum.Checksum(msg, 0))
	return msg
}
