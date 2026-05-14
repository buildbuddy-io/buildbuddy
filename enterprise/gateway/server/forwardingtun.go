package server

// muxTUN is the single TUN device shared by all group networks.
//
// Packet flow for intra-network forwarding (Client A → Client B):
//
//  1. Client A's WireGuard encrypts and sends UDP to the gateway
//  2. Gateway WireGuard decrypts, calls muxTUN.Write()
//  3. muxTUN looks up src and dst in ipToNetwork — drops if cross-network
//  4. Packet copied into outbound channel
//  5. WireGuard calls Read(), gets packet, AllowedIPs lookup → Client B
//  6. WireGuard encrypts, sends UDP to Client B
//
// Packets destined for the network's hub IP (fd00:bb:N::1) are injected into
// that network's private gVisor stack instead, where the DNS server answers
// them locally rather than forwarding.

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"

	"github.com/miekg/dns"
	"golang.zx2c4.com/wireguard/tun"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

// netStack is a per-network gVisor stack used for local services (DNS).
type netStack struct {
	hubIP        netip.Addr
	ep           *channel.Endpoint
	stack        *stack.Stack
	notifyHandle *channel.NotificationHandle
	outbound     chan *buffer.View // shared with muxTUN
}

// WriteNotify implements channel.Notification. Called by gVisor when the stack
// has a packet to send out. We forward it to WireGuard via the outbound channel.
func (ns *netStack) WriteNotify() {
	pkt := ns.ep.Read()
	if pkt == nil {
		return
	}
	view := pkt.ToView()
	pkt.DecRef()
	select {
	case ns.outbound <- view:
	default:
		view.Release()
	}
}

func (ns *netStack) injectInbound(pkt []byte) {
	pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(pkt),
	})
	switch pkt[0] >> 4 {
	case 4:
		ns.ep.InjectInbound(header.IPv4ProtocolNumber, pkb)
	case 6:
		ns.ep.InjectInbound(header.IPv6ProtocolNumber, pkb)
	}
	pkb.DecRef()
}

func (ns *netStack) close() {
	ns.ep.RemoveNotify(ns.notifyHandle)
	ns.ep.Close()
	ns.stack.Close()
}

// muxTUN is the TUN device passed to the single shared WireGuard device.
type muxTUN struct {
	outbound    chan *buffer.View
	ipToNetwork sync.Map // netip.Addr → int (network index)
	netStacks   sync.Map // int (network index) → *netStack
	events      chan tun.Event
	mtu         int
	done        chan struct{}
	closeOnce   sync.Once
}

func newMuxTUN(mtu int) *muxTUN {
	t := &muxTUN{
		outbound: make(chan *buffer.View, 1024),
		events:   make(chan tun.Event, 10),
		mtu:      mtu,
		done:     make(chan struct{}),
	}
	t.events <- tun.EventUp
	return t
}

func (t *muxTUN) registerIP(addr netip.Addr, networkIndex int) {
	t.ipToNetwork.Store(addr, networkIndex)
}

func (t *muxTUN) unregisterIP(addr netip.Addr) {
	t.ipToNetwork.Delete(addr)
}

// startNetworkDNS creates a gVisor stack for the network at index and starts
// a DNS server bound to its hub IP. networkKey is used for logging only.
func (t *muxTUN) startNetworkDNS(index int, networkKey string, lookup func(string) (netip.Addr, bool)) error {
	hubIP := networkHubIP(index)

	opts := stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol4, icmp.NewProtocol6},
		HandleLocal:        true,
	}
	ep := channel.New(64, uint32(t.mtu), "")
	s := stack.New(opts)

	if err := s.CreateNIC(1, ep); err != nil {
		return fmt.Errorf("CreateNIC: %v", err)
	}
	protoAddr := tcpip.ProtocolAddress{
		Protocol:          ipv6.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFromSlice(hubIP.AsSlice()).WithPrefix(),
	}
	if err := s.AddProtocolAddress(1, protoAddr, stack.AddressProperties{}); err != nil {
		return fmt.Errorf("AddProtocolAddress: %v", err)
	}
	s.SetRouteTable([]tcpip.Route{{
		Destination: header.IPv6EmptySubnet,
		NIC:         1,
	}})

	ns := &netStack{
		hubIP:    hubIP,
		ep:       ep,
		stack:    s,
		outbound: t.outbound,
	}
	ns.notifyHandle = ep.AddNotify(ns)

	conn, err := gonet.DialUDP(s, &tcpip.FullAddress{
		NIC:  1,
		Addr: tcpip.AddrFromSlice(hubIP.AsSlice()),
		Port: 53,
	}, nil, ipv6.ProtocolNumber)
	if err != nil {
		ns.close()
		return fmt.Errorf("DNS listen on %s:53: %v", hubIP, err)
	}
	go serveDNS(conn, lookup)

	// Register the hub IP so Write() routes DNS packets into this stack.
	t.ipToNetwork.Store(hubIP, index)
	t.netStacks.Store(index, ns)
	return nil
}

// Write receives decrypted packets from WireGuard and dispatches them.
func (t *muxTUN) Write(bufs [][]byte, offset int) (int, error) {
	for _, b := range bufs {
		pkt := b[offset:]
		if len(pkt) == 0 {
			continue
		}
		switch pkt[0] >> 4 {
		case 4:
			t.dispatch(pkt, extractIPv4Addrs)
		case 6:
			t.dispatch(pkt, extractIPv6Addrs)
		}
	}
	return len(bufs), nil
}

func (t *muxTUN) dispatch(pkt []byte, extractAddrs func([]byte) (src, dst netip.Addr, ok bool)) {
	src, dst, ok := extractAddrs(pkt)
	if !ok {
		return
	}

	srcIdx, srcOk := t.ipToNetwork.Load(src)
	if !srcOk {
		return // unknown source, drop
	}
	dstIdx, dstOk := t.ipToNetwork.Load(dst)
	if !dstOk || dstIdx.(int) != srcIdx.(int) {
		return // unknown dest or cross-network, drop
	}

	// If dst is the hub, inject into that network's gVisor stack (for DNS etc.).
	if ns, ok := t.netStacks.Load(srcIdx.(int)); ok {
		if dst == ns.(*netStack).hubIP {
			ns.(*netStack).injectInbound(pkt)
			return
		}
	}

	// Intra-network peer-to-peer: put in outbound queue for WireGuard.
	pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(pkt)})
	view := pkb.ToView()
	pkb.DecRef()
	select {
	case t.outbound <- view:
	default:
		view.Release()
	}
}

func extractIPv4Addrs(pkt []byte) (src, dst netip.Addr, ok bool) {
	if len(pkt) < header.IPv4MinimumSize {
		return netip.Addr{}, netip.Addr{}, false
	}
	h := header.IPv4(pkt)
	s, d := h.SourceAddress(), h.DestinationAddress()
	src, _ = netip.AddrFromSlice(s.AsSlice())
	dst, _ = netip.AddrFromSlice(d.AsSlice())
	return src.Unmap(), dst.Unmap(), true
}

func extractIPv6Addrs(pkt []byte) (src, dst netip.Addr, ok bool) {
	if len(pkt) < header.IPv6MinimumSize {
		return netip.Addr{}, netip.Addr{}, false
	}
	h := header.IPv6(pkt)
	s, d := h.SourceAddress(), h.DestinationAddress()
	src, _ = netip.AddrFromSlice(s.AsSlice())
	dst, _ = netip.AddrFromSlice(d.AsSlice())
	return src, dst, true
}

func (t *muxTUN) Read(bufs [][]byte, sizes []int, offset int) (int, error) {
	select {
	case view := <-t.outbound:
		n, err := view.Read(bufs[0][offset:])
		if err != nil {
			return 0, err
		}
		sizes[0] = n
		return 1, nil
	case <-t.done:
		return 0, os.ErrClosed
	}
}

func (t *muxTUN) Name() (string, error)    { return "mux", nil }
func (t *muxTUN) File() *os.File           { return nil }
func (t *muxTUN) Events() <-chan tun.Event { return t.events }
func (t *muxTUN) MTU() (int, error)        { return t.mtu, nil }
func (t *muxTUN) BatchSize() int           { return 1 }

func (t *muxTUN) Close() error {
	t.closeOnce.Do(func() {
		close(t.done)
		close(t.events)
		t.netStacks.Range(func(_, v any) bool {
			v.(*netStack).close()
			return true
		})
	})
	return nil
}

func serveDNS(conn *gonet.UDPConn, lookup func(string) (netip.Addr, bool)) {
	buf := make([]byte, 512)
	for {
		n, src, err := conn.ReadFrom(buf)
		if err != nil {
			return // stack closed
		}
		req := new(dns.Msg)
		if err := req.Unpack(buf[:n]); err != nil {
			continue
		}
		resp := new(dns.Msg)
		resp.SetReply(req)
		resp.Authoritative = true
		for _, q := range req.Question {
			name := strings.TrimSuffix(q.Name, ".")
			ip, ok := lookup(name)
			if !ok {
				resp.Rcode = dns.RcodeNameError
				continue
			}
			switch q.Qtype {
			case dns.TypeAAAA:
				if ip.Is6() {
					resp.Answer = append(resp.Answer, &dns.AAAA{
						Hdr:  dns.RR_Header{Name: q.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
						AAAA: net.IP(ip.AsSlice()),
					})
				}
			case dns.TypeA:
				if ip.Is4() {
					a4 := ip.As4()
					resp.Answer = append(resp.Answer, &dns.A{
						Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
						A:   net.IP(a4[:]),
					})
				}
			}
		}
		if b, err := resp.Pack(); err == nil {
			conn.WriteTo(b, src)
		}
	}
}
