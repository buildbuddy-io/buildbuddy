package server

// forwardingTUN is a clone of wireguard-go's netstack TUN device with IP
// forwarding enabled on the gVisor stack. This makes the gateway hub behave
// like a kernel with ip_forward=1: packets not destined for the hub's own IP
// are forwarded back out the interface, where WireGuard's AllowedIPs routing
// delivers them to the correct peer.
//
// Packet flow for Client1 (fd00:bb::2) -> Client2 (fd00:bb::3) via Hub:
//
//  1. Client1 WireGuard encrypts, sends UDP to Hub
//  2. Hub WireGuard decrypts, calls forwardingTUN.Write()
//  3. gVisor stack sees dst=fd00:bb::3, not local -> IP forward
//  4. Forwarded packet written to channel endpoint
//  5. forwardingTUN.WriteNotify() -> incomingPacket channel
//  6. Hub WireGuard calls Read(), gets packet, sees dst=fd00:bb::3
//  7. AllowedIPs lookup -> Client2's peer -> encrypt, send UDP
//
// The hub IP (fd00:bb::1) is local to the gVisor stack, so packets addressed
// to it are delivered locally rather than forwarded. This is used for the
// DNS server: peers send queries to fd00:bb::1:53 and the hub answers them.

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"syscall"

	"github.com/miekg/dns"
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
	"golang.zx2c4.com/wireguard/tun"
)

type forwardingTUN struct {
	ep             *channel.Endpoint
	stack          *stack.Stack
	addr           netip.Addr
	events         chan tun.Event
	notifyHandle   *channel.NotificationHandle
	incomingPacket chan *buffer.View
	mtu            int
}

func createForwardingTUN(ip string, mtu int) (*forwardingTUN, error) {
	opts := stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol6, icmp.NewProtocol4},
		HandleLocal:        true,
	}

	dev := &forwardingTUN{
		ep:             channel.New(1024, uint32(mtu), ""),
		stack:          stack.New(opts),
		events:         make(chan tun.Event, 10),
		incomingPacket: make(chan *buffer.View),
		mtu:            mtu,
	}

	// Enable IP forwarding — the whole point of this custom TUN.
	if err := dev.stack.SetForwardingDefaultAndAllNICs(ipv4.ProtocolNumber, true); err != nil {
		return nil, fmt.Errorf("SetForwarding(IPv4): %v", err)
	}
	if err := dev.stack.SetForwardingDefaultAndAllNICs(ipv6.ProtocolNumber, true); err != nil {
		return nil, fmt.Errorf("SetForwarding(IPv6): %v", err)
	}

	sackOpt := tcpip.TCPSACKEnabled(true)
	if err := dev.stack.SetTransportProtocolOption(tcp.ProtocolNumber, &sackOpt); err != nil {
		return nil, fmt.Errorf("TCP SACK: %v", err)
	}

	dev.notifyHandle = dev.ep.AddNotify(dev)
	if err := dev.stack.CreateNIC(1, dev.ep); err != nil {
		return nil, fmt.Errorf("CreateNIC: %v", err)
	}

	addr := netip.MustParseAddr(ip)
	dev.addr = addr
	protoNum := tcpip.NetworkProtocolNumber(ipv4.ProtocolNumber)
	defaultRoute := tcpip.Route{Destination: header.IPv4EmptySubnet, NIC: 1}
	if addr.Is6() {
		protoNum = ipv6.ProtocolNumber
		defaultRoute = tcpip.Route{Destination: header.IPv6EmptySubnet, NIC: 1}
	}
	protoAddr := tcpip.ProtocolAddress{
		Protocol:          protoNum,
		AddressWithPrefix: tcpip.AddrFromSlice(addr.AsSlice()).WithPrefix(),
	}
	if err := dev.stack.AddProtocolAddress(1, protoAddr, stack.AddressProperties{}); err != nil {
		return nil, fmt.Errorf("AddProtocolAddress: %v", err)
	}
	dev.stack.AddRoute(defaultRoute)

	dev.events <- tun.EventUp
	return dev, nil
}

// StartDNS starts a UDP DNS server on port 53 of the hub's gVisor stack.
// Peers send queries to the hub IP (fd00:bb::1) which is local to the stack,
// so they are delivered here rather than forwarded. lookup maps a peer name
// to its assigned IP; last-write-wins for duplicate names.
func (t *forwardingTUN) StartDNS(lookup func(name string) (netip.Addr, bool)) error {
	protoNum := tcpip.NetworkProtocolNumber(ipv4.ProtocolNumber)
	if t.addr.Is6() {
		protoNum = ipv6.ProtocolNumber
	}
	conn, err := gonet.DialUDP(t.stack, &tcpip.FullAddress{
		NIC:  1,
		Addr: tcpip.AddrFromSlice(t.addr.AsSlice()),
		Port: 53,
	}, nil, protoNum)
	if err != nil {
		return fmt.Errorf("DNS listen on %s:53: %v", t.addr, err)
	}
	go serveDNS(conn, lookup)
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

func (t *forwardingTUN) Name() (string, error)   { return "fwd", nil }
func (t *forwardingTUN) File() *os.File           { return nil }
func (t *forwardingTUN) Events() <-chan tun.Event { return t.events }
func (t *forwardingTUN) MTU() (int, error)        { return t.mtu, nil }
func (t *forwardingTUN) BatchSize() int           { return 1 }

func (t *forwardingTUN) Read(buf [][]byte, sizes []int, offset int) (int, error) {
	view, ok := <-t.incomingPacket
	if !ok {
		return 0, os.ErrClosed
	}
	n, err := view.Read(buf[0][offset:])
	if err != nil {
		return 0, err
	}
	sizes[0] = n
	return 1, nil
}

func (t *forwardingTUN) Write(buf [][]byte, offset int) (int, error) {
	for _, b := range buf {
		packet := b[offset:]
		if len(packet) == 0 {
			continue
		}
		pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(packet)})
		switch packet[0] >> 4 {
		case 4:
			t.ep.InjectInbound(header.IPv4ProtocolNumber, pkb)
		case 6:
			t.ep.InjectInbound(header.IPv6ProtocolNumber, pkb)
		default:
			return 0, syscall.EAFNOSUPPORT
		}
	}
	return len(buf), nil
}

// WriteNotify is called by the channel endpoint when the gVisor stack has a
// packet to send — either locally-originated or forwarded.
func (t *forwardingTUN) WriteNotify() {
	pkt := t.ep.Read()
	if pkt == nil {
		return
	}
	view := pkt.ToView()
	pkt.DecRef()
	t.incomingPacket <- view
}

func (t *forwardingTUN) Close() error {
	t.stack.RemoveNIC(1)
	t.stack.Close()
	t.ep.RemoveNotify(t.notifyHandle)
	t.ep.Close()
	close(t.events)
	close(t.incomingPacket)
	return nil
}
