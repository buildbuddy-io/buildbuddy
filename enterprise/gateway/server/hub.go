package server

// Hub services are what a gateway runs on each network's hub IP
// (fd00:bb:N::1), inside that network's private gVisor stack. Which services a
// gateway offers is decided by composition at construction time — see New.

import (
	"context"
	"io"
	"net"
	"net/netip"
	"strings"

	"github.com/miekg/dns"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// HubNetwork is one network's hub as seen by a HubService: listeners on the
// hub IP plus the lookups a service needs to answer for this network.
type HubNetwork struct {
	// IP is the hub address the service is reachable on.
	IP netip.Addr
	// NetworkKey identifies the network in logs ("owner/network").
	NetworkKey string

	// LookupName resolves a peer name registered in this network.
	LookupName func(name string) (netip.Addr, bool)

	// PeerContext returns a context that is canceled when the registration
	// of the peer at ip ends — it is evicted, or its Connect stream closes.
	PeerContext func(ip netip.Addr) (ctx context.Context, ok bool)

	stack *stack.Stack
}

// ListenTCP opens a TCP listener on the hub IP inside the network's stack.
func (h *HubNetwork) ListenTCP(port int) (net.Listener, error) {
	return gonet.ListenTCP(h.stack, tcpip.FullAddress{
		NIC:  1,
		Addr: tcpip.AddrFromSlice(h.IP.AsSlice()),
		Port: uint16(port),
	}, ipv6.ProtocolNumber)
}

// ListenUDP opens a UDP socket bound to the hub IP inside the network's stack.
func (h *HubNetwork) ListenUDP(port int) (net.PacketConn, error) {
	return gonet.DialUDP(h.stack, &tcpip.FullAddress{
		NIC:  1,
		Addr: tcpip.AddrFromSlice(h.IP.AsSlice()),
		Port: uint16(port),
	}, nil, ipv6.ProtocolNumber)
}

// A HubService serves peers on their network's hub IP. Start is called once
// per network, when the network is created. The returned closer is invoked
// when the gateway shuts the network down.
type HubService interface {
	Start(hub *HubNetwork) (io.Closer, error)
}

// DNSService returns the hub service that resolves registered peer names
// (fd00:bb:N::1 port 53). Peers configure the hub as their resolver, so
// `ssh <peer-name>` works inside the overlay.
func DNSService() HubService { return dnsService{} }

type dnsService struct{}

func (dnsService) Start(hub *HubNetwork) (io.Closer, error) {
	conn, err := hub.ListenUDP(53)
	if err != nil {
		return nil, err
	}
	go serveDNS(conn, hub.LookupName)
	return conn, nil
}

func serveDNS(conn net.PacketConn, lookup func(string) (netip.Addr, bool)) {
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
