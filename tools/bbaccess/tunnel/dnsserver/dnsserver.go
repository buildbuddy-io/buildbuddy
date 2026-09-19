// Package dnsserver answers DNS for the zones the tunnel covers.
//
// It allocates IPs locally from the assigned local IP range that is assigned
// to the tunnel tun device.
package dnsserver

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/fakeip"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"github.com/miekg/dns"
)

// ttl is deliberately short. It costs nothing (we answer from memory) and it
// limits how long a resolver cache pins a name if the config changes.
const ttl = 10

// Prewarmer is notified when a name in a zone is resolved, so the tunnel for
// that zone can start connecting before the application does.
type Prewarmer interface {
	Prewarm(ctx context.Context, zone tunnelconfig.Zone)
}

type Server struct {
	cfg    *tunnelconfig.Config
	table  *fakeip.Table
	warmer Prewarmer

	udp *dns.Server
	tcp *dns.Server
}

func New(cfg *tunnelconfig.Config, table *fakeip.Table, warmer Prewarmer) *Server {
	return &Server{cfg: cfg, table: table, warmer: warmer}
}

// Start binds the DNS listeners. Both UDP and TCP are served: resolvers fall
// back to TCP for truncated answers, and a TCP connection refused mid-fallback
// looks like a resolution failure rather than a retry.
func (s *Server) Start(addr string) error {
	handler := dns.HandlerFunc(s.handle)
	s.udp = &dns.Server{Addr: addr, Net: "udp", Handler: handler}
	s.tcp = &dns.Server{Addr: addr, Net: "tcp", Handler: handler}

	errCh := make(chan error, 2)
	started := make(chan struct{}, 2)
	s.udp.NotifyStartedFunc = func() { started <- struct{}{} }
	s.tcp.NotifyStartedFunc = func() { started <- struct{}{} }

	go func() { errCh <- s.udp.ListenAndServe() }()
	go func() { errCh <- s.tcp.ListenAndServe() }()

	for range 2 {
		select {
		case err := <-errCh:
			s.Shutdown() // whichever listener did start
			return fmt.Errorf("starting DNS server on %s: %w", addr, err)
		case <-started:
		case <-time.After(10 * time.Second):
			s.Shutdown()
			return fmt.Errorf("timed out starting DNS server on %s", addr)
		}
	}
	return nil
}

func (s *Server) Shutdown() {
	if s.udp != nil {
		s.udp.Shutdown()
	}
	if s.tcp != nil {
		s.tcp.Shutdown()
	}
}

func (s *Server) handle(w dns.ResponseWriter, req *dns.Msg) {
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.Authoritative = true

	for _, q := range req.Question {
		// Lower case before the name reaches the table: resolvers that
		// randomize query case (0x20) must not allocate an address per variant.
		name := strings.ToLower(strings.TrimSuffix(q.Name, "."))

		if q.Qtype == dns.TypePTR {
			s.answerPTR(resp, q)
			continue
		}

		zone, ok := s.cfg.MatchZone(name)
		if !ok {
			if tunnelconfig.UnderParent(name) {
				// Everything under the parent is routed here and nothing else
				// can answer it, so a name in no zone does not exist.
				resp.Rcode = dns.RcodeNameError
				continue
			}
			// Not ours. REFUSED (rather than NXDOMAIN) tells the stub resolver
			// to try another server instead of caching a negative answer — it
			// matters because systemd-resolved may route unrelated queries here
			// when a link loses its own DNS configuration.
			resp.Rcode = dns.RcodeRefused
			continue
		}

		switch q.Qtype {
		case dns.TypeA:
			addr, err := s.table.Lookup(name)
			if err != nil {
				log.Warningf("tunnel: allocating an address for %q: %s", name, err)
				resp.Rcode = dns.RcodeServerFailure
				continue
			}
			a4 := addr.As4()
			resp.Answer = append(resp.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl},
				A:   net.IP(a4[:]),
			})
			// Start the tunnel now: an application typically resolves and then
			// connects immediately, and this overlaps the handshake with that gap.
			if s.warmer != nil {
				go s.warmer.Prewarm(context.Background(), zone)
			}
		default:
			// Empty NOERROR for everything else we don't synthesize, especially
			// AAAA and HTTPS (type 65), which macOS queries aggressively.
			// Answering NXDOMAIN for AAAA while A succeeds makes stub resolvers
			// treat the whole name as nonexistent.
		}
	}

	if err := w.WriteMsg(resp); err != nil {
		log.Debugf("tunnel: writing DNS response: %s", err)
	}
}

// answerPTR resolves a fake IP back to the name it stands for, so tools that
// reverse-resolve a peer address show something meaningful.
func (s *Server) answerPTR(resp *dns.Msg, q dns.Question) {
	addr, ok := addrFromARPA(q.Name)
	if !ok || !s.table.Contains(addr) {
		resp.Rcode = dns.RcodeRefused
		return
	}
	name, ok := s.table.Name(addr)
	if !ok {
		resp.Rcode = dns.RcodeNameError
		return
	}
	resp.Answer = append(resp.Answer, &dns.PTR{
		Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: ttl},
		Ptr: dns.Fqdn(name),
	})
}

// addrFromARPA parses "4.3.2.1.in-addr.arpa." into 1.2.3.4.
func addrFromARPA(name string) (netip.Addr, bool) {
	const suffix = ".in-addr.arpa"
	n := strings.ToLower(strings.TrimSuffix(name, "."))
	if !strings.HasSuffix(n, suffix) {
		return netip.Addr{}, false
	}
	n = strings.TrimSuffix(n, suffix)
	parts := strings.Split(n, ".")
	if len(parts) != 4 {
		return netip.Addr{}, false
	}
	reversed := make([]string, 4)
	for i, p := range parts {
		reversed[3-i] = p
	}
	addr, err := netip.ParseAddr(strings.Join(reversed, "."))
	if err != nil {
		return netip.Addr{}, false
	}
	return addr, true
}

// ReverseZones returns the in-addr.arpa zones covering the fake range, for
// resolver configuration.
func ReverseZones(prefix netip.Prefix) []string {
	if !prefix.Addr().Is4() || prefix.Bits() < 8 {
		return nil
	}
	b := prefix.Masked().Addr().As4()
	switch {
	case prefix.Bits() >= 24:
		return []string{fmt.Sprintf("%d.%d.%d.in-addr.arpa", b[2], b[1], b[0])}
	case prefix.Bits() >= 16:
		return []string{fmt.Sprintf("%d.%d.in-addr.arpa", b[1], b[0])}
	default:
		return []string{fmt.Sprintf("%d.in-addr.arpa", b[0])}
	}
}
