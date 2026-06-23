package server

import (
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/maxmind"

	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus"
)

// maxCNAMEDepth bounds how many CNAME hops we follow before giving up
// guarding against CNAME loops.
const maxCNAMEDepth = 8

// asnRoutingExperiment maps a client's ASN to an override A record. It is
// evaluated with the client's ASN and the queried name on the experiment
// context; when it returns a valid IPv4 address we answer with that address
// instead of the static zone record, letting us steer specific networks to
// specific endpoints.
const asnRoutingExperiment = "dns.asn_routing"

// asnRoutingTTL is the TTL applied to experiment-routed A answers. It is short
// so routing decisions can change quickly.
const asnRoutingTTL = 60

type handler struct {
	records map[string][]dns.RR

	// soa is the zone's SOA record. We attach it to "no such answer" responses
	// (the name doesn't exist, or has no record of the requested type); it
	// tells the asking resolver how long it may remember that negative result
	// instead of re-asking us every time. Nil if the zone file has no SOA.
	soa dns.RR

	// env provides the experiment flag provider used for (e.g.) ASN-based
	// routing. The provider may be unconfigured, in which case flags resolve to
	// their default values.
	env environment.Env
}

func (h *handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	start := time.Now()
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	recordType := "NO_QUESTION"
	if len(r.Question) >= 1 {
		recordType = recordTypeLabel(r.Question[0].Qtype)
	}
	defer func() {
		metrics.DNSServerRequestCount.With(prometheus.Labels{
			metrics.DNSRecordTypeLabel:   recordType,
			metrics.DNSResponseCodeLabel: rcodeLabel(m.Rcode),
		}).Inc()
		metrics.DNSServerHandlerDurationUsec.With(prometheus.Labels{
			metrics.DNSRecordTypeLabel: recordType,
		}).Observe(float64(time.Since(start).Microseconds()))
	}()

	if len(r.Question) != 1 {
		m.Rcode = dns.RcodeFormatError
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write FORMERR DNS response: %s", err)
		}
		return
	}

	qName := dns.CanonicalName(r.Question[0].Name)
	qType := r.Question[0].Qtype

	// Determine the client's ASN (from EDNS Client Subnet, falling back to the
	// transport source address) so resolution can route on it via experiments.
	asn := clientASN(w, r)

	var negative bool
	m.Answer, m.Rcode, negative = h.resolve(context.Background(), qName, qType, asn)

	// On a negative answer (NXDOMAIN, or NODATA: name exists but has no record
	// of the requested type), include the zone SOA in the authority section so
	// resolvers can negatively cache it.
	if negative && h.soa != nil {
		m.Ns = append(m.Ns, h.soa)
	}

	if err := w.WriteMsg(m); err != nil {
		log.Warningf("Failed to write DNS response for %q: %s", qName, err)
	}
}

// resolve looks up the answer for (qName, qType), expanding wildcards and
// following in-zone CNAME chains. It returns the answer RRs, the response code,
// and whether the result is negative for the queried type (NXDOMAIN, or NODATA:
// the name exists but has no record of the requested type) and so should carry
// the zone SOA for negative caching. A CNAME chain that exits the zone is not
// negative: it is a normal partial answer the recursive resolver completes.
func (h *handler) resolve(ctx context.Context, qName string, qType uint16, asn uint32) ([]dns.RR, int, bool) {
	// ASN-based routing: an experiment may override the A answer for this client
	// (keyed on its ASN and the queried name). When it yields a valid address,
	// serve that instead of the static zone record.
	if qType == dns.TypeA {
		if rr := h.asnRoutedAnswer(ctx, qName, asn); rr != nil {
			return []dns.RR{rr}, dns.RcodeSuccess, false
		}
	}

	var answer []dns.RR
	name := qName
	for i := 0; i < maxCNAMEDepth; i++ {
		records, ok := h.lookup(name)
		if !ok {
			// The queried name itself not existing is NXDOMAIN. Reaching a
			// dead end while chasing a CNAME just means the chain continues
			// out-of-zone, which is a normal (NOERROR) authoritative answer.
			if i == 0 {
				return nil, dns.RcodeNameError, true
			}
			return answer, dns.RcodeSuccess, false
		}

		if matched := filterByType(records, qType); len(matched) > 0 {
			return append(answer, matched...), dns.RcodeSuccess, false
		}

		// No records of the requested type. If a CNAME lives here (and the
		// client didn't explicitly ask for the CNAME), return it and follow
		// its target.
		if qType != dns.TypeCNAME {
			if cnames := filterByType(records, dns.TypeCNAME); len(cnames) > 0 {
				answer = append(answer, cnames...)
				name = dns.CanonicalName(cnames[0].(*dns.CNAME).Target)
				continue
			}
		}

		// Name exists but has no record of the requested type: NODATA. This
		// holds whether we arrived directly or at the end of an in-zone CNAME
		// chain (in which case answer holds the CNAME RRs).
		return answer, dns.RcodeSuccess, true
	}
	// Exceeded the CNAME hop limit; return what we have without claiming the
	// result is authoritatively negative.
	return answer, dns.RcodeSuccess, false
}

// asnRoutedAnswer consults the ASN-routing experiment for an override A record
// for qName given the client's asn. It returns nil when no experiment provider
// is configured, the experiment returns nothing, or the value is not a valid
// IPv4 address (in which case the caller falls back to the static zone answer).
func (h *handler) asnRoutedAnswer(ctx context.Context, qName string, asn uint32) dns.RR {
	efp := h.env.GetExperimentFlagProvider()
	if efp == nil {
		return nil
	}
	ip := efp.String(ctx, asnRoutingExperiment, "",
		experiments.WithContext("asn", int64(asn)),
		experiments.WithContext("name", qName))
	if ip == "" {
		return nil
	}
	v4 := net.ParseIP(ip).To4()
	if v4 == nil {
		log.CtxWarningf(ctx, "ASN-routing experiment returned invalid IPv4 %q for %q (ASN %d); using static answer", ip, qName, asn)
		return nil
	}
	return &dns.A{
		Hdr: dns.RR_Header{Name: qName, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: asnRoutingTTL},
		A:   v4,
	}
}

// lookup returns the records for name. If there is no exact match, it walks up
// the tree looking for a covering wildcard (*.<parent>), preferring the most
// specific one. Wildcard matches are returned as copies owned by name (the
// queried name) rather than the literal "*.<parent>" owner.
//
// NOTE: this is a pragmatic approximation of RFC 4592. It does not implement
// the closest-encloser / empty-non-terminal rules, so a wildcard could match a
// name that a fully correct server would treat as an existing empty node. This
// holds for every name in the zones we serve, but isn't generally complete.
func (h *handler) lookup(name string) ([]dns.RR, bool) {
	if rrs, ok := h.records[name]; ok {
		return rrs, true
	}
	for off, end := dns.NextLabel(name, 0); !end; off, end = dns.NextLabel(name, off) {
		if rrs, ok := h.records["*."+name[off:]]; ok {
			out := make([]dns.RR, len(rrs))
			for i, rr := range rrs {
				cp := dns.Copy(rr)
				cp.Header().Name = name
				out[i] = cp
			}
			return out, true
		}
	}
	return nil, false
}

// recordTypeLabel maps a query type to a metric label, collapsing unknown
// types (which the client controls) to "OTHER" so cardinality stays bounded.
func recordTypeLabel(qType uint16) string {
	if s, ok := dns.TypeToString[qType]; ok {
		return s
	}
	return "OTHER"
}

// rcodeLabel maps a response code to a metric label, with the same bounding.
func rcodeLabel(rcode int) string {
	if s, ok := dns.RcodeToString[rcode]; ok {
		return s
	}
	return "OTHER"
}

func filterByType(records []dns.RR, qType uint16) []dns.RR {
	var out []dns.RR
	for _, rr := range records {
		if rr.Header().Rrtype == qType {
			out = append(out, rr)
		}
	}
	return out
}

// clientASN returns the autonomous system number of the client the query is on
// behalf of, for use as an experiment attribute. It returns 0 when the client
// network is unknown or absent from the ASN database (e.g. private IPs).
func clientASN(w dns.ResponseWriter, r *dns.Msg) uint32 {
	ip := clientIP(w, r)
	if !ip.IsValid() {
		return 0
	}
	asn, err := maxmind.LookupASN(ip)
	if err != nil {
		log.Debugf("ASN lookup for %s failed: %s", ip, err)
		return 0
	}
	return uint32(asn.Number)
}

// clientIP determines the network address of the end client. It prefers the
// EDNS Client Subnet, which a recursive resolver sets to the querying client's
// network, and falls back to the transport source address (which, behind a
// resolver, is the resolver itself). It returns the zero Addr when neither is
// available.
func clientIP(w dns.ResponseWriter, r *dns.Msg) netip.Addr {
	if opt := r.IsEdns0(); opt != nil {
		for _, o := range opt.Option {
			ecs, ok := o.(*dns.EDNS0_SUBNET)
			if !ok {
				continue
			}
			if addr, ok := netip.AddrFromSlice(ecs.Address); ok {
				return addr.Unmap()
			}
		}
	}
	return addrFromNetAddr(w.RemoteAddr())
}

// addrFromNetAddr extracts the IP from a UDP or TCP transport address. It
// returns the zero Addr for any other address type or a missing IP.
func addrFromNetAddr(a net.Addr) netip.Addr {
	var ip net.IP
	switch v := a.(type) {
	case *net.UDPAddr:
		ip = v.IP
	case *net.TCPAddr:
		ip = v.IP
	default:
		return netip.Addr{}
	}
	if addr, ok := netip.AddrFromSlice(ip); ok {
		return addr.Unmap()
	}
	return netip.Addr{}
}

func NewHandler(resources []dns.RR, env environment.Env) dns.Handler {
	records := make(map[string][]dns.RR, len(resources))
	var soa dns.RR
	for _, rr := range resources {
		name := dns.CanonicalName(rr.Header().Name)
		records[name] = append(records[name], rr)
		if rr.Header().Rrtype == dns.TypeSOA && soa == nil {
			soa = rr
		}
	}
	return &handler{
		records: records,
		soa:     soa,
		env:     env,
	}
}

// ParseZoneFile reads the resource records from a zone file.
//
// origin is the domain that relative owner names (including "@" and records
// under a "$ORIGIN"-less file) are qualified against. If origin is empty, owner
// names must be fully qualified or an error is returned.
func ParseZoneFile(fileName, origin string) ([]dns.RR, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	parser := dns.NewZoneParser(file, origin, filepath.Base(fileName))
	records := make([]dns.RR, 0)
	for rr, ok := parser.Next(); ok; rr, ok = parser.Next() {
		records = append(records, rr)
	}
	if err := parser.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
