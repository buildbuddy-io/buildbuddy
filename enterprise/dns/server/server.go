package server

import (
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/maxmind"

	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// maxCNAMEDepth bounds how many CNAME hops we follow before giving up
	// guarding against CNAME loops.
	maxCNAMEDepth = 8

	// asnRoutingExperiment is the name of an experiment that maps a client's
	// ASN to an override A record.
	asnRoutingExperiment = "dns.asn_routing"

	// asnRoutingTTL is the TTL applied to experiment-routed A answers. It is
	// short so routing decisions can change quickly.
	asnRoutingTTL = 60

	// acmeChallengeLabel is the leftmost label of an ACME DNS-01 validation
	// name (the record is always _acme-challenge.<domain being validated>).
	acmeChallengeLabel = "_acme-challenge"

	// acmeReferralTTL is the TTL on the NS records of an _acme-challenge
	// referral. Short, since challenges are transient.
	acmeReferralTTL = 300
)

type handler struct {
	records map[string][]dns.RR

	// soa is the zone's SOA record. We attach it to "no such answer" responses
	// (the name doesn't exist, or has no record of the requested type); it
	// tells the asking resolver how long it may remember that negative result
	// instead of re-asking us every time. Nil if the zone file has no SOA.
	soa dns.RR

	// apex is the canonical zone apex (the SOA owner). NS records here describe
	// this zone; NS records below it are delegations (zone cuts). Empty if the
	// zone file has no SOA, in which case we can't tell the two apart and treat
	// nothing as a delegation.
	apex string

	// acmeChallengeNS, when non-empty, are the nameservers to which any in-zone
	// _acme-challenge.* query is referred. This delegates ACME DNS-01 validation
	// to a dynamic DNS provider (e.g. Cloud DNS) for every name in the zone
	// without a per-name record -- the leftmost label is always _acme-challenge.
	acmeChallengeNS []string

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

	// Dynamic ACME delegation: refer any in-zone _acme-challenge.* name to the
	// configured nameservers, so DNS-01 validation for every cert in the zone
	// (current and future) is handled by a dynamic provider with no per-name
	// record here.
	if ns := h.acmeChallengeReferral(qName); len(ns) > 0 {
		m.Authoritative = false
		m.Ns = append(m.Ns, ns...)
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write ACME challenge referral for %q: %s", qName, err)
		}
		return
	}

	// If the name lies at or below a zone cut (an in-zone NS record below the
	// apex), we are not authoritative for it: return a referral to the child's
	// nameservers instead of answering from this zone. This is how we hand a
	// subdomain off to another DNS provider -- e.g. delegating an ACME
	// _acme-challenge validation name back to a cloud DNS zone.
	//
	// TODO(tylerw): A DS query AT a zone cut is the exception: the DS RRset is
	// parent-owned authoritative data and must be answered here (or NODATA+SOA),
	// not referred down to the child (RFC 4035 sec. 3.1.4.1). DS queries strictly
	// below the cut still refer, so the guard is "for DS, evaluate the delegation
	// from qName's parent label", not a blanket skip. Inert until we serve
	// DNSSEC (no DS/RRSIG today), and a full fix also needs signed DS/RRSIG/NSEC.
	if ns := h.delegation(qName); len(ns) > 0 {
		m.Authoritative = false
		m.Ns = append(m.Ns, ns...)
		m.Extra = append(m.Extra, h.glue(ns)...)
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write DNS referral for %q: %s", qName, err)
		}
		return
	}

	var negative bool
	m.Answer, m.Rcode, negative = h.resolve(qName, qType)

	// ASN-based routing: when the name already resolves to an address in our
	// zone, an experiment may override that address for this client.
	// TODO(tylerw): Consider supporting more than just A records here?
	if qType == dns.TypeA && onlyARecords(m.Answer) {
		if efp := h.env.GetExperimentFlagProvider(); efp != nil {
			if rrs := asnRoutedAnswer(efp, w, r, qName); len(rrs) > 0 {
				m.Answer = rrs
			}
			// Echo the EDNS Client Subnet scope (RFC 7871) so an ECS-aware
			// resolver caches this answer per-subnet and NOT globally.
			attachClientSubnetScopeIfPresent(m, r)
		}
	}

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
func (h *handler) resolve(qName string, qType uint16) ([]dns.RR, int, bool) {
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

// onlyARecords reports whether rrs is a non-empty set consisting solely of A
// records, i.e. a direct address answer (not NODATA, NXDOMAIN, or a CNAME).
func onlyARecords(rrs []dns.RR) bool {
	if len(rrs) == 0 {
		return false
	}
	for _, rr := range rrs {
		if rr.Header().Rrtype != dns.TypeA {
			return false
		}
	}
	return true
}

// asnRoutingConfig is the value shape of the dns.asn_routing experiment object.
type asnRoutingConfig struct {
	Addresses []string `json:"addresses"`
}

// asnRoutedAnswer evaluates the ASN-routing experiment for qName and returns
// the override A records, or nil when the experiment yields no addresses (the
// caller then keeps the static zone answer).
func asnRoutedAnswer(efp interfaces.ExperimentFlagProvider, w dns.ResponseWriter, r *dns.Msg, qName string) []dns.RR {
	asn := clientASN(w, r)
	obj := efp.Object(context.Background(), asnRoutingExperiment, nil,
		experiments.WithContext("asn", int64(asn)),
		experiments.WithContext("name", qName))
	if len(obj) == 0 {
		return nil
	}

	var cfg asnRoutingConfig
	if err := experiments.ObjectToStruct(obj, &cfg); err != nil {
		log.Warningf("ASN-routing experiment for %q (ASN %d) has an invalid config %v: %s", qName, asn, obj, err)
		return nil
	}

	var answer []dns.RR
	for _, addr := range cfg.Addresses {
		v4 := net.ParseIP(addr).To4()
		if v4 == nil {
			log.Warningf("ASN-routing experiment for %q (ASN %d) has invalid IPv4 %q; skipping it", qName, asn, addr)
			continue
		}
		answer = append(answer, &dns.A{
			Hdr: dns.RR_Header{Name: qName, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: asnRoutingTTL},
			A:   v4,
		})
	}
	return answer
}

// attachClientSubnetScopeIfPresent, when the query carried an EDNS Client
// Subnet option, adds it to the response with the SCOPE prefix-length set to
// the source prefix-length (RFC 7871). This tells the resolver the answer is
// specific to that subnet so it caches per-subnet rather than globally. A "no
// subnet" query (source /0) echoes scope /0, which is correct: that answer was
// not tailored to a client subnet.
func attachClientSubnetScopeIfPresent(m, r *dns.Msg) {
	reqOPT := r.IsEdns0()
	if reqOPT == nil {
		return
	}
	for _, o := range reqOPT.Option {
		ecs, ok := o.(*dns.EDNS0_SUBNET)
		if !ok {
			continue
		}
		udpSize := reqOPT.UDPSize()
		if udpSize < dns.MinMsgSize {
			udpSize = dns.MinMsgSize
		}
		respOPT := &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT}}
		respOPT.SetUDPSize(udpSize)
		respOPT.Option = append(respOPT.Option, &dns.EDNS0_SUBNET{
			Code:          dns.EDNS0SUBNET,
			Family:        ecs.Family,
			SourceNetmask: ecs.SourceNetmask,
			SourceScope:   ecs.SourceNetmask,
			Address:       ecs.Address,
		})
		m.Extra = append(m.Extra, respOPT)
		return
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

// delegation returns the NS records at the closest zone cut enclosing name, or
// nil if name is not under a delegation. A zone cut is any owner below the apex
// that carries NS records; the apex's own NS records describe this zone, not a
// delegation, so they are never treated as one. Walking up from name, the first
// such owner is the cut, since a correct parent holds no data below it.
func (h *handler) delegation(name string) []dns.RR {
	if h.apex == "" {
		return nil
	}
	for n := name; n != h.apex; {
		if ns := filterByType(h.records[n], dns.TypeNS); len(ns) > 0 {
			return ns
		}
		off, end := dns.NextLabel(n, 0)
		if end {
			break
		}
		n = n[off:]
	}
	return nil
}

// acmeChallengeReferral returns NS records delegating name to the configured
// ACME nameservers, or nil if ACME delegation is unconfigured or name is not an
// in-zone _acme-challenge.* name. The referral is synthesized per query (owned
// by name) so a single config covers every _acme-challenge name in the zone.
func (h *handler) acmeChallengeReferral(name string) []dns.RR {
	if len(h.acmeChallengeNS) == 0 || h.apex == "" {
		return nil
	}
	if !dns.IsSubDomain(h.apex, name) {
		return nil
	}
	labels := dns.SplitDomainName(name)
	if len(labels) == 0 || !strings.EqualFold(labels[0], acmeChallengeLabel) {
		return nil
	}
	out := make([]dns.RR, 0, len(h.acmeChallengeNS))
	for _, ns := range h.acmeChallengeNS {
		out = append(out, &dns.NS{
			Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: acmeReferralTTL},
			Ns:  ns,
		})
	}
	return out
}

// glue returns the in-zone (in-bailiwick) address records for the targets of
// the given NS records, for the additional section of a referral. Out-of-zone
// targets -- the usual case for our delegations -- contribute no glue and are
// resolved by the asking resolver itself.
func (h *handler) glue(nsRecords []dns.RR) []dns.RR {
	var out []dns.RR
	for _, rr := range nsRecords {
		ns, ok := rr.(*dns.NS)
		if !ok {
			continue
		}
		for _, a := range h.records[dns.CanonicalName(ns.Ns)] {
			switch a.Header().Rrtype {
			case dns.TypeA, dns.TypeAAAA:
				out = append(out, a)
			}
		}
	}
	return out
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
			// A "no client subnet" ECS (family 0, or a /0 scope) carries an
			// unspecified address (0.0.0.0 / ::); skip it so we fall back to the
			// transport source rather than looking up the all-zeros address.
			if addr, ok := netip.AddrFromSlice(ecs.Address); ok {
				if addr = addr.Unmap(); !addr.IsUnspecified() {
					return addr
				}
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

// NewHandler builds a DNS handler serving resources. acmeChallengeNameservers,
// if non-empty, are the nameservers to which any in-zone _acme-challenge.* query
// is referred (delegating ACME DNS-01 validation to a dynamic provider).
func NewHandler(env environment.Env, resources []dns.RR, acmeChallengeNameservers []string) dns.Handler {
	records := make(map[string][]dns.RR, len(resources))
	var soa dns.RR
	for _, rr := range resources {
		name := dns.CanonicalName(rr.Header().Name)
		records[name] = append(records[name], rr)
		if rr.Header().Rrtype == dns.TypeSOA && soa == nil {
			soa = rr
		}
	}
	apex := ""
	if soa != nil {
		apex = dns.CanonicalName(soa.Header().Name)
	}
	var acmeNS []string
	for _, ns := range acmeChallengeNameservers {
		if ns = strings.TrimSpace(ns); ns != "" {
			acmeNS = append(acmeNS, dns.Fqdn(ns))
		}
	}
	return &handler{
		records:         records,
		soa:             soa,
		apex:            apex,
		acmeChallengeNS: acmeNS,
		env:             env,
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
