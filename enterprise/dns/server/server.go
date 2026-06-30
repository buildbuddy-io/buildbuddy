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

	// acmeChallengePrefix matches the leftmost label of an ACME DNS-01
	// validation name, which is always _acme-challenge.<domain being validated>.
	acmeChallengePrefix = "_acme-challenge."

	// acmeTTL is the TTL on served _acme-challenge TXT records. Short, since
	// challenges are transient.
	acmeTTL = 60

	// acmeBlobstoreTimeout bounds the blobstore reads/writes made on the DNS
	// request path so a slow or hung backend can't block handler goroutines
	// indefinitely.
	acmeBlobstoreTimeout = 10 * time.Second
)

type handler struct {
	records map[string][]dns.RR

	// soa is the zone's SOA record. We attach it to "no such answer" responses
	// (the name doesn't exist, or has no record of the requested type); it
	// tells the asking resolver how long it may remember that negative result
	// instead of re-asking us every time. Nil if the zone file has no SOA.
	soa dns.RR

	// apex is the canonical zone apex (the SOA owner), used to recognize in-zone
	// names. Empty if the zone file has no SOA.
	apex string

	// acme, when non-nil, makes this server self-host ACME DNS-01 validation:
	// it accepts RFC2136 UPDATEs for in-zone _acme-challenge.* TXT records and
	// answers queries for them from the blobstore-backed store.
	acme *Challenges

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

	// RFC2136 dynamic UPDATE (used by cert-manager to set/remove the ACME
	// _acme-challenge TXT records we self-host) is handled separately.
	if r.Opcode == dns.OpcodeUpdate {
		h.serveUpdate(w, r)
		return
	}

	if len(r.Question) != 1 {
		m.Rcode = dns.RcodeFormatError
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write FORMERR DNS response: %s", err)
		}
		return
	}

	qName := dns.CanonicalName(r.Question[0].Name)
	qType := r.Question[0].Qtype

	// Self-hosted ACME: answer in-zone _acme-challenge.* names from the
	// blobstore-backed store (records are set/removed via RFC2136 UPDATE). A name
	// with a current value is answered; one without is authoritative NODATA.
	if h.acme != nil && h.isACMEChallengeName(qName) {
		if qType == dns.TypeTXT {
			ctx, cancel := context.WithTimeout(context.Background(), acmeBlobstoreTimeout)
			defer cancel()
			if vals := h.acme.TXT(ctx, qName); len(vals) > 0 {
				m.Answer = txtRecords(qName, vals)
				if err := w.WriteMsg(m); err != nil {
					log.Warningf("Failed to write ACME challenge answer for %q: %s", qName, err)
				}
				return
			}
		}
		if h.soa != nil {
			m.Ns = append(m.Ns, h.soa)
		}
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write ACME challenge NODATA for %q: %s", qName, err)
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
	ip := clientIP(w, r)
	asn := clientASN(ip)
	obj := efp.Object(context.Background(), asnRoutingExperiment, nil,
		experiments.WithContext("ip", ip.String()),
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

// isACMEChallengeName reports whether name is an in-zone ACME DNS-01 validation
// name (leftmost label _acme-challenge, under the apex). name is canonicalized
// (lowercase, fqdn) by ServeDNS and acmeChallengePrefix is lowercase, so the
// prefix check identifies the leftmost label without allocating.
func (h *handler) isACMEChallengeName(name string) bool {
	return h.apex != "" && strings.HasPrefix(name, acmeChallengePrefix) && dns.IsSubDomain(h.apex, name)
}

// txtRecords builds a TXT RRset owned by name from the given string values.
func txtRecords(name string, vals []string) []dns.RR {
	out := make([]dns.RR, 0, len(vals))
	for _, v := range vals {
		out = append(out, &dns.TXT{
			Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: acmeTTL},
			Txt: []string{v},
		})
	}
	return out
}

// serveUpdate handles RFC2136 dynamic UPDATE messages, used by cert-manager's
// rfc2136 solver to set and remove the _acme-challenge TXT records we self-host.
// It requires a valid TSIG signature and only ever touches in-zone
// _acme-challenge TXT names, so the TSIG key can't be used to rewrite the zone.
func (h *handler) serveUpdate(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)

	reqTSIG := r.IsTsig()
	// TsigStatus() is nil for an unsigned message too, so also require that the
	// request actually carried a TSIG.
	if h.acme == nil || reqTSIG == nil || w.TsigStatus() != nil {
		m.SetRcode(r, dns.RcodeNotAuth)
		writeUpdateReply(w, m, nil)
		return
	}

	// RFC2136 updates are all-or-nothing, so validate every record before
	// applying any: a record we'd refuse (wrong type/name, or an unsupported
	// class) must reject the whole update, not leave an earlier record applied.
	// The RRset-delete form (class ANY) arrives as a *dns.ANY with Rrtype TXT, so
	// dispatch on the header rather than the concrete type.
	for _, rr := range r.Ns { // RFC2136: the records to apply live in the Ns section.
		hdr := rr.Header()
		if hdr.Rrtype != dns.TypeTXT || !h.isACMEChallengeName(dns.CanonicalName(hdr.Name)) {
			m.SetRcode(r, dns.RcodeRefused)
			writeUpdateReply(w, m, reqTSIG)
			return
		}
		switch hdr.Class {
		case dns.ClassINET, dns.ClassNONE, dns.ClassANY:
		default:
			m.SetRcode(r, dns.RcodeRefused)
			writeUpdateReply(w, m, reqTSIG)
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), acmeBlobstoreTimeout)
	defer cancel()
	rcode := dns.RcodeSuccess
	for _, rr := range r.Ns {
		hdr := rr.Header()
		name := dns.CanonicalName(hdr.Name)
		var err error
		switch hdr.Class {
		case dns.ClassINET: // add to the RRset
			if txt, ok := rr.(*dns.TXT); ok {
				err = h.acme.Add(ctx, name, txt.Txt...)
			}
		case dns.ClassNONE: // delete these values from the RRset
			if txt, ok := rr.(*dns.TXT); ok {
				err = h.acme.Delete(ctx, name, txt.Txt...)
			}
		case dns.ClassANY: // delete the whole RRset
			err = h.acme.Delete(ctx, name)
		}
		if err != nil {
			log.Warningf("ACME update for %q failed: %s", name, err)
			rcode = dns.RcodeServerFailure
			break
		}
	}
	m.SetRcode(r, rcode)
	writeUpdateReply(w, m, reqTSIG)
}

// writeUpdateReply writes m, TSIG-signing it (with the request's key and
// algorithm) when the request was signed so the rfc2136 client accepts it.
func writeUpdateReply(w dns.ResponseWriter, m *dns.Msg, reqTSIG *dns.TSIG) {
	if reqTSIG != nil {
		m.SetTsig(reqTSIG.Hdr.Name, reqTSIG.Algorithm, 300, time.Now().Unix())
	}
	if err := w.WriteMsg(m); err != nil {
		log.Warningf("Failed to write DNS UPDATE reply: %s", err)
	}
}

// MsgAccept is the dns.Server message-accept policy for density. It mirrors
// miekg's default policy (accept QUERY and NOTIFY, ignore responses, reject the
// rest as NOTIMP) but additionally accepts dynamic UPDATE messages (opcode 5),
// which we use for self-hosted ACME DNS-01.
//
// This is required: miekg's default MsgAcceptFunc rejects UPDATE with NOTIMP
// *before* the handler runs, so without it the RFC2136 update path is
// unreachable and the server answers every UPDATE with NOTIMP.
func MsgAccept(dh dns.Header) dns.MsgAcceptAction {
	// QR bit (0x8000) set => this is a response; servers don't act on responses.
	if dh.Bits&(1<<15) != 0 {
		return dns.MsgIgnore
	}
	switch int(dh.Bits>>11) & 0xF { // opcode occupies bits 11-14
	case dns.OpcodeQuery, dns.OpcodeNotify, dns.OpcodeUpdate:
		return dns.MsgAccept
	default:
		return dns.MsgRejectNotImplemented
	}
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
func clientASN(ip netip.Addr) uint32 {
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

// NewHandler builds a DNS handler serving resources. acme, if non-nil,
// self-hosts ACME DNS-01: the handler accepts RFC2136 UPDATEs for
// _acme-challenge TXT records and answers queries for them from it.
func NewHandler(env environment.Env, resources []dns.RR, acme *Challenges) dns.Handler {
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
	return &handler{
		records: records,
		soa:     soa,
		apex:    apex,
		acme:    acme,
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
