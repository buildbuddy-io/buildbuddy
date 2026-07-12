package server

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/watcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/maxmind"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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

	// zoneFileInvalidAlert is the constant alert name emitted when a single GCS
	// zone-file object fails to parse or is missing its SOA. It is a Prometheus
	// metric label, so it must stay constant -- the offending object name goes
	// in the alert's log message, not the label.
	zoneFileInvalidAlert = "dns_zone_file_invalid"

	// zoneFileSuffix is the object-name suffix that marks a bucket object as a
	// zone file to serve. Objects without it are ignored.
	zoneFileSuffix = ".zone"
)

// Handler is a DNS handler serving a set of authoritative zones. The served
// records and zones live in an immutable zoneData snapshot behind an atomic
// pointer, so a watcher (e.g. one polling zone files from GCS) can hot-swap the
// entire set with Update while in-flight requests continue to read a consistent
// prior snapshot without locking.
type Handler struct {
	// data holds the current immutable zone snapshot. ServeDNS loads it once per
	// request; Update atomically replaces it. Never nil after NewHandler.
	data atomic.Pointer[zoneData]

	// acme, when non-nil, makes this server self-host ACME DNS-01 validation:
	// it accepts RFC2136 UPDATEs for in-zone _acme-challenge.* TXT records and
	// answers queries for them from the blobstore-backed store.
	acme *Challenges

	env environment.Env
}

// zoneData is an immutable snapshot of the records and zones the handler serves.
// It is never mutated after construction (buildZoneData); a new snapshot is
// built and swapped in wholesale by Handler.Update, so readers that loaded an
// earlier pointer keep seeing a consistent set.
type zoneData struct {
	records map[string][]dns.RR

	// zones are the authoritative zones we serve (one per SOA record), sorted
	// most-specific first so the first apex that encloses a queried name is the
	// closest one. Each carries the SOA we attach to negative answers for names
	// under it, and its apex lets us recognize which names we're authoritative
	// for. Empty when no zone file supplied an SOA, in which case zone
	// membership is not enforced (see zoneFor).
	zones []zone
}

// zone is a single authoritative zone: its apex (the canonical SOA owner) and
// the SOA record itself. The SOA is attached to negative answers for names in
// the zone so resolvers can negatively cache them under the right authority.
type zone struct {
	apex string
	soa  dns.RR
}

// Update atomically replaces the served zone data with a snapshot built from
// resources. Called by a zone-file watcher when the underlying files change; it
// takes effect for requests that load the snapshot after this returns, while
// in-flight requests finish against the snapshot they already loaded.
func (h *Handler) Update(resources []dns.RR) {
	h.data.Store(buildZoneData(resources))
}

// zoneFor returns the zone most specifically enclosing name (the longest apex
// that is a suffix of name), or nil if name falls under none of our zones. It
// also returns nil when we serve no zones at all (no SOA was loaded); callers
// treat that as "membership not enforced" and fall back to plain record lookup.
func (d *zoneData) zoneFor(name string) *zone {
	for i := range d.zones {
		if dns.IsSubDomain(d.zones[i].apex, name) {
			return &d.zones[i]
		}
	}
	return nil
}

func (h *Handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	start := time.Now()
	// Load the zone snapshot once up front so every decision in this request --
	// zone routing, resolution, negative-SOA selection -- sees one consistent
	// set even if a watcher swaps in new data mid-request.
	d := h.data.Load()
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	recordType := "NO_QUESTION"
	if r.Opcode == dns.OpcodeUpdate {
		recordType = "UPDATE"
	} else if len(r.Question) >= 1 {
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
		qName := "."
		if len(r.Question) >= 1 {
			qName = r.Question[0].Name
		}
		log.Debugf("dns: client=%s type=%s name=%q rcode=%s answers=%d dur=%s",
			w.RemoteAddr(), recordType, qName, rcodeLabel(m.Rcode), len(m.Answer), time.Since(start))
	}()

	// RFC2136 dynamic UPDATE (used by cert-manager to set/remove the ACME
	// _acme-challenge TXT records we self-host) is handled separately.
	// serveUpdate replies on its own message, so surface its rcode on m for the
	// metric + log above.
	if r.Opcode == dns.OpcodeUpdate {
		m.Rcode = h.serveUpdate(w, r)
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

	// Route the name to its enclosing zone once, up front: it selects the SOA
	// for any negative answer below, and decides authority. If we serve zones
	// but this name is under none of them, we are not authoritative for it, so
	// answer REFUSED rather than an NXDOMAIN we have no zone SOA to anchor. When
	// no zones are configured, zoneFor is nil for everything and this gate is
	// skipped -- resolution proceeds over the records as an SOA-less set.
	z := d.zoneFor(qName)
	if len(d.zones) > 0 && z == nil {
		m.Rcode = dns.RcodeRefused
		m.Authoritative = false
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write REFUSED DNS response for %q: %s", qName, err)
		}
		return
	}

	// Self-hosted ACME: answer in-zone _acme-challenge.* names from the
	// blobstore-backed store (records are set/removed via RFC2136 UPDATE). A name
	// with a current value is answered; one without is authoritative NODATA.
	if h.acme != nil && d.isACMEChallengeName(qName) {
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
		if z != nil && z.soa != nil {
			m.Ns = append(m.Ns, z.soa)
		}
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write ACME challenge NODATA for %q: %s", qName, err)
		}
		return
	}

	var negative bool
	m.Answer, m.Rcode, negative = d.resolve(qName, qType)

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
	// of the requested type), include the enclosing zone's SOA in the authority
	// section so resolvers can negatively cache it.
	if negative && z != nil && z.soa != nil {
		m.Ns = append(m.Ns, z.soa)
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
func (d *zoneData) resolve(qName string, qType uint16) ([]dns.RR, int, bool) {
	var answer []dns.RR
	name := qName
	for i := 0; i < maxCNAMEDepth; i++ {
		records, ok := d.lookup(name)
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
func (d *zoneData) lookup(name string) ([]dns.RR, bool) {
	if rrs, ok := d.records[name]; ok {
		return rrs, true
	}
	for off, end := dns.NextLabel(name, 0); !end; off, end = dns.NextLabel(name, off) {
		if rrs, ok := d.records["*."+name[off:]]; ok {
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
// name (leftmost label _acme-challenge, under one of our zone apexes). name is
// canonicalized (lowercase, fqdn) by ServeDNS and acmeChallengePrefix is
// lowercase, so the prefix check identifies the leftmost label without
// allocating.
func (d *zoneData) isACMEChallengeName(name string) bool {
	return strings.HasPrefix(name, acmeChallengePrefix) && d.zoneFor(name) != nil
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
func (h *Handler) serveUpdate(w dns.ResponseWriter, r *dns.Msg) int {
	m := new(dns.Msg)
	m.SetReply(r)

	d := h.data.Load()
	reqTSIG := r.IsTsig()
	// TsigStatus() is nil for an unsigned message too, so also require that the
	// request actually carried a TSIG.
	if h.acme == nil || reqTSIG == nil || w.TsigStatus() != nil {
		m.SetRcode(r, dns.RcodeNotAuth)
		writeUpdateReply(w, m, nil)
		return dns.RcodeNotAuth
	}

	// RFC2136 updates are all-or-nothing, so validate every record before
	// applying any: a record we'd refuse (wrong type/name, or an unsupported
	// class) must reject the whole update, not leave an earlier record applied.
	// The RRset-delete form (class ANY) arrives as a *dns.ANY with Rrtype TXT, so
	// dispatch on the header rather than the concrete type.
	for _, rr := range r.Ns { // RFC2136: the records to apply live in the Ns section.
		hdr := rr.Header()
		if hdr.Rrtype != dns.TypeTXT || !d.isACMEChallengeName(dns.CanonicalName(hdr.Name)) {
			m.SetRcode(r, dns.RcodeRefused)
			writeUpdateReply(w, m, reqTSIG)
			return dns.RcodeRefused
		}
		switch hdr.Class {
		case dns.ClassINET, dns.ClassNONE, dns.ClassANY:
		default:
			m.SetRcode(r, dns.RcodeRefused)
			writeUpdateReply(w, m, reqTSIG)
			return dns.RcodeRefused
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
	return rcode
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
//
// The served records can later be replaced wholesale via Handler.Update (e.g.
// by a watcher polling zone files from GCS).
func NewHandler(env environment.Env, resources []dns.RR, acme *Challenges) *Handler {
	h := &Handler{
		acme: acme,
		env:  env,
	}
	h.data.Store(buildZoneData(resources))
	return h
}

// buildZoneData builds an immutable zone snapshot from resources: an
// owner-name-indexed record map and the list of zones (one per SOA), sorted
// most-specific first.
func buildZoneData(resources []dns.RR) *zoneData {
	records := make(map[string][]dns.RR, len(resources))
	var zones []zone
	for _, rr := range resources {
		name := dns.CanonicalName(rr.Header().Name)
		records[name] = append(records[name], rr)
		// Each zone file contributes one SOA at its apex; several files (several
		// zones) contribute several. Later routing picks the closest enclosing
		// one per query.
		if rr.Header().Rrtype == dns.TypeSOA {
			zones = append(zones, zone{apex: name, soa: rr})
		}
	}
	// Sort most-specific first (descending apex label count) so zoneFor's first
	// suffix match is the closest enclosing zone -- e.g. a query under a child
	// zone sub.example.com. is anchored on its SOA, not the parent example.com.
	sort.SliceStable(zones, func(i, j int) bool {
		return dns.CountLabel(zones[i].apex) > dns.CountLabel(zones[j].apex)
	})
	return &zoneData{
		records: records,
		zones:   zones,
	}
}

// ParseZoneFile reads the resource records from a zone file.
//
// Owner names must be fully qualified (or qualified by a "$ORIGIN" directive in
// the file itself); a relative name with no origin to qualify it against is a
// parse error. This matches the zone files we serve, which are exported from
// Cloud DNS with fully-qualified names.
func ParseZoneFile(fileName string) ([]dns.RR, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	return ParseZoneBytes(data, filepath.Base(fileName))
}

// ParseZoneBytes parses the resource records from the bytes of a zone file.
// name is used only to label parse errors (typically the file or object name).
// See ParseZoneFile for owner-name qualification rules.
func ParseZoneBytes(data []byte, name string) ([]dns.RR, error) {
	// The empty origin means relative names can only be qualified by an in-file
	// "$ORIGIN" directive; otherwise they error rather than being silently
	// mis-qualified. bytes.NewReader reads over data in place (no string copy),
	// which matters on the watcher's reload path.
	parser := dns.NewZoneParser(bytes.NewReader(data), "", name)
	records := make([]dns.RR, 0)
	for rr, ok := parser.Next(); ok; rr, ok = parser.Next() {
		records = append(records, rr)
	}
	if err := parser.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// ParseAndVerifyZone parses a zone file's bytes and requires it to define an SOA
// at its apex. A file without an SOA contributes no apex, so its names would
// route to no zone and be answered REFUSED even though they loaded -- a silent
// failure -- so it is rejected here. name labels errors (file or object name).
func ParseAndVerifyZone(data []byte, name string) ([]dns.RR, error) {
	records, err := ParseZoneBytes(data, name)
	if err != nil {
		return nil, err
	}
	if !hasSOA(records) {
		return nil, status.FailedPreconditionErrorf("zone file %q has no SOA record at its apex", name)
	}
	return records, nil
}

// ParseAndVerifyZoneFile reads a zone file and requires it to define an SOA at
// its apex (see ParseAndVerifyZone).
func ParseAndVerifyZoneFile(fileName string) ([]dns.RR, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	return ParseAndVerifyZone(data, filepath.Base(fileName))
}

// hasSOA reports whether rrs contains an SOA record.
func hasSOA(rrs []dns.RR) bool {
	for _, rr := range rrs {
		if rr.Header().Rrtype == dns.TypeSOA {
			return true
		}
	}
	return false
}

// WatchZoneFiles serves, via handler, every ".zone" object in store's bucket,
// reloading them whenever they change. Each object is parsed and verified
// independently: one that fails is skipped (and alerted) while the rest keep
// serving their last-good records. It is non-fatal -- an empty or unreachable
// bucket just comes up serving nothing and keeps watching -- and returns after
// the initial best-effort load; changes thereafter are applied in the
// background until ctx is cancelled.
func WatchZoneFiles(ctx context.Context, handler *Handler, store watcher.Store, interval time.Duration) {
	u := &zoneUpdater{handler: handler, files: make(map[string][]dns.RR)}
	watcher.New(store, zoneFileSuffix, interval, u.apply).Start(ctx)
}

// zoneUpdater turns the raw object deltas a watcher emits into served DNS
// records. It parses each changed object, verifies its SOA, keeps the last-good
// records of any that fail (and alerts), drops removed objects, and swaps the
// assembled record set into the handler. It is driven only from the watcher's
// single poll goroutine, so its files map needs no lock.
type zoneUpdater struct {
	handler *Handler
	files   map[string][]dns.RR // last-good records per object name
}

// apply is the watcher's onUpdate callback. Because the watcher re-delivers an
// object only when its content changes, a parse failure here alerts once per
// bad version rather than on every poll.
func (u *zoneUpdater) apply(up watcher.Update) {
	changed := false
	for name, data := range up.Changed {
		records, err := ParseAndVerifyZone(data, name)
		if err != nil {
			// One bad file must not disturb the others: alert so we can fix it,
			// and keep this object's last-good records, if any.
			alert.UnexpectedEvent(zoneFileInvalidAlert, "zone file %q rejected, keeping last-good: %s", name, err)
			continue
		}
		u.files[name] = records
		changed = true
	}
	for _, name := range up.Removed {
		if _, ok := u.files[name]; ok {
			delete(u.files, name)
			changed = true
		}
	}
	if !changed {
		// Every changed object was rejected (last-good kept) and nothing served
		// was removed: the served set is unchanged, so don't swap or log.
		return
	}
	u.handler.Update(u.assemble())
	log.Infof("dns: serving %d zone file(s) from GCS", len(u.files))
}

// assemble builds the full record set to serve: every last-good object's
// records, ordered by object name so the resulting snapshot is deterministic.
func (u *zoneUpdater) assemble() []dns.RR {
	names := make([]string, 0, len(u.files))
	for name := range u.files {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []dns.RR
	for _, name := range names {
		out = append(out, u.files[name]...)
	}
	return out
}
