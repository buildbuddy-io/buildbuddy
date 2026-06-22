package server

import (
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	"github.com/miekg/dns"
)

// maxCNAMEDepth bounds how many CNAME hops we follow before giving up
// guarding against CNAME loops.
const maxCNAMEDepth = 8

type handler struct {
	records map[string][]dns.RR

	// soa is the zone's SOA record. We attach it to "no such answer" responses
	// (the name doesn't exist, or has no record of the requested type); it tells
	// the asking resolver how long it may remember that negative result instead
	// of re-asking us every time. Nil if the zone file has no SOA record.
	soa dns.RR
}

func (h *handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	if len(r.Question) != 1 {
		m.Rcode = dns.RcodeFormatError
		if err := w.WriteMsg(m); err != nil {
			log.Warningf("Failed to write FORMERR DNS response: %s", err)
		}
		return
	}

	qName := dns.CanonicalName(r.Question[0].Name)
	qType := r.Question[0].Qtype

	var negative bool
	m.Answer, m.Rcode, negative = h.resolve(qName, qType)

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

func filterByType(records []dns.RR, qType uint16) []dns.RR {
	var out []dns.RR
	for _, rr := range records {
		if rr.Header().Rrtype == qType {
			out = append(out, rr)
		}
	}
	return out
}

func NewHandler(resources []dns.RR) dns.Handler {
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
	// Next() returns ok=false both at EOF and on a parse error; the error is
	// only surfaced via Err(). Without this check a malformed zone file would
	// yield a truncated record set with a nil error. With an empty origin this
	// also covers relative names: the parser can't qualify them and reports a
	// "bad owner name" error here.
	if err := parser.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
