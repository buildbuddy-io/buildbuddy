package server

import (
	"os"
	"path/filepath"

	"github.com/miekg/dns"
)

// maxCNAMEDepth bounds how many CNAME hops we follow before giving up
// guarding against CNAME loops.
const maxCNAMEDepth = 8

type handler struct {
	records map[string][]dns.RR

	// soa is the zone's apex SOA record, included in the authority section
	// of negative (NXDOMAIN/NODATA) responses so resolvers can cache them.
	// May be nil if the zone has no SOA.
	soa dns.RR
}

func (h *handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	if len(r.Question) != 1 {
		m.Rcode = dns.RcodeFormatError
		w.WriteMsg(m)
		return
	}

	qName := dns.CanonicalName(r.Question[0].Name)
	qType := r.Question[0].Qtype

	m.Answer, m.Rcode = h.resolve(qName, qType)

	// On a negative answer (NXDOMAIN, or NODATA: name exists but no records
	// of the requested type), include the zone SOA in the authority section
	// if we have one.
	if len(m.Answer) == 0 && h.soa != nil {
		m.Ns = append(m.Ns, h.soa)
	}

	w.WriteMsg(m)
}

// resolve looks up the answer for (qName, qType), expanding wildcards and
// following in-zone CNAME chains. It returns the answer RRs and the response
// code: NXDOMAIN if the queried name does not exist (and no wildcard covers
// it), otherwise NOERROR — which includes the NODATA case (name exists but has
// no records of the requested type), signalled by an empty answer.
func (h *handler) resolve(qName string, qType uint16) ([]dns.RR, int) {
	var answer []dns.RR
	name := qName
	for i := 0; i < maxCNAMEDepth; i++ {
		records, ok := h.lookup(name)
		if !ok {
			// The queried name itself not existing is NXDOMAIN. Reaching a
			// dead end while chasing a CNAME just means the chain continues
			// out-of-zone, which is a normal (NOERROR) authoritative answer.
			if i == 0 {
				return nil, dns.RcodeNameError
			}
			return answer, dns.RcodeSuccess
		}

		if matched := filterByType(records, qType); len(matched) > 0 {
			return append(answer, matched...), dns.RcodeSuccess
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

		// Name exists but has no records of the requested type: NODATA.
		return answer, dns.RcodeSuccess
	}
	return answer, dns.RcodeSuccess
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

func ParseZoneFile(fileName string) ([]dns.RR, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	parser := dns.NewZoneParser(file, "", filepath.Base(fileName))
	records := make([]dns.RR, 0)
	for rr, ok := parser.Next(); ok; rr, ok = parser.Next() {
		records = append(records, rr)
	}
	// Next() returns ok=false both at EOF and on a parse error; the error is
	// only surfaced via Err(). Without this check a malformed zone file would
	// yield a truncated record set with a nil error.
	if err := parser.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
