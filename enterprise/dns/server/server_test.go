package server

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/miekg/dns"
)

// zone is a small stand-in for the real buildbuddy.io zone, exercising exact
// records, wildcards (including a more-specific nested wildcard), and CNAMEs
// pointing both out-of-zone and in-zone.
var zone = []string{
	"buildbuddy.io. 60 IN SOA ns1.example. host.example. 1 21600 3600 259200 300",
	"buildbuddy.io. 60 IN A 34.82.173.239",
	"cache.buildbuddy.io. 60 IN A 1.2.3.4",
	"lb.buildbuddy.io. 60 IN A 1.1.1.1",
	"lb.buildbuddy.io. 60 IN A 2.2.2.2",
	"*.buildbuddy.io. 60 IN A 9.9.9.9",
	"*.us-west1.buildbuddy.io. 60 IN A 8.8.8.8",
	"*.aws.buildbuddy.io. 60 IN CNAME elb.amazonaws.example.",
	"www.buildbuddy.io. 60 IN CNAME external.github.io.",
	"alias.buildbuddy.io. 60 IN CNAME cache.buildbuddy.io.",
}

func newTestHandler(t *testing.T) dns.Handler {
	records := make([]dns.RR, 0, len(zone))
	for _, line := range zone {
		rr, err := dns.NewRR(line)
		require.NoError(t, err, "parsing %q", line)
		records = append(records, rr)
	}
	return NewHandler(records)
}

// fakeResponseWriter captures the message written by the handler.
type fakeResponseWriter struct {
	msg *dns.Msg
}

func (w *fakeResponseWriter) WriteMsg(m *dns.Msg) error    { w.msg = m; return nil }
func (w *fakeResponseWriter) LocalAddr() net.Addr          { return &net.UDPAddr{} }
func (w *fakeResponseWriter) RemoteAddr() net.Addr         { return &net.UDPAddr{} }
func (w *fakeResponseWriter) Write([]byte) (int, error)    { return 0, nil }
func (w *fakeResponseWriter) Close() error                 { return nil }
func (w *fakeResponseWriter) TsigStatus() error            { return nil }
func (w *fakeResponseWriter) TsigTimersOnly(bool)          {}
func (w *fakeResponseWriter) Hijack()                      {}

func query(t *testing.T, h dns.Handler, name string, qType uint16) *dns.Msg {
	t.Helper()
	req := new(dns.Msg)
	req.SetQuestion(dns.Fqdn(name), qType)
	w := &fakeResponseWriter{}
	h.ServeDNS(w, req)
	require.NotNil(t, w.msg, "handler wrote no response")
	return w.msg
}

// answerStrings returns the rdata of each answer RR as "owner|type|value"
// triples for convenient assertions.
func aData(answers []dns.RR) []string {
	var out []string
	for _, rr := range answers {
		switch v := rr.(type) {
		case *dns.A:
			out = append(out, rr.Header().Name+"|A|"+v.A.String())
		case *dns.CNAME:
			out = append(out, rr.Header().Name+"|CNAME|"+v.Target)
		default:
			out = append(out, rr.String())
		}
	}
	return out
}

func TestExactMatch(t *testing.T) {
	h := newTestHandler(t)
	m := query(t, h, "cache.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.True(t, m.Authoritative)
	assert.Equal(t, []string{"cache.buildbuddy.io.|A|1.2.3.4"}, aData(m.Answer))
}

func TestWildcardMatch(t *testing.T) {
	h := newTestHandler(t)
	m := query(t, h, "random123.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	// Answer is synthesized with the queried name as owner, not "*.…".
	assert.Equal(t, []string{"random123.buildbuddy.io.|A|9.9.9.9"}, aData(m.Answer))
}

func TestMostSpecificWildcardWins(t *testing.T) {
	h := newTestHandler(t)
	m := query(t, h, "node.us-west1.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []string{"node.us-west1.buildbuddy.io.|A|8.8.8.8"}, aData(m.Answer))
}

func TestWildcardDoesNotMutateStoredRecord(t *testing.T) {
	h := newTestHandler(t)
	// A wildcard query rewrites the owner name; ensure a second exact query
	// for the wildcard still reports the original "*.…" owner (i.e. the stored
	// RR wasn't mutated in place).
	query(t, h, "first.buildbuddy.io.", dns.TypeA)
	m := query(t, h, "*.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, []string{"*.buildbuddy.io.|A|9.9.9.9"}, aData(m.Answer))
}

func TestCNAMEOutOfZone(t *testing.T) {
	h := newTestHandler(t)
	// Querying A on a CNAME-only name returns the CNAME with NOERROR; the
	// out-of-zone target is left for the recursive resolver to chase.
	m := query(t, h, "www.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []string{"www.buildbuddy.io.|CNAME|external.github.io."}, aData(m.Answer))
}

func TestCNAMEInZoneFollowed(t *testing.T) {
	h := newTestHandler(t)
	// alias -> cache (in-zone), so we return both the CNAME and the resolved A.
	m := query(t, h, "alias.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []string{
		"alias.buildbuddy.io.|CNAME|cache.buildbuddy.io.",
		"cache.buildbuddy.io.|A|1.2.3.4",
	}, aData(m.Answer))
}

func TestCNAMEQueryNotChased(t *testing.T) {
	h := newTestHandler(t)
	// An explicit CNAME query returns just the CNAME, without chasing it.
	m := query(t, h, "alias.buildbuddy.io.", dns.TypeCNAME)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []string{"alias.buildbuddy.io.|CNAME|cache.buildbuddy.io."}, aData(m.Answer))
}

func TestNODATA(t *testing.T) {
	h := newTestHandler(t)
	// Name exists (has an A) but no AAAA: NOERROR + empty answer, SOA in authority.
	m := query(t, h, "buildbuddy.io.", dns.TypeAAAA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Empty(t, m.Answer)
	require.Len(t, m.Ns, 1)
	assert.Equal(t, dns.TypeSOA, m.Ns[0].Header().Rrtype)
}

func TestNXDOMAIN(t *testing.T) {
	h := newTestHandler(t)
	// A name covered by no record and no wildcard: NXDOMAIN, SOA in authority.
	m := query(t, h, "absent.example.com.", dns.TypeA)
	assert.Equal(t, dns.RcodeNameError, m.Rcode)
	assert.Empty(t, m.Answer)
	require.Len(t, m.Ns, 1)
	assert.Equal(t, dns.TypeSOA, m.Ns[0].Header().Rrtype)
}

func TestMultipleRecords(t *testing.T) {
	h := newTestHandler(t)
	// A name with several records of the queried type returns the whole RRset.
	m := query(t, h, "lb.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.ElementsMatch(t, []string{
		"lb.buildbuddy.io.|A|1.1.1.1",
		"lb.buildbuddy.io.|A|2.2.2.2",
	}, aData(m.Answer))
}

func TestSOAQuery(t *testing.T) {
	h := newTestHandler(t)
	// Querying SOA directly returns it in the answer section (not just authority).
	m := query(t, h, "buildbuddy.io.", dns.TypeSOA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.True(t, m.Authoritative)
	require.Len(t, m.Answer, 1)
	assert.Equal(t, dns.TypeSOA, m.Answer[0].Header().Rrtype)
}

func TestWildcardCNAME(t *testing.T) {
	h := newTestHandler(t)
	// Wildcard synthesis also applies to CNAME records.
	m := query(t, h, "x.aws.buildbuddy.io.", dns.TypeCNAME)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []string{"x.aws.buildbuddy.io.|CNAME|elb.amazonaws.example."}, aData(m.Answer))
}

func TestWildcardNODATA(t *testing.T) {
	h := newTestHandler(t)
	// A name that exists only via a wildcard, queried for a missing type: the
	// name "exists", so this is NODATA (NOERROR + SOA), not NXDOMAIN.
	m := query(t, h, "random123.buildbuddy.io.", dns.TypeAAAA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Empty(t, m.Answer)
	require.Len(t, m.Ns, 1)
	assert.Equal(t, dns.TypeSOA, m.Ns[0].Header().Rrtype)
}
