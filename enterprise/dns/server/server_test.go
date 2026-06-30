package server_test

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/maxmind"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/miekg/dns"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	"github.com/open-feature/go-sdk/openfeature"
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

func mustRecords(tb testing.TB) []dns.RR {
	tb.Helper()
	records := make([]dns.RR, 0, len(zone))
	for _, line := range zone {
		rr, err := dns.NewRR(line)
		require.NoError(tb, err, "parsing %q", line)
		records = append(records, rr)
	}
	return records
}

func newTestHandler(t *testing.T) dns.Handler {
	return server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), nil)
}

// fakeResponseWriter captures the message written by the handler and reports a
// configurable transport source address (remote). When remote is nil it reports
// an empty UDP address, i.e. no usable client IP. tsigStatus is what
// TsigStatus() reports -- the real miekg server sets this to the result of TSIG
// verification, so a non-nil value simulates a request signed with a wrong or
// unknown key.
type fakeResponseWriter struct {
	msg        *dns.Msg
	remote     net.Addr
	tsigStatus error
}

func (w *fakeResponseWriter) WriteMsg(m *dns.Msg) error { w.msg = m; return nil }
func (w *fakeResponseWriter) LocalAddr() net.Addr       { return &net.UDPAddr{} }
func (w *fakeResponseWriter) RemoteAddr() net.Addr {
	if w.remote != nil {
		return w.remote
	}
	return &net.UDPAddr{}
}
func (w *fakeResponseWriter) Write([]byte) (int, error) { return 0, nil }
func (w *fakeResponseWriter) Close() error              { return nil }
func (w *fakeResponseWriter) TsigStatus() error         { return w.tsigStatus }
func (w *fakeResponseWriter) TsigTimersOnly(bool)       {}
func (w *fakeResponseWriter) Hijack()                   {}

func newQuery(name string, qType uint16) *dns.Msg {
	req := new(dns.Msg)
	req.SetQuestion(dns.Fqdn(name), qType)
	return req
}

// withECS adds an EDNS Client Subnet option advertising clientIP as the end
// client's network.
func withECS(t *testing.T, req *dns.Msg, clientIP string) *dns.Msg {
	t.Helper()
	ip := net.ParseIP(clientIP)
	require.NotNil(t, ip, "bad client IP %q", clientIP)
	family, mask := uint16(1), uint8(32)
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	} else {
		family, mask = 2, 128
	}
	opt := &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT}}
	opt.Option = append(opt.Option, &dns.EDNS0_SUBNET{
		Code:          dns.EDNS0SUBNET,
		Family:        family,
		SourceNetmask: mask,
		Address:       ip,
	})
	req.Extra = append(req.Extra, opt)
	return req
}

// serve dispatches req to h with the response writer reporting remoteIP as the
// transport source (empty for none) and returns the response.
func serve(t *testing.T, h dns.Handler, req *dns.Msg, remoteIP string) *dns.Msg {
	t.Helper()
	w := &fakeResponseWriter{}
	if remoteIP != "" {
		ip := net.ParseIP(remoteIP)
		require.NotNil(t, ip, "bad remote IP %q", remoteIP)
		w.remote = &net.UDPAddr{IP: ip, Port: 53}
	}
	h.ServeDNS(w, req)
	require.NotNil(t, w.msg, "handler wrote no response")
	return w.msg
}

func query(t *testing.T, h dns.Handler, name string, qType uint16) *dns.Msg {
	t.Helper()
	return serve(t, h, newQuery(name, qType), "")
}

// queryFromIP issues a query carrying an EDNS Client Subnet option advertising
// clientIP as the end client's network, so resolution sees that client's ASN.
func queryFromIP(t *testing.T, h dns.Handler, name string, qType uint16, clientIP string) *dns.Msg {
	t.Helper()
	return serve(t, h, withECS(t, newQuery(name, qType), clientIP), "")
}

// newFlagProvider builds an experiment flag provider backed by an in-process
// flagd resolver reading the given flagd JSON config from a temp file.
func newFlagProvider(t *testing.T, flagConfig string) *experiments.FlagProvider {
	t.Helper()
	path := filepath.Join(t.TempDir(), "flags.flagd.json")
	require.NoError(t, os.WriteFile(path, []byte(flagConfig), 0o644))
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(path))
	require.NoError(t, err)
	require.NoError(t, openfeature.SetProviderAndWait(provider))
	fp, err := experiments.NewFlagProvider("dns-test")
	require.NoError(t, err)
	return fp
}

// handlerWithProvider builds a handler over the test zone whose env exposes fp.
func handlerWithProvider(t *testing.T, fp *experiments.FlagProvider) dns.Handler {
	t.Helper()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)
	return server.NewHandler(te, mustRecords(t), nil)
}

func TestASNRoutingExperiment(t *testing.T) {
	// Target the flag on whatever ASN the embedded DB assigns to 8.8.8.8, so the
	// test is robust to monthly DB updates.
	asn, err := maxmind.LookupASN(netip.MustParseAddr("8.8.8.8"))
	require.NoError(t, err)
	require.NotZero(t, asn.Number)

	flagConfig := fmt.Sprintf(`{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "dns.asn_routing": {
      "state": "ENABLED",
      "variants": { "override": { "addresses": ["10.20.30.40"] }, "none": {} },
      "defaultVariant": "none",
      "targeting": {
        "if": [ { "==": [ { "var": "asn" }, %d ] }, "override", "none" ]
      }
    }
  }
}`, asn.Number)

	h := handlerWithProvider(t, newFlagProvider(t, flagConfig))

	// A client on the targeted ASN (advertised via ECS) gets the override.
	m := queryFromIP(t, h, "cache.buildbuddy.io.", dns.TypeA, "8.8.8.8")
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"cache.buildbuddy.io.", "A", "10.20.30.40"}}, answers(m.Answer))

	// A client on a private network (unknown ASN) falls back to the static record.
	m = queryFromIP(t, h, "cache.buildbuddy.io.", dns.TypeA, "10.1.2.3")
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"cache.buildbuddy.io.", "A", "1.2.3.4"}}, answers(m.Answer))
}

func mustASN(t *testing.T, ip string) uint32 {
	t.Helper()
	asn, err := maxmind.LookupASN(netip.MustParseAddr(ip))
	require.NoError(t, err)
	require.NotZero(t, asn.Number, "no ASN for %s in the DB", ip)
	return uint32(asn.Number)
}

// TestClientNetworkResolution verifies how the client's network (and thus ASN)
// is determined: EDNS Client Subnet is preferred, the transport source address
// is the fallback, and neither yields an unknown ASN. Each case is observed
// through which routing override fires, since the ASN itself isn't returned.
func TestClientNetworkResolution(t *testing.T) {
	// Two distinct public IPs with distinct ASNs so we can tell, by which
	// override fires, which source the resolver used.
	const ecsIP, remoteIP = "8.8.8.8", "1.1.1.1"
	ecsASN, remoteASN := mustASN(t, ecsIP), mustASN(t, remoteIP)
	require.NotEqual(t, ecsASN, remoteASN)

	// override ecsASN -> "ecs" IP, remoteASN -> "remote" IP, anything else none.
	flagConfig := fmt.Sprintf(`{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "dns.asn_routing": {
      "state": "ENABLED",
      "variants": { "ecs": { "addresses": ["10.0.0.1"] }, "remote": { "addresses": ["10.0.0.2"] }, "none": {} },
      "defaultVariant": "none",
      "targeting": {
        "if": [
          { "==": [ { "var": "asn" }, %d ] }, "ecs",
          { "==": [ { "var": "asn" }, %d ] }, "remote",
          "none"
        ]
      }
    }
  }
}`, ecsASN, remoteASN)
	h := handlerWithProvider(t, newFlagProvider(t, flagConfig))

	answerIP := func(m *dns.Msg) string {
		require.Len(t, m.Answer, 1)
		return m.Answer[0].(*dns.A).A.String()
	}

	// ECS present with a *different* transport source: ECS wins.
	m := serve(t, h, withECS(t, newQuery("cache.buildbuddy.io.", dns.TypeA), ecsIP), remoteIP)
	assert.Equal(t, "10.0.0.1", answerIP(m), "ECS should take precedence over the transport source")

	// No ECS: fall back to the transport source address.
	m = serve(t, h, newQuery("cache.buildbuddy.io.", dns.TypeA), remoteIP)
	assert.Equal(t, "10.0.0.2", answerIP(m), "should fall back to the transport source address")

	// Neither ECS nor a usable transport source: unknown ASN, static answer.
	m = serve(t, h, newQuery("cache.buildbuddy.io.", dns.TypeA), "")
	assert.Equal(t, "1.2.3.4", answerIP(m), "unknown client network should use the static record")
}

// asnOnlyRoutingConfig targets every query from asn, overriding to the given
// addresses (the dns.asn_routing object value).
func asnOnlyRoutingConfig(asn uint32, addresses ...string) string {
	return fmt.Sprintf(`{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "dns.asn_routing": {
      "state": "ENABLED",
      "variants": { "override": { "addresses": [%s] }, "none": {} },
      "defaultVariant": "none",
      "targeting": { "if": [ { "==": [ { "var": "asn" }, %d ] }, "override", "none" ] }
    }
  }
}`, jsonStringList(addresses), asn)
}

// jsonStringList renders ss as the body of a JSON array, e.g. `"a", "b"`.
func jsonStringList(ss []string) string {
	quoted := make([]string, len(ss))
	for i, s := range ss {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(quoted, ", ")
}

// TestASNRoutingOnlyOverridesServedNames verifies the override never invents
// records: even with a name-agnostic (asn-only) rule that matches the client,
// it only replaces a name that already resolves to an address in our zone.
func TestASNRoutingOnlyOverridesServedNames(t *testing.T) {
	h := handlerWithProvider(t, newFlagProvider(t, asnOnlyRoutingConfig(mustASN(t, "8.8.8.8"), "10.20.30.40")))

	// A name that resolves to an address is overridden.
	m := queryFromIP(t, h, "cache.buildbuddy.io.", dns.TypeA, "8.8.8.8")
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"cache.buildbuddy.io.", "A", "10.20.30.40"}}, answers(m.Answer))

	// A name with no zone record stays NXDOMAIN; the override does not invent it.
	m = queryFromIP(t, h, "nope.example.com.", dns.TypeA, "8.8.8.8")
	assert.Equal(t, dns.RcodeNameError, m.Rcode)
	assert.Empty(t, m.Answer)

	// A CNAME name is not turned into an A record; the static CNAME chain stands.
	m = queryFromIP(t, h, "alias.buildbuddy.io.", dns.TypeA, "8.8.8.8")
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{
		{"alias.buildbuddy.io.", "CNAME", "cache.buildbuddy.io."},
		{"cache.buildbuddy.io.", "A", "1.2.3.4"},
	}, answers(m.Answer))
}

// TestASNRoutingMultipleAddresses verifies a multi-address override yields the
// full A RRset, so steering can preserve load-balanced/redundant addresses.
func TestASNRoutingMultipleAddresses(t *testing.T) {
	h := handlerWithProvider(t, newFlagProvider(t, asnOnlyRoutingConfig(mustASN(t, "8.8.8.8"), "10.20.30.40", "10.20.30.41")))

	m := queryFromIP(t, h, "cache.buildbuddy.io.", dns.TypeA, "8.8.8.8")
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{
		{"cache.buildbuddy.io.", "A", "10.20.30.40"},
		{"cache.buildbuddy.io.", "A", "10.20.30.41"},
	}, answers(m.Answer))
}

// TestASNRoutingIgnoresNoSubnetECS verifies that a "no client subnet" ECS
// option (an unspecified 0.0.0.0 address) is ignored in favor of the
// transport source.
func TestASNRoutingIgnoresNoSubnetECS(t *testing.T) {
	h := handlerWithProvider(t, newFlagProvider(t, asnOnlyRoutingConfig(mustASN(t, "1.1.1.1"), "10.20.30.40")))

	// ECS advertises no subnet (0.0.0.0); the ASN comes from the transport
	// source (1.1.1.1), whose ASN the flag targets, so the override fires.
	req := withECS(t, newQuery("cache.buildbuddy.io.", dns.TypeA), "0.0.0.0")
	m := serve(t, h, req, "1.1.1.1")
	require.Len(t, m.Answer, 1)
	assert.Equal(t, "10.20.30.40", m.Answer[0].(*dns.A).A.String(),
		"a no-subnet ECS should fall back to the transport source address")
}

// TestASNRoutingEchoesECSScope verifies that a routed answer to an ECS query
// echoes the client subnet with SCOPE set (RFC 7871), so resolvers cache the
// per-ASN answer per-subnet rather than globally.
func TestASNRoutingEchoesECSScope(t *testing.T) {
	h := handlerWithProvider(t, newFlagProvider(t, asnOnlyRoutingConfig(mustASN(t, "8.8.8.8"), "10.20.30.40")))

	m := serve(t, h, withECS(t, newQuery("cache.buildbuddy.io.", dns.TypeA), "8.8.8.8"), "")

	opt := m.IsEdns0()
	require.NotNil(t, opt, "response should carry an OPT echoing ECS")
	var ecs *dns.EDNS0_SUBNET
	for _, o := range opt.Option {
		if e, ok := o.(*dns.EDNS0_SUBNET); ok {
			ecs = e
		}
	}
	require.NotNil(t, ecs, "response should echo an ECS option")
	assert.Equal(t, uint8(32), ecs.SourceScope, "SCOPE should reflect the source prefix length")
	assert.Equal(t, "8.8.8.8", ecs.Address.String())
}

// TestASNRoutingRollout exercises a fractional (percentage) rollout: the flag
// routes ~30% of names to an override IP, bucketed deterministically per name,
// and we verify that a meaningful-but-partial fraction of traffic is steered.
func TestASNRoutingRollout(t *testing.T) {
	const (
		overrideIP = "10.20.30.40"
		staticIP   = "9.9.9.9" // the *.buildbuddy.io wildcard record
	)
	// fractional buckets each query deterministically by name, so ~30% of
	// distinct names land in the "override" variant.
	flagConfig := `{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "dns.asn_routing": {
      "state": "ENABLED",
      "variants": { "override": { "addresses": ["10.20.30.40"] }, "none": {} },
      "defaultVariant": "none",
      "targeting": {
        "fractional": [
          { "var": "name" },
          [ "override", 30 ],
          [ "none", 70 ]
        ]
      }
    }
  }
}`
	h := handlerWithProvider(t, newFlagProvider(t, flagConfig))

	const n = 1000
	overridden := 0
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("host%d.buildbuddy.io.", i)
		m := queryFromIP(t, h, name, dns.TypeA, "8.8.8.8")
		require.Equal(t, dns.RcodeSuccess, m.Rcode)
		require.Len(t, m.Answer, 1)
		switch ip := m.Answer[0].(*dns.A).A.String(); ip {
		case overrideIP:
			overridden++
		case staticIP:
			// Not in the rollout; served the static record.
		default:
			t.Fatalf("unexpected IP %q for %q", ip, name)
		}
	}

	frac := float64(overridden) / n
	// The split is deterministic (a hash of the name), so this is a generous
	// band around the 30% target rather than a flaky statistical bound: it
	// asserts the rollout fires for a real fraction without catching everyone.
	assert.Greater(t, frac, 0.2, "rollout steered too little traffic: %d/%d", overridden, n)
	assert.Less(t, frac, 0.4, "rollout steered too much traffic: %d/%d", overridden, n)
}

// BenchmarkServeUDP measures end-to-end query throughput against the real UDP
// server: it binds the handler to a loopback UDP socket and exchanges queries
// over concurrent reused connections.
func BenchmarkServeUDP(b *testing.B) {
	h := server.NewHandler(testenv.GetTestEnv(b), mustRecords(b), nil)

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(b, err)
	srv := &dns.Server{PacketConn: pc, Handler: h}
	started := make(chan struct{})
	srv.NotifyStartedFunc = func() { close(started) }
	go func() { _ = srv.ActivateAndServe() }()
	<-started
	b.Cleanup(func() { _ = srv.Shutdown() })

	addr := pc.LocalAddr().String()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := (&dns.Client{}).Dial(addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		req := new(dns.Msg)
		req.SetQuestion("cache.buildbuddy.io.", dns.TypeA)
		for pb.Next() {
			if err := conn.WriteMsg(req); err != nil {
				b.Error(err)
				return
			}
			if _, err := conn.ReadMsg(); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// answer is a flattened view of an answer RR for convenient assertions.
type answer struct {
	Name  string
	Type  string
	Value string
}

// answers flattens the rdata of each answer RR into an answer struct.
func answers(rrs []dns.RR) []answer {
	var out []answer
	for _, rr := range rrs {
		switch v := rr.(type) {
		case *dns.A:
			out = append(out, answer{rr.Header().Name, "A", v.A.String()})
		case *dns.CNAME:
			out = append(out, answer{rr.Header().Name, "CNAME", v.Target})
		default:
			out = append(out, answer{rr.Header().Name, dns.TypeToString[rr.Header().Rrtype], rr.String()})
		}
	}
	return out
}

// txtValues returns the string values of the TXT records in rrs.
func txtValues(rrs []dns.RR) []string {
	var out []string
	for _, rr := range rrs {
		if t, ok := rr.(*dns.TXT); ok {
			out = append(out, t.Txt...)
		}
	}
	return out
}

func mustRR(t *testing.T, line string) dns.RR {
	t.Helper()
	rr, err := dns.NewRR(line)
	require.NoError(t, err, "parsing %q", line)
	return rr
}

// memBlobstore is a minimal in-memory interfaces.Blobstore for tests.
type memBlobstore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemBlobstore() *memBlobstore { return &memBlobstore{data: map[string][]byte{}} }

func (m *memBlobstore) ReadBlob(_ context.Context, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[name]
	if !ok {
		return nil, status.NotFoundErrorf("%q not found", name)
	}
	return b, nil
}
func (m *memBlobstore) WriteBlob(_ context.Context, name string, data []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[name] = data
	return len(data), nil
}
func (m *memBlobstore) DeleteBlob(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, name)
	return nil
}
func (m *memBlobstore) BlobExists(_ context.Context, name string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[name]
	return ok, nil
}
func (m *memBlobstore) Writer(context.Context, string) (interfaces.CommittedWriteCloser, error) {
	return nil, status.UnimplementedError("not used")
}

// signedUpdate builds an RFC2136 UPDATE for zone carrying a TSIG so the handler
// treats it as authenticated (fakeResponseWriter.TsigStatus reports nil).
func signedUpdate(zone string) *dns.Msg {
	u := new(dns.Msg)
	u.SetUpdate(dns.Fqdn(zone))
	u.SetTsig("acme.", dns.HmacSHA256, 300, time.Now().Unix())
	return u
}

func TestSelfHostedACMEChallenge(t *testing.T) {
	acme, err := server.NewChallenges(newMemBlobstore(), 10*time.Second)
	require.NoError(t, err)
	h := server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), acme)
	const name = "_acme-challenge.cache.buildbuddy.io."

	// Absent challenge: authoritative NODATA (NOERROR, no answer, SOA).
	m := query(t, h, name, dns.TypeTXT)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.True(t, m.Authoritative)
	assert.Empty(t, m.Answer)
	require.Len(t, m.Ns, 1)
	assert.Equal(t, dns.TypeSOA, m.Ns[0].Header().Rrtype)

	// Set it via an RFC2136 UPDATE, then it's served authoritatively.
	up := signedUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, name+` 60 IN TXT "token-a"`)})
	m = serve(t, h, up, "")
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	m = query(t, h, name, dns.TypeTXT)
	assert.True(t, m.Authoritative)
	assert.Equal(t, []string{"token-a"}, txtValues(m.Answer))

	// A second value for the same name coexists (wildcard + apex cert case).
	up = signedUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, name+` 60 IN TXT "token-b"`)})
	serve(t, h, up, "")
	m = query(t, h, name, dns.TypeTXT)
	assert.ElementsMatch(t, []string{"token-a", "token-b"}, txtValues(m.Answer))

	// Remove one value (RFC2136 class NONE); the other remains.
	up = signedUpdate("buildbuddy.io.")
	up.Remove([]dns.RR{mustRR(t, name+` 0 IN TXT "token-a"`)})
	serve(t, h, up, "")
	m = query(t, h, name, dns.TypeTXT)
	assert.Equal(t, []string{"token-b"}, txtValues(m.Answer))

	// Remove the whole RRset (class ANY); back to NODATA.
	up = signedUpdate("buildbuddy.io.")
	up.RemoveRRset([]dns.RR{mustRR(t, name+` 0 IN TXT ""`)})
	serve(t, h, up, "")
	m = query(t, h, name, dns.TypeTXT)
	assert.Empty(t, m.Answer)
}

func TestSelfHostedACMERejectsUnsignedUpdate(t *testing.T) {
	acme, err := server.NewChallenges(newMemBlobstore(), 10*time.Second)
	require.NoError(t, err)
	h := server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), acme)

	up := new(dns.Msg) // no TSIG
	up.SetUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, `_acme-challenge.x.buildbuddy.io. 60 IN TXT "nope"`)})
	m := serve(t, h, up, "")
	assert.Equal(t, dns.RcodeNotAuth, m.Rcode)

	// Nothing was stored.
	m = query(t, h, "_acme-challenge.x.buildbuddy.io.", dns.TypeTXT)
	assert.Empty(t, m.Answer)
}

func TestSelfHostedACMERejectsWrongKeyUpdate(t *testing.T) {
	acme, err := server.NewChallenges(newMemBlobstore(), 10*time.Second)
	require.NoError(t, err)
	h := server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), acme)

	// A TSIG-signed UPDATE whose signature fails verification (wrong or unknown
	// key) must be rejected: the real server reports the failure via
	// TsigStatus(), which the handler checks even though the message carried a
	// TSIG. This is the realistic attack the unsigned-update test doesn't cover.
	up := signedUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, `_acme-challenge.x.buildbuddy.io. 60 IN TXT "nope"`)})
	w := &fakeResponseWriter{tsigStatus: dns.ErrSig}
	h.ServeDNS(w, up)
	require.NotNil(t, w.msg, "handler wrote no response")
	assert.Equal(t, dns.RcodeNotAuth, w.msg.Rcode)

	// Nothing was stored.
	m := query(t, h, "_acme-challenge.x.buildbuddy.io.", dns.TypeTXT)
	assert.Empty(t, m.Answer)
}

func TestSelfHostedACMERejectsNonChallengeUpdate(t *testing.T) {
	acme, err := server.NewChallenges(newMemBlobstore(), 10*time.Second)
	require.NoError(t, err)
	h := server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), acme)

	// A signed UPDATE for a name that isn't an _acme-challenge TXT is refused, so
	// the TSIG key can't be used to rewrite arbitrary zone records.
	up := signedUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, `cache.buildbuddy.io. 60 IN TXT "hijack"`)})
	m := serve(t, h, up, "")
	assert.Equal(t, dns.RcodeRefused, m.Rcode)
}

func TestSelfHostedACMEUpdateIsAtomic(t *testing.T) {
	acme, err := server.NewChallenges(newMemBlobstore(), 10*time.Second)
	require.NoError(t, err)
	h := server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), acme)

	// An UPDATE carrying a valid add followed by a record we refuse (here, an
	// unsupported class) must reject the whole update without applying the
	// earlier add -- RFC2136 updates are all-or-nothing.
	up := signedUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, `_acme-challenge.a.buildbuddy.io. 60 IN TXT "keep"`)})
	bad := mustRR(t, `_acme-challenge.b.buildbuddy.io. 60 IN TXT "bad"`)
	bad.Header().Class = dns.ClassCHAOS // unsupported update class
	up.Ns = append(up.Ns, bad)
	m := serve(t, h, up, "")
	assert.Equal(t, dns.RcodeRefused, m.Rcode)

	// The valid add was not applied.
	m = query(t, h, "_acme-challenge.a.buildbuddy.io.", dns.TypeTXT)
	assert.Empty(t, m.Answer)
}

func TestMsgAccept(t *testing.T) {
	hdr := func(opcode int) dns.Header { return dns.Header{Bits: uint16(opcode) << 11} }
	assert.Equal(t, dns.MsgAccept, server.MsgAccept(hdr(dns.OpcodeQuery)))
	assert.Equal(t, dns.MsgAccept, server.MsgAccept(hdr(dns.OpcodeNotify)))
	// UPDATE must be accepted -- miekg's default policy rejects it with NOTIMP.
	assert.Equal(t, dns.MsgAccept, server.MsgAccept(hdr(dns.OpcodeUpdate)))
	assert.Equal(t, dns.MsgRejectNotImplemented, server.MsgAccept(hdr(dns.OpcodeStatus)))
	// A response (QR bit set) is ignored, whatever the opcode.
	assert.Equal(t, dns.MsgIgnore, server.MsgAccept(dns.Header{Bits: 1 << 15}))
}

// TestServerAcceptsSignedUpdate drives the real dns.Server stack -- the accept
// policy and TSIG verification that the handler-level tests bypass. It is the
// regression guard for the server answering RFC2136 UPDATEs with NOTIMP.
func TestServerAcceptsSignedUpdate(t *testing.T) {
	const keyName = "acme."
	// Any valid base64 secret; this is base64("0123456789abcdef").
	const secret = "MDEyMzQ1Njc4OWFiY2RlZg=="
	tsig := map[string]string{keyName: secret}

	acme, err := server.NewChallenges(newMemBlobstore(), 10*time.Second)
	require.NoError(t, err)
	h := server.NewHandler(testenv.GetTestEnv(t), mustRecords(t), acme)

	// Serve on a real loopback UDP socket with the same accept policy + TsigSecret
	// density uses in production.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &dns.Server{PacketConn: pc, Handler: h, TsigSecret: tsig, MsgAcceptFunc: server.MsgAccept}
	started := make(chan struct{})
	srv.NotifyStartedFunc = func() { close(started) }
	go func() { _ = srv.ActivateAndServe() }()
	defer srv.Shutdown()
	<-started
	addr := pc.LocalAddr().String()

	const name = "_acme-challenge.cache.buildbuddy.io."

	// A TSIG-signed UPDATE is accepted and applied -- not answered NOTIMP.
	up := new(dns.Msg)
	up.SetUpdate("buildbuddy.io.")
	up.Insert([]dns.RR{mustRR(t, name+` 60 IN TXT "tok"`)})
	up.SetTsig(keyName, dns.HmacSHA256, 300, time.Now().Unix())
	reply, _, err := (&dns.Client{TsigSecret: tsig}).Exchange(up, addr)
	require.NoError(t, err)
	require.Equalf(t, dns.RcodeSuccess, reply.Rcode, "UPDATE rcode = %s", dns.RcodeToString[reply.Rcode])

	// ...and the challenge value is now served.
	q := new(dns.Msg)
	q.SetQuestion(name, dns.TypeTXT)
	reply, _, err = (&dns.Client{}).Exchange(q, addr)
	require.NoError(t, err)
	assert.Equal(t, []string{"tok"}, txtValues(reply.Answer))
}

func TestExactMatch(t *testing.T) {
	h := newTestHandler(t)
	m := query(t, h, "cache.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.True(t, m.Authoritative)
	assert.Equal(t, []answer{{"cache.buildbuddy.io.", "A", "1.2.3.4"}}, answers(m.Answer))
}

func TestWildcardMatch(t *testing.T) {
	h := newTestHandler(t)
	m := query(t, h, "random123.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	// Answer is synthesized with the queried name as owner, not "*.…".
	assert.Equal(t, []answer{{"random123.buildbuddy.io.", "A", "9.9.9.9"}}, answers(m.Answer))
}

func TestMostSpecificWildcardWins(t *testing.T) {
	h := newTestHandler(t)
	m := query(t, h, "node.us-west1.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"node.us-west1.buildbuddy.io.", "A", "8.8.8.8"}}, answers(m.Answer))
}

func TestWildcardDoesNotMutateStoredRecord(t *testing.T) {
	h := newTestHandler(t)
	// A wildcard query rewrites the owner name; ensure a second exact query
	// for the wildcard still reports the original "*.…" owner (i.e. the
	// stored RR wasn't mutated in place).
	query(t, h, "first.buildbuddy.io.", dns.TypeA)
	m := query(t, h, "*.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, []answer{{"*.buildbuddy.io.", "A", "9.9.9.9"}}, answers(m.Answer))
}

func TestCNAMEOutOfZone(t *testing.T) {
	h := newTestHandler(t)
	// Querying A on a CNAME-only name returns the CNAME with NOERROR; the
	// out-of-zone target is left for the recursive resolver to chase.
	m := query(t, h, "www.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"www.buildbuddy.io.", "CNAME", "external.github.io."}}, answers(m.Answer))
}

func TestCNAMEInZoneFollowed(t *testing.T) {
	h := newTestHandler(t)
	// alias -> cache (in-zone), so we return both the CNAME and the resolved A.
	m := query(t, h, "alias.buildbuddy.io.", dns.TypeA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{
		{"alias.buildbuddy.io.", "CNAME", "cache.buildbuddy.io."},
		{"cache.buildbuddy.io.", "A", "1.2.3.4"},
	}, answers(m.Answer))
}

func TestCNAMEQueryNotChased(t *testing.T) {
	h := newTestHandler(t)
	// An explicit CNAME query returns just the CNAME, without chasing it.
	m := query(t, h, "alias.buildbuddy.io.", dns.TypeCNAME)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"alias.buildbuddy.io.", "CNAME", "cache.buildbuddy.io."}}, answers(m.Answer))
}

func TestNODATA(t *testing.T) {
	h := newTestHandler(t)
	// Name exists (has an A) but no AAAA: NOERROR + empty answer, SOA in
	// authority.
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
	assert.ElementsMatch(t, []answer{
		{"lb.buildbuddy.io.", "A", "1.1.1.1"},
		{"lb.buildbuddy.io.", "A", "2.2.2.2"},
	}, answers(m.Answer))
}

func TestSOAQuery(t *testing.T) {
	h := newTestHandler(t)
	// Querying SOA directly returns it in the answer section (not just in
	// the authority section).
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
	assert.Equal(t, []answer{{"x.aws.buildbuddy.io.", "CNAME", "elb.amazonaws.example."}}, answers(m.Answer))
}

func TestEmptyQuestion(t *testing.T) {
	h := newTestHandler(t)
	// A query carrying no question is malformed: FORMERR.
	req := new(dns.Msg)
	w := &fakeResponseWriter{}
	h.ServeDNS(w, req)
	require.NotNil(t, w.msg)
	assert.Equal(t, dns.RcodeFormatError, w.msg.Rcode)
}

func TestParseZoneFileSurfacesErrors(t *testing.T) {
	// dns.ZoneParser.Next() returns ok=false on a parse error as well as at
	// EOF; ParseZoneFile must surface the error rather than returning a
	// partial record set.
	path := filepath.Join(t.TempDir(), "bad.zone")
	require.NoError(t, os.WriteFile(path, []byte("buildbuddy.io. 60 IN A not-an-ip\n"), 0644))
	_, err := server.ParseZoneFile(path, "")
	assert.Error(t, err)
}

func TestParseZoneFileRequiresFQDNWithoutOrigin(t *testing.T) {
	// With an empty origin, a relative owner name can't be qualified and is a
	// parse error, so a misconfigured zone fails startup rather than serving
	// mis-qualified names.
	path := filepath.Join(t.TempDir(), "relative.zone")
	require.NoError(t, os.WriteFile(path, []byte("relative 60 IN A 1.2.3.4\n"), 0644))

	_, err := server.ParseZoneFile(path, "")
	assert.Error(t, err, "relative name with no origin should error")

	// The same file parses when an origin is supplied to qualify against.
	records, err := server.ParseZoneFile(path, "buildbuddy.io.")
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "relative.buildbuddy.io.", records[0].Header().Name)
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

func TestCNAMEChainNODATAGetsSOA(t *testing.T) {
	h := newTestHandler(t)
	// alias -> cache (in-zone), and cache has only an A. Querying AAAA returns
	// the CNAME but no AAAA: this is NODATA at the end of the chain, so the SOA
	// must still be attached even though the answer section is non-empty.
	m := query(t, h, "alias.buildbuddy.io.", dns.TypeAAAA)
	assert.Equal(t, dns.RcodeSuccess, m.Rcode)
	assert.Equal(t, []answer{{"alias.buildbuddy.io.", "CNAME", "cache.buildbuddy.io."}}, answers(m.Answer))
	require.Len(t, m.Ns, 1, "NODATA at end of CNAME chain should carry the SOA")
	assert.Equal(t, dns.TypeSOA, m.Ns[0].Header().Rrtype)
}
