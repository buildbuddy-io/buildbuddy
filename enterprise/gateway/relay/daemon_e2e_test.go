// The workstation daemon (tools/bbaccess/tunnel) end to end, against an
// in-process relay gateway. It lives here rather than beside the daemon
// because composing a gateway needs enterprise packages, and nothing under
// tools/ may depend on enterprise/.
package relay_test

// End-to-end test of the workstation daemon against a real gateway.
//
// This drives the full path a developer's connection takes, minus the TUN
// device (which needs root to create): local DNS answers with a fake IP, the
// daemon looks that IP back up, brings up a real userspace WireGuard tunnel on
// demand, and relays a connection to a service only the "gateway" can reach.

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/apikeyauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/certauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/relay"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauthrelay"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/credentials"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/dnsserver"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/fakeip"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelmgr"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

func freePort(t testing.TB, network string) int {
	t.Helper()
	if network == "udp" {
		l, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()
		return l.LocalAddr().(*net.UDPAddr).Port
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startGateway runs a gateway with the relay enabled, serving gRPC over real
// TCP (not bufconn) so the daemon dials it exactly as it would in production.
// It returns the gRPC target.
func startGateway(t *testing.T) string {
	t.Helper()
	flags.Set(t, "gateway.udp_listen_port", freePort(t, "udp"))
	flags.Set(t, "gateway.public_host", "127.0.0.1")

	// The env authenticator runs in the gRPC interceptor chain and attaches
	// claims to the context; the gateway's own authenticator then reads them.
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)

	gw, err := server.New(server.Options{
		Authenticator: apikeyauth.New(ta),
		HubServices:   []server.HubService{relay.New()},
	})
	require.NoError(t, err)
	t.Cleanup(gw.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer, runFunc := testenv.GRPCServer(env, lis)
	gwsvcpb.RegisterGatewayServiceServer(grpcServer, gw)
	go runFunc()
	t.Cleanup(grpcServer.Stop)

	return fmt.Sprintf("grpc://%s", lis.Addr().String())
}

// startCertGateway runs a gateway that authenticates employees with tunnel
// certificates issued by ca, and knows no API keys at all.
func startCertGateway(t *testing.T, ca *testauthrelay.CA, audience string) string {
	t.Helper()
	flags.Set(t, "gateway.cert_auth.ca", ca.PEM())
	flags.Set(t, "gateway.cert_auth.audience", audience)
	flags.Set(t, "gateway.udp_listen_port", freePort(t, "udp"))
	flags.Set(t, "gateway.public_host", "127.0.0.1")

	env := testenv.GetTestEnv(t)

	certAuth, err := certauth.New()
	require.NoError(t, err)
	gw, err := server.New(server.Options{
		Authenticator: certAuth,
		HubServices:   []server.HubService{relay.New()},
	})
	require.NoError(t, err)
	t.Cleanup(gw.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer, runFunc := testenv.GRPCServer(env, lis)
	gwsvcpb.RegisterGatewayServiceServer(grpcServer, gw)
	go runFunc()
	t.Cleanup(grpcServer.Stop)

	return fmt.Sprintf("grpc://%s", lis.Addr().String())
}

// startTargetService starts a TCP echo service standing in for something only
// the gateway can reach.
func startTargetService(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestDaemon_ResolveAndRelay(t *testing.T) {
	gatewayTarget := startGateway(t)
	targetPort := startTargetService(t)

	cfg := config.Default()
	cfg.Zones = []config.Zone{{
		// A single host rather than a whole domain, so the rewrite maps it to
		// a name this gateway can actually resolve. In production the suffix is
		// a domain and the rewrite is what turns a cluster-qualified name back
		// into the cluster's own name.
		Suffix:    "host.foo.bb.internal",
		Gateway:   gatewayTarget,
		RewriteTo: "localhost",
	}}
	cfg.DNSListen = fmt.Sprintf("127.0.0.1:%d", freePort(t, "udp"))

	table, err := fakeip.NewTable(netip.MustParsePrefix(cfg.FakeCIDR))
	require.NoError(t, err)

	// testauth maps an API key straight to a user ID.
	mgr := tunnelmgr.New(credentials.APIKey("user1"), 5*time.Minute)
	t.Cleanup(mgr.Close)

	dnsSrv := dnsserver.New(cfg, table, mgr)
	require.NoError(t, dnsSrv.Start(cfg.DNSListen))
	t.Cleanup(dnsSrv.Shutdown)

	// 1. Resolve a name in a covered zone. The daemon answers it locally.
	const name = "host.foo.bb.internal"
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), dns.TypeA)
	resp, err := dns.Exchange(m, cfg.DNSListen)
	require.NoError(t, err)
	require.Equal(t, dns.RcodeSuccess, resp.Rcode)
	require.Len(t, resp.Answer, 1)
	fake := resp.Answer[0].(*dns.A).A
	fakeAddr, ok := netip.AddrFromSlice(fake.To4())
	require.True(t, ok)
	require.True(t, table.Contains(fakeAddr), "%s should be inside %s", fakeAddr, cfg.FakeCIDR)

	// 2. The fake IP maps back to the name — this is what the interceptor does
	// when a SYN arrives for it.
	gotName, ok := table.Name(fakeAddr)
	require.True(t, ok)
	require.Equal(t, name, gotName)

	// 3. Dial through the tunnel, which is established on demand right here.
	zone, ok := cfg.MatchZone(gotName)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := mgr.Dial(ctx, zone, zone.TargetName(gotName), targetPort)
	require.NoError(t, err)
	defer conn.Close()

	const want = "hello from the workstation"
	_, err = io.WriteString(conn, want)
	require.NoError(t, err)
	require.NoError(t, conn.(interface{ CloseWrite() error }).CloseWrite())
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	got, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

// TestDaemon_CertCredentialEndToEnd is the whole employee path: bbaccess writes a
// certificate to disk, the daemon loads it, mints a credential bound to the
// WireGuard key it is about to register, and relays a connection — with no API
// key anywhere.
func TestDaemon_CertCredentialEndToEnd(t *testing.T) {
	const audience = "gateway.foo.example"
	ca := testauthrelay.NewCA(t)
	gatewayTarget := startCertGateway(t, ca, audience)
	targetPort := startTargetService(t)

	// Stand in for a bbaccess run: write the credential where the daemon looks.
	credentialDir := t.TempDir()
	store, err := credentials.NewStore(credentialDir)
	require.NoError(t, err)
	certPEM, keyPEM := ca.IssuePEM(t, "someone@example.com", time.Time{})
	require.NoError(t, store.Write("certs_example", certPEM, keyPEM))

	cfg := config.Default()
	cfg.CredentialDir = credentialDir
	cfg.Zones = []config.Zone{{
		Suffix:    "host.foo.bb.internal",
		Gateway:   gatewayTarget,
		RewriteTo: "localhost",
		// The gateway is reached at 127.0.0.1 here, so name the audience
		// explicitly rather than deriving it from the target.
		Audience: audience,
	}}

	mgr := tunnelmgr.New(store, 5*time.Minute)
	t.Cleanup(mgr.Close)

	zone, ok := cfg.MatchZone("host.foo.bb.internal")
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := mgr.Dial(ctx, zone, zone.TargetName("host.foo.bb.internal"), targetPort)
	require.NoError(t, err)
	defer conn.Close()

	const want = "authenticated with a bbaccess certificate"
	_, err = io.WriteString(conn, want)
	require.NoError(t, err)
	require.NoError(t, conn.(interface{ CloseWrite() error }).CloseWrite())
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	got, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

// TestDaemon_WrongAudienceIsRejectedEndToEnd checks that the audience binding
// holds over the wire, not just in the unit tests: a credential minted for
// another gateway must not open a tunnel here.
func TestDaemon_WrongAudienceIsRejectedEndToEnd(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	gatewayTarget := startCertGateway(t, ca, "gateway.foo.example")

	credentialDir := t.TempDir()
	store, err := credentials.NewStore(credentialDir)
	require.NoError(t, err)
	certPEM, keyPEM := ca.IssuePEM(t, "someone@example.com", time.Time{})
	require.NoError(t, store.Write("certs_example", certPEM, keyPEM))

	cfg := config.Default()
	cfg.CredentialDir = credentialDir
	cfg.Zones = []config.Zone{{
		Suffix:   "host.foo.bb.internal",
		Gateway:  gatewayTarget,
		Audience: "gateway.bar.example",
	}}

	mgr := tunnelmgr.New(store, 5*time.Minute)
	t.Cleanup(mgr.Close)

	zone, ok := cfg.MatchZone("host.foo.bb.internal")
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err = mgr.Dial(ctx, zone, "localhost", 22)
	require.Error(t, err)
	require.Contains(t, err.Error(), "audience")
}

func TestDaemon_DNSBehavior(t *testing.T) {
	cfg := config.Default()
	cfg.Zones = []config.Zone{{Suffix: "foo.bb.internal", Gateway: "grpc://localhost:1"}}
	cfg.DNSListen = fmt.Sprintf("127.0.0.1:%d", freePort(t, "udp"))

	table, err := fakeip.NewTable(netip.MustParsePrefix(cfg.FakeCIDR))
	require.NoError(t, err)
	// No prewarmer: this test only cares about the answers themselves.
	dnsSrv := dnsserver.New(cfg, table, nil)
	require.NoError(t, dnsSrv.Start(cfg.DNSListen))
	t.Cleanup(dnsSrv.Shutdown)

	ask := func(name string, qtype uint16) *dns.Msg {
		t.Helper()
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn(name), qtype)
		resp, err := dns.Exchange(m, cfg.DNSListen)
		require.NoError(t, err)
		return resp
	}

	t.Run("A record is stable across queries", func(t *testing.T) {
		first := ask("host.foo.bb.internal", dns.TypeA)
		second := ask("host.foo.bb.internal", dns.TypeA)
		require.Equal(t, first.Answer[0].(*dns.A).A.String(), second.Answer[0].(*dns.A).A.String())
	})

	t.Run("AAAA is empty NOERROR, not NXDOMAIN", func(t *testing.T) {
		// NXDOMAIN here would make stub resolvers treat the name as
		// nonexistent even though the A lookup succeeds.
		resp := ask("host.foo.bb.internal", dns.TypeAAAA)
		require.Equal(t, dns.RcodeSuccess, resp.Rcode)
		require.Empty(t, resp.Answer)
	})

	t.Run("HTTPS (type 65) is empty NOERROR", func(t *testing.T) {
		resp := ask("host.foo.bb.internal", dns.TypeHTTPS)
		require.Equal(t, dns.RcodeSuccess, resp.Rcode)
		require.Empty(t, resp.Answer)
	})

	t.Run("names outside the parent are REFUSED so resolvers fail over", func(t *testing.T) {
		resp := ask("www.example.com", dns.TypeA)
		require.Equal(t, dns.RcodeRefused, resp.Rcode)
	})

	t.Run("names under the parent but in no zone are NXDOMAIN", func(t *testing.T) {
		// Everything under bb.internal is routed here, so there is no other
		// server for a resolver to fail over to.
		resp := ask("host.nowhere.bb.internal", dns.TypeA)
		require.Equal(t, dns.RcodeNameError, resp.Rcode)
	})

	t.Run("PTR resolves a fake IP back to its name", func(t *testing.T) {
		a := ask("ptrhost.foo.bb.internal", dns.TypeA)
		addr := a.Answer[0].(*dns.A).A.To4()
		arpa := fmt.Sprintf("%d.%d.%d.%d.in-addr.arpa", addr[3], addr[2], addr[1], addr[0])
		resp := ask(arpa, dns.TypePTR)
		require.Equal(t, dns.RcodeSuccess, resp.Rcode)
		require.Len(t, resp.Answer, 1)
		require.Equal(t, "ptrhost.foo.bb.internal.", resp.Answer[0].(*dns.PTR).Ptr)
	})
}
