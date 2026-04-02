package server

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/keys"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// testenv starts a healthcheck goroutine that is not stopped on cleanup.
		goleak.IgnoreTopFunction("github.com/buildbuddy-io/buildbuddy/server/util/healthcheck.(*HealthChecker).handleSignals"),
		// testenv starts a DB stats polling goroutine that sleeps between polls
		// and is not stopped on cleanup.
		goleak.IgnoreTopFunction("time.Sleep"),
	)
}

func newPubKeyHex(t *testing.T) string {
	t.Helper()
	priv, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	return priv.PublicKey().Hex()
}

func freeUDPPort(t testing.TB) int {
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()
	return port
}

func setupGateway(t testing.TB, ta *testauth.TestAuthenticator) *Gateway {
	t.Helper()
	flags.Set(t, "gateway.udp_listen_port", freeUDPPort(t))
	flags.Set(t, "gateway.public_host", "127.0.0.1")

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)

	gw, err := New(env)
	require.NoError(t, err)
	t.Cleanup(gw.Close)
	return gw
}

func TestNetworkPrefix(t *testing.T) {
	tests := []struct {
		index int
		want  string
	}{
		{0, "fd00:bb::/48"},
		{1, "fd00:bb:1::/48"},
		{256, "fd00:bb:100::/48"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("index%d", tc.index), func(t *testing.T) {
			require.Equal(t, tc.want, networkPrefix(tc.index).String())
		})
	}
}

func TestNetworkHubIP(t *testing.T) {
	require.Equal(t, "fd00:bb::1", networkHubIP(0).String())
	require.Equal(t, "fd00:bb:1::1", networkHubIP(1).String())
}

func TestNetworkClientIP(t *testing.T) {
	tests := []struct {
		index   int
		hostNum int
		want    string
	}{
		{0, 2, "fd00:bb::2"},
		{0, 3, "fd00:bb::3"},
		{0, 256, "fd00:bb::100"},
		{0, 65534, "fd00:bb::fffe"},
		{1, 2, "fd00:bb:1::2"},
		{2, 2, "fd00:bb:2::2"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("net%d_host%d", tc.index, tc.hostNum), func(t *testing.T) {
			require.Equal(t, tc.want, networkClientIP(tc.index, tc.hostNum).String())
		})
	}
}

func TestRegister_AssignsSequentialIPs(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	resp1, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "peer1", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)
	require.Equal(t, "fd00:bb::2", resp1.GetAssignedIp())
	require.Equal(t, "fd00:bb::1", resp1.GetGatewayIp())
	require.Equal(t, "fd00:bb::/48", resp1.GetNetworkCidr())
	require.NotEmpty(t, resp1.GetServerPublicKey())

	resp2, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "peer2", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)
	require.Equal(t, "fd00:bb::3", resp2.GetAssignedIp())

	// Peers in the same network share the same server endpoint and public key.
	require.Equal(t, resp1.GetServerPublicKey(), resp2.GetServerPublicKey())
	require.Equal(t, resp1.GetServerEndpoint(), resp2.GetServerEndpoint())
}

func TestRegister_IsolatedNetworks(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"user1", "group1",
		"user2", "group2",
	))
	gw := setupGateway(t, ta)

	ctx1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	resp1, err := gw.Register(ctx1, &gwpb.RegisterRequest{NetworkName: "net1", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)
	resp2, err := gw.Register(ctx2, &gwpb.RegisterRequest{NetworkName: "net1", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)

	// All clients share the same WireGuard device and server public key.
	require.Equal(t, resp1.GetServerPublicKey(), resp2.GetServerPublicKey())
	require.Equal(t, resp1.GetServerEndpoint(), resp2.GetServerEndpoint())

	// Different groups get different IP prefixes and hub IPs.
	require.NotEqual(t, resp1.GetNetworkCidr(), resp2.GetNetworkCidr())
	require.NotEqual(t, resp1.GetGatewayIp(), resp2.GetGatewayIp())

	// Both start assigning from ::2 within their own prefix.
	require.Equal(t, "fd00:bb::2", resp1.GetAssignedIp())
	require.Equal(t, "fd00:bb:1::2", resp2.GetAssignedIp())
}

func TestRegister_Unauthenticated(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	gw := setupGateway(t, ta)

	_, err := gw.Register(context.Background(), &gwpb.RegisterRequest{NetworkName: "net1", PublicKey: newPubKeyHex(t)})
	require.Error(t, err)
}

func TestRegister_MissingPublicKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	_, err = gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1"})
	require.Error(t, err)
}

func TestRegister_InvalidPublicKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	_, err = gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PublicKey: "notahexkey"})
	require.Error(t, err)
}

func TestRegister_PeerNameConflict(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	resp1, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "foo", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)
	require.Equal(t, "foo", resp1.GetAssignedPeerName())

	// Second peer requesting the same name gets a suffixed name.
	resp2, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "foo", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)
	require.Equal(t, "foo-1", resp2.GetAssignedPeerName())

	// Third peer gets foo-2 since foo-1 is now also taken.
	resp3, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "foo", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)
	require.Equal(t, "foo-2", resp3.GetAssignedPeerName())
}

func TestRegister_InvalidPeerName(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	for _, name := range []string{"foo.bar", "foo.bar.baz"} {
		_, err = gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: name, PublicKey: newPubKeyHex(t)})
		require.Errorf(t, err, "expected error for peer_name %q", name)
	}
}

// TestCleanupStalePeers verifies that cleanupStalePeers removes peers whose
// registration time (used as a proxy for last-seen when no WireGuard handshake
// has occurred) exceeds stalePeerTimeout, while leaving recently registered
// peers alone.
//
// WireGuard's handshake interval is hardcoded in the library (~3 min) so we
// can't drive actual handshakes in a unit test. The cleanup code falls back to
// registeredAt for peers that have never completed a handshake, which is the
// path exercised here.
func TestCleanupStalePeers(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	flags.Set(t, "gateway.stale_peer_timeout", 5*time.Second)
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	// Register two peers in the same network.
	stalePubKey := newPubKeyHex(t)
	staleResp, err := gw.Register(ctx, &gwpb.RegisterRequest{
		NetworkName: "net1",
		PeerName:    "stale",
		PublicKey:   stalePubKey,
	})
	require.NoError(t, err)

	freshPubKey := newPubKeyHex(t)
	_, err = gw.Register(ctx, &gwpb.RegisterRequest{
		NetworkName: "net1",
		PeerName:    "fresh",
		PublicKey:   freshPubKey,
	})
	require.NoError(t, err)

	// Backdate the stale peer's registration time so it appears old enough to
	// be reaped.
	gw.mu.Lock()
	gw.peers[stalePubKey].registeredAt = time.Now().Add(-10 * time.Second)
	gw.mu.Unlock()

	gw.cleanupStalePeers()

	gw.mu.Lock()
	defer gw.mu.Unlock()

	// Stale peer must be gone; fresh peer must remain.
	require.NotContains(t, gw.peers, stalePubKey, "stale peer should have been removed")
	require.Contains(t, gw.peers, freshPubKey, "fresh peer should not have been removed")

	// Stale peer's IP must be unregistered from the TUN.
	staleIP := netip.MustParseAddr(staleResp.GetAssignedIp())
	_, inTUN := gw.tun.ipToNetwork.Load(staleIP)
	require.False(t, inTUN, "stale peer's IP should be unregistered from the TUN")

	// DNS names: stale name freed, fresh name retained.
	ns := gw.networks["group1/net1"]
	_, staleNameExists := ns.names.Load("stale")
	require.False(t, staleNameExists, "stale peer's DNS name should be removed")
	_, freshNameExists := ns.names.Load("fresh")
	require.True(t, freshNameExists, "fresh peer's DNS name should remain")
}
