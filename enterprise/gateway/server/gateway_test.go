package server

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

// freeUDPPort returns an ephemeral UDP port by briefly binding to :0.
func freeUDPPort(t *testing.T) int {
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()
	return port
}

func setupGateway(t *testing.T, ta *testauth.TestAuthenticator, portCount int) *Gateway {
	t.Helper()
	port := freeUDPPort(t)
	flags.Set(t, "gateway.listen_ports", fmt.Sprintf("%d-%d", port, port+portCount-1))
	flags.Set(t, "gateway.hub_ip", "fd00:bb::1")
	flags.Set(t, "gateway.cidr", "fd00:bb::/64")
	flags.Set(t, "gateway.public_host", "localhost")

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(ta)

	gw, err := New(env)
	require.NoError(t, err)
	return gw
}

func TestParsePortRange(t *testing.T) {
	tests := []struct {
		in      string
		want    []int
		wantErr bool
	}{
		{"20000-20002", []int{20000, 20001, 20002}, false},
		{"5000-5000", []int{5000}, false},
		{"5001-5000", nil, true}, // start > end
		{"abc-5000", nil, true},  // non-integer start
		{"5000-abc", nil, true},  // non-integer end
		{"5000", nil, true},      // missing dash
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parsePortRange(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNetworkClientIP(t *testing.T) {
	tests := []struct {
		cidr    string
		hostNum int
		want    string
	}{
		{"10.0.0.0/24", 2, "10.0.0.2"},
		{"10.0.0.0/24", 5, "10.0.0.5"},
		{"10.0.0.0/24", 255, "10.0.0.255"},
		{"fd00:bb::/64", 1, "fd00:bb::1"},
		{"fd00:bb::/64", 2, "fd00:bb::2"},
		{"fd00:bb::/64", 256, "fd00:bb::100"},
		{"fd00:bb::/64", 65536, "fd00:bb::1:0"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s_host%d", tc.cidr, tc.hostNum), func(t *testing.T) {
			got := networkClientIP(tc.cidr, tc.hostNum)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegister_AssignsSequentialIPs(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta, 4)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	resp1, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "peer1"})
	require.NoError(t, err)
	require.Equal(t, "fd00:bb::2", resp1.GetAssignedIp())
	require.Equal(t, "fd00:bb::1", resp1.GetGatewayIp())
	require.NotEmpty(t, resp1.GetPrivateKey())
	require.NotEmpty(t, resp1.GetServerPublicKey())
	require.Equal(t, "fd00:bb::/64", resp1.GetNetworkCidr())

	resp2, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "peer2"})
	require.NoError(t, err)
	require.Equal(t, "fd00:bb::3", resp2.GetAssignedIp())

	// Each registration generates a unique keypair.
	require.NotEqual(t, resp1.GetPrivateKey(), resp2.GetPrivateKey())
	// Both peers share the same server endpoint within one network.
	require.Equal(t, resp1.GetServerPublicKey(), resp2.GetServerPublicKey())
	require.Equal(t, resp1.GetServerEndpoint(), resp2.GetServerEndpoint())
}

func TestRegister_IsolatedNetworks(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"user1", "group1",
		"user2", "group2",
	))
	// Two groups need two ports.
	gw := setupGateway(t, ta, 4)

	ctx1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	resp1, err := gw.Register(ctx1, &gwpb.RegisterRequest{NetworkName: "net1"})
	require.NoError(t, err)
	resp2, err := gw.Register(ctx2, &gwpb.RegisterRequest{NetworkName: "net1"})
	require.NoError(t, err)

	// Different groups get different WireGuard devices (different server keys and ports).
	require.NotEqual(t, resp1.GetServerPublicKey(), resp2.GetServerPublicKey())
	require.NotEqual(t, resp1.GetServerEndpoint(), resp2.GetServerEndpoint())

	// IP space is not shared: both groups start assigning from .2.
	require.Equal(t, "fd00:bb::2", resp1.GetAssignedIp())
	require.Equal(t, "fd00:bb::2", resp2.GetAssignedIp())
}

func TestRegister_PortPoolExhausted(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"user1", "group1",
		"user2", "group2",
	))
	// Only one port: only one group network can be created.
	gw := setupGateway(t, ta, 1)

	ctx1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	_, err = gw.Register(ctx1, &gwpb.RegisterRequest{NetworkName: "net1"})
	require.NoError(t, err)

	// Second group needs a new port — pool is empty.
	_, err = gw.Register(ctx2, &gwpb.RegisterRequest{NetworkName: "net1"})
	require.Error(t, err)
}

func TestRegister_Unauthenticated(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	gw := setupGateway(t, ta, 4)

	_, err := gw.Register(context.Background(), &gwpb.RegisterRequest{NetworkName: "net1"})
	require.Error(t, err)
}
