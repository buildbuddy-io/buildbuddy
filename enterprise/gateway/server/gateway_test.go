package server

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

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
	priv, err := wgkeys.GeneratePrivateKey()
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

	gw, err := New(env, DNSService())
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

func TestDeregister(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey := newPubKeyHex(t)
	resp, err := gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "mypeer", PublicKey: pubKey})
	require.NoError(t, err)
	assignedIP := netip.MustParseAddr(resp.GetAssignedIp())

	_, err = gw.Deregister(ctx, &gwpb.DeregisterRequest{PublicKey: pubKey})
	require.NoError(t, err)

	gw.mu.Lock()
	defer gw.mu.Unlock()

	// Peer must be gone from the peer map.
	require.NotContains(t, gw.peers, pubKey)

	// IP must be unregistered from the TUN.
	_, inTUN := gw.tun.ipToNetwork.Load(assignedIP)
	require.False(t, inTUN, "IP should be unregistered after deregister")

	// DNS name must be freed.
	ns := gw.networks["group1/net1"]
	_, nameExists := ns.names["mypeer"]
	require.False(t, nameExists, "DNS name should be removed after deregister")
}

func TestDeregister_Unauthenticated(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	_, err := gw.Deregister(context.Background(), &gwpb.DeregisterRequest{PublicKey: newPubKeyHex(t)})
	require.Error(t, err)
}

func TestDeregister_UnknownKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	_, err = gw.Deregister(ctx, &gwpb.DeregisterRequest{PublicKey: newPubKeyHex(t)})
	require.Error(t, err)
}

func TestList(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	// Connect-registered peers show up in List, named or not.
	named, _, _ := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-1",
	})
	startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-2",
	})
	// A peer registered via the deprecated Register RPC has no session ID
	// and must be omitted.
	_, err = gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PeerName: "legacy", PublicKey: newPubKeyHex(t)})
	require.NoError(t, err)

	list, err := gw.List(ctx, &gwpb.ListRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetPeers(), 2)
	bySession := make(map[string]*gwpb.Peer, len(list.GetPeers()))
	for _, p := range list.GetPeers() {
		bySession[p.GetSessionId()] = p
	}
	require.Contains(t, bySession, "session-1")
	require.Contains(t, bySession, "session-2")
	require.Equal(t, "box1", bySession["session-1"].GetName())
	require.Equal(t, named.GetAssignedIp(), bySession["session-1"].GetIp())
	require.Equal(t, "", bySession["session-2"].GetName())
}

func TestList_IsolatedByGroup(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"user1", "group1",
		"user2", "group2",
	))
	gw := setupGateway(t, ta)

	ctx1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	startConnect(t, gw, ctx1, &gwpb.ConnectRequest{
		PeerName:  "box1",
		PublicKey: newPubKeyHex(t),
		SessionId: "session-1",
	})

	// group2 must not see group1's box, even on the same network name.
	list, err := gw.List(ctx2, &gwpb.ListRequest{})
	require.NoError(t, err)
	require.Empty(t, list.GetPeers())

	list, err = gw.List(ctx1, &gwpb.ListRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetPeers(), 1)
	require.Equal(t, "box1", list.GetPeers()[0].GetName())
}

func TestList_Unauthenticated(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	_, err := gw.List(context.Background(), &gwpb.ListRequest{})
	require.Error(t, err)
}

// fakeWatchStream implements gwsvcpb.GatewayService_WatchServer for testing.
// Only Context and Send are used by the Watch handler; the embedded nil
// grpc.ServerStream panics if anything else is called.
type fakeWatchStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses chan *gwpb.WatchResponse
}

func (f *fakeWatchStream) Context() context.Context { return f.ctx }
func (f *fakeWatchStream) Send(rsp *gwpb.WatchResponse) error {
	f.responses <- rsp
	return nil
}

// startWatch runs gw.Watch on a fake stream in the background. The returned
// cancel func closes the stream; the done channel receives Watch's return
// value.
func startWatch(t testing.TB, gw *Gateway, ctx context.Context, sessionID string) (*fakeWatchStream, context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	stream := &fakeWatchStream{ctx: ctx, responses: make(chan *gwpb.WatchResponse, 16)}
	done := make(chan error, 1)
	go func() { done <- gw.Watch(&gwpb.WatchRequest{SessionId: sessionID}, stream) }()
	return stream, cancel, done
}

func TestWatch_Unauthenticated(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	stream := &fakeWatchStream{ctx: context.Background(), responses: make(chan *gwpb.WatchResponse, 1)}
	err := gw.Watch(&gwpb.WatchRequest{SessionId: "session-1"}, stream)
	require.Error(t, err)
}

func TestWatch_MultipleWatchers(t *testing.T) {
	// A long fallback interval ensures both watchers can only be woken by
	// the registration/removal broadcast.
	flags.Set(t, "gateway.watch_fallback_poll_interval", time.Minute)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	stream1, _, done1 := startWatch(t, gw, ctx, "session-1")
	stream2, _, done2 := startWatch(t, gw, ctx, "session-1")

	_, cancelConnect, connectDone := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		PublicKey: newPubKeyHex(t),
		SessionId: "session-1",
	})

	// The broadcast wakes every watcher: both streams report the peer.
	for i, stream := range []*fakeWatchStream{stream1, stream2} {
		select {
		case rsp := <-stream.responses:
			require.Equal(t, "session-1", rsp.GetPeer().GetSessionId())
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for registration event on watcher %d", i+1)
		}
	}

	cancelConnect()
	require.NoError(t, <-connectDone)
	for i, done := range []chan error{done1, done2} {
		select {
		case err := <-done:
			require.True(t, status.IsNotFoundError(err), "watcher %d: expected NotFound, got: %v", i+1, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for watcher %d to report removal", i+1)
		}
	}
}

func TestWatch_RequiresSessionID(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	stream := &fakeWatchStream{ctx: ctx, responses: make(chan *gwpb.WatchResponse, 1)}
	err = gw.Watch(&gwpb.WatchRequest{}, stream)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)
}

func TestWatch_ReportsRegistrationAndRemoval(t *testing.T) {
	// A long fallback interval ensures this test can only pass via the push
	// notifications from addPeerLocked/removePeerLocked, not the backstop
	// ticker.
	flags.Set(t, "gateway.watch_fallback_poll_interval", time.Minute)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	// Watch starts before the peer exists.
	stream, _, watchDone := startWatch(t, gw, ctx, "session-1")

	_, cancelConnect, connectDone := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-1",
	})

	// Registration is pushed to the watcher.
	select {
	case rsp := <-stream.responses:
		require.Equal(t, "box1", rsp.GetPeer().GetName())
		require.Equal(t, "session-1", rsp.GetPeer().GetSessionId())
		require.NotEmpty(t, rsp.GetPeer().GetIp())
		require.Nil(t, rsp.GetPeer().GetLastHandshakeTime())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for registration event")
	}

	// Removal ends the watch with NotFound.
	cancelConnect()
	require.NoError(t, <-connectDone)
	select {
	case err := <-watchDone:
		require.True(t, status.IsNotFoundError(err), "expected NotFound, got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Watch to report removal")
	}
}

func TestWatch_IsolatedByGroup(t *testing.T) {
	flags.Set(t, "gateway.watch_fallback_poll_interval", 10*time.Millisecond)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1", "user2", "group2"))
	gw := setupGateway(t, ta)

	ctx1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	startConnect(t, gw, ctx1, &gwpb.ConnectRequest{
		PublicKey: newPubKeyHex(t),
		SessionId: "session-1",
	})

	// A watcher in another group must not see the peer, even across many
	// poll cycles; its watch behaves exactly like one for a nonexistent
	// session.
	stream, cancelWatch, watchDone := startWatch(t, gw, ctx2, "session-1")
	select {
	case rsp := <-stream.responses:
		t.Fatalf("group2 watcher saw group1's peer: %v", rsp)
	case <-time.After(300 * time.Millisecond):
	}
	cancelWatch()
	select {
	case err := <-watchDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Watch to return after cancel")
	}
}

// fakeConnectStream implements gwsvcpb.GatewayService_ConnectServer for
// testing. Only Context and Send are used by the Connect handler; the
// embedded nil grpc.ServerStream panics if anything else is called.
type fakeConnectStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses chan *gwpb.ConnectResponse
}

func (f *fakeConnectStream) Context() context.Context { return f.ctx }
func (f *fakeConnectStream) Send(rsp *gwpb.ConnectResponse) error {
	f.responses <- rsp
	return nil
}

// startConnect runs gw.Connect on a fake stream in the background and waits
// for the initial config response. The returned cancel func closes the
// stream (simulating the client going away); the done channel receives
// Connect's return value.
func startConnect(t testing.TB, gw *Gateway, ctx context.Context, req *gwpb.ConnectRequest) (*gwpb.ConnectResponse, context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	stream := &fakeConnectStream{ctx: ctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	done := make(chan error, 1)
	go func() { done <- gw.Connect(req, stream) }()
	select {
	case rsp := <-stream.responses:
		return rsp, cancel, done
	case err := <-done:
		t.Fatalf("Connect returned before sending config: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Connect response")
	}
	return nil, nil, nil
}

func TestConnect_LeasesRegistrationToStream(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey := newPubKeyHex(t)
	rsp, cancel, done := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   pubKey,
		SessionId:   "session-1",
	})
	require.NotEmpty(t, rsp.GetAssignedIp())
	require.NotEmpty(t, rsp.GetServerPublicKey())

	// While the stream is open, the peer is registered and listed with its
	// session ID.
	list, err := gw.List(ctx, &gwpb.ListRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetPeers(), 1)
	require.Equal(t, "box1", list.GetPeers()[0].GetName())
	require.Equal(t, "session-1", list.GetPeers()[0].GetSessionId())

	// Closing the stream deregisters the peer: IP, DNS name, and peer entry
	// are all freed.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Connect to return after stream close")
	}

	gw.mu.Lock()
	defer gw.mu.Unlock()
	require.NotContains(t, gw.peers, pubKey)
	_, inTUN := gw.tun.ipToNetwork.Load(netip.MustParseAddr(rsp.GetAssignedIp()))
	require.False(t, inTUN, "IP should be unregistered after stream close")
	_, nameExists := gw.networks["group1/net1"].names["box1"]
	require.False(t, nameExists, "DNS name should be freed after stream close")
}

func TestConnect_RefusesDuplicateName(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	_, cancel, done := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-1",
	})

	// A second connection requesting the same name is refused while the
	// first stream is open. Use a cancelable context so the test fails
	// rather than hanging if the conflict check ever regresses.
	cctx, ccancel := context.WithCancel(ctx)
	t.Cleanup(ccancel)
	stream := &fakeConnectStream{ctx: cctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-2",
	}, stream)
	require.True(t, status.IsAlreadyExistsError(err), "expected AlreadyExists, got: %v", err)

	// Once the first connection closes, the name is immediately reusable.
	cancel()
	require.NoError(t, <-done)
	rsp2, cancel2, done2 := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-3",
	})
	require.NotEmpty(t, rsp2.GetAssignedIp())
	cancel2()
	require.NoError(t, <-done2)
}

func TestConnect_RefusesDuplicatePublicKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey := newPubKeyHex(t)
	startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   pubKey,
		SessionId:   "session-1",
	})

	// A second connection reusing the same public key is refused, even with
	// a different peer name.
	cctx, ccancel := context.WithCancel(ctx)
	t.Cleanup(ccancel)
	stream := &fakeConnectStream{ctx: cctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box2",
		PublicKey:   pubKey,
		SessionId:   "session-2",
	}, stream)
	require.True(t, status.IsAlreadyExistsError(err), "expected AlreadyExists, got: %v", err)
}

func TestConnect_RefusesDuplicateSessionID(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	_, cancel, done := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-1",
	})

	// A second connection reusing the same session ID is refused, even with
	// a different key and name.
	cctx, ccancel := context.WithCancel(ctx)
	t.Cleanup(ccancel)
	stream := &fakeConnectStream{ctx: cctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box2",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-1",
	}, stream)
	require.True(t, status.IsAlreadyExistsError(err), "expected AlreadyExists, got: %v", err)

	// Once the first connection closes, the session ID is reusable.
	cancel()
	require.NoError(t, <-done)
	rsp, cancel2, done2 := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box2",
		PublicKey:   newPubKeyHex(t),
		SessionId:   "session-1",
	})
	require.NotEmpty(t, rsp.GetAssignedIp())
	cancel2()
	require.NoError(t, <-done2)
}

func TestRegister_RefusesDuplicatePublicKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey := newPubKeyHex(t)
	_, err = gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PublicKey: pubKey})
	require.NoError(t, err)

	// Re-registering the same public key is refused rather than silently
	// overwriting (and leaking) the previous registration.
	_, err = gw.Register(ctx, &gwpb.RegisterRequest{NetworkName: "net1", PublicKey: pubKey})
	require.True(t, status.IsAlreadyExistsError(err), "expected AlreadyExists, got: %v", err)
}

func TestConnect_UnnamedPeer(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	// Peers can connect without a name (e.g. transient bb ssh clients).
	pubKey := newPubKeyHex(t)
	rsp, cancel, done := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PublicKey:   pubKey,
		SessionId:   "session-1",
	})
	require.NotEmpty(t, rsp.GetAssignedIp())

	// Unnamed peers are still listed, identified by session ID.
	list, err := gw.List(ctx, &gwpb.ListRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetPeers(), 1)
	require.Equal(t, "", list.GetPeers()[0].GetName())
	require.Equal(t, "session-1", list.GetPeers()[0].GetSessionId())

	cancel()
	require.NoError(t, <-done)
	gw.mu.Lock()
	defer gw.mu.Unlock()
	require.NotContains(t, gw.peers, pubKey)
}

func TestConnect_ClosingOneLeaseLeavesOthersUntouched(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey1 := newPubKeyHex(t)
	_, cancel1, done1 := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   pubKey1,
		SessionId:   "session-1",
	})
	pubKey2 := newPubKeyHex(t)
	rsp2, _, _ := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box2",
		PublicKey:   pubKey2,
		SessionId:   "session-2",
	})

	cancel1()
	require.NoError(t, <-done1)

	// box2 is still registered, listed, and reachable at its IP; its
	// last_handshake_time is unset since no WireGuard handshake happened.
	list, err := gw.List(ctx, &gwpb.ListRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetPeers(), 1)
	require.Equal(t, "box2", list.GetPeers()[0].GetName())
	require.Equal(t, "session-2", list.GetPeers()[0].GetSessionId())
	require.Nil(t, list.GetPeers()[0].GetLastHandshakeTime())

	gw.mu.Lock()
	defer gw.mu.Unlock()
	require.NotContains(t, gw.peers, pubKey1)
	require.Contains(t, gw.peers, pubKey2)
	_, inTUN := gw.tun.ipToNetwork.Load(netip.MustParseAddr(rsp2.GetAssignedIp()))
	require.True(t, inTUN, "box2's IP should still be registered in the TUN")
	_, nameExists := gw.networks["group1/net1"].names["box2"]
	require.True(t, nameExists, "box2's DNS name should still be registered")
}

func TestConnect_RequiresSessionID(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	cctx, ccancel := context.WithCancel(ctx)
	t.Cleanup(ccancel)
	stream := &fakeConnectStream{ctx: cctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   newPubKeyHex(t),
	}, stream)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)
}

func TestConnect_SweepRemovalClosesStream(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	flags.Set(t, "gateway.stale_peer_timeout", 5*time.Second)
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey := newPubKeyHex(t)
	_, _, done := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "box1",
		PublicKey:   pubKey,
		SessionId:   "session-1",
	})

	// Backdate the peer so the stale-peer sweep reaps it (it never completed
	// a WireGuard handshake, so registeredAt is its last-seen baseline).
	gw.mu.Lock()
	gw.peers[pubKey].registeredAt = time.Now().Add(-10 * time.Second)
	gw.mu.Unlock()

	gw.cleanupStalePeers()

	// Removing the peer must close its Connect stream, with an Aborted error
	// so the client can distinguish eviction from a clean shutdown.
	select {
	case err := <-done:
		require.True(t, status.IsAbortedError(err), "expected Aborted, got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Connect to return after sweep removal")
	}
	gw.mu.Lock()
	defer gw.mu.Unlock()
	require.NotContains(t, gw.peers, pubKey)
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
	_, staleNameExists := ns.names["stale"]
	require.False(t, staleNameExists, "stale peer's DNS name should be removed")
	_, freshNameExists := ns.names["fresh"]
	require.True(t, freshNameExists, "fresh peer's DNS name should remain")
}
