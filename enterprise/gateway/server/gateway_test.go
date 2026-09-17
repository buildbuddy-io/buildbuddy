package server

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/apikeyauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/gatewayauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
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
	return setupGatewayWithOptions(t, Options{
		Authenticator: apikeyauth.New(ta),
		HubServices:   []HubService{DNSService()},
	})
}

func setupGatewayWithOptions(t testing.TB, opts Options) *Gateway {
	t.Helper()
	flags.Set(t, "gateway.udp_listen_port", freeUDPPort(t))
	flags.Set(t, "gateway.public_host", "127.0.0.1")

	gw, err := New(opts)
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

func TestConnect_AssignsSequentialIPs(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	resp1, _, _ := startConnect(t, gw, ctx, &gwpb.ConnectRequest{NetworkName: "net1", PeerName: "peer1", PublicKey: newPubKeyHex(t), SessionId: "session-1"})
	require.Equal(t, "fd00:bb::2", resp1.GetAssignedIp())
	require.Equal(t, "fd00:bb::1", resp1.GetGatewayIp())
	require.Equal(t, "fd00:bb::/48", resp1.GetNetworkCidr())
	require.NotEmpty(t, resp1.GetServerPublicKey())

	resp2, _, _ := startConnect(t, gw, ctx, &gwpb.ConnectRequest{NetworkName: "net1", PeerName: "peer2", PublicKey: newPubKeyHex(t), SessionId: "session-2"})
	require.Equal(t, "fd00:bb::3", resp2.GetAssignedIp())

	// Peers in the same network share the same server endpoint and public key.
	require.Equal(t, resp1.GetServerPublicKey(), resp2.GetServerPublicKey())
	require.Equal(t, resp1.GetServerEndpoint(), resp2.GetServerEndpoint())
}

func TestConnect_IsolatedNetworks(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"user1", "group1",
		"user2", "group2",
	))
	gw := setupGateway(t, ta)

	ctx1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	resp1, _, _ := startConnect(t, gw, ctx1, &gwpb.ConnectRequest{NetworkName: "net1", PublicKey: newPubKeyHex(t), SessionId: "session-1"})
	resp2, _, _ := startConnect(t, gw, ctx2, &gwpb.ConnectRequest{NetworkName: "net1", PublicKey: newPubKeyHex(t), SessionId: "session-2"})

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

func TestConnect_Unauthenticated(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	gw := setupGateway(t, ta)

	stream := &fakeConnectStream{ctx: context.Background(), responses: make(chan *gwpb.ConnectResponse, 1)}
	err := gw.Connect(&gwpb.ConnectRequest{NetworkName: "net1", PublicKey: newPubKeyHex(t), SessionId: "session-1"}, stream)
	require.Error(t, err)
}

func TestConnect_MissingPublicKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	stream := &fakeConnectStream{ctx: ctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{NetworkName: "net1", SessionId: "session-1"}, stream)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)
}

func TestConnect_InvalidPublicKey(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	stream := &fakeConnectStream{ctx: ctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{NetworkName: "net1", PublicKey: "notahexkey", SessionId: "session-1"}, stream)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)
}

func TestConnect_InvalidPeerName(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	for _, name := range []string{"foo.bar", "foo.bar.baz"} {
		stream := &fakeConnectStream{ctx: ctx, responses: make(chan *gwpb.ConnectResponse, 1)}
		err = gw.Connect(&gwpb.ConnectRequest{NetworkName: "net1", PeerName: name, PublicKey: newPubKeyHex(t), SessionId: "session-" + name}, stream)
		require.Errorf(t, err, "expected error for peer_name %q", name)
	}
}

func TestConnect_InvalidPeerNameLeavesNoPeer(t *testing.T) {
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	gw := setupGateway(t, ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)

	pubKey := newPubKeyHex(t)
	stream := &fakeConnectStream{ctx: ctx, responses: make(chan *gwpb.ConnectResponse, 1)}
	err = gw.Connect(&gwpb.ConnectRequest{NetworkName: "net1", PeerName: "not.one.label", PublicKey: pubKey, SessionId: "session-1"}, stream)
	require.Error(t, err)

	gw.mu.Lock()
	defer gw.mu.Unlock()
	require.Empty(t, gw.peers, "a rejected registration must not leave a peer behind")
	ipc, err := gw.dev.IpcGet()
	require.NoError(t, err)
	require.NotContains(t, ipc, pubKey, "the rejected key must not be configured on the WireGuard device")
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

	// Connect two peers in the same network.
	stalePubKey := newPubKeyHex(t)
	staleResp, _, _ := startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "stale",
		PublicKey:   stalePubKey,
		SessionId:   "session-stale",
	})

	freshPubKey := newPubKeyHex(t)
	startConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: "net1",
		PeerName:    "fresh",
		PublicKey:   freshPubKey,
		SessionId:   "session-fresh",
	})

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

// staticAuthenticator returns a fixed principal, standing in for any
// credential type. Gateway behavior depends only on the principal, so these
// tests cover cert- and API-key-shaped callers alike.
type staticAuthenticator struct {
	p *gatewayauth.Principal
}

func (s staticAuthenticator) Authenticate(context.Context, string) (*gatewayauth.Principal, error) {
	principal := *s.p
	return &principal, nil
}

func TestConnect_RecordsPrincipal(t *testing.T) {
	// What the authenticator reports is what the cleanup sweep acts on later
	// (credential expiry) and what log lines attribute, so it must be
	// recorded on the peer faithfully.
	expiry := time.Now().Add(12 * time.Hour)
	gw := setupGatewayWithOptions(t, Options{
		Authenticator: staticAuthenticator{&gatewayauth.Principal{
			User:      "vadim@buildbuddy.io",
			Namespace: "user:vadim@buildbuddy.io",
			ExpiresAt: expiry,
		}},
	})

	pubKey := newPubKeyHex(t)
	startConnect(t, gw, context.Background(), &gwpb.ConnectRequest{PublicKey: pubKey, SessionId: "session-1"})

	gw.mu.Lock()
	info := gw.peers[pubKey]
	gw.mu.Unlock()
	require.NotNil(t, info)
	require.Equal(t, "vadim@buildbuddy.io", info.user)
	require.Equal(t, expiry, info.expiresAt)
	require.Equal(t, "user:vadim@buildbuddy.io", info.networkState.namespace)
}

func TestCleanup_ExpiredCredentialEndsTheTunnel(t *testing.T) {
	// An expiring credential must not buy an unbounded tunnel. WireGuard
	// re-handshakes forever, so the cleanup sweep is what enforces expiry.
	gw := setupGatewayWithOptions(t, Options{
		Authenticator: staticAuthenticator{&gatewayauth.Principal{
			User:      "vadim@buildbuddy.io",
			Namespace: "user:vadim@buildbuddy.io",
			ExpiresAt: time.Now().Add(2 * time.Second),
		}},
	})

	pubKey := newPubKeyHex(t)
	startConnect(t, gw, context.Background(), &gwpb.ConnectRequest{PublicKey: pubKey, SessionId: "session-1"})

	gw.mu.Lock()
	_, registered := gw.peers[pubKey]
	gw.mu.Unlock()
	require.True(t, registered)

	require.Eventually(t, func() bool {
		gw.cleanupStalePeers()
		gw.mu.Lock()
		defer gw.mu.Unlock()
		_, stillThere := gw.peers[pubKey]
		return !stillThere
	}, 30*time.Second, 250*time.Millisecond, "the peer should be reaped once its credential expires")
}
