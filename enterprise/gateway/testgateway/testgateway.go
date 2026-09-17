// Package testgateway is a fixture for bringing up and connecting to a gateway instance.
package testgateway

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"google.golang.org/grpc"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

// Setup starts a gateway with the given options on a free UDP port,
// registered for cleanup with t.
func Setup(t testing.TB, opts server.Options) *server.Gateway {
	t.Helper()
	flags.Set(t, "gateway.udp_listen_port", freeUDPPort(t))
	flags.Set(t, "gateway.public_host", "127.0.0.1")

	gw, err := server.New(opts)
	require.NoError(t, err)
	t.Cleanup(gw.Close)
	return gw
}

func freeUDPPort(t testing.TB) int {
	t.Helper()
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()
	return port
}

// Peer is a connected WireGuard client.
type Peer struct {
	Net          *netstack.Net
	Addr         netip.Addr
	GatewayIP    netip.Addr // the network's hub IP, where hub services listen
	AssignedName string
	SessionID    string
	// Disconnect closes the peer's Connect stream, as if its gRPC connection
	// had dropped, and returns once the gateway has removed the
	// registration. The peer's WireGuard device is left running, so from the
	// gateway's point of view the client simply went dark.
	Disconnect func()
}

// sessionCounter generates unique session IDs for peers.
var sessionCounter atomic.Int64

// NextSessionID returns a fresh unique session ID.
func NextSessionID() string {
	return fmt.Sprintf("e2e-session-%d", sessionCounter.Add(1))
}

// RegisterAndConnect connects a new peer to the gateway via the streaming
// Connect RPC and brings up a userspace WireGuard tunnel for it. The
// registration is leased to the Connect stream, which stays open (and the
// tunnel up) until the test ends.
func RegisterAndConnect(t testing.TB, gw *server.Gateway, ctx context.Context, networkName, peerName string) Peer {
	t.Helper()
	return RegisterAndConnectWithSessionID(t, gw, ctx, networkName, peerName, NextSessionID())
}

// RegisterAndConnectAs is RegisterAndConnect for credentials that have to name
// the WireGuard key being registered, as tunnel certificates do: ctxFor is
// called with the freshly generated public key.
func RegisterAndConnectAs(t testing.TB, gw *server.Gateway, ctxFor func(pubKeyHex string) context.Context, networkName, peerName string) Peer {
	t.Helper()
	return connectPeer(t, gw, ctxFor, networkName, peerName, NextSessionID())
}

// RegisterAndConnectWithSessionID is like RegisterAndConnect but allows session ID to be specified by the caller.
func RegisterAndConnectWithSessionID(t testing.TB, gw *server.Gateway, ctx context.Context, networkName, peerName, sessionID string) Peer {
	t.Helper()
	return connectPeer(t, gw, func(string) context.Context { return ctx }, networkName, peerName, sessionID)
}

// connectPeer is the fully general form: explicit session ID and a per-key
// credential context.
//
// persistent_keepalive_interval=1 is used so that the first outbound packet
// triggers an immediate WireGuard handshake rather than waiting for the
// gateway to initiate one.
func connectPeer(t testing.TB, gw *server.Gateway, ctxFor func(pubKeyHex string) context.Context, networkName, peerName, sessionID string) Peer {
	t.Helper()
	privKey, err := wgkeys.GeneratePrivateKey()
	require.NoError(t, err)
	ctx := ctxFor(privKey.PublicKey().Hex())

	resp, cancel, done := StartConnect(t, gw, ctx, &gwpb.ConnectRequest{
		NetworkName: networkName,
		PeerName:    peerName,
		PublicKey:   privKey.PublicKey().Hex(),
		SessionId:   sessionID,
	})

	addr := netip.MustParseAddr(resp.GetAssignedIp())
	gatewayIP := netip.MustParseAddr(resp.GetGatewayIp())
	tunDev, tnet, err := netstack.CreateNetTUN(
		[]netip.Addr{addr},
		// Use the gateway's hub IP as the DNS resolver so peer names
		// registered with peer_name are resolvable by name.
		[]netip.Addr{gatewayIP},
		1420,
	)
	require.NoError(t, err)

	logger := &device.Logger{
		Verbosef: func(format string, args ...any) {},
		Errorf:   func(format string, args ...any) { t.Logf("wg: "+format, args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), logger)
	t.Cleanup(dev.Close)

	ipc := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=1\n",
		privKey.Hex(), resp.GetServerPublicKey(), resp.GetNetworkCidr(), resp.GetServerEndpoint(),
	)
	require.NoError(t, dev.IpcSet(ipc))
	require.NoError(t, dev.Up())

	// Peer names are unique under Connect, so the assigned name is always
	// the requested name.
	return Peer{
		Net:          tnet,
		Addr:         addr,
		GatewayIP:    gatewayIP,
		AssignedName: peerName,
		SessionID:    sessionID,
		Disconnect: sync.OnceFunc(func() {
			cancel()
			<-done
		}),
	}
}

// connectStream implements gwsvcpb.GatewayService_ConnectServer. Only Context
// and Send are used by the Connect handler; the embedded nil grpc.ServerStream
// panics if anything else is called.
type connectStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses chan *gwpb.ConnectResponse
}

func (f *connectStream) Context() context.Context { return f.ctx }
func (f *connectStream) Send(rsp *gwpb.ConnectResponse) error {
	f.responses <- rsp
	return nil
}

// StartConnect runs gw.Connect on a fake stream in the background and waits
// for the initial config response. The returned cancel func closes the stream
// (simulating the client going away); the done channel receives Connect's
// return value.
func StartConnect(t testing.TB, gw *server.Gateway, ctx context.Context, req *gwpb.ConnectRequest) (*gwpb.ConnectResponse, context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	stream := &connectStream{ctx: ctx, responses: make(chan *gwpb.ConnectResponse, 1)}
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

// watchStream implements gwsvcpb.GatewayService_WatchServer. Only Context and
// Send are used by the Watch handler; the embedded nil grpc.ServerStream panics
// if anything else is called.
type watchStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses chan *gwpb.WatchResponse
}

func (f *watchStream) Context() context.Context { return f.ctx }
func (f *watchStream) Send(rsp *gwpb.WatchResponse) error {
	f.responses <- rsp
	return nil
}

// StartWatch runs gw.Watch for sessionID on a fake stream in the background
// and returns the channel its responses arrive on. The returned cancel func
// closes the stream; the done channel receives Watch's return value.
func StartWatch(t testing.TB, gw *server.Gateway, ctx context.Context, sessionID string) (<-chan *gwpb.WatchResponse, context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	stream := &watchStream{ctx: ctx, responses: make(chan *gwpb.WatchResponse, 16)}
	done := make(chan error, 1)
	go func() { done <- gw.Watch(&gwpb.WatchRequest{SessionId: sessionID}, stream) }()
	return stream.responses, cancel, done
}

var (
	_ gwsvcpb.GatewayService_ConnectServer = (*connectStream)(nil)
	_ gwsvcpb.GatewayService_WatchServer   = (*watchStream)(nil)
)
