package grpc_client_test

import (
	"context"
	"net"
	"runtime/pprof"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testpprof"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
)

// trackingListener records accepted connections so that tests can close them.
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

func (l *trackingListener) closeConns() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		c.Close()
	}
	l.conns = nil
}

func startPingServer(t *testing.T) (*trackingListener, string) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	tl := &trackingListener{Listener: lis}
	s := grpc.NewServer()
	pspb.RegisterApiServer(s, &TestService{})
	go s.Serve(tl)
	t.Cleanup(s.Stop)
	return tl, "grpc://" + lis.Addr().String()
}

// pingWithLabels pings with request-style pprof labels on the calling goroutine.
func pingWithLabels(t *testing.T, conn *grpc.ClientConn) {
	pprof.Do(context.Background(), pprof.Labels("group_id", "GR_TEST"), func(ctx context.Context) {
		_, err := pspb.NewApiClient(conn).Ping(ctx, &pspb.PingRequest{})
		require.NoError(t, err)
	})
}

func requireTransportGoroutineLabels(t *testing.T, snap testpprof.Snapshot, want map[string]string) {
	// The in-process server's transport has its own (unlabeled) loopy writer, so
	// match the client's one by the function that runs it.
	for _, f := range []string{"http2Client).reader", "http2Client).keepalive", "loopyWriter).run&NewHTTP2Client"} {
		snap.RequireGoroutineLabels(t, f, want)
	}
}

func TestTransportGoroutineLabels_Dial(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	_, target := startPingServer(t)
	snap := testpprof.TakeSnapshot(t)

	var conn *grpc.ClientConn
	var err error
	pprof.Do(context.Background(), pprof.Labels("group_id", "GR_TEST"), func(ctx context.Context) {
		conn, err = grpc_client.DialSimpleWithoutPooling(target)
	})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	pingWithLabels(t, conn)

	requireTransportGoroutineLabels(t, snap, map[string]string{"subsystem": "grpc_client"})
}

func TestTransportGoroutineLabels_Reconnect(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	lis, target := startPingServer(t)

	conn, err := grpc_client.DialSimpleWithoutPooling(target)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	_, err = pspb.NewApiClient(conn).Ping(context.Background(), &pspb.PingRequest{})
	require.NoError(t, err)

	// Drop the connection and wait for the client conn to go idle, so that the
	// next RPC reconnects on behalf of a labeled goroutine.
	lis.closeConns()
	ctx, cancel := context.WithTimeout(t.Context(), testpprof.WaitTimeout)
	defer cancel()
	for conn.GetState() != connectivity.Idle {
		require.True(t, conn.WaitForStateChange(ctx, conn.GetState()), "timed out waiting for the client conn to go idle")
	}
	snap := testpprof.TakeSnapshot(t)

	pingWithLabels(t, conn)

	requireTransportGoroutineLabels(t, snap, map[string]string{"subsystem": "grpc_client"})
}

func TestCommonGRPCClientOptions_LabelHandlerOnlyWhenEnabled(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", false)
	off := len(grpc_client.CommonGRPCClientOptions())
	flags.Set(t, "monitoring.enable_pprof_labels", true)
	on := len(grpc_client.CommonGRPCClientOptions())
	require.Equal(t, off+1, on)
}
