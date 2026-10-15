package grpc_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime/trace"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	exptrace "golang.org/x/exp/trace"
)

const pingMethod = "/ping.Api/Ping"

type pingServer struct {
	pspb.UnimplementedApiServer
}

func (pingServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	return &pspb.PingResponse{Tag: req.GetTag()}, nil
}

// endNotifier is registered after the trace task stats handler, so by the time
// it sees an RPC's End event that handler has already ended the RPC's task.
// Unlike the response arriving at the client, which happens before the End
// event, this is a reliable sync point.
type endNotifier struct{ ended chan struct{} }

func (e endNotifier) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (e endNotifier) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (e endNotifier) HandleConn(context.Context, stats.ConnStats) {}
func (e endNotifier) HandleRPC(_ context.Context, rs stats.RPCStats) {
	if _, ok := rs.(*stats.End); ok {
		e.ended <- struct{}{}
	}
}

// traceCounts counts the begin and end events of tasks of a given type, and of
// regions of the same type inside them.
type traceCounts struct {
	taskBegins, taskEnds, regionBegins, regionEnds int
}

func tracePing(t *testing.T) traceCounts {
	ended := make(chan struct{}, 1)
	env := testenv.GetTestEnv(t)
	s := grpc.NewServer(grpc_server.CommonGRPCServerOptionsWithConfig(env, grpc_server.GRPCServerConfig{
		ExtraStatsHandlers: []stats.Handler{endNotifier{ended}},
	})...)
	pspb.RegisterApiServer(s, pingServer{})
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	var buf bytes.Buffer
	require.NoError(t, trace.Start(&buf))
	// Make sure the trace is stopped even if the test fails.
	var once sync.Once
	stop := func() { once.Do(trace.Stop) }
	t.Cleanup(stop)
	_, err = pspb.NewApiClient(conn).Ping(context.Background(), &pspb.PingRequest{})
	require.NoError(t, err)
	select {
	case <-ended:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "RPC did not finish")
	}
	stop()

	r, err := exptrace.NewReader(&buf)
	require.NoError(t, err)
	var c traceCounts
	tasks := map[exptrace.TaskID]bool{}
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		switch ev.Kind() {
		case exptrace.EventTaskBegin:
			if ev.Task().Type == pingMethod {
				c.taskBegins++
				tasks[ev.Task().ID] = true
			}
		case exptrace.EventTaskEnd:
			if ev.Task().Type == pingMethod {
				c.taskEnds++
			}
		case exptrace.EventRegionBegin:
			if r := ev.Region(); tasks[r.Task] && r.Type == pingMethod {
				c.regionBegins++
			}
		case exptrace.EventRegionEnd:
			if r := ev.Region(); tasks[r.Task] && r.Type == pingMethod {
				c.regionEnds++
			}
		default:
		}
	}
	return c
}

func TestTraceTasks(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			flags.Set(t, "monitoring.enable_trace_tasks", enabled)

			c := tracePing(t)

			n := 0
			if enabled {
				n = 1
			}
			require.Equal(t, traceCounts{n, n, n, n}, c)
		})
	}
}
