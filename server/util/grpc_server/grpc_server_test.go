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

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	exptrace "golang.org/x/exp/trace"
	gstatus "google.golang.org/grpc/status"
)

const (
	pingMethod    = "/ping.Api/Ping"
	unknownMethod = "/ping.Api/Unknown"
	streamMethod  = "/test.Streamer/Stream"
)

type pingServer struct {
	pspb.UnimplementedApiServer
}

func (pingServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	return &pspb.PingResponse{Tag: req.GetTag()}, nil
}

var streamDesc = grpc.StreamDesc{
	StreamName:    "Stream",
	ServerStreams: true,
	Handler:       func(any, grpc.ServerStream) error { return nil },
}

// traceCounts counts the begin and end events of tasks of a given type, and of
// regions of the same type inside them.
type traceCounts struct {
	taskBegins, taskEnds, regionBegins, regionEnds int
}

// traceRPCs makes a unary RPC, a streaming RPC and an RPC to an unknown method
// while recording a trace, and returns the trace's counts by method.
func traceRPCs(t *testing.T) map[string]traceCounts {
	env := testenv.GetTestEnv(t)
	s := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	pspb.RegisterApiServer(s, pingServer{})
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Streamer",
		HandlerType: (*any)(nil),
		Streams:     []grpc.StreamDesc{streamDesc},
	}, struct{}{})
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
	ctx := context.Background()
	_, err = pspb.NewApiClient(conn).Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)
	err = conn.Invoke(ctx, unknownMethod, &pspb.PingRequest{}, &pspb.PingResponse{})
	require.Equal(t, codes.Unimplemented, gstatus.Code(err), "error: %v", err)
	cs, err := conn.NewStream(ctx, &streamDesc, streamMethod)
	require.NoError(t, err)
	require.NoError(t, cs.CloseSend())
	require.Equal(t, io.EOF, cs.RecvMsg(&pspb.PingResponse{}))
	// The client can see an RPC finish before the server has sent the End
	// stats event. GracefulStop waits for the handler goroutines, which send
	// it, to return.
	require.NoError(t, conn.Close())
	s.GracefulStop()
	stop()

	r, err := exptrace.NewReader(&buf)
	require.NoError(t, err)
	counts := map[string]traceCounts{}
	taskTypes := map[exptrace.TaskID]string{}
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		switch ev.Kind() {
		case exptrace.EventTaskBegin:
			task := ev.Task()
			taskTypes[task.ID] = task.Type
			c := counts[task.Type]
			c.taskBegins++
			counts[task.Type] = c
		case exptrace.EventTaskEnd:
			task := ev.Task()
			c := counts[task.Type]
			c.taskEnds++
			counts[task.Type] = c
		case exptrace.EventRegionBegin:
			if reg := ev.Region(); taskTypes[reg.Task] == reg.Type {
				c := counts[reg.Type]
				c.regionBegins++
				counts[reg.Type] = c
			}
		case exptrace.EventRegionEnd:
			if reg := ev.Region(); taskTypes[reg.Task] == reg.Type {
				c := counts[reg.Type]
				c.regionEnds++
				counts[reg.Type] = c
			}
		default:
		}
	}
	return counts
}

func TestTraceTasks(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			flags.Set(t, "monitoring.enable_trace_tasks", enabled)

			counts := traceRPCs(t)

			want := map[string]traceCounts{}
			if enabled {
				want = map[string]traceCounts{
					pingMethod:   {1, 1, 1, 1},
					streamMethod: {1, 1, 1, 1},
					// gRPC sends no End event for unknown methods, so their
					// task is never ended. Their region must not be left
					// open, though.
					unknownMethod: {1, 0, 0, 0},
				}
			}
			got := map[string]traceCounts{}
			for _, m := range []string{pingMethod, streamMethod, unknownMethod} {
				if c, ok := counts[m]; ok {
					got[m] = c
				}
			}
			require.Equal(t, want, got)
		})
	}
}
