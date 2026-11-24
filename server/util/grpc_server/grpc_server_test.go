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

type traceCounts struct {
	taskBegins, taskEnds, regionBegins, regionEnds int
}

func TestReflection(t *testing.T) {
	env := testenv.GetTestEnv(t)
	for _, disabled := range []bool{false, true} {
		if disabled {
			flags.Set(t, "grpc_disable_reflection", true)
		}
		s, err := grpc_server.New(env, 0, false, grpc_server.GRPCServerConfig{})
		require.NoError(t, err)
		t.Cleanup(s.GetServer().Stop)
		services := s.GetServer().GetServiceInfo()
		require.Contains(t, services, "grpc.health.v1.Health")
		for _, version := range []string{"v1", "v1alpha"} {
			_, registered := services["grpc.reflection."+version+".ServerReflection"]
			require.Equal(t, !disabled, registered, version)
		}
	}
}

// The server is built with the flag flipped, to check it applies without a
// restart.
func traceRPCs(t *testing.T, enabled bool) map[string]traceCounts {
	env := testenv.GetTestEnv(t)
	flags.Set(t, "monitoring.enable_trace_tasks", !enabled)
	s := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	flags.Set(t, "monitoring.enable_trace_tasks", enabled)
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
	// Waits for the server goroutines that send the End stats events.
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
			counts := traceRPCs(t, enabled)

			want := map[string]traceCounts{}
			if enabled {
				want = map[string]traceCounts{
					pingMethod:   {1, 1, 1, 1},
					streamMethod: {1, 1, 1, 1},
					// gRPC never ends unknown methods, so their task stays
					// open.
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
