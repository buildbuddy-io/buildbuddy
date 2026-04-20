package interceptors_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
)

type pingServer struct {
	lastClientIP string
}

func (p *pingServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	p.lastClientIP = clientip.Get(ctx)
	return &pspb.PingResponse{
		Tag: req.GetTag(),
	}, nil
}

func TestClientIPInterceptor_FallsBackToPeerAddress(t *testing.T) {
	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	env := testenv.GetTestEnv(t)
	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	require.NoError(t, err)
	go func() {
		grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client := pspb.NewApiClient(conn)
	_, err = client.Ping(ctx, &pspb.PingRequest{Tag: 123})
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1", ps.lastClientIP)
}

func BenchmarkBare(b *testing.B) {
	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))
	grpcServer := grpc.NewServer()
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

func BenchmarkInstrumentedClient(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))

	grpcServer := grpc.NewServer()
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc_client.DialSimple("grpc://" + listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

func BenchmarkInstrumentedServer(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))

	env := testenv.GetTestEnv(b)
	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

func BenchmarkInstrumentedAll(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))

	env := testenv.GetTestEnv(b)
	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc_client.DialSimple("grpc://" + listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}
