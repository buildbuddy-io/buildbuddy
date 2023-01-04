package interceptors_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"google.golang.org/grpc"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
)

type pingServer struct{}

func (p *pingServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	return &pspb.PingResponse{
		Tag: req.GetTag(),
	}, nil
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

	conn, err := grpc_client.DialTarget("grpc://" + listenAddr)
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

	conn, err := grpc_client.DialTarget("grpc://" + listenAddr)
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
