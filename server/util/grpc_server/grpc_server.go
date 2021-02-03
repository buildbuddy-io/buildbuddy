package grpc_server

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/filters"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
)

func GRPCShutdown(ctx context.Context, grpcServer *grpc.Server) error {
	// Attempt to graceful stop this grpcServer. Graceful stop will
	// disallow new connections, but existing ones are allowed to
	// finish. To ensure this doesn't hang forever, we also kick off
	// a goroutine that will hard stop the server 100ms before the
	// shutdown function deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		grpcServer.Stop()
		return nil
	}
	delay := deadline.Sub(time.Now()) - (100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(ctx, delay)
	go func() {
		select {
		case <-ctx.Done():
			log.Printf("Graceful stop of GRPC server succeeded.")
			grpcServer.Stop()
		case <-time.After(delay):
			log.Printf("Hard-stopping GRPC Server!")
			grpcServer.Stop()
		}
	}()
	grpcServer.GracefulStop()
	cancel()
	return nil
}

func GRPCShutdownFunc(grpcServer *grpc.Server) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return GRPCShutdown(ctx, grpcServer)
	}
}

func CommonGRPCServerOptions(env environment.Env) []grpc.ServerOption {
	return []grpc.ServerOption{
		filters.GetUnaryInterceptor(env),
		filters.GetStreamInterceptor(env),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.MaxRecvMsgSize(env.GetConfigurator().GetGRPCMaxRecvMsgSizeBytes()),
	}
}
