package grpc_server

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

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
			log.Infof("Graceful stop of GRPC server succeeded.")
			grpcServer.Stop()
		case <-time.After(delay):
			log.Warningf("Hard-stopping GRPC Server!")
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

func extractInvocationID(ctx context.Context) []attribute.KeyValue {
	if iid := bazel_request.GetInvocationID(ctx); iid != "" {
		return []attribute.KeyValue{attribute.String("invocation_id", iid)}
	}
	return nil
}

func propagateInvocationIDToSpan(ctx context.Context) {
	invocationId := bazel_request.GetInvocationID(ctx)
	if invocationId == "" {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("invocation_id", invocationId))
}

func propagateInvocationIDToSpanUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		propagateInvocationIDToSpan(ctx)
		return handler(ctx, req)
	}
}

func propagateInvocationIDToSpanStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		propagateInvocationIDToSpan(stream.Context())
		return handler(srv, stream)
	}
}

func CommonGRPCServerOptions(env environment.Env) []grpc.ServerOption {
	return []grpc.ServerOption{
		filters.GetUnaryInterceptor(env),
		filters.GetStreamInterceptor(env),
		grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor(), propagateInvocationIDToSpanUnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor(), propagateInvocationIDToSpanStreamServerInterceptor()),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.MaxRecvMsgSize(env.GetConfigurator().GetGRPCMaxRecvMsgSizeBytes()),
	}
}
