package filters

import (
	"context"
	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

type wrappedStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStreamWithContext) Context() context.Context {
	return w.ctx
}

func authStreamInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if auth := env.GetAuthenticator(); auth != nil {
			ctx = auth.AuthenticateGRPCRequest(ctx)
		}
		return handler(srv, &wrappedStreamWithContext{stream, ctx})
	}
}

func authUnaryInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if auth := env.GetAuthenticator(); auth != nil {
			ctx = auth.AuthenticateGRPCRequest(ctx)
		}
		return handler(ctx, req)
	}
}

func GetUnaryInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		authUnaryInterceptor(env),
	)
}

func GetStreamInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		authStreamInterceptor(env),
	)
}
