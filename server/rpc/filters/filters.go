package filters

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
)

type wrappedStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStreamWithContext) Context() context.Context {
	return w.ctx
}

// contextReplacingStreamInterceptor is a helper to make a stream interceptor that modifies a context.
func contextReplacingStreamInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedStreamWithContext{stream, ctxFn(stream.Context())})
	}
}

// contextReplacingStreamInterceptor is a helper to make a unary interceptor that modifies a context.
func contextReplacingUnaryInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctxFn(ctx), req)
	}
}

func addAuthToContext(env environment.Env, ctx context.Context) context.Context {
	if auth := env.GetAuthenticator(); auth != nil {
		return auth.AuthenticateGRPCRequest(ctx)
	}
	return ctx
}

func authStreamInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	ctxFn := func(ctx context.Context) context.Context {
		return addAuthToContext(env, ctx)
	}
	return contextReplacingStreamInterceptor(ctxFn)
}

func authUnaryInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	ctxFn := func(ctx context.Context) context.Context {
		return addAuthToContext(env, ctx)
	}
	return contextReplacingUnaryInterceptor(ctxFn)
}

func addRequestIdToContext(ctx context.Context) context.Context {
	if rctx, err := uuid.SetInContext(ctx); err == nil {
		return rctx
	}
	return ctx
}

func requestIDStreamInterceptor() grpc.StreamServerInterceptor {
	return contextReplacingStreamInterceptor(addRequestIdToContext)
}

func requestIDUnaryInterceptor() grpc.UnaryServerInterceptor {
	return contextReplacingUnaryInterceptor(addRequestIdToContext)
}

func logRequestUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		r, err := handler(ctx, req)
		log.LogGRPCRequest(ctx, info.FullMethod, time.Now().Sub(start), err)
		return r, err
	}
}

func logRequestStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, &wrappedStreamWithContext{stream, stream.Context()})
		log.LogGRPCRequest(stream.Context(), info.FullMethod, time.Now().Sub(start), err)
		return err
	}
}

func GetUnaryInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		requestIDUnaryInterceptor(),
		logRequestUnaryInterceptor(),
		authUnaryInterceptor(env),
	)
}

func GetStreamInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		requestIDStreamInterceptor(),
		logRequestStreamInterceptor(),
		authStreamInterceptor(env),
	)
}
