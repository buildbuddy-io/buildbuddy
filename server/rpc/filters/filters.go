package filters

import (
	"context"
	"log"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	bblog "github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	headerContextKeys map[string]string
)

func init() {
	headerContextKeys = map[string]string{
		"x-buildbuddy-jwt": "x-buildbuddy-jwt",
	}
}

type wrappedServerStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStreamWithContext) Context() context.Context {
	return w.ctx
}

type wrappedClientStreamWithContext struct {
	grpc.ClientStream
	ctx context.Context
}

func (w *wrappedClientStreamWithContext) Context() context.Context {
	return w.ctx
}

// contextReplacingStreamServerInterceptor is a helper to make a stream interceptor that modifies a context.
func contextReplacingStreamServerInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedServerStreamWithContext{stream, ctxFn(stream.Context())})
	}
}

// contextReplacingStreamServerInterceptor is a helper to make a unary interceptor that modifies a context.
func contextReplacingUnaryServerInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctxFn(ctx), req)
	}
}

// contextReplacingStreamClientInterceptor is a helper to make a stream interceptor that modifies a context.
func contextReplacingStreamClientInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &wrappedClientStreamWithContext{s, ctxFn(ctx)}, nil
	}
}

// contextReplacingStreamClientInterceptor is a helper to make a unary interceptor that modifies a context.
func contextReplacingUnaryClientInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(ctxFn(ctx), method, req, reply, cc, opts...)
	}
}

func addAuthToContext(env environment.Env, ctx context.Context) context.Context {
	if auth := env.GetAuthenticator(); auth != nil {
		return auth.AuthenticateGRPCRequest(ctx)
	}
	return ctx
}

func addRequestIdToContext(ctx context.Context) context.Context {
	if rctx, err := uuid.SetInContext(ctx); err == nil {
		return rctx
	}
	return ctx
}

func copyHeadersToContext(ctx context.Context) context.Context {
	log.Printf("copyHeadersToContext called")
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for headerName, contextKey := range headerContextKeys {
			if headerVals := md.Get(headerName); len(headerVals) > 0 {
				ctx = context.WithValue(ctx, contextKey, headerVals[0])
				log.Printf("Set key %q in context to %q (from header: %q)", contextKey, headerVals[0], headerName)
			}
		}
	}

	return ctx
}

func setHeadersFromContext(ctx context.Context) context.Context {
	log.Printf("setHeadersFromContext called")
	for headerName, contextKey := range headerContextKeys {
		if contextVal, ok := ctx.Value(contextKey).(string); ok {
			ctx = metadata.AppendToOutgoingContext(ctx, headerName, contextVal)
			log.Printf("Set key %q in header to %q (from context key: %q)", headerName, contextVal, contextKey)
		}
	}
	return ctx
}

// authStreamServerInterceptor is a server interceptor that applies the authenticator
// middleware to the request.
func authStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	ctxFn := func(ctx context.Context) context.Context {
		return addAuthToContext(env, ctx)
	}
	return contextReplacingStreamServerInterceptor(ctxFn)
}

// authUnaryServerInterceptor is a server interceptor that applies the authenticator
// middleware to the request.
func authUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	ctxFn := func(ctx context.Context) context.Context {
		return addAuthToContext(env, ctx)
	}
	return contextReplacingUnaryServerInterceptor(ctxFn)
}

// requestIDStreamInterceptor is a server interceptor that inserts a request ID
// into the context if one is not already present.
func requestIDStreamServerInterceptor() grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(addRequestIdToContext)
}

// requestIDUnaryInterceptor is a server interceptor that inserts a request ID
// into the context if one is not already present.
func requestIDUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(addRequestIdToContext)
}

// logRequestUnaryServerInterceptor defers a call to log the GRPC request.
func logRequestUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		r, err := handler(ctx, req)
		bblog.LogGRPCRequest(ctx, info.FullMethod, time.Now().Sub(start), err)
		return r, err
	}
}

// logRequestUnaryServerInterceptor defers a call to log the GRPC request.
func logRequestStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, &wrappedServerStreamWithContext{stream, stream.Context()})
		bblog.LogGRPCRequest(stream.Context(), info.FullMethod, time.Now().Sub(start), err)
		return err
	}
}

// copyHeadersStreamInterceptor is a server interceptor that copies certain
// headers present in the grpc metadata into the context.
func copyHeadersStreamServerInterceptor() grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(copyHeadersToContext)
}

// copyHeadersUnaryInterceptor is a server interceptor that copies certain
// headers present in the grpc metadata into the context.
func copyHeadersUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(copyHeadersToContext)
}

// setHeadersUnaryClientInterceptor is a server interceptor that copies certain
// headers present in the context into the outgoing grpc metadata.
func setHeadersUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return contextReplacingUnaryClientInterceptor(setHeadersFromContext)
}

// setHeadersStreamClientInterceptor is a server interceptor that copies certain
// headers present in the context into the outgoing grpc metadata.
func setHeadersStreamClientInterceptor() grpc.StreamClientInterceptor {
	return contextReplacingStreamClientInterceptor(setHeadersFromContext)
}

func GetUnaryInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		requestIDUnaryServerInterceptor(),
		logRequestUnaryServerInterceptor(),
		authUnaryServerInterceptor(env),
		copyHeadersUnaryServerInterceptor(),
	)
}

func GetStreamInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		requestIDStreamServerInterceptor(),
		logRequestStreamServerInterceptor(),
		authStreamServerInterceptor(env),
		copyHeadersStreamServerInterceptor(),
	)
}

func GetUnaryClientInterceptor(env environment.Env) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		setHeadersUnaryClientInterceptor(),
	)
}

func GetStreamClientInterceptor(env environment.Env) grpc.DialOption {
	return grpc.WithChainStreamInterceptor(
		setHeadersStreamClientInterceptor(),
	)
}
