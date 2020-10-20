package filters

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bblog "github.com/buildbuddy-io/buildbuddy/server/util/log"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
)

var (
	headerContextKeys map[string]string

	once sync.Once

	// *context.CancelFunc -> *context.CancelFunc
	activeCancelFuncs sync.Map
)

func init() {
	headerContextKeys = map[string]string{
		"x-buildbuddy-jwt": "x-buildbuddy-jwt",
		"build.bazel.remote.execution.v2.requestmetadata-bin": "build.bazel.remote.execution.v2.requestmetadata-bin",
	}
}

func startCancelRoutine(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	go func() {
		delay := deadline.Sub(time.Now()) - time.Second
		ctx, cancel := context.WithTimeout(ctx, delay)
		defer cancel()
		select {
		case <-ctx.Done():
			cancelActiveContexts(ctx)
		case <-time.After(delay):
			cancelActiveContexts(ctx)
		}
	}()
	return nil
}

func cancelActiveContexts(ctx context.Context) {
	activeCancelFuncs.Range(func(key, value interface{}) bool {
		cancelFunc, ok := value.(*context.CancelFunc)
		if ok && cancelFunc != nil {
			(*cancelFunc)()
		}
		return true
	})
}

type wrappedServerStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStreamWithContext) Context() context.Context {
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
		return streamer(ctxFn(ctx), desc, cc, method, opts...)
	}
}

// contextReplacingUnaryClientInterceptor is a helper to make a unary interceptor that modifies a context.
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
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for headerName, contextKey := range headerContextKeys {
			if headerVals := md.Get(headerName); len(headerVals) > 0 {
				ctx = context.WithValue(ctx, contextKey, headerVals[0])
			}
		}
	}

	return ctx
}

func setHeadersFromContext(ctx context.Context) context.Context {
	for headerName, contextKey := range headerContextKeys {
		if contextVal, ok := ctx.Value(contextKey).(string); ok {
			ctx = metadata.AppendToOutgoingContext(ctx, headerName, contextVal)
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
		err := handler(srv, stream)
		bblog.LogGRPCRequest(stream.Context(), info.FullMethod, time.Now().Sub(start), err)
		return err
	}
}

// shutdownContextUnaryServerInterceptor cancels the context if the server is
// shutting down.
func shutdownContextUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	once.Do(func() {
		env.GetHealthChecker().RegisterShutdownFunction(startCancelRoutine)
	})
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		activeCancelFuncs.Store(&cancel, &cancel)
		r, err := handler(ctx, req)
		activeCancelFuncs.Delete(&cancel)
		return r, err
	}
}

// shutdownContextStreamServerInterceptor cancels the context if the server is
// shutting down.
func shutdownContextStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	once.Do(func() {
		env.GetHealthChecker().RegisterShutdownFunction(startCancelRoutine)
	})
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, cancel := context.WithCancel(stream.Context())
		defer cancel()
		activeCancelFuncs.Store(&cancel, &cancel)
		err := handler(srv, &wrappedServerStreamWithContext{stream, ctx})
		activeCancelFuncs.Delete(&cancel)
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
		shutdownContextUnaryServerInterceptor(env),
	)
}

func GetStreamInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		requestIDStreamServerInterceptor(),
		logRequestStreamServerInterceptor(),
		authStreamServerInterceptor(env),
		copyHeadersStreamServerInterceptor(),
		shutdownContextStreamServerInterceptor(env),
	)
}

func GetUnaryClientInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		grpc_prometheus.UnaryClientInterceptor,
		setHeadersUnaryClientInterceptor(),
	)
}

func GetStreamClientInterceptor() grpc.DialOption {
	return grpc.WithChainStreamInterceptor(
		grpc_prometheus.StreamClientInterceptor,
		setHeadersStreamClientInterceptor(),
	)
}
