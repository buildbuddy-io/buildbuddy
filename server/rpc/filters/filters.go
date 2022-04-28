package filters

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/role_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	bblog "github.com/buildbuddy-io/buildbuddy/server/util/log"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
)

const (
	buildBuddyServicePrefix = "/buildbuddy.service.BuildBuddyService/"
)

var (
	headerContextKeys map[string]string
	once              sync.Once
)

func init() {
	headerContextKeys = map[string]string{
		"x-buildbuddy-jwt": "x-buildbuddy-jwt",
		"build.bazel.remote.execution.v2.requestmetadata-bin": "build.bazel.remote.execution.v2.requestmetadata-bin",
	}
}

type wrappedServerStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStreamWithContext) Context() context.Context {
	return w.ctx
}

// ContextReplacingStreamServerInterceptor is a helper to make a stream interceptor that modifies a context.
func ContextReplacingStreamServerInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedServerStreamWithContext{stream, ctxFn(stream.Context())})
	}
}

// ContextReplacingStreamServerInterceptor is a helper to make a unary interceptor that modifies a context.
func ContextReplacingUnaryServerInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.UnaryServerInterceptor {
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
		return auth.AuthenticatedGRPCContext(ctx)
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
	return ContextReplacingStreamServerInterceptor(ctxFn)
}

// authUnaryServerInterceptor is a server interceptor that applies the authenticator
// middleware to the request.
func authUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	ctxFn := func(ctx context.Context) context.Context {
		return addAuthToContext(env, ctx)
	}
	return ContextReplacingUnaryServerInterceptor(ctxFn)
}

func roleAuthStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasPrefix(info.FullMethod, buildBuddyServicePrefix) {
			methodName := strings.TrimPrefix(info.FullMethod, buildBuddyServicePrefix)
			if err := role_filter.AuthorizeRPC(stream.Context(), env, methodName); err != nil {
				return err
			}
		}
		return handler(srv, stream)
	}
}

func roleAuthUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if strings.HasPrefix(info.FullMethod, buildBuddyServicePrefix) {
			methodName := strings.TrimPrefix(info.FullMethod, buildBuddyServicePrefix)
			if err := role_filter.AuthorizeRPC(ctx, env, methodName); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

// requestIDStreamInterceptor is a server interceptor that inserts a request ID
// into the context if one is not already present.
func requestIDStreamServerInterceptor() grpc.StreamServerInterceptor {
	return ContextReplacingStreamServerInterceptor(addRequestIdToContext)
}

// requestIDUnaryInterceptor is a server interceptor that inserts a request ID
// into the context if one is not already present.
func requestIDUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return ContextReplacingUnaryServerInterceptor(addRequestIdToContext)
}

// requestContextProtoUnaryServerInterceptor is a server interceptor that
// copies the request context from the request message into the context.
func requestContextProtoUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		msg, ok := req.(proto.Message)
		if ok {
			protoCtx := requestcontext.GetProtoRequestContext(msg)
			ctx = requestcontext.ContextWithProtoRequestContext(ctx, protoCtx)
		}
		return handler(ctx, req)
	}
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

// copyHeadersStreamInterceptor is a server interceptor that copies certain
// headers present in the grpc metadata into the context.
func copyHeadersStreamServerInterceptor() grpc.StreamServerInterceptor {
	return ContextReplacingStreamServerInterceptor(copyHeadersToContext)
}

// copyHeadersUnaryInterceptor is a server interceptor that copies certain
// headers present in the grpc metadata into the context.
func copyHeadersUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return ContextReplacingUnaryServerInterceptor(copyHeadersToContext)
}

// setHeadersUnaryClientInterceptor is a client interceptor that copies certain
// headers present in the context into the outgoing grpc metadata.
func setHeadersUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return contextReplacingUnaryClientInterceptor(setHeadersFromContext)
}

// setHeadersStreamClientInterceptor is a client interceptor that copies certain
// headers present in the context into the outgoing grpc metadata.
func setHeadersStreamClientInterceptor() grpc.StreamClientInterceptor {
	return contextReplacingStreamClientInterceptor(setHeadersFromContext)
}

func propagateInvocationIDToSpan(ctx context.Context) {
	invocationId := bazel_request.GetInvocationID(ctx)
	if invocationId == "" {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("invocation_id", invocationId))
}

func setTracingDecision(ctx context.Context, tracingDecision bool) context.Context {
	return context.WithValue(ctx, tracing.TracingDecisionHeader, tracingDecision)
}

func tracingUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		shouldTrace := tracing.ShouldTraceIncoming(ctx, info.FullMethod)
		ctx = setTracingDecision(ctx, shouldTrace)
		if shouldTrace {
			propagateInvocationIDToSpan(ctx)
			otelInterceptFn := otelgrpc.UnaryServerInterceptor()
			return otelInterceptFn(ctx, req, info, handler)
		}
		return handler(ctx, req)
	}
}

func tracingStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		shouldTrace := tracing.ShouldTraceIncoming(stream.Context(), info.FullMethod)
		ctx := setTracingDecision(stream.Context(), shouldTrace)
		stream = &wrappedServerStreamWithContext{stream, ctx}
		if shouldTrace {
			propagateInvocationIDToSpan(ctx)
			otelInterceptFn := otelgrpc.StreamServerInterceptor()
			return otelInterceptFn(srv, stream, info, handler)
		}
		return handler(srv, stream)
	}
}

func tracingUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		shouldTrace := tracing.ShouldTraceOutgoing(ctx, method)
		ctx = setTracingDecision(ctx, shouldTrace)
		if shouldTrace {
			propagateInvocationIDToSpan(ctx)
			otelInterceptFn := otelgrpc.UnaryClientInterceptor()
			return otelInterceptFn(ctx, method, req, reply, cc, invoker, opts...)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func tracingStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		shouldTrace := tracing.ShouldTraceOutgoing(ctx, method)
		ctx = setTracingDecision(ctx, shouldTrace)
		if shouldTrace {
			propagateInvocationIDToSpan(ctx)
			otelInterceptFn := otelgrpc.StreamClientInterceptor()
			return otelInterceptFn(ctx, desc, cc, method, streamer, opts...)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func GetUnaryInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		requestIDUnaryServerInterceptor(),
		tracingUnaryServerInterceptor(),
		logRequestUnaryServerInterceptor(),
		requestContextProtoUnaryServerInterceptor(),
		authUnaryServerInterceptor(env),
		roleAuthUnaryServerInterceptor(env),
		copyHeadersUnaryServerInterceptor(),
	)
}

func GetStreamInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		requestIDStreamServerInterceptor(),
		tracingStreamServerInterceptor(),
		logRequestStreamServerInterceptor(),
		authStreamServerInterceptor(env),
		roleAuthStreamServerInterceptor(env),
		copyHeadersStreamServerInterceptor(),
	)
}

func GetUnaryClientInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		tracingUnaryClientInterceptor(),
		grpc_prometheus.UnaryClientInterceptor,
		setHeadersUnaryClientInterceptor(),
	)
}

func GetStreamClientInterceptor() grpc.DialOption {
	return grpc.WithChainStreamInterceptor(
		tracingStreamClientInterceptor(),
		grpc_prometheus.StreamClientInterceptor,
		setHeadersStreamClientInterceptor(),
	)
}
