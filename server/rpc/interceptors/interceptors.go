package interceptors

import (
	"context"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/capabilities_filter"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
)

const (
	buildBuddyServicePrefix = "/buildbuddy.service.BuildBuddyService/"
	rpcQuotaPrefix          = "rpc:"
)

var (
	headerContextKeys map[string]string
)

func init() {
	headerContextKeys = map[string]string{
		authutil.ContextTokenStringKey:   authutil.ContextTokenStringKey,
		usageutil.OriginHeaderName:       usageutil.OriginHeaderName,
		usageutil.ClientHeaderName:       usageutil.ClientHeaderName,
		bazel_request.RequestMetadataKey: bazel_request.RequestMetadataKey,
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
func contextReplacingStreamServerInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedServerStreamWithContext{stream, ctxFn(stream.Context())})
	}
}

// ContextReplacingStreamServerInterceptor is a helper to make a unary interceptor that modifies a context.
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

func AddAuthToContext(env environment.Env, ctx context.Context) context.Context {
	// Don't save the trace context so that this isn't a parent of the calls
	// that use the returned context.
	_, span := tracing.StartSpan(ctx)
	defer span.End()
	ctx = env.GetAuthenticator().AuthenticatedGRPCContext(ctx)
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		ctx = log.EnrichContext(ctx, "group_id", c.GetGroupID())
		if c.GetUserID() != "" {
			ctx = log.EnrichContext(ctx, "user_id", c.GetGroupID())
		}
	}
	return ctx
}

func addRequestIdToContext(ctx context.Context) context.Context {
	if rctx, err := uuid.SetInContext(ctx); err == nil {
		return rctx
	}
	return ctx
}

func addClientIPToContext(ctx context.Context) context.Context {
	hdrs := metadata.ValueFromIncomingContext(ctx, "X-Forwarded-For")
	if len(hdrs) == 0 {
		return ctx
	}

	ctx, ok := clientip.SetFromXForwardedForHeader(ctx, hdrs[0])
	if ok {
		return ctx
	}

	if p, ok := peer.FromContext(ctx); ok {
		ap, err := netip.ParseAddrPort(p.Addr.String())
		if err != nil {
			alert.UnexpectedEvent("invalid peer %q", p.Addr.String(), err.Error())
			return ctx
		}
		return context.WithValue(ctx, clientip.ContextKey, ap.Addr().String())
	}

	return ctx
}

func addSubdomainToContext(ctx context.Context) context.Context {
	hdrs := metadata.ValueFromIncomingContext(ctx, ":authority")
	if len(hdrs) == 0 {
		return ctx
	}
	return subdomain.SetHost(ctx, hdrs[0])
}

func copyHeadersToContext(ctx context.Context) context.Context {
	for headerName, contextKey := range headerContextKeys {
		if hdrs := metadata.ValueFromIncomingContext(ctx, headerName); len(hdrs) > 0 {
			ctx = context.WithValue(ctx, contextKey, hdrs[0])
		}
	}
	ctx = bazel_request.ParseRequestMetadataOnce(ctx)
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
		return AddAuthToContext(env, ctx)
	}
	return contextReplacingStreamServerInterceptor(ctxFn)
}

// authUnaryServerInterceptor is a server interceptor that applies the authenticator
// middleware to the request.
func authUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	ctxFn := func(ctx context.Context) context.Context {
		return AddAuthToContext(env, ctx)
	}
	return contextReplacingUnaryServerInterceptor(ctxFn)
}

func roleAuthStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasPrefix(info.FullMethod, buildBuddyServicePrefix) {
			methodName := strings.TrimPrefix(info.FullMethod, buildBuddyServicePrefix)
			if err := capabilities_filter.AuthorizeRPC(stream.Context(), env, methodName); err != nil {
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
			if err := capabilities_filter.AuthorizeRPC(ctx, env, methodName); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func ipAuthUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if irs := env.GetIPRulesService(); irs != nil {
			if err := irs.Authorize(ctx); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func ipAuthStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if irs := env.GetIPRulesService(); irs != nil {
			if err := irs.Authorize(stream.Context()); err != nil {
				return err
			}
		}
		return handler(srv, stream)
	}
}

func identityUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if cis := env.GetClientIdentityService(); cis != nil {
			newCtx, err := cis.ValidateIncomingIdentity(ctx)
			if err != nil {
				return nil, err
			}
			ctx = newCtx
		}
		return handler(ctx, req)
	}
}

func identityStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if cis := env.GetClientIdentityService(); cis != nil {
			newCtx, err := cis.ValidateIncomingIdentity(stream.Context())
			if err != nil {
				return err
			}
			stream = &wrappedServerStreamWithContext{stream, newCtx}
		}
		return handler(srv, stream)
	}
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

// clientIPStreamInterceptor is a server interceptor that inserts the client IP
// into the context.
func clientIPStreamServerInterceptor() grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(addClientIPToContext)
}

// subdomainStreamServerInterceptor adds customer subdomain information to the
// context.
func subdomainStreamServerInterceptor() grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(addSubdomainToContext)
}

// clientIPUnaryInterceptor is a server interceptor that inserts the client IP
// into the context.
func ClientIPUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(addClientIPToContext)
}

// subdomainUnaryServerInterceptor adds customer subdomain information to the
// context.
func subdomainUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(addSubdomainToContext)
}

func addInvocationIdToLog(ctx context.Context) context.Context {
	if iid := bazel_request.GetInvocationID(ctx); iid != "" {
		return log.EnrichContext(ctx, log.InvocationIDKey, iid)
	}
	return ctx
}

func invocationIDLoggerStreamServerInterceptor() grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(addInvocationIdToLog)
}

func invocationIDLoggerUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(addInvocationIdToLog)
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
		log.LogGRPCRequest(ctx, info.FullMethod, time.Since(start), err)
		return r, err
	}
}

// logRequestUnaryServerInterceptor defers a call to log the GRPC request.
func logRequestStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		log.LogGRPCRequest(stream.Context(), info.FullMethod, time.Since(start), err)
		return err
	}
}

func quotaUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if qm := env.GetQuotaManager(); qm != nil {
			err := qm.Allow(ctx, rpcQuotaPrefix+info.FullMethod, 1)
			if err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func quotaStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if qm := env.GetQuotaManager(); qm != nil {
			err := qm.Allow(stream.Context(), rpcQuotaPrefix+info.FullMethod, 1)
			if err != nil {
				return err
			}
		}
		return handler(srv, stream)
	}
}

func alertOnPanic(err any) {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	alert.UnexpectedEvent("recovered_panic", "%v\n%s", err, buf[:n])
}

func unaryRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (rsp interface{}, err error) {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				rsp = nil
				err = status.InternalError("A panic occurred")
				alertOnPanic(panicErr)
			}
		}()
		rsp, err = handler(ctx, req)
		return
	}
}

func streamRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				err = status.InternalError("A panic occurred")
				alertOnPanic(panicErr)
			}
		}()
		err = handler(srv, stream)
		return
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

func getClientIdentityAdder(env environment.Env) func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		if env.GetClientIdentityService() == nil {
			return ctx
		}
		ctx, err := env.GetClientIdentityService().AddIdentityToContext(ctx)
		if err != nil {
			alert.UnexpectedEvent("could_not_add_server_identity", err.Error())
		}
		return ctx
	}
}

func setClientIdentityUnaryClientInteceptor(env environment.Env) grpc.UnaryClientInterceptor {
	return contextReplacingUnaryClientInterceptor(getClientIdentityAdder(env))
}

func setClientIdentityStreamClientInterceptor(env environment.Env) grpc.StreamClientInterceptor {
	return contextReplacingStreamClientInterceptor(getClientIdentityAdder(env))
}

func propagateMetadataFromIncomingToOutgoing(keys ...string) func(context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		for _, key := range keys {
			if values := metadata.ValueFromIncomingContext(ctx, key); len(values) > 0 {
				ctx = metadata.AppendToOutgoingContext(ctx, key, values[len(values)-1])
			}
		}
		return ctx
	}
}

func PropagateMetadataUnaryInterceptor(keys ...string) grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(propagateMetadataFromIncomingToOutgoing(keys...))
}

func PropagateMetadataStreamInterceptor(keys ...string) grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(propagateMetadataFromIncomingToOutgoing(keys...))
}

func propagateBazelRequestMetadataIDsToSpan(ctx context.Context) {
	metadata := bazel_request.GetRequestMetadata(ctx)
	attributes := make([]attribute.KeyValue, 0, 2)
	if id := metadata.GetToolInvocationId(); id != "" {
		attributes = append(attributes, attribute.String("invocation_id", id))
	}
	if id := metadata.GetActionId(); id != "" {
		attributes = append(attributes, attribute.String("action_id", id))
	}
	if len(attributes) == 0 {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attributes...)
}

func propagateRequestMetadataIDsToSpanUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		propagateBazelRequestMetadataIDsToSpan(ctx)
		return handler(ctx, req)
	}
}

func propagateRequestMetadataIDsToSpanStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		propagateBazelRequestMetadataIDsToSpan(ctx)
		return handler(srv, stream)
	}
}

func GetUnaryInterceptor(env environment.Env, extraInterceptors ...grpc.UnaryServerInterceptor) grpc.ServerOption {
	interceptors := []grpc.UnaryServerInterceptor{
		unaryRecoveryInterceptor(),
		copyHeadersUnaryServerInterceptor(),
		propagateRequestMetadataIDsToSpanUnaryServerInterceptor(),
		ClientIPUnaryServerInterceptor(),
		subdomainUnaryServerInterceptor(),
		requestIDUnaryServerInterceptor(),
		invocationIDLoggerUnaryServerInterceptor(),
		logRequestUnaryServerInterceptor(),
		requestContextProtoUnaryServerInterceptor(),
	}
	// Install extra, caller-specified interceptors prior to auth interceptors
	// because the auth interceptors rely on extra interceptors for e.g.
	// propagating auth headers.
	if len(extraInterceptors) > 0 {
		interceptors = append(interceptors, extraInterceptors...)
	}
	interceptors = append(interceptors, authUnaryServerInterceptor(env),
		quotaUnaryServerInterceptor(env),
		identityUnaryServerInterceptor(env),
		ipAuthUnaryServerInterceptor(env),
		roleAuthUnaryServerInterceptor(env))
	return grpc.ChainUnaryInterceptor(interceptors...)
}

func GetStreamInterceptor(env environment.Env, extraInterceptors ...grpc.StreamServerInterceptor) grpc.ServerOption {
	interceptors := []grpc.StreamServerInterceptor{
		streamRecoveryInterceptor(),
		copyHeadersStreamServerInterceptor(),
		propagateRequestMetadataIDsToSpanStreamServerInterceptor(),
		clientIPStreamServerInterceptor(),
		subdomainStreamServerInterceptor(),
		requestIDStreamServerInterceptor(),
		invocationIDLoggerStreamServerInterceptor(),
		logRequestStreamServerInterceptor(),
	}
	// Install extra, caller-specified interceptors prior to auth interceptors
	// because the auth interceptors rely on extra interceptors for e.g.
	// propagating auth headers.
	if len(extraInterceptors) > 0 {
		interceptors = append(interceptors, extraInterceptors...)
	}
	interceptors = append(interceptors, authStreamServerInterceptor(env),
		quotaStreamServerInterceptor(env),
		identityStreamServerInterceptor(env),
		ipAuthStreamServerInterceptor(env),
		roleAuthStreamServerInterceptor(env))
	return grpc.ChainStreamInterceptor(interceptors...)
}

// Metrics returns middleware that can be used to obtain gRPC interceptors
// that add prometheus metrics for handled RPCs.
//
// e.g. interceptors.Metrics().UnaryClientInterceptor()
//
// N.B. OnceValue is used to ensure that prometheus.MustRegister is only called
// once.
var Metrics = sync.OnceValue(func() *grpc_prometheus.ClientMetrics {
	ms := grpc_prometheus.NewClientMetrics()
	prometheus.MustRegister(ms)
	return ms
})

func GetUnaryClientInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		setHeadersUnaryClientInterceptor(),
		Metrics().UnaryClientInterceptor(), // last so it's as close to the RPC as possible
	)
}

func GetUnaryClientIdentityInterceptor(env environment.Env) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(setClientIdentityUnaryClientInteceptor(env))
}

func GetStreamClientInterceptor() grpc.DialOption {
	return grpc.WithChainStreamInterceptor(
		setHeadersStreamClientInterceptor(),
		Metrics().StreamClientInterceptor(), // last so it's as close to the RPC as possible
	)
}

func GetStreamClientIdentityInterceptor(env environment.Env) grpc.DialOption {
	return grpc.WithChainStreamInterceptor(setClientIdentityStreamClientInterceptor(env))
}
