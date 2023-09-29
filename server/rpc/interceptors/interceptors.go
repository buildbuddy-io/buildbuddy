package interceptors

import (
	"context"
	"flag"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/role_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/quota"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
)

const (
	buildBuddyServicePrefix = "/buildbuddy.service.BuildBuddyService/"
)

var (
	headerContextKeys map[string]string
	once              sync.Once

	enableGRPCMetricsByGroupID = flag.Bool("app.enable_grpc_metrics_by_group_id", false, "If enabled, grpc metrics by group ID will be recorded")
)

func init() {
	headerContextKeys = map[string]string{
		"x-buildbuddy-jwt":    "x-buildbuddy-jwt",
		"x-buildbuddy-origin": "x-buildbuddy-origin",
		"x-buildbuddy-client": "x-buildbuddy-client",
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

func addClientIPToContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	hdrs := md.Get("X-Forwarded-For")
	if len(hdrs) == 0 {
		return ctx
	}

	ctx, ok = clientip.SetFromXForwardedForHeader(ctx, hdrs[0])
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
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	hdrs := md.Get(":authority")
	if len(hdrs) == 0 {
		return ctx
	}
	return subdomain.SetHost(ctx, hdrs[0])
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
	return ContextReplacingStreamServerInterceptor(addRequestIdToContext)
}

// requestIDUnaryInterceptor is a server interceptor that inserts a request ID
// into the context if one is not already present.
func requestIDUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return ContextReplacingUnaryServerInterceptor(addRequestIdToContext)
}

// clientIPStreamInterceptor is a server interceptor that inserts the client IP
// into the context.
func clientIPStreamServerInterceptor() grpc.StreamServerInterceptor {
	return ContextReplacingStreamServerInterceptor(addClientIPToContext)
}

// subdomainStreamServerInterceptor adds customer subdomain information to the
// context.
func subdomainStreamServerInterceptor() grpc.StreamServerInterceptor {
	return ContextReplacingStreamServerInterceptor(addSubdomainToContext)
}

// clientIPUnaryInterceptor is a server interceptor that inserts the client IP
// into the context.
func ClientIPUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return ContextReplacingUnaryServerInterceptor(addClientIPToContext)
}

// subdomainUnaryServerInterceptor adds customer subdomain information to the
// context.
func subdomainUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return ContextReplacingUnaryServerInterceptor(addSubdomainToContext)
}

func addInvocationIdToLog(ctx context.Context) context.Context {
	if iid := bazel_request.GetInvocationID(ctx); iid != "" {
		return log.EnrichContext(ctx, log.InvocationIDKey, iid)
	}
	return ctx
}

func invocationIDLoggerStreamServerInterceptor() grpc.StreamServerInterceptor {
	return ContextReplacingStreamServerInterceptor(addInvocationIdToLog)
}

func invocationIDLoggerUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return ContextReplacingUnaryServerInterceptor(addInvocationIdToLog)
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
		allow := true
		var err error
		if qm := env.GetQuotaManager(); qm != nil {
			allow, err = qm.Allow(ctx, info.FullMethod, 1)
			if err != nil {
				log.Warningf("Quota Manager failed: %s", err)
			}
		}
		if *enableGRPCMetricsByGroupID {
			if key, err := quota.GetKey(ctx, env); err == nil {
				metrics.RPCsHandledTotalByQuotaKey.WithLabelValues(info.FullMethod, key, strconv.FormatBool(allow)).Inc()
			}
		}
		r, err := handler(ctx, req)
		return r, err
	}
}

func quotaStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		allow := true
		var err error
		if qm := env.GetQuotaManager(); qm != nil {
			allow, err = qm.Allow(stream.Context(), info.FullMethod, 1)
			if err != nil {
				log.Warningf("Quota Manager failed: %s", err)
			}
		}
		if *enableGRPCMetricsByGroupID {
			if key, err := quota.GetKey(stream.Context(), env); err == nil {
				metrics.RPCsHandledTotalByQuotaKey.WithLabelValues(info.FullMethod, key, strconv.FormatBool(allow)).Inc()
			}
		}
		err = handler(srv, stream)
		return err
	}
}

func alertOnPanic() {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	alert.UnexpectedEvent("recovered_panic", buf[:n])
}

func unaryRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (rsp interface{}, err error) {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				rsp = nil
				err = status.InternalError("A panic occurred")
				alertOnPanic()
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
				alertOnPanic()
			}
		}()
		err = handler(srv, stream)
		return
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

func getClientIdentityAdder(env environment.Env) func(ctx context.Context) context.Context {
	if env == nil || env.GetClientIdentityService() == nil {
		return func(ctx context.Context) context.Context {
			return ctx
		}
	}
	return func(ctx context.Context) context.Context {
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

func GetUnaryInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		unaryRecoveryInterceptor(),
		copyHeadersUnaryServerInterceptor(),
		ClientIPUnaryServerInterceptor(),
		subdomainUnaryServerInterceptor(),
		requestIDUnaryServerInterceptor(),
		invocationIDLoggerUnaryServerInterceptor(),
		logRequestUnaryServerInterceptor(),
		requestContextProtoUnaryServerInterceptor(),
		authUnaryServerInterceptor(env),
		quotaUnaryServerInterceptor(env),
		identityUnaryServerInterceptor(env),
		ipAuthUnaryServerInterceptor(env),
		roleAuthUnaryServerInterceptor(env),
	)
}

func GetStreamInterceptor(env environment.Env) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		streamRecoveryInterceptor(),
		copyHeadersStreamServerInterceptor(),
		clientIPStreamServerInterceptor(),
		subdomainStreamServerInterceptor(),
		requestIDStreamServerInterceptor(),
		invocationIDLoggerStreamServerInterceptor(),
		logRequestStreamServerInterceptor(),
		authStreamServerInterceptor(env),
		quotaStreamServerInterceptor(env),
		identityStreamServerInterceptor(env),
		ipAuthStreamServerInterceptor(env),
		roleAuthStreamServerInterceptor(env),
	)
}

func GetUnaryClientInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		otelgrpc.UnaryClientInterceptor(),
		grpc_prometheus.UnaryClientInterceptor,
		setHeadersUnaryClientInterceptor(),
	)
}

func GetUnaryClientIdentityInterceptor(env environment.Env) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(setClientIdentityUnaryClientInteceptor(env))
}

func GetStreamClientInterceptor() grpc.DialOption {
	return grpc.WithChainStreamInterceptor(
		otelgrpc.StreamClientInterceptor(),
		grpc_prometheus.StreamClientInterceptor,
		setHeadersStreamClientInterceptor(),
	)
}

func GetStreamClientIdentityInterceptor(env environment.Env) grpc.DialOption {
	return grpc.WithChainStreamInterceptor(setClientIdentityStreamClientInterceptor(env))
}
