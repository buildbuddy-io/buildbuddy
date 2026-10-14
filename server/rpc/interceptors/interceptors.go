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
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
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
	"google.golang.org/grpc/stats"

	rtrace "runtime/trace"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
)

const (
	rpcQuotaPrefix = "rpc:"
)

var (
	buildBuddyServicePrefix = "/" + bbspb.BuildBuddyService_ServiceDesc.ServiceName + "/"
	headerContextKeys       map[string]string
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
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedServerStreamWithContext{stream, ctxFn(stream.Context())})
	}
}

// ContextReplacingStreamServerInterceptor is a helper to make a unary interceptor that modifies a context.
func contextReplacingUnaryServerInterceptor(ctxFn func(ctx context.Context) context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(ctxFn(ctx), method, req, reply, cc, opts...)
	}
}

func AddAuthToContext(env environment.Env, ctx context.Context) context.Context {
	// Don't save the trace context so that this isn't a parent of the calls
	// that use the returned context.
	_, span := tracing.StartNamedSpan(ctx, "interceptors.AddAuthToContext")
	defer span.End()
	ctx = env.GetAuthenticator().AuthenticatedGRPCContext(ctx)
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		span.SetAttributes(attribute.String("group_id", c.GetGroupID()))
		ctx = log.EnrichContext(ctx, "group_id", c.GetGroupID())
		if c.GetUserID() != "" {
			span.SetAttributes(attribute.String("user_id", c.GetUserID()))
			ctx = log.EnrichContext(ctx, "user_id", c.GetUserID())
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

func clientIsGRPCProxy(env environment.Env, ctx context.Context) bool {
	cis := env.GetClientIdentityService()
	if cis == nil {
		return false
	}
	si, err := cis.IdentityFromContext(ctx)
	if err != nil || si == nil {
		return false
	}
	return si.Client == interfaces.ClientIdentityGRPCProxy
}

func addClientIPToContext(ctx context.Context, env environment.Env) context.Context {
	// Use the gRPC proxy-supplied client IP if it is provided and the caller
	// is trusted, as verified by its clientidentity.
	if clientIsGRPCProxy(env, ctx) {
		// Require exactly one value: a trusted proxy sets a single header.
		if hdrs := metadata.ValueFromIncomingContext(ctx, clientip.HeaderName); len(hdrs) == 1 {
			return context.WithValue(ctx, clientip.ContextKey, hdrs[0])
		} else if len(hdrs) > 1 {
			log.CtxWarningf(ctx, "Multiple %q headers present in request from trusted proxy; ignoring", clientip.HeaderName)
		}
	}

	hdrs := metadata.ValueFromIncomingContext(ctx, "X-Forwarded-For")
	if len(hdrs) == 0 {
		// No proxy header; use direct connection peer address.
		return addPeerIPToContext(ctx)
	}

	ctx, ok := clientip.SetFromXForwardedForHeader(ctx, hdrs[0], peerIPFromContext(ctx))
	if ok {
		return ctx
	}

	// X-Forwarded-For header is present but not trusted. Fall back to peer.
	return addPeerIPToContext(ctx)
}

func peerIPFromContext(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr.Network() == "unix" {
		return ""
	}
	ap, err := netip.ParseAddrPort(p.Addr.String())
	if err != nil {
		return ""
	}
	return ap.Addr().String()
}

func addPeerIPToContext(ctx context.Context) context.Context {
	if p, ok := peer.FromContext(ctx); ok {
		if p.Addr.Network() == "unix" {
			return ctx
		}
		ap, err := netip.ParseAddrPort(p.Addr.String())
		if err != nil {
			alert.UnexpectedEvent("add_peer_ip_invalid_peer", p.Addr.String(), err.Error())
			return ctx
		}
		return context.WithValue(ctx, clientip.ContextKey, ap.Addr().String())
	}
	return ctx
}

func addSubdomainToContext(ctx context.Context, env environment.Env) context.Context {
	// Use internal-header-provided subdomains from trusted callers.
	if clientIsGRPCProxy(env, ctx) {
		if hdrs := metadata.ValueFromIncomingContext(ctx, subdomain.HeaderName); len(hdrs) == 1 {
			return subdomain.SetResolved(ctx, hdrs[0])
		} else if len(hdrs) > 1 {
			log.CtxWarningf(ctx, "Multiple %q headers present in request from trusted proxy; ignoring", subdomain.HeaderName)
		}
	}

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
	if m, ok := originMethodFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, originMethodHeader, m)
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
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasPrefix(info.FullMethod, buildBuddyServicePrefix) {
			if err := capabilities_filter.AuthorizeRPC(stream.Context(), env, info.FullMethod); err != nil {
				return err
			}
		}
		return handler(srv, stream)
	}
}

func roleAuthUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, buildBuddyServicePrefix) {
			if err := capabilities_filter.AuthorizeRPC(ctx, env, info.FullMethod); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func ipAuthUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if irs := env.GetIPRulesEnforcer(); irs != nil {
			newCtx, err := irs.Authorize(ctx)
			if err != nil {
				return nil, err
			}
			ctx = newCtx
		}
		return handler(ctx, req)
	}
}

func ipAuthStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if irs := env.GetIPRulesEnforcer(); irs != nil {
			newCtx, err := irs.Authorize(stream.Context())
			if err != nil {
				return err
			}
			stream = &wrappedServerStreamWithContext{stream, newCtx}
		}
		return handler(srv, stream)
	}
}

func identityUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
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

// trustedInternalClients are the client identities permitted to set
// authutil.InternalHeaderPrefix headers. A request from any other caller (most
// importantly, one with no validated client identity) has those headers
// stripped.
var trustedInternalClients = map[string]struct{}{
	interfaces.ClientIdentityApp:        {},
	interfaces.ClientIdentityExecutor:   {},
	interfaces.ClientIdentityCacheProxy: {},
	interfaces.ClientIdentityGRPCProxy:  {},
}

func isTrustedInternalClient(env environment.Env, ctx context.Context) bool {
	cis := env.GetClientIdentityService()
	if cis == nil {
		return false
	}
	si, err := cis.IdentityFromContext(ctx)
	if err != nil || si == nil {
		return false
	}
	_, ok := trustedInternalClients[si.Client]
	return ok
}

// stripInternalHeadersFromContext removes every incoming metadata header with
// the authutil.InternalHeaderPrefix prefix unless the request comes from a
// trusted internal client (verified via its client identity). Those headers
// carry internal trust signals (e.g. the resolved client IP) that only trusted
// peers may set, so stripping them from untrusted callers prevents spoofing.
//
// This must run after the identity interceptor (which validates the client
// identity) and before any interceptor that reads an internal header.
func stripInternalHeadersFromContext(env environment.Env, ctx context.Context) context.Context {
	if isTrustedInternalClient(env, ctx) {
		return ctx
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	// Fast path for the common case (no internal header present): scan keys
	// without allocating, and only build a filtered copy if there's a match.
	// Incoming metadata keys are already lowercased by gRPC.
	hasInternal := false
	for k := range md {
		if strings.HasPrefix(k, authutil.InternalHeaderPrefix) {
			hasInternal = true
			break
		}
	}
	if !hasInternal {
		return ctx
	}
	filtered := make(metadata.MD, len(md))
	for k, v := range md {
		if strings.HasPrefix(k, authutil.InternalHeaderPrefix) {
			continue
		}
		filtered[k] = v
	}
	return metadata.NewIncomingContext(ctx, filtered)
}

func stripInternalHeadersUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(func(ctx context.Context) context.Context {
		return stripInternalHeadersFromContext(env, ctx)
	})
}

func stripInternalHeadersStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(func(ctx context.Context) context.Context {
		return stripInternalHeadersFromContext(env, ctx)
	})
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
func clientIPStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(func(ctx context.Context) context.Context {
		return addClientIPToContext(ctx, env)
	})
}

// subdomainStreamServerInterceptor adds customer subdomain information to the
// context.
func subdomainStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return contextReplacingStreamServerInterceptor(func(ctx context.Context) context.Context {
		return addSubdomainToContext(ctx, env)
	})
}

// clientIPUnaryInterceptor is a server interceptor that inserts the client IP
// into the context.
func clientIPUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(func(ctx context.Context) context.Context {
		return addClientIPToContext(ctx, env)
	})
}

// subdomainUnaryServerInterceptor adds customer subdomain information to the
// context.
func subdomainUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return contextReplacingUnaryServerInterceptor(func(ctx context.Context) context.Context {
		return addSubdomainToContext(ctx, env)
	})
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
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		r, err := handler(ctx, req)
		log.LogGRPCRequest(ctx, info.FullMethod, time.Since(start), err)
		return r, err
	}
}

// logRequestUnaryServerInterceptor defers a call to log the GRPC request.
func logRequestStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		log.LogGRPCRequest(stream.Context(), info.FullMethod, time.Since(start), err)
		return err
	}
}

func groupStatusUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if gs := env.GetGroupStatusChecker(); gs != nil {
			if err := gs.CheckAllowed(ctx); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func groupStatusStreamServerInterceptor(env environment.Env) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if gs := env.GetGroupStatusChecker(); gs != nil {
			if err := gs.CheckAllowed(stream.Context()); err != nil {
				return err
			}
		}
		return handler(srv, stream)
	}
}

func quotaUnaryServerInterceptor(env environment.Env) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
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
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (rsp any, err error) {
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
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
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

func requestMetadataAttributes(ctx context.Context) []attribute.KeyValue {
	metadata := bazel_request.GetRequestMetadata(ctx)
	attributes := make([]attribute.KeyValue, 0, 8)
	if id := metadata.GetToolInvocationId(); id != "" {
		attributes = append(attributes, attribute.String("invocation_id", id))
	}
	if id := metadata.GetActionId(); id != "" {
		attributes = append(attributes, attribute.String("action_id", id))
	}
	if id := metadata.GetCorrelatedInvocationsId(); id != "" {
		attributes = append(attributes, attribute.String("correlated_invocations_id", id))
	}
	if mnemonic := metadata.GetActionMnemonic(); mnemonic != "" {
		attributes = append(attributes, attribute.String("action_mnemonic", mnemonic))
	}
	if id := metadata.GetTargetId(); id != "" {
		attributes = append(attributes, attribute.String("target_id", id))
	}
	if id := metadata.GetConfigurationId(); id != "" {
		attributes = append(attributes, attribute.String("configuration_id", id))
	}
	if toolName := metadata.GetToolDetails().GetToolName(); toolName != "" {
		attributes = append(attributes, attribute.String("tool_name", toolName))
	}
	if toolVersion := metadata.GetToolDetails().GetToolVersion(); toolVersion != "" {
		attributes = append(attributes, attribute.String("tool_version", toolVersion))
	}
	return attributes
}

func propagateBazelRequestMetadataToSpan(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attributes := requestMetadataAttributes(ctx)
	if len(attributes) == 0 {
		return
	}
	span.SetAttributes(attributes...)
}

func propagateRequestMetadataToSpanUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		propagateBazelRequestMetadataToSpan(ctx)
		return handler(ctx, req)
	}
}

func propagateRequestMetadataToSpanStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		propagateBazelRequestMetadataToSpan(ctx)
		return handler(srv, stream)
	}
}

// TracedUnaryServerInterceptor returns a unary server interceptor that adds a
// trace span around the nested interceptor (and all the interceptors it calls).
func TracedUnaryServerInterceptor(spanName string, interceptor grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (rsp any, err error) {
		ctx, span := tracing.StartNamedSpan(ctx, spanName)
		defer span.End()
		return interceptor(ctx, req, info, handler)
	}
}

type contextReplacingStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextReplacingStream) Context() context.Context { return s.ctx }

// TracedStreamServerInterceptor is like TracedUnaryServerInterceptor but for
// server stream interceptors.
func TracedStreamServerInterceptor(spanName string, interceptor grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, span := tracing.StartNamedSpan(ss.Context(), spanName)
		defer span.End()
		return interceptor(srv, &contextReplacingStream{ServerStream: ss, ctx: ctx}, info, handler)
	}
}

func GetUnaryInterceptor(env environment.Env, extraInterceptors ...grpc.UnaryServerInterceptor) grpc.ServerOption {
	interceptors := []grpc.UnaryServerInterceptor{
		unaryRecoveryInterceptor(),
		copyHeadersUnaryServerInterceptor(),
		propagateRequestMetadataToSpanUnaryServerInterceptor(),
		// Check client-identity early so it can be used in later interceptors.
		identityUnaryServerInterceptor(env),
		stripInternalHeadersUnaryServerInterceptor(env),
	}
	if pprofutil.Enabled() {
		// Must come after stripInternalHeaders, which sanitizes the header it reads.
		interceptors = append(interceptors, originMethodLabelUnaryServerInterceptor())
	}
	interceptors = append(interceptors,
		clientIPUnaryServerInterceptor(env),
		subdomainUnaryServerInterceptor(env),
		requestIDUnaryServerInterceptor(),
		invocationIDLoggerUnaryServerInterceptor(),
		logRequestUnaryServerInterceptor(),
		requestContextProtoUnaryServerInterceptor())
	// Install extra, caller-specified interceptors prior to auth interceptors
	// because the auth interceptors rely on extra interceptors for e.g.
	// propagating auth headers.
	if len(extraInterceptors) > 0 {
		interceptors = append(interceptors, extraInterceptors...)
	}
	interceptors = append(interceptors, authUnaryServerInterceptor(env))
	if pprofutil.Enabled() || pprofutil.TraceTasksEnabled() {
		interceptors = append(interceptors, groupLabelUnaryServerInterceptor())
	}
	interceptors = append(interceptors,
		groupStatusUnaryServerInterceptor(env),
		quotaUnaryServerInterceptor(env),
		ipAuthUnaryServerInterceptor(env),
		roleAuthUnaryServerInterceptor(env))
	return grpc.ChainUnaryInterceptor(interceptors...)
}

func GetStreamInterceptor(env environment.Env, extraInterceptors ...grpc.StreamServerInterceptor) grpc.ServerOption {
	interceptors := []grpc.StreamServerInterceptor{
		streamRecoveryInterceptor(),
		copyHeadersStreamServerInterceptor(),
		propagateRequestMetadataToSpanStreamServerInterceptor(),
		// Check client-identity early so it can be used in later interceptors.
		identityStreamServerInterceptor(env),
		stripInternalHeadersStreamServerInterceptor(env),
	}
	if pprofutil.Enabled() {
		// Must come after stripInternalHeaders, which sanitizes the header it reads.
		interceptors = append(interceptors, originMethodLabelStreamServerInterceptor())
	}
	interceptors = append(interceptors,
		clientIPStreamServerInterceptor(env),
		subdomainStreamServerInterceptor(env),
		requestIDStreamServerInterceptor(),
		invocationIDLoggerStreamServerInterceptor(),
		logRequestStreamServerInterceptor())
	// Install extra, caller-specified interceptors prior to auth interceptors
	// because the auth interceptors rely on extra interceptors for e.g.
	// propagating auth headers.
	if len(extraInterceptors) > 0 {
		interceptors = append(interceptors, extraInterceptors...)
	}
	interceptors = append(interceptors, authStreamServerInterceptor(env))
	if pprofutil.Enabled() || pprofutil.TraceTasksEnabled() {
		interceptors = append(interceptors, groupLabelStreamServerInterceptor())
	}
	interceptors = append(interceptors,
		groupStatusStreamServerInterceptor(env),
		quotaStreamServerInterceptor(env),
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

// originMethodHeader carries the external gRPC method that caused an internal
// app-to-app hop, so CPU spent on the peer is attributed to that method.
const originMethodHeader = authutil.InternalHeaderPrefix + "origin-method"

type originMethodKey struct{}

// originMethodFromContext returns the origin method stored by the
// originMethodLabel interceptors, if any.
func originMethodFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(originMethodKey{}).(string)
	return v, ok
}

// originMethod returns the method that originated this request: the incoming
// origin-method header, or fullMethod if there is none. It must run after
// stripInternalHeaders so that untrusted callers can't spoof the header.
func originMethod(ctx context.Context, fullMethod string) string {
	if vals := metadata.ValueFromIncomingContext(ctx, originMethodHeader); len(vals) > 0 && vals[0] != "" {
		return vals[0]
	}
	return fullMethod
}

func originMethodLabelUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		m := originMethod(ctx, info.FullMethod)
		ctx = context.WithValue(ctx, originMethodKey{}, m)
		var rsp any
		var err error
		pprofutil.Do(ctx, func(ctx context.Context) {
			rsp, err = handler(ctx, req)
		}, pprofutil.OriginMethod, m)
		return rsp, err
	}
}

func originMethodLabelStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		m := originMethod(stream.Context(), info.FullMethod)
		ctx := context.WithValue(stream.Context(), originMethodKey{}, m)
		var err error
		pprofutil.Do(ctx, func(ctx context.Context) {
			err = handler(srv, &wrappedServerStreamWithContext{stream, ctx})
		}, pprofutil.OriginMethod, m)
		return err
	}
}

type traceTaskKey struct{}

// requestTrace is the runtime/trace task and region covering one RPC.
type requestTrace struct {
	task   *rtrace.Task
	region *rtrace.Region
}

// ProfilingStatsHandler labels the goroutine serving each RPC with pprof
// labels (if enabled) and wraps the RPC in a runtime/trace task and region (if
// enabled).
// It is a stats handler rather than an interceptor because TagRPC runs
// on the request's goroutine before unary request decoding, and the End event
// runs after response encoding, so that work is labeled too.
type ProfilingStatsHandler struct{}

func (ProfilingStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if pprofutil.TraceTasksEnabled() {
		var task *rtrace.Task
		ctx, task = rtrace.NewTask(ctx, info.FullMethodName)
		// go tool trace breaks down running, waiting and blocked time only per
		// region, and attributes goroutines to the task only if they're started
		// inside a region. TagRPC and End run on the same goroutine, so the
		// region can span the whole RPC.
		region := rtrace.StartRegion(ctx, info.FullMethodName)
		ctx = context.WithValue(ctx, traceTaskKey{}, &requestTrace{task, region})
	}
	if !pprofutil.Enabled() {
		return ctx
	}
	// Parse once here; later readers of the request metadata reuse the result.
	ctx = bazel_request.ParseRequestMetadataOnce(ctx)
	kv := []string{
		pprofutil.GRPCMethod, info.FullMethodName,
		pprofutil.Client, usageutil.ClientLabel(ctx),
		pprofutil.Origin, usageutil.OriginLabel(ctx),
	}
	if rmd := bazel_request.GetRequestMetadata(ctx); rmd != nil {
		kv = append(kv, pprofutil.InvocationID, rmd.GetToolInvocationId(), pprofutil.ActionMnemonic, rmd.GetActionMnemonic())
	}
	ctx = pprofutil.SetGoroutineLabels(ctx, kv...)
	return ctx
}

func (ProfilingStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if _, ok := s.(*stats.End); !ok {
		return
	}
	if rt, ok := ctx.Value(traceTaskKey{}).(*requestTrace); ok {
		rt.region.End()
		rt.task.End()
	}
	// Don't leave this request's labels on a pooled worker goroutine.
	pprofutil.RestoreGoroutineLabels(context.Background())
}

func (ProfilingStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (ProfilingStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func groupLabelUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var rsp any
		var err error
		groupID := groupIDFromContext(ctx)
		logGroupID(ctx, groupID)
		pprofutil.Do(ctx, func(ctx context.Context) {
			rsp, err = handler(ctx, req)
		}, pprofutil.GroupID, groupID)
		return rsp, err
	}
}

func groupLabelStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var err error
		groupID := groupIDFromContext(stream.Context())
		logGroupID(stream.Context(), groupID)
		pprofutil.Do(stream.Context(), func(ctx context.Context) {
			err = handler(srv, &wrappedServerStreamWithContext{stream, ctx})
		}, pprofutil.GroupID, groupID)
		return err
	}
}

func logGroupID(ctx context.Context, groupID string) {
	if groupID != "" && pprofutil.TraceTasksEnabled() {
		rtrace.Log(ctx, pprofutil.GroupID, groupID)
	}
}

func groupIDFromContext(ctx context.Context) string {
	c, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return ""
	}
	return c.GetGroupID()
}
