package interceptors

import (
	"context"
	"runtime/pprof"
	"runtime/trace"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

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

// PprofLabelsStatsHandler labels the goroutine serving each RPC with pprof
// labels (if enabled) and wraps the RPC in a runtime/trace task (if enabled).
// It is a stats handler rather than an interceptor because TagRPC runs
// on the request's goroutine before unary request decoding, and the End event
// runs after response encoding, so that work is labeled too.
type PprofLabelsStatsHandler struct{}

func (PprofLabelsStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if pprofutil.TraceTasksEnabled() {
		var task *trace.Task
		ctx, task = trace.NewTask(ctx, info.FullMethodName)
		ctx = context.WithValue(ctx, traceTaskKey{}, task)
	}
	if !pprofutil.Enabled() {
		return ctx
	}
	// Parse once here; later readers of the request metadata reuse the result.
	ctx = bazel_request.ParseRequestMetadataOnce(ctx)
	kv := make([]string, 0, 10)
	add := func(k, v string) {
		if v != "" {
			kv = append(kv, k, v)
		}
	}
	add(pprofutil.GRPCMethod, info.FullMethodName)
	add(pprofutil.Client, usageutil.ClientLabel(ctx))
	add(pprofutil.Origin, usageutil.OriginLabel(ctx))
	if rmd := bazel_request.GetRequestMetadata(ctx); rmd != nil {
		add(pprofutil.InvocationID, rmd.GetToolInvocationId())
		add(pprofutil.ActionMnemonic, rmd.GetActionMnemonic())
	}
	ctx = pprof.WithLabels(ctx, pprof.Labels(kv...))
	pprof.SetGoroutineLabels(ctx)
	return ctx
}

func (PprofLabelsStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if _, ok := s.(*stats.End); !ok {
		return
	}
	if task, ok := ctx.Value(traceTaskKey{}).(*trace.Task); ok {
		task.End()
	}
	if pprofutil.Enabled() {
		// Don't leave this request's labels on a pooled worker goroutine.
		pprof.SetGoroutineLabels(context.Background())
	}
}

func (PprofLabelsStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (PprofLabelsStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

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
		trace.Log(ctx, pprofutil.GroupID, groupID)
	}
}

func groupIDFromContext(ctx context.Context) string {
	c, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return ""
	}
	return c.GetGroupID()
}
