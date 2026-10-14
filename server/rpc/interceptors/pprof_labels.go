package interceptors

import (
	"context"
	"runtime/pprof"

	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// PprofLabelsStatsHandler labels the goroutine serving each RPC with pprof
// labels. It is a stats handler rather than an interceptor because TagRPC runs
// on the request's goroutine before unary request decoding, and the End event
// runs after response encoding, so that work is labeled too.
type PprofLabelsStatsHandler struct{}

func (PprofLabelsStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
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
	if _, ok := s.(*stats.End); ok {
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
		pprofutil.Do(ctx, func(ctx context.Context) {
			rsp, err = handler(ctx, req)
		}, pprofutil.GroupID, groupIDFromContext(ctx))
		return rsp, err
	}
}

func groupLabelStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var err error
		pprofutil.Do(stream.Context(), func(ctx context.Context) {
			err = handler(srv, &wrappedServerStreamWithContext{stream, ctx})
		}, pprofutil.GroupID, groupIDFromContext(stream.Context()))
		return err
	}
}

func groupIDFromContext(ctx context.Context) string {
	c, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return ""
	}
	return c.GetGroupID()
}
