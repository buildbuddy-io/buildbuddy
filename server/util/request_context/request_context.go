package requestcontext

import (
	"context"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
)

const (
	contextProtoRequestContextKey = "proto.requestContext"
)

func ContextWithProtoRequestContext(ctx context.Context, reqCtx *ctxpb.RequestContext) context.Context {
	return context.WithValue(ctx, contextProtoRequestContextKey, reqCtx)
}

func ProtoRequestContextFromContext(ctx context.Context) *ctxpb.RequestContext {
	val := ctx.Value(contextProtoRequestContextKey)
	if val == nil {
		return nil
	}
	return val.(*ctxpb.RequestContext)
}