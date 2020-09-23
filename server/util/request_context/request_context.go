package group

import (
	"context"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
)

const (
	protoRequestContextContextKey
)

func ContextWithProtoRequestContext(ctx *context.Context, reqCtx *ctxpb.RequestContext) {
	return ctx.WithValue(ctx, protoRequestContextContextKey, reqCtx)
}

func ProtoRequestContextFromContext(ctx *context.Context) reqCtx *ctxpb.RequestContext {
	return ctx.Value(protoRequestContextContextKey).(*ctxpb.RequestContext)
}