package requestcontext

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

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

func GetProtoRequestContext(req proto.Message) *ctxpb.RequestContext {
	if req, ok := req.(interface {
		GetRequestContext() *ctxpb.RequestContext
	}); ok {
		return req.GetRequestContext()
	}
	return nil
}
