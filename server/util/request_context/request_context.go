package requestcontext

import (
	"context"
	"reflect"

	"github.com/golang/protobuf/proto"

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
	protoType := reflect.TypeOf(req)
	for i := 0; i < protoType.NumMethod(); i++ {
		method := protoType.Method(i)
		if !isGetRequestContextMethod(method) {
			continue
		}
		args := []reflect.Value{reflect.ValueOf(req)}
		ctxArr := method.Func.Call(args)
		return ctxArr[0].Interface().(*ctxpb.RequestContext)
	}
	return nil
}

func isGetRequestContextMethod(m reflect.Method) bool {
	t := m.Type
	if t.Kind() != reflect.Func {
		return false
	}
	if m.Name != "GetRequestContext" {
		return false
	}
	if t.NumIn() != 1 || t.NumOut() != 1 {
		return false
	}
	// TODO: Figure out why this doesn't work
	// if !t.Out(0).Implements(reflect.TypeOf((*ctxpb.RequestContext)(nil)).Elem()) {
	// 	return false
	// }
	return true
}
