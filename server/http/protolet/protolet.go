package protolet

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const (
	contextProtoMessageKey = "protolet.requestMessage"
)

func isRPCMethod(m reflect.Method) bool {
	t := m.Type
	if t.Kind() != reflect.Func {
		return false
	}
	if t.NumIn() != 3 || t.NumOut() != 2 {
		return false
	}
	// Check signature is about right: (rcvr??, context, proto) (proto, error)
	if !t.In(1).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return false
	}
	if !t.In(2).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
		return false
	}
	if !t.Out(0).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
		return false
	}
	if !t.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return false
	}
	return true
}

func ReadRequestToProto(r *http.Request, req proto.Message) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	switch ct := r.Header.Get("Content-Type"); ct {
	case "", "application/json":
		return protojson.Unmarshal(body, req)
	case "application/proto", "application/protobuf":
		return proto.Unmarshal(body, req)
	case "application/protobuf-text":
		return prototext.Unmarshal(body, req)
	default:
		return fmt.Errorf("Unknown Content-Type: %s, expected application/json or application/protobuf", ct)
	}
}

func WriteProtoToResponse(rsp proto.Message, w http.ResponseWriter, r *http.Request) error {
	switch ct := r.Header.Get("Content-Type"); ct {
	case "", "application/json":
		jsonBytes, err := protojson.Marshal(rsp)
		if err != nil {
			return err
		}
		w.Write(jsonBytes)
		w.Header().Set("Content-Type", "application/json")
		return nil
	case "application/proto", "application/protobuf":
		protoBytes, err := proto.Marshal(rsp)
		if err != nil {
			return err
		}
		w.Write(protoBytes)
		w.Header().Set("Content-Type", ct)
		return nil
	case "application/protobuf-text":
		protoText, err := prototext.Marshal(rsp)
		if err != nil {
			return err
		}
		w.Write(protoText)
		w.Header().Set("Content-Type", ct)
		return nil
	default:
		return fmt.Errorf("Unknown Content-Type: %s, expected application/json or application/protobuf", ct)
	}
}

// TODO(tylerw): restructure protolet as a self-RPC to avoid the need for this
// body parsing middleware.

type HTTPHandlers struct {
	// Middleware that deserializes the request body and adds it to the request context.
	BodyParserMiddleware func(http.Handler) http.Handler
	// Handler that runs after the parsed request message is authenticated, returning the response proto.
	RequestHandler http.Handler
}

func GenerateHTTPHandlers(server interface{}) (*HTTPHandlers, error) {
	if reflect.ValueOf(server).Type().Kind() != reflect.Ptr {
		return nil, fmt.Errorf("GenerateHTTPHandlers must be called with a pointer to an RPC service implementation")
	}
	handlerFns := make(map[string]reflect.Value)

	serverType := reflect.TypeOf(server)
	for i := 0; i < serverType.NumMethod(); i++ {
		method := serverType.Method(i)
		if !isRPCMethod(method) {
			continue
		}
		handlerFns[method.Name] = method.Func
	}

	bodyParserMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method, ok := handlerFns[r.URL.Path]
			if !ok {
				http.Error(w, fmt.Sprintf("Method '%s' not found.", r.URL.Path), http.StatusNotFound)
				return
			}

			methodType := method.Type()
			reqVal := reflect.New(methodType.In(2).Elem())
			req := reqVal.Interface().(proto.Message)
			if err := ReadRequestToProto(r, req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ctx := context.WithValue(r.Context(), contextProtoMessageKey, req)
			reqCtx := requestcontext.GetProtoRequestContext(req)
			ctx = requestcontext.ContextWithProtoRequestContext(ctx, reqCtx)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, ok := handlerFns[r.URL.Path]
		if !ok {
			http.Error(w, fmt.Sprintf("Method '%s' not found.", r.URL.Path), http.StatusNotFound)
			return
		}

		// If we know this is a protolet request and we expect to handle it,
		// override the span name to something legible instead of the generic
		// handled-path name. This means instead of the span appearing with a
		// name like "POST /rpc/BuildBuddyService/", it will instead appear
		// with the name: "POST /rpc/BuildBuddyService/GetUser".
		ctx := r.Context()
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetName(fmt.Sprintf("%s %s", r.Method, r.RequestURI))
		}

		reqVal := reflect.ValueOf(ctx.Value(contextProtoMessageKey).(proto.Message))
		args := []reflect.Value{reflect.ValueOf(server), reflect.ValueOf(ctx), reqVal}
		rspArr := method.Call(args)
		if rspArr[1].Interface() != nil {
			err, _ := rspArr[1].Interface().(error)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rspVal := rspArr[0]
		rsp := rspVal.Interface().(proto.Message)
		if err := WriteProtoToResponse(rsp, w, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	return &HTTPHandlers{
		BodyParserMiddleware: bodyParserMiddleware,
		RequestHandler:       requestHandler,
	}, nil
}
