package protolet

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
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
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	switch ct := r.Header.Get("Content-Type"); ct {
	case "", "application/json":
		return jsonpb.Unmarshal(bytes.NewReader(body), req)
	case "application/proto", "application/protobuf":
		return proto.Unmarshal(body, req)
	case "application/protobuf-text":
		return proto.UnmarshalText(string(body), req)
	default:
		return fmt.Errorf("Unknown Content-Type: %s, expected application/json or application/protobuf", ct)
	}
}

func WriteProtoToResponse(rsp proto.Message, w http.ResponseWriter, r *http.Request) error {
	switch ct := r.Header.Get("Content-Type"); ct {
	case "", "application/json":
		marshaler := jsonpb.Marshaler{}
		marshaler.Marshal(w, rsp)
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
		w.Header().Set("Content-Type", ct)
		return proto.MarshalText(w, rsp)
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
		log.Debugf("Auto-registered HTTP handler for %s", method.Name)
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

		reqVal := reflect.ValueOf(r.Context().Value(contextProtoMessageKey).(proto.Message))
		args := []reflect.Value{reflect.ValueOf(server), reflect.ValueOf(r.Context()), reqVal}
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
