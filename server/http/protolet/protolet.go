package protolet

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
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

func isGetRequestContextMethod(m reflect.Method) bool {
	t := m.Type
	if t.Kind() != reflect.Func {
		return false
	}
	if t.Name != "GetRequestContext" {
		return false
	}
	if t.NumIn() != 0 || t.NumOut() != 1 {
		return false
	}
	if !t.Out(0).Implements(reflect.TypeOf((*ctxpb.RequestContext)(nil)).Elem()) {
		return false
	}
	return true
}

func getProtoRequestContext(req proto.Message) *ctxpb.RequestContext {
	protoType := reflect.TypeOf(req)
	for i := 0; i < protoType.NumMethod(); i++ {
		method := protoType.Method(i)
		if !isGetRequestContextMethod(method) {
			continue
		}
		args := []reflect.Value{reflect.ValueOf(req)}
		ctxArr := method.Call(args)
		return ctxArr[0].Interface().(*ctxpb.RequestContext)
	}
	return nil
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

func GenerateHTTPHandlers(server interface{}) (http.HandlerFunc, error) {
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
		log.Printf("Auto-registered HTTP handler for %s", method.Name)
	}

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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ctx := ContextWithProtoRequestContext(ctx, getProtoRequestContext(reqVal))
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
	}), nil
}
