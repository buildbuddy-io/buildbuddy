package protolet

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
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

func readRequestToProto(r *http.Request, req proto.Message) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	switch ct := r.Header.Get("Content-Type"); ct {
	case "":
		fallthrough
	case "application/proto", "application/protobuf":
		return proto.Unmarshal(body, req)
	case "application/protobuf-text":
		return proto.UnmarshalText(string(body), req)
	default:
		return fmt.Errorf("Unknown Content-Type: %s, expected application/protobuf", ct)
	}
}

func writeProtoToResponse(rsp proto.Message, w http.ResponseWriter, r *http.Request) error {
	switch ct := r.Header.Get("Content-Type"); ct {
	case "":
		fallthrough
	case "application/proto", "application/protobuf":
		protoBytes, err := proto.Marshal(rsp)
		if err != nil {
			return err
		}
		w.Write(protoBytes)
		w.Header().Set("Content-Type", ct)
		return nil
	case "application/protobuf-text":
		return proto.MarshalText(w, rsp)
		w.Header().Set("Content-Type", ct)
		return nil
	default:
		return fmt.Errorf("Unknown Content-Type: %s, expected application/protobuf", ct)
	}
}

func GenerateHTTPHandlers(server interface{}) http.HandlerFunc {
	if reflect.ValueOf(server).Type().Kind() != reflect.Ptr {
		panic("GenerateHTTPHandlers must be called with a pointer to a RPC service implementation")
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
		if err := readRequestToProto(r, req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		args := []reflect.Value{reflect.ValueOf(server), reflect.ValueOf(r.Context()), reqVal}
		rspArr := method.Call(args)
		if rspArr[1].Interface() != nil {
			err, _ := rspArr[1].Interface().(error)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rspVal := rspArr[0]
		rsp := rspVal.Interface().(proto.Message)
		if err := writeProtoToResponse(rsp, w, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}
