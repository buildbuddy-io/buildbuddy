package protolet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const (
	contextProtoMessageKey = "protolet.requestMessage"
)

var (
	ErrUnknownRPC = errors.New("unknown RPC method")
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

func GenerateStreamHTTPHandlers(server Server) (*HTTPHandlers, error) {
	bodyParserMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rpc := r.URL.Path
			req := server.NewRequestMessage(rpc)
			if req == nil {
				http.Error(w, fmt.Sprintf("Missing request message type for: %s", rpc), http.StatusInternalServerError)
				return
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to read request body: %s", err), http.StatusBadRequest)
				return
			}
			if err := proto.Unmarshal(b, req); err != nil {
				http.Error(w, fmt.Sprintf("Failed to unmarshal request message of type %T: %s", req, err), http.StatusBadRequest)
				return
			}
			ctx := context.WithValue(r.Context(), contextProtoMessageKey, req)
			reqCtx := requestcontext.GetProtoRequestContext(req)
			ctx = requestcontext.ContextWithProtoRequestContext(ctx, reqCtx)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpc := r.URL.Path
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

		req := ctx.Value(contextProtoMessageKey).(proto.Message)

		pw, err := NewStreamResponseWriter(w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := server.Handle(ctx, rpc, req, pw); err != nil {
			// TODO: Consider sending an error status if the header hasn't yet
			// been written, and also just writing an error string instead of a
			// varint-encoded error.
			w.WriteHeader(http.StatusOK)
			pw.WriteError(err)
			return
		}
		// Note: This will do nothing if the handler has already streamed at
		// least one value.
		w.WriteHeader(http.StatusOK)
	})

	return &HTTPHandlers{
		BodyParserMiddleware: bodyParserMiddleware,
		RequestHandler:       requestHandler,
	}, nil
}

// Server wraps a gRPC server so that it can be used to serve gRPC-over-HTTP
// requests which accept a single request and return a stream of responses.
// Other types of streaming RPCs are not supported.
//
// This approach is used instead of the reflection approach used for unary RPCs
// because the implementation of server-side streaming gRPC methods requires
// using a `stream` interface which cannot be easily instantiated. So we use
// a codegen approach to generate an instance of this stream.
type Server interface {
	NewRequestMessage(rpc string) proto.Message
	Handle(ctx context.Context, rpc string, req proto.Message, pw *StreamResponseWriter) error
}

type StreamResponseWriter struct {
	w http.ResponseWriter
}

func NewStreamResponseWriter(w http.ResponseWriter) (*StreamResponseWriter, error) {
	_, ok := w.(http.Flusher)
	if !ok {
		return nil, status.InternalErrorf("response writer (type %T) does not implement Flusher() interface", w)
	}
	return &StreamResponseWriter{w: w}, nil
}

func (w *StreamResponseWriter) WriteResponse(rsp proto.Message) error {
	// NOTE: Only binary format is supported for streaming RPCs.
	b, err := proto.Marshal(rsp)
	if err != nil {
		return err
	}
	lengthEncoding := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(lengthEncoding, int64(len(b)))
	if _, err := w.w.Write(lengthEncoding[:n]); err != nil {
		return err
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	// TODO: Don't flush on every single write.
	w.w.(http.Flusher).Flush()
	return nil
}

func (w *StreamResponseWriter) WriteError(err error) error {
	// To encode an error, first write the varint encoding of -1, then the error
	// message length, then the error message itself. Note, we can't use the
	// value 0 for errors because that is already used to encode an empty
	// message.
	negativeOneVarintEncoding := []byte{1}
	if _, err := w.w.Write(negativeOneVarintEncoding); err != nil {
		return err
	}
	b := []byte(err.Error())
	lengthEncoding := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(lengthEncoding, int64(len(b)))
	if _, err := w.w.Write(lengthEncoding[:n]); err != nil {
		return err
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	w.w.(http.Flusher).Flush()
	return nil
}
