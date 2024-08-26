package protolet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"

	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

const (
	contextProtoMessageKey = "protolet.requestMessage"
	// GRPC over HTTP requires protobuf messages to be sent in a series of `Length-Prefixed-Message`s
	// Here's what a Length-Prefixed-Message looks like:
	// 		Length-Prefixed-Message → Compressed-Flag Message-Length Message
	// 		Compressed-Flag → 0 / 1 # encoded as 1 byte unsigned integer
	// 		Message-Length → {length of Message} # encoded as 4 byte unsigned integer (big endian)
	// 		Message → *{binary octet}
	// This means the actual proto we want to deserialize starts at byte 5 because there is 1
	// byte that tells us whether or not the message is compressed, and then 4 bytes that tell
	// us the length of the message.
	// For more info, see: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
	messageByteOffset = 5

	// The http content type that describes requests / responses sent with the `Length-Prefixed-Message` format above.
	prefixedProtoContentType = "application/proto+prefixed"
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

func isStreamingRPCMethod(m reflect.Method) bool {
	t := m.Type
	if t.Kind() != reflect.Func {
		return false
	}
	if t.NumIn() != 3 || t.NumOut() != 1 {
		return false
	}
	if !t.In(1).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
		return false
	}
	if !t.In(2).Implements(reflect.TypeOf((*grpc.ServerStream)(nil)).Elem()) {
		return false
	}
	if !t.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
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
	case prefixedProtoContentType:
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
		if messageByteOffset > len(body) {
			return fmt.Errorf("bad request: expected proto message prefix of length %d, body length is %d", messageByteOffset, len(body))
		}
		return proto.Unmarshal(body[messageByteOffset:], req)
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

func GenerateHTTPHandlers(servicePrefix, serviceName string, server interface{}, grpcServer *grpc.Server) (*HTTPHandlers, error) {
	if reflect.ValueOf(server).Type().Kind() != reflect.Ptr {
		return nil, fmt.Errorf("GenerateHTTPHandlers must be called with a pointer to an RPC service implementation")
	}
	handlerFns := make(map[string]reflect.Value)

	serverType := reflect.TypeOf(server)
	for i := 0; i < serverType.NumMethod(); i++ {
		method := serverType.Method(i)
		if !isRPCMethod(method) && !isStreamingRPCMethod(method) {
			continue
		}
		handlerFns[servicePrefix+method.Name] = method.Func
	}

	bodyParserMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method, ok := handlerFns[r.URL.Path]
			if !ok {
				http.Error(w, fmt.Sprintf("Method '%s' not found.", r.URL.Path), http.StatusNotFound)
				return
			}

			methodType := method.Type()
			requestIndex := 2
			// If we're dealing with a streaming method, the request proto is the first input
			if method.Type().In(1).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
				requestIndex = 1
			}

			reqVal := reflect.New(methodType.In(requestIndex).Elem())
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

		// If we're getting a proto+prefixed request over http, we rewrite the path to point at
		// the grpc server's http handler endpoints and make the request look like an http2 request.
		// We also wrap the ResponseWriter so we can return proper errors to the web front-end.
		if r.Header.Get("content-type") == prefixedProtoContentType {
			r.URL.Path = fmt.Sprintf("/%s/%s", serviceName, strings.TrimPrefix(r.URL.Path, servicePrefix))
			r.ProtoMajor = 2
			r.ProtoMinor = 0
			r.Header.Set("content-type", "application/grpc")
			wrapped := &wrappedResponse{w: w}
			grpcServer.ServeHTTP(wrapped, r)
			wrapped.sendStatus()
			return
		}

		if method.Type().NumOut() != 2 {
			http.Error(w, "Streaming not enabled.", http.StatusNotImplemented)
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

type wrappedResponse struct {
	w           http.ResponseWriter
	wroteHeader bool
	wroteBody   bool
}

func (w *wrappedResponse) Header() http.Header {
	w.w.Header().Set("content-type", prefixedProtoContentType)
	return w.w.Header()
}

func (w *wrappedResponse) Write(b []byte) (int, error) {
	w.wroteBody, w.wroteHeader = true, true
	return w.w.Write(b)
}

func (w *wrappedResponse) WriteHeader(code int) {
	w.wroteHeader = true
	w.w.WriteHeader(code)
}

func (w *wrappedResponse) Flush() {
	if !w.wroteHeader && !w.wroteBody {
		return
	}
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *wrappedResponse) sendStatus() {
	defer w.Flush()

	if w.Header().Get("grpc-status") == "" {
		// The status field winds up being empty if the server is shutting down.
		// Translate this to "Unavailable".
		w.Header().Set("grpc-status", fmt.Sprintf("%d", codes.Unavailable))
		w.Header().Set("grpc-message", "server is unavailable")
	}

	if !w.wroteHeader {
		// If we haven't sent the headers yet, then we can send the status as
		// plain old headers.
		w.WriteHeader(200)
		return
	}

	// If we already wrote the HTTP header then write the status as "trailers"
	// using the same encoding used by gRPC Web.
	h := http.Header{}
	h.Set("grpc-status", w.Header().Get("grpc-status"))
	h.Set("grpc-message", w.Header().Get("grpc-message"))
	var encodedHeaders bytes.Buffer
	h.Write(&encodedHeaders)

	const trailerFlags = 0x80
	prefix := messageHeader(trailerFlags, uint32(encodedHeaders.Len()))
	w.Write(prefix[:])
	w.Write(encodedHeaders.Bytes())
}

func messageHeader(flags byte, length uint32) [5]byte {
	var b [5]byte
	b[0] = flags
	binary.BigEndian.AppendUint32(b[1:1], length)
	return b
}
