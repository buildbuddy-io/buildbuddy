package grpc_stream

import (
	"context"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// gRPC has three types of streaming RPCs, as described here:
// https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc
// - Server streaming (client sends one message, receives many)
// - Client treaming (client sends many messages, receives one)
// - Bidirectional streaming (client and server send many message)
//
// This generic interface represents the server streaming RPC.
type ServerStream[T protoreflect.ProtoMessage] interface {
	Recv() (T, error)
	grpc.ClientStream
}

// A wrapper around a ServerStream that counts the number of bytes returned by
// the server over the lifetime of the stream.
type ByteCountingServerStream[T protoreflect.ProtoMessage] struct {
	bytes *atomic.Int64
	proxy ServerStream[T]
}

func NewByteCountingServerStream[T protoreflect.ProtoMessage](proxy ServerStream[T]) ByteCountingServerStream[T] {
	bytes := &atomic.Int64{}
	return ByteCountingServerStream[T]{
		bytes: bytes,
		proxy: proxy,
	}
}

// Returns the number of bytes received on the stream so far.
func (s *ByteCountingServerStream[T]) GetByteCount() int64 {
	return s.bytes.Load()
}

func (s *ByteCountingServerStream[T]) Recv() (T, error) {
	message, err := s.proxy.Recv()
	s.bytes.Add(int64(proto.Size(message)))
	return message, err
}

func (s *ByteCountingServerStream[T]) Header() (metadata.MD, error) {
	return s.proxy.Header()
}

func (s *ByteCountingServerStream[T]) Trailer() metadata.MD {
	return s.proxy.Trailer()
}

func (s *ByteCountingServerStream[T]) CloseSend() error {
	return s.proxy.CloseSend()
}

func (s *ByteCountingServerStream[T]) Context() context.Context {
	return s.proxy.Context()
}

func (s *ByteCountingServerStream[T]) SendMsg(message any) error {
	return s.proxy.SendMsg(message)
}

func (s *ByteCountingServerStream[T]) RecvMsg(message any) error {
	return s.proxy.RecvMsg(message)
}

// This generic interface represents the client streaming RPC.
type ClientStream[S, T protoreflect.ProtoMessage] interface {
	Send(S) error
	CloseAndRecv() (T, error)
	grpc.ClientStream
}

// A wrapper around a ClientStream that counts the number of bytes sent by the
// client over the lifetime of the stream.
type ByteCountingClientStream[S, T protoreflect.ProtoMessage] struct {
	bytes *atomic.Int64
	proxy ClientStream[S, T]
}

func NewByteCountingClientStream[S, T protoreflect.ProtoMessage](proxy ClientStream[S, T]) ByteCountingClientStream[S, T] {
	bytes := &atomic.Int64{}
	return ByteCountingClientStream[S, T]{
		bytes: bytes,
		proxy: proxy,
	}
}

// Returns the number of bytes sent to the stream so far.
func (s *ByteCountingClientStream[S, T]) GetByteCount() int64 {
	return s.bytes.Load()
}

func (s *ByteCountingClientStream[S, T]) Send(message S) error {
	s.bytes.Add(int64(proto.Size(message)))
	return s.proxy.Send(message)
}

func (s *ByteCountingClientStream[S, T]) CloseAndRecv() (T, error) {
	return s.proxy.CloseAndRecv()
}

func (s *ByteCountingClientStream[S, T]) Header() (metadata.MD, error) {
	return s.proxy.Header()
}

func (s *ByteCountingClientStream[S, T]) Trailer() metadata.MD {
	return s.proxy.Trailer()
}

func (s *ByteCountingClientStream[S, T]) CloseSend() error {
	return s.proxy.CloseSend()
}

func (s *ByteCountingClientStream[S, T]) Context() context.Context {
	return s.proxy.Context()
}

func (s *ByteCountingClientStream[S, T]) SendMsg(message any) error {
	return s.proxy.SendMsg(message)
}

func (s *ByteCountingClientStream[S, T]) RecvMsg(message any) error {
	return s.proxy.RecvMsg(message)
}
