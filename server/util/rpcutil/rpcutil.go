package rpcutil

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
)

type StreamMsg[T proto.Message] struct {
	Data  T
	Error error
}

type RecvStream[T proto.Message] interface {
	Recv() (T, error)
}

type Receiver[T proto.Message] struct {
	ctx      context.Context
	recvChan chan StreamMsg[T]
}

// RecvWithTimeoutCause waits for a message on the underlying stream, waiting
// a maximum of timeout. If timeout is reached, the given cause is returned
// as the error.
func (r *Receiver[T]) RecvWithTimeoutCause(timeout time.Duration, cause error) (T, error) {
	ctx, cancel := context.WithTimeoutCause(r.ctx, timeout, cause)
	defer cancel()
	select {
	case msg := <-r.recvChan:
		return msg.Data, msg.Error
	case <-ctx.Done():
		return *new(T), context.Cause(ctx)
	}
}

// NewReceiver returns a stream handle that can be used to implement more
// advanced stream handling, such as per-receive timeouts.
//
// Example usage:
//
// receiver := rpcutil.NewReceiver[*bspb.ReadResponse](ctx, stream)
//
//	for {
//		rsp, err := receiver.RecvWithTimeoutCause(5 * time.Second, status.DeadlineExceededError("blah blah blah"))
//		// handle rsp and err
//	}
func NewReceiver[T proto.Message](ctx context.Context, stream RecvStream[T]) Receiver[T] {
	streamMsgs := make(chan StreamMsg[T])
	go func() {
		for {
			rsp, err := stream.Recv()
			select {
			case streamMsgs <- StreamMsg[T]{rsp, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return Receiver[T]{ctx, streamMsgs}
}

type SendStream[T proto.Message] interface {
	Send(T) error
}

type Sender[T proto.Message] struct {
	ctx      context.Context
	sendChan chan T
	errChan  chan error
}

// SendWithTimeoutCause attempts to send a message on the underlying stream,
// waiting a maximum of timeout. If timeout is reached, the given cause is
// returned as the error.
//
// Note that gRPC sends are asynchronous in the sense that the protocol does not
// acknowledge individual messages. A timeout wil only occur if the sender
// exhausts the flow-control window and the receiver does not increase it.
func (s *Sender[T]) SendWithTimeoutCause(msg T, timeout time.Duration, cause error) error {
	s.sendChan <- msg

	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()
	select {
	case err := <-s.errChan:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// NewSender returns a stream handle that can be used to implement more
// advanced stream handling, such as per-send timeouts.
//
// Example usage:
//
// sender := rpcutil.NewSender[*bspb.WriteRequest](ctx, stream)
//
//	for {
//		err := sender.SendWithTimeoutCause(5 * time.Second, status.DeadlineExceededError("blah blah blah"))
//		// handle err
//	}
func NewSender[T proto.Message](ctx context.Context, stream SendStream[T]) Sender[T] {
	sendChan := make(chan T, 1)
	errChan := make(chan error)
	go func() {
		for {
			select {
			case req := <-sendChan:
				err := stream.Send(req)
				errChan <- err
			case <-ctx.Done():
				return
			}
		}
	}()
	return Sender[T]{ctx, sendChan, errChan}
}
