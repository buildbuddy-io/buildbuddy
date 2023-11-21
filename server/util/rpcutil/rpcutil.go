package rpcutil

import (
	"context"

	"google.golang.org/protobuf/proto"
)

type StreamMsg[T proto.Message] struct {
	Data  T
	Error error
}

type RecvStream[T proto.Message] interface {
	Recv() (T, error)
}

// RecvChan starts a goroutine that receives messages from a stream and puts it
// on a channel.
//
// Example usage:
//
// msgs := rpcutil.RecvChan[*bspb.ReadResponse](ctx, stream)
//
//	for {
//	  select {
//	    case msg := <-msgs:
//	      // handle stream message/err
//	    case <-ctx.Done():
//	      return ctx.Err()
//	  }
//	}
//
// It's important to always include a ctx.Done() case to avoid leaks.
func RecvChan[T proto.Message](ctx context.Context, stream RecvStream[T]) chan StreamMsg[T] {
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
	return streamMsgs
}

type SendStream[T proto.Message] interface {
	Send(T) error
}

type Sender[T proto.Message] struct {
	sendChan chan T
	errChan  chan error
}

func (s *Sender[T]) Send(msg T) chan error {
	s.sendChan <- msg
	return s.errChan
}

func SendChan[T proto.Message](ctx context.Context, stream SendStream[T]) Sender[T] {
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
	return Sender[T]{sendChan, errChan}
}
