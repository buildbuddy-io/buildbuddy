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
