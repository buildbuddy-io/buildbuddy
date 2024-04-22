package rpcutil_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/stretchr/testify/require"

	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	hugeTimeout = 1_000_000 * time.Hour
)

type message[T any] struct {
	Val T
	Err error
}

type stream[T any] struct {
	ch chan message[T]
}

func (s *stream[T]) Recv() (T, error) {
	var zero T
	msg, ok := <-s.ch
	if !ok {
		return zero, io.EOF
	}
	return msg.Val, msg.Err
}

func (s *stream[T]) Send(msg T) error {
	s.ch <- message[T]{Val: msg}
	return nil
}

func TestReceiver(t *testing.T) {
	ctx := context.Background()
	ch := make(chan message[*tspb.Timestamp])
	stream := &stream[*tspb.Timestamp]{ch: ch}
	receiver := rpcutil.NewReceiver(ctx, stream)
	val := tspb.Now()
	cause := fmt.Errorf("test-cause")

	// Should recv message successfully
	go func() { ch <- message[*tspb.Timestamp]{Val: val} }()
	msg, err := receiver.RecvWithTimeoutCause(hugeTimeout, cause)
	require.Equal(t, val, msg)
	require.NoError(t, err)

	// Should return cause when timed out
	msg, err = receiver.RecvWithTimeoutCause(0, cause)
	require.Nil(t, nil, msg)
	require.Equal(t, cause, err)
}

func TestSender(t *testing.T) {
	ctx := context.Background()
	ch := make(chan message[*tspb.Timestamp])
	stream := &stream[*tspb.Timestamp]{ch: ch}
	sender := rpcutil.NewSender(ctx, stream)
	val := tspb.Now()
	cause := fmt.Errorf("test-cause")

	// Should return cause when timed out
	err := sender.SendWithTimeoutCause(val, 0, cause)
	require.Equal(t, cause, err)
}
