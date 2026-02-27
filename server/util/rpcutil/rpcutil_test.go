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
	ch          chan message[T]
	closeRecvCh chan message[T]
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

func (s *stream[T]) CloseAndRecv() (T, error) {
	if s.closeRecvCh != nil {
		msg := <-s.closeRecvCh
		return msg.Val, msg.Err
	}
	var zero T
	return zero, nil
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

func TestCloseAndRecv(t *testing.T) {
	ctx := context.Background()
	cause := fmt.Errorf("test-cause")
	val := tspb.Now()

	// Should return response successfully
	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(ctx, s)
	closeRecvCh <- message[*tspb.Timestamp]{Val: val}
	msg, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
	require.NoError(t, err)
	require.Equal(t, val, msg)

	// Should return cause when timed out
	closeRecvChTimeout := make(chan message[*tspb.Timestamp])
	s = &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvChTimeout}
	sender = rpcutil.NewSender(ctx, s)
	msg, err = sender.CloseAndRecvWithTimeoutCause(0, cause)
	require.Nil(t, msg)
	require.Equal(t, cause, err)

	// Unblock CloseAndRecv goroutine to avoid leaking it in the timeout case.
	close(closeRecvChTimeout)
}
