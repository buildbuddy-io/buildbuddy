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

type fakeSendStream struct {
	sendFn         func(*tspb.Timestamp) error
	closeAndRecvFn func() (*tspb.Timestamp, error)
}

func (s *fakeSendStream) Send(msg *tspb.Timestamp) error {
	if s.sendFn == nil {
		return nil
	}
	return s.sendFn(msg)
}

func (s *fakeSendStream) CloseAndRecv() (*tspb.Timestamp, error) {
	if s.closeAndRecvFn == nil {
		return nil, nil
	}
	return s.closeAndRecvFn()
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
	require.Nil(t, msg)
	require.Equal(t, cause, err)
}

func TestSender(t *testing.T) {
	t.Run("SendWithTimeoutCause returns cause when timed out", func(t *testing.T) {
		ctx := t.Context()
		stream := &fakeSendStream{
			sendFn: func(msg *tspb.Timestamp) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		sender := rpcutil.NewSender(ctx, stream)
		cause := fmt.Errorf("test-cause")

		err := sender.SendWithTimeoutCause(tspb.Now(), 0, cause)
		require.Equal(t, cause, err)
	})

	t.Run("CloseAndRecvWithTimeoutCause returns response", func(t *testing.T) {
		ctx := t.Context()
		want := tspb.Now()
		stream := &fakeSendStream{
			closeAndRecvFn: func() (*tspb.Timestamp, error) {
				return want, nil
			},
		}
		sender := rpcutil.NewSender(ctx, stream)
		cause := fmt.Errorf("test-cause")

		got, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("CloseAndRecvWithTimeoutCause returns cause when timed out", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		closeAndRecvStarted := make(chan struct{})
		closeAndRecvDone := make(chan struct{})
		stream := &fakeSendStream{
			closeAndRecvFn: func() (*tspb.Timestamp, error) {
				close(closeAndRecvStarted)
				<-ctx.Done()
				close(closeAndRecvDone)
				return nil, ctx.Err()
			},
		}
		sender := rpcutil.NewSender(ctx, stream)
		cause := fmt.Errorf("test-cause")

		got, err := sender.CloseAndRecvWithTimeoutCause(0, cause)
		require.Nil(t, got)
		require.Equal(t, cause, err)

		select {
		case <-closeAndRecvStarted:
		case <-time.After(1 * time.Second):
			t.Fatal("CloseAndRecv was not invoked")
		}

		cancel()
		select {
		case <-closeAndRecvDone:
		case <-time.After(1 * time.Second):
			t.Fatal("CloseAndRecv did not exit after context cancellation")
		}
	})
}
