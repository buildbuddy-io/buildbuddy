package rpcutil_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	hugeTimeout = 1_000_000 * time.Hour
)

type message[T proto.Message] struct {
	Val T
	Err error
}

type stream[T proto.Message] struct {
	ch          chan message[T]
	closeRecvCh chan message[T]
}

type blockingSendStream[T proto.Message] struct {
	ctx          context.Context
	sendStarted  chan struct{}
	sendReturned chan struct{}
}

func (s *blockingSendStream[T]) Send(T) error {
	close(s.sendStarted)
	<-s.ctx.Done()
	close(s.sendReturned)
	return s.ctx.Err()
}

func (s *blockingSendStream[T]) CloseAndRecv() (T, error) {
	var zero T
	return zero, nil
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

// orderingStream lets a test control/observe the ordering of calls on a stream.
type orderingStream[T proto.Message] struct {
	sendBlock        chan struct{}
	sendReturned     chan struct{}
	closeRecvStarted chan struct{}
	closeRecvReturn  chan struct{}
}

func (s *orderingStream[T]) Send(T) error {
	<-s.sendBlock
	close(s.sendReturned)
	return nil
}

func (s *orderingStream[T]) CloseAndRecv() (T, error) {
	close(s.closeRecvStarted)
	<-s.closeRecvReturn
	var zero T
	return zero, nil
}

func TestReceiver(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
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

	// unblock the `stream.Recv` method
	ch <- message[*tspb.Timestamp]{Val: val}
}

func TestSender(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := t.Context()
	ch := make(chan message[*tspb.Timestamp])
	stream := &stream[*tspb.Timestamp]{ch: ch}
	sender := rpcutil.NewSender(ctx, stream)
	val := tspb.Now()
	cause := fmt.Errorf("test-cause")

	// Should return cause when timed out
	err := sender.SendWithTimeoutCause(val, 0, cause)
	require.Equal(t, cause, err)
	<-ch
	sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
}

func TestSender_AllowsMultipleSuccessfulSends(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := t.Context()
	ch := make(chan message[*tspb.Timestamp], 2)
	stream := &stream[*tspb.Timestamp]{ch: ch}
	sender := rpcutil.NewSender(ctx, stream)
	cause := fmt.Errorf("test-cause")
	val1 := tspb.Now()
	val2 := tspb.New(val1.AsTime().Add(time.Second))

	require.NoError(t, sender.SendWithTimeoutCause(val1, hugeTimeout, cause))
	require.NoError(t, sender.SendWithTimeoutCause(val2, hugeTimeout, cause))

	require.Equal(t, val1, (<-ch).Val)
	require.Equal(t, val2, (<-ch).Val)
	sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
}

func TestCloseAndRecv(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := t.Context()
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

func TestSender_CloseAndRecvDoesNotLeakSenderGoroutine(t *testing.T) {
	// Use a background context that is never cancelled, so the only way
	// the sender goroutine can exit is via sendChan being closed.
	defer goleak.VerifyNone(t)

	ch := make(chan message[*tspb.Timestamp], 1)
	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: ch, closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(t.Context(), s)

	require.NoError(t, sender.SendWithTimeoutCause(tspb.Now(), hugeTimeout, fmt.Errorf("cause")))
	<-ch
	closeRecvCh <- message[*tspb.Timestamp]{Val: tspb.Now()}
	_, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, fmt.Errorf("cause"))
	require.NoError(t, err)
}

func TestSender_CloseAndRecvWithoutSendsDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(t.Context(), s)

	closeRecvCh <- message[*tspb.Timestamp]{Val: tspb.Now()}
	_, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, fmt.Errorf("cause"))
	require.NoError(t, err)
}

func TestSender_SendTimeoutDoesNotLeakAfterCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(t.Context())
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(ctx, stream)

	err := sender.SendWithTimeoutCause(tspb.Now(), time.Millisecond, fmt.Errorf("test-cause"))
	require.Error(t, err)
	select {
	case <-stream.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sender goroutine to start Send")
	}

	cancel()
	select {
	case <-stream.sendReturned:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked send to return")
	}
}

// After a SendWithTimeoutCause times out, CloseAndRecvWithTimeoutCause must
// not call stream.CloseAndRecv until the background sender goroutine has
// returned from its in-flight stream.Send — gRPC client streams don't
// support concurrent Send and CloseSend/CloseAndRecv on the same stream.
func TestSender_CloseAndRecvWaitsForInFlightSend(t *testing.T) {
	defer goleak.VerifyNone(t)

	stream := &orderingStream[*tspb.Timestamp]{
		sendBlock:        make(chan struct{}),
		sendReturned:     make(chan struct{}),
		closeRecvStarted: make(chan struct{}),
		closeRecvReturn:  make(chan struct{}),
	}
	sender := rpcutil.NewSender(t.Context(), stream)

	// Make a Send that times out, blocking the sender goroutine in stream.Send.
	err := sender.SendWithTimeoutCause(tspb.Now(), time.Millisecond, fmt.Errorf("send-cause"))
	require.Error(t, err)

	// Start CloseAndRecv in a goroutine; it should wait for the in-flight
	// Send to return before calling stream.CloseAndRecv.
	closeDone := make(chan error, 1)
	go func() {
		_, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, fmt.Errorf("close-cause"))
		closeDone <- err
	}()

	// Ensure stream.CloseAndRecv isn't called yet, Send is still blocked.
	select {
	case <-stream.closeRecvStarted:
		t.Fatal("stream.CloseAndRecv was called before the in-flight Send returned")
	case <-time.After(50 * time.Millisecond):
	}

	// Unblock Send. The sender goroutine exits, done closes, and
	// CloseAndRecvWithTimeoutCause now proceeds to call stream.CloseAndRecv.
	close(stream.sendBlock)
	select {
	case <-stream.sendReturned:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Send to return after unblock")
	}
	select {
	case <-stream.closeRecvStarted:
	case <-time.After(time.Second):
		t.Fatal("stream.CloseAndRecv was not called after Send returned")
	}

	// Let stream.CloseAndRecv return so the test cleans up.
	close(stream.closeRecvReturn)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("CloseAndRecvWithTimeoutCause did not return")
	}
}

// If the background sender goroutine is still stuck in stream.Send (because
// nobody has canceled the underlying ctx), CloseAndRecvWithTimeoutCause must
// still honor its own timeout and return the given cause without blocking
// forever waiting for the sender goroutine to drain.
func TestSender_CloseAndRecvHonorsTimeoutWhileSendStuck(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(t.Context())
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(ctx, stream)

	err := sender.SendWithTimeoutCause(tspb.Now(), time.Millisecond, fmt.Errorf("send-cause"))
	require.Error(t, err)
	<-stream.sendStarted

	closeCause := fmt.Errorf("close-cause")
	_, err = sender.CloseAndRecvWithTimeoutCause(time.Millisecond, closeCause)
	require.Equal(t, closeCause, err)

	// Cancel to let the stuck Send unblock so the test doesn't leak the
	// background goroutine.
	cancel()
	<-stream.sendReturned
}
