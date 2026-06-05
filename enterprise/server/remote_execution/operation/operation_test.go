package operation

import (
	"context"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

var (
	testDigest = &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1}
	testTaskID = digest.NewCASResourceName(testDigest, "test-instance", repb.DigestFunction_SHA256).NewUploadString()
)

// fakeStream is a minimal grpc.ClientStreamingClient that records sent
// operations and lets the test program Send / CloseAndRecv outcomes. The
// embedded grpc.ClientStream is nil; calls to its methods will panic, which
// is fine because retryingClient only uses Send and CloseAndRecv.
type fakeStream struct {
	grpc.ClientStream

	mu       sync.Mutex
	received []*longrunningpb.Operation

	// sendErr, if non-nil, is returned by every Send call (and the message
	// is not recorded).
	sendErr error
	// closeErr, if non-nil, is returned by CloseAndRecv.
	closeErr error
}

func (s *fakeStream) Send(op *longrunningpb.Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.received = append(s.received, op)
	return nil
}

func (s *fakeStream) CloseAndRecv() (*repb.PublishOperationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeErr != nil {
		return nil, s.closeErr
	}
	return &repb.PublishOperationResponse{}, nil
}

func (s *fakeStream) recordedOps() []*longrunningpb.Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.received)
}

// fakeExecutionClient implements just enough of repb.ExecutionClient to drive
// retryingClient. PublishOperation pops the next stream from the streams
// slice; the test sets up streams in the order it expects them to be opened.
type fakeExecutionClient struct {
	repb.ExecutionClient

	mu      sync.Mutex
	streams []*fakeStream
	dialed  int
}

func (c *fakeExecutionClient) PublishOperation(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[longrunningpb.Operation, repb.PublishOperationResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dialed >= len(c.streams) {
		return nil, gstatus.Error(gcodes.Internal, "fakeExecutionClient: no more streams configured")
	}
	s := c.streams[c.dialed]
	c.dialed++
	return s, nil
}

// newPublisher returns a Publisher backed by a fake ExecutionClient whose
// PublishOperation hands out the given streams in order. The first stream
// becomes the initial PublishOperation stream; subsequent streams are used by
// reconnect, in order.
func newPublisher(t *testing.T, streams ...*fakeStream) (*Publisher, *fakeExecutionClient) {
	t.Helper()
	require.NotEmpty(t, streams, "must configure at least one stream")
	client := &fakeExecutionClient{streams: streams}
	pub, err := Publish(t.Context(), client, testTaskID)
	require.NoError(t, err)
	return pub, client
}

func completedOp(t *testing.T) *longrunningpb.Operation {
	t.Helper()
	op, err := Assemble(testTaskID, Metadata(repb.ExecutionStage_COMPLETED, testDigest), nil)
	require.NoError(t, err)
	return op
}

func progressOp(t *testing.T) *longrunningpb.Operation {
	t.Helper()
	op, err := Assemble(testTaskID, Metadata(repb.ExecutionStage_EXECUTING, testDigest), nil)
	require.NoError(t, err)
	return op
}

func TestRetryingClient_Send_BuffersCompletedAfterSuccess(t *testing.T) {
	stream := &fakeStream{}
	pub, _ := newPublisher(t, stream)

	op := completedOp(t)
	require.NoError(t, pub.Send(op))

	// Stream received the op, and the buffer contains it for future replay.
	assert.Equal(t, 1, len(stream.recordedOps()))
	assert.Equal(t, 1, len(pub.retryStream.republishMessages))
}

func TestRetryingClient_Send_KeepsOnlyMostRecentBeforeCompleted(t *testing.T) {
	// Before any COMPLETED has been sent, the buffer holds only the most
	// recent message — a newly-saved message drops whatever was there.
	stream := &fakeStream{}
	pub, _ := newPublisher(t, stream)

	first := progressOp(t)
	require.NoError(t, pub.Send(first))
	assert.Equal(t, 1, len(pub.retryStream.republishMessages))

	second := progressOp(t)
	require.NoError(t, pub.Send(second))
	require.Equal(t, 1, len(pub.retryStream.republishMessages), "older buffered message is dropped")
	assert.Same(t, second, pub.retryStream.republishMessages[0].msg, "buffer holds the most recent")
}

func TestRetryingClient_Send_AccumulatesMessagesAfterCompleted(t *testing.T) {
	// Once a COMPLETED is in the buffer, every subsequent message stays —
	// the execution server needs to see every completion-stage update
	// (e.g. post-completion stats) to build the final execution record.
	stream := &fakeStream{}
	pub, _ := newPublisher(t, stream)

	completed := completedOp(t)
	require.NoError(t, pub.Send(completed))
	require.Equal(t, 1, len(pub.retryStream.republishMessages))

	progress := progressOp(t)
	require.NoError(t, pub.Send(progress))
	assert.Equal(t, 2, len(pub.retryStream.republishMessages), "messages after COMPLETED accumulate")
}

func TestRetryingClient_Send_NonRetryableError_Surfaces(t *testing.T) {
	// The server rejects the message with a non-retryable error code; the
	// error must surface to the caller. The message remains in the buffer
	// (saveMessage runs before Send), which is harmless: the Publisher is
	// effectively dead after surfacing a non-retryable error anyway.
	stream := &fakeStream{sendErr: gstatus.Error(gcodes.InvalidArgument, "bad message")}
	pub, _ := newPublisher(t, stream)

	err := pub.Send(completedOp(t))
	require.Error(t, err)
	assert.Equal(t, gcodes.InvalidArgument, gstatus.Code(err))
}

func TestRetryingClient_Send_ReconnectsAndDeliversCompletedExactlyOnce(t *testing.T) {
	// First stream's Send fails with a retryable error. send saves msg
	// before failing, then reconnect's replay loop delivers it on stream 2.
	// Stream 2 must receive the message exactly once.
	stream1 := &fakeStream{sendErr: io.EOF}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	op := completedOp(t)
	require.NoError(t, pub.Send(op))

	assert.Empty(t, stream1.recordedOps(), "stream 1's Send was rigged to fail")
	assert.Equal(t, 1, len(stream2.recordedOps()), "stream 2 must receive the COMPLETED exactly once")
	assert.Equal(t, 1, len(pub.retryStream.republishMessages))
}

func TestRetryingClient_Send_ReconnectsAndDeliversNonCompletedExactlyOnce(t *testing.T) {
	// A non-COMPLETED Send that fails with a retryable error must also be
	// delivered exactly once on the new stream, via reconnect's replay.
	stream1 := &fakeStream{sendErr: io.EOF}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	op := progressOp(t)
	require.NoError(t, pub.Send(op))

	assert.Empty(t, stream1.recordedOps(), "stream 1's Send was rigged to fail")
	got := stream2.recordedOps()
	require.Equal(t, 1, len(got), "stream 2 must receive the progress update exactly once")
	assert.Equal(t, op, got[0])
	assert.Equal(t, 1, len(pub.retryStream.republishMessages))
}

func TestRetryingClient_Send_ReplaysBufferedCompletedOnNewStream(t *testing.T) {
	// COMPLETED #1 succeeds on stream 1 (and is buffered). Then stream 1
	// breaks. COMPLETED #2 is saved (appended after #1, since the buffer
	// head is COMPLETED) and its Send fails, triggering reconnect, which
	// replays [#1, #2] on stream 2.
	stream1 := &fakeStream{}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	op1 := completedOp(t)
	op2 := completedOp(t)

	require.NoError(t, pub.Send(op1))
	assert.Equal(t, 1, len(stream1.recordedOps()))

	stream1.mu.Lock()
	stream1.sendErr = io.EOF
	stream1.mu.Unlock()

	require.NoError(t, pub.Send(op2))

	got := stream2.recordedOps()
	require.Equal(t, 2, len(got), "stream 2 should see both COMPLETED messages")
	assert.Equal(t, op1, got[0], "buffered COMPLETED replayed first")
	assert.Equal(t, op2, got[1], "new COMPLETED sent after replay")
	assert.Equal(t, 2, len(pub.retryStream.republishMessages))
}

func TestRetryingClient_Send_DropsBufferedNonCompletedWhenSavingNext(t *testing.T) {
	// A non-COMPLETED progress update goes through on stream 1 and is
	// buffered. When stream 1 breaks and a COMPLETED is sent, saveMessage
	// drops the buffered progress (the head isn't COMPLETED yet) before
	// appending the COMPLETED. Stream 2's replay therefore sees only the
	// COMPLETED — the earlier progress update is gone.
	stream1 := &fakeStream{}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	progress := progressOp(t)
	require.NoError(t, pub.Send(progress))
	assert.Equal(t, 1, len(stream1.recordedOps()))

	stream1.mu.Lock()
	stream1.sendErr = io.EOF
	stream1.mu.Unlock()

	op := completedOp(t)
	require.NoError(t, pub.Send(op))

	got := stream2.recordedOps()
	require.Equal(t, 1, len(got), "stream 2 should see only the COMPLETED — progress was dropped on save")
	assert.Same(t, op, got[0])
}

func TestRetryingClient_Reconnect_RetryableReplayFailure_RedialsAndSucceeds(t *testing.T) {
	// Stream 1: first send succeeds, then it breaks.
	// Stream 2: every send fails with a retryable error — exercises
	// reconnect's `continue outer`, where a successful redial but failing
	// replay triggers another redial.
	// Stream 3: works. The buffered COMPLETED is replayed here, then the
	// new COMPLETED is sent.
	stream1 := &fakeStream{}
	stream2 := &fakeStream{sendErr: io.EOF}
	stream3 := &fakeStream{}
	pub, client := newPublisher(t, stream1, stream2, stream3)

	op1 := completedOp(t)
	op2 := completedOp(t)

	require.NoError(t, pub.Send(op1))
	assert.Equal(t, 1, len(stream1.recordedOps()))

	stream1.mu.Lock()
	stream1.sendErr = io.EOF
	stream1.mu.Unlock()

	require.NoError(t, pub.Send(op2))

	assert.Equal(t, 3, client.dialed, "reconnect should have redialed after stream 2's replay failure")
	assert.Empty(t, stream2.recordedOps(), "stream 2's replay was rigged to fail")
	got := stream3.recordedOps()
	require.Equal(t, 2, len(got), "stream 3 must receive op1 (replayed) then op2 (retried)")
	assert.Equal(t, op1, got[0], "buffered COMPLETED replayed first")
	assert.Equal(t, op2, got[1], "new COMPLETED sent after replay")
	assert.Equal(t, 2, len(pub.retryStream.republishMessages))
}

func TestRetryingClient_Reconnect_NonRetryableReplayFailure_Surfaces(t *testing.T) {
	// Stream 1: first send succeeds, then breaks.
	// Stream 2: replay fails with a non-retryable error — reconnect must
	// return the wrapped error immediately rather than redialing or
	// silently swallowing the rejection.
	stream1 := &fakeStream{}
	stream2 := &fakeStream{sendErr: gstatus.Error(gcodes.InvalidArgument, "rejected")}
	pub, _ := newPublisher(t, stream1, stream2)

	op1 := completedOp(t)
	require.NoError(t, pub.Send(op1))

	stream1.mu.Lock()
	stream1.sendErr = io.EOF
	stream1.mu.Unlock()

	err := pub.Send(completedOp(t))
	require.Error(t, err, "the rejected replay must surface to the caller")
	// op2 was saved before reconnect (so reconnect's replay would deliver
	// it) and stays in the buffer when reconnect fails. The Publisher is
	// effectively dead after surfacing a non-retryable error anyway, so
	// leaving op2 buffered is harmless.
	assert.Equal(t, 2, len(pub.retryStream.republishMessages))
}

func TestRetryingClient_CloseAndRecv_ReplaysMultipleBufferedCompleted(t *testing.T) {
	// Two COMPLETED messages get buffered on stream 1. Stream 1's
	// CloseAndRecv returns a retryable error, so reconnect runs and must
	// replay BOTH buffered messages on stream 2 before closeAndRecv loops
	// back to close it.
	stream1 := &fakeStream{closeErr: gstatus.Error(gcodes.Unavailable, "broken")}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	op1 := completedOp(t)
	op2 := completedOp(t)

	require.NoError(t, pub.Send(op1))
	require.NoError(t, pub.Send(op2))
	assert.Equal(t, 2, len(stream1.recordedOps()))

	_, err := pub.CloseAndRecv()
	require.NoError(t, err)

	got := stream2.recordedOps()
	require.Equal(t, 2, len(got), "stream 2 should receive both buffered COMPLETED messages")
	assert.Equal(t, op1, got[0])
	assert.Equal(t, op2, got[1])
}

func TestRetryingClient_CloseAndRecv_ReconnectsAndReplaysCompleted(t *testing.T) {
	// COMPLETED is sent successfully on stream 1 (and buffered). Stream 1's
	// CloseAndRecv then returns a retryable error. closeAndRecv should
	// reconnect, replay [COMPLETED] on stream 2, and close stream 2
	// cleanly. Server sees the COMPLETED once on each stream — duplicate
	// COMPLETED on the wire is the documented tradeoff for resilience.
	stream1 := &fakeStream{closeErr: gstatus.Error(gcodes.Unavailable, "broken")}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	op := completedOp(t)
	require.NoError(t, pub.Send(op))
	assert.Equal(t, 1, len(stream1.recordedOps()))

	res, err := pub.CloseAndRecv()
	require.NoError(t, err)
	assert.NotNil(t, res)

	got := stream2.recordedOps()
	require.Equal(t, 1, len(got), "stream 2 should receive the replayed COMPLETED")
	assert.Equal(t, op, got[0])
}

func TestRetryingClient_CloseAndRecv_ReplaysBufferedNonCompleted(t *testing.T) {
	// A buffered non-COMPLETED stays in the buffer until another saveMessage
	// call drops it. closeAndRecv doesn't go through saveMessage, so when
	// its reconnect triggers, the buffered progress update is replayed on
	// the new stream.
	stream1 := &fakeStream{closeErr: gstatus.Error(gcodes.Unavailable, "broken")}
	stream2 := &fakeStream{}
	pub, _ := newPublisher(t, stream1, stream2)

	progress := progressOp(t)
	require.NoError(t, pub.Send(progress))

	_, err := pub.CloseAndRecv()
	require.NoError(t, err)

	got := stream2.recordedOps()
	require.Equal(t, 1, len(got), "stream 2 should receive the replayed progress update")
	assert.Same(t, progress, got[0])
}

func TestRetryingClient_CloseAndRecv_NonRetryableError_Surfaces(t *testing.T) {
	// CloseAndRecv returning a non-retryable error is surfaced to the
	// caller without any reconnect attempt.
	stream1 := &fakeStream{closeErr: gstatus.Error(gcodes.InvalidArgument, "nope")}
	pub, client := newPublisher(t, stream1)

	_, err := pub.CloseAndRecv()
	require.Error(t, err)
	assert.Equal(t, gcodes.InvalidArgument, gstatus.Code(err))
	assert.Equal(t, 1, client.dialed, "no reconnect attempted on non-retryable error")
}
