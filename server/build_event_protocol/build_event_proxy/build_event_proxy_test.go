package build_event_proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

const (
	blockedSendCheckTimeout = 50 * time.Millisecond
	waitTimeout             = 30 * time.Second
)

func TestPublishBuildToolEventStream_SynchronousModeBackpressuresInsteadOfBuffering(t *testing.T) {
	flags.Set(t, "build_event_proxy.buffer_size", 0)

	// Use a fake upstream stream whose Send call blocks so we can tell whether
	// the proxy forwards backpressure to the caller or absorbs it locally.
	upstream := newBlockingBuildToolEventStream(t.Context())
	proxy := &BuildEventProxyClient{
		client:      &fakePublishBuildEventClient{stream: upstream},
		rootCtx:     t.Context(),
		synchronous: true,
	}

	stream, err := proxy.PublishBuildToolEventStream(t.Context())
	require.NoError(t, err)
	var closeStreamOnce sync.Once
	closeStream := func() {
		closeStreamOnce.Do(func() {
			require.NoError(t, stream.CloseSend())
		})
	}
	t.Cleanup(func() {
		upstream.unblockSend()
		closeStream()
	})

	// Kick off a Send and wait until it reaches the blocked upstream stream.
	req := &pepb.PublishBuildToolEventStreamRequest{}
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- stream.Send(req)
	}()
	waitForSendStart(t, upstream, req)

	// In synchronous mode, a full local buffer should not matter: the caller's
	// Send should remain blocked until the upstream Send unblocks.
	select {
	case err := <-sendDone:
		require.FailNowf(t, "send returned early", "Send() returned before upstream unblocked: %v", err)
	case <-time.After(blockedSendCheckTimeout):
	}

	// Once the upstream stream is released, the caller should complete and the
	// single request should be recorded upstream.
	upstream.unblockSend()
	select {
	case err := <-sendDone:
		require.NoError(t, err)
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for Send() to unblock")
	}

	closeStream()
	waitForCloseSend(t, upstream)

	sent := upstream.sentRequests()
	require.Len(t, sent, 1)
	assert.Same(t, req, sent[0])
}

func TestPublishBuildToolEventStream_AsynchronousModeDropsWhenBufferIsFull(t *testing.T) {
	flags.Set(t, "build_event_proxy.buffer_size", 0)

	// Use a blocked upstream stream while exercising the async proxy path, where
	// requests flow through the local channel before reaching the upstream Send.
	upstream := newBlockingBuildToolEventStream(t.Context())
	proxy := &BuildEventProxyClient{
		client:  &fakePublishBuildEventClient{stream: upstream},
		rootCtx: t.Context(),
	}

	stream, err := proxy.PublishBuildToolEventStream(t.Context())
	require.NoError(t, err)
	var closeStreamOnce sync.Once
	closeStream := func() {
		closeStreamOnce.Do(func() {
			require.NoError(t, stream.CloseSend())
		})
	}
	t.Cleanup(func() {
		upstream.unblockSend()
		closeStream()
	})

	// First, keep retrying until one request has definitely made it through the
	// zero-capacity channel and is now blocking in the upstream Send call.
	firstReq := &pepb.PublishBuildToolEventStreamRequest{}
	secondReq := &pepb.PublishBuildToolEventStreamRequest{}
	sendUntilUpstreamStarts(t, stream, upstream, firstReq)

	// With the only in-flight slot occupied and no channel buffering available,
	// the next request should be dropped rather than forwarded upstream.
	require.NoError(t, stream.Send(secondReq))
	select {
	case req := <-upstream.sendStarted:
		require.FailNowf(t, "unexpected upstream send", "unexpected upstream Send(%p) while first send is blocked", req)
	case <-time.After(blockedSendCheckTimeout):
	}

	// After we close and unblock the stream, only the first request should have
	// made it through to the upstream client.
	closeStream()
	upstream.unblockSend()
	waitForCloseSend(t, upstream)

	sent := upstream.sentRequests()
	require.Len(t, sent, 1)
	assert.Same(t, firstReq, sent[0])
}

// waitForSendStart waits until the fake upstream stream has observed a Send for
// wantReq, which lets the tests distinguish a blocked upstream call from a
// request that was dropped before reaching the upstream stream.
func waitForSendStart(t *testing.T, stream *blockingBuildToolEventStream, wantReq *pepb.PublishBuildToolEventStreamRequest) {
	t.Helper()
	select {
	case gotReq := <-stream.sendStarted:
		assert.Same(t, wantReq, gotReq)
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for upstream Send() to start")
	}
}

// sendUntilUpstreamStarts retries until the async proxy goroutine is actively
// receiving from the zero-capacity channel and has handed req to the upstream
// stream. Without this, the first Send can be dropped before the goroutine
// starts, which would make the async drop test flaky.
func sendUntilUpstreamStarts(t *testing.T, stream pepb.PublishBuildEvent_PublishBuildToolEventStreamClient, upstream *blockingBuildToolEventStream, req *pepb.PublishBuildToolEventStreamRequest) {
	t.Helper()
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	for {
		require.NoError(t, stream.Send(req))
		select {
		case gotReq := <-upstream.sendStarted:
			assert.Same(t, req, gotReq)
			return
		case <-timer.C:
			require.FailNow(t, "timed out waiting for upstream Send() to start")
		default:
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForCloseSend waits for the proxy goroutine to close the upstream stream
// after it finishes draining the request channel.
func waitForCloseSend(t *testing.T, stream *blockingBuildToolEventStream) {
	t.Helper()
	select {
	case <-stream.closeSendCalled:
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for upstream CloseSend()")
	}
}

type fakePublishBuildEventClient struct {
	stream pepb.PublishBuildEvent_PublishBuildToolEventStreamClient
}

func (c *fakePublishBuildEventClient) PublishLifecycleEvent(context.Context, *pepb.PublishLifecycleEventRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (c *fakePublishBuildEventClient) PublishBuildToolEventStream(context.Context, ...grpc.CallOption) (pepb.PublishBuildEvent_PublishBuildToolEventStreamClient, error) {
	return c.stream, nil
}

type blockingBuildToolEventStream struct {
	ctx             context.Context
	sendStarted     chan *pepb.PublishBuildToolEventStreamRequest
	sendUnblocked   chan struct{}
	closeSendCalled chan struct{}

	sentMu sync.Mutex
	sent   []*pepb.PublishBuildToolEventStreamRequest

	unblockSendOnce sync.Once
	closeSendOnce   sync.Once
}

// newBlockingBuildToolEventStream returns a fake upstream stream whose Send
// call blocks until unblockSend is invoked, allowing tests to force
// backpressure and observe whether requests are blocked or dropped.
func newBlockingBuildToolEventStream(ctx context.Context) *blockingBuildToolEventStream {
	return &blockingBuildToolEventStream{
		ctx:             ctx,
		sendStarted:     make(chan *pepb.PublishBuildToolEventStreamRequest, 10),
		sendUnblocked:   make(chan struct{}),
		closeSendCalled: make(chan struct{}),
	}
}

func (s *blockingBuildToolEventStream) Send(req *pepb.PublishBuildToolEventStreamRequest) error {
	s.sendStarted <- req
	<-s.sendUnblocked
	s.sentMu.Lock()
	s.sent = append(s.sent, req)
	s.sentMu.Unlock()
	return nil
}

// unblockSend releases any blocked Send calls on the fake upstream stream.
func (s *blockingBuildToolEventStream) unblockSend() {
	s.unblockSendOnce.Do(func() {
		close(s.sendUnblocked)
	})
}

// sentRequests returns the requests that completed Send after the fake
// upstream stream was unblocked.
func (s *blockingBuildToolEventStream) sentRequests() []*pepb.PublishBuildToolEventStreamRequest {
	s.sentMu.Lock()
	defer s.sentMu.Unlock()
	sent := make([]*pepb.PublishBuildToolEventStreamRequest, len(s.sent))
	copy(sent, s.sent)
	return sent
}

func (s *blockingBuildToolEventStream) Recv() (*pepb.PublishBuildToolEventStreamResponse, error) {
	panic("unexpected Recv")
}

func (s *blockingBuildToolEventStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *blockingBuildToolEventStream) Trailer() metadata.MD {
	return nil
}

func (s *blockingBuildToolEventStream) CloseSend() error {
	s.closeSendOnce.Do(func() {
		close(s.closeSendCalled)
	})
	return nil
}

func (s *blockingBuildToolEventStream) Context() context.Context {
	return s.ctx
}

func (s *blockingBuildToolEventStream) SendMsg(any) error {
	panic("unexpected SendMsg")
}

func (s *blockingBuildToolEventStream) RecvMsg(any) error {
	panic("unexpected RecvMsg")
}
