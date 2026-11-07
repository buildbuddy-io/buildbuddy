package build_event_proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
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

func TestAsynchronousModeForwardsOnlyAPIKey(t *testing.T) {
	client := &fakePublishBuildEventClient{
		stream:         newBlockingBuildToolEventStream(t.Context()),
		streamCalls:    make(chan recordedCall, 1),
		lifecycleCalls: make(chan recordedCall, 1),
	}
	proxy := &BuildEventProxyClient{
		client:  client,
		rootCtx: t.Context(),
	}

	// The request ctx carries the API key plus headers and values that must not
	// be forwarded, and is already canceled because the handler returned, so
	// the ctx recorded by the fake shows whether the calls outlive the request.
	reqCtx := metadata.AppendToOutgoingContext(t.Context(),
		"x-buildbuddy-api-key", "test-api-key",
		"x-buildbuddy-bypass-ip-rules", "true")
	reqCtx = context.WithValue(reqCtx, authutil.ContextTokenStringKey, "test-jwt")
	reqCtx = context.WithValue(reqCtx, usageutil.OriginHeaderName, "test-origin")
	reqCtx, cancel := context.WithCancel(reqCtx)
	cancel()

	stream, err := proxy.PublishBuildToolEventStream(reqCtx)
	require.NoError(t, err)
	t.Cleanup(func() { stream.CloseSend() })
	_, err = proxy.PublishLifecycleEvent(reqCtx, &pepb.PublishLifecycleEventRequest{})
	require.NoError(t, err)

	checkAsyncCall(t, "stream", client.streamCalls)
	checkAsyncCall(t, "lifecycle", client.lifecycleCalls)
}

func checkAsyncCall(t *testing.T, name string, ch chan recordedCall) {
	t.Helper()
	var call recordedCall
	select {
	case call = <-ch:
	case <-time.After(waitTimeout):
		require.FailNowf(t, "timed out", "timed out waiting for upstream %s call", name)
	}
	assert.Equal(t, []string{"test-api-key"}, call.md.Get("x-buildbuddy-api-key"), "%s API key", name)
	assert.Empty(t, call.md.Get("x-buildbuddy-bypass-ip-rules"), "%s should not forward other headers", name)
	assert.NoError(t, call.ctxErr, "%s ctx should outlive the request ctx", name)
	assert.Nil(t, call.jwt, "%s ctx should not carry the JWT", name)
	assert.Nil(t, call.origin, "%s ctx should not carry the origin", name)
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

// recordedCall captures the outgoing metadata, ctx state and selected ctx
// values at the time the fake upstream client was called.
type recordedCall struct {
	md     metadata.MD
	ctxErr error
	jwt    any
	origin any
}

func recordCall(ctx context.Context, ch chan recordedCall) {
	if ch == nil {
		return
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	ch <- recordedCall{
		md:     md,
		ctxErr: ctx.Err(),
		jwt:    ctx.Value(authutil.ContextTokenStringKey),
		origin: ctx.Value(usageutil.OriginHeaderName),
	}
}

type fakePublishBuildEventClient struct {
	stream pepb.PublishBuildEvent_PublishBuildToolEventStreamClient

	// If set, calls are recorded to these channels.
	streamCalls    chan recordedCall
	lifecycleCalls chan recordedCall
}

func (c *fakePublishBuildEventClient) PublishLifecycleEvent(ctx context.Context, _ *pepb.PublishLifecycleEventRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	recordCall(ctx, c.lifecycleCalls)
	return &emptypb.Empty{}, nil
}

func (c *fakePublishBuildEventClient) PublishBuildToolEventStream(ctx context.Context, _ ...grpc.CallOption) (pepb.PublishBuildEvent_PublishBuildToolEventStreamClient, error) {
	recordCall(ctx, c.streamCalls)
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
