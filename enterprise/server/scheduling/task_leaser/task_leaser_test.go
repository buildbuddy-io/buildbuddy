package task_leaser_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_leaser"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

// fakeSchedulerClient returns the queued streams from successive LeaseTask
// calls, and records ReEnqueueTask requests.
type fakeSchedulerClient struct {
	scpb.SchedulerClient

	streams    chan *fakeLeaseStream
	reEnqueued chan *scpb.ReEnqueueTaskRequest
}

func newFakeSchedulerClient(streams ...*fakeLeaseStream) *fakeSchedulerClient {
	return &fakeSchedulerClient{
		streams:    queue(streams...),
		reEnqueued: make(chan *scpb.ReEnqueueTaskRequest, 10),
	}
}

func (c *fakeSchedulerClient) LeaseTask(ctx context.Context, opts ...grpc.CallOption) (scpb.Scheduler_LeaseTaskClient, error) {
	select {
	case stream := <-c.streams:
		return stream, nil
	default:
		return nil, status.FailedPreconditionError("unexpected LeaseTask call")
	}
}

func (c *fakeSchedulerClient) ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest, opts ...grpc.CallOption) (*scpb.ReEnqueueTaskResponse, error) {
	c.reEnqueued <- req
	return &scpb.ReEnqueueTaskResponse{}, nil
}

// fakeLeaseStream replies to each Send with the next queued reply, and stays
// open until a request finalizes or re-enqueues the task, or until end is
// called.
type fakeLeaseStream struct {
	grpc.ClientStream

	// Receives each request passed to Send.
	sent chan *scpb.LeaseTaskRequest
	// Replies to successive Send calls.
	replies chan *scpb.LeaseTaskResponse
	// Results for Recv to return, in order.
	recvs chan leaseResponse
	// Closed by end, after which Send returns io.EOF.
	ended chan struct{}
}

type leaseResponse struct {
	rsp *scpb.LeaseTaskResponse
	err error
}

func newFakeLeaseStream(replies ...*scpb.LeaseTaskResponse) *fakeLeaseStream {
	return &fakeLeaseStream{
		sent:    make(chan *scpb.LeaseTaskRequest, 10),
		replies: queue(replies...),
		recvs:   make(chan leaseResponse, len(replies)+1),
		ended:   make(chan struct{}),
	}
}

// end ends the stream with err, as when the scheduler or a proxy in between
// resets it.
func (s *fakeLeaseStream) end(err error) {
	close(s.ended)
	s.recvs <- leaseResponse{err: err}
}

func (s *fakeLeaseStream) Send(req *scpb.LeaseTaskRequest) error {
	select {
	case <-s.ended:
		return io.EOF
	default:
	}
	s.sent <- req
	select {
	case rsp := <-s.replies:
		s.recvs <- leaseResponse{rsp: rsp}
	default:
	}
	if req.GetFinalize() || req.GetReEnqueue() {
		s.end(io.EOF)
	}
	return nil
}

func (s *fakeLeaseStream) Recv() (*scpb.LeaseTaskResponse, error) {
	next := <-s.recvs
	return next.rsp, next.err
}

// queue returns a buffered channel holding items in order.
func queue[T any](items ...T) chan T {
	ch := make(chan T, len(items))
	for _, item := range items {
		ch <- item
	}
	return ch
}

func TestLease_ReconnectsAsSoonAsStreamEnds(t *testing.T) {
	serializedTask, err := proto.Marshal(&repb.ExecutionTask{ExecutionId: "task-1"})
	require.NoError(t, err)
	// Use a long lease duration so that only the dropped stream can trigger the
	// reconnect, not a keepalive.
	firstStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{
		SerializedTask:       serializedTask,
		LeaseId:              "lease-1",
		ReconnectToken:       "lease-1",
		SupportsReconnect:    true,
		LeaseDurationSeconds: 3600,
	})
	secondStream := newFakeLeaseStream(
		// Ask for the next keepalive right away, which the executor only sends
		// if the reconnect succeeded.
		&scpb.LeaseTaskResponse{LeaseDurationSeconds: 0},
		&scpb.LeaseTaskResponse{LeaseDurationSeconds: 3600},
		&scpb.LeaseTaskResponse{ClosedCleanly: true},
	)
	client := newFakeSchedulerClient(firstStream, secondStream)
	env := real_environment.NewBatchEnv()
	env.SetSchedulerClient(client)
	leaser := task_leaser.NewTaskLeaser(env, "executor-1", "host-1", false /*=supportsExperimentFlags*/)
	lease, err := leaser.Lease(t.Context(), "task-1")
	require.NoError(t, err)
	defer lease.Close(t.Context(), nil, false /*=retry*/)

	// gRPC reports a stream reset by a proxy as Internal.
	firstStream.end(status.InternalError("stream terminated by RST_STREAM with error code: NO_ERROR"))

	var reqs []*scpb.LeaseTaskRequest
	for range 2 {
		select {
		case req := <-secondStream.sent:
			reqs = append(reqs, req)
		case <-lease.Context().Done():
			require.FailNow(t, "lease was canceled instead of reconnecting")
		case <-time.After(30 * time.Second):
			require.FailNow(t, "lease didn't reconnect and renew before the next keepalive")
		}
	}
	require.Equal(t, "lease-1", reqs[0].GetReconnectToken())
	lease.Close(t.Context(), nil, false /*=retry*/)
	require.Empty(t, client.reEnqueued)
}

func TestLease_CancelsWhenStreamEndsWithNonRetryableError(t *testing.T) {
	serializedTask, err := proto.Marshal(&repb.ExecutionTask{ExecutionId: "task-1"})
	require.NoError(t, err)
	firstStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{
		SerializedTask:       serializedTask,
		LeaseId:              "lease-1",
		ReconnectToken:       "lease-1",
		SupportsReconnect:    true,
		LeaseDurationSeconds: 3600,
	})
	// Serve a working stream in case the executor tries to reconnect.
	secondStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{LeaseDurationSeconds: 3600})
	env := real_environment.NewBatchEnv()
	env.SetSchedulerClient(newFakeSchedulerClient(firstStream, secondStream))
	leaser := task_leaser.NewTaskLeaser(env, "executor-1", "host-1", false /*=supportsExperimentFlags*/)
	lease, err := leaser.Lease(t.Context(), "task-1")
	require.NoError(t, err)
	defer lease.Close(t.Context(), nil, false /*=retry*/)

	firstStream.end(status.NotFoundError(`task "task-1" disappeared, possibly cancelled`))

	select {
	case <-lease.Context().Done():
	case <-secondStream.sent:
		require.FailNow(t, "lease reconnected after a non-retryable error")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "lease wasn't canceled before the next keepalive")
	}
}
