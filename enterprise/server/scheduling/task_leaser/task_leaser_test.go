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

type fakeSchedulerClient struct {
	scpb.SchedulerClient

	streams    chan *fakeLeaseStream
	reEnqueued chan *scpb.ReEnqueueTaskRequest
	openStream func() (scpb.Scheduler_LeaseTaskClient, error)
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
	}
	if c.openStream == nil {
		return nil, status.FailedPreconditionError("unexpected LeaseTask call")
	}
	return c.openStream()
}

func (c *fakeSchedulerClient) ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest, opts ...grpc.CallOption) (*scpb.ReEnqueueTaskResponse, error) {
	c.reEnqueued <- req
	return &scpb.ReEnqueueTaskResponse{}, nil
}

type fakeLeaseStream struct {
	grpc.ClientStream

	sent     chan *scpb.LeaseTaskRequest
	replies  chan *scpb.LeaseTaskResponse
	recvs    chan leaseResponse
	ended    chan struct{}
	failWith error
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
	if s.failWith != nil {
		s.end(s.failWith)
		return nil
	}
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
	for _, testCase := range []struct {
		name string
		// Errors returned when the executor tries to open a new stream, before
		// the retried stream opens.
		openErrs []error
	}{
		{
			name: "stream opens on first try",
		},
		{
			name:     "first open fails with Unavailable",
			openErrs: []error{status.UnavailableError("connection refused")},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Prevent keepalives on the initial stream during the test. This
			// ensures that the executor is not relying on keepalive pings to
			// notice dropped streams and reconnect.
			initialStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{
				SerializedTask:       serializedTask,
				LeaseId:              "lease-1",
				ReconnectToken:       "lease-1",
				SupportsReconnect:    true,
				LeaseDurationSeconds: 99999,
			})
			retriedStream := newFakeLeaseStream(
				// Ask for the first keepalive immediately so that we can test
				// keepalive-after-reconnect behavior without delaying the test.
				&scpb.LeaseTaskResponse{LeaseDurationSeconds: 0},
				// After the first keepalive, prevent subsequent keepalives
				// during the test, so that the only subsequent request is the
				// one that finalizes the lease.
				&scpb.LeaseTaskResponse{LeaseDurationSeconds: 99999},
				// Successfully finalize the lease on the server side.
				&scpb.LeaseTaskResponse{ClosedCleanly: true},
			)
			client := newFakeSchedulerClient(initialStream)
			// LeaseTask calls after the initial one first return
			// testCase.openErrs, in order, and then open retriedStream.
			openErrs := queue(testCase.openErrs...)
			client.openStream = func() (scpb.Scheduler_LeaseTaskClient, error) {
				select {
				case err := <-openErrs:
					return nil, err
				default:
					return retriedStream, nil
				}
			}
			env := real_environment.NewBatchEnv()
			env.SetSchedulerClient(client)
			leaser := task_leaser.NewTaskLeaser(env, "executor-1", "host-1", false /*=supportsExperimentFlags*/)

			// Establish a lease successfully.
			lease, err := leaser.Lease(t.Context(), "task-1")
			require.NoError(t, err)
			defer lease.Close(t.Context(), nil, false /*=retry*/)

			// Break the initial stream attempt, having Recv() return an
			// internal error.
			initialStream.end(status.InternalError("stream terminated by RST_STREAM with error code: NO_ERROR"))

			// Executor should reconnect. It should send two requests, one for
			// the initial reconnect containing the lease token, then a regular
			// keepalive.
			var reqs []*scpb.LeaseTaskRequest
			for range 2 {
				select {
				case req := <-retriedStream.sent:
					reqs = append(reqs, req)
				case <-lease.Context().Done():
					require.FailNow(t, "lease was canceled instead of reconnecting")
				case <-time.After(30 * time.Second):
					require.FailNow(t, "lease didn't reconnect and renew before the next keepalive")
				}
			}
			require.Equal(t, "lease-1", reqs[0].GetReconnectToken())

			// Lease should close cleanly, without the task being re-enqueued
			// unnecessarily.
			lease.Close(t.Context(), nil, false /*=retry*/)
			require.Empty(t, client.reEnqueued)
		})
	}
}

func TestLease_CancelsWhenStreamEndsWithNonRetryableError(t *testing.T) {
	serializedTask, err := proto.Marshal(&repb.ExecutionTask{ExecutionId: "task-1"})
	require.NoError(t, err)

	// Set up a normal stream that serves the initial connection attempt and
	// prevents the client from sending keepalives during the test.
	initialStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{
		SerializedTask:       serializedTask,
		LeaseId:              "lease-1",
		ReconnectToken:       "lease-1",
		SupportsReconnect:    true,
		LeaseDurationSeconds: 99999,
	})

	// Serve another working stream in case the executor tries to reconnect, so
	// that the test can tell when it does.
	retriedStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{LeaseDurationSeconds: 99999})
	env := real_environment.NewBatchEnv()
	env.SetSchedulerClient(newFakeSchedulerClient(initialStream, retriedStream))
	leaser := task_leaser.NewTaskLeaser(env, "executor-1", "host-1", false /*=supportsExperimentFlags*/)

	// Establish a lease successfully.
	lease, err := leaser.Lease(t.Context(), "task-1")
	require.NoError(t, err)
	defer lease.Close(t.Context(), nil, false /*=retry*/)

	// Break the initial stream with a non-retryable error, having Recv()
	// return NotFound.
	initialStream.end(status.NotFoundError(`task "task-1" disappeared, possibly cancelled`))

	// Executor should cancel the lease instead of reconnecting.
	select {
	case <-lease.Context().Done():
	case <-retriedStream.sent:
		require.FailNow(t, "lease reconnected after a non-retryable error")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "lease wasn't canceled before the next keepalive")
	}
}

func TestLease_ReEnqueuesOnCloseAfterReconnectRetriesRunOut(t *testing.T) {
	serializedTask, err := proto.Marshal(&repb.ExecutionTask{ExecutionId: "task-1"})
	require.NoError(t, err)
	initialStream := newFakeLeaseStream(&scpb.LeaseTaskResponse{
		SerializedTask:       serializedTask,
		LeaseId:              "lease-1",
		ReconnectToken:       "lease-1",
		SupportsReconnect:    true,
		LeaseDurationSeconds: 99999,
	})
	client := newFakeSchedulerClient(initialStream)

	// Simulate an unreachable scheduler, so that the executor has to retry both
	// opening a new stream and sending the reconnect request on it. The first
	// LeaseTask call fails, and every stream opened after that fails its first
	// request.
	openErrs := queue[error](status.UnavailableError("connection refused"))
	opened := make(chan *fakeLeaseStream, 100)
	client.openStream = func() (scpb.Scheduler_LeaseTaskClient, error) {
		select {
		case err := <-openErrs:
			return nil, err
		default:
		}
		stream := newFakeLeaseStream()
		stream.failWith = status.UnavailableError("no healthy upstream")
		opened <- stream
		return stream, nil
	}
	env := real_environment.NewBatchEnv()
	env.SetSchedulerClient(client)
	leaser := task_leaser.NewTaskLeaser(env, "executor-1", "host-1", false /*=supportsExperimentFlags*/)

	// Establish a lease successfully.
	lease, err := leaser.Lease(t.Context(), "task-1")
	require.NoError(t, err)

	// Break the initial stream, having Recv() return Unavailable.
	initialStream.end(status.UnavailableError("connection reset"))

	// Executor should keep trying to reconnect until the reconnect timeout,
	// then cancel the lease.
	select {
	case <-lease.Context().Done():
	case <-time.After(30 * time.Second):
		require.FailNow(t, "lease wasn't canceled after reconnect retries ran out")
	}
	lease.Close(t.Context(), nil, false /*=retry*/)

	// Executor should have kept retrying after it failed to open a stream,
	// and sent the reconnect request with the lease token on every stream
	// that it opened. Close should not have sent its final request on a
	// stream that never claimed the task.
	close(opened)
	require.NotEmpty(t, opened)
	for stream := range opened {
		require.Len(t, stream.sent, 1)
		req := <-stream.sent
		require.Equal(t, "lease-1", req.GetReconnectToken())
	}
	// Since the lease couldn't be closed cleanly, Close should re-enqueue the
	// task under the original lease.
	require.Len(t, client.reEnqueued, 1)
	reEnqueued := <-client.reEnqueued
	require.Equal(t, "lease-1", reEnqueued.GetLeaseId())
}
