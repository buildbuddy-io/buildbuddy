package task_leaser

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
)

var (
	enableReconnect = flag.Bool("executor.enable_lease_reconnect", true, "Enable task lease reconnection on scheduler server shutdown.")
)

const (
	// Timeout for reconnecting a lease after disconnecting from the server.
	reconnectTimeout = 1 * time.Second
)

// Verify that TaskLeaser implements interfaces.TaskLeaser.
var _ interfaces.TaskLeaser = (*TaskLeaser)(nil)

// Verify that TaskLease implements interfaces.TaskLease.
var _ interfaces.TaskLease = (*TaskLease)(nil)

type TaskLeaser struct {
	env                     environment.Env
	executorID              string
	executorHostname        string
	supportsExperimentFlags bool
}

func NewTaskLeaser(env environment.Env, executorID, executorHostname string, supportsExperimentFlags bool) *TaskLeaser {
	return &TaskLeaser{
		env:                     env,
		executorID:              executorID,
		executorHostname:        executorHostname,
		supportsExperimentFlags: supportsExperimentFlags,
	}
}

func (t *TaskLeaser) Lease(ctx context.Context, taskID string) (interfaces.TaskLease, error) {
	lease := &TaskLease{
		env:                     t.env,
		executorID:              t.executorID,
		executorHostname:        t.executorHostname,
		supportsExperimentFlags: t.supportsExperimentFlags,
		taskID:                  taskID,
		execTask:                &repb.ExecutionTask{},
		quit:                    make(chan struct{}),
		ttl:                     100 * time.Second,
	}
	ctx, serializedTask, err := lease.claim(ctx)
	if err != nil {
		return nil, err
	}
	lease.ctx = ctx
	if err := proto.Unmarshal(serializedTask, lease.execTask); err != nil {
		lease.Close(ctx, nil, false /*=retry*/)
		return nil, status.InternalErrorf("unmarshal ExecutionTask: %s", err)
	}
	return lease, nil
}

type TaskLease struct {
	env                     environment.Env
	executorID              string
	executorHostname        string
	supportsExperimentFlags bool
	taskID                  string

	ctx            context.Context
	execTask       *repb.ExecutionTask
	leaseID        string
	reconnectToken string
	quit           chan struct{}
	mu             sync.Mutex // protects stream
	stream         *leaseStream
	ttl            time.Duration
	cancelFunc     context.CancelFunc
}

func (t *TaskLease) Context() context.Context {
	return t.ctx
}

func (t *TaskLease) Task() *repb.ExecutionTask {
	return t.execTask
}

func (t *TaskLease) sendRequest(req *scpb.LeaseTaskRequest) (*scpb.LeaseTaskResponse, error) {
	if err := t.stream.send(req); err != nil {
		if err != io.EOF {
			return nil, err
		}
		// Read the terminal RPC status so pingServer can decide whether to
		// reconnect. Send's io.EOF only tells us that the stream has ended.
		if _, err := t.stream.nextResponse(); err != nil {
			return nil, err
		}
		// A reply here is unexpected because each successful send consumes its
		// reply before the next request. Return a retryable error so pingServer
		// reconnects rather than treating the failed send as successful.
		return nil, status.InternalError("unexpected EOF with unexpected message on stream")
	}
	return t.stream.nextResponse()
}

func (t *TaskLease) pingServer(ctx context.Context) (b []byte, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed() {
		// Don't try to send keepalive pings after Close() was called.
		return nil, nil
	}

	req := &scpb.LeaseTaskRequest{
		ExecutorId:              t.executorID,
		ExecutorHostname:        t.executorHostname,
		TaskId:                  t.taskID,
		SupportsReconnect:       *enableReconnect,
		ReconnectToken:          t.reconnectToken,
		SupportsExperimentFlags: t.supportsExperimentFlags,
	}
	var rsp *scpb.LeaseTaskResponse
	var r *retry.Retry
	for {
		var err error
		rsp, err = t.sendRequest(req)
		if err == nil {
			break
		}
		if !*enableReconnect || !(status.IsUnavailableError(err) || status.IsInternalError(err)) {
			return nil, err
		}
		originalErr := err
		// Server is unavailable (e.g. shutting down); retry. Note that we don't
		// start the retry context timeout until after observing the disconnect
		// error.
		stream, err := t.env.GetSchedulerClient().LeaseTask(ctx)
		if err != nil {
			return nil, status.WrapError(err, "reconnect lease")
		}
		t.stream = newLeaseStream(stream)
		if r == nil {
			ctx, cancel := context.WithTimeout(ctx, reconnectTimeout)
			defer cancel()
			r = retry.DefaultWithContext(ctx)
		}
		if !r.Next() {
			return nil, originalErr
		}
	}
	if rsp.GetReconnectToken() != "" {
		t.reconnectToken = rsp.GetReconnectToken()
	}
	if rsp.GetLeaseId() != "" {
		t.leaseID = rsp.GetLeaseId()
	}
	t.ttl = time.Duration(rsp.GetLeaseDurationSeconds()) * time.Second
	return rsp.GetSerializedTask(), nil
}

func (t *TaskLease) reEnqueueTask(ctx context.Context, reason string) error {
	req := &scpb.ReEnqueueTaskRequest{
		TaskId:  t.taskID,
		LeaseId: t.leaseID,
		Reason:  reason,
	}
	_, err := t.env.GetSchedulerClient().ReEnqueueTask(ctx, req)
	return err
}

func (t *TaskLease) keepLease(ctx context.Context) {
	go func() {
		for {
			t.mu.Lock()
			stream := t.stream
			t.mu.Unlock()
			select {
			case <-t.quit:
				return
			case <-time.After(t.ttl):
			case <-stream.done:
				// In Close(), both t.quit and stream.done are closed.
				if t.closed() {
					return
				}
				log.CtxWarningf(ctx, "Lease stream ended early: %s", stream.err)
			}
			if _, err := t.pingServer(ctx); err != nil {
				log.CtxErrorf(ctx, "Error updating lease: %s", err)
				t.cancelFunc()
				return
			}
		}
	}()
}

func (t *TaskLease) claim(ctx context.Context) (context.Context, []byte, error) {
	if t.env.GetSchedulerClient() == nil {
		return nil, nil, status.FailedPreconditionError("Scheduler client not configured")
	}
	stream, err := t.env.GetSchedulerClient().LeaseTask(ctx)
	if err != nil {
		return nil, nil, err
	}
	t.stream = newLeaseStream(stream)
	serializedTask, err := t.pingServer(ctx)
	if err == nil {
		defer t.keepLease(ctx)
		log.CtxDebugf(ctx, "Worker leased task: %q", t.taskID)
	}
	ctx, cancel := context.WithCancel(ctx)
	t.cancelFunc = cancel
	return ctx, serializedTask, err
}

func (t *TaskLease) closed() bool {
	select {
	case <-t.quit:
		return true
	default:
		return false
	}
}

func (t *TaskLease) Close(ctx context.Context, taskErr error, retry bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	log.CtxDebugf(ctx, "Closing task lease")
	if t.closed() {
		log.CtxInfof(ctx, "Lease was already closed. Short-circuiting.")
		return
	}
	close(t.quit) // This cancels our lease-keep-alive background goroutine.

	req := &scpb.LeaseTaskRequest{
		ExecutorId: t.executorID,
		TaskId:     t.taskID,
	}

	// We can finalize the task if the execution was successful, or if it failed
	// and we're not going to retry it.
	// Otherwise, we should let the scheduler know that the task needs to be
	// retried.
	if taskErr == nil || !retry {
		req.Finalize = true
	} else {
		req.ReEnqueue = true
		s, _ := gstatus.FromError(taskErr)
		req.ReEnqueueReason = s.Proto()
	}
	if err := t.stream.send(req); err != nil {
		log.CtxWarningf(ctx, "Failed to send final message on task lease stream: %s", err)
	}
	closedCleanly := false
	for {
		rsp, err := t.stream.nextResponse()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.CtxWarningf(ctx, "Lease stream recv failed: %s", err)
			break
		}
		closedCleanly = rsp.GetClosedCleanly()
	}

	if !closedCleanly {
		log.CtxWarningf(ctx, "Lease did not close cleanly; re-enqueueing task")
		reason := ""
		if taskErr != nil {
			reason = taskErr.Error()
		}
		ctx := context.Background()
		if jwt := t.ctx.Value(authutil.ContextTokenStringKey); jwt != nil {
			ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
		}
		if err := t.reEnqueueTask(ctx, reason); err != nil {
			log.CtxWarningf(ctx, "Error re-enqueueing task: %s", err)
		} else {
			log.CtxInfof(ctx, "Successfully re-enqueued task")
		}
	} else {
		log.CtxDebugf(ctx, "Task lease closed cleanly")
	}
}

// leaseStream receives between keepalives so the executor can detect a dropped
// stream and attempt to reconnect immediately. Waiting for the next keepalive
// could miss the scheduler's short reconnection window.
//
// The background receiver buffers replies for nextResponse and closes done
// when the stream ends, waking keepLease to handle the error.
type leaseStream struct {
	stream scpb.Scheduler_LeaseTaskClient
	// Buffer the single reply per request so the receiver can read the stream's
	// terminal status even before the caller consumes the reply. The receiver
	// closes rsps when the stream ends.
	rsps chan *scpb.LeaseTaskResponse
	// The receiver sets err before closing done and rsps. Readers must wait for
	// one of them to close before accessing err.
	done chan struct{}
	err  error
}

func newLeaseStream(stream scpb.Scheduler_LeaseTaskClient) *leaseStream {
	s := &leaseStream{
		stream: stream,
		rsps:   make(chan *scpb.LeaseTaskResponse, 1),
		done:   make(chan struct{}),
	}
	go s.receive()
	return s
}

func (s *leaseStream) receive() {
	for {
		rsp, err := s.stream.Recv()
		if err != nil {
			s.err = err
			close(s.done)
			close(s.rsps)
			return
		}
		s.rsps <- rsp
	}
}

func (s *leaseStream) send(req *scpb.LeaseTaskRequest) error {
	return s.stream.Send(req)
}

// nextResponse returns the next reply from the background receiver, or the
// stream's error once the stream has ended and all replies have been returned.
func (s *leaseStream) nextResponse() (*scpb.LeaseTaskResponse, error) {
	rsp, ok := <-s.rsps
	if !ok {
		return nil, s.err
	}
	return rsp, nil
}
