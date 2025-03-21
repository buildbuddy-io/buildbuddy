package task_leaser

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

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
	env        environment.Env
	executorID string
}

func NewTaskLeaser(env environment.Env, executorID string) *TaskLeaser {
	return &TaskLeaser{
		env:        env,
		executorID: executorID,
	}
}

func (t *TaskLeaser) Lease(ctx context.Context, taskID string) (interfaces.TaskLease, error) {
	lease := &TaskLease{
		env:        t.env,
		executorID: t.executorID,
		taskID:     taskID,
		execTask:   &repb.ExecutionTask{},
		quit:       make(chan struct{}),
		ttl:        100 * time.Second,
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
	env        environment.Env
	executorID string
	taskID     string

	ctx               context.Context
	execTask          *repb.ExecutionTask
	leaseID           string
	supportsReconnect bool
	quit              chan struct{}
	mu                sync.Mutex // protects stream
	stream            scpb.Scheduler_LeaseTaskClient
	ttl               time.Duration
	cancelFunc        context.CancelFunc
}

func (t *TaskLease) Context() context.Context {
	return t.ctx
}

func (t *TaskLease) Task() *repb.ExecutionTask {
	return t.execTask
}

func (t *TaskLease) sendRequest(req *scpb.LeaseTaskRequest) (*scpb.LeaseTaskResponse, error) {
	if err := t.stream.Send(req); err != nil {
		return nil, err
	}
	return t.stream.Recv()
}

func (t *TaskLease) pingServer(ctx context.Context) (b []byte, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed() {
		// Don't try to send keepalive pings after Close() was called.
		return nil, nil
	}

	req := &scpb.LeaseTaskRequest{
		ExecutorId:        t.executorID,
		TaskId:            t.taskID,
		SupportsReconnect: *enableReconnect,
	}
	if t.supportsReconnect {
		req.ReconnectToken = t.leaseID
	}
	var rsp *scpb.LeaseTaskResponse
	var r *retry.Retry
	for {
		var err error
		rsp, err = t.sendRequest(req)
		if err == nil {
			break
		}
		if !*enableReconnect || !status.IsUnavailableError(err) {
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
		t.stream = stream
		if r == nil {
			ctx, cancel := context.WithTimeout(ctx, reconnectTimeout)
			defer cancel()
			r = retry.DefaultWithContext(ctx)
		}
		if !r.Next() {
			return nil, originalErr
		}
	}
	t.supportsReconnect = rsp.GetSupportsReconnect() || rsp.GetReconnectToken() != ""
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
	if apiKey := executor_auth.APIKey(); apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	}
	_, err := t.env.GetSchedulerClient().ReEnqueueTask(ctx, req)
	return err
}

func (t *TaskLease) keepLease(ctx context.Context) {
	go func() {
		for {
			select {
			case <-t.quit:
				return
			case <-time.After(t.ttl):
				if _, err := t.pingServer(ctx); err != nil {
					log.CtxWarningf(ctx, "Error updating lease for task: %q: %s", t.taskID, err.Error())
					t.cancelFunc()
					return
				}
			}
		}
	}()
}

func (t *TaskLease) claim(ctx context.Context) (context.Context, []byte, error) {
	if t.env.GetSchedulerClient() == nil {
		return nil, nil, status.FailedPreconditionError("Scheduler client not configured")
	}
	leaseTaskCtx := ctx
	if apiKey := executor_auth.APIKey(); apiKey != "" {
		leaseTaskCtx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	}
	stream, err := t.env.GetSchedulerClient().LeaseTask(leaseTaskCtx)
	if err != nil {
		return nil, nil, err
	}
	t.stream = stream
	serializedTask, err := t.pingServer(leaseTaskCtx)
	if err == nil {
		defer t.keepLease(leaseTaskCtx)
		log.CtxInfof(ctx, "Worker leased task: %q", t.taskID)
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
	log.CtxInfof(ctx, "TaskLeaser %q Close() called with err: %v", t.taskID, taskErr)
	if t.closed() {
		log.CtxInfof(ctx, "TaskLeaser %q was already closed. Short-circuiting.", t.taskID)
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
	if err := t.stream.Send(req); err != nil {
		log.CtxWarningf(ctx, "Could not send request: %s", err)
	}
	closedCleanly := false
	for {
		rsp, err := t.stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.CtxWarningf(ctx, "TaskLeaser %q: got non-EOF err: %s", t.taskID, err)
			break
		}
		closedCleanly = rsp.GetClosedCleanly()
	}

	if !closedCleanly {
		log.CtxWarningf(ctx, "TaskLeaser %q: did not close cleanly but should have. Will re-enqueue.", t.taskID)
		reason := ""
		if taskErr != nil {
			reason = taskErr.Error()
		}
		if err := t.reEnqueueTask(context.Background(), reason); err != nil {
			log.CtxWarningf(ctx, "TaskLeaser %q: error re-enqueueing task: %s", t.taskID, err.Error())
		} else {
			log.CtxInfof(ctx, "TaskLeaser %q: Successfully re-enqueued.", t.taskID)
		}
	} else {
		log.CtxInfof(ctx, "TaskLeaser %q: closed cleanly :)", t.taskID)
	}
}
