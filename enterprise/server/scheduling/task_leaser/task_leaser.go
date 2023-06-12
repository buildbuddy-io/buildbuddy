package task_leaser

import (
	"context"
	"flag"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
)

var apiKey = flag.String("executor.api_key", "", "API Key used to authorize the executor with the BuildBuddy app server.")

type TaskLeaser struct {
	env        environment.Env
	executorID string
	taskID     string
	leaseID    string
	quit       chan struct{}
	mu         sync.Mutex // protects stream
	stream     scpb.Scheduler_LeaseTaskClient
	ttl        time.Duration
	closed     bool
	cancelFunc context.CancelFunc
}

func NewTaskLeaser(env environment.Env, executorID string, taskID string) *TaskLeaser {
	return &TaskLeaser{
		env:        env,
		executorID: executorID,
		taskID:     taskID,
		quit:       make(chan struct{}),
		ttl:        100 * time.Second,
		closed:     true, // set to false in Claim.
	}
}

func (t *TaskLeaser) pingServer(ctx context.Context) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	req := &scpb.LeaseTaskRequest{
		ExecutorId: t.executorID,
		TaskId:     t.taskID,
	}
	rsp, err := t.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	if rsp.GetTransferToken() != "" {
		log.CtxInfof(ctx, "App is shutting down; transferring lease to new app.")
		if rsp, err = t.transferLease(ctx, rsp.GetTransferToken()); err != nil {
			return nil, status.WrapError(err, "failed to transfer lease")
		}
		log.CtxInfof(ctx, "Successfully transferred lease.")
	}
	t.ttl = time.Duration(rsp.GetLeaseDurationSeconds()) * time.Second
	return rsp.GetSerializedTask(), nil
}

// transferLease attempts to transfer the lease stream to another scheduler
// instance.
func (t *TaskLeaser) transferLease(ctx context.Context, transferToken string) (*scpb.LeaseTaskResponse, error) {
	oldStream := t.stream
	defer oldStream.CloseSend()

	newStream, err := t.env.GetSchedulerClient().LeaseTask(ctx)
	if err != nil {
		return nil, err
	}
	t.stream = newStream
	req := &scpb.LeaseTaskRequest{
		ExecutorId:    t.executorID,
		TaskId:        t.taskID,
		TransferToken: transferToken,
	}
	rsp, err := t.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (t *TaskLeaser) sendRequest(ctx context.Context, req *scpb.LeaseTaskRequest) (*scpb.LeaseTaskResponse, error) {
	if err := t.stream.Send(req); err != nil {
		return nil, status.WrapError(err, "failed to send task lease request")
	}
	rsp, err := t.stream.Recv()
	if err != nil {
		return nil, status.WrapError(err, "failed to receive task lease response")
	}
	return rsp, nil
}

// sendRequest attempts to send req to the server and waits to receive a
// response. It retries upon disconnect.
// func (t *TaskLeaser) sendRequest(ctx context.Context, req *scpb.LeaseTaskRequest) (*scpb.LeaseTaskResponse, error) {
// 	if req.Finalize || req.ReEnqueue {
// 		// TODO: make Finalize/ReEnqueue idempotent, and allow retrying these
// 		// too.
// 		return nil, status.InternalErrorf("retry for Finalize/ReEnqueue is not supported")
// 	}
// 	// Retry send and recv until both succeed, or until the lease expires.
// 	ctx, cancel := context.WithTimeout(ctx, t.ttl)
// 	defer cancel()
// 	r := retry.DefaultWithContext(ctx)
// 	for r.Next() {
// 		err := t.stream.Send(req)
// 		if err != nil && err != io.EOF {
// 			return nil, status.WrapError(err, "failed to send task lease request")
// 		}
// 		var res *scpb.LeaseTaskResponse
// 		if err == nil {
// 			res, err = t.stream.Recv()
// 			if err != nil && err != io.EOF {
// 				return nil, status.WrapError(err, "failed to recv task lease response")
// 			}
// 		}
// 		if err == nil {
// 			return res, nil
// 		}
// 		if t.leaseID == "" {
// 			// Reconnect is not supported by the app; do nothing.
// 			return nil, err
// 		}
// 		// At this point, either send or recv returned EOF. This means the
// 		// stream disconnected unexpectedly. So, we should start a new stream
// 		// and retry the request.
// 		log.CtxInfo(ctx, "Lost task lease connection; attempting to reconnect.")
// 		if err := t.reconnect(ctx); err != nil {
// 			return nil, status.WrapError(err, "failed to reconnect LeaseTask stream")
// 		}
// 		continue // attempt retry
// 	}
// 	if err := ctx.Err(); err != nil {
// 		// Timed out.
// 		return nil, err
// 	}
// 	// Should never happen since we should keep retrying until timeout.
// 	return nil, status.UnknownError("unknown error sending task lease request")
// }

func (t *TaskLeaser) reconnect(ctx context.Context) error {
	r := retry.DefaultWithContext(ctx)
	var lastErr error
	for r.Next() {
		stream, err := t.env.GetSchedulerClient().LeaseTask(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		t.stream = stream
		return nil
	}
	return lastErr
}

func (t *TaskLeaser) reEnqueueTask(ctx context.Context, reason string) error {
	req := &scpb.ReEnqueueTaskRequest{
		TaskId: t.taskID,
		Reason: reason,
	}
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, *apiKey)
	}
	_, err := t.env.GetSchedulerClient().ReEnqueueTask(ctx, req)
	return err
}

func (t *TaskLeaser) keepLease(ctx context.Context) {
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

func (t *TaskLeaser) Claim(ctx context.Context) (context.Context, []byte, error) {
	if t.env.GetSchedulerClient() == nil {
		return nil, nil, status.FailedPreconditionError("Scheduler client not configured")
	}
	leaseTaskCtx := ctx
	if *apiKey != "" {
		leaseTaskCtx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, *apiKey)
	}
	stream, err := t.env.GetSchedulerClient().LeaseTask(leaseTaskCtx)
	if err != nil {
		return nil, nil, err
	}
	t.stream = stream
	serializedTask, err := t.pingServer(ctx)
	if err == nil {
		t.closed = false
		defer t.keepLease(ctx)
		log.CtxInfof(ctx, "Worker leased task: %q", t.taskID)
	}
	ctx, cancel := context.WithCancel(ctx)
	t.cancelFunc = cancel
	return ctx, serializedTask, err
}

func (t *TaskLeaser) Close(ctx context.Context, taskErr error, retry bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	log.CtxInfof(ctx, "TaskLeaser %q Close() called with err: %v", t.taskID, taskErr)
	if t.closed {
		log.CtxInfof(ctx, "TaskLeaser %q was already closed. Short-circuiting.", t.taskID)
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
		if taskErr != nil {
			s, _ := gstatus.FromError(taskErr)
			req.ReEnqueueReason = s.Proto()
		}
	}
	// TODO(bduffany): ensure that Finalize/ReEnqueue are idempotent so that we
	// can safely retry if the stream connection is broken.
	if err := t.stream.Send(req); err != nil {
		log.CtxWarningf(ctx, "Task leaser: failed to send on stream (re-enqueue=%t): %s", req.ReEnqueue, err)
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

	t.closed = true
}

func APIKey() string {
	return *apiKey
}
