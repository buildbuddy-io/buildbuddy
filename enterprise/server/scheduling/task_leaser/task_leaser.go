package task_leaser

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

type TaskLeaser struct {
	env        environment.Env
	log        *log.Logger
	taskID     string
	quit       chan struct{}
	mu         sync.Mutex // protects stream
	stream     scpb.Scheduler_LeaseTaskClient
	ttl        time.Duration
	closed     bool
	cancelFunc context.CancelFunc
}

func NewTaskLeaser(env environment.Env, executorName string, taskID string) *TaskLeaser {
	sublog := log.NamedSubLogger(executorName)
	return &TaskLeaser{
		env:    env,
		log:    &sublog,
		taskID: taskID,
		quit:   make(chan struct{}),
		ttl:    100 * time.Second,
		closed: true, // set to false in Claim.
	}
}

func (t *TaskLeaser) pingServer() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	req := &scpb.LeaseTaskRequest{
		TaskId: t.taskID,
	}
	t.log.Debugf("TaskLeaser ping-SEND %q, req: %+v", t.taskID, req)
	if err := t.stream.Send(req); err != nil {
		return nil, err
	}
	rsp, err := t.stream.Recv()
	t.log.Debugf("TaskLeaser ping-RECV %q, req: %+v,  err: %v", t.taskID, rsp, err)
	if err != nil {
		return nil, err
	}
	t.ttl = time.Duration(rsp.GetLeaseDurationSeconds()) * time.Second
	return rsp.GetSerializedTask(), nil
}

func (t *TaskLeaser) reEnqueueTask(ctx context.Context) error {
	req := &scpb.ReEnqueueTaskRequest{
		TaskId: t.taskID,
	}
	if apiKey := t.env.GetConfigurator().GetExecutorConfig().APIKey; apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, apiKey)
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
				if _, err := t.pingServer(); err != nil {
					t.log.Warningf("Error updating lease for task: %q: %s", t.taskID, err.Error())
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
	if apiKey := t.env.GetConfigurator().GetExecutorConfig().APIKey; apiKey != "" {
		leaseTaskCtx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, apiKey)
	}
	stream, err := t.env.GetSchedulerClient().LeaseTask(leaseTaskCtx)
	if err != nil {
		return nil, nil, err
	}
	t.stream = stream
	serializedTask, err := t.pingServer()
	if err == nil {
		t.closed = false
		defer t.keepLease(ctx)
		t.log.Infof("Worker leased task: %q", t.taskID)
	}
	ctx, cancel := context.WithCancel(ctx)
	t.cancelFunc = cancel
	return ctx, serializedTask, err
}

func isBazelRetryableError(taskError error) bool {
	if gstatus.Code(taskError) == gcodes.ResourceExhausted {
		return true
	}
	if gstatus.Code(taskError) == gcodes.FailedPrecondition {
		if len(gstatus.Convert(taskError).Details()) > 0 {
			return true
		}
	}
	return false
}

func (t *TaskLeaser) Close(taskErr error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.log.Infof("TaskLeaser %q Close() called with err: %v", t.taskID, taskErr)
	if t.closed {
		t.log.Infof("TaskLeaser %q was already closed. Short-circuiting.", t.taskID)
		return nil
	}
	close(t.quit) // This cancels our lease-keep-alive background goroutine.

	closeLeaseCleanly := taskErr == nil || isBazelRetryableError(taskErr)
	closedCleanly := false

	if closeLeaseCleanly {
		req := &scpb.LeaseTaskRequest{
			TaskId:   t.taskID,
			Finalize: true,
		}
		t.log.Debugf("TaskLeaser close-SEND %q, req: %+v", t.taskID, req)
		t.stream.Send(req)
		for {
			rsp, err := t.stream.Recv()
			t.log.Debugf("TaskLeaser close-RECV %q, req: %+v, err: %v", t.taskID, rsp, err)
			if err == io.EOF {
				t.log.Debugf("TaskLeaser %q: Got EOF from lease stream.", t.taskID)
				break
			}
			if err != nil {
				t.log.Warningf("TaskLeaser %q: got non-EOF err: %s", t.taskID, err)
				break
			}
			closedCleanly = rsp.GetClosedCleanly()
		}
	}

	if closeLeaseCleanly && !closedCleanly {
		t.log.Warningf("TaskLeaser %q: did not close cleanly but should have. Will re-enqueue.", t.taskID)
	}

	if !closedCleanly {
		if err := t.reEnqueueTask(context.Background()); err != nil {
			t.log.Warningf("TaskLeaser %q: error re-enqueueing task: %s", t.taskID, err.Error())
		} else {
			t.log.Infof("TaskLeaser %q: Successfully re-enqueued.", t.taskID)
		}
	} else {
		t.log.Infof("TaskLeaser %q: closed cleanly :)", t.taskID)
	}

	t.closed = true
	return taskErr
}
