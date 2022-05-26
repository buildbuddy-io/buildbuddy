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
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var apiKey = flag.String("executor.api_key", "", "API Key used to authorize the executor with the BuildBuddy app server.")

type TaskLeaser struct {
	env        environment.Env
	log        *log.Logger
	executorID string
	taskID     string
	quit       chan struct{}
	mu         sync.Mutex // protects stream
	stream     scpb.Scheduler_LeaseTaskClient
	ttl        time.Duration
	closed     bool
	cancelFunc context.CancelFunc
}

func NewTaskLeaser(env environment.Env, executorID string, taskID string) *TaskLeaser {
	sublog := log.NamedSubLogger(executorID)
	return &TaskLeaser{
		env:        env,
		log:        &sublog,
		executorID: executorID,
		taskID:     taskID,
		quit:       make(chan struct{}),
		ttl:        100 * time.Second,
		closed:     true, // set to false in Claim.
	}
}

func (t *TaskLeaser) pingServer() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	req := &scpb.LeaseTaskRequest{
		ExecutorId: t.executorID,
		TaskId:     t.taskID,
	}
	if err := t.stream.Send(req); err != nil {
		return nil, err
	}
	rsp, err := t.stream.Recv()
	if err != nil {
		return nil, err
	}
	t.ttl = time.Duration(rsp.GetLeaseDurationSeconds()) * time.Second
	return rsp.GetSerializedTask(), nil
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
	if *apiKey != "" {
		leaseTaskCtx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, *apiKey)
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

func (t *TaskLeaser) Close(taskErr error, retry bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.log.Infof("TaskLeaser %q Close() called with err: %v", t.taskID, taskErr)
	if t.closed {
		t.log.Infof("TaskLeaser %q was already closed. Short-circuiting.", t.taskID)
	}
	close(t.quit) // This cancels our lease-keep-alive background goroutine.

	req := &scpb.LeaseTaskRequest{
		ExecutorId: t.executorID,
		TaskId:     t.taskID,
	}

	shouldReEnqueue := false

	// We can finalize the task if the execution was successful, or if it failed and we're not going to retry it.
	// Otherwise, we should release the lease without finalizing the task so that it can be retried.
	if taskErr == nil || !retry {
		req.Finalize = true
	} else {
		req.Release = true
		shouldReEnqueue = true
	}
	if err := t.stream.Send(req); err != nil {
		log.Warningf("Could not send request: %s", err)
	}
	closedCleanly := false
	for {
		rsp, err := t.stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.log.Warningf("TaskLeaser %q: got non-EOF err: %s", t.taskID, err)
			break
		}
		closedCleanly = rsp.GetClosedCleanly()
	}

	if !closedCleanly {
		t.log.Warningf("TaskLeaser %q: did not close cleanly but should have. Will re-enqueue.", t.taskID)
		shouldReEnqueue = true
	}

	if shouldReEnqueue {
		reason := ""
		if taskErr != nil {
			reason = taskErr.Error()
		}
		if err := t.reEnqueueTask(context.Background(), reason); err != nil {
			t.log.Warningf("TaskLeaser %q: error re-enqueueing task: %s", t.taskID, err.Error())
		} else {
			t.log.Infof("TaskLeaser %q: Successfully re-enqueued.", t.taskID)
		}
	} else {
		t.log.Infof("TaskLeaser %q: closed cleanly :)", t.taskID)
	}

	t.closed = true
}

func APIKey() string {
	return *apiKey
}
