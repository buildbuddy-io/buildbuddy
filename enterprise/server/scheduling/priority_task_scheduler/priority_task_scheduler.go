package priority_task_scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_queue"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_leaser"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	queueCheckSleepInterval = 10 * time.Millisecond
)

var shuttingDownLogOnce sync.Once

type PriorityTaskScheduler struct {
	env           environment.Env
	log           log.Logger
	shuttingDown  bool
	pq            *priority_queue.PriorityQueue
	exec          *executor.Executor
	newTaskSignal chan struct{}

	mu                    sync.Mutex
	activeTaskCancelFuncs map[*context.CancelFunc]struct{}
	resourceTracker       interfaces.ResourceTracker
}

func NewPriorityTaskScheduler(env environment.Env, exec *executor.Executor) *PriorityTaskScheduler {
	sublog := log.NamedSubLogger(exec.Name())

	qes := &PriorityTaskScheduler{
		env:                   env,
		log:                   sublog,
		pq:                    priority_queue.NewPriorityQueue(),
		exec:                  exec,
		newTaskSignal:         make(chan struct{}, 1),
		activeTaskCancelFuncs: make(map[*context.CancelFunc]struct{}, 0),
		resourceTracker:       env.GetResourceTracker(),
		shuttingDown:          false,
	}
	// This func ensures that we don't attempt to "claim" work that was enqueued
	// but not started yet, ensuring that someone else will have a chance to get it.
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		qes.mu.Lock()
		defer qes.mu.Unlock()
		qes.shuttingDown = true
		log.Debug("PriorityTaskScheduler received shutdown signal")
		return nil
	})

	// This func ensures that we finish tasks we've already claimed before shutting
	// down -- or at least we try to until we hit the shutdown timeout.
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		log.Debug("PriorityTaskScheduler received shutdown signal, waiting for cancels")
		for {
			qes.mu.Lock()
			activeTasks := len(qes.activeTaskCancelFuncs)
			qes.mu.Unlock()
			if activeTasks == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		log.Debug("PriorityTaskScheduler all cancels finished!")
		return nil
	})

	return qes
}

func (q *PriorityTaskScheduler) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	q.pq.Push(req)
	q.log.Infof("Added task %+v to pq.", req)
	q.newTaskSignal <- struct{}{}
	return &scpb.EnqueueTaskReservationResponse{}, nil
}

func propagateExecutionTaskValuesToContext(ctx context.Context, execTask *repb.ExecutionTask) context.Context {
	ctx = context.WithValue(ctx, "x-buildbuddy-jwt", execTask.GetJwt())
	rmd := &repb.RequestMetadata{
		ToolInvocationId: execTask.GetInvocationId(),
	}
	if data, err := proto.Marshal(rmd); err == nil {
		ctx = context.WithValue(ctx, "build.bazel.remote.execution.v2.requestmetadata-bin", string(data))
	}
	return ctx
}

func (q *PriorityTaskScheduler) runTask(ctx context.Context, execTask *repb.ExecutionTask) error {
	if q.env.GetRemoteExecutionClient() == nil {
		return status.FailedPreconditionError("Execution client not configured")
	}

	ctx = propagateExecutionTaskValuesToContext(ctx, execTask)
	clientStream, err := q.env.GetRemoteExecutionClient().PublishOperation(ctx)
	if err != nil {
		q.log.Warningf("Error opening publish operation stream: %s", err)
		return err
	}
	if err := q.exec.ExecuteTaskAndStreamResults(execTask, clientStream); err != nil {
		q.log.Warningf("ExecuteTaskAndStreamResults error %q: %s", execTask.GetExecutionId(), err.Error())
		return err
	}
	_, err = clientStream.CloseAndRecv()
	return err
}

func (q *PriorityTaskScheduler) trackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	q.activeTaskCancelFuncs[cancel] = struct{}{}
	metrics.RemoteExecutionTasksExecuting.Set(float64(len(q.activeTaskCancelFuncs)))
}

func (q *PriorityTaskScheduler) untrackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	delete(q.activeTaskCancelFuncs, cancel)
	metrics.RemoteExecutionTasksExecuting.Set(float64(len(q.activeTaskCancelFuncs)))
}

func (q *PriorityTaskScheduler) handleTask() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Don't claim work if this machine is about to shutdown.
	if q.shuttingDown {
		shuttingDownLogOnce.Do(func() {
			q.log.Info("Stopping queue processing, machine is shutting down.")
		})
		return
	}

	qLen := q.pq.Len()
	metrics.RemoteExecutionQueueLength.Set(float64(qLen))
	if qLen == 0 {
		return
	}
	reservation := q.pq.Peek()
	if reservation == nil {
		return
	}

	res := &interfaces.Resources{
		MemoryBytes: reservation.GetTaskSize().GetEstimatedMemoryBytes(),
		MilliCPU:    reservation.GetTaskSize().GetEstimatedMilliCpu(),
	}
	// Evict from the runner pool until there are enough resources to run the
	// task.
	// TODO: Consider reducing the estimated memory for the task if it can be
	// guaranteed to run on an existing paused runner, since most of the
	// task's data is probably already available in the runner's memory.
	for !q.resourceTracker.Request(res) {
		didEvict := q.exec.RunnerPool().TryEvict()
		if !didEvict {
			return
		}
	}

	// This pop should always succeed since we peeked above while holding the lock.
	if reservation := q.pq.Pop(); reservation == nil {
		log.Errorf("Bad state: failed to pop reservation from the queue.")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	q.trackTask(reservation, &cancel)

	go func() {
		defer cancel()
		defer func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			q.untrackTask(reservation, &cancel)
			q.resourceTracker.Return(res)
		}()

		taskLease := task_leaser.NewTaskLeaser(q.env, q.exec.Name(), reservation.GetTaskId())
		ctx, serializedTask, err := taskLease.Claim(ctx)
		if err != nil {
			// NotFound means the task is already claimed.
			if gstatus.Code(err) != gcodes.NotFound {
				q.log.Warningf("Error leasing task %q: %s", reservation.GetTaskId(), err.Error())
			}
			return
		}

		execTask := &repb.ExecutionTask{}
		if err := proto.Unmarshal(serializedTask, execTask); err != nil {
			q.log.Errorf("error unmarshalling task %q: %s", reservation.GetTaskId(), err.Error())
			taskLease.Close(nil)
			return
		}
		err = q.runTask(ctx, execTask)
		if err != nil {
			q.log.Errorf("Error running task %q: %s", reservation.GetTaskId(), err.Error())
		}
		// err can be nil and that's ok! Task will be retried if it's not.
		taskLease.Close(err)
	}()
}

func (q *PriorityTaskScheduler) Start() error {
	go func() {
		for {
			select {
			case _ = <-q.newTaskSignal:
				q.handleTask()
			case _ = <-time.After(queueCheckSleepInterval):
				q.handleTask()
			}
		}
	}()
	return nil
}

func (q *PriorityTaskScheduler) Stop() error {
	return nil
}
