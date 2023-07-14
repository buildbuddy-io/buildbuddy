package priority_task_scheduler

import (
	"container/list"
	"context"
	"flag"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_queue"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_leaser"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	exclusiveTaskScheduling = flag.Bool("executor.exclusive_task_scheduling", false, "If true, only one task will be scheduled at a time. Default is false")
	shutdownCleanupDuration = flag.Duration("executor.shutdown_cleanup_duration", 15*time.Second, "The minimum duration during the shutdown window to allocate for cleaning up containers. This is capped to the value of `max_shutdown_duration`.")
)

var shuttingDownLogOnce sync.Once

type groupPriorityQueue struct {
	*priority_queue.PriorityQueue
	groupID string
}

type taskQueue struct {
	// List of *groupPriorityQueue items.
	pqs *list.List
	// Map to allow quick lookup of a specific *groupPriorityQueue element in the pqs list.
	pqByGroupID map[string]*list.Element
	// The *groupPriorityQueue element from which the next task will be obtained.
	// Will be nil when there are no tasks remaining.
	currentPQ *list.Element
	// Number of tasks across all queues.
	numTasks int
}

func newTaskQueue() *taskQueue {
	return &taskQueue{
		pqs:         list.New(),
		pqByGroupID: make(map[string]*list.Element),
		currentPQ:   nil,
	}
}

func (t *taskQueue) GetAll() []*scpb.EnqueueTaskReservationRequest {
	var reservations []*scpb.EnqueueTaskReservationRequest

	for e := t.pqs.Front(); e != nil; e = e.Next() {
		pq, ok := e.Value.(*groupPriorityQueue)
		if !ok {
			log.Error("not a *groupPriorityQueue!??!")
			continue
		}

		reservations = append(reservations, pq.GetAll()...)
	}

	return reservations
}

func (t *taskQueue) Enqueue(req *scpb.EnqueueTaskReservationRequest) {
	taskGroupID := req.GetSchedulingMetadata().GetTaskGroupId()
	var pq *groupPriorityQueue
	if el, ok := t.pqByGroupID[taskGroupID]; ok {
		pq, ok = el.Value.(*groupPriorityQueue)
		if !ok {
			// Why would this ever happen?
			log.Error("not a *groupPriorityQueue!??!")
			return
		}
	} else {
		pq = &groupPriorityQueue{
			PriorityQueue: priority_queue.NewPriorityQueue(),
			groupID:       taskGroupID,
		}
		el := t.pqs.PushBack(pq)
		t.pqByGroupID[taskGroupID] = el
		if t.currentPQ == nil {
			t.currentPQ = el
		}
	}
	pq.Push(req)
	t.numTasks++
	metrics.RemoteExecutionQueueLength.With(prometheus.Labels{metrics.GroupID: taskGroupID}).Set(float64(pq.Len()))
}

func (t *taskQueue) Dequeue() *scpb.EnqueueTaskReservationRequest {
	if t.currentPQ == nil {
		return nil
	}
	pqEl := t.currentPQ
	pq, ok := pqEl.Value.(*groupPriorityQueue)
	if !ok {
		// Why would this ever happen?
		log.Error("not a *groupPriorityQueue!??!")
		return nil
	}
	req := pq.Pop()

	t.currentPQ = t.currentPQ.Next()
	if pq.Len() == 0 {
		t.pqs.Remove(pqEl)
		delete(t.pqByGroupID, pq.groupID)
	}
	if t.currentPQ == nil {
		t.currentPQ = t.pqs.Front()
	}
	t.numTasks--
	metrics.RemoteExecutionQueueLength.With(prometheus.Labels{metrics.GroupID: req.GetSchedulingMetadata().GetTaskGroupId()}).Set(float64(pq.Len()))
	return req
}

func (t *taskQueue) Peek() *scpb.EnqueueTaskReservationRequest {
	if t.currentPQ == nil {
		return nil
	}
	pq, ok := t.currentPQ.Value.(*groupPriorityQueue)
	if !ok {
		// Why would this ever happen?
		log.Error("not a *groupPriorityQueue!??!")
		return nil
	}
	return pq.Peek()
}

func (t *taskQueue) Len() int {
	return t.numTasks
}

type Options struct {
	RAMBytesCapacityOverride  int64
	CPUMillisCapacityOverride int64
}

type PriorityTaskScheduler struct {
	env              environment.Env
	log              log.Logger
	shuttingDown     bool
	exec             *executor.Executor
	runnerPool       interfaces.RunnerPool
	checkQueueSignal chan struct{}
	rootContext      context.Context
	rootCancel       context.CancelFunc

	mu                      sync.Mutex
	q                       *taskQueue
	activeTaskCancelFuncs   map[*context.CancelFunc]struct{}
	ramBytesCapacity        int64
	ramBytesUsed            int64
	cpuMillisCapacity       int64
	cpuMillisUsed           int64
	exclusiveTaskScheduling bool
}

func NewPriorityTaskScheduler(env environment.Env, exec *executor.Executor, runnerPool interfaces.RunnerPool, options *Options) *PriorityTaskScheduler {
	ramBytesCapacity := options.RAMBytesCapacityOverride
	if ramBytesCapacity == 0 {
		ramBytesCapacity = int64(float64(resources.GetAllocatedRAMBytes()) * tasksize.MaxResourceCapacityRatio)
	}
	cpuMillisCapacity := options.CPUMillisCapacityOverride
	if cpuMillisCapacity == 0 {
		cpuMillisCapacity = int64(float64(resources.GetAllocatedCPUMillis()) * tasksize.MaxResourceCapacityRatio)
	}

	rootContext, rootCancel := context.WithCancel(context.Background())
	rootContext = log.EnrichContext(rootContext, "executor_host_id", exec.HostID())
	rootContext = log.EnrichContext(rootContext, "executor_id", exec.ID())

	qes := &PriorityTaskScheduler{
		env:                     env,
		q:                       newTaskQueue(),
		exec:                    exec,
		runnerPool:              runnerPool,
		checkQueueSignal:        make(chan struct{}, 64),
		rootContext:             rootContext,
		rootCancel:              rootCancel,
		activeTaskCancelFuncs:   make(map[*context.CancelFunc]struct{}, 0),
		shuttingDown:            false,
		ramBytesCapacity:        ramBytesCapacity,
		cpuMillisCapacity:       cpuMillisCapacity,
		exclusiveTaskScheduling: *exclusiveTaskScheduling,
	}

	env.GetHealthChecker().RegisterShutdownFunction(qes.Shutdown)
	return qes
}

// Shutdown ensures that we don't attempt to claim work that was enqueued but
// not started yet, allowing another executor a chance to complete it. This is
// client-side "graceful" stop -- we stop processing queued work as soon as we
// receive a shutdown signal, but permit in-progress work to continue, up until
// just before the shutdown timeout, at which point we hard-cancel it.
func (q *PriorityTaskScheduler) Shutdown(ctx context.Context) error {
	log.Debug("PriorityTaskScheduler received shutdown signal")
	q.mu.Lock()
	q.shuttingDown = true
	q.mu.Unlock()

	// Compute a deadline that is 1 second before our hard-kill
	// deadline: that is when we'll cancel our own root context.
	deadline, ok := ctx.Deadline()
	if !ok {
		alert.UnexpectedEvent("no_deadline_on_shutdownfunc_context")
		q.rootCancel()
	}
	delay := deadline.Sub(time.Now()) - time.Second
	if runner.ContextBasedShutdownEnabled() {
		// Cancel all tasks early enough to allow containers and workspaces to be
		// cleaned up.
		delay = deadline.Sub(time.Now()) - *shutdownCleanupDuration
	}
	ctx, cancel := context.WithTimeout(ctx, delay)
	defer cancel()

	// Start a goroutine that will:
	//   - log success on graceful shutdown
	//   - cancel root context after delay has passed
	go func() {
		select {
		case <-ctx.Done():
			log.Infof("Graceful stop of executor succeeded.")
		case <-time.After(delay):
			log.Warningf("Hard-stopping executor!")
			q.rootCancel()
		}
	}()

	// Wait for all active tasks to finish.
	for {
		q.mu.Lock()
		activeTasks := len(q.activeTaskCancelFuncs)
		q.mu.Unlock()
		if activeTasks == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Since all tasks have finished, no new runners can be created, and it is now
	// safe to wait for all pending cleanup jobs to finish.
	q.runnerPool.Wait()

	return nil
}

func (q *PriorityTaskScheduler) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	ctx = log.EnrichContext(ctx, log.ExecutionIDKey, req.GetTaskId())

	if req.GetTaskSize().GetEstimatedMemoryBytes() > q.ramBytesCapacity ||
		req.GetTaskSize().GetEstimatedMilliCpu() > q.cpuMillisCapacity {
		// TODO(bduffany): Return an error here instead. Currently we cannot
		// return an error because it causes the executor to disconnect and
		// reconnect to the scheduler, and the scheduler will keep attempting to
		// re-enqueue the oversized task onto this executor once reconnected.
		log.CtxErrorf(ctx,
			"Task exceeds executor capacity: requires %d bytes memory of %d available and %d milliCPU of %d available",
			req.GetTaskSize().GetEstimatedMemoryBytes(), q.ramBytesCapacity,
			req.GetTaskSize().GetEstimatedMilliCpu(), q.cpuMillisCapacity,
		)
	}

	q.mu.Lock()
	q.q.Enqueue(req)
	q.mu.Unlock()
	log.CtxInfof(ctx, "Added task %+v to pq.", req)
	// Wake up the scheduling loop so that it can run the task if there are
	// enough resources available.
	q.checkQueueSignal <- struct{}{}
	return &scpb.EnqueueTaskReservationResponse{}, nil
}

func (q *PriorityTaskScheduler) propagateExecutionTaskValuesToContext(ctx context.Context, execTask *repb.ExecutionTask) context.Context {
	if execTask.GetJwt() != "" {
		ctx = context.WithValue(ctx, "x-buildbuddy-jwt", execTask.GetJwt())
	}
	rmd := execTask.GetRequestMetadata()
	if rmd == nil {
		rmd = &repb.RequestMetadata{ToolInvocationId: execTask.GetInvocationId()}
	}
	rmd = proto.Clone(rmd).(*repb.RequestMetadata)
	rmd.ExecutorDetails = &repb.ExecutorDetails{ExecutorHostId: q.exec.HostID()}
	if data, err := proto.Marshal(rmd); err == nil {
		ctx = context.WithValue(ctx, bazel_request.RequestMetadataKey, string(data))
	}
	return ctx
}

func (q *PriorityTaskScheduler) runTask(ctx context.Context, st *repb.ScheduledTask) (retry bool, err error) {
	if q.env.GetRemoteExecutionClient() == nil {
		return false, status.FailedPreconditionError("Execution client not configured")
	}

	execTask := st.ExecutionTask
	ctx = q.propagateExecutionTaskValuesToContext(ctx, execTask)
	clientStream, err := q.env.GetRemoteExecutionClient().PublishOperation(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Error opening publish operation stream: %s", err)
		return true, err
	}
	start := time.Now()
	// TODO(http://go/b/1192): Figure out why CloseAndRecv() hangs if we call
	// it too soon after establishing the clientStream, and remove this delay.
	const closeStreamDelay = 10 * time.Millisecond
	if retry, err := q.exec.ExecuteTaskAndStreamResults(ctx, st, clientStream); err != nil {
		log.CtxWarningf(ctx, "ExecuteTaskAndStreamResults error: %s", err)
		time.Sleep(time.Until(start.Add(closeStreamDelay)))
		_, _ = clientStream.CloseAndRecv()
		return retry, err
	}
	time.Sleep(time.Until(start.Add(closeStreamDelay)))
	if _, err = clientStream.CloseAndRecv(); err != nil {
		return true, err
	}
	return false, nil
}

func (q *PriorityTaskScheduler) trackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	q.activeTaskCancelFuncs[cancel] = struct{}{}
	if size := res.GetTaskSize(); size != nil {
		q.ramBytesUsed += size.GetEstimatedMemoryBytes()
		q.cpuMillisUsed += size.GetEstimatedMilliCpu()
		metrics.RemoteExecutionAssignedRAMBytes.Set(float64(q.ramBytesUsed))
		metrics.RemoteExecutionAssignedMilliCPU.Set(float64(q.cpuMillisUsed))
		log.CtxInfof(q.rootContext, "Claimed task resources. Queue stats: %s", q.stats())
	}
}

func (q *PriorityTaskScheduler) untrackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	delete(q.activeTaskCancelFuncs, cancel)
	if size := res.GetTaskSize(); size != nil {
		q.ramBytesUsed -= size.GetEstimatedMemoryBytes()
		q.cpuMillisUsed -= size.GetEstimatedMilliCpu()
		metrics.RemoteExecutionAssignedRAMBytes.Set(float64(q.ramBytesUsed))
		metrics.RemoteExecutionAssignedMilliCPU.Set(float64(q.cpuMillisUsed))
		log.CtxInfof(q.rootContext, "Released task resources. Queue stats: %s", q.stats())
	}
}

func (q *PriorityTaskScheduler) stats() string {
	ramBytesRemaining := q.ramBytesCapacity - q.ramBytesUsed
	cpuMillisRemaining := q.cpuMillisCapacity - q.cpuMillisUsed
	return message.NewPrinter(language.English).Sprintf(
		"Mem: %d of %d bytes allocated (%d remaining), CPU: %d of %d milliCPU allocated (%d remaining), Tasks: %d active, %d queued",
		q.ramBytesUsed, q.ramBytesCapacity, ramBytesRemaining,
		q.cpuMillisUsed, q.cpuMillisCapacity, cpuMillisRemaining,
		len(q.activeTaskCancelFuncs), q.q.Len())
}

func (q *PriorityTaskScheduler) canFitAnotherTask(res *scpb.EnqueueTaskReservationRequest) bool {
	// Only ever run as many sized tasks as we have memory for.
	knownRAMremaining := q.ramBytesCapacity - q.ramBytesUsed
	knownCPUremaining := q.cpuMillisCapacity - q.cpuMillisUsed
	willFit := knownRAMremaining >= res.GetTaskSize().GetEstimatedMemoryBytes() && knownCPUremaining >= res.GetTaskSize().GetEstimatedMilliCpu()

	// If we're running in exclusiveTaskScheduling mode, only ever allow one task to run at
	// a time. Otherwise fall through to the logic below.
	if willFit && q.exclusiveTaskScheduling && len(q.activeTaskCancelFuncs) >= 1 {
		return false
	}

	if willFit {
		if res.GetTaskSize().GetEstimatedMemoryBytes() == 0 {
			log.CtxWarningf(q.rootContext, "Scheduling another unknown size task. THIS SHOULD NOT HAPPEN! res: %+v", res)
		} else {
			log.CtxInfof(q.rootContext, "Scheduling another task of size: %+v", res.GetTaskSize())
		}
	}
	return willFit
}

func (q *PriorityTaskScheduler) handleTask() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Don't claim work if this machine is about to shutdown.
	if q.shuttingDown {
		shuttingDownLogOnce.Do(func() {
			log.CtxInfof(q.rootContext, "Stopping queue processing, machine is shutting down.")
		})
		return
	}

	qLen := q.q.Len()
	if qLen == 0 {
		return
	}
	nextTask := q.q.Peek()
	if nextTask == nil || !q.canFitAnotherTask(nextTask) {
		return
	}
	reservation := q.q.Dequeue()
	if reservation == nil {
		log.CtxWarningf(q.rootContext, "reservation is nil")
		return
	}
	ctx := log.EnrichContext(q.rootContext, log.ExecutionIDKey, reservation.GetTaskId())
	ctx, cancel := context.WithCancel(ctx)
	ctx = tracing.ExtractProtoTraceMetadata(ctx, reservation.GetTraceMetadata())

	q.trackTask(reservation, &cancel)

	go func() {
		defer cancel()
		defer func() {
			q.mu.Lock()
			q.untrackTask(reservation, &cancel)
			q.mu.Unlock()
			// Wake up the scheduling loop since the resources we just freed up
			// may allow another task to become runnable.
			q.checkQueueSignal <- struct{}{}
		}()

		taskLease := task_leaser.NewTaskLeaser(q.env, q.exec.ID(), reservation.GetTaskId())
		leaseCtx, serializedTask, err := taskLease.Claim(ctx)
		if err != nil {
			// NotFound means the task is already claimed.
			if status.IsNotFoundError(err) {
				log.CtxInfof(ctx, "Could not claim task %q: %s", reservation.GetTaskId(), err)
			} else {
				log.CtxWarningf(ctx, "Error leasing task %q: %s", reservation.GetTaskId(), err)
			}
			return
		}
		ctx = leaseCtx

		execTask := &repb.ExecutionTask{}
		if err := proto.Unmarshal(serializedTask, execTask); err != nil {
			log.CtxErrorf(ctx, "error unmarshalling task %q: %s", reservation.GetTaskId(), err)
			taskLease.Close(ctx, nil, false /*=retry*/)
			return
		}
		scheduledTask := &repb.ScheduledTask{
			ExecutionTask:      execTask,
			SchedulingMetadata: reservation.GetSchedulingMetadata(),
		}
		retry, err := q.runTask(ctx, scheduledTask)
		if err != nil {
			log.CtxErrorf(ctx, "Error running task %q (re-enqueue for retry: %t): %s", reservation.GetTaskId(), retry, err)
		}
		taskLease.Close(ctx, err, retry)
	}()
}

func (q *PriorityTaskScheduler) Start() error {
	go func() {
		for range q.checkQueueSignal {
			q.handleTask()
		}
	}()
	return nil
}

func (q *PriorityTaskScheduler) Stop() error {
	return nil
}

func (q *PriorityTaskScheduler) GetQueuedTaskReservations() []*scpb.EnqueueTaskReservationRequest {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.GetAll()
}
