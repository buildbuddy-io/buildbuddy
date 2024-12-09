package priority_task_scheduler

import (
	"container/list"
	"context"
	"flag"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	exclusiveTaskScheduling = flag.Bool("executor.exclusive_task_scheduling", false, "If true, only one task will be scheduled at a time. Default is false")
	shutdownCleanupDuration = flag.Duration("executor.shutdown_cleanup_duration", 15*time.Second, "The minimum duration during the shutdown window to allocate for cleaning up containers. This is capped to the value of `max_shutdown_duration`.")
	excessCapacityThreshold = flag.Float64("executor.excess_capacity_threshold", .40, "A percentage (of RAM and CPU) utilization below which this executor may request additional work")
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
	if req.GetSchedulingMetadata().GetTrackQueuedTaskSize() {
		metrics.RemoteExecutionAssignedOrQueuedEstimatedMilliCPU.
			Add(float64(req.TaskSize.EstimatedMilliCpu))
		metrics.RemoteExecutionAssignedOrQueuedEstimatedRAMBytes.
			Add(float64(req.TaskSize.EstimatedMemoryBytes))
	}
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
	if req.GetSchedulingMetadata().GetTrackQueuedTaskSize() {
		metrics.RemoteExecutionAssignedOrQueuedEstimatedMilliCPU.
			Sub(float64(req.TaskSize.EstimatedMilliCpu))
		metrics.RemoteExecutionAssignedOrQueuedEstimatedRAMBytes.
			Sub(float64(req.TaskSize.EstimatedMemoryBytes))
	}
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
	customResourcesCapacity map[string]customResourceCount
	customResourcesUsed     map[string]customResourceCount
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
	customResourcesCapacity := map[string]customResourceCount{}
	customResourcesUsed := map[string]customResourceCount{}
	for _, r := range resources.GetAllocatedCustomResources() {
		customResourcesCapacity[r.GetName()] = customResource(r.GetValue())
		customResourcesUsed[r.GetName()] = 0
	}

	rootContext, rootCancel := context.WithCancel(context.Background())
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
		customResourcesCapacity: customResourcesCapacity,
		customResourcesUsed:     customResourcesUsed,
		exclusiveTaskScheduling: *exclusiveTaskScheduling,
	}
	qes.rootContext = qes.enrichContext(qes.rootContext)

	env.GetHealthChecker().RegisterShutdownFunction(qes.Shutdown)
	return qes
}

func (q *PriorityTaskScheduler) enrichContext(ctx context.Context) context.Context {
	ctx = log.EnrichContext(ctx, "executor_host_id", q.exec.HostID())
	ctx = log.EnrichContext(ctx, "executor_id", q.exec.ID())
	return ctx
}

// Shutdown ensures that we don't attempt to claim work that was enqueued but
// not started yet, allowing another executor a chance to complete it. This is
// client-side "graceful" stop -- we stop processing queued work as soon as we
// receive a shutdown signal, but permit in-progress work to continue, up until
// just before the shutdown timeout, at which point we hard-cancel it.
func (q *PriorityTaskScheduler) Shutdown(ctx context.Context) error {
	ctx = q.enrichContext(ctx)
	log.CtxDebug(ctx, "PriorityTaskScheduler received shutdown signal")
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

	// Cancel all tasks early enough to allow containers and workspaces to be
	// cleaned up.
	delay := time.Until(deadline) - *shutdownCleanupDuration
	ctx, cancel := context.WithTimeout(ctx, delay)
	defer cancel()

	// Start a goroutine that will:
	//   - log success on graceful shutdown
	//   - cancel root context after delay has passed
	go func() {
		select {
		case <-ctx.Done():
			log.CtxInfof(ctx, "Graceful stop of executor succeeded.")
		case <-time.After(delay):
			log.CtxWarningf(ctx, "Hard-stopping executor!")
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

	enqueueFn := func() {
		q.mu.Lock()
		q.q.Enqueue(req)
		q.mu.Unlock()
		log.CtxInfof(ctx, "Added task %+v to pq.", req)
		// Wake up the scheduling loop so that it can run the task if there are
		// enough resources available.
		q.checkQueueSignal <- struct{}{}
	}
	if req.GetDelay().AsDuration() > 0 {
		go func() {
			time.Sleep(req.GetDelay().AsDuration())
			enqueueFn()
		}()
	} else {
		enqueueFn()
	}
	return &scpb.EnqueueTaskReservationResponse{}, nil
}

func (q *PriorityTaskScheduler) propagateExecutionTaskValuesToContext(ctx context.Context, execTask *repb.ExecutionTask) context.Context {
	// Make sure we identify any executor cache requests as being from the
	// executor, and also set the client origin (e.g. internal / external).
	ctx = usageutil.WithLocalServerLabels(ctx)

	if execTask.GetJwt() != "" {
		ctx = context.WithValue(ctx, "x-buildbuddy-jwt", execTask.GetJwt())
	}
	rmd := execTask.GetRequestMetadata()
	if rmd == nil {
		rmd = &repb.RequestMetadata{ToolInvocationId: execTask.GetInvocationId()}
	}
	rmd = rmd.CloneVT()
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
	if u, err := auth.UserFromTrustedJWT(ctx); err == nil {
		ctx = log.EnrichContext(ctx, "group_id", u.GetGroupID())
	}
	clientStream, err := operation.Publish(ctx, q.env.GetRemoteExecutionClient(), execTask.GetExecutionId())
	if err != nil {
		log.CtxWarningf(ctx, "Error opening publish operation stream: %s", err)
		return true, status.WrapError(err, "failed to open execution status update stream")
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
		return true, status.WrapError(err, "failed to finalize execution update stream")
	}
	return false, nil
}

func (q *PriorityTaskScheduler) trackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	q.activeTaskCancelFuncs[cancel] = struct{}{}
	if size := res.GetTaskSize(); size != nil {
		q.ramBytesUsed += size.GetEstimatedMemoryBytes()
		q.cpuMillisUsed += size.GetEstimatedMilliCpu()
		for _, r := range size.GetCustomResources() {
			if _, ok := q.customResourcesUsed[r.GetName()]; ok {
				q.customResourcesUsed[r.GetName()] += customResource(r.GetValue())
			}
		}
		metrics.RemoteExecutionAssignedRAMBytes.Set(float64(q.ramBytesUsed))
		metrics.RemoteExecutionAssignedMilliCPU.Set(float64(q.cpuMillisUsed))
		log.CtxDebugf(q.rootContext, "Claimed task resources. Queue stats: %s", q.stats())
	}
}

func (q *PriorityTaskScheduler) untrackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	delete(q.activeTaskCancelFuncs, cancel)
	if size := res.GetTaskSize(); size != nil {
		q.ramBytesUsed -= size.GetEstimatedMemoryBytes()
		q.cpuMillisUsed -= size.GetEstimatedMilliCpu()
		for _, r := range size.GetCustomResources() {
			if _, ok := q.customResourcesUsed[r.GetName()]; ok {
				q.customResourcesUsed[r.GetName()] -= customResource(r.GetValue())
			}
		}
		metrics.RemoteExecutionAssignedRAMBytes.Set(float64(q.ramBytesUsed))
		metrics.RemoteExecutionAssignedMilliCPU.Set(float64(q.cpuMillisUsed))
		log.CtxDebugf(q.rootContext, "Released task resources. Queue stats: %s", q.stats())
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

func (q *PriorityTaskScheduler) canFitTask(res *scpb.EnqueueTaskReservationRequest) bool {
	// If we're running in exclusiveTaskScheduling mode, only ever allow one
	// task to run at a time. Otherwise fall through to the logic below.
	if q.exclusiveTaskScheduling && len(q.activeTaskCancelFuncs) >= 1 {
		return false
	}

	size := res.GetTaskSize()

	availableRAM := q.ramBytesCapacity - q.ramBytesUsed
	if size.GetEstimatedMemoryBytes() > availableRAM {
		return false
	}

	availableCPU := q.cpuMillisCapacity - q.cpuMillisUsed
	if size.GetEstimatedMilliCpu() > availableCPU {
		return false
	}

	for _, r := range size.GetCustomResources() {
		used, ok := q.customResourcesUsed[r.GetName()]
		if !ok {
			// The scheduler server should never send us tasks that require
			// resources we haven't set up in the config.
			alert.UnexpectedEvent("missing_custom_resource", "Task requested custom resource %q which is not configured for this executor", r.GetName())
			continue
		}
		available := q.customResourcesCapacity[r.GetName()] - used
		if customResource(r.GetValue()) > available {
			return false
		}
	}

	// The scheduler server should prevent CPU/memory requests that are <= 0.
	// Alert if we get a task like this.
	if size.GetEstimatedMemoryBytes() <= 0 {
		alert.UnexpectedEvent("invalid_task_memory", "Requested memory %d is invalid", size.GetEstimatedMemoryBytes())
	}
	if size.GetEstimatedMilliCpu() <= 0 {
		alert.UnexpectedEvent("invalid_task_cpu", "Requested CPU %d is invalid", size.GetEstimatedMilliCpu())
	}

	return true
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
	if nextTask == nil || !q.canFitTask(nextTask) {
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
	log.CtxInfof(ctx, "Scheduling task of size %s", tasksize.String(nextTask.GetTaskSize()))

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
		if iid := execTask.GetInvocationId(); iid != "" {
			ctx = log.EnrichContext(ctx, log.InvocationIDKey, iid)
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

// HasExcessCapacity returns a boolean indicating if this executor has excess
// capacity for work. The scheduler-client may use this to request more work
// from the scheduler, or reset a timeout if there is no excess capacity.
func (q *PriorityTaskScheduler) HasExcessCapacity() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.shuttingDown {
		return false
	}

	// If more than n% of RAM is used; don't request extra work.
	if float64(q.ramBytesUsed) > float64(q.ramBytesCapacity)*(*excessCapacityThreshold) {
		return false
	}

	// If more than n% of CPU is used; don't request extra work.
	if float64(q.cpuMillisUsed) > float64(q.cpuMillisCapacity)*(*excessCapacityThreshold) {
		return false
	}
	return true
}

// customResourceCount represents custom resource values in such a way that
// floating point rounding errors are not accumulated as values are added and
// subtracted over time.
type customResourceCount int64

func customResource(value float32) customResourceCount {
	// Represent the value as an integer value up to the 6th decimal place. This
	// is a deterministic transformation that avoids accumulating errors over
	// time (because each float value is mapped to the same integer every time,
	// and integer arithmetic is exact), while also providing reasonably high
	// precision.
	millionths := int64(value * 1e6)
	return customResourceCount(millionths)
}
