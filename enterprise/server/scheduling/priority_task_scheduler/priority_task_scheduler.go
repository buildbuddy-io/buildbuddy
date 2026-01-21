package priority_task_scheduler

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	exclusiveTaskScheduling = flag.Bool("executor.exclusive_task_scheduling", false, "If true, only one task will be scheduled at a time. Default is false")
	shutdownCleanupDuration = flag.Duration("executor.shutdown_cleanup_duration", 15*time.Second, "The minimum duration during the shutdown window to allocate for cleaning up containers. This is capped to the value of `max_shutdown_duration`.")
	queueTrimInterval       = flag.Duration("executor.queue_trim_interval", 15*time.Second, "The interval between attempts to prune tasks that have already been completed by other executors.  A value <= 0 disables this feature.")
	excessCapacityThreshold = flag.Float64("executor.excess_capacity_threshold", .40, "A percentage (of RAM and CPU) utilization below which this executor may request additional work")
	region                  = flag.String("executor.region", "", "Region metadata associated with executions.")
	roundTaskCpuSize        = flag.Bool("executor.round_task_cpu_size", false, "If true, round tasks' CPU sizes up to the nearest whole number.")
)

var shuttingDownLogOnce sync.Once

type queuedTask struct {
	*scpb.EnqueueTaskReservationRequest

	// WorkerQueuedTimestamp is the timestamp at which the task was added to
	// the local queue.
	WorkerQueuedTimestamp time.Time
}

type groupPriorityQueue struct {
	*priority_queue.ThreadSafePriorityQueue[*queuedTask]
	groupID string
}

func (pq *groupPriorityQueue) Clone() *groupPriorityQueue {
	return &groupPriorityQueue{
		ThreadSafePriorityQueue: pq.ThreadSafePriorityQueue.Clone(),
		groupID:                 pq.groupID,
	}
}

type taskQueueIterator struct {
	q *taskQueue
	// Global index across all group queues - ranges from 0 to q.Len()
	i              int
	current        *queuePosition
	nextGroupQueue *list.Element
	groupIterators map[*list.Element]*groupPriorityQueue
}

// Next returns the next task in the taskQueue, or nil if the iterator has
// reached the end of the queue.
func (t *taskQueueIterator) Next() *queuedTask {
	t.current = nil
	if t.i >= t.q.Len() {
		return nil
	}
	// Rotate through group queues until we find one that we haven't exhausted,
	// then return the next task from that group queue.
	for {
		currentPQ := t.nextGroupQueue

		// Rotate
		t.nextGroupQueue = t.nextGroupQueue.Next()
		if t.nextGroupQueue == nil {
			t.nextGroupQueue = t.q.pqs.Front()
		}

		// Lazily copy the group queue.
		if _, ok := t.groupIterators[currentPQ]; !ok {
			t.groupIterators[currentPQ] = currentPQ.Value.(*groupPriorityQueue).Clone()
		}

		// Pop from the iterator's copy of the group queue to get the next task
		// in the iteration.
		if groupIterator := t.groupIterators[currentPQ]; groupIterator.Len() > 0 {
			numPopped := currentPQ.Value.(*groupPriorityQueue).Len() - groupIterator.Len()
			t.current = &queuePosition{
				GroupQueue: currentPQ,
				Index:      numPopped,
			}
			t.i++
			el, _ := groupIterator.Pop()
			return el
		}
	}
}

// Current returns the current iteration index as a reference to an element in
// the task queue. It returns nil if Next() has not been called for the first
// time, or if the iterator has been exhausted.
func (t *taskQueueIterator) Current() *queuePosition {
	return t.current
}

type taskQueue struct {
	clock clockwork.Clock
	// List of *groupPriorityQueue items.
	pqs *list.List
	// Map to allow quick lookup of a specific *groupPriorityQueue element in the pqs list.
	pqByGroupID map[string]*list.Element
	// Map tracking all task IDs across all groups.
	taskIDs map[string]struct{}
	// The *groupPriorityQueue element from which the next task will be obtained.
	// Will be nil when there are no tasks remaining.
	currentPQ *list.Element
}

func newTaskQueue(clock clockwork.Clock) *taskQueue {
	return &taskQueue{
		clock:       clock,
		pqs:         list.New(),
		pqByGroupID: make(map[string]*list.Element),
		currentPQ:   nil,
		taskIDs:     make(map[string]struct{}),
	}
}

func (t *taskQueue) GetAll() []*scpb.EnqueueTaskReservationRequest {
	reservations := make([]*scpb.EnqueueTaskReservationRequest, 0, len(t.taskIDs))

	for e := t.pqs.Front(); e != nil; e = e.Next() {
		pq, ok := e.Value.(*groupPriorityQueue)
		if !ok {
			log.Error("not a *groupPriorityQueue!??!")
			continue
		}
		for _, t := range pq.GetAll() {
			reservations = append(reservations, t.EnqueueTaskReservationRequest)
		}
	}

	return reservations
}

// Enqueue enqueues a task reservation request into the task queue.
// If the task is already enqueued, it will not be enqueued again.
// Returns true if the task was enqueued, false if it was already enqueued.
func (t *taskQueue) Enqueue(req *scpb.EnqueueTaskReservationRequest) (ok bool) {
	enqueuedAt := t.clock.Now()

	// Don't enqueue the same task twice to avoid inflating queue length
	// metrics. Duplicate enqueues can happen when the executor asks for more
	// work during mostly-idle periods, since the scheduler doesn't know which
	// tasks that are already in our queue.
	if t.HasTask(req.GetTaskId()) {
		return false
	}
	taskGroupID := req.GetSchedulingMetadata().GetTaskGroupId()
	var pq *groupPriorityQueue
	if el, ok := t.pqByGroupID[taskGroupID]; ok {
		pq, ok = el.Value.(*groupPriorityQueue)
		if !ok {
			// Why would this ever happen?
			log.Error("not a *groupPriorityQueue!??!")
			return false
		}
	} else {
		pq = &groupPriorityQueue{
			ThreadSafePriorityQueue: priority_queue.New[*queuedTask](),
			groupID:                 taskGroupID,
		}
		el := t.pqs.PushBack(pq)
		t.pqByGroupID[taskGroupID] = el
		if t.currentPQ == nil {
			t.currentPQ = el
		}
	}
	qt := &queuedTask{
		EnqueueTaskReservationRequest: req,
		WorkerQueuedTimestamp:         enqueuedAt,
	}
	// Using negative priority here, since the remote execution API specifies
	// that tasks with lower priority values should be scheduled first.
	pq.Push(qt, -float64(req.GetSchedulingMetadata().GetPriority()))
	t.taskIDs[req.GetTaskId()] = struct{}{}
	metrics.RemoteExecutionQueueLength.With(prometheus.Labels{metrics.GroupID: taskGroupID}).Set(float64(pq.Len()))
	if req.GetSchedulingMetadata().GetTrackQueuedTaskSize() {
		metrics.RemoteExecutionAssignedOrQueuedEstimatedMilliCPU.
			Add(float64(req.TaskSize.EstimatedMilliCpu))
		metrics.RemoteExecutionAssignedOrQueuedEstimatedRAMBytes.
			Add(float64(req.TaskSize.EstimatedMemoryBytes))
	}

	return true
}

// headRef returns a reference to the current head of the task queue.
func (t *taskQueue) headRef() *queuePosition {
	if t.currentPQ == nil {
		return nil
	}
	return &queuePosition{
		GroupQueue: t.currentPQ,
		Index:      0,
	}
}

// Dequeue removes the task at the head of the task queue and returns it.
func (t *taskQueue) Dequeue() *queuedTask {
	if t.currentPQ == nil {
		return nil
	}
	return t.DequeueAt(t.headRef())
}

// DequeueAt removes the task at the given pointer from the taskQueue and
// returns it.
//
// For simplicity, the per-group queues are only rotated when popping from the
// head of the taskQueue. If we are skipping past tasks that are currently only
// blocked on custom resources, then we don't rotate to the next the
// groupPriorityQueue. This is fine for now, because we don't support custom
// resources in multi-tenant scenarios yet.
func (t *taskQueue) DequeueAt(pos *queuePosition) *queuedTask {
	// Remove from the group queue.
	pq := pos.GroupQueue.Value.(*groupPriorityQueue)
	req, ok := pq.RemoveAt(pos.Index)
	if !ok {
		return nil
	}

	// Remove the task ID from the set of all IDs.
	delete(t.taskIDs, req.GetTaskId())

	// Rotate to the next group queue but only if we're doing a normal dequeue,
	// removing from the head of the taskQueue, as opposed to skipping ahead.
	if pos.GroupQueue == t.currentPQ && pos.Index == 0 {
		t.currentPQ = t.currentPQ.Next()
		if t.currentPQ == nil {
			t.currentPQ = t.pqs.Front()
		}
	}

	// Remove the group queue from the rotation if it's empty.
	if pq.Len() == 0 {
		t.pqs.Remove(pos.GroupQueue)
		if t.pqs.Front() == nil {
			// List is empty; clear currentPQ.
			t.currentPQ = nil
		}
		delete(t.pqByGroupID, pq.groupID)
	}

	metrics.RemoteExecutionQueueLength.With(prometheus.Labels{metrics.GroupID: req.GetSchedulingMetadata().GetTaskGroupId()}).Set(float64(pq.Len()))
	if req.GetSchedulingMetadata().GetTrackQueuedTaskSize() {
		metrics.RemoteExecutionAssignedOrQueuedEstimatedMilliCPU.
			Sub(float64(req.TaskSize.EstimatedMilliCpu))
		metrics.RemoteExecutionAssignedOrQueuedEstimatedRAMBytes.
			Sub(float64(req.TaskSize.EstimatedMemoryBytes))
	}
	return req
}

func (t *taskQueue) Peek() *queuedTask {
	if t.currentPQ == nil {
		return nil
	}
	pq, ok := t.currentPQ.Value.(*groupPriorityQueue)
	if !ok {
		// Why would this ever happen?
		log.Error("not a *groupPriorityQueue!??!")
		return nil
	}
	v, _ := pq.Peek()
	return v
}

// Iterator returns a new iterator over the tasks in the queue.
//
// The iterator starts from the next task waiting to be scheduled, and iterates
// across all tasks in the queue, in the same order in which they would normally
// be dequeued.
//
// NOTE: while the iterator is in use, the task queue must be locked. Once the
// task queue is unlocked, the caller cannot use the iterator again.
func (t *taskQueue) Iterator() *taskQueueIterator {
	return &taskQueueIterator{
		q:              t,
		nextGroupQueue: t.currentPQ,
		groupIterators: make(map[*list.Element]*groupPriorityQueue, 0),
	}
}

func (t *taskQueue) Len() int {
	return len(t.taskIDs)
}

func (t *taskQueue) HasTask(taskID string) bool {
	_, ok := t.taskIDs[taskID]
	return ok
}

// findTask returns the position of the task with the given ID, or nil if not found.
func (t *taskQueue) FindTask(taskID string) *queuePosition {
	// Do a quick map lookup to avoid more expensive iteration if we know the
	// task doesn't exist.
	if !t.HasTask(taskID) {
		return nil
	}
	it := t.Iterator()
	for task := it.Next(); task != nil; task = it.Next() {
		if task.GetTaskId() == taskID {
			return it.Current()
		}
	}
	return nil
}

type Options struct {
	RAMBytesCapacityOverride  int64
	CPUMillisCapacityOverride int64
}

// IExecutor is the executor interface expected by the PriorityTaskScheduler.
// TODO: make operation.Publisher an interface and move this to interfaces.go
type IExecutor interface {
	ID() string
	HostID() string
	ExecuteTaskAndStreamResults(ctx context.Context, st *repb.ScheduledTask, stream interfaces.Publisher) (retry bool, err error)
}

type PriorityTaskScheduler struct {
	env              environment.Env
	log              log.Logger
	shuttingDown     bool
	exec             IExecutor
	taskLeaser       interfaces.TaskLeaser
	clock            clockwork.Clock
	runnerPool       interfaces.RunnerPool
	checkQueueSignal chan struct{}
	rootContext      context.Context
	rootCancel       context.CancelFunc

	mu                      sync.Mutex
	q                       *taskQueue
	activeTaskCancelFuncs   sync.WaitGroup
	activeCancelFuncsCount  atomic.Int64
	resourceCapacity        *resourceCounts
	resourcesUsed           *resourceCounts
	exclusiveTaskScheduling bool
}

func NewPriorityTaskScheduler(env environment.Env, exec IExecutor, runnerPool interfaces.RunnerPool, taskLeaser interfaces.TaskLeaser, options *Options) (*PriorityTaskScheduler, error) {
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
	customResourcesAllocated, err := resources.GetAllocatedCustomResources()
	if err != nil {
		return nil, err
	}
	for _, r := range customResourcesAllocated {
		customResourcesCapacity[r.GetName()] = customResource(r.GetValue())
		customResourcesUsed[r.GetName()] = 0
	}
	rootContext, rootCancel := context.WithCancel(context.Background())
	qes := &PriorityTaskScheduler{
		env:              env,
		q:                newTaskQueue(env.GetClock()),
		exec:             exec,
		taskLeaser:       taskLeaser,
		clock:            env.GetClock(),
		runnerPool:       runnerPool,
		checkQueueSignal: make(chan struct{}, 64),
		rootContext:      rootContext,
		rootCancel:       rootCancel,
		shuttingDown:     false,
		resourceCapacity: &resourceCounts{
			RAMBytes:  ramBytesCapacity,
			CPUMillis: cpuMillisCapacity,
			Custom:    customResourcesCapacity,
		},
		resourcesUsed: &resourceCounts{
			RAMBytes:  0,
			CPUMillis: 0,
			Custom:    customResourcesUsed,
		},
		exclusiveTaskScheduling: *exclusiveTaskScheduling,
	}
	qes.rootContext = qes.enrichContext(qes.rootContext)

	env.GetHealthChecker().RegisterShutdownFunction(qes.Shutdown)

	// Initialize the queue length metric to 0 so that the horizontal pod
	// autoscaler gets accurate information about the queue length when
	// executors are first started, even if they don't receive any work on
	// startup. The autoscaler sums up all the queue lengths for all groups, so
	// it should be sufficient to set the metric to 0 for the ANON group. Using
	// ANON as the group label is convenient since we don't know the group ID at
	// this point, and it's also technically correct (0 tasks are in the queue
	// for anonymous users).
	metrics.RemoteExecutionQueueLength.With(prometheus.Labels{
		metrics.GroupID: interfaces.AuthAnonymousUser,
	}).Set(0)

	log.CtxInfof(qes.rootContext, "Initialized task scheduler: %s", qes.stats())

	return qes, nil
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
	delay := deadline.Sub(q.clock.Now()) - *shutdownCleanupDuration
	ctx, cancel := context.WithTimeout(ctx, delay)
	defer cancel()

	// Start a goroutine that will:
	//   - log success on graceful shutdown
	//   - cancel root context after delay has passed
	go func() {
		select {
		case <-ctx.Done():
			log.CtxInfof(ctx, "Graceful stop of executor succeeded.")
		case <-q.clock.After(delay):
			log.CtxWarningf(ctx, "Hard-stopping executor!")
			q.rootCancel()
		}
	}()

	// Wait for all active tasks to finish.
	q.activeTaskCancelFuncs.Wait()

	// Since all tasks have finished, no new runners can be created, and it is now
	// safe to wait for all pending cleanup jobs to finish.
	q.runnerPool.Wait()

	return nil
}

func (q *PriorityTaskScheduler) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	ctx = log.EnrichContext(ctx, log.ExecutionIDKey, req.GetTaskId())

	q.mu.Lock()
	alreadyEnqueued := q.q.HasTask(req.GetTaskId())
	q.mu.Unlock()
	if alreadyEnqueued {
		log.CtxDebugf(ctx, "Task %q is already enqueued, skipping", req.GetTaskId())
		return &scpb.EnqueueTaskReservationResponse{}, nil
	}

	if *roundTaskCpuSize {
		rounded := int64(math.Ceil(float64(req.GetTaskSize().GetEstimatedMilliCpu())/1000.0)) * 1000
		if rounded < q.resourceCapacity.CPUMillis {
			req.GetTaskSize().EstimatedMilliCpu = rounded
		}
	}

	if req.GetTaskSize().GetEstimatedMemoryBytes() > q.resourceCapacity.RAMBytes ||
		req.GetTaskSize().GetEstimatedMilliCpu() > q.resourceCapacity.CPUMillis {
		// TODO(bduffany): Return an error here instead. Currently we cannot
		// return an error because it causes the executor to disconnect and
		// reconnect to the scheduler, and the scheduler will keep attempting to
		// re-enqueue the oversized task onto this executor once reconnected.
		log.CtxErrorf(ctx,
			"Task exceeds executor capacity: requires %d bytes memory of %d available and %d milliCPU of %d available",
			req.GetTaskSize().GetEstimatedMemoryBytes(), q.resourceCapacity.RAMBytes,
			req.GetTaskSize().GetEstimatedMilliCpu(), q.resourceCapacity.CPUMillis,
		)
	}

	enqueueFn := func() {
		q.mu.Lock()
		ok := q.q.Enqueue(req)
		q.mu.Unlock()
		if !ok {
			// Already enqueued. This normally shouldn't happen since we checked
			// HasTask above, but that check can return false if a task is
			// delayed and waiting to be enqueued, but not actually enqueued
			// yet.
			return
		}
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

func (q *PriorityTaskScheduler) remove(taskID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p := q.q.FindTask(taskID); p != nil {
		q.q.DequeueAt(p)
		return true
	}
	return false

}

func (q *PriorityTaskScheduler) CancelTaskReservation(ctx context.Context, taskID string) {
	ctx = log.EnrichContext(ctx, log.ExecutionIDKey, taskID)
	if removed := q.remove(taskID); removed {
		log.CtxInfof(ctx, "Removed completed task from queue")
		// If the task we removed was at the head of the queue, the task behind
		// it in the queue might now be schedulable. Wake up the scheduler to
		// check for that.
		select {
		case q.checkQueueSignal <- struct{}{}:
		default:
		}
	}
}

func (q *PriorityTaskScheduler) propagateExecutionTaskValuesToContext(ctx context.Context, execTask *repb.ExecutionTask) context.Context {
	// Make sure we identify any executor cache requests as being from the
	// executor, and also set the client origin (e.g. internal / external).
	ctx = usageutil.WithLocalServerLabels(ctx)

	if execTask.GetJwt() != "" {
		ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, execTask.GetJwt())
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

func (q *PriorityTaskScheduler) publishOperation(ctx context.Context, executionID string) (*operation.Publisher, error) {
	if *region != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-executor-region", *region)
	}
	return operation.Publish(ctx, q.env.GetRemoteExecutionClient(), executionID)
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
	clientStream, err := q.publishOperation(ctx, execTask.GetExecutionId())
	if err != nil {
		log.CtxWarningf(ctx, "Error opening publish operation stream: %s", err)
		return true, status.WrapError(err, "failed to open execution status update stream")
	}
	start := time.Now()
	// TODO(http://go/b/1192): Figure out why CloseAndRecv() hangs if we call
	// it too soon after establishing the clientStream, and remove this delay.
	const closeStreamDelay = 10 * time.Millisecond

	retry, executionError := q.exec.ExecuteTaskAndStreamResults(ctx, st, clientStream)
	if executionError != nil {
		log.CtxWarningf(ctx, "ExecuteTaskAndStreamResults error: %s", executionError)
	}

	time.Sleep(time.Until(start.Add(closeStreamDelay)))
	if _, streamErr := clientStream.CloseAndRecv(); streamErr != nil {
		log.CtxWarningf(ctx, "Error closing execution status update stream: %s", streamErr)
		return true, status.WrapError(streamErr, "finalize execution update stream")
	}

	if executionError != nil {
		return retry, executionError
	}
	return false, nil
}

func (q *PriorityTaskScheduler) trackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	q.activeCancelFuncsCount.Add(1)
	if size := res.GetTaskSize(); size != nil {
		q.resourcesUsed.RAMBytes += size.GetEstimatedMemoryBytes()
		q.resourcesUsed.CPUMillis += size.GetEstimatedMilliCpu()
		for _, r := range size.GetCustomResources() {
			if _, ok := q.resourcesUsed.Custom[r.GetName()]; ok {
				q.resourcesUsed.Custom[r.GetName()] += customResource(r.GetValue())
			}
		}
		metrics.RemoteExecutionAssignedRAMBytes.Set(float64(q.resourcesUsed.RAMBytes))
		metrics.RemoteExecutionAssignedMilliCPU.Set(float64(q.resourcesUsed.CPUMillis))
		for name, count := range q.resourcesUsed.Custom {
			metrics.RemoteExecutionAssignedCustomResources.With(prometheus.Labels{
				metrics.CustomResourceNameLabel: name,
			}).Set(float64(count / 1e6))
		}
		log.CtxDebugf(q.rootContext, "Claimed task resources. Queue stats: %s", q.stats())
	}
}

func (q *PriorityTaskScheduler) untrackTask(res *scpb.EnqueueTaskReservationRequest, cancel *context.CancelFunc) {
	q.activeCancelFuncsCount.Add(-1)
	if size := res.GetTaskSize(); size != nil {
		q.resourcesUsed.RAMBytes -= size.GetEstimatedMemoryBytes()
		q.resourcesUsed.CPUMillis -= size.GetEstimatedMilliCpu()
		for _, r := range size.GetCustomResources() {
			if _, ok := q.resourcesUsed.Custom[r.GetName()]; ok {
				q.resourcesUsed.Custom[r.GetName()] -= customResource(r.GetValue())
			}
		}
		metrics.RemoteExecutionAssignedRAMBytes.Set(float64(q.resourcesUsed.RAMBytes))
		metrics.RemoteExecutionAssignedMilliCPU.Set(float64(q.resourcesUsed.CPUMillis))
		for name, count := range q.resourcesUsed.Custom {
			metrics.RemoteExecutionAssignedCustomResources.With(prometheus.Labels{
				metrics.CustomResourceNameLabel: name,
			}).Set(float64(count / 1e6))
		}
		log.CtxDebugf(q.rootContext, "Released task resources. Queue stats: %s", q.stats())
	}
}

func (q *PriorityTaskScheduler) stats() string {
	cpuMillisRemaining := q.resourceCapacity.CPUMillis - q.resourcesUsed.CPUMillis
	ramBytesRemaining := q.resourceCapacity.RAMBytes - q.resourcesUsed.RAMBytes
	var customResourcesStrs []string
	for k, v := range q.resourcesUsed.Custom {
		customResourcesStrs = append(customResourcesStrs, fmt.Sprintf("%s: %s of %s allocated (%s remaining)", k, v, q.resourceCapacity.Custom[k], q.resourceCapacity.Custom[k]-v))
	}
	customResourcesDesc := ""
	if len(customResourcesStrs) > 0 {
		customResourcesDesc = fmt.Sprintf(" %s", strings.Join(customResourcesStrs, ", "))
	}
	return message.NewPrinter(language.English).Sprintf(
		"CPU: %d of %d milliCPU allocated (%d remaining), Memory: %d of %d bytes allocated (%d remaining),%s Tasks: %d active, %d queued",
		q.resourcesUsed.CPUMillis, q.resourceCapacity.CPUMillis, cpuMillisRemaining,
		q.resourcesUsed.RAMBytes, q.resourceCapacity.RAMBytes, ramBytesRemaining,
		customResourcesDesc,
		q.activeCancelFuncsCount.Load(), q.q.Len())
}

// canFitTask returns whether the task can fit, given the resource capacity and
// resource reservations. "Reserved" in this context means the resources that
// are currently assigned to executing tasks, plus the resources needed by tasks
// that are in front of this task in the queue.
func (q *PriorityTaskScheduler) canFitTask(res *queuedTask, reservedResources *resourceCounts) bool {
	// If we're running in exclusiveTaskScheduling mode, only ever allow one
	// task to run at a time. Otherwise fall through to the logic below.
	if q.exclusiveTaskScheduling && q.activeCancelFuncsCount.Load() >= 1 {
		return false
	}

	size := res.GetTaskSize()

	availableRAM := q.resourceCapacity.RAMBytes - reservedResources.RAMBytes
	if size.GetEstimatedMemoryBytes() > availableRAM {
		return false
	}

	availableCPU := q.resourceCapacity.CPUMillis - reservedResources.CPUMillis
	if size.GetEstimatedMilliCpu() > availableCPU {
		return false
	}

	for _, r := range size.GetCustomResources() {
		reserved, ok := reservedResources.Custom[r.GetName()]
		if !ok {
			// The scheduler server should never send us tasks that require
			// resources we haven't set up in the config.
			alert.UnexpectedEvent("missing_custom_resource", "Task requested custom resource %q which is not configured for this executor", r.GetName())
			continue
		}
		available := q.resourceCapacity.Custom[r.GetName()] - reserved
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

func (q *PriorityTaskScheduler) nextTaskForPruning() *queuedTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shuttingDown || q.q.Len() == 0 {
		return nil
	}
	return q.q.Peek()
}

// This function peeks at the front of the queue and checks to see if the task
// still exists--if it doesn't, then we know the task was picked up *and
// finished* by another worker, so we can safely prune it from the local queue,
// allowing our queue size to go down even when we aren't picking up new tasks.
//
// During sudden bursts of large tasks, the task queues on individual executors
// can get pretty long.  If we are autoscaling according to task queue length,
// then doubling the number of executors might completely empty out the "real"
// task queue, but if the original executor pool is fully stuck on large tasks,
// the average task queue length will only be cut in half!  If that new length
// is still over our autoscaling threshold, we'll scale up more even though we
// don't need to.  Trimming makes this case less likely.
//
// This function returns true if a task was successfully dequeued.
func (q *PriorityTaskScheduler) trimQueue() bool {
	nextTask := q.nextTaskForPruning()
	if nextTask == nil {
		return false
	}

	ctx := log.EnrichContext(q.rootContext, log.ExecutionIDKey, nextTask.GetTaskId())
	ctx = tracing.ExtractProtoTraceMetadata(ctx, nextTask.GetTraceMetadata())
	resp, err := q.env.GetSchedulerClient().TaskExists(ctx, &scpb.TaskExistsRequest{TaskId: nextTask.GetTaskId()})
	if err != nil {
		log.Infof("Failed to check if task exists: %s", err)
		return false
	}
	if resp.GetExists() {
		// This task hasn't been finished by another executor.  Don't prune.
		return false
	}

	// Now that we know the task is gone, make sure it's still at the front of the queue.
	q.mu.Lock()
	defer q.mu.Unlock()

	if nextTask != q.q.Peek() {
		return false
	}

	// Queue hasn't changed--since the task is gone, that means we can safely remove it.
	if t := q.q.Dequeue(); nextTask != t {
		alert.UnexpectedEvent("nondeterministic_dequeue", "Dequeue() returned a different value than what Peek() returned")
		return false
	}
	log.CtxInfof(ctx, "Dropped queued task %q: task is gone.", nextTask.GetTaskId())
	return true
}

// queuePosition points to a specific position in the global task queue ordering
// (across all groups), allowing immediate access to the element at that
// position.
//
// If the queue is modified, the queuePosition is no longer valid.
type queuePosition struct {
	// Which of the per-group queues the task is in.
	GroupQueue *list.Element
	// The index of the task within the GroupQueue.
	Index int
}

// getNextSchedulableTask returns the next task that can be scheduled, and a
// pointer to the task in the queue.
func (q *PriorityTaskScheduler) getNextSchedulableTask() (*queuedTask, *queuePosition) {
	// Use custom resource configuration as a flag guard for the backfilling
	// logic, since backfilling only helps if custom resources are configured
	// anyway.
	// TODO: add more tests for the multi-tenant case and turn this on
	// unconditionally to simplify logic.
	if len(q.resourceCapacity.Custom) == 0 {
		nextTask := q.q.Peek()
		if nextTask == nil {
			return nil, nil
		}
		if canFit := q.canFitTask(nextTask, q.resourcesUsed); !canFit {
			return nil, nil
		}
		return nextTask, q.q.headRef()
	}

	// Iterate through the queue looking for a task that can schedule, but add
	// each skipped task's resources to the "reserved" resource counts, which is
	// initialized to the resource allocation for the tasks currently executing.
	// This ensures that when tasks skip ahead in the queue, they don't delay
	// the start time of other tasks that have been waiting for longer.
	reservedResources := q.resourcesUsed.Clone()
	iterator := q.q.Iterator()
	for task := iterator.Next(); task != nil; task = iterator.Next() {
		canFit := q.canFitTask(task, reservedResources)
		if canFit {
			return task, iterator.Current()
		}
		reservedResources.Add(q.taskResourceCounts(task.GetTaskSize()))

		// If all resources are reserved, short circuit - none of the remaining
		// tasks will be able to schedule.
		if reservedResources.AllGTE(q.resourceCapacity) {
			break
		}
	}
	return nil, nil
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
	nextTask, ref := q.getNextSchedulableTask()
	if nextTask == nil {
		return
	}
	reservation := q.q.DequeueAt(ref)
	if reservation == nil {
		log.CtxWarningf(q.rootContext, "reservation is nil")
		return
	}
	ctx := log.EnrichContext(q.rootContext, log.ExecutionIDKey, reservation.GetTaskId())
	ctx, cancel := context.WithCancel(ctx)
	ctx = tracing.ExtractProtoTraceMetadata(ctx, reservation.GetTraceMetadata())
	ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, reservation.GetJwt())
	log.CtxInfof(ctx, "Scheduling task of size %s", tasksize.String(nextTask.GetTaskSize()))

	q.trackTask(reservation.EnqueueTaskReservationRequest, &cancel)

	q.activeTaskCancelFuncs.Go(func() {
		defer cancel()
		defer func() {
			q.mu.Lock()
			q.untrackTask(reservation.EnqueueTaskReservationRequest, &cancel)
			q.mu.Unlock()
			// Wake up the scheduling loop since the resources we just freed up
			// may allow another task to become runnable.
			q.checkQueueSignal <- struct{}{}
		}()

		lease, err := q.taskLeaser.Lease(ctx, reservation.GetTaskId())
		if err != nil {
			// NotFound means the task is already claimed.
			if status.IsNotFoundError(err) {
				log.CtxInfof(ctx, "Could not claim task %q: %s", reservation.GetTaskId(), err)
			} else {
				log.CtxWarningf(ctx, "Error leasing task %q: %s", reservation.GetTaskId(), err)
			}
			return
		}
		ctx = lease.Context()

		if iid := lease.Task().GetInvocationId(); iid != "" {
			ctx = log.EnrichContext(ctx, log.InvocationIDKey, iid)
		}
		scheduledTask := &repb.ScheduledTask{
			ExecutionTask:         lease.Task(),
			SchedulingMetadata:    reservation.GetSchedulingMetadata(),
			WorkerQueuedTimestamp: timestamppb.New(reservation.WorkerQueuedTimestamp),
		}
		retry, err := q.runTask(ctx, scheduledTask)
		if err != nil {
			log.CtxErrorf(ctx, "Error running task %q (re-enqueue for retry: %t): %s", reservation.GetTaskId(), retry, err)
		}
		lease.Close(ctx, err, retry)
	})
}

func (q *PriorityTaskScheduler) Start() error {
	go func() {
		for range q.checkQueueSignal {
			q.handleTask()
		}
	}()

	if *queueTrimInterval > 0 {
		go func() {
			ticker := q.clock.NewTicker(*queueTrimInterval)
			defer ticker.Stop()

			for {
				select {
				case <-q.rootContext.Done():
					return
				case <-ticker.Chan():
				}
				trimCount := 0
				for q.trimQueue() {
					trimCount++
				}
				if trimCount > 0 {
					log.CtxDebugf(q.rootContext, "Trimmed %d tasks from queue", trimCount)
					// Wake up the scheduling loop since the task after the
					// trimmed task may now be schedulable.
					select {
					case q.checkQueueSignal <- struct{}{}:
					default:
					}
				}
			}
		}()
	}
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

// QueueLength returns the current number of tasks in the queue.
func (q *PriorityTaskScheduler) QueueLength() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.Len()
}

func (q *PriorityTaskScheduler) ActiveTaskCount() int {
	return int(q.activeCancelFuncsCount.Load())
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
	if float64(q.resourcesUsed.RAMBytes) >= float64(q.resourceCapacity.RAMBytes)*(*excessCapacityThreshold) {
		return false
	}

	// If more than n% of CPU is used; don't request extra work.
	if float64(q.resourcesUsed.CPUMillis) >= float64(q.resourceCapacity.CPUMillis)*(*excessCapacityThreshold) {
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

func (c customResourceCount) String() string {
	return fmt.Sprintf("%.2f", float64(c)/1e6)
}

// resourceCounts is a general-purpose struct holding a count of each resource
// supported by the executor. It can be used for tracking things like resource
// usage for one or more tasks, as well as the total resource capacity.
type resourceCounts struct {
	RAMBytes  int64
	CPUMillis int64
	Custom    map[string]customResourceCount
}

func (q *PriorityTaskScheduler) taskResourceCounts(res *scpb.TaskSize) *resourceCounts {
	custom := make(map[string]customResourceCount, len(res.GetCustomResources()))
	for _, r := range res.GetCustomResources() {
		// Skip unknown resources for now.
		if _, ok := q.resourceCapacity.Custom[r.GetName()]; !ok {
			continue
		}
		custom[r.GetName()] = customResource(r.GetValue())
	}
	return &resourceCounts{
		RAMBytes:  res.GetEstimatedMemoryBytes(),
		CPUMillis: res.GetEstimatedMilliCpu(),
		Custom:    custom,
	}
}

// Clone returns a deep copy of the resourceCounts object.
func (r *resourceCounts) Clone() *resourceCounts {
	clone := *r
	clone.Custom = maps.Clone(r.Custom)
	return &clone
}

// Add mutates the resourceCounts object, adding the given counts to it.
func (r *resourceCounts) Add(other *resourceCounts) {
	r.RAMBytes += other.RAMBytes
	r.CPUMillis += other.CPUMillis
	for k, v := range other.Custom {
		r.Custom[k] += v
	}
}

// AllGTE returns true if all resource counts are greater than or equal to the
// other resource counts.
func (r *resourceCounts) AllGTE(other *resourceCounts) bool {
	if r.RAMBytes < other.RAMBytes {
		return false
	}
	if r.CPUMillis < other.CPUMillis {
		return false
	}
	for k, v := range r.Custom {
		if v < other.Custom[k] {
			return false
		}
	}
	return true
}
