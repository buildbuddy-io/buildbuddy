// Package oomkiller monitors executor memory usage and asks registered work to
// stop before the executor reaches its own memory limit.
//
// A [Killer] tracks [KillableTask] interface values registered by the executor.
// Runners can implement this task interface for both active command executions
// and paused persistent worker containers.
//
// When executor memory usage crosses the configured threshold, the killer
// chooses a victim in priority order:
//
//   - First, active tasks above their estimate.
//   - Next, the least recently used paused runners.
//   - Finally, the shortest running active tasks.
//
// Tasks that report unknown (zero) memory usage are never chosen as victims.
//
// The killer also respects remote_execution_priority. After choosing a victim
// candidate using the rules above, it checks whether the candidate's group owns
// lower-priority active tasks that can be killed instead. This two-phase
// approach lets the killer apply the global victim selection rules across all
// groups while preserving priority ordering within the selected group. Priority
// values are never compared across groups.
package oomkiller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	enabled              = flag.Bool("executor.oom_killer.enabled", false, "Whether to kill running tasks before executor memory exhaustion kills the executor.", flag.Internal)
	pollInterval         = flag.Duration("executor.oom_killer.poll_interval", time.Second, "How often the executor OOM killer checks executor memory and task memory.", flag.Internal)
	memoryUsageThreshold = flag.Float64("executor.oom_killer.memory_usage_threshold", 0.95, "Fraction of executor memory usage at which the executor OOM killer starts killing tasks.", flag.Internal)
)

// Killer kills running execution tasks when executor memory usage crosses a
// configured threshold.
type Killer interface {
	// Register makes task eligible for OOM killing until the returned function
	// is called or ctx is done. Register is non-blocking.
	Register(ctx context.Context, task KillableTask) func()
}

// KillableTask is the victim surface needed by the OOM killer. It can
// represent an active execution task or a paused recycled runner.
type KillableTask interface {
	// State returns the victim's last recorded memory state without blocking.
	// If memory is unknown, it returns a nil UsageStats or a UsageStats with
	// MemoryBytes == 0. The returned TaskState must be an immutable snapshot;
	// implementations must not return pointers to concurrently mutated state.
	State(ctx context.Context) (*TaskState, error)

	// Kill asks the task to stop. Active execution tasks should convert stats
	// to the retryable error reported when command execution unwinds. Paused
	// runners should remove the paused container. Kill must be idempotent.
	Kill(ctx context.Context, stats KillStats)
}

// TaskState describes a registered victim's current memory usage and whether
// it is actively executing work.
type TaskState struct {
	// EstimatedMemoryBytes is the victim's scheduled or expected memory usage.
	EstimatedMemoryBytes int64
	// GroupID identifies the group that owns an active task.
	GroupID string
	// RemoteExecutionPriority is the task's remote execution priority. Positive
	// values mark low priority tasks, and larger values are lower priority.
	// This field is only comparable among active tasks with the same non-empty
	// GroupID.
	RemoteExecutionPriority int32
	// StartedAt is when the currently active task started running. It is used
	// to prefer shorter running active tasks.
	StartedAt time.Time
	// LRURank orders inactive paused runners by recency of use. Lower values
	// are less recently used and are killed first. Callers may use either a list
	// index or a timestamp as long as older entries have lower ranks.
	LRURank int64
	// UsageStats is the victim's live resource usage.
	UsageStats *repb.UsageStats
	// Active is true when killing the victim fails in-flight work. Inactive
	// victims, such as paused runners, are persistent workers with no active
	// task.
	Active bool
}

// KillStats describes why the OOM killer chose a victim.
type KillStats struct {
	// EstimatedMemoryBytes is the victim's scheduled or expected memory usage.
	EstimatedMemoryBytes int64
	// ObservedMemoryBytes is the live memory usage reported by the victim.
	ObservedMemoryBytes int64
	// ExecutorMemorySnapshot is the executor memory snapshot that triggered the
	// kill.
	ExecutorMemorySnapshot *MemorySnapshot
}

// MemorySnapshot is a point in time view of executor memory usage.
type MemorySnapshot struct {
	// UsedBytes is the executor memory usage in bytes.
	UsedBytes int64
	// LimitBytes is the executor memory limit in bytes.
	LimitBytes int64
	// AvailableBytes is the executor memory headroom in bytes.
	AvailableBytes int64
}

// MemoryMonitor reads executor memory usage.
type MemoryMonitor interface {
	// Snapshot reads current executor memory usage.
	Snapshot(ctx context.Context) (*MemorySnapshot, error)
}

// NewMemoryMonitor returns the default executor memory monitor.
func NewMemoryMonitor(_ string) MemoryMonitor {
	return resourcesMemoryMonitor{}
}

type resourcesMemoryMonitor struct{}

func (resourcesMemoryMonitor) Snapshot(_ context.Context) (*MemorySnapshot, error) {
	limitBytes := resources.GetAllocatedRAMBytes()
	availableBytes := resources.GetSysFreeRAMBytes()
	return &MemorySnapshot{
		UsedBytes:      max(int64(0), limitBytes-availableBytes),
		LimitBytes:     limitBytes,
		AvailableBytes: availableBytes,
	}, nil
}

type killer struct {
	monitor              MemoryMonitor
	memoryUsageThreshold float64
	// runDone is closed when the background polling loop exits.
	runDone chan struct{}

	mu     sync.Mutex
	nextID uint64
	tasks  map[uint64]registeredTask

	taskChange chan struct{}
}

type registeredTask struct {
	task KillableTask
}

type noopKiller struct{}

func (noopKiller) Register(ctx context.Context, task KillableTask) func() {
	return func() {}
}

// New returns an executor OOM killer using monitor when enabled. It returns a
// no-op killer when the OOM killer flag is disabled, and an error when the OOM
// killer is enabled with a memory usage threshold not greater than 0 and less
// than 1.
func New(ctx context.Context, monitor MemoryMonitor) (Killer, error) {
	if !*enabled {
		return noopKiller{}, nil
	}
	threshold := *memoryUsageThreshold
	if threshold <= 0 || threshold >= 1 {
		return nil, fmt.Errorf("executor OOM killer memory usage threshold must be greater than 0 and less than 1, got %g", threshold)
	}
	k := &killer{
		monitor:              monitor,
		memoryUsageThreshold: threshold,
		runDone:              make(chan struct{}),
		tasks:                make(map[uint64]registeredTask),
		taskChange:           make(chan struct{}, 1),
	}
	go func() {
		defer close(k.runDone)
		k.run(ctx)
	}()
	return k, nil
}

func (k *killer) Register(ctx context.Context, task KillableTask) func() {
	k.mu.Lock()
	wasEmpty := len(k.tasks) == 0
	k.nextID++
	id := k.nextID
	k.tasks[id] = registeredTask{
		task: task,
	}
	k.mu.Unlock()
	if wasEmpty {
		k.signalTaskChange()
	}

	var once sync.Once
	unregisterTask := func() {
		once.Do(func() {
			k.unregister(id)
		})
	}
	stopContextCallback := context.AfterFunc(ctx, unregisterTask)
	unregister := func() {
		stopContextCallback()
		unregisterTask()
	}
	return unregister
}

func (k *killer) run(ctx context.Context) {
	for {
		k.waitForTasks(ctx)
		if ctx.Err() != nil {
			return
		}
		k.pollWhileTasks(ctx)
		if ctx.Err() != nil {
			return
		}
	}
}

func (k *killer) waitForTasks(ctx context.Context) {
	for {
		// The task map is the source of truth; taskChange is only a wakeup
		// signal. Check the map before waiting so coalesced signals cannot
		// leave registered tasks unpolled.
		if k.hasTasks() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-k.taskChange:
		}
	}
}

// pollWhileTasks owns the polling ticker while tasks are registered. It stops
// the ticker when the last task is removed; run will wait for the next
// taskChange signal before creating another ticker.
func (k *killer) pollWhileTasks(ctx context.Context) {
	interval := *pollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-k.taskChange:
			if !k.hasTasks() {
				return
			}
		case <-ticker.C:
			if !k.hasTasks() {
				return
			}
			if err := k.check(ctx); err != nil {
				log.CtxWarningf(ctx, "Executor OOM killer check failed: %s", err)
			}
		}
	}
}

func (k *killer) check(ctx context.Context) error {
	snapshot, err := k.monitor.Snapshot(ctx)
	if err != nil {
		return err
	}
	// Killing a single task per poll may leave us over the threshold for many
	// polls, so keep killing victims until executor memory usage is projected to
	// fall back under the threshold (or we run out of victims). A killed
	// victim's memory isn't reclaimed instantly, so we can't re-snapshot after
	// each kill; instead we optimistically subtract each victim's observed
	// memory from the projected usage.
	projectedUsedBytes := snapshot.UsedBytes
	killed := 0
	for k.shouldKill(ctx, projectedUsedBytes, snapshot.LimitBytes) {
		victim := k.chooseVictim(ctx)
		if victim == nil {
			if killed == 0 {
				log.CtxWarningf(ctx, "Executor OOM killer wanted to kill a victim but no registered victim reported state")
			}
			return nil
		}
		// Assume the victim's memory will be reclaimed whether we kill it or it
		// already finished on its own.
		projectedUsedBytes -= victim.observedMemoryBytes
		if !k.unregister(victim.id) {
			// The victim was unregistered (most likely it just finished)
			// between selection and now. Look for another victim instead of
			// waiting for the next poll.
			continue
		}
		stats := KillStats{
			EstimatedMemoryBytes:   victim.state.EstimatedMemoryBytes,
			ObservedMemoryBytes:    victim.observedMemoryBytes,
			ExecutorMemorySnapshot: snapshot,
		}
		log.CtxWarningf(ctx, "Executor OOM killer terminating victim %s: executor memory used=%d limit=%d available=%d victim memory=%d estimated=%d active=%t", taskName(victim.task), snapshot.UsedBytes, snapshot.LimitBytes, snapshot.AvailableBytes, victim.observedMemoryBytes, victim.state.EstimatedMemoryBytes, victim.state.Active)
		metrics.RemoteExecutionOOMKillerTargetedTaskMemoryBytes.Observe(float64(victim.observedMemoryBytes))
		victim.task.Kill(ctx, stats)
		killed++
	}
	return nil
}

func (k *killer) shouldKill(ctx context.Context, usedBytes, limitBytes int64) bool {
	// This should probably never happen, but make sure we never divide by 0.
	if limitBytes <= 0 {
		log.CtxDebugf(ctx, "Memory snapshot reported LimitBytes <= 0 (%d)", limitBytes)
		return false
	}
	return float64(usedBytes)/float64(limitBytes) >= k.memoryUsageThreshold
}

type victimCandidate struct {
	id                  uint64
	task                KillableTask
	state               *TaskState
	observedMemoryBytes int64
	overageBytes        int64
}

func (k *killer) chooseVictim(ctx context.Context) *victimCandidate {
	k.mu.Lock()
	candidates := make([]victimCandidate, 0, len(k.tasks))
	for id, rt := range k.tasks {
		candidates = append(candidates, victimCandidate{
			id:   id,
			task: rt.task,
		})
	}
	k.mu.Unlock()

	for i := range candidates {
		candidate := &candidates[i]
		task := candidate.task
		state, err := task.State(ctx)
		if err != nil {
			log.CtxWarningf(ctx, "Executor OOM killer could not read state for victim %s: %s", taskName(task), err)
			continue
		}
		if state == nil {
			continue
		}
		observedMemoryBytes := max(int64(0), state.UsageStats.GetMemoryBytes())
		if observedMemoryBytes == 0 {
			// A task reporting zero memory has unknown usage per the
			// KillableTask.State contract. Never kill it: it frees no measurable
			// memory, and since killing it wouldn't reduce the projected usage,
			// selecting it could let the kill loop clear out every such task in
			// a single poll.
			continue
		}
		candidate.state = state
		candidate.observedMemoryBytes = observedMemoryBytes
		candidate.overageBytes = observedMemoryBytes - state.EstimatedMemoryBytes
	}
	if candidate, ok := chooseBestCandidate(candidates, overEstimatedTask, betterMemoryOverage); ok {
		best := chooseLowestPriorityTaskInActiveGroup(candidates, candidate, overEstimatedTask, betterMemoryOverage)
		return &best
	}
	if best, ok := chooseBestCandidate(candidates, pausedRunner, betterLeastRecentlyUsed); ok {
		return &best
	}
	if candidate, ok := chooseBestCandidate(candidates, activeTask, betterShortestRunningTask); ok {
		best := chooseLowestPriorityTaskInActiveGroup(candidates, candidate, activeTask, betterShortestRunningTask)
		return &best
	}
	return nil
}

func overEstimatedTask(candidate victimCandidate) bool {
	return candidate.state.Active && candidate.overageBytes > 0
}

func pausedRunner(candidate victimCandidate) bool {
	return !candidate.state.Active
}

func activeTask(candidate victimCandidate) bool {
	return candidate.state.Active
}

func chooseBestCandidate(candidates []victimCandidate, matches func(victimCandidate) bool, better func(victimCandidate, victimCandidate) bool) (victimCandidate, bool) {
	var best victimCandidate
	var found bool
	for _, candidate := range candidates {
		if candidate.state == nil || !matches(candidate) {
			continue
		}
		if !found || better(candidate, best) {
			best = candidate
			found = true
		}
	}
	return best, found
}

type activeCandidateGroupKey struct {
	groupID string
	taskID  uint64
}

func chooseLowestPriorityTaskInActiveGroup(candidates []victimCandidate, victim victimCandidate, matches func(victimCandidate) bool, tieBreaker func(victimCandidate, victimCandidate) bool) victimCandidate {
	groupKey := activeTaskGroupKey(victim)
	best := victim
	for _, candidate := range candidates {
		if candidate.state == nil || !activeTask(candidate) || activeTaskGroupKey(candidate) != groupKey {
			continue
		}
		if betterWithinActiveGroup(candidate, best, matches, tieBreaker) {
			best = candidate
		}
	}
	return best
}

func activeTaskGroupKey(candidate victimCandidate) activeCandidateGroupKey {
	if candidate.state.GroupID == "" {
		return activeCandidateGroupKey{taskID: candidate.id}
	}
	return activeCandidateGroupKey{groupID: candidate.state.GroupID}
}

func betterWithinActiveGroup(candidate, best victimCandidate, matches func(victimCandidate) bool, tieBreaker func(victimCandidate, victimCandidate) bool) bool {
	if candidate.state.RemoteExecutionPriority != best.state.RemoteExecutionPriority {
		return candidate.state.RemoteExecutionPriority > best.state.RemoteExecutionPriority
	}
	candidateMatches := matches(candidate)
	bestMatches := matches(best)
	if candidateMatches != bestMatches {
		return candidateMatches
	}
	return tieBreaker(candidate, best)
}

func betterMemoryOverage(candidate, best victimCandidate) bool {
	if candidate.overageBytes != best.overageBytes {
		return candidate.overageBytes > best.overageBytes
	}
	return candidate.observedMemoryBytes > best.observedMemoryBytes
}

func betterLeastRecentlyUsed(candidate, best victimCandidate) bool {
	if candidate.state.LRURank != best.state.LRURank {
		return candidate.state.LRURank < best.state.LRURank
	}
	return candidate.observedMemoryBytes > best.observedMemoryBytes
}

func betterShortestRunningTask(candidate, best victimCandidate) bool {
	if !candidate.state.StartedAt.Equal(best.state.StartedAt) {
		return candidate.state.StartedAt.After(best.state.StartedAt)
	}
	return candidate.observedMemoryBytes > best.observedMemoryBytes
}

func (k *killer) unregister(id uint64) bool {
	if !k.removeTask(id) {
		return false
	}
	// Allow the ticker to stop in case the executor is now idle (we removed the
	// last task).
	k.signalTaskChange()
	return true
}

func (k *killer) removeTask(id uint64) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.tasks[id]; !ok {
		alert.UnexpectedEvent("oom_killer_unregister_missing_task", "Tried to unregister task from the OOM killer that is not currently registered (id=%d)", id)
		return false
	}
	delete(k.tasks, id)
	return true
}

func (k *killer) hasTasks() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.tasks) > 0
}

func (k *killer) signalTaskChange() {
	select {
	case k.taskChange <- struct{}{}:
	default:
	}
}

func taskName(task KillableTask) string {
	if s, ok := task.(fmt.Stringer); ok {
		return s.String()
	}
	return fmt.Sprintf("%T", task)
}
