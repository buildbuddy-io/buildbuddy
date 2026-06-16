package oomkiller

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestKillerKillsTaskIfOutOfMemory(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	task := newFakeTask("task", 512, 768)
	task.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregister := killer.Register(ctx, task)
	defer unregister()

	// When executor memory is over threshold, a single registered task is a
	// valid victim.
	killStats := requireKilled(t, task)
	require.Equal(t, int64(512), killStats.EstimatedMemoryBytes)
	require.Equal(t, int64(768), killStats.ObservedMemoryBytes)
	require.Equal(t, int64(950), killStats.ExecutorMemorySnapshot.UsedBytes)
}

func TestKillerDoesNotKillBelowThreshold(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 500, LimitBytes: 1000, AvailableBytes: 500}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	task := newFakeTask("task", 100, 800)
	unregister := killer.Register(ctx, task)
	defer unregister()

	requireNotKilled(t, task)
}

func TestKillerUnregistersTaskWhenContextIsDone(t *testing.T) {
	killer := &killer{
		tasks:      make(map[uint64]registeredTask),
		taskChange: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(t.Context())
	task := newFakeTask("task", 100, 800)
	unregister := killer.Register(ctx, task)
	defer unregister()

	require.True(t, killer.hasTasks())

	// Canceling the registered context removes the task without waiting for
	// the caller to invoke the unregister function.
	cancel()
	require.Eventually(t, func() bool {
		return !killer.hasTasks()
	}, time.Second, time.Millisecond)
}

func TestKillerOnlyKillsTaskOnce(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	task := newFakeTask("task", 100, 800)
	unregister := killer.Register(ctx, task)
	defer unregister()

	requireKilled(t, task)
	requireNotKilledAgain(t, task)
}

func TestKillerContinuesKillingWhilePressureRemains(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	first := newFakeTask("first", 100, 800)
	second := newFakeTask("second", 100, 700)
	second.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterFirst := killer.Register(ctx, first)
	defer unregisterFirst()
	unregisterSecond := killer.Register(ctx, second)
	defer unregisterSecond()

	// The first kill does not relieve pressure, so the killer should keep
	// polling and kill another candidate on a later tick.
	requireKilled(t, first)
	requireKilled(t, second)
	require.Equal(t, 1, first.killCount())
	require.Equal(t, 1, second.killCount())
}

func TestKillerKillsMostOverEstimatedTask(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	lowOverage := newFakeTask("low-overage", 1000, 1100)
	highOverage := newFakeTask("high-overage", 1000, 1500)
	highOverage.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}
	lowPriorityTask := newFakeTask("low-priority-task", 1000, 100)
	lowPriorityTask.remoteExecutionPriority = 100

	unregisterLow := killer.Register(ctx, lowOverage)
	defer unregisterLow()
	unregisterHigh := killer.Register(ctx, highOverage)
	defer unregisterHigh()
	unregisterLowPriority := killer.Register(ctx, lowPriorityTask)
	defer unregisterLowPriority()

	requireKilled(t, highOverage)
	requireNotKilled(t, lowOverage, lowPriorityTask)
}

func TestKillerDoesNotUsePeakMemoryBytes(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	peakMemoryTask := newFakeTask("peak-memory-task", 1000, 0)
	peakMemoryTask.stats.PeakMemoryBytes = 2000
	currentMemoryTask := newFakeTask("current-memory-task", 1000, 1100)
	currentMemoryTask.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterPeakMemory := killer.Register(ctx, peakMemoryTask)
	defer unregisterPeakMemory()
	unregisterCurrentMemory := killer.Register(ctx, currentMemoryTask)
	defer unregisterCurrentMemory()

	// The killer only considers current memory usage. A stale high-water mark
	// should not make a task look over its estimate.
	requireKilled(t, currentMemoryTask)
	requireNotKilled(t, peakMemoryTask)
}

func TestKillerPrefersLowPriorityTaskWithinGroup(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	highPriorityTask := newFakeTask("high-priority-task", 1000, 2000)
	highPriorityTask.groupID = "GR1"
	lowPriorityTask := newFakeTask("low-priority-task", 1000, 1500)
	lowPriorityTask.groupID = "GR1"
	lowPriorityTask.remoteExecutionPriority = 100
	lowPriorityTask.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterHigh := killer.Register(ctx, highPriorityTask)
	defer unregisterHigh()
	unregisterLow := killer.Register(ctx, lowPriorityTask)
	defer unregisterLow()

	requireKilled(t, lowPriorityTask)
	requireNotKilled(t, highPriorityTask)
}

func TestKillerDoesNotComparePriorityAcrossGroups(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	highOverageTask := newFakeTask("high-overage-task", 1000, 2000)
	highOverageTask.groupID = "GR1"
	highOverageTask.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}
	lowPriorityTask := newFakeTask("low-priority-task", 1000, 1500)
	lowPriorityTask.groupID = "GR2"
	lowPriorityTask.remoteExecutionPriority = 100

	unregisterHighOverage := killer.Register(ctx, highOverageTask)
	defer unregisterHighOverage()
	unregisterLowPriority := killer.Register(ctx, lowPriorityTask)
	defer unregisterLowPriority()

	requireKilled(t, highOverageTask)
	requireNotKilled(t, lowPriorityTask)
}

func TestKillerNeverKillsTaskWhenSameGroupHasLowerPriorityTask(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	highOverageTask := newFakeTask("high-overage-task", 1000, 2000)
	highOverageTask.groupID = "GR1"
	lowerPriorityTask := newFakeTask("lower-priority-task", 1000, 900)
	lowerPriorityTask.groupID = "GR1"
	lowerPriorityTask.remoteExecutionPriority = 100
	lowerPriorityTask.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}
	otherGroupTask := newFakeTask("other-group-task", 1000, 1500)
	otherGroupTask.groupID = "GR2"

	unregisterHighOverage := killer.Register(ctx, highOverageTask)
	defer unregisterHighOverage()
	unregisterLowerPriority := killer.Register(ctx, lowerPriorityTask)
	defer unregisterLowerPriority()
	unregisterOtherGroup := killer.Register(ctx, otherGroupTask)
	defer unregisterOtherGroup()

	// The highest overage task selects GR1 as the group to reclaim from. Within
	// GR1, lower priority work must be killed first even though that task is not
	// itself over its estimate.
	requireKilled(t, lowerPriorityTask)
	requireNotKilled(t, highOverageTask, otherGroupTask)
}

func TestKillerBreaksSamePriorityTiesWithinGroupUsingPassOrdering(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	highPriorityTask := newFakeTask("high-priority-task", 1000, 2000)
	highPriorityTask.groupID = "GR1"
	lowPriorityLowOverageTask := newFakeTask("low-priority-low-overage-task", 1000, 1200)
	lowPriorityLowOverageTask.groupID = "GR1"
	lowPriorityLowOverageTask.remoteExecutionPriority = 100
	lowPriorityHighOverageTask := newFakeTask("low-priority-high-overage-task", 1000, 1500)
	lowPriorityHighOverageTask.groupID = "GR1"
	lowPriorityHighOverageTask.remoteExecutionPriority = 100
	lowPriorityHighOverageTask.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterHighPriority := killer.Register(ctx, highPriorityTask)
	defer unregisterHighPriority()
	unregisterLowPriorityLowOverage := killer.Register(ctx, lowPriorityLowOverageTask)
	defer unregisterLowPriorityLowOverage()
	unregisterLowPriorityHighOverage := killer.Register(ctx, lowPriorityHighOverageTask)
	defer unregisterLowPriorityHighOverage()

	// The selected group has two equally low priority active tasks, so the
	// above-estimate pass breaks the tie by killing the highest overage task.
	requireKilled(t, lowPriorityHighOverageTask)
	requireNotKilled(t, highPriorityTask, lowPriorityLowOverageTask)
}

func TestKillerKillsPausedRunnerBeforeLowPriorityTask(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	lowPriorityTask := newFakeTask("low-priority-task", 1000, 100)
	lowPriorityTask.remoteExecutionPriority = 100
	pausedRunner := newFakeTask("paused-runner", 1000, 900)
	pausedRunner.active = false
	pausedRunner.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterLow := killer.Register(ctx, lowPriorityTask)
	defer unregisterLow()
	unregisterPaused := killer.Register(ctx, pausedRunner)
	defer unregisterPaused()

	// A positive remote execution priority does not by itself make active work
	// a better victim than a paused runner.
	requireKilled(t, pausedRunner)
	requireNotKilled(t, lowPriorityTask)
}

func TestKillerKillsPausedRunnerBeforeNormalPriorityTask(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	activeTask := newFakeTask("active-task", 1000, 900)
	pausedRunner := newFakeTask("paused-runner", 1000, 400)
	pausedRunner.active = false
	pausedRunner.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterActive := killer.Register(ctx, activeTask)
	defer unregisterActive()
	unregisterPaused := killer.Register(ctx, pausedRunner)
	defer unregisterPaused()

	killStats := requireKilled(t, pausedRunner)
	require.Equal(t, int64(1000), killStats.EstimatedMemoryBytes)
	require.Equal(t, int64(400), killStats.ObservedMemoryBytes)
	requireNotKilled(t, activeTask)
}

func TestKillerKillsLeastRecentlyUsedPausedRunner(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	oldPausedRunner := newFakeTask("old-paused-runner", 1000, 400)
	oldPausedRunner.active = false
	oldPausedRunner.lruRank = 10
	oldPausedRunner.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}
	newPausedRunner := newFakeTask("new-paused-runner", 1000, 900)
	newPausedRunner.active = false
	newPausedRunner.lruRank = 20

	unregisterOld := killer.Register(ctx, oldPausedRunner)
	defer unregisterOld()
	unregisterNew := killer.Register(ctx, newPausedRunner)
	defer unregisterNew()

	// Paused runners are killed by LRU rank before memory usage.
	killStats := requireKilled(t, oldPausedRunner)
	require.Equal(t, int64(400), killStats.ObservedMemoryBytes)
	requireNotKilled(t, newPausedRunner)
}

func TestKillerKillsShortestRunningTask(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := newTestKiller(t, ctx, monitor, time.Millisecond)
	start := time.Unix(1700000000, 0)
	longRunning := newFakeTask("long-running", 1000, 900)
	longRunning.startedAt = start
	shortRunning := newFakeTask("short-running", 1000, 100)
	shortRunning.startedAt = start.Add(time.Minute)
	shortRunning.onKill = func() {
		monitor.set(&MemorySnapshot{UsedBytes: 100, LimitBytes: 1000, AvailableBytes: 900})
	}

	unregisterLong := killer.Register(ctx, longRunning)
	defer unregisterLong()
	unregisterShort := killer.Register(ctx, shortRunning)
	defer unregisterShort()

	requireKilled(t, shortRunning)
	requireNotKilled(t, longRunning)
}

func TestKillerDoesNotKillUnregisteredVictim(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}
	killer := &killer{
		monitor:              monitor,
		memoryUsageThreshold: 0.9,
		tasks:                make(map[uint64]registeredTask),
		taskChange:           make(chan struct{}, 1),
	}
	task := newFakeTask("task", 100, 800)
	task.onState = func() {
		killer.unregister(1)
	}
	killer.tasks[1] = registeredTask{
		task: task,
	}

	// The task can finish and unregister after victim selection snapshots the
	// task map. The killer must not kill it after it is no longer eligible.
	require.NoError(t, killer.check(ctx))
	require.Equal(t, 0, task.killCount())
	require.False(t, killer.hasTasks())
}

func TestKillerDoesNotPollWithoutRegisteredTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer func() {
			cancel()
			synctest.Wait()
		}()
		monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 500, LimitBytes: 1000, AvailableBytes: 500}}
		killer := newTestKiller(t, ctx, monitor, time.Second)

		// With no registered tasks, fake time can advance through many poll
		// intervals without taking any executor memory snapshots.
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Equal(t, 0, monitor.snapshotCount())

		// Registering a task starts polling again.
		task := newFakeTask("task", 100, 500)
		unregister := killer.Register(ctx, task)
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 1, monitor.snapshotCount())

		// Removing the final task stops polling again.
		unregister()
		synctest.Wait()
		snapshotsAfterUnregister := monitor.snapshotCount()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Equal(t, snapshotsAfterUnregister, monitor.snapshotCount())

	})
}

func TestKillerDoesNotLoseRegisterWakeupWhenTaskChangeSignalIsCoalesced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flags.Set(t, "executor.oom_killer.poll_interval", time.Second)
		flags.Set(t, "executor.oom_killer.memory_usage_threshold", 0.9)
		ctx, cancel := context.WithCancel(t.Context())
		defer func() {
			cancel()
			synctest.Wait()
		}()
		monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 500, LimitBytes: 1000, AvailableBytes: 500}}
		killer := &killer{
			monitor:              monitor,
			memoryUsageThreshold: 0.9,
			tasks:                make(map[uint64]registeredTask),
			taskChange:           make(chan struct{}, 1),
		}

		// A stale taskChange signal can already be queued. In that case, the
		// Register signal for the 0 to 1 task transition will be coalesced into
		// the existing buffered signal.
		killer.signalTaskChange()
		task := newFakeTask("task", 100, 500)
		unregister := killer.Register(ctx, task)
		defer unregister()

		// The polling loop must still notice the registered task because the
		// task map is checked before waiting for another signal.
		go killer.run(ctx)
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 1, monitor.snapshotCount())

	})
}

func TestNewReturnsNoopKillerWhenDisabled(t *testing.T) {
	flags.Set(t, "executor.oom_killer.enabled", false)
	flags.Set(t, "executor.oom_killer.memory_usage_threshold", 0)
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 950, LimitBytes: 1000, AvailableBytes: 50}}

	// A disabled OOM killer still returns a usable Killer, even when OOM killer
	// settings would be invalid if the feature were enabled.
	oomKiller, err := New(ctx, monitor)
	require.NoError(t, err)
	require.NotNil(t, oomKiller)
	unregister := oomKiller.Register(ctx, newFakeTask("task", 100, 200))
	require.NotNil(t, unregister)
	unregister()
	require.Equal(t, 0, monitor.snapshotCount())
}

func TestNewRejectsInvalidMemoryUsageThreshold(t *testing.T) {
	ctx := t.Context()
	monitor := &fakeMemoryMonitor{}

	for _, testCase := range []struct {
		name      string
		threshold float64
	}{
		{name: "zero", threshold: 0},
		{name: "negative", threshold: -0.1},
		{name: "one", threshold: 1},
		{name: "over one", threshold: 1.1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			flags.Set(t, "executor.oom_killer.enabled", true)
			flags.Set(t, "executor.oom_killer.memory_usage_threshold", testCase.threshold)

			// Invalid thresholds are rejected before the killer starts polling.
			oomKiller, err := New(ctx, monitor)
			require.Error(t, err)
			require.Nil(t, oomKiller)
			require.Contains(t, err.Error(), "greater than 0 and less than 1")
		})
	}
}

func TestCgroupMemoryMonitorReturnsErrorOnMemoryLimitReadFailure(t *testing.T) {
	ctx := t.Context()
	monitor, fallback := newTestCgroupMemoryMonitor(t.TempDir())

	snapshot, err := monitor.Snapshot(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read cgroup memory max")
	require.Nil(t, snapshot)
	require.Equal(t, 0, fallback.snapshotCount())
}

func TestCgroupMemoryMonitorFallsBackForUnlimitedCgroup(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0644))
	monitor, fallback := newTestCgroupMemoryMonitor(dir)

	snapshot, err := monitor.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, fallback.snapshot, snapshot)
	require.Equal(t, 1, fallback.snapshotCount())
}

func TestCgroupMemoryMonitorReadsCgroupSnapshot(t *testing.T) {
	for _, testCase := range []struct {
		name                   string
		max                    string
		current                string
		expectedUsedBytes      int64
		expectedLimitBytes     int64
		expectedAvailableBytes int64
	}{
		{name: "positive limit", max: "1000\n", current: "250\n", expectedUsedBytes: 250, expectedLimitBytes: 1000, expectedAvailableBytes: 750},
		{name: "zero limit", max: "0\n", current: "250\n", expectedUsedBytes: 250, expectedLimitBytes: 0, expectedAvailableBytes: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(testCase.max), 0644))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte(testCase.current), 0644))
			monitor, fallback := newTestCgroupMemoryMonitor(dir)

			// Numeric memory.max values are cgroup limits, including zero; only
			// the string "max" means unlimited and should use the fallback.
			snapshot, err := monitor.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, testCase.expectedUsedBytes, snapshot.UsedBytes)
			require.Equal(t, testCase.expectedLimitBytes, snapshot.LimitBytes)
			require.Equal(t, testCase.expectedAvailableBytes, snapshot.AvailableBytes)
			require.Equal(t, 0, fallback.snapshotCount())
		})
	}
}

type fakeMemoryMonitor struct {
	mu        sync.Mutex
	snapshot  *MemorySnapshot
	snapshots int
}

func (m *fakeMemoryMonitor) Snapshot(ctx context.Context) (*MemorySnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots++
	return m.snapshot, nil
}

func (m *fakeMemoryMonitor) set(snapshot *MemorySnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = snapshot
}

func (m *fakeMemoryMonitor) snapshotCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshots
}

type fakeTask struct {
	name                    string
	groupID                 string
	estimatedMemory         int64
	remoteExecutionPriority int32
	startedAt               time.Time
	lruRank                 int64
	active                  bool
	stats                   *repb.UsageStats
	onState                 func()
	onKill                  func()

	mu        sync.Mutex
	killCalls int
	killed    chan KillStats
}

func newFakeTask(taskID string, estimatedMemoryBytes, memoryBytes int64) *fakeTask {
	return &fakeTask{
		name:            taskID,
		estimatedMemory: estimatedMemoryBytes,
		active:          true,
		stats:           &repb.UsageStats{MemoryBytes: memoryBytes},
		killed:          make(chan KillStats, 10),
	}
}

func (t *fakeTask) String() string {
	return t.name
}

func (t *fakeTask) State(ctx context.Context) (*TaskState, error) {
	if t.onState != nil {
		t.onState()
	}
	return &TaskState{
		EstimatedMemoryBytes:    t.estimatedMemory,
		GroupID:                 t.groupID,
		RemoteExecutionPriority: t.remoteExecutionPriority,
		StartedAt:               t.startedAt,
		LRURank:                 t.lruRank,
		UsageStats:              t.stats,
		Active:                  t.active,
	}, nil
}

func (t *fakeTask) Kill(ctx context.Context, stats KillStats) {
	t.mu.Lock()
	t.killCalls++
	t.mu.Unlock()
	if t.onKill != nil {
		t.onKill()
	}
	t.killed <- stats
}

func (t *fakeTask) killCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.killCalls
}

func requireKilled(t testing.TB, task *fakeTask) KillStats {
	t.Helper()
	var stats KillStats
	require.Eventually(t, func() bool {
		select {
		case stats = <-task.killed:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	return stats
}

func requireNotKilled(t testing.TB, tasks ...*fakeTask) {
	t.Helper()
	for _, task := range tasks {
		require.Never(t, func() bool {
			select {
			case <-task.killed:
				return true
			default:
				return false
			}
		}, 50*time.Millisecond, time.Millisecond, "%s was killed", task.name)
	}
}

func requireNotKilledAgain(t testing.TB, task *fakeTask) {
	t.Helper()
	require.Never(t, func() bool {
		return task.killCount() > 1
	}, 50*time.Millisecond, time.Millisecond)
}

func newTestKiller(t testing.TB, ctx context.Context, monitor *fakeMemoryMonitor, pollInterval time.Duration) Killer {
	flags.Set(t, "executor.oom_killer.enabled", true)
	flags.Set(t, "executor.oom_killer.poll_interval", pollInterval)
	flags.Set(t, "executor.oom_killer.memory_usage_threshold", 0.9)

	oomKiller, err := New(ctx, monitor)
	require.NoError(t, err)
	require.NotNil(t, oomKiller)
	k, ok := oomKiller.(*killer)
	require.True(t, ok)
	t.Cleanup(func() {
		select {
		case <-k.runDone:
		case <-time.After(time.Second):
			t.Fatal("OOM killer did not stop")
		}
	})
	return oomKiller
}

func newTestCgroupMemoryMonitor(dir string) (*cgroupMemoryMonitor, *fakeMemoryMonitor) {
	fallback := &fakeMemoryMonitor{snapshot: &MemorySnapshot{UsedBytes: 1, LimitBytes: 2, AvailableBytes: 1}}
	return &cgroupMemoryMonitor{
		dir:      dir,
		fallback: fallback,
	}, fallback
}
