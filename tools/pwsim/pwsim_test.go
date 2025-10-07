package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	_ "embed"
	mrand "math/rand/v2"

	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/buildbuddy-io/buildbuddy/server/util/wrand"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

var (
	executionsPath = flag.String("executions_path", "/tmp/executions.jsonl", "Path to the executions.jsonl file. Contains Execution rows from ClickHouse with the executions to replay - one JSON object per line.")

	numExecutors            = flag.Int("num_executors", 128, "Number of executors to simulate")
	executorMilliCPU        = flag.Int64("executor_milli_cpu", 15_000, "Assignable millicpu for each executor.")
	executorMemoryBytes     = flag.Int64("executor_memory_bytes", 56*1024*1024*1024, "Assignable memory for each executor.")
	excessCapacityThreshold = flag.Float64("executor_excess_capacity_threshold", 0.40, "Fraction of resources used where the executor will proactively request more work from the scheduler.")
	runnerPoolMaxCount      = flag.Int("executor_runner_pool_max_count", 5, "Maximum number of runners in the pool.")
)

var (
	rand = mrand.New(mrand.New(mrand.NewPCG(0, 0)))
)

const (
	// Number of tasks to sample when pulling work from the scheduler.
	tasksToSample = 20

	// TODO: set more realistically
	executorSchedulerRTT = 50 * time.Microsecond

	// When using weight-based strategies, this is the weight added to each
	// executor when it executes a task. The value is large to support decaying
	// weights over time; the weighted random shuffle algorithm requires integer
	// weights.
	executorWeightIncrement = int64(1e12)
)

/*
Query (populate group_id then run with clickhouse outputformat JSONEachRow)

SELECT

	output_path,
	action_mnemonic,
	target_label,
	queued_timestamp_usec,
	worker_start_timestamp_usec,
	worker_completed_timestamp_usec,
	platform_hash,
	persistent_worker_key,
	runner_task_number,
	requested_compute_units,
	requested_memory_bytes,
	requested_milli_cpu

FROM

	buildbuddy_prod.Executions

WHERE

	group_id = GROUP_ID_HERE
	AND updated_at_usec > 1759009564281740
	AND exit_code = 0
	AND status_code = 0
	AND runner_task_number > 0

ORDER BY

	updated_at_usec DESC

LIMIT 1000000
*/
type ExecutionRowJSON struct {
	OutputPath                   string  `json:"output_path"`
	ActionMnemonic               string  `json:"action_mnemonic"`
	TargetLabel                  string  `json:"target_label"`
	QueuedTimestampUsec          string  `json:"queued_timestamp_usec"`
	WorkerStartTimestampUsec     string  `json:"worker_start_timestamp_usec"`
	WorkerCompletedTimestampUsec string  `json:"worker_completed_timestamp_usec"`
	PlatformHash                 string  `json:"platform_hash"`
	PersistentWorkerKey          string  `json:"persistent_worker_key"`
	RunnerTaskNumber             string  `json:"runner_task_number"`
	RequestedComputeUnits        float64 `json:"requested_compute_units"`
	RequestedMemoryBytes         string  `json:"requested_memory_bytes"`
	RequestedMilliCPU            string  `json:"requested_milli_cpu"`
}

func mustAtoi(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

func debugln(args ...any) {
	if os.Getenv("DEBUG") == "1" {
		fmt.Println(args...)
	}
}

func TestSimulation(t *testing.T) {
	// Read executions data file
	t.Logf("Reading execution rows...")
	executions := readExecutionRows(t)
	// Sort by queued timestamp, so we replay from oldest => newest
	t.Logf("Sorting executions...")
	slices.SortFunc(executions, func(a, b *ExecutionRowJSON) int {
		ai := mustAtoi(a.QueuedTimestampUsec)
		bi := mustAtoi(b.QueuedTimestampUsec)
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	})
	t.Logf("Sample execution: %+#v\n", executions[0])

	// Compute persistent worker hit rate. As a sanity check, this should
	// roughly match the hit rate we get if we use the
	// "persistent_worker_router" strategy below when we run real prod
	// executions through the tool.
	var pwHits, pwTotal int64
	for _, e := range executions {
		if e.PersistentWorkerKey != "" {
			pwTotal++
			if mustAtoi(e.RunnerTaskNumber) > 1 {
				pwHits++
			}
		}
	}
	t.Logf("Persistent worker hit rate (from data set): %.3f", float64(pwHits)/float64(pwTotal))
	pwKeyFreq := map[string]int64{}
	pwKeyMnemonics := map[string][]string{}
	outputPathFreq := map[string]int64{}
	numPersistentWorkerActions := 0
	for _, e := range executions {
		if e.PersistentWorkerKey != "" {
			pwKeyFreq[e.PersistentWorkerKey]++
			numPersistentWorkerActions++
			if !slices.Contains(pwKeyMnemonics[e.PersistentWorkerKey], e.ActionMnemonic) {
				pwKeyMnemonics[e.PersistentWorkerKey] = append(pwKeyMnemonics[e.PersistentWorkerKey], e.ActionMnemonic)
			}
		}
		if e.OutputPath != "" {
			outputPathFreq[e.OutputPath]++
		}
	}
	t.Logf("Unique output_path (used as stable action IDs): %d", len(outputPathFreq))
	t.Logf("Persistent worker actions: %d (%.3f%% of total)", numPersistentWorkerActions, float64(numPersistentWorkerActions)/float64(len(executions))*100)
	t.Logf("Persistent worker unique keys: %d", len(pwKeyFreq))
	persistentWorkerKeys := slices.Collect(maps.Keys(pwKeyFreq))
	slices.SortFunc(persistentWorkerKeys, func(a, b string) int {
		return -cmp.Compare(pwKeyFreq[a], pwKeyFreq[b])
	})
	t.Logf("Top 10 persistent worker key frequencies:")
	for _, k := range persistentWorkerKeys[:min(10, len(persistentWorkerKeys))] {
		t.Logf("%s %v: %d", k, pwKeyMnemonics[k], pwKeyFreq[k])
	}
	t.Logf("Bottom 10 persistent worker key frequencies:")
	for _, k := range persistentWorkerKeys[max(0, len(persistentWorkerKeys)-10):] {
		t.Logf("%s %v: %d", k, pwKeyMnemonics[k], pwKeyFreq[k])
	}
	frequencies := slices.Collect(maps.Values(pwKeyFreq))
	slices.Sort(frequencies)
	var frequencyQuantileStrs []string
	for _, q := range []float64{0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999} {
		frequencyQuantileStrs = append(frequencyQuantileStrs, fmt.Sprintf("%.3f: %d", q, frequencies[int(float64(len(frequencies))*q)]))
	}
	t.Logf("Frequency quantiles: %s", strings.Join(frequencyQuantileStrs, ", "))

	// Run simulations within a synctest bubble.
	for _, sim := range []*Simulation{
		// Simple uniform random routing. This serves as a baseline.

		// {
		// 	name:            "Random",
		// 	routingStrategy: "random",
		// },

		// Affinity router: this is what we had before adding the persistent
		// worker router. The routing key is based on platform props and first
		// action output, and we'd route only to the most recent executor that
		// executed the action.
		// {
		// 	name:            "AffinityRouter",
		// 	routingStrategy: "affinity_router",
		// },

		// Persistent worker router: this is what we do today. We hash each task
		// key to a subset of 3 executors then probe only that subset. It
		// suffers from a problem where if a single persistent worker key is
		// very common, then those 3 workers will get more work than they can
		// handle, and the system winds up relying heavily on work stealing in
		// order to balance the load. This significantly increases queue
		// duration since

		// {
		// 	name:            "PersistentWorkerRouter",
		// 	routingStrategy: "persistent_worker_router",
		// },

		// Random (baseline) routing but trying a random runner pool eviction
		// strategy (evict random runner instead of LRU runner so higher
		// represented runners are more likely to be evicted).
		// {
		// 	name:                       "Random/runnerPoolEviction=random",
		// 	routingStrategy:            "random",
		// 	runnerPoolEvictionStrategy: "random",
		// },
		// Diversity-based runner pool eviction policy targets the
		// highest-frequency runner in the pool which helps keep at least one
		// runner of each key type in the pool, maximizing the diversity of the
		// pool. Ties are broken using LRU eviction.
		// {
		// 	name:                       "Random/runnerPoolEviction=diversity",
		// 	routingStrategy:            "random",
		// 	runnerPoolEvictionStrategy: "diversity",
		// },
		// Probabilistic (fixed) runner pool eviction accounts for the fact that
		// most runner keys are highly unlikely to be reused. When the pool is
		// full, it allows evicting runners with only a small fixed probability.
		// {
		// 	name:                       "Random/runnerPoolEviction=probabilistic_fixed;P=0.05",
		// 	routingStrategy:            "random",
		// 	runnerPoolEvictionStrategy: "probabilistic_fixed",
		// 	evictionProbability:        0.05,
		// },
		// {
		// 	name:                       "Random/runnerPoolEviction=probabilistic_fixed;P=0.25",
		// 	routingStrategy:            "random",
		// 	runnerPoolEvictionStrategy: "probabilistic_fixed",
		// 	evictionProbability:        0.25,
		// },
		// {
		// 	name:                       "Random/runnerPoolEviction=probabilistic_fixed;P=0.50",
		// 	routingStrategy:            "random",
		// 	runnerPoolEvictionStrategy: "probabilistic_fixed",
		// 	evictionProbability:        0.50,
		// },
		// {
		// 	name:                       "Random/runnerPoolEviction=probabilistic_fixed;P=0.75",
		// 	routingStrategy:            "random",
		// 	runnerPoolEvictionStrategy: "probabilistic_fixed",
		// 	evictionProbability:        0.75,
		// },

		// History-based routing: for each affinity key, track the most recent N
		// executors to execute tasks with that key. When sending probes, remove
		// only the most recent executor from the list, with the theory being
		// that this executor is most likely the one that will execute the task.
		// We don't remove all 3 of the most recent executors, since it wastes
		// those history entries if the executors don't get to run the task.

		// {
		// 	name:            "HistoryBased/historySize=16",
		// 	routingStrategy: "history_based",
		// 	historySize:     16,
		// },
		// {
		// 	name:            "HistoryBased/historySize=32",
		// 	routingStrategy: "history_based",
		// 	historySize:     32,
		// },
		// {
		// 	name:            "HistoryBased/historySize=128",
		// 	routingStrategy: "history_based",
		// 	historySize:     128,
		// },
		// {
		// 	name:            "HistoryBased/historySize=256",
		// 	routingStrategy: "history_based",
		// 	historySize:     256,
		// },

		// "FrequencyWeighted" uses a map of taskKey => executorID => weight.
		// Weights are roughly based on frequency of task execution but decay
		// over time with a configurable half-life. Weights are incremented by a
		// fixed value each time an executor completes a task with the given
		// affinity key. Weights can also (optionally) be decremented each time
		// a probe is sent to an executor, as an attempt to model the fact that
		// the warm runner will be occupied until the task has completed.

		// {
		// 	name:            "FrequencyWeighted",
		// 	routingStrategy: "frequency_weighted",
		// },
		// {
		// 	name:            "FrequencyWeighted/halfLife=1m",
		// 	routingStrategy: "frequency_weighted",
		// 	weightHalfLife:  1 * time.Minute,
		// },
		// {
		// 	name:            "FrequencyWeighted/halfLife=5m",
		// 	routingStrategy: "frequency_weighted",
		// 	weightHalfLife:  5 * time.Minute,
		// },
		// {
		// 	name:                  "FrequencyWeighted/halfLife=1m;subtractWeightOnProbe=true",
		// 	routingStrategy:       "frequency_weighted",
		// 	weightHalfLife:        1 * time.Minute,
		// 	subtractWeightOnProbe: true,
		// },

		// {
		// 	name:            "AdaptiveSubpools/halfLife=1m",
		// 	routingStrategy: "adaptive_subpools",
		// 	weightHalfLife:  1 * time.Minute,
		// },
		// {
		// 	name:            "AdaptiveSubpools/halfLife=10m",
		// 	routingStrategy: "adaptive_subpools",
		// 	weightHalfLife:  10 * time.Minute,
		// },
		// {
		// 	name:            "AdaptiveSubpools/halfLife=30m",
		// 	routingStrategy: "adaptive_subpools",
		// 	weightHalfLife:  30 * time.Minute,
		// },
		// {
		// 	name:            "AdaptiveSubpools/halfLife=1h",
		// 	routingStrategy: "adaptive_subpools",
		// 	weightHalfLife:  1 * time.Hour,
		// },
		{
			name:            "AdaptiveSubpools/halfLife=3h",
			routingStrategy: "adaptive_subpools",
			weightHalfLife:  3 * time.Hour,
		},
	} {
		t.Run(sim.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Re-seed random source so that tests aren't affected
				// if we enable/disable certain other tests.
				rand = mrand.New(mrand.New(mrand.NewPCG(0, 0)))

				// Set virtual clock to the first execution's queued timestamp
				// (executions are expected to be sorted in order of queued
				// timestamp at this point)
				time.Sleep(time.Until(time.UnixMicro(mustAtoi(executions[0].QueuedTimestampUsec))))
				startTime := time.Now()

				// Run simulation
				runSimulation(t, sim, executions)

				// Print stats
				t.Logf("Virtual clock started @ %s (first execution queued time)", startTime)
				t.Logf("Virtual clock ended   @ %s (simulated duration: %s)", time.Now(), time.Since(startTime))
				t.Logf("Executors: %d", *numExecutors)
				t.Logf("Routing strategy: %s", sim.routingStrategy)
				t.Logf("Tasks completed: %d", sim.tasksCompleted.Load())
				t.Logf("Idle executor tasks pulled: %d", sim.idleExecutorTasksPulled.Load())
				t.Logf("Total queued    duration: %s", time.Duration(sim.totalQueueDurationNanos.Load()))
				t.Logf("Total worker    duration: %s", time.Duration(sim.totalWorkerDurationNanos.Load()))
				t.Logf("Total e2e (q+w) duration: %s", time.Duration(sim.totalQueueDurationNanos.Load()+sim.totalWorkerDurationNanos.Load()))
				t.Logf("Total cold runner worker duration overhead: %s", time.Duration(sim.coldRunnerOverheadNanos.Load()))
				t.Logf("Runner hit rate: %.3f", float64(sim.runnerHitCount.Load())/float64(sim.runnerRequestCount.Load()))
				t.Logf("Runners evicted: %d", sim.runnersEvicted.Load())
			})
		})
	}
}

func runSimulation(t *testing.T, sim *Simulation, executions []*ExecutionRowJSON) {
	ctx := t.Context()

	// Compute stats about tasks (by stable output_path identifier)
	warmDurations := make(map[string][]time.Duration)
	coldDurations := make(map[string][]time.Duration)
	for _, execution := range executions {
		workerDuration := time.Duration(mustAtoi(execution.WorkerCompletedTimestampUsec)-mustAtoi(execution.WorkerStartTimestampUsec)) * time.Microsecond
		if mustAtoi(execution.RunnerTaskNumber) > 1 {
			warmDurations[execution.OutputPath] = append(warmDurations[execution.OutputPath], workerDuration)
		} else {
			coldDurations[execution.OutputPath] = append(coldDurations[execution.OutputPath], workerDuration)
		}
	}
	// Compute p50 durations
	warmP50 := make(map[string]time.Duration)
	coldP50 := make(map[string]time.Duration)
	for outputPath, durations := range warmDurations {
		slices.Sort(durations)
		warmP50[outputPath] = durations[len(durations)/2]
	}
	for outputPath, durations := range coldDurations {
		slices.Sort(durations)
		coldP50[outputPath] = durations[len(durations)/2]
	}

	// Create scheduler
	scheduler := NewScheduler(sim)

	// Create and start executors
	executors := make([]*Executor, *numExecutors)
	for i := range executors {
		executors[i] = NewExecutor(sim, scheduler)
		go executors[i].Run(ctx)
	}

	// Register executors to scheduler
	scheduler.SetExecutors(executors)

	tasksStarted := make(chan *Task, 1024)
	var wg sync.WaitGroup
	wg.Go(func() {
		nDone := 0
		for task := range tasksStarted {
			<-task.Done
			nDone++
			if nDone%1000 == 0 {
				debugln(nDone, "tasks completed")
			}
		}
		// Assert that we executed as many tasks as we expected
		require.Equal(t, int64(len(executions)), sim.tasksCompleted.Load())
	})

	// Run all tasks.
	// TODO: allow waiting until the original queued timestamp is reached.
	for _, execution := range executions {
		// Advance the virtual clock to the timestamp at which the execution was
		// originally queued. Several executions might happen while time elapses
		// here, giving the executors to drain their queues as they would have
		// done originally.
		//
		// Without this sleep, we would wind up creating an unrealistic scenario
		// in which all tasks are enqueued at the same time, even if we're
		// simulating executions that would normally have occurred during a span
		// of several days.
		time.Sleep(time.Until(time.UnixMicro(mustAtoi(execution.QueuedTimestampUsec))))

		taskID := uuid.New()
		hasWarmDuration := warmP50[execution.OutputPath] != 0

		computeUnits := execution.RequestedComputeUnits
		if computeUnits == 0 {
			t.Fatalf("compute units is 0 - unexpected. Execution: %+#v", execution)
		}
		taskMilliCPU := int64(computeUnits) * 1000
		taskMemoryBytes := int64(computeUnits * 2.5e9)
		requestedMilliCPU := mustAtoi(execution.RequestedMilliCPU)
		requestedMemoryBytes := mustAtoi(execution.RequestedMemoryBytes)
		if requestedMilliCPU != 0 {
			taskMilliCPU = requestedMilliCPU
		}
		if requestedMemoryBytes != 0 {
			taskMemoryBytes = requestedMemoryBytes
		}

		task := &Task{
			QueuedAt: time.Now(),

			ID:        taskID,
			Resources: [2]int64{taskMilliCPU, taskMemoryBytes},

			ActionMnemonic: execution.ActionMnemonic,
			TargetLabel:    execution.TargetLabel,
			OutputPath:     execution.OutputPath,

			RecycleRunner:       hasWarmDuration || execution.PersistentWorkerKey != "",
			PlatformHash:        execution.PlatformHash,
			PersistentWorkerKey: execution.PersistentWorkerKey,

			ColdDuration: coldP50[execution.OutputPath],
			WarmDuration: warmP50[execution.OutputPath],

			Done: make(chan struct{}),
		}
		// Make sure both cold/warm durations are set if applicable.
		if task.ColdDuration == 0 {
			// TODO: this probably skews results; maybe report metrics here on
			// how often this happens.
			task.ColdDuration = task.WarmDuration
		}
		if task.RecycleRunner && task.WarmDuration == 0 {
			task.WarmDuration = task.ColdDuration
		}
		if task.ColdDuration == 0 && task.WarmDuration == 0 {
			panic("cold and warm durations are both 0 - should not happen")
		}

		// XXX
		// If warm duration is not less than cold duration and the durations
		// are significantly different, something is probably abnormal?
		// Maybe the task changed significantly or there is pressure stalling.
		// Try to compensate for this somehow

		// if task.RecycleRunner && task.WarmDuration > task.ColdDuration {
		// 	fraction := float64(task.WarmDuration) / float64(task.ColdDuration)
		// 	if task.WarmDuration > 1*time.Second && fraction > 1.20 {
		// 		t.Fatalf("p50 cold duration %s for task %s is significantly faster than p50 warm duration %s (unexpected)", task.ColdDuration, execution.OutputPath, task.WarmDuration)
		// 	}
		// 	// Just make the durations equal
		// 	task.WarmDuration = task.ColdDuration
		// }
		scheduler.Enqueue(ctx, task)
		tasksStarted <- task
	}
	close(tasksStarted)

	// Wait for all tasks to be completed
	debugln("Waiting for all executions to complete")
	wg.Wait()
}

func readExecutionRows(t *testing.T) []*ExecutionRowJSON {
	f, err := os.Open(*executionsPath)
	require.NoError(t, err)
	defer f.Close()
	s := bufio.NewScanner(f)
	var executions []*ExecutionRowJSON
	for s.Scan() {
		var row ExecutionRowJSON
		err := json.Unmarshal(s.Bytes(), &row)
		require.NoError(t, err)
		executions = append(executions, &row)
	}
	require.NoError(t, s.Err())
	return executions
}

type Simulation struct {
	name string

	routingStrategy string

	runnerPoolEvictionStrategy string
	// Used by probabilistic_fixed runner pool eviction strategy
	evictionProbability float64

	// Routing strategy params. Not all params apply to all strategies.

	// Only used by history_based strategy.
	historySize int

	// Only used by frequency_weighted strategy.
	// Time period after which weights should decay by half.
	weightHalfLife time.Duration
	// Only used by frequency_weighted strategy.
	// Whether to subtract weight before probing.
	// The idea here is that if we send a probe and our runner hit rate
	// is (optimistically) high, then we should subtract some weight
	// to account for the fact that one runner on the executor will be occupied.
	subtractWeightOnProbe bool

	// Metrics.

	tasksCompleted           atomic.Int64
	idleExecutorTasksPulled  atomic.Int64
	totalQueueDurationNanos  atomic.Int64
	totalWorkerDurationNanos atomic.Int64
	coldRunnerOverheadNanos  atomic.Int64
	runnersEvicted           atomic.Int64
	runnerHitCount           atomic.Int64
	runnerRequestCount       atomic.Int64

	scheduler *Scheduler
}

type Scheduler struct {
	sim *Simulation

	executors         []*Executor
	executorsByHostID map[string]*Executor
	executorCH        *consistent_hash.ConsistentHash

	mu     sync.RWMutex
	tasks  map[string]*Task
	leased map[string]bool

	// Only used by "affinity" strategy
	recentWorkersByAffinityKey map[string][]*Executor

	// Only used by adaptive consistent hash strategy:
	taskFrequencyByAffinityKey            map[string]float64
	taskFrequencyByAffinityKeyLastUpdated time.Time

	// Only used by frequency weighting strategy:
	// taskKey -> executorHostID -> weight
	executorWeightsByAffinityKey            map[string]map[string]int64
	executorWeightsLastUpdatedByAffinityKey map[string]time.Time
}

func NewScheduler(sim *Simulation) *Scheduler {
	return &Scheduler{
		sim:    sim,
		tasks:  make(map[string]*Task),
		leased: make(map[string]bool),
		// history_based + affinity_router
		recentWorkersByAffinityKey: make(map[string][]*Executor),
		// frequency_weighting
		executorWeightsByAffinityKey:            make(map[string]map[string]int64),
		executorWeightsLastUpdatedByAffinityKey: make(map[string]time.Time),
		// adaptive_subpools
		taskFrequencyByAffinityKey: make(map[string]float64),
	}
}

func (s *Scheduler) SetExecutors(executors []*Executor) {
	s.executors = executors
	s.executorsByHostID = make(map[string]*Executor, len(executors))
	for _, executor := range executors {
		s.executorsByHostID[executor.HostID] = executor
	}
	executorCH := consistent_hash.NewConsistentHash(consistent_hash.SHA256, 4096)
	var executorIDs []string
	for _, executor := range executors {
		executorIDs = append(executorIDs, executor.HostID)
	}
	executorCH.Set(executorIDs...)
	s.executorCH = executorCH
}

func (s *Scheduler) Enqueue(ctx context.Context, task *Task) error {
	s.mu.Lock()
	s.tasks[task.ID] = task
	s.leased[task.ID] = false
	s.mu.Unlock()

	executors := s.route(task)
	for _, executor := range executors {
		err := simulateRPCRoundTrip(ctx, executorSchedulerRTT, func() {
			executor.Enqueue(task)
		})
		if err != nil {
			return err // context cancelled
		}
	}
	return nil
}

// Returns 3 probes for routing the task
// TODO: simulate redis RTT for both read + update
func (s *Scheduler) route(task *Task) []*Executor {
	executors := slices.Clone(s.executors)

	switch s.sim.routingStrategy {
	case "random":
		return s.routeRandomly(task)

	case "affinity_router":
		key := getAffinityRouterKey(task)
		s.mu.RLock()
		recent := s.recentWorkersByAffinityKey[key]
		if len(recent) == 0 {
			s.mu.RUnlock()
			return s.routeRandomly(task)
		}
		affinityNode := recent[0]
		s.mu.RUnlock()
		// Shuffle remaining nodes then return the first 2 after the
		// affinity node
		executors = slices.DeleteFunc(executors, func(e *Executor) bool {
			return e == affinityNode
		})
		rand.Shuffle(len(executors), func(i, j int) {
			executors[i], executors[j] = executors[j], executors[i]
		})
		return append([]*Executor{affinityNode}, executors[:min(2, len(executors))]...)

	case "persistent_worker_router":
		// Note: no locking required
		key := task.PersistentWorkerKey
		if key == "" {
			return s.routeRandomly(task)
		}
		sort.Slice(executors, func(i, j int) bool {
			return hash.MemHashString(key+executors[i].HostID) < hash.MemHashString(key+executors[j].HostID)
		})
		return executors[:min(3, len(executors))]

	case "history_based":
		taskAffinityKey := getTaskAffinityKey(task)
		if taskAffinityKey == "" {
			return s.routeRandomly(task)
		}

		// Pop up to 3 of the last unique executors that executed a similar
		// task
		var probes []*Executor
		s.mu.Lock()
		recent := s.recentWorkersByAffinityKey[taskAffinityKey]
		for len(recent) > 0 && len(probes) < 3 {
			// Pop
			r0 := recent[0]
			recent = recent[1:]
			// Add to probes list if not already added
			if !slices.Contains(probes, r0) {
				probes = append(probes, r0)
			}
		}
		// Remove only the first probe from the list since that executor will
		// most likely get to execute the task simply because it's probed
		// first.
		// TODO: try removing a random one instead? We don't really know
		// which one will get the task...
		if len(probes) > 0 {
			s.recentWorkersByAffinityKey[taskAffinityKey] = s.recentWorkersByAffinityKey[taskAffinityKey][1:]
		}
		s.mu.Unlock()
		// If we need more probes to reach the minimum of 3 probes, then shuffle
		// the remaining executors and route randomly.
		if need := 3 - len(probes); need > 0 {
			executors = slices.DeleteFunc(executors, func(e *Executor) bool {
				return slices.Contains(probes, e)
			})
			rand.Shuffle(len(executors), func(i, j int) {
				executors[i], executors[j] = executors[j], executors[i]
			})
			probes = append(probes, executors[:min(len(executors), need)]...)
		}
		return probes

	case "frequency_weighted":
		taskAffinityKey := getTaskAffinityKey(task)
		if taskAffinityKey == "" {
			return s.routeRandomly(task)
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		executorWeights := s.executorWeightsByAffinityKey[taskAffinityKey]
		if executorWeights == nil {
			return s.routeRandomly(task)
		}

		// Decay weights according to how much time has elapsed. Note that this
		// doesn't affect the weighted random shuffle since we're scaling all
		// weights by the same factor.
		decayWeights(executorWeights, s.executorWeightsLastUpdatedByAffinityKey[taskAffinityKey], s.sim.weightHalfLife)

		weightFn := func(executor *Executor) int64 {
			return int64(executorWeights[executor.HostID])
		}
		executors = wrand.Shuffle(executors, weightFn)
		probes := executors[:min(3, len(executors))]

		// Ideally we'd decrement the weight of whichever executor gets the
		// task. We don't know which one will get the task at this point, so
		// optimistically let's assume it's equally likely each one will get the
		// task. To model this, distribute the weight decrement evenly across
		// each probed executor.
		//
		// TODO: in practice it's probably more likely that the first probe
		// gets the task, just because probes are sent in order. Might make
		// sense to slightly bias the weight reduction towards the earlier
		// probes here.
		if s.sim.subtractWeightOnProbe {
			weightReduction := executorWeightIncrement / int64(len(probes))
			for _, executor := range probes {
				executorWeights[executor.HostID] -= weightReduction
				executorWeights[executor.HostID] = max(0, executorWeights[executor.HostID])
			}
		}
		s.executorWeightsLastUpdatedByAffinityKey[taskAffinityKey] = time.Now()
		return probes

	case "adaptive_subpools":
		taskAffinityKey := getTaskAffinityKey(task)
		if taskAffinityKey == "" {
			return s.routeRandomly(task)
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		// Compute relative frequency of the task key.
		var keyFreq, totalFreq float64
		for k, v := range s.taskFrequencyByAffinityKey {
			totalFreq += v
			if k == taskAffinityKey {
				keyFreq = v
			}
		}
		if keyFreq == 0 || totalFreq == 0 {
			return s.routeRandomly(task)
		}
		keyRelativeFreq := keyFreq / totalFreq
		// Total number of tickets available to all task keys; i.e. the max
		// number of times the consistent hash ring can be queried per task.
		// This also determines the max "subpool" size.
		const availableTickets = 1024
		// Each key gets assigned a number of tickets based on how frequent
		// they are. More frequent tasks can probe the ring more times.
		keyTickets := int(keyRelativeFreq * float64(availableTickets))
		// Allow at least one affinity probe, in case the key is dwarfed by
		// other keys.
		keyTickets = max(1, keyTickets)
		var probes []*Executor
		// Query the consistent hash `keyTickets` times, hashing the task key
		// with the ticket number so that each ticket queries a different spot
		// on the ring and has a chance to be matched with a different executor.
		for i := range keyTickets {
			executorHostID := s.executorCH.Get(hash.Strings(strconv.Itoa(i), taskAffinityKey))
			executor := s.executorsByHostID[executorHostID]
			if !slices.Contains(probes, executor) {
				probes = append(probes, executor)
				if len(probes) == 3 {
					break
				}
			}
		}
		// If we didn't get 3 probes, assign the remaining ones randomly.
		if len(probes) < 3 {
			executors = slices.DeleteFunc(executors, func(e *Executor) bool {
				return slices.Contains(probes, e)
			})
			rand.Shuffle(len(executors), func(i, j int) {
				executors[i], executors[j] = executors[j], executors[i]
			})
			probes = append(probes, executors[:min(len(executors), 3-len(probes))]...)
		}
		return probes

	default:
		panic("invalid routing strategy: " + s.sim.routingStrategy)
	}
}

func (s *Scheduler) updateRouter(executor *Executor, task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.sim.routingStrategy {
	case "affinity_router":
		taskAffinityKey := getAffinityRouterKey(task)
		if taskAffinityKey == "" {
			return
		}
		s.recentWorkersByAffinityKey[taskAffinityKey] = []*Executor{executor}

	case "history_based":
		taskAffinityKey := getTaskAffinityKey(task)
		if taskAffinityKey == "" {
			return
		}
		s.recentWorkersByAffinityKey[taskAffinityKey] = append(
			[]*Executor{executor},
			s.recentWorkersByAffinityKey[taskAffinityKey]...,
		)
		if len(s.recentWorkersByAffinityKey[taskAffinityKey]) > s.sim.historySize {
			s.recentWorkersByAffinityKey[taskAffinityKey] = s.recentWorkersByAffinityKey[taskAffinityKey][:s.sim.historySize]
		}

	case "frequency_weighted":
		taskAffinityKey := getTaskAffinityKey(task)
		if taskAffinityKey == "" {
			return
		}
		executorWeights := s.executorWeightsByAffinityKey[taskAffinityKey]
		if executorWeights == nil {
			executorWeights = make(map[string]int64)
			s.executorWeightsByAffinityKey[taskAffinityKey] = executorWeights
		}

		decayWeights(executorWeights, s.executorWeightsLastUpdatedByAffinityKey[taskAffinityKey], s.sim.weightHalfLife)
		executorWeights[executor.HostID] += executorWeightIncrement
		s.executorWeightsLastUpdatedByAffinityKey[taskAffinityKey] = time.Now()

	case "adaptive_subpools":
		taskAffinityKey := getTaskAffinityKey(task)
		if taskAffinityKey == "" {
			return
		}
		// Decay weights _then_ increment the frequency.
		decayWeights(s.taskFrequencyByAffinityKey, s.taskFrequencyByAffinityKeyLastUpdated, s.sim.weightHalfLife)
		s.taskFrequencyByAffinityKey[taskAffinityKey] += 1
		s.taskFrequencyByAffinityKeyLastUpdated = time.Now()
	}
}

func getAffinityRouterKey(task *Task) string {
	return hash.Strings(task.PlatformHash, task.OutputPath)
}

func decayWeights[T ~int64 | ~float64](m map[string]T, lastUpdated time.Time, halfLife time.Duration) {
	if halfLife <= 0 || lastUpdated.IsZero() {
		return
	}
	decayFactor := math.Pow(2, -float64(time.Since(lastUpdated))/float64(halfLife))
	for key, value := range m {
		m[key] = T(float64(value) * decayFactor)
	}
	// TODO: delete entries for very small weights
}

func sumValues[K comparable, V ~int64 | ~float64](m map[K]V) V {
	var sum V
	for _, value := range m {
		sum += value
	}
	return sum
}

func getTaskAffinityKey(task *Task) string {
	// TODO: simulate cache so that we can evaluate cache affinity benefits
	// even if there is no recycling key set.
	if !task.RecycleRunner {
		return ""
	}
	return task.RunnerKey()
}

func (s *Scheduler) routeRandomly(task *Task) []*Executor {
	executors := slices.Clone(s.executors)
	rand.Shuffle(len(executors), func(i, j int) {
		executors[i], executors[j] = executors[j], executors[i]
	})
	return executors[:min(3, len(executors))]
}

func (e *Scheduler) SampleUnclaimedTasks() []*Task {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Get all task IDs
	taskIDs := slices.Collect(maps.Keys(e.tasks))

	// Filter out leased tasks

	{
		var unclaimed []string
		for _, taskID := range taskIDs {
			if !e.leased[taskID] {
				unclaimed = append(unclaimed, taskID)
			}
		}
		taskIDs = unclaimed
	}

	// Shuffle
	rand.Shuffle(len(taskIDs), func(i, j int) {
		taskIDs[i], taskIDs[j] = taskIDs[j], taskIDs[i]
	})

	// Return up to tasksToSample tasks
	taskIDs = taskIDs[:min(tasksToSample, len(taskIDs))]
	tasks := make([]*Task, len(taskIDs))
	for i, taskID := range taskIDs {
		tasks[i] = e.tasks[taskID]
	}
	return tasks
}

type TaskLease = Task

func (e *Scheduler) Lease(taskID string) (close func(), ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	_, ok = e.tasks[taskID]
	if !ok {
		return nil, false // Task doesn't exist
	}
	if e.leased[taskID] {
		return nil, false // Task is already leased
	}

	// Mark the task leased
	e.leased[taskID] = true

	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.leased, taskID)
		delete(e.tasks, taskID)
	}, true
}

type Executor struct {
	sim *Simulation

	HostID string

	scheduler  *Scheduler
	runnerPool *RunnerPool

	enqueueCh      chan *Task
	resourcesFreed chan struct{}

	ResourcesUsed    [2]int64 // [CPU, Memory]
	ResourceCapacity [2]int64 // [CPU, Memory]

	mu    sync.RWMutex
	queue []*Task
}

func NewExecutor(sim *Simulation, scheduler *Scheduler) *Executor {
	return &Executor{
		sim: sim,

		HostID: uuid.New(),

		scheduler:  scheduler,
		runnerPool: &RunnerPool{sim: sim},

		enqueueCh:      make(chan *Task, 128),
		resourcesFreed: make(chan struct{}, 1),

		ResourcesUsed:    [2]int64{0, 0},
		ResourceCapacity: [2]int64{*executorMilliCPU, *executorMemoryBytes},
	}
}

func (e *Executor) Run(ctx context.Context) {
	// Every second, if we're idle, increment the idle ticker, otherwise reset
	// the idle ticker.
	idleTicker := time.NewTicker(1 * time.Second)
	defer idleTicker.Stop()
	idleSeconds := 0

	// Every so often, check if we're idle, and if so, request more work.
	// Start at 5s, but if we don't get any work then we'll back off below.
	const initialRequestWorkDelay = 5 * time.Second
	requestWorkDelay := initialRequestWorkDelay
	requestWorkTicker := time.NewTicker(requestWorkDelay)
	defer requestWorkTicker.Stop()

	var lastWorkTime time.Time

	for {
		select {
		case <-ctx.Done():
			debugln("ctx Done")
			return

		// If enqueueCh receives a task, add it to the queue and try to schedule
		// it.
		case task := <-e.enqueueCh:
			e.mu.Lock()
			e.queue = append(e.queue, task)
			// l := len(e.queue)
			e.mu.Unlock()
			// if l >= 100 && l%50 == 0 {
			// 	fmt.Printf("qlen=%d (host=%s)\n", l, e.HostID)
			// }

			// Simulate EnqueueTaskReservationResponse resetting the idle work
			// request backoff
			lastWorkTime = time.Time{}
			e.trySchedule(ctx)
		// If resources are freed due to a completed execution, try to schedule
		// more queued tasks.
		case <-e.resourcesFreed:
			e.trySchedule(ctx)

		// Every second, check whether we're idle.
		case <-idleTicker.C:
			if e.HasExcessCapacity() {
				idleSeconds++
			} else {
				idleSeconds = 0
			}

		case <-requestWorkTicker.C:
			// Don't request more work unless we've gone at least 5 seconds of
			// being idle.
			if idleSeconds < 5 {
				continue
			}
			// Simulate scheduler
			if lastWorkTime.IsZero() {
				lastWorkTime = time.Now()
			}
			timeSinceLastWork := time.Since(lastWorkTime)
			lastWorkTime = time.Now()

			tasks := e.scheduler.SampleUnclaimedTasks()
			if len(tasks) == 0 {
				// No unclaimed tasks are available - back off.
				newDelay := clamp(timeSinceLastWork*2, 5*time.Second, time.Minute)
				requestWorkTicker.Reset(newDelay)
				continue
			}
			for _, task := range tasks {
				e.Enqueue(task)
			}
			e.sim.idleExecutorTasksPulled.Add(int64(len(tasks)))
		}
	}
}

func (e *Executor) trySchedule(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for len(e.queue) > 0 {
		task := e.queue[0]
		if !e.canFit(task) {
			// Stop trying to schedule.
			break
		}

		// Remove the task from the queue.
		e.queue = e.queue[1:]

		// Add the task's resources to the executor's used resources.
		for i := range e.ResourcesUsed {
			e.ResourcesUsed[i] += task.Resources[i]
		}
		freeResources := func() {
			e.mu.Lock()
			for i := range e.ResourcesUsed {
				e.ResourcesUsed[i] -= task.Resources[i]
			}
			e.mu.Unlock()
			select {
			case e.resourcesFreed <- struct{}{}:
			default:
			}
		}
		go func() {
			defer freeResources()

			// Try to lease the task. If it's already claimed, just drop the task.

			var closeLease func()
			var ok bool
			err := simulateRPCRoundTrip(ctx, executorSchedulerRTT, func() {
				closeLease, ok = e.scheduler.Lease(task.ID)
			})
			if err != nil || !ok {
				return
			}

			// Once the task finishes executing, simulate a scheduler RPC to
			// close the lease and update the task router.
			defer func() {
				simulateRPCRoundTrip(ctx, executorSchedulerRTT, func() {
					closeLease()
					e.scheduler.updateRouter(e, task)
				})
			}()

			// Execute the task and record stats
			start := time.Now()
			task.StartedAt = start
			// fmt.Println("Task queued for", time.Since(task.QueuedAt), "(@", task.QueuedAt, ", now=", start, ")")
			e.sim.totalQueueDurationNanos.Add(time.Since(task.QueuedAt).Nanoseconds())
			e.execute(task)
			task.CompletedAt = time.Now()
			close(task.Done)
			e.sim.totalWorkerDurationNanos.Add(time.Since(start).Nanoseconds())
			e.sim.tasksCompleted.Add(1)
		}()
	}
}

func (e *Executor) execute(task *Task) {
	// Get runner from pool
	r := e.runnerPool.getRunner(task)
	defer e.runnerPool.returnRunner(r)

	// Simulate running the task by sleeping for cold duration or warm duration
	// depending on whether the runner is cold or warm.
	var sleep time.Duration
	if r.TaskNumber > 1 {
		sleep = task.WarmDuration
	} else {
		sleep = task.ColdDuration
		e.sim.coldRunnerOverheadNanos.Add(int64(task.ColdDuration - task.WarmDuration))
	}
	time.Sleep(sleep)
}

type RunnerPool struct {
	sim *Simulation

	mu      sync.RWMutex
	runners []*Runner
}

func (p *RunnerPool) getRunner(task *Task) *Runner {
	if !task.Recyclable() {
		return &Runner{
			TaskNumber: 1,
			Recyclable: false,
			Active:     true,
		}
	}

	p.sim.runnerRequestCount.Add(1)

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.runners {
		if r.Active || r.Key != task.RunnerKey() {
			// Runner is in use, or doesn't match the task
			continue
		}
		// Update runner state and return it
		r.Active = true
		r.TaskNumber++
		p.sim.runnerHitCount.Add(1)
		return r
	}

	r := &Runner{
		TaskNumber: 1,
		Recyclable: true,
		Active:     true,
		Key:        task.RunnerKey(),
	}
	p.runners = append(p.runners, r)
	return r
}

func (p *RunnerPool) returnRunner(r *Runner) {
	if !r.Recyclable {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// If slice is too large, remove a paused runner according to the eviction
	// strategy:
	// - default: LRU (first) runner
	// - random: random runner. Higher frequency keys are more likely to be
	//   targeted for eviction.
	if p.pausedRunnerCount() > *runnerPoolMaxCount {
		evictIndex := -1
		switch p.sim.runnerPoolEvictionStrategy {
		case "", "default":
			// LRU runner
			for i, rr := range p.runners {
				if !rr.Active {
					evictIndex = i
					break
				}
			}

		case "random":
			// Random runner
			var pausedIdxs []int
			for i, rr := range p.runners {
				if !rr.Active {
					pausedIdxs = append(pausedIdxs, i)
				}
			}
			if len(pausedIdxs) > 0 {
				evictIndex = pausedIdxs[rand.IntN(len(pausedIdxs))]
			}

		case "diversity":
			// Highest frequency runner (i.e. runner with the highest
			// representation of its runner key in the pool), tie-breaking by
			// LRU
			var maxFreq int64
			freq := map[string]int64{}
			for _, rr := range p.runners {
				if !rr.Active {
					freq[rr.Key]++
					maxFreq = max(maxFreq, freq[rr.Key])
				}
			}
			for i, rr := range p.runners {
				if !rr.Active && freq[rr.Key] == maxFreq {
					evictIndex = i
					break
				}
			}

		case "probabilistic_fixed":
			// LRU runner but only with a small, fixed probability
			if rand.Float64() < p.sim.evictionProbability {
				for i, rr := range p.runners {
					if !rr.Active {
						evictIndex = i
						break
					}
				}
			} else {
				// Evict *this* runner
				evictIndex = slices.Index(p.runners, r)
			}
		}

		if evictIndex < 0 {
			panic("could not find paused runner to evict - should not happen")
		}
		// Evict
		p.runners = append(p.runners[:evictIndex], p.runners[evictIndex+1:]...)
		p.sim.runnersEvicted.Add(1)
	}

	// Shift this runner to the end of the list, unless it was evicted
	ri := slices.Index(p.runners, r)
	if ri < 0 {
		return
	}
	p.runners = append(p.runners[:ri], p.runners[ri+1:]...)
	p.runners = append(p.runners, r)
	// Mark this runner paused
	r.Active = false
}
func (p *RunnerPool) pausedRunnerCount() (count int) {
	for _, r := range p.runners {
		if !r.Active {
			count++
		}
	}
	return
}

func (e *Executor) HasExcessCapacity() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for i := range e.ResourceCapacity {
		fractionUsed := float64(e.ResourcesUsed[i]) / float64(e.ResourceCapacity[i])
		if fractionUsed > *excessCapacityThreshold {
			return false
		}
	}

	return true
}

func (e *Executor) Enqueue(task *Task) {
	e.enqueueCh <- task
}

func (e *Executor) canFit(task *Task) bool {
	for r := range e.ResourceCapacity {
		if task.Resources[r]+e.ResourcesUsed[r] > e.ResourceCapacity[r] {
			return false
		}
	}
	return true
}

type Runner struct {
	TaskNumber int64
	Recyclable bool
	Active     bool
	Key        string
}

const (
	ResourceCPU    = 0
	ResourceMemory = 1
)

type Task struct {
	QueuedAt    time.Time
	StartedAt   time.Time
	CompletedAt time.Time

	ID        string
	Resources [2]int64 // [CPU, Memory]

	ActionMnemonic string
	TargetLabel    string
	OutputPath     string

	RecycleRunner       bool
	PlatformHash        string
	PersistentWorkerKey string

	ColdDuration time.Duration
	WarmDuration time.Duration

	Done chan struct{}
}

func (t *Task) Recyclable() bool {
	return t.RecycleRunner || t.PersistentWorkerKey != ""
}
func (t *Task) RunnerKey() string {
	return t.PlatformHash + "|" + t.PersistentWorkerKey
}

func clamp[T cmp.Ordered](d, lower, upper T) T {
	return min(max(d, lower), upper)
}

func simulateRPCRoundTrip(ctx context.Context, rtt time.Duration, f func()) error {
	if err := sleep(ctx, rtt/2); err != nil {
		return err
	}
	f()
	if err := sleep(ctx, rtt/2); err != nil {
		return err
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// hashToFloat computes a hash and normalizes its value to the range (0.0, 1.0].
// We use xxhash for its excellent speed and distribution.
func hashToFloat(executorID, key string) float64 {
	h := xxhash.New()
	// Write both executor and key to the hasher to get a unique score for the pair.
	h.WriteString(executorID)
	h.WriteString(key)
	hashVal := h.Sum64()

	// Normalize the uint64 hash to a float64 between (0.0, 1.0].
	// We add 1 to the numerator and denominator to avoid a 0.0 value,
	// which would cause -log(0) to be infinity.
	return float64(hashVal+1) / float64(math.MaxUint64+1)
}

// hashToUint64 is a simple helper for getting a hash value.
func hashToUint64(s string) uint64 {
	return xxhash.Sum64String(s)
}
