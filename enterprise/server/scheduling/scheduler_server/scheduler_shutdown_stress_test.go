package scheduler_server

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var shutdownStressEnabled = flag.Bool("run_shutdown_stress", false, "Whether to run the scheduler shutdown stress tests. Run with --@io_bazel_rules_go//go/config:race to detect scheduler code that runs after shutdown.")

// These tests stress the scheduler's shutdown contract: once the health
// checker's shutdown functions have returned, no scheduler code should touch
// shared state. The main detector is the race detector: after shutdown
// returns, the test keeps writing the unclaimed tasks cache TTL flag, so any
// scheduler code still reading it is reported as a data race.

// zrangeStaller delays ZRANGE commands, optionally ignoring cancellation.
type zrangeStaller struct {
	delay    time.Duration
	honorCtx bool
}

func (s *zrangeStaller) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if cmd.Name() != "zrange" {
		return ctx, nil
	}
	if !s.honorCtx {
		time.Sleep(s.delay)
		return ctx, nil
	}
	select {
	case <-time.After(s.delay):
		return ctx, nil
	case <-ctx.Done():
		return ctx, ctx.Err()
	}
}
func (s *zrangeStaller) AfterProcess(ctx context.Context, cmd redis.Cmder) error { return nil }
func (s *zrangeStaller) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}
func (s *zrangeStaller) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return nil
}

type stressConfig struct {
	executors int
	tasks     int
	// If set, each executor sends AskForMoreWork requests in a tight loop.
	askForMore bool
	cacheTTL   time.Duration
	// Upper bound on the random delay between starting executors and
	// initiating shutdown.
	maxShutdownDelay time.Duration
	// Value for --max_shutdown_duration.
	maxShutdownDuration time.Duration
	redisHook           redis.Hook
	// Shutdown must complete within this long.
	shutdownDeadline time.Duration
	// Require that no scheduler goroutines outlive shutdown, and write the
	// TTL flag after shutdown returns, so that any scheduler code still
	// reading it shows up as a data race. Disable for scenarios where
	// goroutines are known to outlive shutdown by design (timeouts).
	expectCleanShutdown bool
	// How long to wait after the test for stragglers to finish, so they
	// don't pollute later tests.
	drain time.Duration
}

// stressExecutor registers with the scheduler and drains its stream,
// optionally spamming AskForMoreWork requests. Unlike fakeExecutor, it
// tolerates any errors, since the scheduler may shut down at any point.
func stressExecutor(ctx context.Context, client scpb.SchedulerClient, id string, askForMore bool) {
	stream, err := client.RegisterAndStreamWork(ctx)
	if err != nil {
		return
	}
	node := &scpb.ExecutionNode{
		ExecutorId:            id,
		OsFamily:              defaultOS,
		Arch:                  defaultArch,
		Host:                  "stress",
		AssignableMemoryBytes: 64_000_000_000,
		AssignableMilliCpu:    32_000,
	}
	if err := stream.Send(&scpb.RegisterAndStreamWorkRequest{RegisterExecutorRequest: &scpb.RegisterExecutorRequest{Node: node}}); err != nil {
		return
	}
	// Acknowledge reservations so that ScheduleTask doesn't wait on them.
	// Only this goroutine sends on the stream.
	acks := make(chan string, 1024)
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			if req := msg.GetEnqueueTaskReservationRequest(); req != nil {
				select {
				case acks <- req.GetTaskId():
				default:
				}
			}
		}
	}()
	var ask <-chan time.Time
	if askForMore {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		ask = ticker.C
	}
	for {
		var req *scpb.RegisterAndStreamWorkRequest
		select {
		case <-recvDone:
			return
		case taskID := <-acks:
			req = &scpb.RegisterAndStreamWorkRequest{EnqueueTaskReservationResponse: &scpb.EnqueueTaskReservationResponse{TaskId: taskID}}
		case <-ask:
			req = &scpb.RegisterAndStreamWorkRequest{AskForMoreWorkRequest: &scpb.AskForMoreWorkRequest{}}
		}
		if err := stream.Send(req); err != nil {
			return
		}
	}
}

var goroutineHeader = regexp.MustCompile(`^goroutine \d+ \[([^\]]*)\]`)

// schedulerGoroutines returns a summary of live goroutines that are running
// scheduler_server.go code, keyed by the innermost scheduler function.
func schedulerGoroutines() map[string]int {
	buf := make([]byte, 16<<20)
	buf = buf[:runtime.Stack(buf, true)]
	out := map[string]int{}
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		lines := strings.Split(g, "\n")
		state := ""
		if m := goroutineHeader.FindStringSubmatch(lines[0]); m != nil {
			state = m[1]
		}
		for i := 1; i+1 < len(lines); i++ {
			if strings.Contains(lines[i+1], "scheduler_server/scheduler_server.go:") {
				fn := lines[i]
				if idx := strings.LastIndex(fn, "("); idx > 0 {
					fn = fn[:idx]
				}
				fn = strings.TrimPrefix(fn, "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/")
				out[fmt.Sprintf("%s [%s]", fn, state)]++
				break
			}
		}
	}
	return out
}

func runShutdownStress(t *testing.T, cfg stressConfig) {
	flags.Set(t, "remote_execution.unclaimed_tasks_cache_ttl", cfg.cacheTTL)
	if cfg.maxShutdownDuration > 0 {
		flags.Set(t, "max_shutdown_duration", cfg.maxShutdownDuration)
	}
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	execCtx, cancelExecutors := context.WithCancel(authenticatedContext(t, env, "user2"))
	defer cancelExecutors()
	var wg sync.WaitGroup
	// ScheduleTask requires at least one registered executor.
	wg.Go(func() {
		stressExecutor(execCtx, env.GetSchedulerClient(), "seed", false /*=askForMore*/)
	})
	scheduleCtx, cancelSchedule := context.WithTimeout(ctx, 30*time.Second)
	defer cancelSchedule()
	for {
		_, err := env.GetSchedulerService().ScheduleTask(scheduleCtx, newScheduleRequest(scheduleCtx, t, env, scheduleOpts{}))
		if err == nil {
			break
		}
		require.NoError(t, scheduleCtx.Err(), "seed executor never became schedulable: %s", err)
		time.Sleep(50 * time.Millisecond)
	}
	for range cfg.tasks {
		scheduleTask(scheduleCtx, t, env, map[string]string{})
	}
	cancelSchedule()
	if cfg.redisHook != nil {
		env.GetRemoteExecutionRedisClient().AddHook(cfg.redisHook)
	}

	for i := range cfg.executors {
		wg.Go(func() {
			stressExecutor(execCtx, env.GetSchedulerClient(), fmt.Sprintf("stress-%d", i), cfg.askForMore)
		})
	}

	time.Sleep(time.Duration(rand.Int63n(int64(cfg.maxShutdownDelay) + 1)))

	hc := env.GetHealthChecker()
	start := time.Now()
	hc.Shutdown()
	hc.WaitForGracefulShutdown()
	elapsed := time.Since(start)
	t.Logf("shutdown took %s", elapsed)

	if cfg.expectCleanShutdown {
		// Any scheduler code reading the flag from here on races with
		// these writes.
		for i := range 50 {
			ttl := time.Duration(i%2) * time.Second
			require.NoError(t, flagutil.SetValueForFlagName("remote_execution.unclaimed_tasks_cache_ttl", ttl, nil, false))
			time.Sleep(2 * time.Millisecond)
		}
		require.NoError(t, flagutil.SetValueForFlagName("remote_execution.unclaimed_tasks_cache_ttl", cfg.cacheTTL, nil, false))
	}

	// Let goroutines unblocked by shutdown finish exiting, then report
	// whatever scheduler code is still running. Goroutines tied to executor
	// streams exit shortly after shutdown ends the streams.
	leftovers := schedulerGoroutines()
	for deadline := time.Now().Add(2 * time.Second); len(leftovers) > 0 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
		leftovers = schedulerGoroutines()
	}
	keys := make([]string, 0, len(leftovers))
	for k := range leftovers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("STRESS leftover scheduler goroutine after shutdown: %dx %s", leftovers[k], k)
	}
	if cfg.expectCleanShutdown {
		require.Empty(t, leftovers, "scheduler goroutines still running after shutdown")
	}

	cancelExecutors()
	wg.Wait()
	time.Sleep(cfg.drain)

	require.Less(t, elapsed, cfg.shutdownDeadline, "shutdown took too long")
}

// Run with:
//
//	bazel test //enterprise/server/scheduling/scheduler_server:scheduler_server_test \
//	  --@io_bazel_rules_go//go/config:race \
//	  --test_filter=TestShutdownStress --test_arg=--run_shutdown_stress
func TestShutdownStress(t *testing.T) {
	if !*shutdownStressEnabled {
		t.Skip("set --run_shutdown_stress to run")
	}
	for _, tc := range []struct {
		name string
		cfg  stressConfig
	}{
		{
			name: "join_storm_ttl0",
			cfg: stressConfig{
				executors: 30, tasks: 50, cacheTTL: 0,
				maxShutdownDelay: 100 * time.Millisecond,
				shutdownDeadline: 5 * time.Second, expectCleanShutdown: true,
			},
		},
		{
			name: "join_storm_ttl1s",
			cfg: stressConfig{
				executors: 30, tasks: 50, cacheTTL: time.Second,
				maxShutdownDelay: 100 * time.Millisecond,
				shutdownDeadline: 5 * time.Second, expectCleanShutdown: true,
			},
		},
		{
			name: "ask_for_more_storm",
			cfg: stressConfig{
				executors: 10, tasks: 50, cacheTTL: 0, askForMore: true,
				maxShutdownDelay: 200 * time.Millisecond,
				shutdownDeadline: 5 * time.Second, expectCleanShutdown: true,
			},
		},
		{
			name: "slow_redis_honors_ctx",
			cfg: stressConfig{
				executors: 10, tasks: 10, cacheTTL: 0,
				redisHook:        &zrangeStaller{delay: 5 * time.Second, honorCtx: true},
				maxShutdownDelay: 100 * time.Millisecond,
				shutdownDeadline: 2 * time.Second, expectCleanShutdown: true,
			},
		},
		{
			name: "hung_redis_ignores_ctx",
			cfg: stressConfig{
				executors: 10, tasks: 10, cacheTTL: 0,
				redisHook:           &zrangeStaller{delay: 3 * time.Second, honorCtx: false},
				maxShutdownDelay:    100 * time.Millisecond,
				maxShutdownDuration: time.Second,
				shutdownDeadline:    2500 * time.Millisecond,
				drain:               4 * time.Second,
			},
		},
	} {
		for i := range 5 {
			t.Run(fmt.Sprintf("%s/%d", tc.name, i), func(t *testing.T) {
				runShutdownStress(t, tc.cfg)
			})
		}
	}
}
