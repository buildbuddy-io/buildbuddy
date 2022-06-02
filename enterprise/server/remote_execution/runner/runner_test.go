package runner

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

const (
	unlimited = -1

	sysMemoryBytes = tasksize.DefaultMemEstimate * 10
	sysMilliCPU    = tasksize.DefaultCPUEstimate * 10
)

var (
	defaultCfg = &RunnerPoolOptions{
		MaxRunnerCount:            *maxRunnerCount,
		MaxRunnerDiskSizeBytes:    *maxRunnerDiskSizeBytes,
		MaxRunnerMemoryUsageBytes: *maxRunnerMemoryUsageBytes,
	}

	noLimitsCfg = &RunnerPoolOptions{
		MaxRunnerCount:            unlimited,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	}
)

type RunnerPoolOptions struct {
	MaxRunnerCount            int
	MaxRunnerDiskSizeBytes    int64
	MaxRunnerMemoryUsageBytes int64
}

func newTask() *repb.ExecutionTask {
	return &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"pwd"},
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: platform.RecycleRunnerPropertyName, Value: "true"},
				},
			},
		},
	}
}

func newWorkflowTask() *repb.ExecutionTask {
	t := newTask()
	t.Command.Platform.Properties = append(t.Command.Platform.Properties, &repb.Platform_Property{
		Name: "workflow-id", Value: "WF123",
	})
	return t
}

func newTaskWithAffinityKey(key string) *repb.ExecutionTask {
	t := newTask()
	t.Command.Platform.Properties = append(t.Command.Platform.Properties, &repb.Platform_Property{
		Name: platform.HostedBazelAffinityKeyPropertyName, Value: key,
	})
	return t
}

func newWorkspace(t *testing.T, env *testenv.TestEnv) *workspace.Workspace {
	tmpDir := testfs.MakeTempDir(t)
	ws, err := workspace.New(env, tmpDir, &workspace.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func newUUID(t *testing.T) string {
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func newTestEnv(t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers(
		"US1", "GR1",
		"US2", "GR2",
		"US3", "GR3",
	)))
	return env
}

func withAuthenticatedUser(t *testing.T, ctx context.Context, userID string) context.Context {
	jwt, err := testauth.TestJWTForUserID(userID)
	require.NoError(t, err)
	return context.WithValue(ctx, "x-buildbuddy-jwt", jwt)
}

func mustRun(t *testing.T, r *commandRunner) {
	res := r.Run(context.Background())
	require.NoError(t, res.Error)
}

func newRunnerPool(t *testing.T, env *testenv.TestEnv, cfg *RunnerPoolOptions) *pool {
	flags.Set(t, "executor.runner_pool.max_runner_count", cfg.MaxRunnerCount)
	flags.Set(t, "executor.runner_pool.max_runner_disk_size_bytes", cfg.MaxRunnerDiskSizeBytes)
	flags.Set(t, "executor.runner_pool.max_runner_memory_usage_bytes", cfg.MaxRunnerMemoryUsageBytes)
	p, err := NewPool(env)
	require.NoError(t, err)
	require.NotNil(t, p)
	return p
}

func get(ctx context.Context, p *pool, task *repb.ExecutionTask) (*commandRunner, error) {
	r, err := p.Get(ctx, task)
	if err != nil {
		return nil, err
	}
	return r.(*commandRunner), nil
}

func mustGet(t *testing.T, ctx context.Context, pool *pool, task *repb.ExecutionTask) *commandRunner {
	initialActiveCount := pool.ActiveRunnerCount()
	r, err := get(ctx, pool, task)
	require.NoError(t, err)
	require.Equal(t, initialActiveCount+1, pool.ActiveRunnerCount())
	mustRun(t, r)
	return r
}

func mustAdd(t *testing.T, ctx context.Context, pool *pool, r *commandRunner) {
	initialActiveCount := pool.ActiveRunnerCount()

	err := pool.Add(ctx, r)

	require.NoError(t, err)
	require.Equal(t, initialActiveCount-1, pool.ActiveRunnerCount(), "active runner count should decrease when adding back to pool")
}

func mustAddWithoutEviction(t *testing.T, ctx context.Context, pool *pool, r *commandRunner) {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()

	mustAdd(t, ctx, pool, r)

	require.Equal(
		t, initialPausedCount+1, pool.PausedRunnerCount(),
		"pooled runner count should increase by 1 after adding without eviction",
	)
	require.Equal(
		t, initialCount, pool.RunnerCount(),
		"total runner count (pooled + active) should stay the same after adding without eviction",
	)
}

func mustAddWithEviction(t *testing.T, ctx context.Context, pool *pool, r *commandRunner) {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()

	mustAdd(t, ctx, pool, r)

	require.Equal(
		t, initialPausedCount, pool.PausedRunnerCount(),
		"pooled runner count should stay the same after adding with eviction",
	)
	require.Equal(
		t, initialCount-1, pool.RunnerCount(),
		"total runner count (pooled + active) should decrease by 1 after adding with eviction",
	)
}

func mustGetPausedRunner(t *testing.T, ctx context.Context, pool *pool, task *repb.ExecutionTask) *commandRunner {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()
	r := mustGet(t, ctx, pool, task)
	require.Equal(t, initialPausedCount-1, pool.PausedRunnerCount())
	require.Equal(t, initialCount, pool.RunnerCount())
	return r
}

func mustGetNewRunner(t *testing.T, ctx context.Context, pool *pool, task *repb.ExecutionTask) *commandRunner {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()
	r := mustGet(t, ctx, pool, task)
	require.Equal(t, initialPausedCount, pool.PausedRunnerCount())
	require.Equal(t, initialCount+1, pool.RunnerCount())
	return r
}

func sleepRandMicros(max int64) {
	time.Sleep(time.Duration(rand.Int63n(max) * int64(time.Microsecond)))
}

func TestRunnerPool_CanAddAndGetBackSameRunner(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	r1 := mustGetNewRunner(t, ctx, pool, newTask())

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetPausedRunner(t, ctx, pool, newTask())

	assert.Same(t, r1, r2)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_CannotTakeRunnerFromOtherGroup(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctxUser1 := withAuthenticatedUser(t, context.Background(), "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), "US2")

	r1 := mustGetNewRunner(t, ctxUser1, pool, newTask())

	mustAddWithoutEviction(t, ctxUser1, pool, r1)

	r2 := mustGetNewRunner(t, ctxUser2, pool, newTask())

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_CannotTakeRunnerFromOtherInstanceName(t *testing.T) {
	env := newTestEnv(t)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")
	pool := newRunnerPool(t, env, noLimitsCfg)
	task1 := newTask()
	task1.ExecuteRequest = &repb.ExecuteRequest{InstanceName: "instance/1"}
	task2 := newTask()
	task2.ExecuteRequest = &repb.ExecuteRequest{InstanceName: "instance/2"}

	r1 := mustGetNewRunner(t, ctx, pool, task1)

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetNewRunner(t, ctx, pool, task2)

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_CannotTakeRunnerFromOtherWorkflow(t *testing.T) {
	env := newTestEnv(t)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")
	pool := newRunnerPool(t, env, noLimitsCfg)
	task1 := newTask()
	task1.Command.Platform.Properties = append(
		task1.Command.Platform.Properties,
		&repb.Platform_Property{Name: "workflow-id", Value: "WF1"},
	)
	task2 := newTask()
	task2.Command.Platform.Properties = append(
		task1.Command.Platform.Properties,
		&repb.Platform_Property{Name: "workflow-id", Value: "WF2"},
	)

	r1 := mustGetNewRunner(t, ctx, pool, task1)

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetNewRunner(t, ctx, pool, task2)

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_Shutdown_RemovesAllRunners(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	r := mustGetNewRunner(t, ctx, pool, newTask())

	mustAdd(t, ctx, pool, r)

	err := pool.Shutdown(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, pool.PausedRunnerCount())
	assert.Equal(t, 0, pool.ActiveRunnerCount())
}

func TestRunnerPool_Shutdown_RunnersReturnRetriableOrNilError(t *testing.T) {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	t.Logf("Random seed: %d", seed)

	env := newTestEnv(t)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	// Run 100 trials where we create a pool that runs 50 tasks using runner
	// recycling, shutting down the pool after roughly half of the tasks have been
	// started.
	for i := 0; i < 100; i++ {
		pool := newRunnerPool(t, env, noLimitsCfg)
		numTasks := 50
		tasksStarted := make(chan struct{}, numTasks)
		errs := make(chan error, numTasks)
		runTask := func() error {
			r, err := get(ctx, pool, newTask())
			if err != nil {
				return err
			}
			// Random delay to simulate downloading inputs
			sleepRandMicros(10)
			tasksStarted <- struct{}{}
			if result := r.Run(ctx); result.Error != nil {
				return result.Error
			}
			// Random delay to simulate uploading outputs
			sleepRandMicros(10)
			if err := pool.Add(ctx, r); err != nil {
				return err
			}
			return nil
		}

		var wg sync.WaitGroup
		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- runTask()
			}()
			// Random, tiny delay to stagger the tasks a bit more.
			sleepRandMicros(1)
		}

		nStarted := 0
		for range tasksStarted {
			nStarted++
			if nStarted == numTasks/2 {
				err := pool.Shutdown(ctx)
				require.NoError(t, err)
				break
			}
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			if err == nil || status.IsUnavailableError(err) {
				continue
			}
			require.NoError(t, err, "runner pool shutdown caused non-retriable error")
		}
	}
}

func TestRunnerPool_DefaultSystemBasedLimits_CanAddAtLeastOneRunner(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, defaultCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	r, err := get(ctx, pool, newTask())

	require.NoError(t, err)

	mustRun(t, r)

	err = pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.PausedRunnerCount())
}

func TestRunnerPool_ExceedMaxRunnerCount_OldestRunnerEvicted(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, &RunnerPoolOptions{
		MaxRunnerCount:            2,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	})
	ctxUser1 := withAuthenticatedUser(t, context.Background(), "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), "US2")
	ctxUser3 := withAuthenticatedUser(t, context.Background(), "US3")

	r1 := mustGetNewRunner(t, ctxUser1, pool, newTask())
	r2 := mustGetNewRunner(t, ctxUser2, pool, newTask())
	r3 := mustGetNewRunner(t, ctxUser3, pool, newTask())

	// Limit is 2, so r1 and r2 should be added with no problem.

	mustAddWithoutEviction(t, ctxUser2, pool, r2)
	mustAddWithoutEviction(t, ctxUser1, pool, r1)
	mustAddWithEviction(t, ctxUser3, pool, r3)

	// Should be able to get r1 and r3 back from the pool. r2 should have been
	// evicted since it's the oldest (least recently added back to the pool).

	mustGetPausedRunner(t, ctxUser1, pool, newTask())
	mustGetPausedRunner(t, ctxUser3, pool, newTask())
	mustGetNewRunner(t, ctxUser2, pool, newTask())
}

func TestRunnerPool_DiskLimitExceeded_CannotAdd(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, &RunnerPoolOptions{
		MaxRunnerCount:            unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
		// At least one byte should be needed for the workspace root dir.
		MaxRunnerDiskSizeBytes: 1,
	})
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	r := mustGetNewRunner(t, ctx, pool, newTask())

	err := pool.Add(context.Background(), r)

	assert.True(t, status.IsResourceExhaustedError(err), "should exceed disk limit")
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_ExceedMemoryLimit_OldestRunnerEvicted(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, &RunnerPoolOptions{
		MaxRunnerCount:            unlimited,
		MaxRunnerMemoryUsageBytes: 5200e6,
		MaxRunnerDiskSizeBytes:    unlimited,
	})
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	// Get 3 runners for workflow tasks.
	r1 := mustGetNewRunner(t, ctx, pool, newWorkflowTask())
	r2 := mustGetNewRunner(t, ctx, pool, newWorkflowTask())
	r3 := mustGetNewRunner(t, ctx, pool, newWorkflowTask())

	// Try adding all of them to the pool. 3rd runner should result in eviction,
	// since the estimated memory usage for paused bare runners is 2.6GB, and the
	// pool can only fit 2 * 2.6GB = 5.2GB worth of runners.
	mustAddWithoutEviction(t, ctx, pool, r1)
	mustAddWithoutEviction(t, ctx, pool, r2)
	mustAddWithEviction(t, ctx, pool, r3)

	// Now take one from the pool and put it back; this should not evict.
	r4 := mustGetPausedRunner(t, ctx, pool, newWorkflowTask())
	mustAddWithoutEviction(t, ctx, pool, r4)
}

func TestRunnerPool_ActiveRunnersTakenFromPool_RemovedOnShutdown(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	task := newTask()
	task.Command.Arguments = []string{"sh", "-c", "touch foo.txt && sleep infinity"}
	r, err := get(ctx, pool, task)

	require.NoError(t, err)

	go func() {
		mustRun(t, r)
	}()
	fooPath := path.Join(r.Workspace.Path(), "foo.txt")
	err = disk.WaitUntilExists(ctx, fooPath, disk.WaitOpts{Timeout: 5 * time.Second})
	require.NoError(t, err)

	require.Equal(t, 1, pool.ActiveRunnerCount())

	// Shut down while the runner is active (and still executing).
	err = pool.Shutdown(context.Background())

	require.NoError(t, err)
	require.Equal(t, 0, pool.ActiveRunnerCount())
	_, err = os.Stat(path.Join(r.Workspace.Path(), "foo.txt"))
	require.True(t, os.IsNotExist(err), "runner should have been removed on shutdown")
}

func TestRunnerPool_GetSameRunnerForSameAffinityKey(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	r1 := mustGetNewRunner(t, ctx, pool, newTaskWithAffinityKey("key1"))

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetPausedRunner(t, ctx, pool, newTaskWithAffinityKey("key1"))

	assert.Same(t, r1, r2)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_GetDifferentRunnerForDifferentAffinityKey(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctxUser1 := withAuthenticatedUser(t, context.Background(), "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), "US2")

	r1 := mustGetNewRunner(t, ctxUser1, pool, newTaskWithAffinityKey("key1"))

	mustAddWithoutEviction(t, ctxUser1, pool, r1)

	r2 := mustGetNewRunner(t, ctxUser2, pool, newTaskWithAffinityKey("key2"))

	assert.NotSame(t, r1, r2)
}

func newPersistentRunnerTask(t *testing.T, key, arg, protocol string, resp *wkpb.WorkResponse) *repb.ExecutionTask {
	workerPath := testfs.RunfilePath(t, "enterprise/server/remote_execution/runner/testworker/testworker_/testworker")
	return &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{
				workerPath,
				"--protocol=" + protocol,
				"--response_base64=" + encodedResponse(t, protocol, resp),
				arg,
			},
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "persistentWorkerKey", Value: key},
					{Name: "persistentWorkerProtocol", Value: protocol},
					{Name: platform.RecycleRunnerPropertyName, Value: "true"},
				},
			},
		},
	}
}

func encodedResponse(t *testing.T, protocol string, resp *wkpb.WorkResponse) string {
	buf := []byte{}
	if protocol == "json" {
		out, err := protojson.Marshal(resp)
		buf = append(buf, out...)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		out, err := proto.Marshal(resp)
		size := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(size, uint64(len(out)))
		buf = append(append(buf, size[:n]...), out...)
		if err != nil {
			t.Fatal(err)
		}
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func TestRunnerPool_PersistentWorker(t *testing.T) {
	for _, testCase := range []struct {
		protocol string
	}{
		{"proto"},
		{""},
		{"json"},
	} {
		resp := &wkpb.WorkResponse{
			ExitCode: 0,
			Output:   "Test output!",
		}

		env := newTestEnv(t)
		pool := newRunnerPool(t, env, noLimitsCfg)
		ctx := withAuthenticatedUser(t, context.Background(), "US1")

		// Note: in each test step below, we use a fresh context and cancel it
		// after the step is done, to ensure that the worker sticks around across task
		// contexts

		// Make a new persistent worker
		(func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", testCase.protocol, resp))
			require.NoError(t, err)
			res := r.Run(ctx)
			require.NoError(t, res.Error)
			assert.Equal(t, 0, res.ExitCode)
			assert.Equal(t, []byte(resp.Output), res.Stderr)
			pool.TryRecycle(ctx, r, true)
			assert.Equal(t, 1, pool.PausedRunnerCount())
		})()

		// Reuse the persistent worker
		(func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", testCase.protocol, resp))
			require.NoError(t, err)
			res := r.Run(ctx)
			require.NoError(t, res.Error)
			assert.Equal(t, 0, res.ExitCode)
			assert.Equal(t, []byte(resp.Output), res.Stderr)
			pool.TryRecycle(ctx, r, true)
			assert.Equal(t, 1, pool.PausedRunnerCount())
		})()

		// Try a persistent worker with a new key
		(func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			r, err := pool.Get(ctx, newPersistentRunnerTask(t, "def", "", testCase.protocol, resp))
			require.NoError(t, err)
			res := r.Run(ctx)
			require.NoError(t, res.Error)
			assert.Equal(t, 0, res.ExitCode)
			assert.Equal(t, []byte(resp.Output), res.Stderr)
			pool.TryRecycle(ctx, r, true)
			assert.Equal(t, 2, pool.PausedRunnerCount())
		})()
	}
}

func TestRunnerPool_PersistentWorkerUnknownProtocol(t *testing.T) {
	resp := &wkpb.WorkResponse{
		ExitCode: 0,
		Output:   "Test output!",
	}
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	// Make a new persistent worker
	r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", "unknown", resp))
	require.NoError(t, err)
	res := r.Run(context.Background())
	require.Error(t, res.Error)
}

func TestRunnerPool_PersistentWorker_Failure(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	// Persistent runner with unknown flagfile
	r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "@flagfile", "", &wkpb.WorkResponse{}))
	require.NoError(t, err)
	res := r.Run(context.Background())
	require.Error(t, res.Error)

	// Make sure that after trying to recycle doesn't put the worker back in the pool.
	pool.TryRecycle(ctx, r, true)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}
