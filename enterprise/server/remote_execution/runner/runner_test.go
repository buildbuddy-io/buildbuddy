package runner_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

const (
	unlimited = -1

	sysMemoryBytes = tasksize.DefaultMemEstimate * 10
	sysMilliCPU    = tasksize.DefaultCPUEstimate * 10
)

var (
	defaultCfg = &config.RunnerPoolConfig{}

	noLimitsCfg = &config.RunnerPoolConfig{
		MaxRunnerCount:            unlimited,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	}
)

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

func mustRun(t *testing.T, r *runner.CommandRunner) {
	res := r.Run(context.Background())
	require.NoError(t, res.Error)
}

func newRunnerPool(t *testing.T, env *testenv.TestEnv, cfg *config.RunnerPoolConfig) *runner.Pool {
	env.GetConfigurator().GetExecutorConfig().RunnerPool = *cfg
	p, err := runner.NewPool(env)
	require.NoError(t, err)
	require.NotNil(t, p)
	return p
}

func mustGet(t *testing.T, ctx context.Context, pool *runner.Pool, task *repb.ExecutionTask) *runner.CommandRunner {
	initialActiveCount := pool.ActiveRunnerCount()
	r, err := pool.Get(ctx, task)
	require.NoError(t, err)
	require.Equal(t, initialActiveCount+1, pool.ActiveRunnerCount())
	mustRun(t, r)
	return r
}

func mustAdd(t *testing.T, ctx context.Context, pool *runner.Pool, r *runner.CommandRunner) {
	initialActiveCount := pool.ActiveRunnerCount()

	err := pool.Add(ctx, r)

	require.NoError(t, err)
	require.Equal(t, initialActiveCount-1, pool.ActiveRunnerCount(), "active runner count should decrease when adding back to pool")
}

func mustAddWithoutEviction(t *testing.T, ctx context.Context, pool *runner.Pool, r *runner.CommandRunner) {
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

func mustAddWithEviction(t *testing.T, ctx context.Context, pool *runner.Pool, r *runner.CommandRunner) {
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

func mustGetPausedRunner(t *testing.T, ctx context.Context, pool *runner.Pool, task *repb.ExecutionTask) *runner.CommandRunner {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()
	r := mustGet(t, ctx, pool, task)
	require.Equal(t, initialPausedCount-1, pool.PausedRunnerCount())
	require.Equal(t, initialCount, pool.RunnerCount())
	return r
}

func mustGetNewRunner(t *testing.T, ctx context.Context, pool *runner.Pool, task *repb.ExecutionTask) *runner.CommandRunner {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()
	r := mustGet(t, ctx, pool, task)
	require.Equal(t, initialPausedCount, pool.PausedRunnerCount())
	require.Equal(t, initialCount+1, pool.RunnerCount())
	return r
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

func TestRunnerPool_DefaultSystemBasedLimits_CanAddAtLeastOneRunner(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, defaultCfg)
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	r, err := pool.Get(ctx, newTask())

	require.NoError(t, err)

	mustRun(t, r)

	err = pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.PausedRunnerCount())
}

func TestRunnerPool_ExceedMaxRunnerCount_OldestRunnerEvicted(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, &config.RunnerPoolConfig{
		MaxRunnerCount:            2,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	})
	task := newTask()
	ctxUser1 := withAuthenticatedUser(t, context.Background(), "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), "US2")
	ctxUser3 := withAuthenticatedUser(t, context.Background(), "US3")

	r1 := mustGetNewRunner(t, ctxUser1, pool, task)
	r2 := mustGetNewRunner(t, ctxUser2, pool, task)
	r3 := mustGetNewRunner(t, ctxUser3, pool, task)

	// Limit is 2, so r1 and r2 should be added with no problem.

	mustAddWithoutEviction(t, ctxUser2, pool, r2)
	mustAddWithoutEviction(t, ctxUser1, pool, r1)
	mustAddWithEviction(t, ctxUser3, pool, r3)

	// Should be able to get r1 and r3 back from the pool. r2 should have been
	// evicted since it's the oldest (least recently added back to the pool).

	mustGetPausedRunner(t, ctxUser1, pool, task)
	mustGetPausedRunner(t, ctxUser3, pool, task)
	mustGetNewRunner(t, ctxUser2, pool, task)
}

func TestRunnerPool_DiskLimitExceeded_CannotAdd(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, &config.RunnerPoolConfig{
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
	pool := newRunnerPool(t, env, &config.RunnerPoolConfig{
		MaxRunnerCount:            unlimited,
		MaxRunnerMemoryUsageBytes: 16 * 1e9,
		MaxRunnerDiskSizeBytes:    unlimited,
	})
	ctx := withAuthenticatedUser(t, context.Background(), "US1")

	// Get 3 runners for workflow tasks.
	r1 := mustGetNewRunner(t, ctx, pool, newWorkflowTask())
	r2 := mustGetNewRunner(t, ctx, pool, newWorkflowTask())
	r3 := mustGetNewRunner(t, ctx, pool, newWorkflowTask())

	// Try adding all of them to the pool. 3rd runner should result in eviction,
	// since the estimated memory usage for bare runners is 8GB, and the pool can
	// only fit 2 * 8GB = 16GB worth of runners.
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
	r, err := pool.Get(ctx, task)

	require.NoError(t, err)

	go func() {
		mustRun(t, r)
	}()
	// Poll for foo.txt to exist.
	for {
		_, err = os.Stat(path.Join(r.Workspace.Path(), "foo.txt"))
		if err == nil {
			break
		}
		if os.IsNotExist(err) {
			<-time.After(10 * time.Millisecond)
		} else {
			require.FailNow(t, err.Error())
		}
	}

	require.Equal(t, 1, pool.ActiveRunnerCount())

	// Shut down while the runner is active (and still executing).
	err = pool.Shutdown(context.Background())

	require.NoError(t, err)
	require.Equal(t, 0, pool.ActiveRunnerCount())
	_, err = os.Stat(path.Join(r.Workspace.Path(), "foo.txt"))
	require.True(t, os.IsNotExist(err), "runner should have been removed on shutdown")
}

func newPersistentRunnerTask(t *testing.T, key, arg, protocol string, resp *wkpb.WorkResponse) *repb.ExecutionTask {
	encodedResponse := encodedResponse(t, protocol, resp, 2)
	return &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: append([]string{"sh", "-c", `echo ` + encodedResponse + ` | base64 --decode && tee`}, arg),
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

func encodedResponse(t *testing.T, protocol string, resp *wkpb.WorkResponse, count int) string {
	if protocol == "json" {
		marshaler := jsonpb.Marshaler{}
		buf := new(bytes.Buffer)
		for i := 0; i < count; i++ {
			err := marshaler.Marshal(buf, resp)
			if err != nil {
				t.Fatal(err)
			}
		}
		return base64.StdEncoding.EncodeToString([]byte(buf.Bytes()))
	}
	buf := proto.NewBuffer( /* buf */ nil)
	for i := 0; i < count; i++ {
		if err := buf.EncodeMessage(resp); err != nil {
			t.Fatal(err)
		}
	}
	return base64.StdEncoding.EncodeToString([]byte(buf.Bytes()))
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

		// Make a new persistent worker
		r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", testCase.protocol, resp))
		require.NoError(t, err)
		res := r.Run(context.Background())
		require.NoError(t, res.Error)
		assert.Equal(t, 0, res.ExitCode)
		assert.Equal(t, []byte(resp.Output), res.Stderr)
		pool.TryRecycle(r, true)
		assert.Equal(t, 1, pool.PausedRunnerCount())

		// Reuse the persistent worker
		r, err = pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", testCase.protocol, resp))
		require.NoError(t, err)
		res = r.Run(context.Background())
		require.NoError(t, res.Error)
		assert.Equal(t, 0, res.ExitCode)
		assert.Equal(t, []byte(resp.Output), res.Stderr)
		pool.TryRecycle(r, true)
		assert.Equal(t, 1, pool.PausedRunnerCount())

		// Try a persistent worker with a new key
		r, err = pool.Get(ctx, newPersistentRunnerTask(t, "def", "", testCase.protocol, resp))
		require.NoError(t, err)
		res = r.Run(context.Background())
		require.NoError(t, res.Error)
		assert.Equal(t, 0, res.ExitCode)
		assert.Equal(t, []byte(resp.Output), res.Stderr)
		pool.TryRecycle(r, true)
		assert.Equal(t, 2, pool.PausedRunnerCount())
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
	pool.TryRecycle(r, true)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}
