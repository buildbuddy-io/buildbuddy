package runner

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

const (
	unlimited = -1

	sysMemoryBytes = tasksize.DefaultMemEstimate * 10
	sysMilliCPU    = tasksize.DefaultCPUEstimate * 10

	recycleRunnerPropertyName = "recycle-runner"
)

var (
	// set by x_defs in BUILD file
	testworkerRunfilePath string
)

func defaultCfg() *RunnerPoolOptions {
	return &RunnerPoolOptions{
		PoolOptions:               &PoolOptions{},
		MaxRunnerCount:            *maxRunnerCount,
		MaxRunnerDiskSizeBytes:    *maxRunnerDiskSizeBytes,
		MaxRunnerMemoryUsageBytes: *maxRunnerMemoryUsageBytes,
	}
}

func noLimitsCfg() *RunnerPoolOptions {
	return &RunnerPoolOptions{
		PoolOptions:               &PoolOptions{},
		MaxRunnerCount:            unlimited,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	}
}

type fakeContainer struct {
	container.CommandContainer // TODO: implement all methods
	CreateError                error
	Removed                    chan struct{}
	Result                     *interfaces.CommandResult
	Isolation                  string // Fake isolation type name
	ImageCached                bool   // Return value for IsImageCached
	BlockPull                  bool   // PullImage blocks forever if true.
}

func NewFakeContainer() *fakeContainer {
	return &fakeContainer{
		Result:  &interfaces.CommandResult{},
		Removed: make(chan struct{}),
	}
}

func (c *fakeContainer) IsolationType() string {
	if c.Isolation == "" {
		return "bare"
	}
	return c.Isolation
}

func (c *fakeContainer) Run(ctx context.Context, cmd *repb.Command, workdir string, creds oci.Credentials) *interfaces.CommandResult {
	return c.Result
}

func (c *fakeContainer) IsImageCached(ctx context.Context) (bool, error) {
	return c.ImageCached, nil
}

func (c *fakeContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if c.BlockPull {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *fakeContainer) Create(ctx context.Context, workdir string) error {
	return c.CreateError
}

func (c *fakeContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	return c.Result
}

func (c *fakeContainer) Remove(ctx context.Context) error {
	close(c.Removed)
	return nil
}

// fakeFirecrackerContainer behaves like a bare container except it returns
// 0 mem / CPU resources, like Firecracker.
type fakeFirecrackerContainer struct {
	container.CommandContainer
}

func newFakeFirecrackerContainer() *fakeFirecrackerContainer {
	return &fakeFirecrackerContainer{CommandContainer: bare.NewBareCommandContainer(&bare.Opts{})}
}

func (*fakeFirecrackerContainer) Stats(context.Context) (*repb.UsageStats, error) {
	return &repb.UsageStats{}, nil
}

type RunnerPoolOptions struct {
	*PoolOptions
	MaxRunnerCount            int
	MaxRunnerDiskSizeBytes    int64
	MaxRunnerMemoryUsageBytes int64
}

func newTask() *repb.ScheduledTask {
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"pwd"},
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: recycleRunnerPropertyName, Value: "true"},
				},
			},
		},
	}
	return &repb.ScheduledTask{ExecutionTask: task}
}

func newWorkflowTask() *repb.ScheduledTask {
	t := newTask()
	plat := t.ExecutionTask.Command.Platform
	plat.Properties = append(plat.Properties, &repb.Platform_Property{
		Name: "workflow-id", Value: "WF123",
	})
	return t
}

func newTaskWithAffinityKey(key string) *repb.ScheduledTask {
	t := newTask()
	plat := t.ExecutionTask.Command.Platform
	plat.Properties = append(plat.Properties, &repb.Platform_Property{
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

func newTestEnv(t *testing.T) *real_environment.RealEnv {
	env := testenv.GetTestEnv(t)
	var userGroups []string
	for i := 0; i < 10; i++ {
		userGroups = append(userGroups, fmt.Sprintf("US%d", i), fmt.Sprintf("GR%d", i))
	}
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers(userGroups...)))
	return env
}

func withAuthenticatedUser(t *testing.T, ctx context.Context, env *testenv.TestEnv, userID string) context.Context {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func mustRun(t *testing.T, r *taskRunner) {
	res := r.Run(context.Background(), &repb.IOStats{})
	require.NoError(t, res.Error)
}

func newRunnerPool(t *testing.T, env *testenv.TestEnv, cfg *RunnerPoolOptions) *pool {
	flags.Set(t, "executor.runner_pool.max_runner_count", cfg.MaxRunnerCount)
	flags.Set(t, "executor.runner_pool.max_runner_disk_size_bytes", cfg.MaxRunnerDiskSizeBytes)
	flags.Set(t, "executor.runner_pool.max_runner_memory_usage_bytes", cfg.MaxRunnerMemoryUsageBytes)
	if cfg.PoolOptions == nil {
		cfg.PoolOptions = &PoolOptions{}
	}
	cacheRoot := testfs.MakeTempDir(t)
	p, err := NewPool(env, cacheRoot, cfg.PoolOptions)
	require.NoError(t, err)
	require.NotNil(t, p)
	t.Cleanup(func() {
		// Always make sure we can cleanly shut down the pool at the end of each
		// test.
		err := p.Shutdown(env.GetServerContext())
		require.NoError(t, err)
	})
	return p
}

func get(ctx context.Context, p *pool, task *repb.ScheduledTask) (*taskRunner, error) {
	r, err := p.Get(ctx, task)
	if err != nil {
		return nil, err
	}
	return r.(*taskRunner), nil
}

func mustGet(t *testing.T, ctx context.Context, pool *pool, task *repb.ScheduledTask) *taskRunner {
	initialActiveCount := pool.ActiveRunnerCount()
	r, err := get(ctx, pool, task)
	require.NoError(t, err)
	require.Equal(t, initialActiveCount+1, pool.ActiveRunnerCount())
	mustRun(t, r)
	return r
}

func mustAdd(t *testing.T, ctx context.Context, pool *pool, r *taskRunner) {
	initialActiveCount := pool.ActiveRunnerCount()

	err := pool.Add(ctx, r)

	require.NoError(t, err)
	require.Equal(t, initialActiveCount-1, pool.ActiveRunnerCount(), "active runner count should decrease when adding back to pool")
}

func mustAddWithoutEviction(t *testing.T, ctx context.Context, pool *pool, r *taskRunner) {
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

func mustAddWithEviction(t *testing.T, ctx context.Context, pool *pool, r *taskRunner) {
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

func mustGetPausedRunner(t *testing.T, ctx context.Context, pool *pool, task *repb.ScheduledTask) *taskRunner {
	initialPausedCount := pool.PausedRunnerCount()
	initialCount := pool.RunnerCount()
	r := mustGet(t, ctx, pool, task)
	require.Equal(t, initialPausedCount-1, pool.PausedRunnerCount())
	require.Equal(t, initialCount, pool.RunnerCount())
	return r
}

func mustGetNewRunner(t *testing.T, ctx context.Context, pool *pool, task *repb.ScheduledTask) *taskRunner {
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
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	r1 := mustGetNewRunner(t, ctx, pool, newTask())

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetPausedRunner(t, ctx, pool, newTask())

	assert.Same(t, r1, r2)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_CanAddAndGetBackSameRunner_DockerReuseAlias(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	t1 := &repb.ScheduledTask{
		ExecutionTask: &repb.ExecutionTask{
			Command: &repb.Command{
				Arguments: []string{"pwd"},
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "dockerReuse", Value: "true"},
					},
				},
			},
		},
	}
	r1 := mustGetNewRunner(t, ctx, pool, t1)

	mustAddWithoutEviction(t, ctx, pool, r1)

	t2 := t1.CloneVT()
	r2 := mustGetPausedRunner(t, ctx, pool, t2)

	assert.Same(t, r1, r2)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_CannotTakeRunnerFromOtherGroup(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctxUser1 := withAuthenticatedUser(t, context.Background(), env, "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), env, "US2")

	r1 := mustGetNewRunner(t, ctxUser1, pool, newTask())

	mustAddWithoutEviction(t, ctxUser1, pool, r1)

	r2 := mustGetNewRunner(t, ctxUser2, pool, newTask())

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_CannotTakeRunnerFromOtherInstanceName(t *testing.T) {
	env := newTestEnv(t)
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")
	pool := newRunnerPool(t, env, noLimitsCfg())
	task1 := newTask()
	task1.ExecutionTask.ExecuteRequest = &repb.ExecuteRequest{InstanceName: "instance/1"}
	task2 := newTask()
	task2.ExecutionTask.ExecuteRequest = &repb.ExecuteRequest{InstanceName: "instance/2"}

	r1 := mustGetNewRunner(t, ctx, pool, task1)

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetNewRunner(t, ctx, pool, task2)

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_CannotTakeRunnerFromOtherWorkflow(t *testing.T) {
	env := newTestEnv(t)
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")
	pool := newRunnerPool(t, env, noLimitsCfg())
	task1 := newTask()
	task1.ExecutionTask.Command.Platform.Properties = append(
		task1.ExecutionTask.Command.Platform.Properties,
		&repb.Platform_Property{Name: "workflow-id", Value: "WF1"},
	)
	task2 := newTask()
	task2.ExecutionTask.Command.Platform.Properties = append(
		task1.ExecutionTask.Command.Platform.Properties,
		&repb.Platform_Property{Name: "workflow-id", Value: "WF2"},
	)

	r1 := mustGetNewRunner(t, ctx, pool, task1)

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetNewRunner(t, ctx, pool, task2)

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_Shutdown_RemovesPausedRunners(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	r := mustGetNewRunner(t, ctx, pool, newTask())

	mustAdd(t, ctx, pool, r)

	err := pool.Shutdown(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, pool.PausedRunnerCount())
	assert.Equal(t, 0, pool.ActiveRunnerCount())
}

func TestRunnerPool_Shutdown_RunnersReturnRetriableOrNilError(t *testing.T) {
	env := newTestEnv(t)
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	// Run 30 trials where we create a pool that runs 50 tasks using runner
	// recycling, shutting down the pool after roughly half of the tasks have been
	// started.
	for i := 0; i < 30; i++ {
		pool := newRunnerPool(t, env, noLimitsCfg())
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
			if result := r.Run(ctx, &repb.IOStats{}); result.Error != nil {
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
	if runtime.GOOS == "darwin" {
		// TODO(bduffany): Set macos memory limit to match total system
		// memory instead of free memory, then re-enable.
		t.SkipNow()
	}

	env := newTestEnv(t)
	pool := newRunnerPool(t, env, defaultCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	r, err := get(ctx, pool, newTask())

	require.NoError(t, err)

	mustRun(t, r)

	err = pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.PausedRunnerCount())
}

type providerFunc func(ctx context.Context, args *container.Init) (container.CommandContainer, error)

func (f providerFunc) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	return f(ctx, args)
}

// Returns containers that only consume disk resources when paused (like firecracker).
type DiskOnlyContainerProvider struct{}

func (*DiskOnlyContainerProvider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	fc := newFakeFirecrackerContainer()
	return container.NewTracedCommandContainer(fc), nil
}

func TestRunnerPool_DiskOnlyContainer_CanAddMultiple(t *testing.T) {
	env := newTestEnv(t)
	maxRunnerCount := 10

	pool := newRunnerPool(t, env, &RunnerPoolOptions{
		PoolOptions: &PoolOptions{
			ContainerProvider: &DiskOnlyContainerProvider{},
		},
		MaxRunnerCount:         maxRunnerCount,
		MaxRunnerDiskSizeBytes: unlimited,
		// Only allow up to 1 byte of memory. This should be OK since FC
		// containers use no mem.
		MaxRunnerMemoryUsageBytes: 1,
	})

	// Make sure we can add up to `maxRunnerCount` runners without eviction.
	ctx := context.Background()
	for i := 0; i < maxRunnerCount; i++ {
		ctx = withAuthenticatedUser(t, ctx, env, fmt.Sprintf("US%d", i))
		r := mustGetNewRunner(t, ctx, pool, newTask())
		mustAddWithoutEviction(t, ctx, pool, r)
	}
	for i := 0; i < maxRunnerCount; i++ {
		ctx = withAuthenticatedUser(t, ctx, env, fmt.Sprintf("US%d", i))
		_ = mustGetPausedRunner(t, ctx, pool, newTask())
	}
}

func TestRunnerPool_ExceedMaxRunnerCount_OldestRunnerEvicted(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, &RunnerPoolOptions{
		MaxRunnerCount:            2,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	})
	ctxUser1 := withAuthenticatedUser(t, context.Background(), env, "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), env, "US2")
	ctxUser3 := withAuthenticatedUser(t, context.Background(), env, "US3")

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
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	r := mustGetNewRunner(t, ctx, pool, newTask())

	err := pool.Add(context.Background(), r)

	assert.True(t, status.IsResourceExhaustedError(err), "should exceed disk limit")
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_ActiveRunnersTakenFromPool_NotRemovedOnShutdown(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	task := newTask()
	task.ExecutionTask.Command.Arguments = []string{"sh", "-c", "touch foo.txt && sleep infinity"}
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
	require.Equal(t, 1, pool.ActiveRunnerCount())
	_, err = os.Stat(path.Join(r.Workspace.Path(), "foo.txt"))
	require.NoError(t, err, "runner should not have been removed on shutdown")
}

func TestRunnerPool_GetSameRunnerForSameAffinityKey(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	r1 := mustGetNewRunner(t, ctx, pool, newTaskWithAffinityKey("key1"))

	mustAddWithoutEviction(t, ctx, pool, r1)

	r2 := mustGetPausedRunner(t, ctx, pool, newTaskWithAffinityKey("key1"))

	assert.Same(t, r1, r2)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_GetDifferentRunnerForDifferentAffinityKey(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctxUser1 := withAuthenticatedUser(t, context.Background(), env, "US1")
	ctxUser2 := withAuthenticatedUser(t, context.Background(), env, "US2")

	r1 := mustGetNewRunner(t, ctxUser1, pool, newTaskWithAffinityKey("key1"))

	mustAddWithoutEviction(t, ctxUser1, pool, r1)

	r2 := mustGetNewRunner(t, ctxUser2, pool, newTaskWithAffinityKey("key2"))

	assert.NotSame(t, r1, r2)
}

func TestRunnerPool_TaskSize(t *testing.T) {
	oneGB := "1000000000"
	twoGB := "2000000000"

	for _, test := range []struct {
		Name          string
		Size1, Size2  string
		WFID1, WFID2  string
		ShouldRecycle bool
	}{
		{Name: "DifferentSize_NonWorkflow_ShouldNotRecycle", Size1: oneGB, Size2: twoGB, WFID1: "", WFID2: "", ShouldRecycle: false},
		{Name: "SameSize_Workflow_ShouldRecycle", Size1: oneGB, Size2: oneGB, WFID1: "WF1", WFID2: "WF1", ShouldRecycle: true},
		{Name: "DifferentSize_Workflow_ShouldNotRecycle", Size1: oneGB, Size2: twoGB, WFID1: "WF1", WFID2: "WF1", ShouldRecycle: false},
		{Name: "SameSize_DifferentWorkflowIDs_ShouldNotRecycle", Size1: oneGB, Size2: oneGB, WFID1: "WF1", WFID2: "WF2", ShouldRecycle: false},
	} {
		t.Run(test.Name, func(t *testing.T) {
			env := newTestEnv(t)
			pool := newRunnerPool(t, env, noLimitsCfg())
			ctxUser1 := withAuthenticatedUser(t, context.Background(), env, "US1")
			t1 := newTask()
			p1 := t1.ExecutionTask.Command.Platform
			p1.Properties = append(p1.Properties, &repb.Platform_Property{Name: platform.EstimatedMemoryPropertyName, Value: test.Size1})
			p1.Properties = append(p1.Properties, &repb.Platform_Property{Name: platform.WorkflowIDPropertyName, Value: test.WFID1})
			t2 := newTask()
			p2 := t2.ExecutionTask.Command.Platform
			p2.Properties = append(p2.Properties, &repb.Platform_Property{Name: platform.EstimatedMemoryPropertyName, Value: test.Size2})
			p2.Properties = append(p2.Properties, &repb.Platform_Property{Name: platform.WorkflowIDPropertyName, Value: test.WFID2})

			r1 := mustGetNewRunner(t, ctxUser1, pool, t1)
			mustAddWithoutEviction(t, ctxUser1, pool, r1)

			var r2 *taskRunner
			if test.ShouldRecycle {
				r2 = mustGetPausedRunner(t, ctxUser1, pool, t2)
				require.Same(t, r1, r2)
			} else {
				r2 = mustGetNewRunner(t, ctxUser1, pool, t2)
				require.NotSame(t, r1, r2)
			}
		})
	}
}

func newPersistentRunnerTask(t *testing.T, key, arg, protocol string, resp *wkpb.WorkResponse) *repb.ScheduledTask {
	workerPath := testfs.RunfilePath(t, testworkerRunfilePath)
	task := &repb.ExecutionTask{
		Action: &repb.Action{},
		Command: &repb.Command{
			Arguments: []string{
				workerPath,
				"--protocol=" + protocol,
				"--response_base64=" + encodedResponse(t, protocol, resp),
			},
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "persistentWorkerKey", Value: key},
					{Name: "persistentWorkerProtocol", Value: protocol},
					// Note: we don't need to explicitly set recycle-runner=true.
				},
			},
		},
	}
	if arg != "" {
		task.Command.Arguments = append(task.Command.Arguments, arg)
	}
	return &repb.ScheduledTask{ExecutionTask: task}
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
		pool := newRunnerPool(t, env, noLimitsCfg())
		ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

		// Note: in each test step below, we use a fresh context and cancel it
		// after the step is done, to ensure that the worker sticks around across task
		// contexts

		// Make a new persistent worker
		(func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", testCase.protocol, resp))
			require.NoError(t, err)
			res := r.Run(ctx, &repb.IOStats{})
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
			res := r.Run(ctx, &repb.IOStats{})
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
			res := r.Run(ctx, &repb.IOStats{})
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
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	// Make a new persistent worker
	r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "", "unknown", resp))
	require.NoError(t, err)
	res := r.Run(context.Background(), &repb.IOStats{})
	require.Error(t, res.Error)
}

func TestRunnerPool_PersistentWorker_UnknownFlagFileError(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	// Persistent worker with unknown flagfile
	r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "@flagfile", "", &wkpb.WorkResponse{}))
	require.NoError(t, err)
	res := r.Run(context.Background(), &repb.IOStats{})
	require.Error(t, res.Error)

	// Make sure that after the error, trying to recycle doesn't put the worker
	// back in the pool.
	pool.TryRecycle(ctx, r, true)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_PersistentWorker_LargeFlagFile(t *testing.T) {
	env := newTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	go runServer()
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	// Write a large flag (100KB) to the flag file and upload it as an input.
	tmp := testfs.MakeTempDir(t)
	testfs.WriteFile(t, tmp, "flags", strings.Repeat("a", 100*1024))
	task := newPersistentRunnerTask(t, "abc", "@flags", "", &wkpb.WorkResponse{})
	inputRootDigest, _, err := cachetools.UploadDirectoryToCAS(ctx, env, "", repb.DigestFunction_SHA256, tmp)
	require.NoError(t, err)
	task.ExecutionTask.Action.InputRootDigest = inputRootDigest

	r, err := pool.Get(ctx, task)
	require.NoError(t, err)
	err = r.DownloadInputs(ctx, &repb.IOStats{})
	require.NoError(t, err)
	res := r.Run(context.Background(), &repb.IOStats{})
	require.NoError(t, res.Error)

	// Make sure that recycling succeeds.
	pool.TryRecycle(ctx, r, true)
	assert.Equal(t, 1, pool.PausedRunnerCount())
}

func TestRunnerPool_PersistentWorker_Crash_ShowsWorkerStderrInOutput(t *testing.T) {
	env := newTestEnv(t)
	pool := newRunnerPool(t, env, noLimitsCfg())
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	// Persistent worker with runner that crashes
	r, err := pool.Get(ctx, newPersistentRunnerTask(t, "abc", "--fail_with_stderr=TestStderrMessage", "", &wkpb.WorkResponse{}))
	require.NoError(t, err)
	res := r.Run(context.Background(), &repb.IOStats{})
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "persistent worker stderr:", res.Error.Error())
	assert.Contains(t, res.Error.Error(), "TestStderrMessage")

	pool.TryRecycle(ctx, r, true)
	assert.Equal(t, 0, pool.PausedRunnerCount())
}

func TestRunnerPool_RecycleAfterCreateFailed_CallsRemove(t *testing.T) {
	env := newTestEnv(t)
	cfg := noLimitsCfg()
	var ctr *fakeContainer
	// Set up a fake command container that always returns a fixed error
	// when calling Create().
	fakeCreateError := fmt.Errorf("test-create-error")
	cfg.ContainerProvider = providerFunc(func(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
		ctr = NewFakeContainer()
		ctr.CreateError = fakeCreateError
		return ctr, nil
	})
	pool := newRunnerPool(t, env, cfg)
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")

	r, err := pool.Get(ctx, newTask())
	require.NoError(t, err)
	// Try running a task; Create() should fail with our fixed error, and be
	// surfaced in the command result.
	res := r.Run(ctx, &repb.IOStats{})
	require.Equal(t, fakeCreateError, res.Error)
	pool.TryRecycle(ctx, r, false /*=finishedCleanly*/)
	// Remove should be called, closing this channel.
	<-ctr.Removed
}

func TestDoNotRecycleSpecialFile(t *testing.T) {
	for _, createFile := range []bool{false, true} {
		t.Run(fmt.Sprintf("createFile=%t", createFile), func(t *testing.T) {
			env := newTestEnv(t)
			pool := newRunnerPool(t, env, noLimitsCfg())
			ctx := withAuthenticatedUser(t, context.Background(), env, "US1")
			task := newTask()
			if createFile {
				task.ExecutionTask.Command.Arguments = []string{
					"touch", ".BUILDBUDDY_DO_NOT_RECYCLE",
				}
			}
			r, err := pool.Get(ctx, task)
			require.NoError(t, err)
			res := r.Run(ctx, &repb.IOStats{})
			assert.Equal(t, createFile, res.DoNotRecycle)
			pool.TryRecycle(ctx, r, false)
		})
	}
}

func TestImagePullTimeout(t *testing.T) {
	// Enable OCI isolation so we can pull images.
	flags.Set(t, "executor.enable_oci", true)
	// Time out image pulls immediately
	flags.Set(t, "executor.image_pull_timeout", 1*time.Nanosecond)

	env := newTestEnv(t)
	cfg := noLimitsCfg()
	cfg.ContainerProvider = providerFunc(func(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
		ctr := NewFakeContainer()
		ctr.BlockPull = true
		ctr.Isolation = "oci"
		return ctr, nil
	})
	pool := newRunnerPool(t, env, cfg)
	ctx := withAuthenticatedUser(t, context.Background(), env, "US1")
	task := newTask()
	plat := task.ExecutionTask.Command.Platform
	plat.Properties = append(plat.Properties, []*repb.Platform_Property{
		{Name: "container-image", Value: "docker://busybox"},
		{Name: "workload-isolation-type", Value: "oci"},
	}...)
	r, err := pool.Get(ctx, task)
	require.NoError(t, err)

	err = r.PrepareForTask(ctx)
	require.Error(t, err)
	assert.True(t, status.IsUnavailableError(err), "expected Unavailable, got %T", err)
	assert.Contains(t, err.Error(), "deadline exceeded")
}
