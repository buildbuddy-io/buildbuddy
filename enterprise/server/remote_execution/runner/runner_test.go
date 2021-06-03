package runner_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

func newWorkspace(t *testing.T, env *testenv.TestEnv) *workspace.Workspace {
	tmpDir, err := ioutil.TempDir("/tmp", "buildbuddy_test_runner_workspace_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Fatal(err)
		}
	})
	ws, err := workspace.New(env, tmpDir, &workspace.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func newBareRunner(t *testing.T, env *testenv.TestEnv, user interfaces.UserInfo) *runner.CommandRunner {
	return &runner.CommandRunner{
		PlatformProperties: &platform.Properties{
			RecycleRunner: true,
		},
		ACL:       runner.ACLForUser(user),
		Container: bare.NewBareCommandContainer(),
		Workspace: newWorkspace(t, env),
	}
}

func newUser(t *testing.T) interfaces.UserInfo {
	groupID := newUUID(t)
	return &testauth.TestUser{
		UserID:        newUUID(t),
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
	}
}

func newUUID(t *testing.T) string {
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func runMinimalCommand(t *testing.T, r *runner.CommandRunner) {
	res := r.Run(context.Background(), &repb.Command{Arguments: []string{"pwd"}})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
}

func TestRunnerPool_CanAddAndTakeSameRunner(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	r := newBareRunner(t, env, u)
	pool := runner.NewPool(noLimitsCfg)

	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.Size())

	taken := pool.Take(&runner.Query{User: u})

	assert.Same(t, r, taken)
	assert.Equal(t, 0, pool.Size())
}

func TestRunnerPool_CannotTakeRunnerFromOtherGroup(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u1 := newUser(t)
	u2 := newUser(t)
	r := newBareRunner(t, env, u1)
	pool := runner.NewPool(noLimitsCfg)

	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	require.NoError(t, err)

	taken := pool.Take(&runner.Query{User: u2})

	assert.Nil(t, taken)
	assert.Equal(t, 1, pool.Size())
}

func TestRunnerPool_CannotTakeRunnerFromOtherInstanceName(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	r := newBareRunner(t, env, u)
	pool := runner.NewPool(noLimitsCfg)
	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.Size())

	taken := pool.Take(&runner.Query{
		User:         u,
		InstanceName: "nonempty_instance_name",
	})

	assert.Nil(t, taken)
	assert.Equal(t, 1, pool.Size())
}

func TestRunnerPool_CannotTakeRunnerFromOtherWorkflow(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	r := newBareRunner(t, env, u)
	r.PlatformProperties.WorkflowID = "WF1"
	pool := runner.NewPool(noLimitsCfg)
	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.Size())

	taken := pool.Take(&runner.Query{
		User:       u,
		WorkflowID: "WF2",
	})

	assert.Nil(t, taken)
	assert.Equal(t, 1, pool.Size())
}

func TestRunnerPool_Shutdown_RemovesAllRunners(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	r := newBareRunner(t, env, u)
	pool := runner.NewPool(noLimitsCfg)

	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.Size())

	err = pool.Shutdown(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, pool.Size())
}

func TestRunnerPool_DefaultSystemBasedLimits_CanAddAtLeastOneRunner(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	r := newBareRunner(t, env, u)
	pool := runner.NewPool(noLimitsCfg)

	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	require.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
}

func TestRunnerPool_ExceedMaxRunnerCount_OldestRunnerEvicted(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	pool := runner.NewPool(&config.RunnerPoolConfig{
		MaxRunnerCount:            1,
		MaxRunnerDiskSizeBytes:    unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
	})

	createAndAdd := func() *runner.CommandRunner {
		r := newBareRunner(t, env, u)
		runMinimalCommand(t, r)
		err := pool.Add(context.Background(), r)
		require.NoError(t, err)
		return r
	}

	createAndAdd()
	require.Equal(t, 1, pool.Size())

	r2 := createAndAdd()
	require.Equal(t, 1, pool.Size(), "should have evicted an older runner and added another one, leaving the size at 1")

	taken := pool.Take(&runner.Query{User: u})
	assert.Same(t, r2, taken, "since runner limit is 1, last added runner should be the only runner in the pool")
	assert.Equal(t, 0, pool.Size())
}

func TestRunnerPool_DiskLimitExceeded_CannotAdd(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := newUser(t)
	r := newBareRunner(t, env, u)
	pool := runner.NewPool(&config.RunnerPoolConfig{
		MaxRunnerCount:            unlimited,
		MaxRunnerMemoryUsageBytes: unlimited,
		// At least one byte should be needed for the workspace root dir.
		MaxRunnerDiskSizeBytes: 1,
	})

	runMinimalCommand(t, r)

	err := pool.Add(context.Background(), r)

	assert.True(t, status.IsResourceExhaustedError(err), "should exceed disk limit")
	assert.Equal(t, 0, pool.Size())
}

// TODO: Test mem limit. We currently don't compute mem usage for bare runners,
// so there's not a great way to test this yet.
