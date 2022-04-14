// package rbe_retry_test contains end-to-end tests for the remote execution
// service using Bazel as the client.
package bazel_rbe_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// The max number of bazel retries; passed as the --remote_retries flag to
	// bazel. The default is 5, but we set it to 2 to reduce test execution time
	// since bazel does exponential backoff between retries. We don't use 1 here
	// so that we can distinguish between cases where bazel allows exactly 1
	// retry in certain error scenarios; particularly, when refreshing after
	// UNAUTHENTICATED errors:
	// https://cs.github.com/bazelbuild/bazel/blob/93677c68f0bc688dbfa75484688160cdbdae7328/src/main/java/com/google/devtools/build/lib/remote/util/Utils.java#L519
	bazelRemoteRetries = 2

	// The number of times that the scheduler will retry. This should match
	// the const with the same name in scheduler_server.go
	schedulerMaxTaskAttemptCount = 5
)

func TestSimpleAction_Exit0(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, "echo Success; exit 0")

	require.NoError(t, res.Error)
	assert.Contains(t, res.Stdout, "Success\n")
	assert.Equal(t, 1, tasksStarted(t))
}

func TestSimpleAction_Exit1(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, "echo >&2 stderr output; exit 1")

	require.Error(t, res.Error)
	assert.Equal(t, 1, tasksStarted(t))
}

func TestSimpleAction_TerminateWithSIGKILL(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(
		t, ctx, env,
		"echo >&2 'User debug message to help diagnose OOM'; kill -KILL $$")

	require.Error(t, res.Error)
	assert.Contains(t, res.Stderr, "User debug message to help diagnose OOM")
	// SIGKILL usually happens due to OOM, so make sure these are retried
	assert.Contains(t, res.Stderr, "Resource Exhausted")
	assert.Contains(t, res.Stderr, "signal: killed")
	assert.Equal(t, 1+bazelRemoteRetries, tasksStarted(t))
}

func TestSimpleAction_TerminateWithSIGABRT(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(
		t, ctx, env,
		"echo >&2 'Aborting test action'; kill -ABRT $$")

	assert.Contains(
		t, res.Stderr, "Aborting test action",
		"Error message logged before aborting should be visible in bazel stderr")
	assert.Equal(t, 1+bazelRemoteRetries, tasksStarted(t))
}

func TestPersistentUnavailableError_Retried(t *testing.T) {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	const errMsg = "error injected by test"
	errResult := commandutil.ErrorResult(status.UnavailableError(errMsg))
	env.AddExecutorWithOptions(&rbetest.ExecutorOptions{
		RunInterceptor: rbetest.AlwaysReturn(errResult),
	})
	// observe initial count so that we can get the diff at the end of the test
	_ = tasksStarted(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, "exit 0")

	require.Error(t, res.Error)
	assert.Contains(t, res.Stderr, "Unavailable")
	assert.Contains(t, res.Stderr, errMsg)
	assert.Equal(t, 1+bazelRemoteRetries, tasksStarted(t))
}

func TestTransientUnavailableError_Retried(t *testing.T) {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	const errMsg = "error injected by test"
	errResult := commandutil.ErrorResult(status.UnavailableError(errMsg))
	env.AddExecutorWithOptions(&rbetest.ExecutorOptions{
		RunInterceptor: rbetest.ReturnForFirstAttempt(errResult),
	})
	// observe initial count so that we can get the diff at the end of the test
	_ = tasksStarted(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, "exit 0")

	require.NoError(t, res.Error)
	assert.NotContains(t, res.Stderr, errMsg)
	// 1 failed attempt due to transient error + 1 successful attempt
	assert.Equal(t, 2, tasksStarted(t), "unexpected number of retries")
}

func TestTransientInternalError_Retried(t *testing.T) {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	const errMsg = "error injected by test"
	errResult := commandutil.ErrorResult(status.InternalError(errMsg))
	env.AddExecutorWithOptions(&rbetest.ExecutorOptions{
		RunInterceptor: rbetest.ReturnForFirstAttempt(errResult),
	})
	// observe initial count so that we can get the diff at the end of the test
	_ = tasksStarted(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, "exit 0")

	require.NoError(t, res.Error)
	assert.NotContains(t, res.Stderr, errMsg)
	// 1 failed attempt due to transient error + 1 successful attempt
	assert.Equal(t, 2, tasksStarted(t))
}

func TestUnauthenticatedError_RetriedOnce(t *testing.T) {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	const errMsg = "error injected by test"
	errResult := commandutil.ErrorResult(status.UnauthenticatedError(errMsg))
	env.AddExecutorWithOptions(&rbetest.ExecutorOptions{
		RunInterceptor: rbetest.AlwaysReturn(errResult),
	})
	// observe initial count so that we can get the diff at the end of the test
	_ = tasksStarted(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, "exit 0")

	require.Error(t, res.Error)
	assert.Contains(t, res.Stderr, errMsg)
	// Bazel allows a single retry for Unauthenticated errors via to allow for
	// refreshing stale auth credentials. See:
	// https://cs.github.com/bazelbuild/bazel/blob/93677c68f0bc688dbfa75484688160cdbdae7328/src/main/java/com/google/devtools/build/lib/remote/GrpcRemoteExecutor.java#L138
	assert.Equal(t, 2, tasksStarted(t))
}

func TestExecutorShutdown_Retried(t *testing.T) {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	// TODO(bduffany): Simplify executor shutdown logic across runner types and
	// remove reliance on ErrSIGKILL here
	errResult := commandutil.ErrorResult(commandutil.ErrSIGKILL)
	env.AddExecutorWithOptions(&rbetest.ExecutorOptions{
		RunInterceptor: rbetest.AlwaysReturn(errResult),
	})
	// observe initial count so that we can get the diff at the end of the test
	_ = tasksStarted(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(t, ctx, env, `
		echo THIS_MSG_SHOULD_APPEAR_IN_BAZEL_STDERR >&2
		exit 0
	`)

	require.Error(t, res.Error)
	assert.Contains(
		t, res.Stderr,
		"command was terminated by SIGKILL, likely due to executor shutdown or OOM")
	// TODO(bduffany): Fail faster. Currently, these get retried by both the
	// scheduler and Bazel, where each bazel attempt results in 5 scheduler
	// attempts.
	assert.Equal(t, (1+bazelRemoteRetries)*schedulerMaxTaskAttemptCount, tasksStarted(t))
	// TODO(bduffany): Ensure that we get partial debug output in this case
	// assert.Contains(t, res.Stderr, "THIS_MSG_SHOULD_APPEAR_IN_BAZEL_STDERR")
}

func TestActionWithContainerImage_InvalidArgument(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(
		t, ctx, env, "exit 0",
		"--remote_default_exec_properties=container-image=INVALID")

	require.Error(t, res.Error)
	assert.Contains(t, res.Stderr, "InvalidArgument")
	// TODO(bduffany): Fail faster. Currently, these get retried by both the
	// scheduler and Bazel, where each bazel attempt results in 5 scheduler
	// attempts.
	assert.Equal(t, (1+bazelRemoteRetries)*(schedulerMaxTaskAttemptCount), tasksStarted(t))
}

func TestActionWithRunnerRecycling_Unauthenticated(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	res := runRemoteShellActionViaBazel(
		t, ctx, env, "exit 0",
		// runner recycling requires authentication, but this build doesn't specify
		// an API key
		"--remote_default_exec_properties=recycle-runner=true")

	require.Error(t, res.Error)
	// TODO(bduffany): return Unauthenticated here, not Unavailable
	assert.Contains(t, res.Stderr, "Unavailable")
	assert.Contains(t, res.Stderr, "runner recycling is not supported for anonymous builds")
	// TODO(bduffany): Fail faster. Currently, these get retried by both the
	// scheduler and Bazel, where each bazel attempt results in 5 scheduler
	// attempts.
	assert.Equal(t, (1+bazelRemoteRetries)*(schedulerMaxTaskAttemptCount), tasksStarted(t))
}

func setup(t *testing.T) *rbetest.Env {
	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServer()
	env.AddExecutor()
	// observe initial count so that we can get the diff at the end of the test
	_ = tasksStarted(t)
	return env
}

var previousTasksStarted = 0

// tasksStarted returns the change in the tasks started count since this func
// was last called.
func tasksStarted(t testing.TB) int {
	counterValue := int(testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount))
	diff := counterValue - previousTasksStarted
	previousTasksStarted = counterValue
	return diff
}

func runRemoteShellActionViaBazel(t *testing.T, ctx context.Context, env *rbetest.Env, shCommand string, extraBazelArgs ...string) *bazel.InvocationResult {
	ws := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		// Define a bazel rule that runs exactly one action, which creates a dummy
		// file (because Bazel requires actions to have outputs) then executes our
		// test command.
		//
		// This approach is used instead of using built-in rules like `sh_test` or
		// `genrule` because those rules have expensive/inconvenient CC deps, and
		// also wind up executing more than one action, so test assertions are less
		// intuitive.
		"defs.bzl": `
def _exec_impl(ctx):
  out = ctx.actions.declare_file(ctx.label.name)
  ctx.actions.run_shell(
    outputs = [out],
    command = """
      set -e
      touch "%s"
      %s
		""" % (out.path, ctx.attr.command),
  )
  return [DefaultInfo(files = depset([out]))]

exec = rule(implementation = _exec_impl, attrs = {"command": attr.string()})
`,
		"BUILD": `
load(":defs.bzl", "exec")
exec(name = "exec", command = """` + shCommand + `""")
`,
	})
	// Execute just the test action remotely.
	buildArgs := []string{
		":exec",
		"--remote_executor=" + env.GetRemoteExecutionTarget(),
		"--remote_retries=" + fmt.Sprintf("%d", bazelRemoteRetries),
	}
	buildArgs = append(buildArgs, extraBazelArgs...)
	return testbazel.Invoke(ctx, t, ws, "build", buildArgs...)
}
