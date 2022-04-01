// package rbe_retry_test contains end-to-end tests for the remote execution
// service using Bazel as the client.
package bazel_rbe_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	// 1 initial attempt + 5 scheduler retries
	assert.Equal(t, 6, tasksStarted(t))
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
	// 1 initial attempt + 5 scheduler retries
	assert.Equal(t, 6, tasksStarted(t))
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
	// scheduler and Bazel, causing 5 bazel attempts, each of which results in
	// 6 attempts by the scheduler, with exponential backoff between each bazel
	// attempt.
	assert.Equal(t, 30, tasksStarted(t))
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
	// scheduler and Bazel, causing 5 bazel attempts, each of which results in
	// 6 attempts by the scheduler, with exponential backoff between each bazel
	// attempt.
	assert.Equal(t, 30, tasksStarted(t))
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
	buildArgs := []string{":exec", "--remote_executor=" + env.GetRemoteExecutionTarget()}
	buildArgs = append(buildArgs, extraBazelArgs...)
	return testbazel.Invoke(ctx, t, ws, "build", buildArgs...)
}
