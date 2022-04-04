package commandutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestRun_NormalExit_NoError(t *testing.T) {
	ctx := context.Background()

	{
		res := runSh(ctx, "exit 0")

		assert.NoError(t, res.Error)
		assert.Equal(t, 0, res.ExitCode)
	}
	{
		res := runSh(ctx, "exit 1")

		assert.NoError(t, res.Error)
		assert.Equal(t, 1, res.ExitCode)
	}
	{
		res := runSh(ctx, "exit 137")

		assert.NoError(t, res.Error)
		assert.Equal(t, 137, res.ExitCode)
	}
}

// TODO(bduffany): Treat SIGABRT as a normal exit rather than an unexpected
// termination, and ensure that unexpected terminations are retried rather than
// immediately reporting them to Bazel.

func TestRun_Killed_ErrorResult(t *testing.T) {
	ctx := context.Background()

	{
		res := runSh(ctx, "kill -ABRT $$")

		require.Error(t, res.Error)
		assert.Contains(t, res.Error.Error(), "signal: aborted")
		assert.Equal(t, -1, res.ExitCode)
	}
	{
		res := runSh(ctx, "kill -INT $$")

		require.Error(t, res.Error)
		assert.Contains(t, res.Error.Error(), "signal: interrupt")
		assert.Equal(t, -1, res.ExitCode)
	}
	{
		res := runSh(ctx, "kill -TERM $$")

		require.Error(t, res.Error)
		assert.Contains(t, res.Error.Error(), "signal: terminated")
		assert.Equal(t, -1, res.ExitCode)
	}
	{
		res := runSh(ctx, "kill -KILL $$")

		require.Error(t, res.Error)
		assert.Contains(t, res.Error.Error(), "signal: killed")
		assert.Equal(t, -1, res.ExitCode)
	}
}

// TODO(bduffany): All the error codes when the command is not found or not
// executable should probably have the same error code, instead of a mix of
// NOT_FOUND / UNAVAILABLE. Also we should probably ensure that Bazel doesn't
// retry these errors at the executor level, since retrying is unlikely to
// help (these errors are most likely due to an incorrect input specification or
// container-image that doesn't contain the executable in PATH)

func TestRun_CommandNotFound_ErrorResult(t *testing.T) {
	ctx := context.Background()

	{
		cmd := &repb.Command{Arguments: []string{"./command_not_found_in_working_dir"}}
		res := commandutil.Run(ctx, cmd, ".", nil /*=stdin*/, nil /*=stdout*/)

		assert.Error(t, res.Error)
		assert.True(
			t, status.IsUnavailableError(res.Error),
			"expecting UNAVAILABLE when executable file is missing, got: %s", res.Error)
		assert.Equal(t, -2, res.ExitCode)
	}
	{
		cmd := &repb.Command{Arguments: []string{"command_not_found_in_PATH"}}
		res := commandutil.Run(ctx, cmd, ".", nil /*=stdin*/, nil /*=stdout*/)

		assert.Error(t, res.Error)
		assert.True(
			t, status.IsNotFoundError(res.Error),
			"expecting NOT_FOUND when executable is not found in $PATH, got: %s", res.Error)
		assert.Equal(t, -2, res.ExitCode)
	}
}

func TestRun_CommandNotExecutable_ErrorResult(t *testing.T) {
	ctx := context.Background()
	wd := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, wd, map[string]string{"non_executable_file": ""})

	cmd := &repb.Command{Arguments: []string{"./non_executable_file"}}
	res := commandutil.Run(ctx, cmd, wd, nil /*=stdin*/, nil /*=stdout*/)

	assert.Error(t, res.Error)
	assert.True(
		t, status.IsUnavailableError(res.Error),
		"expecting UNAVAILABLE when command is not executable, got: %s", res.Error)
	assert.Equal(t, -2, res.ExitCode)
}

func TestRun_Timeout(t *testing.T) {
	ctx := context.Background()
	wd := testfs.MakeTempDir(t)

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		echo stdout >&1
		echo stderr >&2
		sleep infinity
	`}}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	res := commandutil.Run(ctx, cmd, wd, nil, nil)

	require.True(t, status.IsDeadlineExceededError(res.Error), "expected DeadlineExceeded but got: %s", res.Error)
	assert.Equal(t, "stdout\n", string(res.Stdout))
	assert.Equal(t, "stderr\n", string(res.Stderr))
}

func runSh(ctx context.Context, script string) *interfaces.CommandResult {
	cmd := &repb.Command{Arguments: []string{"sh", "-c", script}}
	return commandutil.Run(ctx, cmd, ".", nil /*=stdin*/, nil /*=stdout*/)
}
