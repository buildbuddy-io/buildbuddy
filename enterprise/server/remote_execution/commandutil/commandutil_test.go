package commandutil_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
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

func TestRun_Stdio(t *testing.T) {
	ctx := context.Background()
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		if ! [ "$(cat)" = "TestInput" ] ; then
			echo "ERROR: missing expected TestInput on stdin"
			exit 1
		fi
		echo TestOutput
		echo TestError >&2
	`}}
	var stdout, stderr bytes.Buffer
	res := commandutil.Run(ctx, cmd, ".", &container.ExecOpts{
		Stdin:  strings.NewReader("TestInput\n"),
		Stdout: &stdout,
		Stderr: &stderr,
	})

	assert.NoError(t, res.Error)
	assert.Equal(t, "TestOutput\n", stdout.String(), "stdout opt should be respected")
	assert.Empty(t, string(res.Stdout), "stdout in command result should be empty when stdout opt is specified")
	assert.Equal(t, "TestError\n", stderr.String(), "stderr opt should be respected")
	assert.Empty(t, string(res.Stderr), "stderr in command result should be empty when stderr opt is specified")
}

func TestRun_Stdio_StdinNotConsumedByCommand(t *testing.T) {
	ctx := context.Background()
	// Note: this command does not consume stdin. We should still be able to write
	// stdin to it, and not have it fail/hang. See
	// https://go.dev/play/p/DpKaVrx8d8G for an example implementation that does
	// not handle this correctly.
	cmd := &repb.Command{Arguments: []string{"echo", "foo"}}

	inr, inw := io.Pipe()
	outr, outw := io.Pipe()

	go func() {
		defer inr.Close()
		defer outw.Close()

		res := commandutil.Run(ctx, cmd, ".", &container.ExecOpts{
			Stdin:  inr,
			Stdout: outw,
		})
		assert.Empty(t, res.Stdout)
	}()

	inw.Write([]byte("foo"))
	out, err := io.ReadAll(outr)
	require.NoError(t, err)
	assert.Equal(t, "foo\n", string(out))
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
		res := commandutil.Run(ctx, cmd, ".", &container.ExecOpts{})

		assert.Error(t, res.Error)
		assert.True(
			t, status.IsUnavailableError(res.Error),
			"expecting UNAVAILABLE when executable file is missing, got: %s", res.Error)
		assert.Equal(t, -2, res.ExitCode)
	}
	{
		cmd := &repb.Command{Arguments: []string{"command_not_found_in_PATH"}}
		res := commandutil.Run(ctx, cmd, ".", &container.ExecOpts{})

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
	res := commandutil.Run(ctx, cmd, wd, &container.ExecOpts{})

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

	res := commandutil.Run(ctx, cmd, wd, &container.ExecOpts{})

	require.True(t, status.IsDeadlineExceededError(res.Error), "expected DeadlineExceeded but got: %s", res.Error)
	assert.Equal(t, "stdout\n", string(res.Stdout))
	assert.Equal(t, "stderr\n", string(res.Stderr))
}

func TestRun_SubprocessInOwnProcessGroup_Timeout(t *testing.T) {
	ctx := context.Background()
	wd := testfs.MakeTempDir(t)

	cmd := &repb.Command{Arguments: []string{
		"sh", "-c",
		// Spawn a nested process inside the shell which becomes its own process group leader.
		// Using a go binary here because sh does not have a straightforward
		// way to make the setpgid system call.
		testfs.RunfilePath(t, "enterprise/server/remote_execution/commandutil/test_binary/test_binary_/test_binary"),
	}}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	res := commandutil.Run(ctx, cmd, wd, &container.ExecOpts{})

	assert.True(t, status.IsDeadlineExceededError(res.Error), "expected DeadlineExceeded but got: %s", res.Error)
	assert.Equal(t, "stdout\n", string(res.Stdout))
	assert.Equal(t, "stderr\n", string(res.Stderr))
}

func runSh(ctx context.Context, script string) *interfaces.CommandResult {
	cmd := &repb.Command{Arguments: []string{"sh", "-c", script}}
	return commandutil.Run(ctx, cmd, ".", &container.ExecOpts{})
}
