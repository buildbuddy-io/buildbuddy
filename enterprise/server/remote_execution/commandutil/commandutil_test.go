package commandutil_test

import (
	"bytes"
	"context"
	"fmt"
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
	res := commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &container.Stdio{
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

		res := commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &container.Stdio{
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
		res := commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &container.Stdio{})

		assert.Error(t, res.Error)
		assert.True(
			t, status.IsUnavailableError(res.Error),
			"expecting UNAVAILABLE when executable file is missing, got: %s", res.Error)
		assert.Equal(t, -2, res.ExitCode)
	}
	{
		cmd := &repb.Command{Arguments: []string{"command_not_found_in_PATH"}}
		res := commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &container.Stdio{})

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
	res := commandutil.Run(ctx, cmd, wd, nil /*=statsListener*/, &container.Stdio{})

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

	res := commandutil.Run(ctx, cmd, wd, nil /*=statsListener*/, &container.Stdio{})

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

	res := commandutil.Run(ctx, cmd, wd, nil /*=statsListener*/, &container.Stdio{})

	assert.True(t, status.IsDeadlineExceededError(res.Error), "expected DeadlineExceeded but got: %s", res.Error)
	assert.Equal(t, "stdout\n", string(res.Stdout))
	assert.Equal(t, "stderr\n", string(res.Stderr))
}

func TestRun_EnableStats_RecordsMemoryStats(t *testing.T) {
	cmd := &repb.Command{Arguments: []string{
		"python3", "-c", useMemPythonScript(1e9, 2*time.Second),
	}}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res := commandutil.Run(ctx, cmd, ".", nopStatsListener, &container.Stdio{})

	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)
	require.GreaterOrEqual(
		t, res.UsageStats.PeakMemoryBytes, int64(1e9),
		"expected at least 1GB of memory usage")
	require.LessOrEqual(
		t, res.UsageStats.PeakMemoryBytes, int64(1.1e9),
		"expected not much more than 1GB of memory usage")
}

func TestRun_EnableStats_RecordsCPUStats(t *testing.T) {
	cmd := &repb.Command{Arguments: []string{
		"python3", "-c", useCPUPythonScript(3 * time.Second),
	}}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res := commandutil.Run(ctx, cmd, ".", nopStatsListener, &container.Stdio{})

	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)
	require.GreaterOrEqual(
		t, res.UsageStats.GetCpuNanos(), int64(2e9),
		"expected around 3s of CPU usage")
	require.LessOrEqual(
		t, res.UsageStats.GetCpuNanos(), int64(4e9),
		"expected around 3s of CPU usage")
}

func TestRun_EnableStats_ComplexProcessTree_RecordsStatsFromAllChildren(t *testing.T) {
	workDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"cpu1.py": useCPUPythonScript(3 * time.Second),
		"cpu2.py": useCPUPythonScript(1 * time.Second),
		"mem1.py": useMemPythonScript(1e9, 3*time.Second),
		"mem2.py": useMemPythonScript(500e6, 2*time.Second),
	})
	// Run 3 python processes concurrently, staggering them a bit to  make sure
	// we measure over time. Wrap some with 'sh -c' to test that we can handle
	// grandchildren as well.
	cmd := &repb.Command{Arguments: []string{"bash", "-c", `
		python3 ./cpu1.py &
		sleep 0.1
		sh -c 'python3 ./mem1.py' &
		sleep 0.1
		sh -c 'python3 ./cpu2.py' &
		sleep 0.1
		python3 ./mem2.py &
		wait
		`,
	}}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res := commandutil.Run(ctx, cmd, workDir, nopStatsListener, &container.Stdio{})

	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)
	require.GreaterOrEqual(
		t, res.UsageStats.GetCpuNanos(), int64(3e9),
		"expected around 4s of CPU usage")
	require.LessOrEqual(
		t, res.UsageStats.GetCpuNanos(), int64(5e9),
		"expected around 4s of CPU usage")
	require.GreaterOrEqual(
		t, res.UsageStats.GetPeakMemoryBytes(), int64(1500e6),
		"expected at least 1.5GB peak memory")
	require.LessOrEqual(
		t, res.UsageStats.GetPeakMemoryBytes(), int64(1700e6),
		"expected not much more than 1.5GB peak memory")
}

// Returns a python script that consumes 1 CPU core continuously for the given
// duration.
func useCPUPythonScript(dur time.Duration) string {
	return fmt.Sprintf(`
import time
end = time.time() + %f
while time.time() < end:
    pass
`, dur.Seconds())
}

// Returns a python script that uses the given amount of resident memory and
// holds onto that memory for the given duration.
func useMemPythonScript(memBytes int64, dur time.Duration) string {
	return fmt.Sprintf(`
import time
arr = b'1' * %d
time.sleep(%f)
`, memBytes, dur.Seconds())
}

func runSh(ctx context.Context, script string) *interfaces.CommandResult {
	cmd := &repb.Command{Arguments: []string{"sh", "-c", script}}
	return commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &container.Stdio{})
}

func nopStatsListener(*repb.UsageStats) {}
