package remote_execution_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	maxSchedulerAttempts = 5
)

func TestSimpleCommandWithNonZeroExitCode(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2 && exit 5")
	res := cmd.Wait()

	assert.Equal(t, 5, res.ExitCode, "exit code should be propagated")
	assert.Equal(t, "hello\n", res.Stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", res.Stderr, "stderr should be propagated")
}

func TestActionResultCacheWithSuccessfulAction(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	invocationID := "testabc123"
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"echo"},
		Platform:  platform,
	}, &rbetest.ExecuteOpts{
		InvocationID: invocationID,
	})

	res := cmd.Wait()
	assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")

	_, err := rbe.GetActionResultForFailedAction(context.Background(), cmd, invocationID)
	assert.True(t, status.IsNotFoundError(err))
}

func TestActionResultCacheWithFailedAction(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	invocationID := "testabc123"
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"sh", "-c", "echo hello && echo bye >&2 && exit 5"},
		Platform:  platform,
	}, &rbetest.ExecuteOpts{
		InvocationID: invocationID,
	})

	res := cmd.Wait()
	assert.Equal(t, 5, res.ExitCode, "exit code should be propagated")

	ctx := context.Background()
	failedActionResult, err := rbe.GetActionResultForFailedAction(ctx, cmd, invocationID)
	assert.NoError(t, err)
	assert.Equal(t, int32(5), failedActionResult.GetExitCode(), "exit code should be set in action result")
	stdout, stderr, err := rbe.GetStdoutAndStderr(ctx, failedActionResult, res.InstanceName)
	assert.NoError(t, err)
	assert.Equal(t, "hello\n", stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", stderr, "stderr should be propagated")

	// Verify that it returns NotFound error when looking up action result with unmodified
	// action digest when the action failed.
	_, err = cachetools.GetActionResult(ctx, rbe.GetActionResultStorageClient(), cmd.GetActionResourceName())
	assert.True(t, status.IsNotFoundError(err))
}

func TestSimpleCommandWithZeroExitCode(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2")
	res := cmd.Wait()

	assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
	assert.Equal(t, "hello\n", res.Stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", res.Stderr, "stderr should be propagated")
}

func TestSimpleCommand_Timeout_StdoutStderrStillVisible(t *testing.T) {
	ctx := context.Background()
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	invocationID := "testabc123"

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(
		&repb.Command{Arguments: []string{"sh", "-c", `
			echo >&1 ExampleStdout
			echo >&2 ExampleStderr
      # Wait for context to be canceled
			sleep 100
		`},
			Platform: platform,
		},
		&rbetest.ExecuteOpts{
			ActionTimeout: 750 * time.Millisecond,
			InvocationID:  invocationID,
		},
	)
	res := cmd.MustTerminateAbnormally()

	require.True(t, status.IsDeadlineExceededError(res.Err), "expected DeadlineExceeded, got: %s", res.Err)
	ar := res.ActionResult
	assert.Less(t, ar.GetExitCode(), int32(0), "expecting exit code < 0 since command did not exit normally")
	stdout, stderr, err := rbe.GetStdoutAndStderr(ctx, ar, "")
	require.NoError(t, err)
	assert.Equal(t, "ExampleStdout\n", stdout, "stdout should be propagated")
	assert.Equal(t, "ExampleStderr\n", stderr, "stderr should be propagated")
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, 1, int(taskCount-initialTaskCount), "unexpected number of tasks started")
	ar, err = rbe.GetActionResultForFailedAction(ctx, cmd, invocationID)
	require.NoError(t, err)
	assert.True(
		t, proto.Equal(res.ActionResult, ar),
		"failed action result should match what was sent in the ExecuteResponse")
}

func TestSimpleCommand_CommandNotFound_FailedPrecondition(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	cmd := rbe.ExecuteCustomCommand("/COMMAND_THAT_DOES_NOT_EXIST")
	res := cmd.MustTerminateAbnormally()
	err := res.Err

	require.Error(t, err)
	assert.True(t, status.IsFailedPreconditionError(err))
	assert.Contains(t, status.Message(err), "no such file or directory")
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, 1, int(taskCount-initialTaskCount), "unexpected number of tasks started")
}

func TestSimpleCommand_Abort_ReturnsExecutionError(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	cmd := rbe.ExecuteCustomCommand("sh", "-c", `
		echo >&2 "Debug message to help diagnose abort()"
		kill -ABRT $$
	`)
	res := cmd.MustTerminateAbnormally()
	err := res.Err

	// TODO(bduffany): Aborted probably makes a bit more sense here
	assert.True(t, status.IsResourceExhaustedError(err), "expecting ResourceExhausted error but got: %s", err)
	assert.Contains(t, err.Error(), fmt.Sprintf("signal: %s", syscall.SIGABRT))
	assert.Equal(t, "Debug message to help diagnose abort()\n", res.Stderr)
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	// The executor thinks this is a Bazel task, so will let bazel retry.
	assert.Equal(t, 1, int(taskCount-initialTaskCount), "unexpected number of tasks started")
}

func TestWorkflowCommand_InternalError_RetriedByScheduler(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	errResult := commandutil.ErrorResult(status.InternalError("test error message"))
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{
		RunInterceptor: rbetest.AlwaysReturn(errResult),
	})
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	// Args here don't matter since we stub out the result, but the CI runner
	// command identifies this as a CI runner task.
	cmd := rbe.ExecuteCustomCommand("./buildbuddy_ci_runner", "...")
	err := cmd.MustFailAfterSchedulerRetry()

	require.True(t, status.IsInternalError(err), "expected Internal error, got: %s", err)
	require.Contains(t, err.Error(), "test error message")
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, maxSchedulerAttempts, int(taskCount-initialTaskCount), "scheduler should have retried the task")
}

func TestWorkflowCommand_ExecutorShutdown_RetriedByScheduler(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	errResult := commandutil.ErrorResult(commandutil.ErrSIGKILL)
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{
		RunInterceptor: rbetest.AlwaysReturn(errResult),
	})
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	// Args here don't matter since we stub out the result, but the CI runner
	// command identifies this as a CI runner task.
	cmd := rbe.ExecuteCustomCommand("./buildbuddy_ci_runner", "...")
	err := cmd.MustFailAfterSchedulerRetry()

	require.Contains(t, err.Error(), "command was terminated by SIGKILL, likely due to executor shutdown or OOM")
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, maxSchedulerAttempts, int(taskCount-initialTaskCount), "scheduler should have retried the task")
}

func TestTimeoutAlwaysReturnsDeadlineExceeded(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{
		RunInterceptor: func(ctx context.Context, original rbetest.RunFunc) *interfaces.CommandResult {
			// Wait for the action timeout to pass
			<-ctx.Done()
			// Intentionally return Unavailable despite the context deadline being
			// exceeded, to test that Container implementations should not have to
			// worry too much about the exact error returned when there is a timeout.
			wrappedErr := status.UnavailableErrorf("wrapped error: %s", ctx.Err())
			return commandutil.ErrorResult(wrappedErr)
		},
	})
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	opts := &rbetest.ExecuteOpts{ActionTimeout: 1 * time.Millisecond}
	// Note: The actual command here doesn't matter since we override the response.
	cmd := rbe.Execute(
		&repb.Command{
			Arguments: []string{"sh", "-c", "exit 0"},
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "OSFamily", Value: runtime.GOOS},
					{Name: "Arch", Value: runtime.GOARCH},
				},
			},
		},
		opts,
	)
	res := cmd.MustTerminateAbnormally()

	assert.True(t, status.IsDeadlineExceededError(res.Err), "expected DeadlineExceeded, got: %s", res.Err)
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, 1, int(taskCount-initialTaskCount), "unexpected attempt count")
}

func TestSimpleCommandWithExecutorAuthorizationEnabled(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		SchedulerServerOptions: scheduler_server.Options{
			RequireExecutorAuthorization: true,
		},
	})
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{
		Name:   "executor",
		APIKey: rbe.APIKey1,
	})

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2")
	cmd.Wait()
}

func TestSimpleCommand_RunnerReuse_CanReadPreviouslyWrittenFileButNotOutputDirs(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "none"},
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}

	// Note: authentication is required for workspace reuse, currently.
	opts := &rbetest.ExecuteOpts{APIKey: rbe.APIKey1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"touch", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform:          platform,
		OutputDirectories: []string{"output_dir"},
		OutputFiles:       []string{"output.txt"},
	}, opts)
	cmd.Wait()

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{
			"ls", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform: platform,
	}, opts)
	res := cmd.Wait()

	assert.Equal(
		t, "undeclared_output.txt\n", res.Stdout,
		"should be able to read undeclared outputs but not other outputs")
}

func TestSimpleCommand_RunnerReuse_ReLinksFilesFromFileCache(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"f1.input": "A",
		"f2.input": "B",
	})
	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "none"},
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir, APIKey: rbe.APIKey1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, "AB", res.Stdout)

	tmpDir = testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		// Overwrite "a.input" with "B" so that we attempt to link over "a.input"
		// from the filecache ("B" should exist in the filecache since it was
		// present as "b.input" in the previous action).
		"f1.input": "B",
	})
	opts = &rbetest.ExecuteOpts{InputRootDir: tmpDir, APIKey: rbe.APIKey1}

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	// If this still equals "A" then we probably didn't properly delete
	// "a.input" before linking it from FileCache.
	require.Equal(t, "BB", res.Stdout)
}

func TestSimpleCommand_RunnerReuse_ReLinksFilesFromDuplicateInputs(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"f1.input": "A",
		"f2.input": "A",
	})
	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "none"},
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir, APIKey: rbe.APIKey1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, "AA", res.Stdout)

	tmpDir = testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"f1.input": "B",
		"f2.input": "B",
	})
	opts = &rbetest.ExecuteOpts{InputRootDir: tmpDir, APIKey: rbe.APIKey1}

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"cat", "f1.input", "f2.input"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	// If either of these is equal to "A" then we didn't properly link the file
	// from its duplicate.
	require.Equal(t, "BB", res.Stdout)
}

func TestSimpleCommand_RunnerReuse_MultipleExecutors_RoutesCommandToSameExecutor(t *testing.T) {
	ctx := context.Background()
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServers(3)
	rbe.AddExecutors(t, 10)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
			{Name: "runner-recycling-max-wait", Value: "1m"},
		},
	}
	opts := &rbetest.ExecuteOpts{APIKey: rbe.APIKey1}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"touch", "foo.txt"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)

	rbetest.WaitForAnyPooledRunner(t, ctx)

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"stat", "foo.txt"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	require.Equal(t, "", res.Stderr)
	require.Equal(t, 0, res.ExitCode)
}

func TestSimpleCommand_RunnerReuse_PoolSelectionViaHeader_RoutesCommandToSameExecutor(t *testing.T) {
	ctx := context.Background()
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServers(3)
	for i := 0; i < 5; i++ {
		rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Pool: "foo"})
	}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "preserve-workspace", Value: "true"},
			{Name: "Pool", Value: "THIS_VALUE_SHOULD_BE_OVERRIDDEN"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
			{Name: "runner-recycling-max-wait", Value: "1m"},
		},
	}
	opts := &rbetest.ExecuteOpts{
		APIKey: rbe.APIKey1,
		RemoteHeaders: map[string]string{
			"x-buildbuddy-platform.pool": "foo",
		},
	}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"touch", "foo.txt"},
		Platform:  platform,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)

	rbetest.WaitForAnyPooledRunner(t, ctx)

	cmd = rbe.Execute(&repb.Command{
		Arguments: []string{"stat", "foo.txt"},
		Platform:  platform,
	}, opts)
	res = cmd.Wait()

	require.Equal(t, "", res.Stderr)
	require.Equal(t, 0, res.ExitCode)
}

func TestSimpleCommandWithMultipleExecutors(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(t, 5)

	cmd := rbe.ExecuteCustomCommand("sh", "-c", "echo hello && echo bye >&2")
	res := cmd.Wait()

	assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
	assert.Equal(t, "hello\n", res.Stdout, "stdout should be propagated")
	assert.Equal(t, "bye\n", res.Stderr, "stderr should be propagated")
}

func TestSimpleCommandWithPoolSelectionViaPlatformProp_Success(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Pool: "FOO"})

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "Pool", Value: "Foo"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	opts := &rbetest.ExecuteOpts{}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"touch", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform:          platform,
		OutputDirectories: []string{"output_dir"},
		OutputFiles:       []string{"output.txt"},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
}

func TestSimpleCommandWithPoolSelectionViaPlatformProp_Failure(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Pool: "bar"})
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "Pool", Value: "foo"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	opts := &rbetest.ExecuteOpts{}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"touch", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform:          platform,
		OutputDirectories: []string{"output_dir"},
		OutputFiles:       []string{"output.txt"},
	}, opts)
	err := cmd.MustFailToStart()

	require.Contains(t, err.Error(), `No registered executors in pool "foo"`)
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, 0, int(taskCount-initialTaskCount), "unexpected number of tasks started")
}

func TestSimpleCommandWithPoolSelectionViaHeader(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Pool: "foo"})
	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "Pool", Value: "THIS_VALUE_SHOULD_BE_OVERRIDDEN"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	opts := &rbetest.ExecuteOpts{
		RemoteHeaders: map[string]string{
			"x-buildbuddy-platform.pool": "foo",
		},
	}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"touch", "output.txt", "undeclared_output.txt", "output_dir/output.txt",
		},
		Platform:          platform,
		OutputDirectories: []string{"output_dir"},
		OutputFiles:       []string{"output.txt"},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
}

func TestSimpleCommandWithOSArchPool_CaseInsensitive(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("Test assumed running on linux")
	}
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Pool: "foo"})
	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "Pool", Value: "FoO"},
			// GOOS / GOARCH are normally lowercase, make sure uppercase
			// variants are treated equivalently.
			{Name: "OSFamily", Value: strings.ToUpper(runtime.GOOS)},
			{Name: "Arch", Value: strings.ToUpper(runtime.GOARCH)},
		},
	}

	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"pwd"},
		Platform:  platform,
	}, &rbetest.ExecuteOpts{})
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
}

func TestSimpleCommand_DefaultWorkspacePermissions(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("Test requires GNU stat")
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	inputRoot := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, inputRoot, map[string]string{
		"input_dir/input.txt": "",
	})

	dirs := []string{
		".", "output_dir", "output_dir_parent", "output_dir_parent/output_dir_child",
		"output_file_parent", "input_dir",
	}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}

	cmd := rbe.Execute(&repb.Command{
		// %a %n prints perms in octal followed by the file name.
		Arguments:         append([]string{"stat", "--format", "%a %n"}, dirs...),
		OutputDirectories: []string{"output_dir", "output_dir_parent/output_dir_child"},
		OutputFiles:       []string{"output_file_parent/output.txt"},
		Platform:          platform,
	}, &rbetest.ExecuteOpts{InputRootDir: inputRoot})
	res := cmd.Wait()

	expectedOutput := ""
	for _, dir := range dirs {
		expectedOutput += "755 " + dir + "\n"
	}

	require.Equal(t, expectedOutput, res.Stdout)
}

func TestSimpleCommand_NonrootWorkspacePermissions(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("Test requires GNU stat")
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "nonroot-workspace", Value: "true"},
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}

	inputRoot := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, inputRoot, map[string]string{
		"input_dir/input.txt": "",
	})

	dirs := []string{
		".", "output_dir", "output_dir_parent", "output_dir_parent/output_dir_child",
		"output_file_parent", "input_dir",
	}

	cmd := rbe.Execute(&repb.Command{
		// %a %n prints perms in octal followed by the file name.
		Arguments:         append([]string{"stat", "--format", "%a %n"}, dirs...),
		OutputDirectories: []string{"output_dir", "output_dir_parent/output_dir_child"},
		OutputFiles:       []string{"output_file_parent/output.txt"},
		Platform:          platform,
	}, &rbetest.ExecuteOpts{InputRootDir: inputRoot})
	res := cmd.Wait()

	expectedOutput := ""
	for _, dir := range dirs {
		expectedOutput += "777 " + dir + "\n"
	}

	require.Equal(t, expectedOutput, res.Stdout)
}

func TestManySimpleCommandsWithMultipleExecutors(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(t, 5)

	var cmds []*rbetest.Command
	for i := 0; i < 5; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for i := range cmds {
		res := cmds[i].Wait()
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}
}

func TestRedisAvailabilityMonitoring(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	flags.Set(t, "remote_execution.enable_redis_availability_monitoring", true)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(t, 5)

	var cmds []*rbetest.Command
	for i := 0; i < 5; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for i := range cmds {
		res := cmds[i].Wait()
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}
}

func TestBasicActionIO(t *testing.T) {
	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"greeting.input":       "Hello ",
		"child/farewell.input": "Goodbye ",
	})

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"sh", "-c", strings.Join([]string{
				`set -e`,
				// No need to create the output directory itself; executor is
				// responsible for that because it's specified as an
				// OutputDirectory.
				`cp greeting.input out_dir/hello_world.output`,
				`printf 'world' >> out_dir/hello_world.output`,
				// Create a file in a child dir of the output directory.
				`mkdir out_dir/child`,
				`cp child/farewell.input out_dir/child/goodbye_world.output`,
				`printf 'world' >> out_dir/child/goodbye_world.output`,
				// Create an explicitly declared output
				`cp greeting.input out_files_dir/hello_bb.output`,
				`printf 'BB' >> out_files_dir/hello_bb.output`,
				// Create another explicitly declared output.
				// Because this is an OutputFile, its parent is created by the
				// executor.
				`cp child/farewell.input out_files_dir/child/goodbye_bb.output`,
				`printf 'BB' >> out_files_dir/child/goodbye_bb.output`,
			}, "\n"),
		},
		Platform:          platform,
		OutputDirectories: []string{"out_dir"},
		OutputFiles: []string{
			"out_files_dir/hello_bb.output",
			"out_files_dir/child/goodbye_bb.output",
		},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	testfs.AssertExactFileContents(t, outDir, map[string]string{
		"out_dir/hello_world.output":            "Hello world",
		"out_dir/child/goodbye_world.output":    "Goodbye world",
		"out_files_dir/hello_bb.output":         "Hello BB",
		"out_files_dir/child/goodbye_bb.output": "Goodbye BB",
	})
}

func TestComplexActionIO(t *testing.T) {
	tmpDir := testfs.MakeTempDir(t)
	// Write a mix of small and large files, to ensure we can handle batching
	// lots of small files that fit within the gRPC limit, as well as individual
	// that exceed the gRPC limit.
	smallSizes := []int{
		1e2, 1e3, 1e4, 1e5, 1e6,
		1e2, 1e3, 1e4, 1e5, 1e6,
	}
	largeSizes := []int{1e7}
	// Write files to several different directories to ensure we handle directory
	// creation properly.
	dirLayout := map[string][]int{
		"" /*root*/ : smallSizes,
		"a":          smallSizes,
		"b":          smallSizes,
		"a/child":    smallSizes,
		"b/child":    largeSizes,
	}
	outputFiles := []string{}
	contents := map[string]string{}
	for dir, sizes := range dirLayout {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0777); err != nil {
			assert.FailNow(t, err.Error())
		}
		for i, size := range sizes {
			relPath := filepath.Join(dir, fmt.Sprintf("file_%d.input", i))
			content := testfs.WriteRandomString(t, tmpDir, relPath, size)
			contents[relPath] = content
			outputFiles = append(outputFiles, filepath.Join("out_files_dir", dir, fmt.Sprintf("file_%d.output", i)))
		}
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"sh", "-c", strings.Join([]string{
			`set -e`,
			`input_paths=$(find . -type f)`,
			// Mirror the input tree to out_files_dir, skipping the first byte
			// so that the output digests are different. Note that we don't
			// create directories here since the executor is responsible for
			// creating parent dirs of output files.
			`
			for path in $input_paths; do
				opath="out_files_dir/$(echo "$path" | sed 's/.input/.output/')"
				cat "$path" | tail -c +2 > "$opath"
			done
			`,
			// Mirror the input tree to out_dir, skipping the first 2 bytes this
			// time. We *do* need to create parent dirs since the executor is
			// only responsible for creating the top-level out_dir.
			`
			for path in $input_paths; do
				output_path="out_dir/$(echo "$path" | sed 's/.input/.output/')"
				mkdir -p out_dir/"$(dirname "$path")"
				cat "$path" | tail -c +3 > "$output_path"
			done
			`,
		}, "\n")},
		Platform:          platform,
		OutputDirectories: []string{"out_dir"},
		OutputFiles:       outputFiles,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
	require.Empty(t, res.Stderr)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	skippedBytes := map[string]int{
		"out_files_dir": 1,
		"out_dir":       2,
	}
	missing := []string{}
	for parent, nSkippedBytes := range skippedBytes {
		for dir, sizes := range dirLayout {
			for i := range sizes {
				inputRelPath := filepath.Join(dir, fmt.Sprintf("file_%d.input", i))
				outputRelPath := filepath.Join(parent, dir, fmt.Sprintf("file_%d.output", i))
				if testfs.Exists(t, outDir, outputRelPath) {
					inputContents, ok := contents[inputRelPath]
					require.Truef(t, ok, "sanity check: missing input contents of %s", inputRelPath)
					expectedContents := inputContents[nSkippedBytes:]
					actualContents := testfs.ReadFileAsString(t, outDir, outputRelPath)
					// Not using assert.Equal here since the diff can be huge.
					assert.Truef(
						t, expectedContents == actualContents,
						"contents of %s did not match (expected len: %d; actual len: %d)",
						outputRelPath, len(expectedContents), len(actualContents),
					)
				} else {
					missing = append(missing, outputRelPath)
				}
			}
		}
	}
	assert.Empty(t, missing)
}

func TestOutputDirectoriesAndFiles(t *testing.T) {
	tmpDir := testfs.MakeTempDir(t)
	dirLayout := []string{
		"", "a", "a/a", "a/b", "b", "b/a", "b/b", "c", "c/a", "c/b", "d", "d/a",
		"d/b", "e", "e/a", "e/b",
	}
	files := []string{"a.txt", "b.txt"}
	for _, dir := range dirLayout {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0777); err != nil {
			assert.FailNow(t, err.Error())
		}
		for _, fname := range files {
			relPath := filepath.Join(dir, fname)
			testfs.WriteRandomString(t, tmpDir, relPath /* size= */, 10)
		}
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"sh", "-c", "cp -r " + filepath.Join(tmpDir, "*") + " output",
		},
		Platform:          platform,
		OutputDirectories: []string{"output/a", "output/b/a", "output/c/a"},
		OutputFiles: []string{
			"output/c/b/a.txt", "output/d/a.txt", "output/d/a/a.txt",
		},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
	require.Empty(t, res.Stderr)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	expectedFiles := []string{
		"a/a/a.txt", "a/a/b.txt", "a/b/a.txt", "a/b/b.txt", "b/a/a.txt",
		"b/a/b.txt", "c/a/a.txt", "c/a/b.txt", "c/b/a.txt", "d/a.txt",
		"d/a/a.txt",
	}
	unexpectedFiles := []string{
		"a.txt", "b.txt", "b/b", "c/b/b.txt", "d/a/b.txt", "d/b", "e",
	}
	for _, expectedFile := range expectedFiles {
		expectedOutputFile := filepath.Join("output", expectedFile)
		assert.Truef(t, testfs.Exists(t, outDir, expectedOutputFile),
			"expected file to exist: %s", expectedOutputFile)
	}
	for _, unexpectedFile := range unexpectedFiles {
		unexpectedOutputFile := filepath.Join("output", unexpectedFile)
		assert.Falsef(t, testfs.Exists(t, outDir, unexpectedOutputFile),
			"expected file to not exist: %s", unexpectedOutputFile)
	}
}

func TestOutputPaths(t *testing.T) {
	tmpDir := testfs.MakeTempDir(t)
	dirLayout := []string{
		"", "a", "a/a", "a/b", "b", "b/a", "b/b", "c", "c/a", "c/b", "d", "d/a",
		"d/b", "e", "e/a", "e/b",
	}
	files := []string{"a.txt", "b.txt"}
	for _, dir := range dirLayout {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0777); err != nil {
			assert.FailNow(t, err.Error())
		}
		for _, fname := range files {
			relPath := filepath.Join(dir, fname)
			testfs.WriteRandomString(t, tmpDir, relPath /* size= */, 10)
		}
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"sh", "-c", "cp -r " + filepath.Join(tmpDir, "*") + " output",
		},
		Platform: platform,
		OutputPaths: []string{
			"output/a", "output/b/a", "output/c/a", "output/c/b/a.txt",
			"output/d/a.txt", "output/d/a/a.txt",
		},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
	require.Empty(t, res.Stderr)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	expectedFiles := []string{
		"a/a/a.txt", "a/a/b.txt", "a/b/a.txt", "a/b/b.txt", "b/a/a.txt",
		"b/a/b.txt", "c/a/a.txt", "c/a/b.txt", "c/b/a.txt", "d/a.txt",
		"d/a/a.txt",
	}
	unexpectedFiles := []string{
		"a.txt", "b.txt", "b/b", "c/b/b.txt", "d/a/b.txt", "d/b", "e",
	}
	for _, expectedFile := range expectedFiles {
		expectedOutputFile := filepath.Join("output", expectedFile)
		assert.Truef(t, testfs.Exists(t, outDir, expectedOutputFile),
			"expected file to exist: %s", expectedOutputFile)
	}
	for _, unexpectedFile := range unexpectedFiles {
		unexpectedOutputFile := filepath.Join("output", unexpectedFile)
		assert.Falsef(t, testfs.Exists(t, outDir, unexpectedOutputFile),
			"expected file to not exist: %s", unexpectedOutputFile)
	}
}

func TestOutputPathsDirectoriesAndFiles(t *testing.T) {
	tmpDir := testfs.MakeTempDir(t)
	dirLayout := []string{"", "a", "b", "c"}
	files := []string{"a.txt", "b.txt"}
	for _, dir := range dirLayout {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0777); err != nil {
			assert.FailNow(t, err.Error())
		}
		for _, fname := range files {
			relPath := filepath.Join(dir, fname)
			testfs.WriteRandomString(t, tmpDir, relPath /* size= */, 10)
		}
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{
			"sh", "-c", "cp -r " + filepath.Join(tmpDir, "*") + " output",
		},
		Platform:          platform,
		OutputFiles:       []string{"output/a/a.txt"},
		OutputDirectories: []string{"output/b"},
		OutputPaths:       []string{"output/c"},
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
	require.Empty(t, res.Stderr)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	// output_paths should supersede output_files and output_directories.
	expectedFiles := []string{"c/a.txt", "c/b.txt"}
	unexpectedFiles := []string{"a", "b"}
	for _, expectedFile := range expectedFiles {
		expectedOutputFile := filepath.Join("output", expectedFile)
		assert.Truef(t, testfs.Exists(t, outDir, expectedOutputFile),
			"expected file to exist: %s", expectedOutputFile)
	}
	for _, unexpectedFile := range unexpectedFiles {
		unexpectedOutputFile := filepath.Join("output", unexpectedFile)
		assert.Falsef(t, testfs.Exists(t, outDir, unexpectedOutputFile),
			"expected file to not exist: %s", unexpectedOutputFile)
	}
}

func TestComplexActionIOWithCompression(t *testing.T) {
	flags.Set(t, "cache.zstd_transcoding_enabled", true)
	flags.Set(t, "cache.client.enable_upload_compression", true)
	flags.Set(t, "cache.client.enable_download_compression", true)

	tmpDir := testfs.MakeTempDir(t)
	// Write a mix of small and large files, to ensure we can handle batching
	// lots of small files that fit within the gRPC limit, as well as individual
	// that exceed the gRPC limit.
	smallSizes := []int{
		1e2, 1e3, 1e4, 1e5, 1e6,
		1e2, 1e3, 1e4, 1e5, 1e6,
	}
	largeSizes := []int{1e7}
	// Write files to several different directories to ensure we handle directory
	// creation properly.
	dirLayout := map[string][]int{
		"" /*root*/ : smallSizes,
		"a":          smallSizes,
		"b":          smallSizes,
		"a/child":    smallSizes,
		"b/child":    largeSizes,
	}
	outputFiles := []string{}
	contents := map[string]string{}
	for dir, sizes := range dirLayout {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0777); err != nil {
			assert.FailNow(t, err.Error())
		}
		for i, size := range sizes {
			relPath := filepath.Join(dir, fmt.Sprintf("file_%d.input", i))
			content := testfs.WriteRandomString(t, tmpDir, relPath, size)
			contents[relPath] = content
			outputFiles = append(outputFiles, filepath.Join("out_files_dir", dir, fmt.Sprintf("file_%d.output", i)))
		}
	}

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"sh", "-c", strings.Join([]string{
			`set -e`,
			`input_paths=$(find . -type f)`,
			// Mirror the input tree to out_files_dir, skipping the first byte so that
			// the output digests are different. Note that we don't create directories
			// here since the executor is responsible for creating parent dirs of
			// output files.
			`
			for path in $input_paths; do
				output_path="out_files_dir/$(echo "$path" | sed 's/.input/.output/')"
				cat "$path" | tail -c +2 > "$output_path"
			done
			`,
			// Mirror the input tree to out_dir, skipping the first 2 bytes this time.
			// We *do* need to create parent dirs since the executor is only
			// responsible for creating the top-level out_dir.
			`
			for path in $input_paths; do
				output_path="out_dir/$(echo "$path" | sed 's/.input/.output/')"
				mkdir -p out_dir/"$(dirname "$path")"
				cat "$path" | tail -c +3 > "$output_path"
			done
			`,
		}, "\n")},
		Platform:          platform,
		OutputDirectories: []string{"out_dir"},
		OutputFiles:       outputFiles,
	}, opts)
	res := cmd.Wait()

	require.Equal(t, 0, res.ExitCode)
	require.Empty(t, res.Stderr)

	outDir := rbe.DownloadOutputsToNewTempDir(res)

	skippedBytes := map[string]int{
		"out_files_dir": 1,
		"out_dir":       2,
	}
	missing := []string{}
	for parent, nSkippedBytes := range skippedBytes {
		for dir, sizes := range dirLayout {
			for i := range sizes {
				inputRelPath := filepath.Join(dir, fmt.Sprintf("file_%d.input", i))
				outputRelPath := filepath.Join(parent, dir, fmt.Sprintf("file_%d.output", i))
				if testfs.Exists(t, outDir, outputRelPath) {
					inputContents, ok := contents[inputRelPath]
					require.Truef(t, ok, "sanity check: missing input contents of %s", inputRelPath)
					expectedContents := inputContents[nSkippedBytes:]
					actualContents := testfs.ReadFileAsString(t, outDir, outputRelPath)
					// Not using assert.Equal here since the diff can be huge.
					assert.Truef(
						t, expectedContents == actualContents,
						"contents of %s did not match (expected len: %d; actual len: %d)",
						outputRelPath, len(expectedContents), len(actualContents),
					)
				} else {
					missing = append(missing, outputRelPath)
				}
			}
		}
	}
	assert.Empty(t, missing)
}

func TestSaturateTaskQueue(t *testing.T) {
	// Configure the scheduler to allow 2 BCU worth of concurrent tasks.
	mem := 2 * tasksize.ComputeUnitsToRAMBytes / tasksize.MaxResourceCapacityRatio
	cpu := 2 * tasksize.ComputeUnitsToMilliCPU / tasksize.MaxResourceCapacityRatio
	flags.Set(t, "executor.memory_bytes", int64(mem)+1)
	flags.Set(t, "executor.millicpu", int64(cpu)+1)

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	// Schedule 10 tasks concurrently which each consume 1 BCU, so that only up
	// to 2 tasks can be run at once.
	cmdProto := &repb.Command{
		Arguments: []string{"sleep", "0.05"}, // 50ms
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "OSFamily", Value: runtime.GOOS},
				{Name: "Arch", Value: runtime.GOARCH},
				{Name: "EstimatedComputeUnits", Value: "1"},
			},
		},
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			cmd := rbe.Execute(cmdProto, &rbetest.ExecuteOpts{})
			res := cmd.Wait()

			require.NoError(t, res.Err)
			require.Equal(t, 0, res.ExitCode)
		}()
	}
	wg.Wait()
}

func TestUnregisterExecutor(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()

	// Start with two executors.
	// AddExecutors will block until both are registered.
	executors := rbe.AddExecutors(t, 2)

	// Remove one of the executors.
	// RemoveExecutor will block until the executor is unregistered.
	rbe.RemoveExecutor(executors[0])
}

func TestMultipleSchedulersAndExecutors(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	// Start with 2 BuildBuddy servers.
	rbe.AddBuildBuddyServer()
	rbe.AddBuildBuddyServer()
	rbe.AddExecutors(t, 5)

	var cmds []*rbetest.Command
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for i := range cmds {
		res := cmds[i].Wait()
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}
}

func TestWorkSchedulingOnNewExecutor(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServers(5)
	rbe.AddSingleTaskExecutorWithOptions(t, &rbetest.ExecutorOptions{Name: "busyExecutor1"})
	rbe.AddSingleTaskExecutorWithOptions(t, &rbetest.ExecutorOptions{Name: "busyExecutor2"})

	// Schedule 2 controlled commands to keep existing executors busy.
	cmd1 := rbe.ExecuteControlledCommand("command1", &rbetest.ExecuteControlledOpts{})
	cmd2 := rbe.ExecuteControlledCommand("command2", &rbetest.ExecuteControlledOpts{})

	// Wait until both the commands actually start running on the executors.
	cmd1.WaitStarted()
	cmd2.WaitStarted()

	// Schedule some additional commands that existing executors can't take on.
	var cmds []*rbetest.Command
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}

	for _, cmd := range cmds {
		cmd.WaitAccepted()
	}

	// Add a new executor that should get assigned the additional tasks.
	rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Name: "newExecutor"})

	for i, cmd := range cmds {
		res := cmd.Wait()
		assert.Equal(t, "newExecutor", res.ActionResult.GetExecutionMetadata().GetExecutorId(), "[%s] should have been executed on new executor", cmd.Name)
		assert.Equal(t, 0, res.ExitCode, "exit code should be propagated")
		assert.Equal(t, fmt.Sprintf("hello from command %d\n", i), res.Stdout, "stdout should be propagated")
		assert.Equal(t, "", res.Stderr, "stderr should be empty")
	}

	// Allow controlled commands to exit.
	cmd1.Exit(0)
	cmd2.Exit(0)

	cmd1.Wait()
	cmd2.Wait()
}

// Test WaitExecution across different severs.
func TestWaitExecution(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	// Start multiple servers so that executions are spread out across different servers.
	for i := 0; i < 5; i++ {
		rbe.AddBuildBuddyServer()
	}
	rbe.AddExecutors(t, 5)

	var cmds []*rbetest.ControlledCommand
	for i := 0; i < 10; i++ {
		cmds = append(cmds, rbe.ExecuteControlledCommand(fmt.Sprintf("command%d", i+1), &rbetest.ExecuteControlledOpts{}))
	}

	// Wait until all the commands have started running & have been accepted by the server.
	for _, c := range cmds {
		c.WaitStarted()
		c.WaitAccepted()
	}

	// Cancel in-flight Execute requests and call the WaitExecution API.
	for _, c := range cmds {
		c.ReplaceWaitUsingWaitExecutionAPI()
	}

	for i, c := range cmds {
		c.Exit(int32(i))
	}

	for i, cmd := range cmds {
		res := cmd.Wait()
		assert.Equal(t, i, res.ExitCode, "exit code should be propagated for command %q", cmd.Name)
	}
}

type fakeRankedNode struct {
	node interfaces.ExecutionNode
}

func (n fakeRankedNode) GetExecutionNode() interfaces.ExecutionNode {
	return n.node
}

func (_ fakeRankedNode) IsPreferred() bool {
	return false
}

type fixedNodeTaskRouter struct {
	mu          sync.Mutex
	executorIDs map[string]struct{}
}

func newFixedNodeTaskRouter(executorIDs []string) *fixedNodeTaskRouter {
	idSet := make(map[string]struct{})
	for _, id := range executorIDs {
		idSet[id] = struct{}{}
	}
	return &fixedNodeTaskRouter{executorIDs: idSet}
}

func (f *fixedNodeTaskRouter) RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.RankedExecutionNode {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []interfaces.RankedExecutionNode
	for _, n := range nodes {
		if _, ok := f.executorIDs[n.GetExecutorID()]; ok {
			out = append(out, fakeRankedNode{node: n})
		}
	}
	return out
}

func (f *fixedNodeTaskRouter) MarkComplete(ctx context.Context, cmd *repb.Command, remoteInstanceName, executorHostID string) {
}

func (f *fixedNodeTaskRouter) UpdateSubset(executorIDs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idSet := make(map[string]struct{})
	for _, id := range executorIDs {
		idSet[id] = struct{}{}
	}
	f.executorIDs = idSet
}

func TestTaskReservationsNotLostOnExecutorShutdown(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	var busyExecutorIDs []string
	for i := 1; i <= 3; i++ {
		busyExecutorIDs = append(busyExecutorIDs, fmt.Sprintf("busyExecutor%d", i))
	}

	// Set up the task router to send all reservations to "busy" executors. These executors will queue up tasks but not
	// try to execute any of them.
	taskRouter := newFixedNodeTaskRouter(busyExecutorIDs)
	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{EnvModifier: func(env *testenv.TestEnv) {
		env.SetTaskRouter(taskRouter)
	}})

	var busyExecutors []*rbetest.Executor
	for _, id := range busyExecutorIDs {
		e := rbe.AddSingleTaskExecutorWithOptions(t, &rbetest.ExecutorOptions{Name: id})
		e.ShutdownTaskScheduler()
		busyExecutors = append(busyExecutors, e)
	}
	// Add another executor that should execute all scheduled commands once the "busy" executors are shut down.
	_ = rbe.AddExecutorWithOptions(t, &rbetest.ExecutorOptions{Name: "newExecutor"})

	// Now schedule some commands. The fake task router will ensure that the reservations only land on "busy"
	// executors.
	var cmds []*rbetest.Command
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteCustomCommand("sh", "-c", fmt.Sprintf("echo 'hello from command %d'", i))
		cmds = append(cmds, cmd)
	}
	for _, cmd := range cmds {
		cmd.WaitAccepted()
	}

	// Update the task router to allow tasks to be routed to the non-busy executor.
	taskRouter.UpdateSubset(append(busyExecutorIDs, "newExecutor"))

	// Now shutdown the "busy" executors which should still have all the commands in their queues.
	// During shutdown the tasks should get re-enqueued onto the non-busy executor.
	for _, e := range busyExecutors {
		rbe.RemoveExecutor(e)
	}

	for _, cmd := range cmds {
		res := cmd.Wait()
		assert.Equal(t, "newExecutor", res.ActionResult.GetExecutionMetadata().GetExecutorId(), "[%s] should have been executed on new executor", cmd.Name)
	}
}

func TestCommandWithMissingInputRootDigest(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := rbe.Execute(&repb.Command{
		Arguments: []string{"echo"},
		Platform:  platform,
	}, &rbetest.ExecuteOpts{SimulateMissingDigest: true})
	err := cmd.MustFailToStart()

	require.True(t, status.IsFailedPreconditionError(err))
	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	// NotFound errors should not be retried by the scheduler, since this test
	// is simulating a bazel task, and bazel will retry on its own.
	assert.Equal(t, 1, int(taskCount-initialTaskCount), "unexpected number of tasks started")
}

func TestRedisRestart(t *testing.T) {
	workspaceContents := map[string]string{
		"WORKSPACE": `workspace(name = "integration_test")`,
		"BUILD": fmt.Sprintf(`genrule(
  name = "hello_txt",
  outs = ["hello.txt"],
  cmd_bash = "sleep 5 && echo 'Hello world' > $@",
  exec_properties = {
    "OSFamily": "%s",
    "Arch": "%s",
  },
)`, runtime.GOOS, runtime.GOARCH),
	}

	var redisShards []*testredis.Handle
	for i := 0; i < 4; i++ {
		redisShards = append(redisShards, testredis.StartTCP(t))
	}

	args := []string{
		"--remote_execution.enable_remote_exec=true",
		"--remote_execution.enable_redis_availability_monitoring=true",
	}
	for _, shard := range redisShards {
		args = append(args, "--remote_execution.sharded_redis.shards="+shard.Target)
	}
	app := buildbuddy_enterprise.RunWithConfig(t, buildbuddy_enterprise.DefaultAppConfig(t), buildbuddy_enterprise.NoAuthConfig, args...)

	_ = testexecutor.Run(
		t,
		testexecutor.ExecutorRunfilePath,
		[]string{"--executor.app_target=" + app.GRPCAddress()},
	)

	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := []string{"//:hello.txt"}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, app.RemoteExecutorBazelFlags()...)

	resultCh := make(chan *bazel.InvocationResult)
	go func() {
		resultCh <- testbazel.Invoke(ctx, t, ws, "build", buildFlags...)
	}()

	// Wait for the remote execution to start by looking for the presence of a
	// redis task key on one of the shards. This shard will become the victim.
	deadline := time.Now().Add(30 * time.Second)
	var victimShard *testredis.Handle
	for time.Now().Before(deadline) && victimShard == nil {
		for _, shard := range redisShards {
			if shard.KeyCount("task/*") > 0 {
				if victimShard != nil {
					require.FailNow(t, "multiple redis shards contain task information")
				}
				victimShard = shard
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotNil(t, victimShard, "could not find victim shard")

	// Restart the shard containing task information and verify that the Bazel
	// invocation can still finish successfully.
	victimShard.Restart()

	result := <-resultCh

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	require.NotContains(
		t, result.Stderr, "1 remote cache hit",
		"sanity check: initial build shouldn't be cached",
	)
}

func TestInvocationCancellation(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	bbServer := rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)
	initialTaskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	iid := uuid.NewString()
	bep, err := build_event_publisher.New(bbServer.GRPCAddress(), "", iid)
	require.NoError(t, err)

	ctx := context.Background()
	bep.Start(ctx)

	startTime := time.Now()
	err = bep.Publish(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{
				StartTime: timestamppb.New(startTime),
			},
		},
	})
	require.NoError(t, err)

	cmd1 := rbe.ExecuteControlledCommand("command1", &rbetest.ExecuteControlledOpts{InvocationID: iid})
	cmd1.WaitStarted()

	// Cancel the invocation.
	finishTime := time.Now()
	err = bep.Publish(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{
				ExitCode:   &bespb.BuildFinished_ExitCode{Name: "INTERRUPTED", Code: build_event_handler.InterruptedExitCode},
				FinishTime: timestamppb.New(finishTime),
			},
		},
	})
	require.NoError(t, err)

	// Verify that command cancellation was reported.
	cmd1.MustBeCancelled()

	// Also verify that the action itself was terminated.
	cmd1.WaitDisconnected()

	taskCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)
	assert.Equal(t, 1, int(taskCount-initialTaskCount), "unexpected number of tasks started")
}

func WaitForPendingExecution(rdb redis.UniversalClient, opID string) error {
	forwardKey, err := rdb.Get(context.Background(), fmt.Sprintf("pendingExecutionDigest/%s", opID)).Result()
	if err == redis.Nil {
		return status.NotFoundError("No reverse key for pending execution")
	}
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		_, err := rdb.Get(context.Background(), forwardKey).Result()
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return status.NotFoundError("No forward key for pending execution")
}

func TestActionMerging(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)

	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", "sleep 10"},
		Platform:  platform,
	}
	cmd1 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, InvocationID: "invocation1"})
	op1 := cmd1.WaitAccepted()
	WaitForPendingExecution(rbe.GetRedisClient(), op1)

	cmd2 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, InvocationID: "invocation2"})
	op2 := cmd2.WaitAccepted()
	require.Equal(t, op1, op2, "the execution IDs for both commands should be the same")

	cmd3 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, APIKey: rbe.APIKey1, InvocationID: "invocation3"})
	op3 := cmd3.WaitAccepted()
	WaitForPendingExecution(rbe.GetRedisClient(), op3)
	require.NotEqual(t, op2, op3, "actions under different organizations should not be merged")

	cmd4 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, APIKey: rbe.APIKey1, InvocationID: "invocation4"})
	op4 := cmd4.WaitAccepted()
	require.Equal(t, op3, op4, "expected actions to be merged")
}

func TestActionMerging_LongTask(t *testing.T) {
	flags.Set(t, "remote_execution.lease_duration", 100*time.Millisecond)
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		SchedulerServerOptions: scheduler_server.Options{
			ActionMergingLeaseTTLOverride: 250 * time.Millisecond,
		},
	})
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
		},
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", "sleep 60"},
		Platform:  platform,
	}
	cmd1 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, InvocationID: "invocation1"})
	op1 := cmd1.WaitAccepted()
	WaitForPendingExecution(rbe.GetRedisClient(), op1)

	// Wait long enough for the first action-merging state to have expired.
	time.Sleep(350 * time.Millisecond)

	// Ensure actions are still merged against the running original execution.
	cmd2 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, InvocationID: "invocation2"})
	op2 := cmd2.WaitAccepted()
	require.Equal(t, op1, op2, "the execution IDs for both commands should be the same")
}

func TestActionMerging_ClaimingAppDies(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	app := rbe.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		SchedulerServerOptions: scheduler_server.Options{
			ActionMergingLeaseTTLOverride: time.Millisecond,
		},
	})
	rbe.AddExecutor(t)

	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", "sleep 10"},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "OSFamily", Value: runtime.GOOS},
				{Name: "Arch", Value: runtime.GOARCH},
			},
		},
	}

	// Run a command which we know will be assigned by `app` to `executor`.
	cmd1 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, InvocationID: "invocation1"})
	op1 := cmd1.WaitAccepted()
	WaitForPendingExecution(rbe.GetRedisClient(), op1)

	// Start a new app, kill `app`, potentially orphaning `cmd1`, and then run
	// the command again, verifying it's not merged.
	rbe.AddBuildBuddyServer()
	rbe.RemoveBuildBuddyServer(app)

	// Sleep long enough for the action-merging data to timeout of Redis.
	time.Sleep(5 * time.Millisecond)

	// Confirm that subsequent actions are not merged against the orphaned
	// execution.
	cmd2 := rbe.Execute(cmd, &rbetest.ExecuteOpts{CheckCache: true, InvocationID: "invocation2"})
	op2 := cmd2.WaitAccepted()
	require.NotEqual(t, op1, op2, "unexpected action merge: dead app shouldn't block future actions")
}

func TestAppShutdownDuringExecution_PublishOperationRetried(t *testing.T) {
	// Set a short progress publish interval since we want to test killing an
	// app while an update stream is in progress, and want to catch the error
	// early.
	flags.Set(t, "executor.task_progress_publish_interval", 50*time.Millisecond)
	initialTasksStartedCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	rbe := rbetest.NewRBETestEnv(t)

	app1 := rbe.AddBuildBuddyServer()
	app2 := rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	// Set up a custom proxy director that makes sure we choose app1 for the
	// initial PublishOperation request, so that we can test stopping app1 while
	// the task is in progress.
	var mu sync.Mutex
	app1Healthy := true
	director := func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		mu.Lock()
		target := ""
		if app1Healthy && fullMethodName == "/build.bazel.remote.execution.v2.Execution/PublishOperation" {
			log.Debugf("Routing %q to app 1", fullMethodName)
			target = app1.GRPCAddress()
		} else {
			log.Debugf("Routing %q to app 2", fullMethodName)
			target = app2.GRPCAddress()
		}
		mu.Unlock()

		conn, err := grpc_client.DialSimpleWithoutPooling(target)
		if err != nil {
			return nil, nil, err
		}
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md.Copy())
		}
		return ctx, conn, nil
	}
	rbe.AppProxy.SetDirector(director)

	var cmds []*rbetest.ControlledCommand
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteControlledCommand(fmt.Sprintf("cmd-%d", i), &rbetest.ExecuteControlledOpts{
			// Allow reconnecting with WaitExecution since the test will hard
			// stop the app, which kills the Execute stream.
			AllowReconnect: true,
		})
		cmds = append(cmds, cmd)
	}
	for _, cmd := range cmds {
		cmd.WaitStarted()
	}

	// Maybe give enough time for a few progress updates to be published.
	randSleepMillis(0, 100)

	// Initiate a graceful shutdown of app 1 and have the proxy start directing
	// to app2. As soon as the graceful shutdown starts, the executor should try
	// to reconnect to a different app.
	mu.Lock()
	app1Healthy = false
	mu.Unlock()
	app1.Shutdown()

	// Simulate the shutdown grace period elapsing, which should cause a
	// hard stop of the gRPC server on app 1.
	time.Sleep(200 * time.Millisecond)
	app1.Stop()

	eg := &errgroup.Group{}
	for _, cmd := range cmds {
		cmd := cmd
		eg.Go(func() error {
			// Maybe let the command continue execution for a bit, then exit.
			randSleepMillis(0, 50)

			cmd.Exit(0)
			res := cmd.Wait()
			require.Equal(t, 0, res.ExitCode)
			return res.Err
		})
	}
	err := eg.Wait()
	require.NoError(t, err)

	// Make sure we only ever started a single task execution per command (tasks
	// should not be re-executed just because the app goes down).
	tasksStartedCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount) - initialTasksStartedCount
	require.Equal(t, float64(len(cmds)), tasksStartedCount, "no tasks should have been retried")
}

func TestAppShutdownDuringExecution_LeaseTaskRetried(t *testing.T) {
	// Set a short lease TTL since we want to test killing an app while an
	// update stream is in progress, and want to catch the error early.
	flags.Set(t, "remote_execution.lease_duration", 50*time.Millisecond)
	initialTasksStartedCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount)

	rbe := rbetest.NewRBETestEnv(t)

	app1 := rbe.AddBuildBuddyServer()
	app2 := rbe.AddBuildBuddyServer()

	// Set up a custom proxy director that makes sure we choose app1 for the
	// initial LeaseTask request, so that we can test stopping app1 while
	// the task is in progress.
	var mu sync.Mutex
	app1Healthy := true
	director := func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		mu.Lock()
		target := ""
		if app1Healthy && fullMethodName == "/scheduler.Scheduler/LeaseTask" {
			log.Debugf("Routing %q to app 1", fullMethodName)
			target = app1.GRPCAddress()
		} else {
			log.Debugf("Routing %q to app 2", fullMethodName)
			target = app2.GRPCAddress()
		}
		mu.Unlock()

		conn, err := grpc_client.DialSimpleWithoutPooling(target)
		if err != nil {
			return nil, nil, err
		}
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md.Copy())
		}
		return ctx, conn, nil
	}
	rbe.AppProxy.SetDirector(director)

	// Add the executor after the proxy so the executor registers with app 1.
	rbe.AddExecutor(t)

	var cmds []*rbetest.ControlledCommand
	for i := 0; i < 10; i++ {
		cmd := rbe.ExecuteControlledCommand(fmt.Sprintf("cmd-%d", i), &rbetest.ExecuteControlledOpts{
			// Allow reconnecting with WaitExecution since the test will hard
			// stop the app, which kills the Execute stream.
			AllowReconnect: true,
		})
		cmds = append(cmds, cmd)
	}
	for _, cmd := range cmds {
		cmd.WaitStarted()
	}

	// Maybe give enough time for a few lease pings to occur.
	randSleepMillis(0, 100)

	// Initiate a graceful shutdown of app 1 and have the proxy start directing
	// to app2. As soon as the graceful shutdown starts, the executor should try
	// to reconnect to a different app.
	mu.Lock()
	app1Healthy = false
	mu.Unlock()
	app1.Shutdown()

	// Simulate the shutdown grace period elapsing, which should cause a
	// hard stop of the gRPC server on app 1.
	time.Sleep(200 * time.Millisecond)
	app1.Stop()

	eg := &errgroup.Group{}
	for _, cmd := range cmds {
		cmd := cmd
		eg.Go(func() error {
			// Maybe let the command continue execution for a bit, then exit.
			randSleepMillis(0, 50)

			cmd.Exit(0)
			res := cmd.Wait()
			require.Equal(t, 0, res.ExitCode)
			return res.Err
		})
	}
	err := eg.Wait()
	require.NoError(t, err)

	// Make sure we only ever started a single task execution per command (tasks
	// should not be re-executed just because the app goes down).
	tasksStartedCount := testmetrics.CounterValue(t, metrics.RemoteExecutionTasksStartedCount) - initialTasksStartedCount
	require.Equal(t, float64(len(cmds)), tasksStartedCount, "no tasks should have been retried")
}

func randSleepMillis(min, max int) {
	r := rand.Int63n(int64(max-min)) + int64(min)
	time.Sleep(time.Duration(r))
}
