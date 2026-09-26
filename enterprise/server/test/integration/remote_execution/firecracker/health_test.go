package firecracker_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func testFirecrackerGuestHealthCheckFailure(t *testing.T, rbe *firecrackerEnv) {
	inputDir := testfs.MakeTempDir(t)
	command := firecrackerCommand(`
		set -e
		# As in the container-level FirecrackerHealthChecking test, let
		# vmexec collect some stats, then freeze it with SIGSTOP. Use an
		# explicit ps format so this also works with BusyBox's ps.
		sleep 0.5
		pids="$(ps -o pid,args | grep '\--vmexec' | grep -v grep | awk '{print $1}')"
		test -n "$pids"
		kill -STOP $pids
	`,
		&repb.Platform_Property{Name: "recycle-runner", Value: "false"},
	)
	result := rbe.Execute(command, &rbetest.ExecuteOpts{
		TestingT:         t,
		APIKey:           rbe.APIKey1,
		InputRootDir:     inputDir,
		ActionTimeout:    30 * time.Second,
		DoNotCacheAction: true,
	}).MustTerminateAbnormally()

	// A task timeout, scheduler error, or guest shell failure is not the
	// health-check failure under test.
	require.True(t, status.IsUnavailableError(result.Err), "expected Unavailable, got %s", result.Err)
	require.Contains(t, status.Message(result.Err), "VM health check failed")
	metadata := result.ActionResult.GetExecutionMetadata()
	require.NotEmpty(t, metadata.GetWorker())
	// Detailed VM metrics are not part of the streamed Execute response.
	// This black-box test checks the failure category and executor recovery.

	// The health failure should discard the broken VM, not poison the
	// executor or prevent a subsequent task from completing.
	recovery := rbe.Execute(firecrackerCommand(`printf 'healthy guest'`), &rbetest.ExecuteOpts{
		TestingT:         t,
		APIKey:           rbe.APIKey1,
		InputRootDir:     inputDir,
		ActionTimeout:    30 * time.Second,
		DoNotCacheAction: true,
	}).Wait()
	require.Equal(t, 0, recovery.ExitCode, "stderr: %s", recovery.Stderr)
	require.Equal(t, "healthy guest", recovery.Stdout)
	require.Equal(t, metadata.GetWorker(), recovery.ActionResult.GetExecutionMetadata().GetWorker())
}
