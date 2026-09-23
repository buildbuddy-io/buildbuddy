//go:build windows

package commandutil_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestRun_Win_NormalExit_NoError(t *testing.T) {
	for _, tc := range []int{0, 1, 137} {
		t.Run(fmt.Sprintf("exit%d", tc), func(t *testing.T) {
			cmd := &repb.Command{Arguments: []string{"powershell", "-c", fmt.Sprintf("Exit %d", tc)}}
			res := commandutil.Run(context.Background(), cmd, ".", nopStatsListener, &interfaces.Stdio{})

			assert.NoError(t, res.Error)
			assert.Equal(t, tc, res.ExitCode)
		})
	}
}

func TestRun_Win_NegativeExitIsNotReportedAsKilled(t *testing.T) {
	cmd := &repb.Command{Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", "Exit -1"}}

	res := commandutil.Run(context.Background(), cmd, ".", nopStatsListener, &interfaces.Stdio{})

	require.NoError(t, res.Error)
}

func TestRun_Win_CompletedJobReportsNoCurrentMemory(t *testing.T) {
	cmd := &repb.Command{Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", `
		Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 300' | Out-Null
		Start-Sleep -Seconds 1
	`}}

	res := commandutil.Run(context.Background(), cmd, ".", nopStatsListener, &interfaces.Stdio{})

	require.NoError(t, res.Error)
	require.Zero(t, res.UsageStats.GetMemoryBytes())
}

func TestRun_Win_TimeoutReturnsDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cmd := &repb.Command{Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", "Start-Sleep -Seconds 300"}}

	res := commandutil.Run(ctx, cmd, ".", nopStatsListener, &interfaces.Stdio{})

	require.True(t, status.IsDeadlineExceededError(res.Error), "expected deadline exceeded, got %v", res.Error)
	require.Equal(t, commandutil.KilledExitCode, res.ExitCode)
}

func useCPUPowerShellScript(dur time.Duration) string {
	return fmt.Sprintf(`
$timer = [System.Diagnostics.Stopwatch]::StartNew()
while ($timer.ElapsedMilliseconds -lt %d) {}
`, dur.Milliseconds())
}

func useMemoryPowerShellScript(memoryBytes int64, dur time.Duration) string {
	return fmt.Sprintf(`
$memory = New-Object byte[] %d
for ($i = 0; $i -lt $memory.Length; $i += 4096) { $memory[$i] = 1 }
Start-Sleep -Milliseconds %d
if ($memory.Length -ne %d) { exit 1 }
`, memoryBytes, dur.Milliseconds(), memoryBytes)
}

func TestComplexProcessTree(t *testing.T) {
	// Setup
	workDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"cpu1.ps1": useCPUPowerShellScript(3 * time.Second),
		"cpu2.ps1": useCPUPowerShellScript(1 * time.Second),
		"mem1.ps1": useMemoryPowerShellScript(500e6, 3*time.Second),
		"mem2.ps1": useMemoryPowerShellScript(250e6, 2*time.Second),
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", `
		$processes = @(
			Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-File','cpu1.ps1' -WorkingDirectory '.' -PassThru
			Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-File','cpu2.ps1' -WorkingDirectory '.' -PassThru
			Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-File','mem1.ps1' -WorkingDirectory '.' -PassThru
			Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-File','mem2.ps1' -WorkingDirectory '.' -PassThru
		)
		$processes | Wait-Process
		`},
	}
	res := commandutil.Run(context.Background(), cmd, workDir, nopStatsListener, &interfaces.Stdio{})

	// Assert
	require.NoError(t, res.Error)
	require.Equal(t, 0, res.ExitCode)
	// CPU usage includes PowerShell startup and memory initialization, which vary by runner.
	require.GreaterOrEqual(t, res.UsageStats.GetCpuNanos(), int64(1e9), "expected CPU usage from child processes")
	require.GreaterOrEqual(t, res.UsageStats.GetPeakMemoryBytes(), int64(750e6), "expected peak memory from child processes")
	require.LessOrEqual(t, res.UsageStats.GetPeakMemoryBytes(), int64(2e9), "unexpectedly high peak memory")
}

func TestRun_Win_NormalExit_KillsDescendants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	workDir := testfs.MakeTempDir(t)
	pidPath := filepath.Join(workDir, "child.pid")
	releasePath := filepath.Join(workDir, "release-parent")
	var childHandle windows.Handle
	var res *interfaces.CommandResult
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		if childHandle != 0 {
			_ = windows.TerminateProcess(childHandle, 1)
			_ = windows.CloseHandle(childHandle)
		}
		<-done
	})
	script := fmt.Sprintf(`
		$ErrorActionPreference = 'Stop'
		$child = Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 300' -NoNewWindow -PassThru
		Set-Content -LiteralPath '%s' -Value $child.Id
		while (!(Test-Path -LiteralPath '%s')) { Start-Sleep -Milliseconds 10 }
	`, pidPath, releasePath)
	cmd := &repb.Command{Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", script}}
	go func() {
		defer close(done)
		res = commandutil.Run(ctx, cmd, workDir, nopStatsListener, &interfaces.Stdio{})
	}()

	// Retain the child's handle before allowing the parent to exit, so the
	// final check cannot accidentally open a recycled PID on a busy runner.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for childHandle == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the child PID")
		case <-done:
			t.Fatalf("parent exited before publishing child PID: %+v", res)
		case <-ticker.C:
			pidBytes, err := os.ReadFile(pidPath)
			if err != nil {
				continue
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if err != nil {
				continue
			}
			childHandle, err = windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
			require.NoError(t, err)
		}
	}
	require.NoError(t, os.WriteFile(releasePath, nil, 0600))
	<-done
	require.NoError(t, res.Error)
	require.Zero(t, res.ExitCode)
	require.NoError(t, ctx.Err(), "inherited output pipes kept the command running until cancellation")
	var exitCode uint32
	require.NoError(t, windows.GetExitCodeProcess(childHandle, &exitCode))
	require.Equal(t, ^uint32(0), exitCode, "descendant did not receive the job termination exit code")
	// The job can become inactive before Windows signals individual process
	// handles. Confirm teardown completes using the retained child handle.
	waitResult, err := windows.WaitForSingleObject(childHandle, 5000)
	require.NoError(t, err)
	require.EqualValues(t, windows.WAIT_OBJECT_0, waitResult, "descendant teardown did not complete")
}
