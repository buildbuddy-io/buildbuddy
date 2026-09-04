//go:build windows

package commandutil_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
	require.GreaterOrEqual(t, res.UsageStats.GetCpuNanos(), int64(1e9), "expected CPU usage from child processes")
	require.LessOrEqual(t, res.UsageStats.GetCpuNanos(), int64(10e9), "unexpectedly high CPU usage")
	require.GreaterOrEqual(t, res.UsageStats.GetPeakMemoryBytes(), int64(750e6), "expected peak memory from child processes")
	require.LessOrEqual(t, res.UsageStats.GetPeakMemoryBytes(), int64(2e9), "unexpectedly high peak memory")
}

func TestRun_Win_NormalExit_KillsDescendants(t *testing.T) {
	workDir := testfs.MakeTempDir(t)
	pidPath := filepath.Join(workDir, "child.pid")
	script := fmt.Sprintf(`
		$child = Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 300' -PassThru
		Set-Content -LiteralPath '%s' -Value $child.Id
	`, pidPath)
	cmd := &repb.Command{Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", script}}

	res := commandutil.Run(context.Background(), cmd, workDir, nopStatsListener, &interfaces.Stdio{})

	require.NoError(t, res.Error)
	pidBytes, err := os.ReadFile(pidPath)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	require.NoError(t, err)
	defer exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T", "/F").Run()
	require.Eventually(t, func() bool {
		check := fmt.Sprintf("if (Get-Process -Id %d -ErrorAction SilentlyContinue) { exit 1 }", pid)
		return exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command", check).Run() == nil
	}, 5*time.Second, 50*time.Millisecond, "descendant process %d was left running", pid)
}
