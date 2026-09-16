//go:build windows

package commandutil

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMonitorUsageIncludesExitedChildCPU(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	workDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workDir, "child.ps1"), []byte(`
$timer = [System.Diagnostics.Stopwatch]::StartNew()
while ($timer.ElapsedMilliseconds -lt 250) {}
`), 0600))
	cmd := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command", `
		$ErrorActionPreference = 'Stop'
		$child = Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-File','child.ps1' -PassThru
		$child.WaitForExit()
		if ($child.ExitCode -ne 0) { exit 1 }
		Write-Output $child.TotalProcessorTime.Ticks
	`)
	cmd.Dir = workDir
	cmd.SysProcAttr = getDefaultSysProcAttr()
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	p, err := startNewProcess(ctx, cmd)
	require.NoError(t, err)
	defer p.cleanup()
	_, err = p.wait()
	require.NoError(t, err, "stderr: %s", stderr.String())
	require.NoError(t, ctx.Err())
	childCPUTicks, err := strconv.ParseInt(strings.TrimSpace(stdout.String()), 10, 64)
	require.NoError(t, err)
	require.Positive(t, childCPUTicks)

	// Neither process exists when monitoring starts. A PID poller cannot
	// recover their CPU usage, but Job Object accounting must retain at least
	// the CPU independently measured by the parent for its exited child.
	stats := p.monitorUsage(nil)
	require.GreaterOrEqual(t, stats.GetCpuNanos(), childCPUTicks*100)
}
