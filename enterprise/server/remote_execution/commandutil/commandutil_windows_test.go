//go:build windows

package commandutil_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
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

func TestRun_Win_CompletedJobCurrentlyReportsStaleMemory(t *testing.T) {
	cmd := &repb.Command{Arguments: []string{"powershell", "-NoProfile", "-NonInteractive", "-Command", `
		Start-Process powershell -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 300' | Out-Null
		Start-Sleep -Seconds 1
	`}}

	res := commandutil.Run(context.Background(), cmd, ".", nopStatsListener, &interfaces.Stdio{})

	require.NoError(t, res.Error)
	// Document the current deficiency: the descendant is killed during final
	// cleanup, but the returned current memory remains at its last live sample.
	require.Positive(t, res.UsageStats.GetMemoryBytes())
}

func TestComplexProcessTree(t *testing.T) {
	// Setup
	workDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"cpu1.py": useCPUPythonScript(3 * time.Second),
		"cpu2.py": useCPUPythonScript(1 * time.Second),
		"mem1.py": useMemPythonScript(500e6, 3*time.Second),
		"mem2.py": useMemPythonScript(250e6, 2*time.Second),
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"powershell", "-c", `
		Start-Job -Name cpu1 -ScriptBlock { python cpu1.py }
		Start-Job -Name cpu2 -ScriptBlock { python cpu2.py }
		Start-Job -Name mem1 -ScriptBlock { python mem1.py }
		Start-Job -Name mem2 -ScriptBlock { python mem2.py }
		Get-Job | Wait-Job
		`},
	}
	res := commandutil.Run(context.Background(), cmd, workDir, nopStatsListener, &interfaces.Stdio{})

	// Assert
	assert.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
}
