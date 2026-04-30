package commandutil_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

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
	return commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &interfaces.Stdio{})
}

func echoCommand(msg string) *repb.Command {
	if runtime.GOOS == "windows" {
		return &repb.Command{Arguments: []string{"powershell", "-Command", fmt.Sprintf("Write-Output '%s'", msg)}}
	}
	return &repb.Command{Arguments: []string{"sh", "-c", fmt.Sprintf("printf '%s\\n'", msg)}}
}

func printAndSleepCommand(msg string, sleepSeconds int) *repb.Command {
	if runtime.GOOS == "windows" {
		return &repb.Command{Arguments: []string{"powershell", "-Command", fmt.Sprintf("Write-Host -NoNewline '%s'; Start-Sleep -Seconds %d", msg, sleepSeconds)}}
	}
	return &repb.Command{Arguments: []string{"sh", "-c", fmt.Sprintf("printf '%s' && sleep %d", msg, sleepSeconds)}}
}

func nopStatsListener(*repb.UsageStats) {}

func TestLimitStdOutErrWriter(t *testing.T) {
	tests := []struct {
		name    string
		limit   int64
		writes  []string
		wantN   []int
		wantErr []bool
		wantOut string
	}{
		{
			name:    "no limit",
			limit:   0,
			writes:  []string{"hello world"},
			wantN:   []int{11},
			wantErr: []bool{false},
			wantOut: "hello world",
		},
		{
			name:    "within limit",
			limit:   20,
			writes:  []string{"hello world"},
			wantN:   []int{11},
			wantErr: []bool{false},
			wantOut: "hello world",
		},
		{
			name:    "exceeds limit",
			limit:   10,
			writes:  []string{"hello world this is too long"},
			wantN:   []int{10},
			wantErr: []bool{true},
			wantOut: "hello worl",
		},
		{
			name:    "multiple writes",
			limit:   15,
			writes:  []string{"hello", " world", " extra"},
			wantN:   []int{5, 6, 4},
			wantErr: []bool{false, false, true},
			wantOut: "hello world ext",
		},
		{
			name:    "zero-length write",
			limit:   5,
			writes:  []string{"hello", ""},
			wantN:   []int{5, 0},
			wantErr: []bool{false, false},
			wantOut: "hello",
		},
		{
			name:    "at limit",
			limit:   5,
			writes:  []string{"hello", " world"},
			wantN:   []int{5, 0},
			wantErr: []bool{false, true},
			wantOut: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags.Set(t, "executor.stdouterr_max_size_bytes", tt.limit)
			var buf bytes.Buffer
			w := commandutil.LimitStdOutErrWriter(&buf)

			for i, write := range tt.writes {
				n, err := w.Write([]byte(write))
				assert.Equal(t, tt.wantN[i], n)
				if tt.wantErr[i] {
					require.Error(t, err)
					assert.True(t, status.IsResourceExhaustedError(err), "expected resource exhausted error, got: %v", err)
					assert.Contains(t, err.Error(), "stdout/stderr output size limit exceeded")
				} else {
					require.NoError(t, err)
				}
			}
			assert.Equal(t, tt.wantOut, buf.String())
		})
	}
}

func TestCommandWithOutputLimit(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", 10)
	ctx := context.Background()

	// Command succeeds when within limit
	result := commandutil.Run(ctx, echoCommand("hello"), ".", nil, &interfaces.Stdio{})
	assert.Equal(t, 0, result.ExitCode)
	assert.NoError(t, result.Error)

	// Command fails when exceeding limit
	result = commandutil.Run(ctx, echoCommand("this is too long"), ".", nil, &interfaces.Stdio{})
	assert.Equal(t, commandutil.NoExitCode, result.ExitCode)
	assert.Error(t, result.Error)
	assert.True(t, status.IsResourceExhaustedError(result.Error), "expected resource exhausted error, got: %v", result.Error)
	assert.Contains(t, result.Error.Error(), "stdout/stderr output size limit exceeded")

	// Command succeeds when exceeding limit if DisableOutputLimits is set
	result = commandutil.Run(ctx, echoCommand("this is too long"), ".", nil, &interfaces.Stdio{DisableOutputLimits: true})
	assert.Equal(t, 0, result.ExitCode)
	assert.NoError(t, result.Error)
}

func TestCommandWithOutputLimit_LongRunningCommand(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Reproduce the original regression: overflow stdout immediately, then
	// keep the process alive long enough that the runner must interrupt it.
	result := commandutil.Run(ctx, printAndSleepCommand("12345678901234567890", 30), ".", nil, &interfaces.Stdio{})

	assert.Equal(t, commandutil.NoExitCode, result.ExitCode)
	require.Error(t, result.Error)
	assert.True(t, status.IsResourceExhaustedError(result.Error), "expected resource exhausted error, got: %v", result.Error)
	assert.Nil(t, ctx.Err(), "expected command to fail before the context deadline, got: %v", ctx.Err())
}
