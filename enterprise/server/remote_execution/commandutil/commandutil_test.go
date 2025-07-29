package commandutil_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
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

func nopStatsListener(*repb.UsageStats) {}

func TestLimitStdErrOutWriter(t *testing.T) {
	tests := []struct {
		name    string
		limit   uint64
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
			buf := ioutil.NewLimitBuffer(tt.limit, "stdout/stderr output size")

			for i, write := range tt.writes {
				n, err := buf.Write([]byte(write))
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

func TestLimitReader_AllowsExactLimit(t *testing.T) {
	r := commandutil.LimitReader(bytes.NewBufferString("hello"), 5)
	buf := make([]byte, 3)
	// First read should succeed and consume 3 bytes.
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, "hel", string(buf[:n]))

	// Second read should consume the remaining 2 bytes without error.
	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "lo", string(buf[:n]))

	// Subsequent read should fail with ResourceExhausted since the limit is reached.
	n, err = r.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)
}

func TestLimitReader_ErrorsWhenOverLimit(t *testing.T) {
	r := commandutil.LimitReader(bytes.NewBufferString("hello world"), 5)
	buf := make([]byte, 8)

	// First read returns up to the limit without error.
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Next read detects additional data and surfaces a limit error.
	n, err = r.Read(buf)
	assert.Equal(t, 0, n)
	assert.Error(t, err)
	assert.True(t, status.IsResourceExhaustedError(err), "expected resource exhausted error, got: %v", err)
}

func TestCommandWithOutputLimit(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", 10)
	ctx := context.Background()

	// Command succeeds when within limit
	result := commandutil.Run(ctx, &repb.Command{Arguments: []string{"echo", "hello"}}, ".", nil, &interfaces.Stdio{})
	assert.Equal(t, 0, result.ExitCode)
	assert.NoError(t, result.Error)

	// Command fails when exceeding limit
	result = commandutil.Run(ctx, &repb.Command{Arguments: []string{"echo", "this is too long"}}, ".", nil, &interfaces.Stdio{})
	assert.Equal(t, commandutil.NoExitCode, result.ExitCode)
	assert.Error(t, result.Error)
	assert.True(t, status.IsResourceExhaustedError(result.Error), "expected resource exhausted error, got: %v", result.Error)
	assert.Contains(t, result.Error.Error(), "stdout/stderr output size limit exceeded")

	// Command succeeds when exceeding limit if DisableOutputLimits is set
	result = commandutil.Run(ctx, &repb.Command{Arguments: []string{"echo", "this is too long"}}, ".", nil, &interfaces.Stdio{DisableOutputLimits: true})
	assert.Equal(t, 0, result.ExitCode)
	assert.NoError(t, result.Error)
}
