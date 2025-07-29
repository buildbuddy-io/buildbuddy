package commandutil_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
			wantN:   []int{0},
			wantErr: []bool{true},
			wantOut: "",
		},
		{
			name:    "multiple writes",
			limit:   15,
			writes:  []string{"hello", " world", " extra"},
			wantN:   []int{5, 6, 0},
			wantErr: []bool{false, false, true},
			wantOut: "hello world",
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
			flags.Set(t, "executor.stderrout_max_size_bytes", tt.limit)
			var buf bytes.Buffer
			w := commandutil.LimitStdErrOutWriter(&buf)

			for i, write := range tt.writes {
				n, err := w.Write([]byte(write))
				assert.Equal(t, tt.wantN[i], n)
				if tt.wantErr[i] {
					require.Error(t, err)
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
	flags.Set(t, "executor.stderrout_max_size_bytes", 10)
	ctx := context.Background()

	// Command succeeds when within limit
	result := commandutil.Run(ctx, &repb.Command{Arguments: []string{"echo", "hello"}}, ".", nil, &interfaces.Stdio{})
	assert.Equal(t, 0, result.ExitCode)
	assert.NoError(t, result.Error)

	// Command fails when exceeding limit
	result = commandutil.Run(ctx, &repb.Command{Arguments: []string{"echo", "this is too long"}}, ".", nil, &interfaces.Stdio{})
	assert.Equal(t, commandutil.NoExitCode, result.ExitCode)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "stdout/stderr output size limit exceeded")
}
