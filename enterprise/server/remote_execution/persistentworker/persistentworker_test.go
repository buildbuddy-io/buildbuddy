package persistentworker_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime/pprof"
	"testing"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/persistentworker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testpprof"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type stderrContainer struct {
	container.CommandContainer
	stderr []byte
}

func (c *stderrContainer) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	// Wait for a complete work request before exiting so that the stderr is
	// associated with the current request rather than the worker startup.
	r := bufio.NewReader(stdio.Stdin)
	size, err := binary.ReadUvarint(r)
	if err == nil {
		_, err = io.CopyN(io.Discard, r, int64(size))
	}
	if err != nil {
		return &interfaces.CommandResult{Error: err, ExitCode: -2}
	}
	_, _ = stdio.Stderr.Write(c.stderr)
	return &interfaces.CommandResult{ExitCode: 1}
}

// blockingContainer blocks in Exec until the worker context is canceled.
type blockingContainer struct {
	container.CommandContainer
}

func (c *blockingContainer) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	<-ctx.Done()
	return &interfaces.CommandResult{ExitCode: 1}
}

func TestStart_PprofLabels(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			flags.Set(t, "monitoring.enable_pprof_labels", enabled)
			before := testpprof.TakeSnapshot(t)
			cmd := &repb.Command{Arguments: []string{"worker"}}
			var w *persistentworker.Worker
			var err error
			start := func(ctx context.Context) {
				w, err = persistentworker.Start(ctx, &workspace.Workspace{}, &blockingContainer{}, "proto", cmd)
			}
			// The worker outlives the task that started it, so it must not
			// keep the task's labels.
			if enabled {
				pprof.Do(context.Background(), pprof.Labels("task_id", "X"), start)
			} else {
				start(context.Background())
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, w.Stop()) })

			want := map[string]string{}
			if enabled {
				want["subsystem"] = "executor.persistent_worker"
			}
			// The goroutine blocks in Exec until the worker is stopped at the
			// end of the test, so it is guaranteed to show up.
			before.RequireGoroutineLabels(t, "persistentworker.Start.func", want)
		})
	}
}

func TestWorkerExec_Stderr(t *testing.T) {
	tests := []struct {
		name       string
		stderr     []byte
		wantStderr string
	}{
		{
			name:       "empty",
			wantStderr: "<empty>",
		},
		{
			name:       "valid UTF-8",
			stderr:     []byte("hello, 世界"),
			wantStderr: "hello, 世界",
		},
		{
			name:       "invalid UTF-8",
			stderr:     []byte{'a', 0xff, 'b'},
			wantStderr: "a�b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			cmd := &repb.Command{Arguments: []string{"worker"}}
			ctr := &stderrContainer{stderr: test.stderr}
			w, err := persistentworker.Start(ctx, &workspace.Workspace{}, ctr, "proto", cmd)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, w.Stop())
			})

			result := w.Exec(ctx, cmd)
			require.Error(t, result.Error)
			require.True(t, utf8.ValidString(result.Error.Error()), "error contains invalid UTF-8: %q", result.Error)
			require.Contains(t, result.Error.Error(), "persistent worker stderr:\n"+test.wantStderr)
		})
	}
}
