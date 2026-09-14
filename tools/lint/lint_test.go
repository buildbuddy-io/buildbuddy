package main

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	zerologlog "github.com/rs/zerolog/log"
)

// The logger is global, so tests capturing output must not run in parallel.
func captureLogs(t *testing.T) *lockingbuffer.LockingBuffer {
	t.Helper()
	buf := lockingbuffer.New()
	previousLogger := zerologlog.Logger
	zerologlog.Logger = zerolog.New(zerolog.ConsoleWriter{Out: buf, NoColor: true}).Level(zerolog.InfoLevel)
	t.Cleanup(func() { zerologlog.Logger = previousLogger })
	return buf
}

// Use tiny shell subprocesses rather than building the real lint tools or
// depending on their runfile paths.
func shellTool(name, script string) Tool {
	return Tool{
		Name: name,
		Run: func(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
			cmd := exec.CommandContext(ctx, "sh", "-c", script)
			cmd.Stdout = stdout
			cmd.Stderr = stderr
			return cmd.Run()
		},
	}
}

func TestRunToolSubprocessOutput(t *testing.T) {
	for _, tc := range []struct {
		name       string
		script     string
		wantOutput []string
		wantError  bool
	}{
		{
			name:       "stderr-only failure",
			script:     "echo 'dependency resolution failed' >&2; exit 7",
			wantOutput: []string{"dependency resolution failed", "exit status 7"},
			wantError:  true,
		},
		{
			name:       "failure with both streams",
			script:     "echo 'formatting diff'; echo 'formatter failed' >&2; exit 2",
			wantOutput: []string{"formatting diff", "formatter failed", "exit status 2"},
			wantError:  true,
		},
		{
			name:       "failure without output",
			script:     "exit 3",
			wantOutput: []string{"exit status 3"},
			wantError:  true,
		},
		{
			name:       "successful stderr warning",
			script:     "echo 'warning: deprecated dependency' >&2",
			wantOutput: []string{"warning: deprecated dependency"},
		},
		{
			name:       "successful stdout",
			script:     "echo 'formatted files'",
			wantOutput: []string{"formatted files"},
		},
		{
			name:       "successful output on both streams",
			script:     "echo 'tool progress'; echo 'warning: deprecated setting' >&2",
			wantOutput: []string{"tool progress", "warning: deprecated setting"},
		},
		{
			name:   "silent success",
			script: "exit 0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			err := runTool(context.Background(), shellTool("TestTool", tc.script), false, nil)
			output := logs.String()
			require.Contains(t, output, "[TestTool] starting")
			if tc.wantError {
				require.EqualError(t, err, "one or more lint checks failed - run ./buildfix.sh to attempt automatic fixes")
				require.Contains(t, output, "[TestTool] failed:")
				require.NotContains(t, output, "[TestTool] done")
			} else {
				require.NoError(t, err)
				require.Contains(t, output, "[TestTool] done")
				require.NotContains(t, output, "[TestTool] failed:")
				if len(tc.wantOutput) > 0 {
					require.Contains(t, output, "[TestTool] output:")
				} else {
					require.NotContains(t, output, "[TestTool] output:")
				}
			}
			for _, want := range tc.wantOutput {
				require.Contains(t, output, want)
			}
		})
	}
}

func TestRunToolsReportsAllFailuresWithoutCancellation(t *testing.T) {
	logs := captureLogs(t)
	var tools []Tool
	// Exceed the concurrency limit so some tools start after a failure has
	// already been returned. They must still run with an uncanceled context.
	for i := range 5 {
		tools = append(tools, shellTool(fmt.Sprintf("Fail%d", i), fmt.Sprintf("echo 'diagnostic %d' >&2; exit 1", i)))
	}
	tools = append(tools, shellTool("Success", "echo 'warning from successful tool' >&2"))

	err := runTools(context.Background(), tools, false, nil)

	require.EqualError(t, err, "one or more lint checks failed - run ./buildfix.sh to attempt automatic fixes")
	output := logs.String()
	for i := range 5 {
		require.Contains(t, output, fmt.Sprintf("[Fail%d] failed: exit status 1", i))
		require.Contains(t, output, fmt.Sprintf("diagnostic %d", i))
	}
	require.Contains(t, output, "warning from successful tool")
	require.Contains(t, output, "[Success] done")
	require.NotContains(t, output, "[Success] failed:")
}
