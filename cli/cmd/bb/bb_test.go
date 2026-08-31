package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
)

var bbRunfilePath string

func TestStartupDoesNotQueryTerminal(t *testing.T) {
	// The regression this test guards against (terminal queries during
	// startup) is detected by inspecting the output bytes below, not by
	// timing: a binary that probes the terminal emits the query escape
	// sequences whether or not it also stalls waiting for a reply. Avoid a
	// short wall-clock deadline here; on heavily loaded CI machines just
	// exec-ing the CLI binary can take several seconds, which made this
	// test flaky. Instead, allow the subprocess to run until shortly
	// before the test itself times out, so a true hang still fails with
	// useful output instead of tripping the bazel test timeout.
	ctx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-10*time.Second))
		defer cancel()
	}

	// version --cli exits without starting Bazel, but it still imports the CLI
	// command registry and the UI package, covering package-level initialization.
	cmd := exec.CommandContext(ctx, testfs.RunfilePath(t, bbRunfilePath), "version", "--cli")
	cmd.Env = append(envWithout("CI", "TERM"), "TERM=xterm-256color")
	f, err := pty.Start(cmd)
	require.NoError(t, err)

	var output bytes.Buffer
	readDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&output, f)
		close(readDone)
	}()

	err = cmd.Wait()
	_ = f.Close()
	<-readDone
	require.NoError(t, ctx.Err(), "bb startup timed out; output: %q", output.String())
	require.NoError(t, err, "output: %q", output.String())
	require.NotContains(t, output.String(), "\x1b]11;?", "queried terminal background color")
	require.NotContains(t, output.String(), "\x1b[6n", "queried cursor position")
}

func envWithout(keys ...string) []string {
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, ok := blocked[key]; !ok {
			env = append(env, entry)
		}
	}
	return env
}
