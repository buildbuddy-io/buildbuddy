package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
)

var bbRunfilePath string

func TestStartupDoesNotQueryTerminal(t *testing.T) {
	// No explicit deadline here: cold-starting the CLI binary can take
	// several seconds on loaded machines, so rely on the test timeout to
	// catch hangs. t.Context() kills the subprocess on cleanup.
	// version --cli exits without starting Bazel, but it still imports the CLI
	// command registry and the UI package, covering package-level initialization.
	cmd := exec.CommandContext(t.Context(), testfs.RunfilePath(t, bbRunfilePath), "version", "--cli")
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
