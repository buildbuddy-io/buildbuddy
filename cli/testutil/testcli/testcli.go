// package testcli contains test utilities for end-to-end CLI integration tests.
package testcli

import (
	"bytes"
	"flag"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

var (
	streamOutputs = flag.Bool("test_stream_cli_output", false, "Show live CLI output during test execution.")

	initEnvOnce sync.Once
)

// Command returns an *exec.Cmd for the CLI binary.
func Command(t *testing.T, workspacePath string, args ...string) *exec.Cmd {
	initEnvOnce.Do(func() {
		// Need a HOME dir for .cache and .config dirs.
		home := testfs.MakeTempDir(t)
		err := os.Setenv("HOME", home)
		require.NoError(t, err)
	})

	path := testfs.RunfilePath(t, "cli/cmd/bb/bb_/bb")
	cmd := exec.Command(path, args...)
	cmd.Dir = workspacePath
	if *streamOutputs {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}
	return cmd
}

// CombinedOutput is like cmd.CombinedOutput() except that it allows streaming
// CLI outputs for debugging purposes.
func CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var w io.Writer = buf
	if *streamOutputs {
		w = io.MultiWriter(w, os.Stderr)
	}
	cmd.Stdout = w
	cmd.Stderr = w
	err := cmd.Run()
	return buf.Bytes(), err
}

// NewWorkspace creates a new bazel workspace with .bazelversion configured
// to use the pre-downloaded bazel binary from test runfiles.
func NewWorkspace(t *testing.T) string {
	ws := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE":     "",
		".bazelversion": testbazel.BinaryPath(t),
	})
	return ws
}
