// package testcli contains test utilities for end-to-end CLI integration tests.
package testcli

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/stretchr/testify/require"
)

var (
	streamOutputs = flag.Bool("test_stream_cli_output", false, "Show live CLI output during test execution.")
	verbose       = flag.Bool("test_cli_verbose", false, "Whether to add --verbose=1 to the CLI.")

	initEnvOnce sync.Once
)

// Command returns an *exec.Cmd for the CLI binary.
func Command(t *testing.T, workspacePath string, args ...string) *exec.Cmd {
	initEnvOnce.Do(func() {
		// Need a HOME dir for .cache and .config dirs.
		home := testfs.MakeTempDir(t)
		err := os.Setenv("HOME", home)
		require.NoError(t, err)
		// Always run the sidecar in debug mode for tests.
		err = os.Setenv("BB_SIDECAR_ARGS", "--app.log_level=debug")
		require.NoError(t, err)
	})

	path := testfs.RunfilePath(t, "cli/cmd/bb/bb_/bb")
	if *verbose {
		args = append(args, "--verbose=1")
	}
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
	// Make it a git workspace to test git metadata.
	testgit.Init(t, ws)
	return ws
}

// DumpSidecarLog dumps the sidecar log to the terminal. Useful for debugging.
func DumpSidecarLog(t *testing.T) {
	cacheDir, err := storage.CacheDir()
	require.NoError(t, err)
	entries, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(cacheDir, entry.Name()))
		require.NoError(t, err)
		fmt.Printf("--- Sidecar log ---\n%s", string(b))
		return
	}
	require.FailNowf(t, "could not find sidecar logs in cache dir", "")
}
