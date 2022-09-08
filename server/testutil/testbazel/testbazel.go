package testbazel

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	// BazelBinaryPath specifies the path to the bazel binary used for invocations.
	// Must match the path in the build rule.
	BazelBinaryPath = "server/util/bazel/bazel-5.0.0rc3"
)

// BinaryPath returns the path to the bazel binary.
func BinaryPath(t *testing.T) string {
	path, err := bazelgo.Runfile(BazelBinaryPath)
	require.NoError(t, err, "look up bazel binary path")
	return path
}

// Invoke the bazel CLI from within the given workspace dir.
func Invoke(ctx context.Context, t *testing.T, workspaceDir string, subCommand string, args ...string) *bazel.InvocationResult {
	bazelBinaryPath := BinaryPath(t)
	return bazel.Invoke(ctx, bazelBinaryPath, workspaceDir, subCommand, args...)
}

// Clean runs `bazel clean` within the given workspace.
func Clean(ctx context.Context, t *testing.T, workspaceDir string) *bazel.InvocationResult {
	return Invoke(ctx, t, workspaceDir, "clean")
}

// shutdown runs `bazel shutdown` within the given workspace.
func shutdown(ctx context.Context, t *testing.T, workspaceDir string) {
	start := time.Now()
	defer func() {
		log.Infof("bazel shutdown completed in %s", time.Since(start))
	}()
	if shutdownResult := Invoke(ctx, t, workspaceDir, "shutdown"); shutdownResult.Error != nil {
		t.Fatal(shutdownResult.Error)
	}
}

func MakeTempWorkspace(t *testing.T, contents map[string]string) string {
	workspaceDir := testfs.MakeTempDir(t)
	t.Cleanup(func() {
		// Run Bazel shutdown so that the server process associated with the temp
		// workspace doesn't stick around. The bazel server eventually shuts down
		// automatically, but we want to shut the server down ASAP so that high test
		// volume doesn't cause tons of idle server processes to be running.
		shutdown(context.Background(), t, workspaceDir)
	})
	for path, fileContents := range contents {
		fullPath := filepath.Join(workspaceDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0777)
		require.NoError(t, err, "failed to create bazel workspace contents")
		err = os.WriteFile(fullPath, []byte(fileContents), 0777)
		require.NoError(t, err, "failed to create bazel workspace contents")
	}
	return workspaceDir
}
