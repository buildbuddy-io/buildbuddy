package testbazel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	// Version is the bazel version of the embedded test bazel binary.
	Version = "5.3.0"

	// BazelBinaryPath specifies the path to the bazel binary used for
	// invocations. Must match the path in the build rule.
	BazelBinaryPath = "server/util/bazel/bazel-" + Version
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

func MakeTempWorkspace(t *testing.T, contents map[string]string) string {
	workspaceDir := testfs.MakeTempDir(t)
	for path, fileContents := range contents {
		fullPath := filepath.Join(workspaceDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0777)
		require.NoError(t, err, "failed to create bazel workspace contents")
		err = os.WriteFile(fullPath, []byte(fileContents), 0777)
		require.NoError(t, err, "failed to create bazel workspace contents")
	}
	return workspaceDir
}
