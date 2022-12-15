package testbazel

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	// Version is the bazel version of the embedded test bazel binary.
	Version = "5.3.2"

	// BazelBinaryPath specifies the path to the bazel binary used for
	// invocations. Must match the path in the build rule.
	BazelBinaryPath = "server/util/bazel/bazel-" + Version

	// installBasePath is the path to the pre-extracted Bazel installation
	// relative to the runfiles dir.
	installBasePath = "server/util/bazel/bazel-" + Version + "_install"
)

var (
	initOnce sync.Once
)

// BinaryPath returns the path to the bazel binary.
func BinaryPath(t *testing.T) string {
	// Write an entrypoint script that runs bazel with --install_base.
	entrypoint := filepath.Join(os.Getenv("TEST_TMPDIR"), "bazel-"+Version+"_test_entrypoint.sh")
	initOnce.Do(func() {
		path, err := bazelgo.Runfile(BazelBinaryPath)
		require.NoError(t, err, "look up bazel binary path")
		installBase := initInstallBase(t)
		script := "#!/usr/bin/env sh\n"
		script += "exec " + path + " --install_base=" + installBase + ` "$@"`
		err = os.WriteFile(entrypoint, []byte(script), 0755)
		require.NoError(t, err)
	})
	return entrypoint
}

func initInstallBase(t *testing.T) string {
	path, err := bazelgo.Runfile(installBasePath)
	require.NoError(t, err)
	// Make a physical copy of the install dir (if it's symlinked via bazel
	// sandboxing) and set file mtimes to be in the future so that bazel sees it
	// as a pristine install.
	if target, _ := os.Readlink(filepath.Join(path, "build-label.txt")); target != "" {
		tmp := os.Getenv("TEST_TMPDIR")
		require.NotEmpty(t, tmp, "TEST_TMPDIR should not be empty")

		copyPath := filepath.Join(tmp, "bazel_install_base")
		cmd := exec.Command("cp", "-R", filepath.Dir(target), copyPath)
		b, err := cmd.CombinedOutput()
		require.NoError(t, err, "failed to init bazel install base: %s", string(b))

		path = copyPath
	}
	mtime := time.Now().Add(10 * 365 * 24 * time.Hour).Format("2006-01-02T00:00:00")
	cmd := exec.Command(
		"find", ".", "-type", "f",
		"-exec", "touch", "-d", mtime, "{}", "+")
	cmd.Dir = path
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "command output: %s", string(b))
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
