package testbazel

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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

	// installBasePath is the path to the pre-extracted Bazel installation
	// relative to the runfiles dir.
	installBasePath = "server/util/bazel/bazel-" + Version + "_install"
)

var (
	initInstallBaseOnce sync.Once
	initWrapperOnce     sync.Once
)

// BinaryPath returns the path to the bazel binary.
func BinaryPath(t *testing.T) string {
	path, err := bazelgo.Runfile(BazelBinaryPath)
	require.NoError(t, err, "look up bazel binary path")

	wrapperPath := filepath.Join("TEST_TEMPDIR", "bazel-"+Version+"_testwrapper")
	initWrapperOnce.Do(func() {
		installBase := InstallBasePath(t)
		wrapperContents := "#!/usr/bin/env sh\n"
		wrapperContents += "exec " + path + " --install_base=" + installBase + ` "$@"`
		err := os.WriteFile(wrapperPath, []byte(wrapperContents), 0755)
		require.NoError(t, err)
	})

	return wrapperPath
}

// InstallBasePath returns the path to where the bazel installation has been
// pre-extracted.
func InstallBasePath(t *testing.T) string {
	path, err := bazelgo.Runfile(installBasePath)
	require.NoError(t, err)

	// When running tests in a local sandbox, the extracted bazel installation
	// in the runfiles tree will consist of symlinks to a read-only view of
	// the extracted installation in execroot:
	// https://github.com/bazelbuild/bazel/issues/7937
	//
	// Unfortunately, Bazel ignores any symlinks when scanning the installation:
	// https://github.com/bazelbuild/bazel/blob/4864f1e90519e530b333177a8339ed110659f08b/src/main/java/com/google/devtools/build/lib/exec/BinTools.java#L144
	//
	// Worse yet, Bazel tries to modify the installation dir after reading from
	// it, so we can't directly use the symlink target as the install base:
	// https://github.com/bazelbuild/bazel/blob/4864f1e90519e530b333177a8339ed110659f08b/src/main/cpp/blaze.cc#L1210
	//
	// So to work around this, we create a recursive copy of the installation.
	// This is still about 20X faster than allowing Bazel to extract the
	// installation itself.
	srcInstallDir := ""
	if target, _ := os.Readlink(filepath.Join(path, "build-label.txt")); target != "" {
		srcInstallDir = filepath.Dir(target)
		tmp := os.Getenv("TEST_TMPDIR")
		require.NotEmpty(t, tmp, "TEST_TMPDIR should not be empty")
		path = filepath.Join(os.Getenv("TEST_TMPDIR"), "bazel_install")
	}

	initInstallBaseOnce.Do(func() {
		if srcInstallDir != "" {
			cmd := exec.Command("cp", "-R", srcInstallDir, path)
			b, err := cmd.CombinedOutput()
			require.NoError(t, err, "failed to init bazel install base. command output:\n%s", string(b))
		}
		// Bazel has some basic tamper detection built in that checks whether
		// its installed files all have an mtime in the "distant future". Since
		// mtime is not preserved on RBE, we set it explicitly here.
		cmd := exec.Command(
			"find", ".", "-type", "f",
			"-exec", "touch", "-d", "10 years", "{}", "+")
		cmd.Dir = path
		b, err := cmd.CombinedOutput()
		require.NoError(t, err, "command output: %s", string(b))
	})

	return path
}

// Invoke the bazel CLI from within the given workspace dir.
func Invoke(ctx context.Context, t *testing.T, workspaceDir string, subCommand string, args ...string) *bazel.InvocationResult {
	installBase := InstallBasePath(t)
	bazelBinaryPath := BinaryPath(t)
	args = append([]string{"--install_base=" + installBase, subCommand}, args...)
	// Disable sandboxed execution since we're already in a test sandbox (when
	// executing locally) and bazel doesn't support "nested" sandboxing.
	if subCommand == "build" || subCommand == "test" || subCommand == "run" {
		args = append(args, "--spawn_strategy=remote,local")
	}
	return bazel.Invoke(ctx, bazelBinaryPath, workspaceDir, args...)
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
