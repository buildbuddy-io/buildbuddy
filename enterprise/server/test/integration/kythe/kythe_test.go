package kythe_test

import (
	"os/exec"
	"runtime"
	"syscall"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// set by x_defs in BUILD file
	ciRunnerRunfilePath string
)

type result struct {
	Output   string
	ExitCode int
}

func invokeRunner(t *testing.T, args []string, env []string, workDir string) *result {
	binPath, err := runfiles.Rlocation(ciRunnerRunfilePath)
	if err != nil {
		t.Fatal(err)
	}
	args = append([]string{
		"--bazel_command=" + testbazel.BinaryPath(t),
		"--bazel_startup_flags=--max_idle_secs=5 --noblock_for_lock",
	}, args...)

	cmd := exec.Command(binPath, args...)
	cmd.Dir = workDir
	cmd.Env = env
	outputBytes, err := cmd.CombinedOutput()
	exitCode := -1
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			ws := exitErr.Sys().(syscall.WaitStatus)
			if ws.Exited() {
				exitCode = ws.ExitStatus()
			}
		} else {
			t.Fatal(err)
		}
	} else {
		exitCode = 0
	}
	return &result{
		Output:   string(outputBytes),
		ExitCode: exitCode,
	}
}

func makeGitRepo(t *testing.T, contents map[string]string) (path, commitSHA string) {
	path = testbazel.MakeTempModule(t, contents)
	commitSHA = testgit.Init(t, path)
	return path, commitSHA
}

// TestKytheIndexingScript tests that the Kythe indexing script works correctly,
// including finding kzip files using $(bazel info output_path) for Bazel 8+ compatibility.
func TestKytheIndexingScript(t *testing.T) {
	// Skip on non-Linux platforms since the Kythe tools archive contains Linux binaries.
	if runtime.GOOS != "linux" {
		t.Skipf("Skipping Kythe integration test on %s (requires Linux)", runtime.GOOS)
	}

	// Use a simple C++ project to avoid rules_go/bzlmod complexity.
	// The key thing we're testing is that find -L "$BAZEL_OUT" works correctly.
	workspaceContents := map[string]string{
		"WORKSPACE": `workspace(name = "kythe_test")`,
		"BUILD.bazel": `
cc_library(
    name = "hello",
    srcs = ["hello.cc"],
    hdrs = ["hello.h"],
)
`,
		"hello.h": `#ifndef HELLO_H_
#define HELLO_H_
const char* Hello();
#endif
`,
		"hello.cc": `#include "hello.h"
const char* Hello() { return "world"; }
`,
		"buildbuddy.yaml": `
actions:
  - name: "Kythe Indexing Test"
    steps:
      - run: |
          set -euo pipefail

          # Download Kythe tools
          export KYTHE_DIR="$BUILDBUDDY_CI_RUNNER_ROOT_DIR/kythe-test"
          mkdir -p "$KYTHE_DIR"
          echo "Downloading Kythe tools..."
          curl -sL "https://storage.googleapis.com/buildbuddy-tools/archives/kythe-v0.0.76-buildbuddy.tar.gz" | tar -xz -C "$KYTHE_DIR" --strip-components 1
          echo "Kythe tools downloaded to $KYTHE_DIR"

          # Determine Bazel version and set appropriate repository flag
          BZL_MAJOR_VERSION=$(bazel info release | cut -d' ' -f2 | xargs | cut -d'.' -f1)
          if [ "$BZL_MAJOR_VERSION" -ge 8 ]; then
            KYTHE_REPO_ARG="--inject_repository=kythe_release=$KYTHE_DIR"
          else
            KYTHE_REPO_ARG="--override_repository=kythe_release=$KYTHE_DIR"
          fi
          echo "Using Bazel $BZL_MAJOR_VERSION with $KYTHE_REPO_ARG"

          # Build with Kythe extractors to produce kzip files
          echo "Building with Kythe extractors..."
          bazel --bazelrc="$KYTHE_DIR"/extractors.bazelrc build $KYTHE_REPO_ARG //...
          echo "Build complete"

          # Use bazel info output_path (the fix for Bazel 8+ compatibility)
          BAZEL_OUT=$(bazel info output_path)
          echo "Using output path: $BAZEL_OUT"

          # Find kzip files (C++ uses extra_actions directory)
          echo "Searching for kzip files in $BAZEL_OUT..."
          find -L "$BAZEL_OUT" -name "*.kzip" 2>/dev/null || true
          kzip_count=$(find -L "$BAZEL_OUT" -name "*.kzip" 2>/dev/null | wc -l | tr -d ' ')
          echo "Found $kzip_count kzip files"

          if [ "$kzip_count" -eq 0 ]; then
            echo "ERROR: No kzip files found"
            echo "Listing output directory structure:"
            find "$BAZEL_OUT" -type d 2>/dev/null | head -20 || true
            exit 1
          fi

          echo "SUCCESS: Found kzip files using bazel info output_path"

          # Fix permissions and shutdown bazel to allow test cleanup
          OUTPUT_BASE=$(bazel info output_base)
          chmod -R u+w "$OUTPUT_BASE" 2>/dev/null || true
          bazel shutdown
`,
	}

	repoPath, commitSHA := makeGitRepo(t, workspaceContents)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Kythe Indexing Test",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + commitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
	}

	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	wsPath := testfs.MakeTempDir(t)
	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	// Log output for debugging
	t.Logf("Runner output:\n%s", result.Output)

	require.Equal(t, 0, result.ExitCode, "Runner failed with exit code %d", result.ExitCode)
	assert.Contains(t, result.Output, "SUCCESS: Found kzip files using bazel info output_path")
}
