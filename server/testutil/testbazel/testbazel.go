package testbazel

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	// BazelBinaryPath specifies the path to the bazel binary used for invocations.
	// Must match the path in the build rule.
	BazelBinaryPath = "server/util/bazel/bazel-3.7"
)

// Invoke the bazel CLI from within the given workspace dir.
func Invoke(ctx context.Context, t *testing.T, workspaceDir string, subCommand string, args ...string) *bazel.InvocationResult {
	bazelBinaryPath, err := bazelgo.Runfile(BazelBinaryPath)
	if err != nil {
		return &bazel.InvocationResult{Error: err}
	}
	return bazel.Invoke(ctx, bazelBinaryPath, workspaceDir, subCommand, args...)
}

// Clean runs `bazel clean` within the given workspace.
func Clean(ctx context.Context, t *testing.T, workspaceDir string) *bazel.InvocationResult {
	return Invoke(ctx, t, workspaceDir, "clean")
}

func MakeTempWorkspace(t *testing.T, contents map[string]string) string {
	workspaceDir, err := ioutil.TempDir("/tmp", "bazel-workspace-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Run Bazel shutdown so that the server process associated with the temp
		// workspace doesn't stick around. We specify --max_idle_secs=5 above but
		// we want to shut the server down ASAP so that high test volume doesn't
		// cause tons of idle server processes to be running.
		if shutdownResult := Invoke(context.Background(), t, workspaceDir, "shutdown"); shutdownResult.Error != nil {
			t.Fatal(shutdownResult.Error)
		}
		err := os.RemoveAll(workspaceDir)
		if err != nil {
			t.Fatal(err)
		}
	})
	for path, fileContents := range contents {
		fullPath := filepath.Join(workspaceDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(fullPath, []byte(fileContents), 0777); err != nil {
			t.Fatal(err)
		}
	}
	return workspaceDir
}
