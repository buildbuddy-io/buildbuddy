package bazel

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"

	context "context"
)

var (
	invocationIDRegexp = regexp.MustCompile("http://localhost:8080/invocation/([[:graph:]]+)")
)

type InvocationResult struct {
	Output       string
	InvocationID string
	Error        error
}

// Invoke the bazel CLI from within the given workspace dir, pointing it at a local BuildBuddy server.
func Invoke(ctx context.Context, t *testing.T, workspaceDir string, subCommand string, args ...string) *InvocationResult {
	bazelArgs := []string{
		subCommand,
		"--bes_results_url=http://localhost:8080/invocation/",
		"--bes_backend=grpc://localhost:1985",
		"--remote_cache=grpc://localhost:1985",
		"--remote_upload_local_results",
	}
	bazelArgs = append(bazelArgs, args...)
	bazelBinaryPath, err := bazelgo.Runfile("server/testutil/bazel/bazel-3.7.0-linux-x86_64")
	if err != nil {
		return &InvocationResult{Error: err}
	}
	cmd := exec.CommandContext(ctx, bazelBinaryPath, bazelArgs...)
	cmd.Dir = workspaceDir
	// Bazel needs a HOME dir to store its local cache.
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", filepath.Join(workspaceDir, ".home")),
	}

	b, err := cmd.CombinedOutput()
	output := string(b)
	invocationID := ""
	if m := invocationIDRegexp.FindAllStringSubmatch(output, -1); len(m) > 0 {
		invocationID = m[0][1]
	}
	return &InvocationResult{
		Output:       string(b),
		InvocationID: invocationID,
		Error:        err,
	}
}

// Clean runs `bazel clean` within the given workspace.
func Clean(ctx context.Context, t *testing.T, workspaceDir string) {
	bazelBinaryPath, err := bazelgo.Runfile("server/testutil/bazel/bazel-3.7.0-linux-x86_64")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, bazelBinaryPath, "clean")
	cmd.Dir = workspaceDir
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", filepath.Join(workspaceDir, ".home")),
	}
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
}

func MakeTempWorkspace(t *testing.T, contents map[string]string) string {
	workspaceDir, err := ioutil.TempDir("/tmp", "bazel-workspace-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(workspaceDir)
		if err != nil {
			t.Fatal(err)
		}
	})
	for path, fileContents := range contents {
		fullPath := filepath.Join(workspaceDir, path)
		if err := os.MkdirAll(filepath.Dir(path), 0644); err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(fullPath, []byte(fileContents), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return workspaceDir
}
