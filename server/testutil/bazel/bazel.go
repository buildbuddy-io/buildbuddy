package bazel

import (
	"bytes"
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

const (
	// Path to the bazel binary. Must match the path in the build rule.
	bazelPath = "server/testutil/bazel/bazel-3.7.0"
)

var (
	invocationIDRegexp = regexp.MustCompile("http://localhost:8080/invocation/([[:graph:]]+)")
)

type cleanupFunc func()

type InvocationResult struct {
	Stderr       string
	Stdout       string
	InvocationID string
	Error        error
}

// Invoke the bazel CLI from within the given workspace dir.
func Invoke(ctx context.Context, t *testing.T, workspaceDir string, subCommand string, args ...string) *InvocationResult {
	bazelBinaryPath, err := bazelgo.Runfile(bazelPath)
	if err != nil {
		return &InvocationResult{Error: err}
	}
	// --max_idle_secs prevents the Bazel server (that is potentially spun up by this command)
	// from sticking around for a long time.
	// See https://docs.bazel.build/versions/master/guide.html#clientserver-implementation
	bazelArgs := []string{"--max_idle_secs=5", subCommand}
	bazelArgs = append(bazelArgs, args...)
	var stderr, stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, bazelBinaryPath, bazelArgs...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = workspaceDir
	// Bazel needs a HOME dir to store its local cache; store it under ".home" in the workspace.
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", filepath.Join(workspaceDir, ".home")),
	}

	err = cmd.Run()
	invocationID := ""
	if m := invocationIDRegexp.FindAllStringSubmatch(string(stderr.Bytes()), -1); len(m) > 0 {
		invocationID = m[0][1]
	}
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			fmt.Printf("WARNING: Process exited with non-zero exit code %d. Stderr: %s\n", err.ExitCode(), string(stderr.Bytes()))
		}
	}
	return &InvocationResult{
		Stdout:       string(stdout.Bytes()),
		Stderr:       string(stderr.Bytes()),
		InvocationID: invocationID,
		Error:        err,
	}
}

// Clean runs `bazel clean` within the given workspace.
func Clean(ctx context.Context, t *testing.T, workspaceDir string) *InvocationResult {
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
