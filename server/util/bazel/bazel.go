package bazel

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	invocationIDRegexp = regexp.MustCompile("https?://.*/invocation/([[:graph:]]+)")
)

type InvocationResult struct {
	Error        error
	Stderr       string
	Stdout       string
	InvocationID string
}

// Invoke starts the bazel binary from within the given workspace dir.
func Invoke(ctx context.Context, bazelBinary string, workspaceDir string, subCommand string, args ...string) *InvocationResult {
	// --max_idle_secs prevents the Bazel server (that is potentially spun up by this command)
	// from sticking around for a long time.
	// See https://docs.bazel.build/versions/master/guide.html#clientserver-implementation
	bazelArgs := []string{"--max_idle_secs=5", subCommand}
	bazelArgs = append(bazelArgs, args...)
	var stderr, stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, bazelBinary, bazelArgs...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = workspaceDir
	// Bazel needs a HOME dir to store its local cache; store it under ".home" in the workspace.
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", filepath.Join(workspaceDir, ".home")),
	}

	err := cmd.Run()
	invocationID := ""
	if m := invocationIDRegexp.FindAllStringSubmatch(string(stderr.Bytes()), -1); len(m) > 0 {
		invocationID = m[0][1]
	}
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			log.Warningf("Process exited with non-zero exit code %d. Stderr: %s\n", err.ExitCode(), string(stderr.Bytes()))
		}
	}
	return &InvocationResult{
		Stdout:       string(stdout.Bytes()),
		Stderr:       string(stderr.Bytes()),
		InvocationID: invocationID,
		Error:        err,
	}
}
