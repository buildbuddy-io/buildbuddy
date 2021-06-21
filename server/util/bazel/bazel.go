package bazel

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

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

type bazelStderrHandler struct {
	stderrBuffer bytes.Buffer
	invocationID string
}

func (w *bazelStderrHandler) Write(b []byte) (int, error) {
	w.stderrBuffer.Write(b)
	line := string(b)
	if m := invocationIDRegexp.FindAllStringSubmatch(line, -1); len(m) > 0 {
		w.invocationID = m[0][1]
	}
	log.Infof("[bazel] %s", strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}

// Invoke starts the bazel binary from within the given workspace dir.
func Invoke(ctx context.Context, bazelBinary string, workspaceDir string, subCommand string, args ...string) *InvocationResult {
	// --max_idle_secs prevents the Bazel server (that is potentially spun up by this command)
	// from sticking around for a long time.
	// See https://docs.bazel.build/versions/master/guide.html#clientserver-implementation
	bazelArgs := []string{"--max_idle_secs=5", subCommand}
	bazelArgs = append(bazelArgs, args...)
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, bazelBinary, bazelArgs...)
	cmd.Stdout = &stdout
	stderrHandler := &bazelStderrHandler{}
	cmd.Stderr = stderrHandler
	cmd.Dir = workspaceDir
	// Bazel needs a HOME dir to store its local cache; store it under ".home" in the workspace.
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", filepath.Join(workspaceDir, ".home")),
	}
	err := cmd.Run()
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			log.Warningf("Process exited with non-zero exit code %d.", err.ExitCode())
		}
	}
	return &InvocationResult{
		Stdout:       string(stdout.Bytes()),
		Stderr:       stderrHandler.stderrBuffer.String(),
		InvocationID: stderrHandler.invocationID,
		Error:        err,
	}
}
