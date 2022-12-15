package bazel

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	log.Infof("[bazel] %s", strings.TrimSuffix(line, "\n"))
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
	cmd.Env = append(os.Environ(), []string{
		fmt.Sprintf("HOME=%s", filepath.Join(workspaceDir, ".home")),
	}...)
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

// FindWorkspaceFile finds the location of the Bazel workspace file (either WORKSPACE.bazel or WORKSPACE) in which the
// startDir is located.
func FindWorkspaceFile(startDir string) (string, error) {
	path, err := filepath.Abs(startDir)
	if err != nil {
		return "", nil
	}
	for path != "/" {
		for _, wsFilename := range []string{"WORKSPACE.bazel", "WORKSPACE"} {
			wsFilePath := filepath.Join(path, wsFilename)
			_, err := os.Stat(wsFilePath)
			if err == nil {
				return wsFilePath, nil
			}
			if !os.IsNotExist(err) {
				log.Warningf("Could not check existence of workspace file at %q: %s\n", wsFilePath, err)
				continue
			}
		}
		path = filepath.Dir(path)
	}
	return "", status.NotFoundError("could not detect workspace root (WORKSPACE.bazel or WORKSPACE file not found)")
}
