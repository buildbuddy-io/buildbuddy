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

const (
	bazelCommand    = "bazel"
	bazeliskCommand = "bazelisk"
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
		Stdout:       stdout.String(),
		Stderr:       stderrHandler.stderrBuffer.String(),
		InvocationID: stderrHandler.invocationID,
		Error:        err,
	}
}

// FindWorkspaceFile finds the location of the Bazel workspace file (either WORKSPACE.bazel, WORKSPACE, MODULE, or
// MODULE.bazel) in which the startDir is located.
func FindWorkspaceFile(startDir string) (string, error) {
	path, err := filepath.Abs(startDir)
	if err != nil {
		return "", nil
	}
	for path != "/" {
		for _, wsFilename := range []string{"WORKSPACE.bazel", "WORKSPACE", "MODULE", "MODULE.bazel"} {
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
	return "", status.NotFoundError("could not detect workspace root (WORKSPACE.bazel, WORKSPACE, MODULE, MODULE.bazel file not found)")
}

// Hard coded list of bazel commands.
var bazelCommands = map[string]struct{}{
	"analyze-profile":    {},
	"aquery":             {},
	"build":              {},
	"canonicalize-flags": {},
	"clean":              {},
	"coverage":           {},
	"cquery":             {},
	"dump":               {},
	"fetch":              {},
	"help":               {},
	"info":               {},
	"license":            {},
	"mobile-install":     {},
	"print_action":       {},
	"query":              {},
	"run":                {},
	"shutdown":           {},
	"sync":               {},
	"test":               {},
	"version":            {},
}

// GetBazelCommandAndIndex returns the bazel command and its index in the string.
// Ex. For `bazel build //...` it will return `build` and `1`.
// This can be helpful for splitting bazel commands into their different components.
//
// We hard-code the commands here because running `bazel help` to generate them
// can result in undesirable behavior. For example if it's run with different
// startup options than the last bazel command, it will restart the bazel server.
//
// TODO: Return an empty string if the subcommand happens to come after
// a bb-specific command. For example, `bb install --path test` should
// return an empty string, not "test".
// TODO: More robust parsing of startup options. For example, this has a bug
// that passing `bazel --output_base build test ...` returns "build" as the
// bazel command, even though "build" is the argument to --output_base.
func GetBazelCommandAndIndex(args []string) (string, int) {
	for i, a := range args {
		if _, ok := bazelCommands[a]; ok {
			return a, i
		}
	}
	return "", -1
}

// GetStartupOptions makes a best-attempt effort to return the startup options for a bazel command.
// Ex. for `bazel --digest_function=blake3 build //...` it will return ['--digest_function=blake3']
func GetStartupOptions(bazelCommandArgs []string) ([]string, error) {
	_, cmdIdx := GetBazelCommandAndIndex(bazelCommandArgs)
	if cmdIdx == -1 {
		return nil, status.InvalidArgumentErrorf("no bazel command in %v", bazelCommandArgs)
	}
	startupOptions := bazelCommandArgs[:cmdIdx]
	if len(startupOptions) > 0 && startupOptions[0] == bazelCommand || startupOptions[0] == bazeliskCommand {
		startupOptions = startupOptions[1:]
	}
	return startupOptions, nil
}
