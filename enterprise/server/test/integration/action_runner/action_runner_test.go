package ci_runner_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/stretchr/testify/assert"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

var (
	testWorkspaceContents = map[string]string{
		"WORKSPACE":     `workspace(name = "test")`,
		".bazelversion": "3.7.0",
		"buildbuddy.yaml": `
actions:
  - name: "Show bazel version"
    triggers: { push: { branches: [ main ] } }
    bazel_commands: [ version ]
`,
	}

	invocationIDPattern = regexp.MustCompile(`Invocation ID:\s+([a-f0-9-]+)`)
)

type result struct {
	// Output is the combined stdout and stderr of the action runner
	Output string
	// InvocationIDs are the invocation IDs parsed from the output.
	// There should be one invocation ID for each action.
	InvocationIDs []string
	// ExitCode is the exit code of the runner itself.
	ExitCode int
}

func invokeRunner(t *testing.T, args []string, env []string) *result {
	binPath, err := bazelgo.Runfile("enterprise/server/cmd/ci_runner/ci_runner_/ci_runner")
	if err != nil {
		t.Fatal(err)
	}

	runnerWorkDir := bazel.MakeTempWorkspace(t, map[string]string{})
	// Need a home dir so bazel commands invoked by the runner know where to put their local cache.
	runnerHomeDir := filepath.Join(runnerWorkDir, ".home")
	err = os.Mkdir(runnerHomeDir, 0777)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binPath, args...)
	cmd.Dir = runnerWorkDir
	// Use the same environment, including PATH, as this dev machine for now.
	// TODO: Make this closer to the real deployed runner setup.
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, env...)
	cmd.Env = append(cmd.Env, []string{
		fmt.Sprintf("HOME=%s", runnerHomeDir),
	}...)
	outputBytes, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	output := string(outputBytes)

	invocationIDs := []string{}
	iidMatches := invocationIDPattern.FindAllStringSubmatch(output, -1)
	if iidMatches != nil {
		for _, m := range iidMatches {
			invocationIDs = append(invocationIDs, m[1])
		}
	}
	return &result{
		Output:        output,
		ExitCode:      exitCode,
		InvocationIDs: invocationIDs,
	}
}

// Run a shell command and return its stdout, exiting fatally if it fails.
func sh(t *testing.T, dir, command string) string {
	cmd := exec.Command("sh", []string{"-c", command}...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatal(fmt.Errorf("command %q failed: %s. stderr:\n%s", command, err, string(stderr.Bytes())))
	}
	return string(stdout.Bytes())
}

func gitInitAndCommit(t *testing.T, path string) string {
	sh(t, path, "git init")
	sh(t, path, "git config --local user.email test@buildbuddy.io")
	sh(t, path, "git config --local user.name Test")
	sh(t, path, "git add .")
	sh(t, path, `git commit --message 'Initial commit'`)
	return strings.TrimSpace(sh(t, path, "git rev-parse HEAD"))
}

func TestActionRunner_WorkspaceWithTestAllAction_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := bazel.MakeTempWorkspace(t, testWorkspaceContents)
	headCommitSHA := gitInitAndCommit(t, wsPath)
	runnerFlags := []string{
		"--repo_url=file://" + wsPath,
		"--commit_sha=" + headCommitSHA,
		"--trigger_event=push",
		"--trigger_branch=main",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{})

	assert.Equal(t, 0, result.ExitCode)
	require.Equal(t, 1, len(result.InvocationIDs))
	bbService := app.BuildBuddyServiceClient(t)
	res, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: result.InvocationIDs[0],
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Invocation), "couldn't find runner invocation in DB")
	runnerInvocation := res.Invocation[0]
	// Since our workflow just runs `bazel version`, we should be able to see its
	// output in the action logs.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: 3.7.0")
}
