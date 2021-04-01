package ci_runner_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	git "github.com/go-git/go-git/v5"
	gitobject "github.com/go-git/go-git/v5/plumbing/object"
)

var (
	testWorkspaceContents = map[string]string{
		"WORKSPACE":     `workspace(name = "test")`,
		".bazelversion": "3.7.0",
		"buildbuddy.yaml": `
actions:
  - name: "Show bazel version"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ version ]
`,
	}

	invocationIDPattern = regexp.MustCompile(`Invocation URL:\s+.*?/invocation/([a-f0-9-]+)`)
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

func makeRunnerWorkspace(t *testing.T) string {
	wsDir := bazel.MakeTempWorkspace(t, map[string]string{})
	// Need a home dir so bazel commands invoked by the runner know where to put
	// their local cache.
	homeDir := filepath.Join(wsDir, ".home")
	if err := os.Mkdir(homeDir, 0777); err != nil {
		t.Fatal(err)
	}
	return wsDir
}

func invokeRunner(t *testing.T, args []string, env []string, workDir string) *result {
	binPath, err := bazelgo.Runfile("enterprise/server/cmd/ci_runner/ci_runner_/ci_runner")
	if err != nil {
		t.Fatal(err)
	}
	bazelPath, err := bazelgo.Runfile("server/testutil/bazel/bazel-3.7.0")
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(binPath, args...)
	cmd.Dir = workDir
	// Use the same environment, including PATH, as this dev machine for now.
	// TODO: Make this closer to the real deployed runner setup.
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, env...)
	cmd.Env = append(cmd.Env, []string{
		fmt.Sprintf("BAZEL_COMMAND=%s", bazelPath),
		fmt.Sprintf("HOME=%s", filepath.Join(workDir, ".home")),
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

func newUUID(t *testing.T) string {
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func makeGitRepo(t *testing.T) (path, commitSHA string) {
	path = bazel.MakeTempWorkspace(t, testWorkspaceContents)
	// Make the repo contents globally unique so that this makeGitRepo func can be
	// called more than once to create unique repos with incompatible commit
	// history.
	if err := ioutil.WriteFile(filepath.Join(path, ".repo_id"), []byte(newUUID(t)), 0644); err != nil {
		t.Fatal(err)
	}

	repo, err := git.PlainInit(path, false /*=bare*/)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if err := tree.AddGlob("*"); err != nil {
		t.Fatal(err)
	}
	hash, err := tree.Commit("Initial commit", &git.CommitOptions{
		Author: &gitobject.Signature{
			Name:  "Test",
			Email: "test@buildbuddy.io",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return path, hash.String()
}

func TestCIRunner_WorkspaceWithTestAllAction_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)
	repoPath, headCommitSHA := makeGitRepo(t)
	runnerFlags := []string{
		"--repo_url=file://" + repoPath,
		"--commit_sha=" + headCommitSHA,
		"--branch=master",
		"--trigger_event=push",
		"--trigger_branch=master",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, 1, len(result.InvocationIDs))
	if result.ExitCode != 0 || len(result.InvocationIDs) != 1 {
		t.Logf("runner output:\n===\n%s\n===\n", result.Output)
		t.FailNow()
	}
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
	// output in the action logs.testWorkspaceContents
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: 3.7.0")
}

func TestCIRunner_ReusedWorkspaceWithTestAllAction_CanReuseWorkspace(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)
	repoPath, headCommitSHA := makeGitRepo(t)
	runnerFlags := []string{
		"--repo_url=file://" + repoPath,
		"--commit_sha=" + headCommitSHA,
		"--branch=master",
		"--trigger_event=push",
		"--trigger_branch=master",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, 1, len(result.InvocationIDs))
	if result.ExitCode != 0 || len(result.InvocationIDs) != 1 {
		t.Logf("runner output (first run):\n===\n%s\n===\n", result.Output)
		t.FailNow()
	}

	// Invoke the runner a second time in the same workspace.
	result = invokeRunner(t, runnerFlags, []string{}, wsPath)

	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, 1, len(result.InvocationIDs))
	if result.ExitCode != 0 || len(result.InvocationIDs) != 1 {
		t.Logf("runner output (second run in reused workspace):\n===\n%s\n===\n", result.Output)
		t.FailNow()
	}

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

func TestCIRunner_ReusedWorkspaceWithMultipleGitRepos_CanReuseWorkspace(t *testing.T) {
	// Use the same workspace to hold multiple different repos.
	wsPath := makeRunnerWorkspace(t)

	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	bbService := app.BuildBuddyServiceClient(t)

	runWithNewRepo := func() {
		repoPath, headCommitSHA := makeGitRepo(t)
		runnerFlags := []string{
			"--repo_url=file://" + repoPath,
			"--commit_sha=" + headCommitSHA,
			"--branch=master",
			"--trigger_event=push",
			"--trigger_branch=master",
		}
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)

		assert.Equal(t, 0, result.ExitCode)
		assert.Equal(t, 1, len(result.InvocationIDs))
		if result.ExitCode != 0 || len(result.InvocationIDs) != 1 {
			t.Logf("runner output:\n===\n%s\n===\n", result.Output)
			t.FailNow()
		}
		res, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
			Lookup: &inpb.InvocationLookup{
				InvocationId: result.InvocationIDs[0],
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(res.Invocation), "couldn't find runner invocation in DB")
		runnerInvocation := res.Invocation[0]
		// Since our workflow just runs `bazel version`, we should be able to see its
		// output in the action logs.testWorkspaceContents
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: 3.7.0")
	}

	runWithNewRepo()
	runWithNewRepo()
}
