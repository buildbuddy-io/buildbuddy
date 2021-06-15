package ci_runner_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	git "github.com/go-git/go-git/v5"
	gitobject "github.com/go-git/go-git/v5/plumbing/object"
)

var (
	workspaceContentsWithBazelVersionAction = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"buildbuddy.yaml": `
actions:
  - name: "Show bazel version"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ version ]
`,
	}

	workspaceContentsWithNoBuildBuddyYAML = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD": `
sh_test(name = "pass", srcs = ["pass.sh"])
sh_test(name = "fail", srcs = ["fail.sh"])
`,
		"pass.sh": `exit 0`,
		"fail.sh": `exit 1`,
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
	wsDir := testbazel.MakeTempWorkspace(t, nil /*=contents*/)
	// Need a home dir so bazel commands invoked by the runner know where to put
	// their local cache.
	homeDir := filepath.Join(wsDir, ".home")
	if err := os.Mkdir(homeDir, 0777); err != nil {
		t.Fatal(err)
	}
	return wsDir
}

// TODO: Replace with testfs.MakeTempDir once #361 is merged.
func makeTempDir(t testing.TB) string {
	tmpDir, err := os.MkdirTemp("", "buildbuddy-test-*")
	if err != nil {
		assert.FailNow(t, "failed to create temp dir", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			assert.FailNow(t, "failed to clean up temp dir", err)
		}
	})
	return tmpDir
}

// TODO: Replace with testfs.WriteAllFileContents once #361 is merged.
func writeAllFileContents(t testing.TB, rootDir string, contents map[string]string) {
	for relPath, content := range contents {
		path := filepath.Join(rootDir, relPath)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			assert.FailNow(t, "failed to create parent dir for file", err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			assert.FailNow(t, "write failed", err)
		}
	}
}

func invokeRunner(t *testing.T, args []string, env []string, workDir string) *result {
	binPath, err := bazelgo.Runfile("enterprise/server/cmd/ci_runner/ci_runner_/ci_runner")
	if err != nil {
		t.Fatal(err)
	}
	bazelPath, err := bazelgo.Runfile(testbazel.BazelBinaryPath)
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

func checkRunnerResult(t *testing.T, res *result) {
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, 1, len(res.InvocationIDs))
	if res.ExitCode != 0 || len(res.InvocationIDs) != 1 {
		t.Logf("runner output:\n===\n%s\n===\n", res.Output)
		t.FailNow()
	}
}

func newUUID(t *testing.T) string {
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func makeGitRepo(t *testing.T, contents map[string]string) (path, commitSHA string) {
	// NOTE: Not using bazel.MakeTempWorkspace here since this repo itself does
	// not need `bazel shutdown` run inside it (only the clone of this repo made
	// by the runner needs `bazel shutdown` to be run).
	path = makeTempDir(t)
	writeAllFileContents(t, path, contents)
	for fileRelPath := range contents {
		filePath := filepath.Join(path, fileRelPath)
		// Make shell scripts executable.
		if strings.HasSuffix(filePath, ".sh") {
			if err := os.Chmod(filePath, 0750); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Make the repo contents globally unique so that this makeGitRepo func can be
	// called more than once to create unique repos with incompatible commit
	// history.
	if err := ioutil.WriteFile(filepath.Join(path, ".repo_id"), []byte(newUUID(t)), 0775); err != nil {
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

func TestCIRunner_WorkspaceWithCustomConfig_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
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

	checkRunnerResult(t, result)

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
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
}

func TestCIRunner_WorkspaceWithDefaultTestAllConfig_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithNoBuildBuddyYAML)
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

	checkRunnerResult(t, result)

	bbService := app.BuildBuddyServiceClient(t)
	res, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: result.InvocationIDs[0],
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Invocation), "couldn't find runner invocation in DB")
	runnerInvocation := res.Invocation[0]
	assert.Contains(
		t, runnerInvocation.ConsoleBuffer,
		"Executed 2 out of 2 tests: 1 test passes and 1 fails locally.",
		"pass.sh test should have passed and fail.sh should have failed.",
	)
}

func TestCIRunner_ReusedWorkspaceWithTestAllAction_CanReuseWorkspace(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--repo_url=file://" + repoPath,
		"--commit_sha=" + headCommitSHA,
		"--branch=master",
		"--trigger_event=push",
		"--trigger_branch=master",
		// Disable clean checkout fallback for this test since we expect to sync
		// the existing repo without errors.
		"--fallback_to_clean_checkout=false",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)

	// Invoke the runner a second time in the same workspace.
	result = invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)

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
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
}

func TestCIRunner_FailedSync_CanRecoverAndRunCommand(t *testing.T) {
	// Use the same workspace to hold multiple different repos.
	wsPath := makeRunnerWorkspace(t)

	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	bbService := app.BuildBuddyServiceClient(t)

	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--repo_url=file://" + repoPath,
		"--commit_sha=" + headCommitSHA,
		"--branch=master",
		"--trigger_event=push",
		"--trigger_branch=master",
	}
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	run := func() {
		result := invokeRunner(t, runnerFlags, []string{}, wsPath)

		checkRunnerResult(t, result)

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
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
	}

	run()

	if err := os.RemoveAll(filepath.Join(wsPath, ".git/refs")); err != nil {
		t.Fatal(err)
	}

	run()
}
