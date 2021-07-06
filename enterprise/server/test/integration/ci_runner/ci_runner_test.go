package ci_runner_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
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

	workspaceContentsWithTestsAndNoBuildBuddyYAML = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD": `
sh_test(name = "pass", srcs = ["pass.sh"])
sh_test(name = "fail", srcs = ["fail.sh"])
`,
		"pass.sh": `exit 0`,
		"fail.sh": `exit 1`,
	}

	workspaceContentsWithTestsAndBuildBuddyYAML = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD": `
sh_test(name = "pass", srcs = ["pass.sh"])
sh_test(name = "fail", srcs = ["fail.sh"])
`,
		"pass.sh": `exit 0`,
		"fail.sh": `exit 1`,
		"buildbuddy.yaml": `
actions:
  - name: "Test"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    bazel_commands:
      - test //... --test_output=streamed --nocache_test_results
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
	return testbazel.MakeTempWorkspace(t, nil /*=contents*/)
}

func bash(t *testing.T, workDir string, script string) string {
	cmd := exec.Command("/usr/bin/env", "bash", "-e", "-c", script)
	cmd.Dir = workDir
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "script %q failed: %s", script, string(b))
	return string(b)
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
	args = append(args, "--bazel_command="+bazelPath)

	cmd := exec.Command(binPath, args...)
	cmd.Dir = workDir
	// Use the same environment, including PATH, as this dev machine for now.
	// TODO: Make this closer to the real deployed runner setup.
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, env...)
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
	// Make the repo contents globally unique so that this makeGitRepo func can be
	// called more than once to create unique repos with incompatible commit
	// history.
	contents[".repo_id"] = newUUID(t)
	return testgit.MakeTempRepo(t, contents)
}

func copyGitRepo(t *testing.T, path string) string {
	copyPath := testfs.MakeTempDir(t)
	bash(t, path, fmt.Sprintf(`cp -r * %q/ && cp -r .git %q/`, copyPath, copyPath))
	return copyPath
}

func TestCIRunner_WorkspaceWithCustomConfig_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
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
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithTestsAndNoBuildBuddyYAML)
	runnerFlags := []string{
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
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
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
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
	wsPath := makeRunnerWorkspace(t)

	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	bbService := app.BuildBuddyServiceClient(t)

	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
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

func TestCIRunner_PullRequest_MergesTargetBranchBeforeRunning(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)

	targetRepoPath, _ := makeGitRepo(t, workspaceContentsWithTestsAndBuildBuddyYAML)
	pushedRepoPath := copyGitRepo(t, targetRepoPath)

	// Push one commit to the target repo (to get ahead of the pushed repo),
	// and one commit to the pushed repo (compatible with the target repo).
	bash(t, targetRepoPath, `
		printf 'echo "Hello from target repo" && exit 0\n' > pass.sh
		git add pass.sh
		git commit -m "Update pass.sh"
	`)
	bash(t, pushedRepoPath, `
		git checkout -b feature
		printf 'echo "Goodbye from pushed repo" && exit 1\n' > fail.sh
		git add fail.sh
		git commit -m "Update fail.sh"
	`)
	commitSHA := strings.TrimSpace(bash(t, pushedRepoPath, `git rev-parse HEAD`))

	runnerFlags := []string{
		"--trigger_event=push",
		"--pushed_repo_url=file://" + pushedRepoPath,
		"--pushed_branch=feature",
		"--commit_sha=" + commitSHA,
		"--target_repo_url=file://" + targetRepoPath,
		"--target_branch=master",
		// Disable clean checkout fallback for this test since we expect to sync
		// without errors.
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
	// We should be able to see both of the changes we made, since they should
	// be merged together.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Hello from target repo")
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Goodbye from pushed repo")
	if t.Failed() {
		t.Log(runnerInvocation.ConsoleBuffer)
	}
}

func TestCIRunner_PullRequest_MergeConflict_FailsWithMergeConflictMessage(t *testing.T) {
	wsPath := makeRunnerWorkspace(t)

	targetRepoPath, _ := makeGitRepo(t, workspaceContentsWithTestsAndBuildBuddyYAML)
	pushedRepoPath := copyGitRepo(t, targetRepoPath)

	// Push one commit to the target repo (to get ahead of the pushed repo),
	// and one commit to the pushed repo (compatible with the target repo).
	bash(t, targetRepoPath, `
		printf 'echo "Hello from target repo" && exit 0\n' > pass.sh
		git add pass.sh
		git commit -m "Update pass.sh"
	`)
	bash(t, pushedRepoPath, `
		git checkout -b feature
		printf 'echo "CONFLICTING EDIT!!!" && exit 0\n' > pass.sh
		git add pass.sh
		git commit -m "Update pass.sh"
	`)
	commitSHA := strings.TrimSpace(bash(t, pushedRepoPath, `git rev-parse HEAD`))

	runnerFlags := []string{
		"--trigger_event=push",
		"--pushed_repo_url=file://" + pushedRepoPath,
		"--pushed_branch=feature",
		"--commit_sha=" + commitSHA,
		"--target_repo_url=file://" + targetRepoPath,
		"--target_branch=master",
		// Disable clean checkout fallback for this test since we expect to sync
		// without errors.
		"--fallback_to_clean_checkout=false",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	bbService := app.BuildBuddyServiceClient(t)
	res, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: result.InvocationIDs[0],
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Invocation), "couldn't find runner invocation in DB")
	runnerInvocation := res.Invocation[0]
	// We should be able to see both of the changes we made, since they should
	// be merged together.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Action failed: Merge conflict (feature -> master)")
	if t.Failed() {
		t.Log(runnerInvocation.ConsoleBuffer)
	}
}
