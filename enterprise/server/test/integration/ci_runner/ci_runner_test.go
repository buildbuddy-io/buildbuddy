package ci_runner_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

var (
	workspaceContentsWithBazelVersionAction = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"buildbuddy.yaml": `
actions:
  - name: "Show bazel version"
    triggers:
      push: { branches: [ master ] }
      pull_request: { branches: [ master ] }
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

	workspaceContentsWithRunScript = map[string]string{
		"WORKSPACE":     `workspace(name = "test")`,
		"BUILD":         `sh_binary(name = "print_args", srcs = ["print_args.sh"])`,
		"print_args.sh": "echo 'args: {{' $@ '}}'",
		"buildbuddy.yaml": `
actions:
  - name: "Print args"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    bazel_commands:
      - run //:print_args -- "Hello world"
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

func invokeRunner(t *testing.T, args []string, env []string, workDir string) *result {
	binPath, err := bazelgo.Runfile("enterprise/server/cmd/ci_runner/ci_runner_/ci_runner")
	if err != nil {
		t.Fatal(err)
	}
	bazelPath, err := bazelgo.Runfile(testbazel.BazelBinaryPath)
	if err != nil {
		t.Fatal(err)
	}
	args = append(args, []string{
		"--bazel_command=" + bazelPath,
		"--bazel_startup_flags=--max_idle_secs=5 --noblock_for_lock",
	}...)

	cmd := exec.Command(binPath, args...)
	cmd.Dir = workDir
	cmd.Env = env
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
	assert.Equal(t, 0, res.ExitCode, "runner returned exit code %d", res.ExitCode)
	assert.Equal(t, 1, len(res.InvocationIDs), "no invocation IDs found in runner output")
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

func singleInvocation(t *testing.T, app *app.App, res *result) *inpb.Invocation {
	bbService := app.BuildBuddyServiceClient(t)
	if !assert.Equal(t, 1, len(res.InvocationIDs)) {
		require.FailNowf(t, "Runner did not output invocation IDs", "output: %s", res.Output)
	}
	invResp, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: res.InvocationIDs[0],
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(invResp.Invocation), "couldn't find runner invocation in DB")
	logResp, err := bbService.GetEventLogChunk(context.Background(), &elpb.GetEventLogChunkRequest{
		InvocationId: res.InvocationIDs[0],
		MinLines:     math.MaxInt32,
	})
	require.NoError(t, err)
	invResp.Invocation[0].ConsoleBuffer = string(logResp.Buffer)
	return invResp.Invocation[0]
}

func TestCIRunner_Push_WorkspaceWithCustomConfig_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Show bazel version",
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

	runnerInvocation := singleInvocation(t, app, result)
	// Since our workflow just runs `bazel version`, we should be able to see its
	// output in the action logs.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
}

func TestCIRunner_Push_WorkspaceWithDefaultTestAllConfig_RunsAndUploadsResultsToBES(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithTestsAndNoBuildBuddyYAML)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test all targets",
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

	assert.NotEqual(t, 0, result.ExitCode)

	runnerInvocation := singleInvocation(t, app, result)
	assert.Contains(
		t, runnerInvocation.ConsoleBuffer,
		"Executed 2 out of 2 tests",
		"2 tests should have been executed",
	)
	assert.Contains(
		t, runnerInvocation.ConsoleBuffer,
		"1 test passes",
		"1 test should have passed",
	)
	assert.Contains(
		t, runnerInvocation.ConsoleBuffer,
		"1 fails locally",
		"1 test should have failed",
	)
}

func TestCIRunner_Push_ReusedWorkspaceWithBazelVersionAction_CanReuseWorkspace(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Show bazel version",
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

	runnerInvocation := singleInvocation(t, app, result)
	// Since our workflow just runs `bazel version`, we should be able to see its
	// output in the action logs.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
}

func TestCIRunner_Push_FailedSync_CanRecoverAndRunCommand(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)

	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Show bazel version",
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

		runnerInvocation := singleInvocation(t, app, result)
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
	wsPath := testfs.MakeTempDir(t)

	targetRepoPath, _ := makeGitRepo(t, workspaceContentsWithTestsAndBuildBuddyYAML)
	pushedRepoPath := testgit.MakeTempRepoClone(t, targetRepoPath)

	// Push one commit to the target repo (to get ahead of the pushed repo),
	// and one commit to the pushed repo (compatible with the target repo).
	testshell.Run(t, targetRepoPath, `
		printf 'echo NONCONFLICTING_EDIT_1 && exit 0\n' > pass.sh
		git add pass.sh
		git commit -m "Update pass.sh"
	`)
	testshell.Run(t, pushedRepoPath, `
		git checkout -b feature
		printf 'echo NONCONFLICTING_EDIT_2 && exit 1\n' > fail.sh
		git add fail.sh
		git commit -m "Update fail.sh"
	`)
	commitSHA := strings.TrimSpace(testshell.Run(t, pushedRepoPath, `git rev-parse HEAD`))

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=pull_request",
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

	require.NotEqual(t, 0, result.ExitCode, "test should fail, so CI runner exit code should be non-zero")

	// Invoke the runner a second time in the same workspace.
	result = invokeRunner(t, runnerFlags, []string{}, wsPath)

	require.NotEqual(t, 0, result.ExitCode, "test should fail, so CI runner exit code should be non-zero")

	runnerInvocation := singleInvocation(t, app, result)
	// We should be able to see both of the changes we made, since they should
	// be merged together.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "NONCONFLICTING_EDIT_1")
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "NONCONFLICTING_EDIT_2")
	if t.Failed() {
		t.Log(runnerInvocation.ConsoleBuffer)
	}
}

func TestCIRunner_PullRequest_MergeConflict_FailsWithMergeConflictMessage(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	targetRepoPath, _ := makeGitRepo(t, workspaceContentsWithTestsAndBuildBuddyYAML)
	pushedRepoPath := testgit.MakeTempRepoClone(t, targetRepoPath)

	// Push one commit to the target repo (to get ahead of the pushed repo),
	// and one commit to the pushed repo (compatible with the target repo).
	testshell.Run(t, targetRepoPath, `
		printf 'echo "CONFLICTING_EDIT_1" && exit 0\n' > pass.sh
		git add pass.sh
		git commit -m "Update pass.sh"
	`)
	testshell.Run(t, pushedRepoPath, `
		git checkout -b feature
		printf 'echo "CONFLICTING_EDIT_2" && exit 0\n' > pass.sh
		git add pass.sh
		git commit -m "Update pass.sh"
	`)
	commitSHA := strings.TrimSpace(testshell.Run(t, pushedRepoPath, `git rev-parse HEAD`))

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=pull_request",
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

	runnerInvocation := singleInvocation(t, app, result)
	assert.Contains(t, runnerInvocation.ConsoleBuffer, `Action failed: Merge conflict between branches "feature" and "master"`)
	if t.Failed() {
		t.Log(runnerInvocation.ConsoleBuffer)
	}
}

func TestCIRunner_PullRequest_FailedSync_CanRecoverAndRunCommand(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	targetRepoPath, _ := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	pushedRepoPath := testgit.MakeTempRepoClone(t, targetRepoPath)

	// Make a commit to the "forked" repository so that the merge is nontrivial.
	testshell.Run(t, pushedRepoPath, `
		git checkout -b feature
		touch feature.sh
		git add feature.sh
		git commit -m "Add feature.sh"
	`)
	commitSHA := strings.TrimSpace(testshell.Run(t, pushedRepoPath, `git rev-parse HEAD`))

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Show bazel version",
		"--trigger_event=pull_request",
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

	run := func() {
		result := invokeRunner(t, runnerFlags, []string{}, wsPath)

		checkRunnerResult(t, result)

		runnerInvocation := singleInvocation(t, app, result)
		// Since our workflow just runs `bazel version`, we should be able to see its
		// output in the action logs.
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
	}

	run()

	// Make a destructive change to the runner workspace and make sure it
	// can recover.
	if err := os.RemoveAll(filepath.Join(wsPath, ".git/refs")); err != nil {
		t.Fatal(err)
	}

	run()
}

func TestRunAction_RespectsArgs(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithRunScript)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Print args",
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

	assert.Contains(t, result.Output, "args: {{ Hello world }}")
}

func TestGitCleanExclude(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	targetRepoPath, commitSHA := makeGitRepo(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `sh_binary(name = "check_repo", srcs = ["check_repo.sh"])`,
		"check_repo.sh": `
			cd "$BUILD_WORKSPACE_DIRECTORY"
			echo "not_excluded.txt exists:" $([[ -e not_excluded.txt ]] && echo yes || echo no)
			echo "excluded.txt exists:" $([[ -e excluded.txt ]] && echo yes || echo no)
			touch ./not_excluded.txt
			touch ./excluded.txt
		`,
		"buildbuddy.yaml": `
actions:
- name: Check repo
  bazel_commands: [ 'bazel run :check_repo' ]
`,
	})

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Check repo",
		"--trigger_event=pull_request",
		"--pushed_repo_url=file://" + targetRepoPath,
		"--pushed_branch=master",
		"--commit_sha=" + commitSHA,
		"--target_repo_url=file://" + targetRepoPath,
		"--target_branch=master",
		"--git_clean_exclude=excluded.txt",
		// Disable clean checkout fallback for this test since we expect to sync
		// without errors.
		"--fallback_to_clean_checkout=false",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)
	require.Contains(t, result.Output, "excluded.txt exists: no")
	require.Contains(t, result.Output, "not_excluded.txt exists: no")

	result = invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)
	require.Contains(t, result.Output, "excluded.txt exists: yes")
	require.Contains(t, result.Output, "not_excluded.txt exists: no")
}

func TestBazelWorkspaceDir(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	repoPath, commitSHA := makeGitRepo(t, map[string]string{
		"subdir/WORKSPACE": "",
		"subdir/BUILD":     `sh_test(name = "pass", srcs = ["pass.sh"])`,
		"subdir/pass.sh":   "",
		"buildbuddy.yaml": `
actions:
- name: Test
  bazel_workspace_dir: subdir
  bazel_commands: [ 'bazel test :pass' ]
`,
	})

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=pull_request",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + commitSHA,
		"--target_repo_url=file://" + repoPath,
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
}

func TestHostedBazel_ApplyingAndDiscardingPatches(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	targetRepoPath, _ := makeGitRepo(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `sh_test(name = "pass", srcs = ["pass.sh"])`,
		"pass.sh":   "exit 0",
	})

	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)

	patch := `
--- a/pass.sh
+++ b/pass.sh
@@ -1 +1 @@
-exit 0
\ No newline at end of file
+echo "EDIT" && exit 0
\ No newline at end of file
`

	ctx := context.Background()
	bsClient := app.ByteStreamClient(t)
	patchDigest, err := cachetools.UploadBlob(ctx, bsClient, "", bytes.NewReader([]byte(patch)))
	require.NoError(t, err)

	// Execute a Bazel command with a patched `pass.sh` that should output 'EDIT'.
	{
		runnerFlags := []string{
			"--pushed_repo_url=file://" + targetRepoPath,
			"--pushed_branch=master",
			"--target_repo_url=file://" + targetRepoPath,
			"--target_branch=master",
			"--cache_backend=" + app.GRPCAddress(),
			"--patch_digest=" + fmt.Sprintf("%s/%d", patchDigest.GetHash(), patchDigest.GetSizeBytes()),
			"--bazel_sub_command", "test --test_output=streamed --nocache_test_results //...",
			// Disable clean checkout fallback for this test since we expect to sync
			// without errors.
			"--fallback_to_clean_checkout=false",
		}
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)
		runnerInvocation := singleInvocation(t, app, result)
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "EDIT")

		if t.Failed() {
			t.Log(runnerInvocation.ConsoleBuffer)
		}
	}

	// Re-run Bazel without a patched `pass.sh` which should revert the previous change.
	{
		runnerFlags := []string{
			"--pushed_repo_url=file://" + targetRepoPath,
			"--pushed_branch=master",
			"--target_repo_url=file://" + targetRepoPath,
			"--target_branch=master",
			"--bazel_sub_command", "test --test_output=streamed --nocache_test_results //...",
			// Disable clean checkout fallback for this test since we expect to sync
			// without errors.
			"--fallback_to_clean_checkout=false",
		}
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)
		runnerInvocation := singleInvocation(t, app, result)
		assert.NotContains(t, runnerInvocation.ConsoleBuffer, "EDIT")

		if t.Failed() {
			t.Log(runnerInvocation.ConsoleBuffer)
		}
	}
}
