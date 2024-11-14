package ci_runner_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protodelim"
	"gopkg.in/yaml.v2"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rlpb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

const (
	// Startup flags to be applied to bazel for the test only. max_idle_secs
	// prevents the process from sticking around after the test completes.
	// noblock_for_lock is set as a way to assert that we never have multiple
	// bazel processes contending for the workspace lock.
	bazelStartupFlags = "--max_idle_secs=5 --noblock_for_lock"
)

var (
	// set by x_defs in BUILD file
	ciRunnerRunfilePath string

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

	workspaceContentsWithEnvVars = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD":     `sh_test(name = "check_env", srcs = ["check_env.sh"])`,
		"check_env.sh": `
		
		if [[ "$TEST_SECRET_1" != "test_secret_1_value" ]]; then
				echo "TEST_SECRET_1 env var: expected 'test_secret_1_value', got $TEST_SECRET_1"
				exit 1
			fi
			if [[ "$1" != "test_secret_2_value" ]]; then
				echo "test arg #1: expected 'test_secret_2_value', got $1"
				exit 1
			fi

			echo "env checks passed"
		`,
		"buildbuddy.yaml": `
actions:
  - name: "Test env expansion"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    bazel_commands:
      - test :check_env --test_env=TEST_SECRET_1 --test_arg=$TEST_SECRET_2 --test_output=all
`,
	}

	workspaceContentsWithLocalEnvironmentalErrorAction = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD":     `sh_binary(name = "exit", srcs = ["exit.sh"])`,
		"exit.sh":   `exit "$1"`,
		"buildbuddy.yaml": `
actions:
  - name: "Exit 36"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    bazel_commands:
      - run :exit -- 36
`,
	}

	workspaceContentsWithExitScriptAndMergeDisabled = map[string]string{
		"WORKSPACE": "",
		"BUILD":     `sh_binary(name = "exit", srcs = ["exit.sh"])`,
		"exit.sh":   `exit "$1"`,
		"buildbuddy.yaml": `
actions:
  - name: "Test"
    triggers:
      pull_request:
        branches: [ master ]
        merge_with_base: false
    bazel_commands:
      - run :exit -- 0
`,
	}

	workspaceContentsWithArtifactUploads = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD": `
sh_test(name = "pass", srcs = ["pass.sh"])
sh_binary(name = "check_artifacts_dir", srcs = ["check_artifacts_dir.sh"])
`,
		"pass.sh": `exit 0`,
		"check_artifacts_dir.sh": `
			# Make sure artifacts dir exists
			artifacts_root="$1/.."
			if ! [[ -e "$artifacts_root/command-0" ]]; then exit 1; fi
			# Make sure there are no files from previous invocations anywhere
			# under the arrtifacts root dir
			if [[ "$(find "$artifacts_root" -type f)" ]]; then exit 1; fi
			exit 0
		`,
		"buildbuddy.yaml": `
actions:
  - name: "Test"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    bazel_commands:
      - run :check_artifacts_dir -- $BUILDBUDDY_ARTIFACTS_DIRECTORY
      - test //... --config=buildbuddy_remote_cache --experimental_remote_grpc_log=$BUILDBUDDY_ARTIFACTS_DIRECTORY/grpc.log
`,
	}

	workspaceContentsWithGitLog = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD":     `sh_binary(name = "log", srcs = ["log.sh"])`,
		"log.sh": `
			cd "$BUILD_WORKSPACE_DIRECTORY"
			git log
`,
		"buildbuddy.yaml": `
actions:
  - name: "Test"
    triggers:
      push: { branches: [ master ] }
    git_fetch_depth: 1
    bazel_commands:
      - run :log
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
	// ExitCode is the exit code of the runner itself, or -1 if the runner was
	// terminated by a signal.
	ExitCode int
	// Signal is the signal that terminated the runner, or -1 if the runner
	// exited.
	Signal syscall.Signal
	// Set to true if the ".BUILDBUDDY_DO_NOT_RECYCLE" file was created in the
	// workspace.
	DoNotRecycle bool
}

func invokeRunner(t *testing.T, args []string, env []string, workDir string) *result {
	binPath, err := runfiles.Rlocation(ciRunnerRunfilePath)
	if err != nil {
		t.Fatal(err)
	}
	bazelPath, err := runfiles.Rlocation(testbazel.BazelBinaryPath)
	if err != nil {
		t.Fatal(err)
	}
	args = append([]string{
		"--bazel_command=" + bazelPath,
		"--bazel_startup_flags=" + bazelStartupFlags,
	}, args...)

	cmd := exec.Command(binPath, args...)
	cmd.Dir = workDir
	cmd.Env = env
	outputBytes, err := cmd.CombinedOutput()
	exitCode := -1
	signal := syscall.Signal(-1)
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			ws := exitErr.Sys().(syscall.WaitStatus)
			if ws.Exited() {
				exitCode = ws.ExitStatus()
			} else {
				signal = ws.Signal()
			}
		} else {
			t.Fatal(err)
		}
	} else {
		exitCode = 0
	}
	output := string(outputBytes)
	t.Log(output)

	invocationIDs := []string{}
	iidMatches := invocationIDPattern.FindAllStringSubmatch(output, -1)
	for _, m := range iidMatches {
		invocationIDs = append(invocationIDs, m[1])
	}
	return &result{
		Output:        output,
		ExitCode:      exitCode,
		Signal:        signal,
		InvocationIDs: invocationIDs,
		DoNotRecycle:  testfs.Exists(t, workDir, ".BUILDBUDDY_DO_NOT_RECYCLE"),
	}
}

func checkRunnerResult(t *testing.T, res *result) {
	assert.Equal(t, 0, res.ExitCode, "runner returned exit code %d", res.ExitCode)
	assert.Equal(t, 1, len(res.InvocationIDs), "no invocation IDs found in runner output")
	if res.ExitCode != 0 || len(res.InvocationIDs) != 1 {
		t.Logf("runner output:\n===\n%s\n===\n", res.Output)
		t.FailNow()
	}
	assert.False(t, res.DoNotRecycle, ".BUILDBUDDY_DO_NOT_RECYCLE file unexpectedly exists")
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

func getRunnerInvocation(t *testing.T, app *app.App, res *result) *inpb.Invocation {
	bbService := app.BuildBuddyServiceClient(t)
	if !assert.Equal(t, 1, len(res.InvocationIDs)) {
		require.FailNowf(t, "Runner did not output invocation IDs", "output: %s", res.Output)
	}
	invResp, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId:          res.InvocationIDs[0],
			FetchChildInvocations: true,
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

func getInnerInvocation(t *testing.T, app *app.App, res *result) *inpb.Invocation {
	bbService := app.BuildBuddyServiceClient(t)

	pattern := `Streaming build results to: ` + app.HTTPURL() + `/invocation/([a-f0-9-]+)`
	innerIIDPattern := regexp.MustCompile(pattern)
	iidMatches := innerIIDPattern.FindAllStringSubmatch(res.Output, -1)
	invocationIDs := make([]string, 0)
	for _, m := range iidMatches {
		invocationIDs = append(invocationIDs, m[1])
	}

	if !assert.GreaterOrEqual(t, len(invocationIDs), 1) {
		require.FailNowf(t, "Runner did not output invocation IDs", "output: %s", res.Output)
	}

	invResp, err := bbService.GetInvocation(context.Background(), &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationIDs[0],
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

func TestCIRunner_RunsBashCommands(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	workspaceContentsWithBashCommands := map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"buildbuddy.yaml": `
actions:
  - name: "Show bazel version"
    triggers:
      push: { branches: [ master ] }
      pull_request: { branches: [ master ] }
    steps:
      - run: |
          for i in {1..2}; do
            echo "Loop $i: ";
            bazel version;
          done
`,
	}
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBashCommands)
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

	runnerInvocation := getRunnerInvocation(t, app, result)
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Loop 1:")
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Loop 2:")
}

func TestCIRunner_RunsBashCommands_BazelWithOptions(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	testCases := []struct {
		name           string
		command        string
		expectedOutput string
	}{
		{
			name: "Has startup options",
			// Set a small JVM memory limit to cause Bazel to OOM.
			command:        "bazel --host_jvm_args=-Xmx5m build //:print_args",
			expectedOutput: "java.lang.OutOfMemoryError",
		},
		{
			name:           "Has additional bazel args",
			command:        "bazel build //:print_args --invocation_id=00000000-0000-0000-0000-000000000000",
			expectedOutput: "00000000-0000-0000-0000-000000000000",
		},
	}

	for _, tc := range testCases {
		workspaceContents := map[string]string{
			"WORKSPACE":     `workspace(name = "test")`,
			"BUILD":         `sh_binary(name = "print_args", srcs = ["print_args.sh"])`,
			"print_args.sh": "echo 'args: {{' $@ '}}'",
			"buildbuddy.yaml": `
actions:
  - name: "Test action"
    steps:
      - run: ` + tc.command,
		}

		repoPath, _ := makeGitRepo(t, workspaceContents)
		runnerFlags := []string{
			"--workflow_id=test-workflow",
			"--action_name=Test action",
			"--trigger_event=push",
			"--pushed_repo_url=file://" + repoPath,
			"--pushed_branch=master",
		}
		// Start the app so the runner can use it as the BES backend.
		app := buildbuddy.Run(t)
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		runnerInvocation := getRunnerInvocation(t, app, result)
		require.Contains(t, runnerInvocation.ConsoleBuffer, tc.expectedOutput, tc.name)
	}
}

func TestCIRunner_AppliesCustomBazelrc(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	testCases := []struct {
		name                   string
		workspaceContents      map[string]string
		expectedStartupOptions []string
		expectModifiedIID      bool
	}{
		{
			name: "Has a workspace .bazelrc",
			workspaceContents: map[string]string{
				"WORKSPACE": `workspace(name = "test")`,
				".bazelrc": `
common --invocation_id=00000000-0000-0000-0000-000000000000
`,
				"BUILD":         `sh_binary(name = "print_args", srcs = ["print_args.sh"])`,
				"print_args.sh": "echo 'args: {{' $@ '}}'",
				"buildbuddy.yaml": `
actions:
  - name: "Test action"
    steps:
      - run: bazel build //:print_args
`,
			},
			expectedStartupOptions: []string{
				"buildbuddy.bazelrc",
				"repo-root/.bazelrc",
				"--noworkspace_rc",
			},
			expectModifiedIID: true,
		},
		{
			name: "Does not have a workspace .bazelrc",
			workspaceContents: map[string]string{
				"WORKSPACE":     `workspace(name = "test")`,
				"BUILD":         `sh_binary(name = "print_args", srcs = ["print_args.sh"])`,
				"print_args.sh": "echo 'args: {{' $@ '}}'",
				"buildbuddy.yaml": `
actions:
  - name: "Test action"
    steps:
      - run: bazel build //:print_args
`,
			},
			expectedStartupOptions: []string{
				"buildbuddy.bazelrc",
			},
			expectModifiedIID: false,
		},
		{
			// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/3688):
			// don't require bazel_workspace_dir to be set in order for
			// recycling to work properly.
			name: "Workspace is in a subdir",
			workspaceContents: map[string]string{
				"subdir/WORKSPACE": `workspace(name = "test")`,
				"subdir/.bazelrc": `
common --invocation_id=00000000-0000-0000-0000-000000000000
`,
				"subdir/BUILD":         `sh_binary(name = "print_args", srcs = ["print_args.sh"])`,
				"subdir/print_args.sh": "echo 'args: {{' $@ '}}'",
				"buildbuddy.yaml": `
actions:
  - name: "Test action"
    bazel_workspace_dir: subdir
    steps:
      - run: bazel build //:print_args
`,
			},
			expectedStartupOptions: []string{
				"buildbuddy.bazelrc",
				"repo-root/subdir/.bazelrc",
				"--noworkspace_rc",
			},
			expectModifiedIID: true,
		},
	}

	for _, tc := range testCases {
		repoPath, _ := makeGitRepo(t, tc.workspaceContents)
		runnerFlags := []string{
			"--workflow_id=test-workflow",
			"--action_name=Test action",
			"--pushed_repo_url=file://" + repoPath,
			"--pushed_branch=master",
		}
		// Start the app so the runner can use it as the BES backend.
		app := buildbuddy.Run(t)
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)

		runnerInvocation := getRunnerInvocation(t, app, result)
		innerInvocation := getInnerInvocation(t, app, result)

		missingStartupFlags := make(map[string]struct{}, 0)
		for _, sf := range tc.expectedStartupOptions {
			missingStartupFlags[sf] = struct{}{}
		}

		for _, cl := range innerInvocation.StructuredCommandLine {
			for _, s := range cl.Sections {
				if s.SectionLabel == "startup options" {
					optionList, ok := s.SectionType.(*clpb.CommandLineSection_OptionList)
					if !ok {
						continue
					}
					for _, o := range optionList.OptionList.Option {
						for _, so := range tc.expectedStartupOptions {
							if strings.Contains(o.CombinedForm, so) {
								delete(missingStartupFlags, so)
							}
						}
					}
				}
			}
		}
		require.Empty(t, missingStartupFlags)

		// Check logs that BES url from buildbuddy.bazelrc was applied
		require.Contains(t, runnerInvocation.ConsoleBuffer, app.HTTPURL(), tc.name)

		if tc.expectModifiedIID {
			require.Contains(t, runnerInvocation.ConsoleBuffer, "00000000-0000-0000-0000-000000000000")
		} else {
			require.NotContains(t, runnerInvocation.ConsoleBuffer, "00000000-0000-0000-0000-000000000000")
		}
	}
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

	runnerInvocation := getRunnerInvocation(t, app, result)
	// Since our workflow just runs `bazel version`, we should be able to see its
	// output in the action logs.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
}

func TestCredentialHelper(t *testing.T) {
	binPath, err := runfiles.Rlocation(ciRunnerRunfilePath)
	require.NoError(t, err)

	for _, tc := range []struct {
		user     string
		token    string
		expected string
	}{
		{
			user:     "foo",
			token:    "bar",
			expected: "username=foo\npassword=bar\n",
		},
		{
			user:     "",
			token:    "bar",
			expected: "username=x-access-token\npassword=bar\n",
		},
	} {
		// credential helper should run relatively quickly
		ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFn()

		cmd := exec.CommandContext(ctx, binPath, "--credential_helper", "get")
		cmd.Env = []string{
			"REPO_USER=" + tc.user,
			"REPO_TOKEN=" + tc.token,
		}
		// Simulating passing empty pipe to the binary
		//   echo '' | <binary> <args...>
		stdin, err := cmd.StdinPipe()
		require.NoError(t, err)
		var stdout bytes.Buffer
		cmd.Stdout = &stdout

		require.NoError(t, cmd.Start())
		require.NoError(t, stdin.Close())
		require.NoError(t, cmd.Wait())
		require.Equal(t, tc.expected, stdout.String())
	}
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

	runnerInvocation := getRunnerInvocation(t, app, result)
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

	runnerInvocation := getRunnerInvocation(t, app, result)
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

		runnerInvocation := getRunnerInvocation(t, app, result)
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

func TestCIRunner_Fork_MergesTargetBranchBeforeRunning(t *testing.T) {
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

	testCases := []struct {
		name      string
		repoFlags []string
	}{
		{
			name: "Forked repo has branch and commit sha set",
			repoFlags: []string{
				"--pushed_branch=feature",
				"--commit_sha=" + commitSHA,
			},
		},
		{
			name: "Forked repo just has branch set",
			repoFlags: []string{
				"--pushed_branch=feature",
			},
		},
		{
			name: "Forked repo just has commit sha set",
			repoFlags: []string{
				"--commit_sha=" + commitSHA,
			},
		},
	}

	baselineRunnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=pull_request",
		"--pushed_repo_url=file://" + pushedRepoPath,
		"--target_repo_url=file://" + targetRepoPath,
		"--target_branch=master",
		// Disable clean checkout fallback for this test since we expect to sync
		// without errors.
		"--fallback_to_clean_checkout=false",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)

	for _, tc := range testCases {
		runnerFlags := append(baselineRunnerFlags, tc.repoFlags...)
		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		runnerInvocation := getRunnerInvocation(t, app, result)
		// We should be able to see both of the changes we made, since they should
		// be merged together.
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "NONCONFLICTING_EDIT_1", tc.name)
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "NONCONFLICTING_EDIT_2", tc.name)
	}
}

func TestCIRunner_Fork_MergeConflict_FailsWithMergeConflictMessage(t *testing.T) {
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

	runnerInvocation := getRunnerInvocation(t, app, result)
	assert.Contains(t, runnerInvocation.ConsoleBuffer, `Action failed: Merge conflict between branches "feature" and "master"`)
	if t.Failed() {
		t.Log(runnerInvocation.ConsoleBuffer)
	}
}

func TestCIRunner_Merge_FetchesCompleteGitHistory(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, _ := makeGitRepo(t, workspaceContentsWithRunScript)

	testshell.Run(t, repoPath, `
		# Create a base branch
		git checkout -B base
		printf 'echo "Base Commit" && exit 0\n' > base1.sh
		git add base1.sh
		git commit -m "Original commit on base"

		# Create a feature branch off the first commit of the base branch
		git checkout -B feature
		printf 'echo NONCONFLICTING_EDIT && exit 0\n' > feature.sh
		git add feature.sh
		git commit -m "Commit from feature branch"

		# Add another commit to the base branch
		printf 'echo "Second commit on base" && exit 0\n' > base2.sh
		git add base2.sh
		git commit -m "Second commit on base"
	`)

	baselineRunnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Print args",
		"--trigger_event=pull_request",
		"--pushed_repo_url=file://" + repoPath,
		// Disable clean checkout fallback for this test since we expect to sync
		// without errors.
		"--fallback_to_clean_checkout=false",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)

	// Have the runner checkout the base branch with --depth=1
	flagsShallowFetchBase := append(baselineRunnerFlags, "--pushed_branch=base", "--git_fetch_depth=1")
	result := invokeRunner(t, flagsShallowFetchBase, []string{}, wsPath)
	checkRunnerResult(t, result)

	// Now have the runner checkout the feature branch, with merge_with_base enabled.
	// This should succeed, even though the runner will have to fetch the original
	// commit on the `base` branch (the merge-base)
	flagsMergeFeatureBranch := append(baselineRunnerFlags,
		"--pushed_branch=feature",
		"--target_repo_url=file://"+repoPath,
		"--target_branch=base",
	)
	result = invokeRunner(t, flagsMergeFeatureBranch, []string{}, wsPath)
	checkRunnerResult(t, result)
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

		runnerInvocation := getRunnerInvocation(t, app, result)
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

func TestCIRunner_IgnoresInvalidFlags(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Show bazel version",
		"--trigger_event=push",
		"--fake=blah",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
		"--fake2=blah",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)

	runnerInvocation := getRunnerInvocation(t, app, result)
	// Since our workflow just runs `bazel version`, we should be able to see its
	// output in the action logs.
	assert.Contains(t, runnerInvocation.ConsoleBuffer, "Build label: ")
}

func TestRunAction_RespectsCommitSha(t *testing.T) {
	testCases := []struct {
		setBranchName bool
	}{
		{
			setBranchName: true,
		},
		{
			setBranchName: false,
		},
	}

	for _, tc := range testCases {
		wsPath := testfs.MakeTempDir(t)
		repoPath, initialCommitSHA := makeGitRepo(t, workspaceContentsWithRunScript)

		baselineRunnerFlags := []string{
			"--workflow_id=test-workflow",
			"--action_name=Print args",
			"--trigger_event=push",
			"--pushed_repo_url=file://" + repoPath,
			"--target_repo_url=file://" + repoPath,
		}
		// Start the app so the runner can use it as the BES backend.
		app := buildbuddy.Run(t)
		baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)
		if tc.setBranchName {
			baselineRunnerFlags = append(baselineRunnerFlags, "--pushed_branch=master", "--target_branch=master")
		}

		runnerFlagsCommit1 := append(baselineRunnerFlags, "--commit_sha="+initialCommitSHA)
		result := invokeRunner(t, runnerFlagsCommit1, []string{}, wsPath)
		checkRunnerResult(t, result)
		assert.Contains(t, result.Output, "args: {{ Hello world }}")

		// Commit changes to the print statement in the workflow config
		modifiedWorkflowConfig := `
actions:
  - name: "Print args"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    bazel_commands:
      - run //:print_args -- "Switcheroo!"
`
		newCommitSha := testgit.CommitFiles(t, repoPath, map[string]string{"buildbuddy.yaml": modifiedWorkflowConfig})

		// When invoked with the initial commit sha, should not contain the modified print statement
		result = invokeRunner(t, runnerFlagsCommit1, []string{}, wsPath)
		if !tc.setBranchName {
			// Older versions of Git do not support fetching non-HEAD commits by
			// default. In this case, if pushed_branch is not set as a fallback, the
			// fetch will fail. However, the default behavior for this has since
			// changed and the old behavior been entirely removed as an option in
			// https://patchwork.kernel.org/project/git/patch/20181217221625.1523-1-avarab@gmail.com/#22387021
			if result.ExitCode != 0 {
				require.NotEqual(t, 0, result.ExitCode)
				// The DO_NOT_RECYCLE file should get created here since we failed
				// to set up the workspace - recreate the workspace here to match
				// what the executor would do.
				require.True(t, testfs.Exists(t, wsPath, ".BUILDBUDDY_DO_NOT_RECYCLE"))
				wsPath = testfs.MakeTempDir(t)
			} else {
				checkRunnerResult(t, result)
				assert.Contains(t, result.Output, "args: {{ Hello world }}")
			}
		} else {
			checkRunnerResult(t, result)
			assert.Contains(t, result.Output, "args: {{ Hello world }}")
		}

		// When invoked with the new commit sha, should contain the modified print statement
		runnerFlagsCommit2 := append(baselineRunnerFlags, "--commit_sha="+newCommitSha)
		result = invokeRunner(t, runnerFlagsCommit2, []string{}, wsPath)
		checkRunnerResult(t, result)
		assert.Contains(t, result.Output, "args: {{ Switcheroo! }}")
	}
}

func TestRunAction_PushedRepoOnly(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, initialCommitSHA := makeGitRepo(t, workspaceContentsWithRunScript)

	testCases := []struct {
		name                    string
		repoFlags               []string
		expectedReportingValues map[string]string
	}{
		{
			name: "Pushed branch and commit sha",
			repoFlags: []string{
				"--pushed_branch=master",
				"--commit_sha=" + initialCommitSHA,
			},
			expectedReportingValues: map[string]string{
				"branch": "master",
				"commit": initialCommitSHA,
			},
		},
		{
			name: "Just pushed branch",
			repoFlags: []string{
				"--pushed_branch=master",
			},
			expectedReportingValues: map[string]string{
				"branch": "master",
				"commit": initialCommitSHA,
			},
		},
		{
			name: "Just commit sha",
			repoFlags: []string{
				"--commit_sha=" + initialCommitSHA,
			},
			expectedReportingValues: map[string]string{
				"branch": "",
				"commit": initialCommitSHA,
			},
		},
	}
	baselineRunnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Print args",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)

	for _, tc := range testCases {
		runnerFlags := append(baselineRunnerFlags, tc.repoFlags...)
		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)
		assert.Contains(t, result.Output, "args: {{ Hello world }}", tc.name)

		// Check that metadata was reported correctly
		runnerInvocation := getRunnerInvocation(t, app, result)
		var workspaceStatusEvent *bespb.WorkspaceStatus
		for _, e := range runnerInvocation.Event {
			if e.BuildEvent.GetWorkspaceStatus() != nil {
				workspaceStatusEvent = e.BuildEvent.GetWorkspaceStatus()
				break
			}
		}
		require.NotNil(t, workspaceStatusEvent, tc.name)

		workspaceStatusMap := make(map[string]string, len(workspaceStatusEvent.Item))
		for _, i := range workspaceStatusEvent.Item {
			workspaceStatusMap[i.GetKey()] = i.GetValue()
		}

		require.Equal(t, tc.expectedReportingValues["branch"], workspaceStatusMap["GIT_BRANCH"], tc.name)
		require.Equal(t, tc.expectedReportingValues["commit"], workspaceStatusMap["COMMIT_SHA"], tc.name)
	}
}

func TestRunAction_PushedAndTargetBranchAreEqual(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, initialCommitSHA := makeGitRepo(t, workspaceContentsWithRunScript)

	testCases := []struct {
		name      string
		useSha    bool
		repoFlags []string
	}{
		{
			name:   "With commit sha",
			useSha: true,
		},
		{
			name:   "Without commit sha",
			useSha: false,
		},
	}
	baselineRunnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Print args",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)

	for _, tc := range testCases {
		runnerFlags := baselineRunnerFlags
		if tc.useSha {
			runnerFlags = append(runnerFlags, "--commit_sha="+initialCommitSHA)
		}

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)
		assert.Contains(t, result.Output, "args: {{ Hello world }}", tc.name)
		assert.NotContains(t, result.Output, "git merge", tc.name)
	}
}

func TestEnvExpansion(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithEnvVars)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test env expansion",
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

	env := []string{
		"TEST_SECRET_1=test_secret_1_value",
		"TEST_SECRET_2=test_secret_2_value",
	}
	result := invokeRunner(t, runnerFlags, env, wsPath)

	checkRunnerResult(t, result)

	assert.Contains(t, result.Output, "env checks passed")
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
		"subdir/.bazelrc": `
# This role should take priority over the CI role.
build --build_metadata=ROLE=TEST
`,
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

	in := getRunnerInvocation(t, app, result)
	children := in.GetChildInvocations()
	require.Equal(t, 1, len(children))
	require.Equal(t, "TEST", children[0].GetRole())
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
	patchDigest, err := cachetools.UploadBlob(ctx, bsClient, "", repb.DigestFunction_SHA256, bytes.NewReader([]byte(patch)))
	require.NoError(t, err)

	// Execute a Bazel command with a patched `pass.sh` that should output 'EDIT'.
	{
		runAction := &config.Action{
			Name: "remote run",
			Steps: []*rnpb.Step{
				{Run: "bazel test --test_output=streamed --nocache_test_results //..."},
			},
		}
		actionBytes, err := yaml.Marshal(runAction)
		require.NoError(t, err)
		serializedAction := base64.StdEncoding.EncodeToString(actionBytes)

		runnerFlags := []string{
			"--pushed_repo_url=file://" + targetRepoPath,
			"--pushed_branch=master",
			"--target_repo_url=file://" + targetRepoPath,
			"--target_branch=master",
			"--cache_backend=" + app.GRPCAddress(),
			"--patch_uri=" + fmt.Sprintf("blobs/%s/%d", patchDigest.GetHash(), patchDigest.GetSizeBytes()),
			"--serialized_action=" + serializedAction,
			// Disable clean checkout fallback for this test since we expect to sync
			// without errors.
			"--fallback_to_clean_checkout=false",
		}
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)
		runnerInvocation := getRunnerInvocation(t, app, result)
		assert.Contains(t, runnerInvocation.ConsoleBuffer, "EDIT")

		if t.Failed() {
			t.Log(runnerInvocation.ConsoleBuffer)
		}
	}

	// Re-run Bazel without a patched `pass.sh` which should revert the previous change.
	{
		runAction := &config.Action{
			Name: "remote run",
			Steps: []*rnpb.Step{
				{Run: "bazel test --test_output=streamed --nocache_test_results //..."},
			},
		}
		actionBytes, err := yaml.Marshal(runAction)
		require.NoError(t, err)
		serializedAction := base64.StdEncoding.EncodeToString(actionBytes)

		runnerFlags := []string{
			"--pushed_repo_url=file://" + targetRepoPath,
			"--pushed_branch=master",
			"--target_repo_url=file://" + targetRepoPath,
			"--target_branch=master",
			"--serialized_action=" + serializedAction,
			// Disable clean checkout fallback for this test since we expect to sync
			// without errors.
			"--fallback_to_clean_checkout=false",
		}
		runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

		result := invokeRunner(t, runnerFlags, []string{}, wsPath)
		checkRunnerResult(t, result)
		runnerInvocation := getRunnerInvocation(t, app, result)
		assert.NotContains(t, runnerInvocation.ConsoleBuffer, "EDIT")

		if t.Failed() {
			t.Log(runnerInvocation.ConsoleBuffer)
		}
	}
}

func TestLocalEnvironmentalError(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithLocalEnvironmentalErrorAction)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Exit 36",
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

	result := invokeRunner(t, runnerFlags, nil, wsPath)

	require.Equal(t, syscall.SIGKILL, result.Signal, "runner process should have signaled its own PID with SIGKILL")
	runnerInvocation := getRunnerInvocation(t, app, result)
	require.NotEqual(
		t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS,
		runnerInvocation.GetInvocationStatus(),
		"runner invocation status not be COMPLETE_INVOCATION_STATUS")
}

func TestFailedGitSetup_StillPublishesBuildMetadata(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	_, headCommitSHA := makeGitRepo(t, workspaceContentsWithTestsAndBuildBuddyYAML)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=push",
		// Use an invalid repo path so that the git repo setup fails.
		"--pushed_repo_url=file://INVALID_REPO_PATH",
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://INVALID_REPO_PATH",
		"--target_branch=master",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, nil, wsPath)

	require.NotEqual(t, 0, result.ExitCode)
	runnerInvocation := getRunnerInvocation(t, app, result)

	require.Equal(
		t, "CI_RUNNER", runnerInvocation.GetRole(),
		"should publish workflow invocation metadata to BES despite failed repo setup")
}

func TestFetchFilters(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBazelVersionAction)
	app := buildbuddy.Run(t)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Show bazel version",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
		"--git_fetch_filters=blob:none",
	}
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)
}

func TestDisableBaseBranchMerging(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithExitScriptAndMergeDisabled)
	testshell.Run(t, repoPath, `
		# Create a PR branch
		git checkout -b pr-branch

		# Add a bad commit to the master branch;
		# this should not break our CI run on the PR branch which doesn't have
		# this change yet.
		git checkout master
		echo 'exit 1' > exit.sh
		git add .
		git commit -m "Fail"
	`)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=pull_request",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=pr-branch",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
	}
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	result := invokeRunner(t, runnerFlags, nil, wsPath)
	checkRunnerResult(t, result)
}

func TestFetchDepth1(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, initialCommitSHA := makeGitRepo(t, workspaceContentsWithGitLog)
	newCommitSha := testgit.CommitFiles(t, repoPath, map[string]string{"new_file.txt": "echo 1"})
	// Allow fetching non-HEAD commits
	testshell.Run(t, repoPath, "git config uploadpack.allowReachableSHA1InWant true")

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		// Need to set the fetch_depth flag even though it's set in the config,
		// since this is required in order to fetch the config.
		"--git_fetch_depth=1",
	}
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)

	// Should succeed if we set commit_sha to the HEAD commit
	flagsOnHead := append(runnerFlags, "--commit_sha="+newCommitSha)
	result := invokeRunner(t, flagsOnHead, nil, wsPath)
	checkRunnerResult(t, result)
	require.Contains(t, result.Output, "--depth=1")
	require.Contains(t, result.Output, newCommitSha)
	require.NotContains(t, result.Output, initialCommitSHA)

	// Should still succeed if we set commit_sha to a non-HEAD commit - it should
	// fetch that commit with --depth=1
	flagsOnOldCommit := append(runnerFlags, "--commit_sha="+initialCommitSHA)
	result = invokeRunner(t, flagsOnOldCommit, nil, wsPath)
	checkRunnerResult(t, result)
	require.Contains(t, result.Output, "--depth=1")
	require.NotContains(t, result.Output, newCommitSha)
	require.Contains(t, result.Output, initialCommitSHA)
}

func TestArtifactUploads_GRPCLog(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithArtifactUploads)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
	}
	// Start the app so the runner can use it as the BES+cache backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)
	runnerFlags = append(runnerFlags, "--cache_backend="+app.GRPCAddress())

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)

	runnerInvocation := getRunnerInvocation(t, app, result)

	var files []*bespb.File
	for _, tg := range runnerInvocation.GetTargetGroups() {
		for _, t := range tg.GetTargets() {
			files = append(files, t.GetFiles()...)
		}
	}

	bytestreamURI := files[0].GetUri()
	require.NotEmpty(t, bytestreamURI)
	fileName := files[0].GetName()
	require.Equal(t, "grpc.log", fileName)

	// Make sure that we can download the artifact and parse it as a gRPC log.
	downloadURL := fmt.Sprintf(
		"%s/file/download?invocation_id=%s&bytestream_url=%s",
		app.HTTPURL(),
		url.QueryEscape(runnerInvocation.GetInvocationId()),
		url.QueryEscape(bytestreamURI))
	res, err := http.Get(downloadURL)
	require.NoError(t, err)
	defer res.Body.Close()

	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		require.FailNowf(t, res.Status, "response body: %s", string(b))
	}

	br := bufio.NewReader(res.Body)
	m := &rlpb.LogEntry{}
	nParsed := 0
	for {
		err = protodelim.UnmarshalFrom(br, m)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		nParsed++
	}
	require.Greater(t, nParsed, 0, "expected to parse at least one grpc log message")

	// Run the action again. Note, the workflow bazel command runs a script
	// which asserts that there are no artifacts sticking around in the artifact
	// directory from the previous run.
	result = invokeRunner(t, runnerFlags, []string{}, wsPath)

	checkRunnerResult(t, result)
}

func TestArtifactUploads_JVMLog(t *testing.T) {
	workspaceSimulateOOM := map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD": `
sh_test(name = "simulate_oom", srcs = ["simulate_oom.sh"])
`,
		"simulate_oom.sh": `
output_base="$1"
echo "java.lang.OutOfMemoryError" > "$output_base/server/jvm.out"
exit 37
`,
		"buildbuddy.yaml": `
actions:
  - name: "Test"
    triggers:
      pull_request: { branches: [ master ] }
      push: { branches: [ master ] }
    steps:
      - run: |
          output_base=$(bazel info output_base)
          bazel run :simulate_oom "$output_base"
`,
	}

	wsPath := testfs.MakeTempDir(t)
	repoPath, headCommitSHA := makeGitRepo(t, workspaceSimulateOOM)

	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Test",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--commit_sha=" + headCommitSHA,
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
	}
	// Start the app so the runner can use it as the BES+cache backend.
	app := buildbuddy.Run(t)
	runnerFlags = append(runnerFlags, app.BESBazelFlags()...)
	runnerFlags = append(runnerFlags, "--cache_backend="+app.GRPCAddress())

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)

	require.Equal(t, 37, result.ExitCode, "bazel should have exited with code 37 due to OOM")

	runnerInvocation := getRunnerInvocation(t, app, result)

	var files []*bespb.File
	for _, tg := range runnerInvocation.GetTargetGroups() {
		for _, t := range tg.GetTargets() {
			files = append(files, t.GetFiles()...)
		}
	}

	bytestreamURI := files[0].GetUri()
	require.NotEmpty(t, bytestreamURI)
	fileName := files[0].GetName()
	require.Equal(t, "jvm.out", fileName)

	// Make sure that we can download the artifact.
	downloadURL := fmt.Sprintf(
		"%s/file/download?invocation_id=%s&bytestream_url=%s",
		app.HTTPURL(),
		url.QueryEscape(runnerInvocation.GetInvocationId()),
		url.QueryEscape(bytestreamURI))
	res, err := http.Get(downloadURL)
	require.NoError(t, err)
	defer res.Body.Close()

	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		require.FailNowf(t, res.Status, "response body: %s", string(b))
	}

	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(b), "java.lang.OutOfMemoryError")
}

func TestTimeout(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, _ := makeGitRepo(t, workspaceContentsWithRunScript)

	baselineRunnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Print args",
		"--trigger_event=push",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
		"--timeout=500ms",
	}
	// Start the app so the runner can use it as the BES backend.
	app := buildbuddy.Run(t)
	baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)

	runnerFlags := baselineRunnerFlags

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)
	// Expect runner to timeout and exit early
	require.NotEqual(t, 0, result.ExitCode)
	runnerInvocation := getRunnerInvocation(t, app, result)
	require.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, runnerInvocation.InvocationStatus)
	require.Contains(t, runnerInvocation.ConsoleBuffer, "Remote run exceeded timeout")
}

func TestBazelLock(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)
	repoPath, _ := makeGitRepo(t, map[string]string{
		"WORKSPACE":     "",
		"BUILD":         `sh_test(name = "sleep_test", srcs = ["sleep_test.sh"], tags = ["no-sandbox"])`,
		"sleep_test.sh": `touch "$TEST_STARTED" && sleep 99999999`,
		"buildbuddy.yaml": `
actions:
  - name: HogBazelLock
    steps:
      - run: |
          # Run bazel in the background so it hogs the workspace lock.
          bazel test :all --test_env=TEST_STARTED="$PWD/.test_started" &
          while ! [[ -e .test_started ]]; do sleep 0.01; done
`,
	})
	baselineRunnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=HogBazelLock",
		"--trigger_event=manual_dispatch",
		"--pushed_repo_url=file://" + repoPath,
		"--pushed_branch=master",
		"--target_repo_url=file://" + repoPath,
		"--target_branch=master",
	}
	app := buildbuddy.Run(t)
	baselineRunnerFlags = append(baselineRunnerFlags, app.BESBazelFlags()...)

	runnerFlags := baselineRunnerFlags

	result := invokeRunner(t, runnerFlags, []string{}, wsPath)
	assert.True(t, result.DoNotRecycle, "bazel should still hold workspace lock")
}

func TestRedactsLogs(t *testing.T) {
	wsPath := testfs.MakeTempDir(t)

	workspaceContentsWithBashCommands := map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"buildbuddy.yaml": `
actions:
  - name: "Echo secrets"
    steps:
      - run: |
          echo '--remote_header=x-buildbuddy-api-key=secret hello okay password@uri fine'
`,
	}
	repoPath, headCommitSHA := makeGitRepo(t, workspaceContentsWithBashCommands)
	runnerFlags := []string{
		"--workflow_id=test-workflow",
		"--action_name=Echo secrets",
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

	runnerInvocation := getRunnerInvocation(t, app, result)
	expectedStr := "--remote_header=<REDACTED> hello okay <REDACTED>@uri fine"
	assert.Contains(t, runnerInvocation.ConsoleBuffer, expectedStr)
}
