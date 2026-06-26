package remotebazel

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

// Used to mock logs streamed from the BuildBuddy server.
type scriptedBuildBuddyClient struct {
	bbspb.BuildBuddyServiceClient

	mu        sync.Mutex
	responses []*elpb.GetEventLogChunkResponse
	hooks     []func()
}

func (c *scriptedBuildBuddyClient) GetEventLogChunk(ctx context.Context, req *elpb.GetEventLogChunkRequest, opts ...grpc.CallOption) (*elpb.GetEventLogChunkResponse, error) {
	c.mu.Lock()
	if len(c.responses) == 0 {
		c.mu.Unlock()
		return &elpb.GetEventLogChunkResponse{}, nil
	}
	response := c.responses[0]
	c.responses = c.responses[1:]
	var hook func()
	if len(c.hooks) > 0 {
		hook = c.hooks[0]
		c.hooks = c.hooks[1:]
	}
	c.mu.Unlock()

	if hook != nil {
		hook()
	}
	return response, nil
}

func TestParseRemoteCliFlags(t *testing.T) {
	type testCase struct {
		name              string
		inputArgs         []string
		expectedOutput    []string
		expectedFlagValue map[string]string
		expectedError     bool
	}

	testCases := []testCase{
		{
			name: "one remote cli flag",
			inputArgs: []string{
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "one remote cli flag - space between val",
			inputArgs: []string{
				"--remote_runner",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "multiple remote cli flags",
			inputArgs: []string{
				"--remote_runner=val",
				"--os=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "repeated remote cli flags",
			inputArgs: []string{
				"--env=key=val",
				"--remote_runner=val",
				"--env=key2=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"env":           "key=val,key2=val2",
			},
		},
		{
			name: "no flags",
			inputArgs: []string{
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags",
			inputArgs: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags - space between value",
			inputArgs: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with cli flag",
			inputArgs: []string{
				"--os",
				"val2",
				"--output_base=val",
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with startup flag",
			inputArgs: []string{
				"--output_base=val",
				"--os",
				"val2",
				"--remote_runner=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name:              "empty",
			inputArgs:         []string{},
			expectedOutput:    []string{},
			expectedFlagValue: map[string]string{},
			expectedError:     true,
		},
		{
			name: "flags after the bazel command shouldn't be affected",
			inputArgs: []string{
				"--os",
				"val2",
				"build",
				"//...",
				"--os=untouched",
			},
			expectedOutput: []string{
				"build",
				"//...",
				"--os=untouched",
			},
			expectedFlagValue: map[string]string{
				"os": "val2",
			},
		},
	}
	for _, tc := range testCases {
		actualOutput, err := parseRemoteCliFlags(tc.inputArgs)
		if tc.expectedError {
			require.Error(t, err, tc.name)
		} else {
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedOutput, actualOutput, tc.name)
		}

		for flag, expectedVal := range tc.expectedFlagValue {
			actualVal := RemoteFlagset.Lookup(flag).Value
			require.Equal(t, expectedVal, actualVal.String(), tc.name)
		}
	}
}

func TestLiveLogUpdate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		previous   []string
		current    []string
		wantDelete int
		wantPrint  int
	}{
		{
			name:      "initial render",
			current:   []string{"setup", "progress"},
			wantPrint: 0,
		},
		{
			name:       "append stable lines",
			previous:   []string{"setup"},
			current:    []string{"setup", "progress"},
			wantDelete: 0,
			wantPrint:  1,
		},
		{
			name:       "redraw changed suffix",
			previous:   []string{"setup", "progress 1", "fetch 1"},
			current:    []string{"setup", "progress 2", "fetch 2"},
			wantDelete: 2,
			wantPrint:  1,
		},
		{
			name:       "redraw whole chunk if no prefix matches",
			previous:   []string{"old setup", "old progress"},
			current:    []string{"new setup", "new progress"},
			wantDelete: 2,
			wantPrint:  0,
		},
		{
			name:       "truncate stale live lines",
			previous:   []string{"setup", "progress", "stale"},
			current:    []string{"setup", "progress"},
			wantDelete: 1,
			wantPrint:  2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleteCount, printFrom := liveLogUpdate(tc.previous, tc.current)
			require.Equal(t, tc.wantDelete, deleteCount)
			require.Equal(t, tc.wantPrint, printFrom)
		})
	}
}

func TestStreamLogs_TerminalClearDoesNotReplayStableLogs(t *testing.T) {
	clearScreenBeforeSecondResponse := make(chan struct{})
	continueSecondResponse := make(chan struct{})
	client := &scriptedBuildBuddyClient{
		responses: []*elpb.GetEventLogChunkResponse{
			{
				Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			},
			{
				Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\n"),
				NextChunkId: "0001",
				Live:        true,
			},
			{
				Buffer: []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\nDone.\n"),
			},
		},
		hooks: []func(){
			nil,
			func() {
				close(clearScreenBeforeSecondResponse)
				<-continueSecondResponse
			},
		},
	}

	raw, rendered := runStreamLogsWithPTY(t, client, func(ptmx *os.File) {
		// Clear the terminal before the second log response is streamed.
		// Duplicate logs should not be reprinted.
		waitForSignal(t, clearScreenBeforeSecondResponse)
		_, err := os.Stdout.Write([]byte("\x1b[2J\x1b[H"))
		require.NoError(t, err)
		close(continueSecondResponse)
	})

	// On failure, dump the captured output.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("rendered logs:\n%s\x1b[0m", rendered)
		}
	})

	// Stable logs should not be duplicated.
	require.Equal(t, 1, strings.Count(rendered, "Analyzing: 2"))
	require.Equal(t, 1, strings.Count(rendered, "Done."))

	// Stale logs should not be printed.
	require.NotContains(t, rendered, "Analyzing: 1")

	// After the terminal was cleared, stable logs should not be reprinted.
	require.NotContains(t, rendered, "Applied patch cleanly.")

	// The raw output should contain the original logs before they were cleared.
	require.Equal(t, 1, strings.Count(raw, "Applied patch cleanly."))

}

func TestStreamLogs_TypedInputDoesNotCorruptOutput(t *testing.T) {
	typeInputBeforeSecondResponse := make(chan struct{})
	continueSecondResponse := make(chan struct{})
	client := &scriptedBuildBuddyClient{
		responses: []*elpb.GetEventLogChunkResponse{
			{
				Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			},
			{
				Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\n"),
				NextChunkId: "0001",
				Live:        true,
			},
			{
				Buffer: []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\nDone.\n"),
			},
		},
		hooks: []func(){
			nil,
			func() {
				close(typeInputBeforeSecondResponse)
				<-continueSecondResponse
			},
		},
	}

	_, rendered := runStreamLogsWithPTY(t, client, func(ptmx *os.File) {
		// Between the first and second log responses, type some input into the terminal.
		waitForSignal(t, typeInputBeforeSecondResponse)
		_, err := ptmx.Write([]byte("typed input\n"))
		require.NoError(t, err)
		// Close the channel to signal the test to continue.
		close(continueSecondResponse)
	})

	// On failure, dump the captured output.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("rendered logs:\n%s\x1b[0m", rendered)
		}
	})

	// Input should not be echoed into the terminal, to prevent corrupting log streaming.
	require.NotContains(t, rendered, "typed input")

	// No logs should be duplicated.
	require.Equal(t, 1, strings.Count(rendered, "Applied patch cleanly."))
	require.Equal(t, 1, strings.Count(rendered, "Setup completed."))
	require.Equal(t, 1, strings.Count(rendered, "Analyzing: 2"))
	require.Equal(t, 1, strings.Count(rendered, "Done."))
}

func TestGitConfig_BranchAndSha(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, originalMasterHeadCommit := testgit.MakeTempRepo(t, map[string]string{"hello.txt": "exit 0"})

	// Create a remote branch
	testshell.Run(t, remoteRepoPath, "git checkout -B remote_b")
	remoteBranchHeadCommit := testgit.CommitFiles(t, remoteRepoPath, map[string]string{"new_file.txt": "exit 0"})
	testshell.Run(t, remoteRepoPath, "git checkout master")

	type testCase struct {
		name string

		localBranchExistsRemotely bool
		localCommitExistsRemotely bool
		unpushedLocalCommit       bool

		expectedBranch  string
		expectedCommit  string
		expectedPatches []string
	}

	testCases := []testCase{
		{
			name:                      "Local branch and commit exist remotely",
			localBranchExistsRemotely: true,
			localCommitExistsRemotely: true,
			expectedBranch:            "remote_b",
			expectedCommit:            remoteBranchHeadCommit,
			expectedPatches:           []string{},
		},
		{
			name:                      "Local branch does not exist remotely",
			localBranchExistsRemotely: false,
			localCommitExistsRemotely: false,
			expectedBranch:            "master",
			expectedCommit:            originalMasterHeadCommit,
			expectedPatches:           []string{"local_file.txt"},
		},
		{
			name:                      "Local commit does not exist remotely",
			localBranchExistsRemotely: true,
			localCommitExistsRemotely: false,
			expectedBranch:            "master",
			expectedCommit:            originalMasterHeadCommit,
			expectedPatches:           []string{"local_file.txt"},
		},
		{
			name:                "On master with an unpushed commit",
			unpushedLocalCommit: true,
			expectedBranch:      "master",
			expectedCommit:      originalMasterHeadCommit,
			expectedPatches:     []string{"local_only_commited_file.txt"},
		},
	}

	for i, tc := range testCases {
		// Setup a "local" repo
		localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
		err := os.Chdir(localRepoPath)
		require.NoError(t, err, tc.name)
		resetRepoRootPathForTest(t)

		if tc.unpushedLocalCommit {
			testgit.CommitFiles(t, localRepoPath, map[string]string{"local_only_commited_file.txt": "exit 0"})
		} else if tc.localBranchExistsRemotely {
			testshell.Run(t, localRepoPath, "git checkout remote_b")
		} else {
			testshell.Run(t, localRepoPath, "git checkout -B local_only")

			// Simulate that the remote master is ahead of the local master
			testshell.Run(t, remoteRepoPath, "git checkout master")
			newFileName := fmt.Sprintf("new_file%d.txt", i)
			_ = testgit.CommitFiles(t, remoteRepoPath, map[string]string{newFileName: "exit 0"})
		}
		if !tc.localCommitExistsRemotely {
			testgit.CommitFiles(t, localRepoPath, map[string]string{"local_file.txt": "exit 0"})
		}

		config, err := Config()
		require.NoError(t, err, tc.name)

		require.Equal(t, tc.expectedBranch, config.Ref, tc.name)
		require.Equal(t, tc.expectedCommit, config.CommitSHA, tc.name)
		require.Equal(t, len(tc.expectedPatches), len(config.Patches), tc.name)
		if len(tc.expectedPatches) > 0 {
			require.Contains(t, string(config.Patches[0]), tc.expectedPatches[0], tc.name)
		}

		// Reset remote repo for future test cases
		testshell.Run(t, remoteRepoPath, "git checkout master && git clean -fdx && git reset --hard "+originalMasterHeadCommit)
	}
}

func TestGitConfig_FetchURL(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{"hello.txt": "exit 0"})
	remoteUrl := "file://" + remoteRepoPath

	testCases := []struct {
		name            string
		expectedURL     string
		multipleRemotes bool
		isRemoteCached  bool
	}{
		{
			name:        "One remote is configured",
			expectedURL: remoteUrl,
		},
		{
			name:            "Selected remote is cached",
			multipleRemotes: true,
			isRemoteCached:  true,
			expectedURL:     remoteUrl,
		},
	}

	for _, tc := range testCases {
		// Setup a "local" repo
		localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
		err := os.Chdir(localRepoPath)
		require.NoError(t, err, tc.name)
		resetRepoRootPathForTest(t)

		if tc.multipleRemotes {
			testshell.Run(t, localRepoPath, "git remote add extra "+remoteUrl)
		}
		if tc.isRemoteCached {
			testshell.Run(t, localRepoPath, fmt.Sprintf("git config --replace-all %s.%s extra", gitConfigSection, gitConfigRemoteBazelRemote))
		}

		config, err := Config()
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.expectedURL, config.URL)
	}
}

func TestGeneratingPatches(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{
		"hello.txt": "echo HI",
		"b.bin":     "",
	})

	// Setup a "local" repo
	localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
	// Remote bazel runs commands in the working directory, so make sure it
	// is set correctly
	err := os.Chdir(localRepoPath)
	require.NoError(t, err)
	resetRepoRootPathForTest(t)

	testshell.Run(t, localRepoPath, `
		# Generate a diff on a pre-existing file
		echo "echo HELLO" > hello.txt

		# Generate a diff for a new untracked file
		echo "echo BYE" > bye.txt

		# Generate a binary diff on a pre-existing file
		echo -ne '\x00\x01\x02\x03\x04' > b.bin

		# Generate a binary diff on an untracked file
		echo -ne '\x00\x01\x02\x03\x04' > b2.bin
`)

	config, err := Config()
	require.NoError(t, err)

	require.Equal(t, 4, len(config.Patches))
	for _, patchBytes := range config.Patches {
		p := string(patchBytes)
		if strings.Contains(p, "hello.txt") {
			require.Contains(t, p, "HELLO")
		} else if strings.Contains(p, "bye.txt") {
			require.Contains(t, p, "BYE")
		} else if strings.Contains(p, "b.bin") {
			require.Contains(t, p, "GIT binary patch")
		} else if strings.Contains(p, "b2.bin") {
			require.Contains(t, p, "GIT binary patch")
		} else {
			require.FailNowf(t, "unexpected patch %s", p)
		}
	}
}

func TestWorkingDirectory(t *testing.T) {
	rootDir := t.TempDir()
	repoRoot := filepath.Join(rootDir, "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repoRoot, "subdir", "nested"), 0755))

	testCases := []struct {
		name              string
		workspaceFilePath string
		expectedDir       string
		expectedError     string
	}{
		{
			name:              "Repo root workspace",
			workspaceFilePath: filepath.Join(repoRoot, "MODULE.bazel"),
			expectedDir:       "",
		},
		{
			name:              "Nested workspace",
			workspaceFilePath: filepath.Join(repoRoot, "subdir", "MODULE.bazel"),
			expectedDir:       "subdir",
		},
		{
			name:              "Deeply nested workspace",
			workspaceFilePath: filepath.Join(repoRoot, "subdir", "nested", "MODULE.bazel"),
			expectedDir:       filepath.Join("subdir", "nested"),
		},
		{
			name:              "Workspace outside repo root",
			workspaceFilePath: filepath.Join(rootDir, "outside", "MODULE.bazel"),
			expectedError:     "outside repo root",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := workingDirectory(repoRoot, tc.workspaceFilePath)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedDir, dir)
		})
	}
}

func resetRepoRootPathForTest(t *testing.T) {
	storage.RepoRootPath = sync.OnceValues(func() (string, error) {
		return os.Getwd()
	})
}

func TestParseArgs(t *testing.T) {
	t.Setenv("BUILDBUDDY_API_KEY", "test-api-key")

	bazelArgs, execArgs, err := parseArgs([]string{
		"--output_base", "/tmp/output_base",
		"test",
		"-c", "opt",
		"--config=remote_only",
		"--bes_backend=grpc://user-bes",
		"--remote_cache=grpc://user-cache",
		"--remote_header=x-custom=1",
		"//foo",
		"--",
		"--exec_arg",
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		// Startup flags should be preserved.
		"--output_base=/tmp/output_base",
		"test",
		// Bazel flags should be canonicalized.
		"--compilation_mode=opt",
		// Config flags should not be expanded and passed through to the remote runner as is.
		"--config=remote_only",
		// Remote headers should be preserved.
		"--remote_header=x-custom=1",
		// API key should be set.
		"--remote_header=x-buildbuddy-api-key=test-api-key",
		"//foo",
		// Remote flags should be removed and replaced by the CLI.
		"--config=buildbuddy_bes_backend",
		"--config=buildbuddy_bes_results_url",
		"--config=buildbuddy_remote_cache",
	}, bazelArgs)
	// Exec args should be preserved.
	require.Equal(t, []string{"--exec_arg"}, execArgs)
}

func TestGetRemoteRunnerTarget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		envValue   string
		flagArgs   []string
		wantRunner string
	}{
		{
			name:       "neither flag nor env set",
			wantRunner: login.DefaultApiTarget,
		},
		{
			name:       "env set, no flag",
			envValue:   "grpcs://env-runner.dev",
			wantRunner: "grpcs://env-runner.dev",
		},
		{
			name:       "flag takes precedence over env",
			envValue:   "grpcs://env-runner.dev",
			flagArgs:   []string{"--remote_runner=grpc://flag-runner.dev", "build", "//..."},
			wantRunner: "grpc://flag-runner.dev",
		},
		{
			name:       "flag set, no env",
			flagArgs:   []string{"--remote_runner=grpcs://flag-runner.dev", "build", "//..."},
			wantRunner: "grpcs://flag-runner.dev",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BUILDBUDDY_REMOTE_RUNNER", tc.envValue)

			// Reset the remoteRunner flag to its default before each subtest so
			// prior parses don't leak.
			_ = RemoteFlagset.Set("remote_runner", login.DefaultApiTarget)

			actual := getRemoteRunnerTarget(tc.flagArgs)
			require.Equal(t, tc.wantRunner, actual)
		})
	}
}

// Helper to run the streamLogs function with a test terminal.
// The first output is the exact output captured from the terminal, including ANSI escape sequences.
// The second output is the rendered output, with ANSI escape sequences removed.
func runStreamLogsWithPTY(t *testing.T, client bbspb.BuildBuddyServiceClient, whileRunning func(ptmx *os.File)) (string, string) {
	// This helper swaps the process-global os.Stdin/os.Stdout, so the test must
	// not run in parallel. t.Setenv makes the testing package panic if
	// t.Parallel() is ever called on this test (in either order), enforcing
	// non-parallel execution at runtime.
	t.Setenv("BB_REMOTEBAZEL_PTY_TEST", "1")

	ptmx, tty, err := pty.Open()
	require.NoError(t, err)
	defer ptmx.Close()

	require.NoError(t, pty.Setsize(ptmx, &pty.Winsize{Rows: 24, Cols: 80}))

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	os.Stdin = tty
	os.Stdout = tty
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	output := lockingbuffer.New()
	copyDone := make(chan struct{})
	go func() {
		defer close(copyDone)
		_, _ = io.Copy(output, ptmx)
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- streamLogs(context.Background(), client, "test-invocation-id")
	}()
	if whileRunning != nil {
		whileRunning(ptmx)
	}
	err = <-errCh
	require.NoError(t, err)

	_ = tty.Close()
	<-copyDone

	raw := output.String()
	screen, err := terminal.NewScreenWriter(math.MaxInt, 0)
	require.NoError(t, err)
	_, err = screen.Write([]byte(raw))
	require.NoError(t, err)
	rendered := screen.OutputAccumulator.String() + screen.Render()

	return raw, rendered
}

func waitForSignal(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for streamLogs test signal")
	}
}
