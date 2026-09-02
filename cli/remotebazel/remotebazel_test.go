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

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/creack/pty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

// Used to mock logs streamed from the BuildBuddy server.
type scriptedBuildBuddyClient struct {
	bbspb.BuildBuddyServiceClient

	mu sync.Mutex
	// Results returned from successive Recv calls, shared across all streams
	// the client returns; once exhausted, Recv returns io.EOF.
	script []scriptedRecv
	// ChunkId of each GetEventLog request: the initial request, plus the
	// chunk each reconnect resumed from.
	requestedChunkIDs []string
}

type scriptedRecv struct {
	rsp *elpb.GetEventLogChunkResponse
	err error
	// If set, hook runs before the result is returned.
	hook func()
}

func (c *scriptedBuildBuddyClient) GetEventLog(ctx context.Context, req *elpb.GetEventLogChunkRequest, opts ...grpc.CallOption) (bbspb.BuildBuddyService_GetEventLogClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestedChunkIDs = append(c.requestedChunkIDs, req.GetChunkId())
	return &scriptedEventLogStream{client: c}, nil
}

// scriptedEventLogStream returns the scripted results, then ends the stream
// with io.EOF like a real server-side stream would.
type scriptedEventLogStream struct {
	grpc.ClientStream

	client *scriptedBuildBuddyClient
}

func (s *scriptedEventLogStream) Recv() (*elpb.GetEventLogChunkResponse, error) {
	c := s.client
	c.mu.Lock()
	if len(c.script) == 0 {
		c.mu.Unlock()
		return nil, io.EOF
	}
	next := c.script[0]
	c.script = c.script[1:]
	c.mu.Unlock()

	if next.hook != nil {
		next.hook()
	}
	return next.rsp, next.err
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
		{
			name: "explicitly passing `bazel` should error",
			inputArgs: []string{
				"bazel",
				"build",
				"//...",
			},
			expectedError: true,
		},
		{
			name: "unexpected token before bazel command should error",
			inputArgs: []string{
				"random",
				"build",
				"//...",
			},
			expectedError: true,
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
	client := &scriptedBuildBuddyClient{
		script: []scriptedRecv{
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
			{
				rsp: &elpb.GetEventLogChunkResponse{
					Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\n"),
					NextChunkId: "0001",
					Live:        true,
				},
				// Clear the terminal, as a user might mid-build. Duplicate
				// logs should not be reprinted afterwards. The hook runs on
				// the drawing goroutine, so the clear lands after the first
				// response is drawn and before the second one is.
				hook: func() {
					_, _ = os.Stdout.Write([]byte("\x1b[2J\x1b[H"))
				},
			},
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer: []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\nDone.\n"),
			}},
		},
	}

	raw, rendered := runStreamLogsWithPTY(t, client, nil)

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
	var ptmx *os.File
	client := &scriptedBuildBuddyClient{
		script: []scriptedRecv{
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
			{
				rsp: &elpb.GetEventLogChunkResponse{
					Buffer:      []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\n"),
					NextChunkId: "0001",
					Live:        true,
				},
				// Type input into the terminal, as a user might mid-build.
				hook: func() {
					_, err := ptmx.Write([]byte("typed input\n"))
					if !assert.NoError(t, err) {
						return
					}
					// Read the line back to force the kernel's echo decision
					// to happen now, while echo is still disabled. Otherwise
					// it may process the input only after streamLogs restores
					// echo.
					readBack := make(chan string, 1)
					go func() {
						buf := make([]byte, 64)
						n, _ := os.Stdin.Read(buf)
						readBack <- string(buf[:n])
					}()
					select {
					case line := <-readBack:
						// The read should return the typed line, confirming
						// the line discipline consumed it rather than the read
						// returning early on some other input.
						assert.Equal(t, "typed input\n", line)
					case <-time.After(10 * time.Second):
						t.Error("timed out reading typed input back from the tty")
					}
				},
			},
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer: []byte("Applied patch cleanly.\nSetup completed.\nAnalyzing: 2\nDone.\n"),
			}},
		},
	}

	_, rendered := runStreamLogsWithPTY(t, client, func(p *os.File) {
		ptmx = p
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

func TestStreamLogs_ReconnectsAfterTransientStreamError(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		script: []scriptedRecv{
			// Serve a live chunk with some in-progress output.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Applied patch cleanly.\nAnalyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
			// Drop the stream with a retryable error, as when the app
			// restarts during a deploy. streamLogs should reconnect instead
			// of returning the error, which would cancel the remote run.
			{err: status.UnavailableError("connection reset by peer")},
			// After the reconnect, re-serve the live chunk from the start,
			// now with fresh progress.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Applied patch cleanly.\nAnalyzing: 2\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
			// Finalize the chunk and end the log.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer: []byte("Applied patch cleanly.\nAnalyzing: 2\nDone.\n"),
			}},
		},
	}

	_, rendered := runStreamLogsWithPTY(t, client, nil)

	// On failure, dump the captured output.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("rendered logs:\n%s\x1b[0m", rendered)
		}
	})

	// The reconnect should resume from the live chunk rather than restarting
	// the log from the beginning.
	require.Equal(t, []string{"", "0001"}, client.requestedChunkIDs)

	// The re-served chunk should be deduplicated against what was already
	// drawn: stable lines appear exactly once, and the stale progress line is
	// replaced by the fresh one.
	require.Equal(t, 1, strings.Count(rendered, "Applied patch cleanly."))
	require.Equal(t, 1, strings.Count(rendered, "Analyzing: 2"))
	require.Equal(t, 1, strings.Count(rendered, "Done."))
	require.NotContains(t, rendered, "Analyzing: 1")
}

func TestPrintLogs(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		script: []scriptedRecv{
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Analyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Analyzing: 2\nBuilding.\n"),
				NextChunkId: "0002",
			}},
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer: []byte("Done.\n"),
			}},
		},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.NoError(t, err)
	// Analyzing: 1 should not get printed because it was
	// a live chunk that was overwritten.
	require.Equal(t, "Analyzing: 2\nBuilding.\nDone.\n", out)
}

func TestPrintLogs_ReturnsStreamError(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		script: []scriptedRecv{
			// Serve a finalized chunk, which should be printed.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Analyzing: 1\n"),
				NextChunkId: "0001",
			}},
			// Fail the stream with a non-retryable error. printLogs should
			// return the error rather than treating it as a clean end of
			// the log.
			{err: status.NotFoundError("invocation not found")},
		},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.True(t, status.IsNotFoundError(err), "expected NotFound, got: %v", err)
	require.Equal(t, "Analyzing: 1\n", out)
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
		detachedHead              bool
		detachedHeadMoved         bool

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
		{
			name:            "Detached HEAD without additional commits",
			detachedHead:    true,
			expectedBranch:  "master",
			expectedCommit:  originalMasterHeadCommit,
			expectedPatches: []string{"local_file.txt"},
		},
		{
			name:              "Detached HEAD with additional commits",
			detachedHead:      true,
			detachedHeadMoved: true,
			expectedBranch:    "master",
			expectedCommit:    originalMasterHeadCommit,
			expectedPatches:   []string{"detached_file.txt"},
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

		if tc.detachedHead {
			testshell.Run(t, localRepoPath, "git checkout --detach")
			if tc.detachedHeadMoved {
				// A commit in a detached-head condition updates the `git branch` output from "detached at"
				// to "detached from".
				testgit.CommitFiles(t, localRepoPath, map[string]string{"detached_file.txt": "exit 0"})
			}
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
		"hello.txt":      "echo HI",
		"b.bin":          "",
		"deleted.bin":    "\x00\x01\x02\x03\x04",
		"attributed.md":  "v1",
		".gitattributes": "attributed.md binary\n",
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

		# Delete a pre-existing binary file
		rm deleted.bin

		# Diff a file git treats as binary by attribute, though its bytes are text
		echo "v2" > attributed.md
`)

	config, err := Config()
	require.NoError(t, err)

	all := ""
	for _, patchBytes := range config.Patches {
		all += string(patchBytes)
	}
	require.Contains(t, all, "HELLO")
	require.Contains(t, all, "BYE")
	// Every file git renders as binary needs the binary format, deletions and
	// attribute-marked files included.
	for _, binaryFile := range []string{"b.bin", "b2.bin", "deleted.bin", "attributed.md"} {
		require.Contains(t, all, binaryFile)
	}
	require.Equal(t, 4, strings.Count(all, "GIT binary patch"))

	// The runner applies the patchset; a binary patch without its full index line fails there.
	runnerRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
	for i, patchBytes := range config.Patches {
		patchPath := filepath.Join(t.TempDir(), fmt.Sprintf("%d.patch", i))
		require.NoError(t, os.WriteFile(patchPath, patchBytes, 0644))
		testshell.Run(t, runnerRepoPath, fmt.Sprintf("git apply %q", patchPath))
	}
	for _, file := range []string{"hello.txt", "bye.txt", "b.bin", "b2.bin", "attributed.md"} {
		want, err := os.ReadFile(filepath.Join(localRepoPath, file))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(runnerRepoPath, file))
		require.NoError(t, err)
		require.Equal(t, want, got, "%s should match the local working tree", file)
	}
	require.NoFileExists(t, filepath.Join(runnerRepoPath, "deleted.bin"))
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
		// Remote configs should be added immediately after the Bazel command.
		"--config=buildbuddy_remote_cache",
		"--config=buildbuddy_bes_results_url",
		"--config=buildbuddy_bes_backend",
		// Bazel flags should be canonicalized.
		"--compilation_mode=opt",
		// Config flags should not be expanded and passed through to the remote runner as is.
		"--config=remote_only",
		// Remote headers should be preserved.
		"--remote_header=x-custom=1",
		// API key should be set.
		"--remote_header=x-buildbuddy-api-key=test-api-key",
		"//foo",
	}, bazelArgs)
	// Exec args should be preserved.
	require.Equal(t, []string{"--exec_arg"}, execArgs)
}

func TestParseArgs_RunAddsRemoteArgsBeforeExecutableArgs(t *testing.T) {
	t.Setenv("BUILDBUDDY_API_KEY", "test-api-key")
	originalRunRemotely := *runRemotely
	*runRemotely = false
	t.Cleanup(func() { *runRemotely = originalRunRemotely })

	bazelArgs, execArgs, err := parseArgs([]string{
		"run",
		"--noremote_upload_local_results",
		"--remote_build_event_upload=minimal",
		"--script_path=/tmp/custom-run-script.sh",
		"@bazel-diff//cli:bazel-diff",
		"generate-hashes",
		"--",
		"--includeTargetType",
		"-w",
		".",
	})
	require.NoError(t, err)

	// Rejoin and re-split the args to ensure that they are still properly formatted.
	forwardedBazelArgs, forwardedExecArgs := arg.SplitExecutableArgs(
		arg.JoinExecutableArgs(bazelArgs, execArgs),
	)
	require.Equal(t, "run", arg.GetCommand(forwardedBazelArgs))
	require.Equal(t, []string{"@bazel-diff//cli:bazel-diff"}, arg.GetTargets(forwardedBazelArgs))
	require.ElementsMatch(t, []string{
		"buildbuddy_bes_backend",
		"buildbuddy_bes_results_url",
		"buildbuddy_remote_cache",
	}, arg.GetMulti(forwardedBazelArgs, "config"))
	require.Contains(t, forwardedBazelArgs, "--remote_upload_local_results")
	require.Equal(t, "minimal", arg.Get(forwardedBazelArgs, "remote_build_event_upload"))
	require.Equal(t,
		"$BUILDBUDDY_CI_RUNNER_ROOT_DIR/bazel-run-scripts/run.sh",
		arg.Get(forwardedBazelArgs, "script_path"),
	)
	require.Equal(t, []string{
		"generate-hashes",
		"--includeTargetType",
		"-w",
		".",
	}, forwardedExecArgs)
	require.Contains(t,
		quoteRemoteBazelArgs(bazelArgs),
		`--script_path="$BUILDBUDDY_CI_RUNNER_ROOT_DIR"/bazel-run-scripts/run.sh`,
	)
}

func TestEnvForLocalRun(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"RUNFILES_DIR=/old/runfiles",
		"RUNFILES_MANIFEST_FILE=/old/MANIFEST",
		"RUNFILES_MANIFEST_ONLY=1",
		"BUILD_WORKSPACE_DIRECTORY=/old/workspace",
		"BUILD_WORKING_DIRECTORY=/old/working-directory",
		"USER=test",
	}

	require.Equal(t, []string{
		"PATH=/usr/bin",
		"USER=test",
		"RUNFILES_DIR=/new/runfiles",
		"BUILD_WORKSPACE_DIRECTORY=/new/workspace",
		"BUILD_WORKING_DIRECTORY=/new/working-directory",
	}, envForLocalRun(env, "/new/runfiles", "/new/workspace", "/new/working-directory"))
}

func TestEnvForLocalRun_NoRunfiles(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"RUNFILES_DIR=/old/runfiles",
		"RUNFILES_MANIFEST_FILE=/old/MANIFEST",
		"BUILD_WORKSPACE_DIRECTORY=/old/workspace",
		"BUILD_WORKING_DIRECTORY=/old/working-directory",
		"USER=test",
	}

	require.Equal(t, []string{
		"PATH=/usr/bin",
		"USER=test",
		"BUILD_WORKSPACE_DIRECTORY=/new/workspace",
		"BUILD_WORKING_DIRECTORY=/new/working-directory",
	}, envForLocalRun(env, "", "/new/workspace", "/new/working-directory"))
}

func TestHasSupportingRunfiles(t *testing.T) {
	executablePath := "bazel-out/k8-fastbuild/bin/main.sh"
	executable := &bespb.Runfile{File: &bespb.File{Name: executablePath}}

	require.False(t, hasSupportingRunfiles([]*bespb.Runfile{executable}, nil, executablePath))
	require.True(t, hasSupportingRunfiles([]*bespb.Runfile{
		executable,
		{File: &bespb.File{Name: "bazel-out/k8-fastbuild/bin/main.sh.runfiles/_main/data.txt"}},
	}, nil, executablePath))
	require.True(t, hasSupportingRunfiles(
		[]*bespb.Runfile{executable},
		[]*bespb.Tree{{Name: "bazel-out/k8-fastbuild/bin/main.sh.runfiles/_main/data"}},
		executablePath,
	))
}

func TestQuoteRemoteBazelArgs_RunScriptEnvVarExpanded(t *testing.T) {
	// This flag should not be quoted with shlex.Quote, which explicitly prevents env var expansion.
	// The path should be quoted with double quotes, so the remote shell expands the BUILDBUDDY_CI_RUNNER_ROOT_DIR
	// env var.
	require.Equal(t,
		`--script_path="$BUILDBUDDY_CI_RUNNER_ROOT_DIR"/bazel-run-scripts/run.sh`,
		quoteRemoteBazelArgs([]string{runScriptPathFlag}),
	)
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
// The setup callback, if set, receives the pty master before log streaming
// starts; script hooks can capture it to interact with the terminal
// mid-stream.
// The first output is the exact output captured from the terminal, including ANSI escape sequences.
// The second output is the rendered output, with ANSI escape sequences removed.
func runStreamLogsWithPTY(t *testing.T, client bbspb.BuildBuddyServiceClient, setup func(ptmx *os.File)) (string, string) {
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

	if setup != nil {
		setup(ptmx)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- streamLogs(context.Background(), client, "test-invocation-id")
	}()
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

// Helper to run the printLogs function with os.Stdout captured, returning the
// captured output and the error returned by printLogs.
func runPrintLogsWithCapturedStdout(t *testing.T, client bbspb.BuildBuddyServiceClient) (string, error) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	printErr := printLogs(t.Context(), client, "test-invocation-id")

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out), printErr
}
