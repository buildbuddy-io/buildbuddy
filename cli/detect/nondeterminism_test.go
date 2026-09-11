package detect

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazel_command"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	npb "github.com/buildbuddy-io/buildbuddy/proto/notification"
	spawn_diff "github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestAddBazelFlags(t *testing.T) {
	args, err := addBazelFlags(
		bazelArgsForTest(t, "--bazelrc=/tmp/bazelrc", "test", "//foo:bar"),
		"/tmp/output-base",
		"/tmp/log.pb.zst",
		"test-invocation-id",
		"grpcs://bes.example.com",
		"https://example.com/invocation/",
	)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"--bazelrc=/tmp/bazelrc",
		"--output_base=/tmp/output-base",
		"test",
		"--bes_backend=grpcs://bes.example.com",
		"--bes_results_url=https://example.com/invocation/",
		"--noremote_accept_cached",
		"--repo_contents_cache=",
		"--disk_cache=",
		"--noexperimental_convenience_symlinks",
		"--execution_log_compact_file=/tmp/log.pb.zst",
		"--invocation_id=test-invocation-id",
		"//foo:bar",
	}, args)
}

func TestAddBazelFlags_DoesNotMutateBaseArgs(t *testing.T) {
	baseArgs := bazelArgsForTest(t, "build", "//foo:bar")

	_, err := addBazelFlags(baseArgs, "/tmp/output-base", "/tmp/log.pb.zst", "test-invocation-id", defaultBESBackend, defaultBESResultsURL)
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "//foo:bar"}, baseArgs.Forwarded())
}

func TestParseBazelCommand(t *testing.T) {
	bazelArgs, err := parseBazelCommand(`--bazelrc=/tmp/bazelrc test //foo:bar --test_output=errors`)
	require.NoError(t, err)
	assert.Equal(t, []string{"--bazelrc=/tmp/bazelrc", "test", "--test_output=errors", "//foo:bar"}, bazelArgs.Forwarded())

	_, err = parseBazelCommand("//foo:bar")
	require.Error(t, err)
}

func TestRunReturnsDetectionError(t *testing.T) {
	diff := &spawn_diff.DiffResult{SpawnDiffs: []*spawn_diff.SpawnDiff{nondeterministicSpawnDiff("//foo:bar")}}
	explainer := &fakeExplainer{diff: diff}
	runner := &fakeRunner{}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner:    runner,
		explainer: explainer,
	}

	err := c.Run(context.Background())
	require.ErrorIs(t, err, errNondeterminismDetected)

	require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
	assert.True(t, explainer.nondeterministicOnly, "detector should request only non-deterministic spawns")
	assert.Equal(t, 1, explainer.writeCalls)
	assert.Same(t, diff, explainer.wroteDiff)
}

// nondeterministicSpawnDiff returns a representative non-deterministic spawn
// diff, i.e. what explain.Diff returns for --nondeterministic_only: an exit code
// change despite unchanged inputs. The classification itself is tested in the
// cli/explain package; the detector just reports whatever explain.Diff surfaces.
func nondeterministicSpawnDiff(label string) *spawn_diff.SpawnDiff {
	return &spawn_diff.SpawnDiff{
		TargetLabel: label,
		Diff: &spawn_diff.SpawnDiff_Modified{Modified: &spawn_diff.Modified{
			Diffs: []*spawn_diff.Diff{{
				Diff: &spawn_diff.Diff_ExitCode{ExitCode: &spawn_diff.IntDiff{Old: 0, New: 1}},
			}},
		}},
	}
}

func TestRunReturnsNilWhenNoDiffs(t *testing.T) {
	explainer := &fakeExplainer{diff: &spawn_diff.DiffResult{}}
	runner := &fakeRunner{}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner:    runner,
		explainer: explainer,
	}

	require.NoError(t, c.Run(context.Background()))

	require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
	assert.Equal(t, 0, explainer.writeCalls)
}

func TestRemovesOutputBaseAfterEachRun(t *testing.T) {
	m, err := newBuildMetadata()
	require.NoError(t, err)
	defer os.RemoveAll(m.tempDir)

	var runner fakeRunner
	var buildRuns int
	runner.onRun = func(ctx context.Context, call commandCall) error {
		command, _ := bazel_command.GetCommandAndIndex(call.args)
		if command == "shutdown" {
			return nil
		}
		outputBase := outputBaseFromArgs(t, call.args)
		if command == "clean" {
			return os.RemoveAll(outputBase)
		}

		buildRuns++
		require.NoError(t, os.MkdirAll(filepath.Join(outputBase, "execroot"), 0755))
		if buildRuns == 2 {
			require.NoDirExists(t, filepath.Join(m.tempDir, "output_base_1"))
		}
		return nil
	}
	c := &checker{
		opts: options{
			bazelArgs:     bazelArgsForTest(t, "build", "//foo:bar"),
			besBackend:    defaultBESBackend,
			besResultsURL: defaultBESResultsURL,
		},
		runner: &runner,
	}

	require.NoError(t, c.runBuilds(context.Background(), m))

	require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
	require.NoDirExists(t, filepath.Join(m.tempDir, "output_base_1"))
	require.NoDirExists(t, filepath.Join(m.tempDir, "output_base_2"))
}

func bazelArgsForTest(t *testing.T, args ...string) *arg.BazelArgs {
	t.Helper()
	parsedArgs, err := arg.NewBazelArgsNoResolve(args)
	require.NoError(t, err)
	return parsedArgs
}

func outputBaseFromArgs(t *testing.T, args []string) string {
	t.Helper()
	for _, arg := range args {
		if value, ok := strings.CutPrefix(arg, "--output_base="); ok {
			return value
		}
	}
	require.FailNow(t, "missing --output_base arg", "args: %v", args)
	return ""
}

func bazelCommands(calls []commandCall) []string {
	var commands []string
	for _, call := range calls {
		command, _ := bazel_command.GetCommandAndIndex(call.args)
		commands = append(commands, command)
	}
	return commands
}

type commandCall struct {
	name string
	args []string
}

type fakeRunner struct {
	runs   []commandCall
	runErr error
	onRun  func(context.Context, commandCall) error
}

func (r *fakeRunner) Run(ctx context.Context, name string, args ...string) error {
	call := commandCall{name: name, args: append([]string(nil), args...)}
	r.runs = append(r.runs, call)
	if r.onRun != nil {
		if err := r.onRun(ctx, call); err != nil {
			return err
		}
	}
	return r.runErr
}

type fakeExplainer struct {
	diff                 *spawn_diff.DiffResult
	diffErr              error
	nondeterministicOnly bool
	writeCalls           int
	wroteDiff            *spawn_diff.DiffResult
}

func (e *fakeExplainer) Diff(oldLog, newLog string, nondeterministicOnly bool) (*spawn_diff.DiffResult, error) {
	e.nondeterministicOnly = nondeterministicOnly
	return e.diff, e.diffErr
}

func (e *fakeExplainer) WriteText(w io.Writer, diff *spawn_diff.DiffResult, verbose bool) {
	e.writeCalls++
	e.wroteDiff = diff
}

func setupEndpointWorkspace(t *testing.T, rc string) string {
	t.Helper()
	ws := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("BUILDBUDDY_CI_RUNNER_ROOT_DIR", "")
	require.NoError(t, os.WriteFile(filepath.Join(ws, "WORKSPACE"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(ws, ".bazelrc"), []byte(rc), 0644))
	workspace.SetForTest(t, ws)
	return ws
}

func endpointFlags(t *testing.T, args ...string) *flag.FlagSet {
	t.Helper()
	flags := flag.NewFlagSet("detect nondeterminism", flag.ContinueOnError)
	flags.String("bes_backend", defaultBESBackend, "")
	flags.String("bes_results_url", defaultBESResultsURL, "")
	require.NoError(t, arg.ParseFlagSet(flags, args))
	return flags
}

func TestResolveNondeterminismOptions(t *testing.T) {
	const prodRC = `
common --bes_backend=grpcs://prod.example.com
common --bes_results_url=https://prod.example.com/invocation/
`
	const devRC = `
common --bes_backend=grpcs://dev.example.com
common --bes_results_url=https://dev.example.com/invocation/
common:buildbuddy_bes_backend --bes_backend=grpcs://dev.example.com
common:buildbuddy_bes_results_url --bes_results_url=https://dev.example.com/invocation/
`
	const workflowRC = prodRC + `
common:linux-workflows --config=workflows
common:workflows --config=buildbuddy_bes_backend
common:workflows --config=buildbuddy_bes_results_url
`
	for _, tc := range []struct {
		name          string
		workspaceRC   string
		noWorkspaceRC bool
		runnerRC      string
		explicitRC    string
		startupArgs   []string
		commandArgs   []string
		detectorFlags []string
		wantBackend   string
		wantURL       string
		wantError     string
	}{
		{
			name: "defaults", wantBackend: defaultBESBackend, wantURL: defaultBESResultsURL,
		},
		{
			name: "workspace", workspaceRC: prodRC,
			wantBackend: "grpcs://prod.example.com", wantURL: "https://prod.example.com/invocation/",
		},
		{
			name: "independent defaults", workspaceRC: "common --bes_backend=grpcs://dev.example.com\n",
			wantBackend: "grpcs://dev.example.com", wantURL: defaultBESResultsURL,
		},
		{
			name: "runner defaults", runnerRC: devRC,
			wantBackend: "grpcs://dev.example.com", wantURL: "https://dev.example.com/invocation/",
		},
		{
			name: "runner without workspace rc", runnerRC: devRC, noWorkspaceRC: true,
			wantBackend: "grpcs://dev.example.com", wantURL: "https://dev.example.com/invocation/",
		},
		{
			name: "imported rc", workspaceRC: "import %workspace%/custom.bazelrc\n", explicitRC: devRC,
			wantBackend: "grpcs://dev.example.com", wantURL: "https://dev.example.com/invocation/",
		},
		{
			name: "workspace overrides runner defaults", runnerRC: devRC, workspaceRC: prodRC,
			wantBackend: "grpcs://prod.example.com", wantURL: "https://prod.example.com/invocation/",
		},
		{
			name: "workflow named configs override workspace defaults", runnerRC: devRC, workspaceRC: workflowRC,
			commandArgs: []string{"--config=linux-workflows"},
			wantBackend: "grpcs://dev.example.com", wantURL: "https://dev.example.com/invocation/",
		},
		{
			name: "explicit rc overrides runner and workspace", runnerRC: devRC, workspaceRC: prodRC,
			explicitRC:  "common --bes_backend=grpcs://custom.example.com\ncommon --bes_results_url=https://custom.example.com/\n",
			wantBackend: "grpcs://custom.example.com", wantURL: "https://custom.example.com/",
		},
		{
			name: "command overrides config", runnerRC: devRC, workspaceRC: workflowRC,
			commandArgs: []string{"--config=linux-workflows", "--bes_backend=grpcs://custom.example.com", "--bes_results_url", "https://custom.example.com/"},
			wantBackend: "grpcs://custom.example.com", wantURL: "https://custom.example.com/",
		},
		{
			name: "config after command flag wins", runnerRC: devRC, workspaceRC: workflowRC,
			commandArgs: []string{"--bes_backend=grpcs://custom.example.com", "--config=linux-workflows"},
			wantBackend: "grpcs://dev.example.com", wantURL: "https://dev.example.com/invocation/",
		},
		{
			name: "detector overrides command", workspaceRC: prodRC,
			commandArgs:   []string{"--bes_backend=grpcs://command.example.com", "--bes_results_url=https://command.example.com/"},
			detectorFlags: []string{"--bes_backend=" + defaultBESBackend, "--bes_results_url=" + defaultBESResultsURL},
			wantBackend:   defaultBESBackend, wantURL: defaultBESResultsURL,
		},
		{
			name: "override only one endpoint", workspaceRC: prodRC,
			detectorFlags: []string{"--bes_backend=grpcs://custom.example.com"},
			wantBackend:   "grpcs://custom.example.com", wantURL: "https://prod.example.com/invocation/",
		},
		{
			name: "empty rc values preserved", workspaceRC: "common --bes_backend=\ncommon --bes_results_url=\n",
		},
		{
			name: "empty command values preserved", workspaceRC: prodRC,
			commandArgs: []string{"--bes_backend=", "--bes_results_url="},
		},
		{
			name: "empty detector values preserved", workspaceRC: prodRC,
			detectorFlags: []string{"--bes_backend=", "--bes_results_url="},
		},
		{
			name: "ignore rc files includes runner", runnerRC: devRC, workspaceRC: prodRC,
			startupArgs: []string{"--ignore_all_rc_files"},
			wantBackend: defaultBESBackend, wantURL: defaultBESResultsURL,
		},
		{
			name: "undefined config is an error", commandArgs: []string{"--config=missing"},
			wantError: "not defined",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := setupEndpointWorkspace(t, tc.workspaceRC)
			if tc.noWorkspaceRC {
				require.NoError(t, os.Remove(filepath.Join(ws, ".bazelrc")))
			}
			if tc.runnerRC != "" {
				root := t.TempDir()
				t.Setenv("BUILDBUDDY_CI_RUNNER_ROOT_DIR", root)
				require.NoError(t, os.WriteFile(filepath.Join(root, "buildbuddy.bazelrc"), []byte(tc.runnerRC), 0644))
			}
			args := append([]string{"--nosystem_rc", "--nohome_rc"}, tc.startupArgs...)
			if tc.explicitRC != "" {
				path := filepath.Join(ws, "custom.bazelrc")
				require.NoError(t, os.WriteFile(path, []byte(tc.explicitRC), 0644))
				args = append(args, "--bazelrc="+path)
			}
			args = append(args, "build", "//foo:bar")
			args = append(args, tc.commandArgs...)
			base := bazelArgsForTest(t, args...)
			original := base.Forwarded()
			opts, err := resolveNondeterminismOptions(base, endpointFlags(t, tc.detectorFlags...))
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantBackend, opts.besBackend)
			assert.Equal(t, tc.wantURL, opts.besResultsURL)
			assert.Same(t, base, opts.bazelArgs)
			assert.Equal(t, original, base.Forwarded())

			m, err := newBuildMetadata()
			require.NoError(t, err)
			t.Cleanup(func() { os.RemoveAll(m.tempDir) })
			runner := &fakeRunner{}
			c := &checker{opts: opts, runner: runner}
			require.NoError(t, c.runBuilds(context.Background(), m))
			require.Equal(t, []string{"build", "shutdown", "clean", "build", "shutdown", "clean"}, bazelCommands(runner.runs))
			for _, call := range []commandCall{runner.runs[0], runner.runs[3]} {
				assert.Equal(t, tc.wantBackend, arg.Get(call.args, "bes_backend"))
				assert.Equal(t, tc.wantURL, arg.Get(call.args, "bes_results_url"))
			}
			assert.Equal(t, original, base.Forwarded())
		})
	}
}

func TestResolveNondeterminismOptions_IgnoresExecutableArgs(t *testing.T) {
	setupEndpointWorkspace(t, "")
	base := bazelArgsForTest(t, "--ignore_all_rc_files", "run", "//foo:bar", "--", "--bes_backend=executable", "--bes_results_url=executable")
	opts, err := resolveNondeterminismOptions(base, endpointFlags(t))
	require.NoError(t, err)
	assert.Equal(t, defaultBESBackend, opts.besBackend)
	assert.Equal(t, defaultBESResultsURL, opts.besResultsURL)
}

type notificationRequest struct {
	request *npb.SendNotificationRequest
	apiKeys []string
}

type fakeNotificationServer struct {
	bbspb.BuildBuddyServiceServer
	requests chan notificationRequest
}

func (s *fakeNotificationServer) SendNotification(ctx context.Context, req *npb.SendNotificationRequest) (*npb.SendNotificationResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.requests <- notificationRequest{request: req, apiKeys: md.Get("x-buildbuddy-api-key")}
	return &npb.SendNotificationResponse{}, nil
}

func TestNotifyNondeterminism_UsesResolvedBackend(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	server := grpc.NewServer()
	notifications := &fakeNotificationServer{requests: make(chan notificationRequest, 1)}
	bbspb.RegisterBuildBuddyServiceServer(server, notifications)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	setupEndpointWorkspace(t, fmt.Sprintf("common --bes_backend=grpc://%s\n", listener.Addr()))
	t.Setenv("BB_NOTIFY_API_KEY", "notification-key")
	t.Setenv(WorkflowInvocationIDEnvVar, "workflow-id")
	oldEmail, oldSlack := *notifyEmail, *notifySlack
	*notifyEmail, *notifySlack = true, "slack-secret"
	t.Cleanup(func() { *notifyEmail, *notifySlack = oldEmail, oldSlack })
	opts, err := resolveNondeterminismOptions(bazelArgsForTest(t, "--nosystem_rc", "--nohome_rc", "build", "//foo:bar"), endpointFlags(t))
	require.NoError(t, err)
	c := &checker{opts: opts}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.notifyNondeterminism(ctx, &buildMetadata{oldInvocationID: "old-id", newInvocationID: "new-id"}))
	received := <-notifications.requests
	assert.Equal(t, []string{"notification-key"}, received.apiKeys)
	assert.Equal(t, []string{"old-id", "new-id"}, received.request.GetNondeterminismDetected().GetBuildInvocationIds())
	assert.Equal(t, "workflow-id", received.request.GetNondeterminismDetected().GetParentInvocationId())
	require.Len(t, received.request.GetChannels(), 2)
	assert.NotNil(t, received.request.GetChannels()[0].GetEmail())
	assert.Equal(t, "slack-secret", received.request.GetChannels()[1].GetSlack().GetWebhookUrlSecretName())
}
