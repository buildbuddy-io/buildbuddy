package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/bes_artifacts"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"github.com/docker/go-units"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/logrusorgru/aurora"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v2"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	backendLog "github.com/buildbuddy-io/buildbuddy/server/util/log"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

const (
	// buildbuddyBazelrcPath is the path where we write a bazelrc file to be
	// applied to all bazel commands. The path is relative to the runner workspace
	// root, which notably is not the same as the bazel workspace / git repo root.
	//
	// This bazelrc takes lower precedence than the workspace .bazelrc.
	buildbuddyBazelrcPath = "buildbuddy.bazelrc"

	// Name of the dir into which the repo is cloned.
	repoDirName = "repo-root"
	// Name of the bazel output base dir. This is written under the workspace
	// so that it can be cleaned up when the workspace is cleaned up.
	outputBaseDirName = "output-base"
	// Name of the dir where we write bazel run scripts.
	runScriptDirName = "bazel-run-scripts"

	// Fraction of disk space that must be in use before we attempt to reclaim
	// disk space.
	highDiskUsageThreshold = 0.9

	// Name of the dir where artifacts can be written.
	// The CI runner provisions subdirectories under this directory, one per
	// bazel command, e.g. artifacts/command-0/, artifacts/command-1/, etc.
	// The absolute path to the numbered directory is exposed to each Bazel
	// command as BUILDBUDDY_ARTIFACTS_DIRECTORY.
	// Bazel commands can reference this like:
	//     bazel build --experimental_remote_grpc_log_file=$BUILDBUDDY_ARTIFACTS_DIRECTORY/grpc.log
	// After each Bazel command, the CI runner will scan for artifacts under
	// this directory and upload all artifacts to cache, and report all uploads
	// as NamedSetOfFiles in the workflow build event stream.
	artifactsDirName = "artifacts"

	// Name of the env var exposed to bazel commands containing the path where
	// files can be written to have them associated with the workflow BES
	// stream.
	artifactsDirEnvVarName = "BUILDBUDDY_ARTIFACTS_DIRECTORY"

	defaultGitRemoteName = "origin"
	forkGitRemoteName    = "fork"

	// If smart fetch depth is enabled, the runner will try to fetch the minimum
	// depth required.
	smartFetchDepth = -1

	// Env vars set by workflow runner
	// NOTE: These env vars are not populated for non-private repos.

	buildbuddyAPIKeyEnvVarName = "BUILDBUDDY_API_KEY"
	repoUserEnvVarName         = "REPO_USER"
	repoTokenEnvVarName        = "REPO_TOKEN"

	// Exit code placeholder used when a command doesn't return an exit code on its own.
	noExitCode         = -1
	failedExitCodeName = "Failed"

	// progressFlushInterval specifies how often we should flush
	// each Bazel command's output while it is running.
	progressFlushInterval = 200 * time.Millisecond
	// progressFlushThresholdBytes specifies how full the log buffer
	// should be before we force a flush, regardless of the flush interval.
	progressFlushThresholdBytes = 1_000

	// Bazel binary constants

	bazelBinaryName    = "bazel"
	bazeliskBinaryName = "bazelisk"

	// Bazel exit codes
	// https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/ExitCode.java

	bazelOOMErrorExitCode                = 33
	bazelLocalEnvironmentalErrorExitCode = 36
	bazelInternalErrorExitCode           = 37

	// ANSI codes for cases where the aurora equivalent is not supported by our UI
	// (ex: aurora's "grayscale" mode results in some ANSI codes that we don't currently
	// parse correctly).

	ansiGray  = "\033[90m"
	ansiReset = "\033[0m"

	clientIdentityEnvVar = "BB_GRPC_CLIENT_IDENTITY"
)

var (
	// Subcommands of the ci_runner.
	// Go binaries are relatively large, so these are included as subcommands instead
	// of separate scripts.
	credentialHelper = flag.Bool("credential_helper", false, "Run in git credential helper mode. For internal usage only.")

	// In order to ensure bazel commands point to the correct env (Ex. if the remote
	// run was triggered in dev, it should point to the dev app), set --config=buildbuddy_bes_backend,
	// --config=buildbuddy_bes_results_url --config=buildbuddy_remote_cache, and/or --config=buildbuddy_remote_executor.
	// These configs are defined in a .bazelrc the ci_runner creates.
	besBackend    = flag.String("bes_backend", "", "gRPC endpoint for BuildBuddy's BES backend.")
	cacheBackend  = flag.String("cache_backend", "", "gRPC endpoint for BuildBuddy Cache.")
	rbeBackend    = flag.String("rbe_backend", "", "gRPC endpoint for BuildBuddy RBE.")
	besResultsURL = flag.String("bes_results_url", "", "URL prefix for BuildBuddy invocation URLs.")

	remoteInstanceName = flag.String("remote_instance_name", "", "Remote instance name used to retrieve patches (for hosted bazel) or the remote instance name running the workflow action.")
	workflowID         = flag.String("workflow_id", "", "ID of the workflow associated with this CI run.")
	actionName         = flag.String("action_name", "", "If set, run the specified action and *only* that action, ignoring trigger conditions.")
	serializedAction   = flag.String("serialized_action", "", "If set, run this b64+yaml encoded action, ignoring trigger conditions.")
	invocationID       = flag.String("invocation_id", "", "If set, use the specified invocation ID for the workflow action. Ignored if action_name is not set.")
	visibility         = flag.String("visibility", "", "If set, use the specified value for VISIBILITY build metadata for the workflow invocation.")
	timeout            = flag.Duration("timeout", 0, "Timeout before all commands will be canceled automatically.")

	// Flags to configure setting up git repo
	triggerEvent    = flag.String("trigger_event", "", "Event type that triggered the action runner.")
	pushedRepoURL   = flag.String("pushed_repo_url", "", "URL of the pushed repo. This is required.")
	pushedBranch    = flag.String("pushed_branch", "", "Branch name of the commit to be checked out.")
	commitSHA       = flag.String("commit_sha", "", "Commit SHA to report statuses for.")
	prNumber        = flag.Int64("pull_request_number", 0, "PR number, if applicable (0 if not triggered by a PR).")
	patchURIs       = flag.Slice("patch_uri", []string{}, "URIs of patches to apply to the repo after checkout. Can be specified multiple times to apply multiple patches.")
	gitCleanExclude = flag.Slice("git_clean_exclude", []string{}, "Directories to exclude from `git clean` while setting up the repo.")
	gitFetchFilters = flag.Slice("git_fetch_filters", []string{}, "Filters to apply to `git fetch` commands.")
	gitFetchDepth   = flag.Int("git_fetch_depth", smartFetchDepth, "Depth to use for `git fetch` commands.")
	// Flags to configure merge-with-base behavior
	targetRepoURL = flag.String("target_repo_url", "", "If different from pushed_repo_url, indicates a fork (`pushed_repo_url`) is being merged into this repo.")
	targetBranch  = flag.String("target_branch", "", "If different from pushed_branch, pushed_branch should be merged into this branch in the target repo.")

	shutdownAndExit = flag.Bool("shutdown_and_exit", false, "If set, runs bazel shutdown with the configured bazel_command, and exits. No other commands are run.")

	bazelCommand      = flag.String("bazel_command", "", "Bazel command to use.")
	bazelStartupFlags = flag.String("bazel_startup_flags", "", "Startup flags to pass to bazel. The value can include spaces and will be properly tokenized.")
	extraBazelArgs    = flag.String("extra_bazel_args", "", "Extra flags to pass to the bazel command. The value can include spaces and will be properly tokenized.")
	debug             = flag.Bool("debug", false, "Print additional debug information in the action logs.")

	ptyRows = flag.Int("pty_rows", 20, "Terminal height, in rows")
	ptyCols = flag.Int("pty_cols", 114, "Terminal width, in columns")

	// These command line options are used in the UI, even if they aren't used
	// directly by this binary.
	digestFunction = flag.String("digest_function", repb.DigestFunction_BLAKE3.String(), "The digest function used for the ci_runner execution.")

	// Test-only flags
	fallbackToCleanCheckout = flag.Bool("fallback_to_clean_checkout", true, "Fallback to cloning the repo from scratch if sync fails (for testing purposes only).")

	shellCharsRequiringQuote = regexp.MustCompile(`[^\w@%+=:,./-]`)
	invocationIDRegex        = regexp.MustCompile(`Streaming build results to:\s+.*?/invocation/([a-f0-9-]+)`)
)

type workspace struct {
	// Whether the workspace setup phase duration and logs were reported as part
	// of any action's logs yet.
	reportedInitMetrics bool

	// The root dir under which all work is done.
	//
	// This dir has the following structure:
	//     {rootDir}/
	//         bazelisk             (copy of embedded bazelisk binary)
	//         buildbuddy.bazelrc   (BuildBuddy-controlled bazelrc that applies to all bazel commands)
	//         artifacts/
	//             command-0/       (artifacts for bazel_commands[0])
	//             command-1/       (artifacts for bazel_commands[1])
	//             ...
	//         output-base/         (bazel output base)
	//         repo-root/           (cloned git repo)
	//             .git/
	//             WORKSPACE
	//             buildbuddy.yaml  (optional workflow config)
	//             ...
	//         bazel-run-scripts/   (generated run scripts for targets that were
	//                              built remotely, but intended to run locally
	//                              on the client's machine)
	//
	// The CI runner stays in the rootDir while setting up the repo, and then
	// changes to the "repo-root" dir just before executing any actions.
	//
	// For nested bazel workspaces, the CI runner will explicitly set `cmd.Dir`
	// on the spawned bazel subprocess to match the `bazel_workspace_dir` that
	// specified in the action config, but the CI runner itself will stay in
	// "repo-root".
	rootDir string

	// The machine's hostname.
	hostname string

	// The operating user's username.
	username string

	// The buildbuddy API key, or "" if none was found.
	buildbuddyAPIKey string

	// An invocation ID that should be forced, or "" if any is allowed.
	forcedInvocationID string

	// A unique ID for each ci_runner run.
	// If the ci_runner execution is retried, it should have the same
	// invocation ID, but a different run ID.
	runID string

	// An error that occurred while setting up the workspace, which should be
	// reported for all action logs instead of actually executing the action.
	setupError error

	// The start time of the setup phase.
	startTime time.Time

	// log contains logs from the workspace setup phase (cloning the git repo and
	// deciding which actions to run), which are reported as part of the first
	// action's logs.
	log *buildEventReporter
}

func artifactsRootPath(ws *workspace) string {
	return filepath.Join(ws.rootDir, artifactsDirName)
}

func artifactsPathForCommand(ws *workspace, bazelCommandIndex int) string {
	return filepath.Join(artifactsRootPath(ws), fmt.Sprintf("command-%d", bazelCommandIndex))
}

func provisionArtifactsDir(ws *workspace, bazelCommandIndex int) error {
	path := artifactsPathForCommand(ws, bazelCommandIndex)
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	if err := os.Setenv(artifactsDirEnvVarName, path); err != nil {
		return err
	}
	return nil
}

type buildEventReporter struct {
	isWorkflow bool
	apiKey     string
	bep        *build_event_publisher.Publisher
	uploader   *bes_artifacts.Uploader
	log        *invocationLog

	invocationID          string
	startTime             time.Time
	cancelBackgroundFlush func()

	// Child invocations detected by scanning the build logs
	childInvocations []string

	mu            sync.Mutex // protects(progressCount)
	progressCount int32
}

func newBuildEventReporter(ctx context.Context, besBackend string, apiKey string, forcedInvocationID string, isWorkflow bool) (*buildEventReporter, error) {
	iid := forcedInvocationID
	if iid == "" {
		var err error
		iid, err = newUUID()
		if err != nil {
			return nil, err
		}
	}

	bep, err := build_event_publisher.New(besBackend, apiKey, iid)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to initialize build event publisher: %s", err)
	}
	bep.Start(ctx)

	var uploader *bes_artifacts.Uploader
	if *cacheBackend != "" {
		ul, err := bes_artifacts.NewUploader(ctx, bep, *cacheBackend, *remoteInstanceName)
		if err != nil {
			return nil, status.UnavailableErrorf("failed to initialize BES artifact uploader: %s", err)
		}
		uploader = ul
	}

	return &buildEventReporter{apiKey: apiKey, bep: bep, uploader: uploader, log: newInvocationLog(), invocationID: iid, isWorkflow: isWorkflow, childInvocations: []string{}}, nil
}

func (r *buildEventReporter) InvocationID() string {
	return r.invocationID
}

func (r *buildEventReporter) Publish(event *bespb.BuildEvent) error {
	return r.bep.Publish(event)
}

type parsedBazelArgs struct {
	cmd      string
	flags    []string
	patterns []string
}

func parseBazelArgs(cmd string) (*parsedBazelArgs, error) {
	args, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}
	if len(args) < 1 {
		return nil, status.FailedPreconditionError("missing command")
	}
	parsedArgs := &parsedBazelArgs{
		cmd: args[0],
	}
	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--") {
			parsedArgs.flags = append(parsedArgs.flags, arg)
		} else {
			parsedArgs.patterns = append(parsedArgs.patterns, arg)
		}
	}
	return parsedArgs, nil
}

func (r *buildEventReporter) Start(startTime time.Time) error {
	if !r.startTime.IsZero() {
		// Already started.
		return nil
	}
	r.startTime = startTime

	options := []string{}
	if *besBackend != "" {
		options = append(options, fmt.Sprintf("--bes_backend='%s'", *besBackend))
	}
	if r.apiKey != "" {
		options = append(options, fmt.Sprintf("--remote_header='x-buildbuddy-api-key=%s'", r.apiKey))
	}

	optionsDescription := strings.Join(options, " ")
	cmd := ""

	// Set the `command` for the outer invocation
	patterns := []string{}
	if r.isWorkflow {
		cmd = "workflow run"
	} else {
		action, err := getActionToRun()
		if err != nil {
			return err
		}

		cmd = action.Name
	}

	startedEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
			{Id: &bespb.BuildEventId_ChildInvocationsConfigured{ChildInvocationsConfigured: &bespb.BuildEventId_ChildInvocationsConfiguredId{}}},
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 0}}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
			{Id: &bespb.BuildEventId_StructuredCommandLine{StructuredCommandLine: &bespb.BuildEventId_StructuredCommandLineId{CommandLineLabel: "original"}}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
			Uuid:               r.invocationID,
			StartTime:          timestamppb.New(startTime),
			OptionsDescription: optionsDescription,
			Command:            cmd,
		}},
	}
	if r.isWorkflow {
		startedEvent.Children = append(startedEvent.Children, &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowConfigured{WorkflowConfigured: &bespb.BuildEventId_WorkflowConfiguredId{}}})
		r.Printf("Streaming workflow logs to: %s", invocationURL(*invocationID))
	} else {
		startedEvent.Children = append(startedEvent.Children, &bespb.BuildEventId{Id: &bespb.BuildEventId_Pattern{Pattern: &bespb.BuildEventId_PatternExpandedId{Pattern: patterns}}})
		r.Printf("Streaming remote runner logs to: %s", invocationURL(*invocationID))
	}
	if err := r.bep.Publish(startedEvent); err != nil {
		return err
	}

	if len(patterns) > 0 {
		patternEvent := &bespb.BuildEvent{
			Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_Pattern{Pattern: &bespb.BuildEventId_PatternExpandedId{Pattern: patterns}}},
			Payload: &bespb.BuildEvent_Expanded{Expanded: &bespb.PatternExpanded{}},
		}
		if err := r.bep.Publish(patternEvent); err != nil {
			return err
		}
	}

	structuredCommandLineEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_StructuredCommandLine{StructuredCommandLine: &bespb.BuildEventId_StructuredCommandLineId{CommandLineLabel: "original"}}},
		Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: getStructuredCommandLine()},
	}
	if err := r.bep.Publish(structuredCommandLineEvent); err != nil {
		return err
	}

	r.log.writeListener = func(s string) {
		r.emitBuildEventsForBazelCommands(s)
		// Flush whenever the log buffer fills past a certain threshold.
		if size := r.log.Len(); size >= progressFlushThresholdBytes {
			r.FlushProgress() // ignore error; it will surface in `bep.Finish()`
		}
	}

	stopFlushingProgress := r.startBackgroundProgressFlush()
	r.cancelBackgroundFlush = stopFlushingProgress
	return nil
}

func (r *buildEventReporter) PublishFinishedEvent(exitCode int, exitCodeName string) {
	now := time.Now()
	_ = r.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		},
		Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
			ExitCode: &bespb.BuildFinished_ExitCode{
				Name: exitCodeName,
				Code: int32(exitCode),
			},
			FinishTime: timestamppb.New(now),
		}},
	})
}

func (r *buildEventReporter) Stop() error {
	if r.cancelBackgroundFlush != nil {
		r.cancelBackgroundFlush()
		r.cancelBackgroundFlush = nil
	}
	r.FlushProgress()

	elapsedTimeSeconds := float64(time.Since(r.startTime)) / float64(time.Second)
	// NB: This is the last message -- if more are added afterwards, be sure to
	// update the `LastMessage` flag
	_ = r.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		Payload: &bespb.BuildEvent_BuildToolLogs{BuildToolLogs: &bespb.BuildToolLogs{
			Log: []*bespb.File{
				{Name: "elapsed time", File: &bespb.File_Contents{Contents: []byte(string(fmt.Sprintf("%.6f", elapsedTimeSeconds)))}},
			},
		}},
		LastMessage: true,
	})

	if err := r.bep.Finish(); err != nil {
		// If we don't publish a build event successfully, then the status may not be
		// reported to the Git provider successfully. Terminate with a code indicating
		// that the executor can retry the action, so that we have another chance.
		return status.UnavailableErrorf("failed to publish build event: %s", err)
	}

	return nil
}

func (r *buildEventReporter) FlushProgress() error {
	event, err := r.nextProgressEvent()
	if err != nil {
		return err
	}
	if event == nil {
		// No progress to flush.
		return nil
	}

	return r.bep.Publish(event)
}

func (r *buildEventReporter) nextProgressEvent() (*bespb.BuildEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	buf, err := r.log.ReadAll()
	if err != nil {
		return nil, status.WrapError(err, "failed to read action logs")
	}
	if len(buf) == 0 {
		return nil, nil
	}
	count := r.progressCount
	r.progressCount++

	output := string(buf)

	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count + 1}}},
		},
		Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{
			// Only outputting to stderr for now, like Bazel does.
			Stderr: output,
		}},
	}, nil
}

func (r *buildEventReporter) startBackgroundProgressFlush() func() {
	stop := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(progressFlushInterval):
				r.FlushProgress()
			}
		}
	}()
	return func() {
		stop <- struct{}{}
	}
}

// emitBuildEventsForBazelCommands scans command output logs for bazel invocations
// in order to emit bazel build events.
//
// Event publishing errors will be surfaced in the caller func when calling
// `buildEventPublisher.Finish()`
func (r *buildEventReporter) emitBuildEventsForBazelCommands(output string) {
	// Check whether a bazel invocation was invoked
	iidMatches := invocationIDRegex.FindAllStringSubmatch(output, -1)
	for _, m := range iidMatches {
		iid := m[1]
		childStarted := slices.Contains(r.childInvocations, iid)

		var buildEvent *bespb.BuildEvent
		if childStarted {
			// The `Streaming build results to` log line is printed at the start and
			// end of a bazel build. If we've already seen it for this invocation,
			// we know the build has finished.
			buildEvent = &bespb.BuildEvent{
				Id: &bespb.BuildEventId{
					Id: &bespb.BuildEventId_ChildInvocationCompleted{
						ChildInvocationCompleted: &bespb.BuildEventId_ChildInvocationCompletedId{InvocationId: iid},
					},
				},
				Payload: &bespb.BuildEvent_ChildInvocationCompleted{ChildInvocationCompleted: &bespb.ChildInvocationCompleted{}},
			}
		} else {
			r.childInvocations = append(r.childInvocations, iid)

			cic := &bespb.ChildInvocationsConfigured{
				Invocation: []*bespb.ChildInvocationsConfigured_InvocationMetadata{
					{
						InvocationId: iid,
					},
				},
			}
			buildEvent = &bespb.BuildEvent{
				Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_ChildInvocationsConfigured{ChildInvocationsConfigured: &bespb.BuildEventId_ChildInvocationsConfiguredId{}}},
				Payload: &bespb.BuildEvent_ChildInvocationsConfigured{ChildInvocationsConfigured: cic},
				Children: []*bespb.BuildEventId{
					{
						Id: &bespb.BuildEventId_ChildInvocationCompleted{
							ChildInvocationCompleted: &bespb.BuildEventId_ChildInvocationCompletedId{InvocationId: iid},
						},
					},
				},
			}
		}

		if err := r.Publish(buildEvent); err != nil {
			continue
		}
	}
}

func main() {
	if os.Getenv("CI_RUNNER_DEBUG") == "1" {
		*backendLog.LogLevel = "debug"
		backendLog.Configure()
	}
	if err := run(); err != nil {
		if result, ok := err.(*actionResult); ok {
			os.Exit(result.exitCode)
		}
		backendLog.Errorf("%s", err)
		os.Exit(int(gstatus.Code(err)))
	}
}

func run() error {
	// Do not parse flags in bazel wrapper mode, because it causes parsing errors
	// with bazel startup options.
	isBazelWrapper := os.Getenv("BAZEL_WRAPPER_MODE") == "1"
	if slices.Contains(os.Args, "--credential_helper") {
		flag.Parse()
	} else if !isBazelWrapper {
		if err := parseFlags(); err != nil {
			return err
		}
	}
	if *credentialHelper {
		return runCredentialHelper()
	}
	if isBazelWrapper {
		return runBazelWrapper()
	}

	runID, err := newUUID()
	if err != nil {
		return err
	}

	ws := &workspace{
		startTime:          time.Now(),
		buildbuddyAPIKey:   os.Getenv(buildbuddyAPIKeyEnvVarName),
		forcedInvocationID: *invocationID,
		runID:              runID,
	}

	ctx := context.Background()
	if ws.buildbuddyAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, ws.buildbuddyAPIKey)
	}
	if ci := os.Getenv(clientIdentityEnvVar); ci != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, clientidentity.IdentityHeaderName, ci)
	}
	contextWithoutTimeout := ctx
	if *timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	// Use a context without a timeout for the build event reporter, so that even
	// if the `timeout` is reached, any events will finish getting published
	buildEventReporter, err := newBuildEventReporter(contextWithoutTimeout, *besBackend, ws.buildbuddyAPIKey, *invocationID, *workflowID != "" /*=isWorkflow*/)
	if err != nil {
		return err
	}
	// Note: logs written to the buildEventReporter will be written as
	// invocation progress events as well as written to the workflow action's
	// stderr.
	ws.log = buildEventReporter
	ws.hostname, ws.username = ws.getHostAndUserName()

	// Set BUILDBUDDY_CI_RUNNER_ABSPATH so that we can re-invoke ourselves
	// as the git credential helper reliably, even after chdir.
	absPath, err := filepath.Abs(os.Args[0])
	if err != nil {
		return status.WrapError(err, "compute CI runner binary abspath")
	}
	os.Setenv("BUILDBUDDY_CI_RUNNER_ABSPATH", absPath)

	// Store the original task workspace dir since we change directories later.
	taskWorkspaceDir, err := os.Getwd()
	if err != nil {
		return err
	}
	// Change the current working directory to respect WORKDIR_OVERRIDE, if set.
	if wd := os.Getenv("WORKDIR_OVERRIDE"); wd != "" {
		if err := os.MkdirAll(wd, 0755); err != nil {
			return status.WrapError(err, "create WORKDIR_OVERRIDE directory")
		}
		if err := os.Chdir(wd); err != nil {
			return err
		}
	}

	rootDir, err := os.Getwd()
	if err != nil {
		return err
	}
	ws.rootDir = rootDir
	os.Setenv("BUILDBUDDY_CI_RUNNER_ROOT_DIR", rootDir)

	// Bazel needs a HOME dir; ensure that one is set.
	if err := ensureHomeDir(); err != nil {
		return status.WrapError(err, "ensure HOME")
	}
	// Bazel also needs USER to be set.
	if err := ensureUser(); err != nil {
		return status.WrapError(err, "ensure USER")
	}
	// Make sure PATH is set.
	if err := ensurePath(); err != nil {
		return status.WrapError(err, "ensure PATH")
	}
	// Try to disable interactivity since we don't provide a real terminal
	// connection.
	if err := disableInteractivity(); err != nil {
		return status.WrapError(err, "disable interactivity")
	}

	// Write default bazelrc
	if err := writeBazelrc(buildbuddyBazelrcPath, buildEventReporter.invocationID, runID, rootDir); err != nil {
		return status.WrapError(err, "write "+buildbuddyBazelrcPath)
	}
	// Delete bazelrc before exiting. Use abs path since we might cd after this
	// point.
	absBazelrcPath, err := filepath.Abs(buildbuddyBazelrcPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(absBazelrcPath); err != nil {
			ws.log.Printf("Could not remove buildbuddy bazelrc: %s", err.Error())
		}
	}()

	// Configure TERM to get prettier output from executed commands.
	if err := os.Setenv("TERM", "xterm-256color"); err != nil {
		return status.WrapError(err, "could not setup TERM")
	}

	// Make sure we have a bazel / bazelisk binary available.
	if *bazelCommand == "" {
		bazeliskPath := filepath.Join(rootDir, bazeliskBinaryName)
		if err := extractBazelisk(bazeliskPath); err != nil {
			return status.WrapError(err, "failed to extract bazelisk")
		}
		*bazelCommand = bazeliskPath
	}

	// Use the bazel wrapper script, which adds some common flags to all
	// Bazel builds.
	if err := ws.writeBazelWrapperScript(); err != nil {
		return status.WrapError(err, "write bazel wrapper script")
	}

	if *shutdownAndExit {
		ws.log.Println("--shutdown_and_exit requested; will run bazel shutdown then exit.")
		if _, err := os.Stat(repoDirName); err != nil {
			ws.log.Println("Workspace does not exist; exiting.")
			return nil
		}
		if err := os.Chdir(repoDirName); err != nil {
			return err
		}
		args, err := ws.bazelArgsWithCustomBazelrc("shutdown")
		if err != nil {
			return err
		}
		if err := printCommandLine(ws.log, *bazelCommand, args...); err != nil {
			return err
		}
		bazelWorkspacePath, err := ws.bazelWorkspacePath()
		if err != nil {
			return err
		}
		if err := runCommand(ctx, *bazelCommand, args, nil, bazelWorkspacePath, ws.log); err != nil {
			return err
		}
		ws.log.Println("Shutdown complete.")
		return nil
	}

	if err := buildEventReporter.Start(ws.startTime); err != nil {
		return status.WrapError(err, "could not publish started event")
	}
	result, err := ws.RunAction(ctx, buildEventReporter)
	if err != nil {
		buildEventReporter.PublishFinishedEvent(noExitCode, failedExitCodeName)
		_ = buildEventReporter.Stop()
		return err
	}
	buildEventReporter.PublishFinishedEvent(result.exitCode, result.exitCodeName)

	ws.prepareRunnerForNextInvocation(ctx, taskWorkspaceDir)

	// Print an empty line to display the end time of the workflow
	ws.log.Printf("\n%sRemote run completed at %s%s", ansiGray, formatNowUTC(), ansiReset)

	if err := buildEventReporter.Stop(); err != nil {
		return err
	}
	if result.exitCode != 0 {
		return result // as error
	}
	return nil
}

// Prepares the runner for the next invocation to be executed. This is intended
// to be called after the current invocation has completed, to avoid blocking
// the invocation status from being reported.
func (ws *workspace) prepareRunnerForNextInvocation(ctx context.Context, taskWorkspaceDir string) {
	log := ws.log

	// After the invocation is complete, ensure that the bazel lock is not
	// still held. If it is, avoid recycling.
	if err := ws.checkBazelWorkspaceLock(ctx); err != nil {
		log.Printf("WARNING: command 'bazel --noblock_for_lock info workspace' failed: %s", err)
		log.Printf("WARNING: bazel workspace lock check failed. Runner will not be recycled")
		marker := filepath.Join(taskWorkspaceDir, ".BUILDBUDDY_DO_NOT_RECYCLE")
		if err := os.WriteFile(marker, nil, 0644); err != nil {
			log.Printf("ERROR: failed to create %s: %s", marker, err)
		}
	}

	// After the invocation is complete, attempt to reclaim disk space if
	// we're low on disk.
	if err := ws.reclaimDiskSpace(ctx); err != nil {
		log.Printf("WARNING: failed to reclaim disk space: %s", err)
	}
}

// parseFlags should not fail when parsing an undefined flag.
// This lets us add new flags to this script without breaking older executors
// (on self-hosted executors, for example) that aren't expecting them when the
// app server tries to send them.
func parseFlags() error {
	// Ignore errors when parsing an unknown flag
	flagset := flag.CommandLine
	flagset.Init(os.Args[0], flag.ContinueOnError)
	flagset.Usage = func() {}

	unparsedArgs := os.Args[1:]
	for len(unparsedArgs) > 0 {
		err := flagset.Parse(unparsedArgs)
		// Ignore undefined flag errors. The flag package will automatically print
		// a warning error message.
		if err == nil || strings.Contains(err.Error(), "flag provided but not defined") {
			unparsedArgs = flagset.Args()
		} else {
			return err
		}
	}
	return nil
}

type actionResult struct {
	exitCode     int
	exitCodeName string
}

func (r *actionResult) Error() string {
	return fmt.Sprintf("exit status %d", r.exitCode)
}

// RunAction runs the specified action and streams the progress to the invocation via the buildEventReporter.
func (ws *workspace) RunAction(ctx context.Context, buildEventReporter *buildEventReporter) (*actionResult, error) {
	// NB: Anything logged to `ar.log` gets output to both the stdout of this binary
	// and the logs uploaded to BuildBuddy for this action. Anything that we want
	// the user to see in the invocation UI needs to go in that log, instead of
	// the global `log.Print`.
	ar := &actionRunner{
		isWorkflow: *workflowID != "",
		reporter:   buildEventReporter,
		rootDir:    ws.rootDir,
		hostname:   ws.hostname,
		username:   ws.username,
	}
	exitCode := 0
	exitCodeName := "OK"

	if err := ar.Run(ctx, ws); err != nil {
		ar.reporter.Printf(aurora.Sprintf(aurora.Red("\nAction failed: %s"), status.Message(err)))
		exitCode = getExitCode(err)
		// TODO: More descriptive exit code names, so people have a better
		// sense of what happened without even needing to open the invocation.
		exitCodeName = failedExitCodeName
	}

	return &actionResult{exitCode, exitCodeName}, nil
}

func (r *buildEventReporter) Write(b []byte) (int, error) {
	return r.log.Write(b)
}

func (r *buildEventReporter) Println(vals ...interface{}) {
	r.log.Println(vals...)
}
func (r *buildEventReporter) Printf(format string, vals ...interface{}) {
	r.log.Printf(format, vals...)
}

type invocationLog struct {
	lockingbuffer.LockingBuffer
	writer        io.Writer
	writeListener func(s string)
}

func newInvocationLog() *invocationLog {
	invLog := &invocationLog{writeListener: func(s string) {}}
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, os.Stderr)
	return invLog
}

func (invLog *invocationLog) Write(b []byte) (int, error) {
	output := string(b)

	redacted := redact.RedactText(output)

	invLog.writeListener(redacted)
	_, err := invLog.writer.Write([]byte(redacted))

	// Return the size of the original buffer even if a redacted size was written,
	// or clients will return a short write error
	return len(b), err
}

func (invLog *invocationLog) Println(vals ...interface{}) {
	invLog.Write([]byte(fmt.Sprintln(vals...)))
}
func (invLog *invocationLog) Printf(format string, vals ...interface{}) {
	invLog.Write([]byte(fmt.Sprintf(format+"\n", vals...)))
}

// actionRunner runs a single action in the BuildBuddy config.
type actionRunner struct {
	isWorkflow bool
	reporter   *buildEventReporter
	rootDir    string
	username   string
	hostname   string
}

func (ar *actionRunner) Run(ctx context.Context, ws *workspace) error {
	// NOTE: In this func we return immediately with an error of nil if event publishing fails,
	// because that error is instead surfaced in the caller func when calling
	// `buildEventPublisher.Wait()`

	actionName, err := getActionNameForWorkflowConfiguredEvent()
	if err != nil {
		return err
	}
	wfc := &bespb.WorkflowConfigured{
		WorkflowId:         *workflowID,
		ActionName:         actionName,
		ActionTriggerEvent: *triggerEvent,
		PushedRepoUrl:      *pushedRepoURL,
		PushedBranch:       *pushedBranch,
		CommitSha:          *commitSHA,
		TargetRepoUrl:      *targetRepoURL,
		TargetBranch:       *targetBranch,
		Os:                 runtime.GOOS,
		Arch:               runtime.GOARCH,
	}
	wfcEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowConfigured{WorkflowConfigured: &bespb.BuildEventId_WorkflowConfiguredId{}}},
		Payload: &bespb.BuildEvent_WorkflowConfigured{WorkflowConfigured: wfc},
	}
	if ar.isWorkflow {
		if err := ar.reporter.Publish(wfcEvent); err != nil {
			return nil
		}
	}

	buildMetadata := &bespb.BuildMetadata{
		Metadata: map[string]string{},
	}
	if ar.isWorkflow {
		buildMetadata.Metadata["ROLE"] = "CI_RUNNER"
	} else {
		buildMetadata.Metadata["ROLE"] = "HOSTED_BAZEL"
	}
	if *prNumber != 0 {
		buildMetadata.Metadata["PULL_REQUEST_NUMBER"] = fmt.Sprintf("%d", *prNumber)
	}
	if isPushedRefInFork() {
		buildMetadata.Metadata["FORK_REPO_URL"] = *pushedRepoURL
	}
	buildMetadata.Metadata["REPO_URL"] = baseRepoURL()
	if *visibility != "" {
		buildMetadata.Metadata["VISIBILITY"] = *visibility
	}
	buildMetadata.Metadata["RUN_ID"] = ws.runID
	buildMetadataEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
	}
	if err := ar.reporter.Publish(buildMetadataEvent); err != nil {
		return nil
	}

	// Publish WorkspaceStatus eagerly if the git repo state is specified
	// in advance. This allows the UI to load a little sooner. Otherwise,
	// wait until we've initialized the repo.
	// Note that this has to happen after the BuildMetadata event is published.
	publishedWorkspaceStatus := false
	if *commitSHA == "" {
		ar.reporter.Printf("WARNING: 'commit_sha' field is missing from ExecuteWorkflow request. Set a commit SHA to ensure there are no race conditions if the remote branch is updated.")
	} else {
		if err := ar.reporter.Publish(ar.workspaceStatusEvent()); err != nil {
			return nil
		}
		publishedWorkspaceStatus = true
	}

	// Only print this to the local logs -- it's mostly useful for development purposes.
	backendLog.Infof("Invocation URL:  %s", invocationURL(ar.reporter.InvocationID()))

	if err := ws.setup(ctx); err != nil {
		return status.WrapError(err, "failed to set up git repo")
	}
	action, err := getActionToRun()
	if err != nil {
		return status.WrapError(err, "failed to get action to run")
	}

	if !publishedWorkspaceStatus {
		if err := ar.reporter.Publish(ar.workspaceStatusEvent()); err != nil {
			return nil
		}
		publishedWorkspaceStatus = true
	}

	if ws.setupError != nil {
		return ws.setupError
	}

	uploader := ar.reporter.uploader
	// Log upload results at the end of all Bazel commands.
	defer func() {
		if uploader == nil {
			return
		}
		uploads, err := uploader.Wait()
		if err != nil {
			ar.reporter.Printf("WARNING: failed to upload some artifacts written to $%s: %s", artifactsDirEnvVarName, err)
		}
		writeCommandSummary(ws.log, "Uploaded %d artifacts", len(uploads))
		for _, u := range uploads {
			if u.Err != nil {
				ar.reporter.Printf("WARNING: failed to upload artifact %s/%s", u.NamedSetID, u.Name)
				continue
			}
			ar.reporter.Printf(
				"Uploaded artifact %s/%s (%s) in %s",
				u.NamedSetID, u.Name,
				units.HumanSize(float64(u.Digest.SizeBytes)), u.Duration)
		}
	}()

	// Migrate deprecated `action.BazelCommands` to `action.Steps`
	if len(action.Steps) > 0 && len(action.DeprecatedBazelCommands) > 0 {
		return status.InternalError("only one of `Steps` or `BazelCommands` should be set")
	}
	if len(action.Steps) == 0 {
		action.Steps = make([]*rnpb.Step, 0)
	}
	for _, cmd := range action.DeprecatedBazelCommands {
		if !(strings.HasPrefix(cmd, bazeliskBinaryName) || strings.HasPrefix(cmd, bazelBinaryName)) {
			cmd = "bazel " + cmd
		}
		action.Steps = append(action.Steps, &rnpb.Step{
			Run: cmd,
		})
	}

	for i, step := range action.Steps {
		cmdStartTime := time.Now()

		// The UI uses TargetConfigured/Completed build events to render artifacts
		// associated with targets.
		// Here we consider the step a "target" and publish the events for it,
		// so that we can render artifacts for it.
		targetLabel := fmt.Sprintf("steps[%d]", i)
		ar.reporter.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_TargetConfigured{
				TargetConfigured: &bespb.BuildEventId_TargetConfiguredId{
					Label: targetLabel,
				},
			}},
			Payload: &bespb.BuildEvent_Configured{Configured: &bespb.TargetConfigured{}},
		})
		if err := provisionArtifactsDir(ws, i); err != nil {
			return err
		}

		// Provision the directory where we write bazel run scripts
		runScriptDir := filepath.Join(ws.rootDir, runScriptDirName)
		if err := os.MkdirAll(runScriptDir, 0755); err != nil {
			return err
		}

		runErr := runBashCommand(ctx, step.Run, nil, action.BazelWorkspaceDir, ar.reporter)
		exitCode := getExitCode(runErr)

		artifactsDir := artifactsPathForCommand(ws, i)
		namedSetID := filepath.Base(artifactsDir)
		ar.reporter.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_TargetCompleted{
				TargetCompleted: &bespb.BuildEventId_TargetCompletedId{
					Label: targetLabel,
				},
			}},
			Payload: &bespb.BuildEvent_Completed{Completed: &bespb.TargetComplete{
				Success: runErr == nil,
				OutputGroup: []*bespb.OutputGroup{
					{
						FileSets: []*bespb.BuildEventId_NamedSetOfFilesId{
							{Id: namedSetID},
						},
					},
				},
			}},
		})

		if exitCode != noExitCode {
			ar.reporter.Printf("%s(command exited with code %d)%s\n", ansiGray, exitCode, ansiReset)
		}

		// If this is a workflow, kill-signal the current process on certain
		// exit codes (rather than exiting) so that the workflow action is
		// retried. Note that we do this immediately after the Bazel command is
		// completed so that the outer workflow invocation gets disconnected
		// rather than finishing with an error.
		if *workflowID != "" && exitCode == bazelLocalEnvironmentalErrorExitCode {
			p, err := os.FindProcess(os.Getpid())
			if err != nil {
				return err
			}
			if err := p.Kill(); err != nil {
				return err
			}
		}

		// If we get an OOM or a Bazel internal error, copy debug outputs to the
		// artifacts directory so they get uploaded as workflow artifacts.
		if exitCode == bazelOOMErrorExitCode || exitCode == bazelInternalErrorExitCode {
			jvmOutPath := filepath.Join(ar.rootDir, outputBaseDirName, "server/jvm.out")
			if err := os.Link(jvmOutPath, filepath.Join(artifactsDir, "jvm.out")); err != nil {
				ar.reporter.Printf("%sfailed to preserve jvm.out: %s%s\n", ansiGray, err, ansiReset)
			}
		}
		if exitCode == bazelOOMErrorExitCode {
			bazelInvocationIDs := ar.reporter.childInvocations
			if len(bazelInvocationIDs) > 0 {
				lastInvocationID := bazelInvocationIDs[len(bazelInvocationIDs)-1]
				heapDumpPath := filepath.Join(ar.rootDir, outputBaseDirName, fmt.Sprintf("%s.heapdump.hprof", lastInvocationID))
				if err := os.Link(heapDumpPath, filepath.Join(artifactsDir, "heapdump.hprof")); err != nil {
					ar.reporter.Printf("%sfailed to preserve heapdump.hprof: %s%s\n", ansiGray, err, ansiReset)
				}
			}
		}

		// Kick off background uploads for the action that just completed
		if uploader != nil {
			writeCommandSummary(ws.log, "Uploading artifacts from %s", artifactsDir)
			uploader.UploadDirectory(namedSetID, artifactsDir) // does not return an error
		}

		// If extracting run information from builds was requested,
		// extract it and send it via the event stream.
		if _, err = os.Stat(runScriptDir); err == nil {
			err = filepath.Walk(runScriptDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() {
					runScriptInfo, err := processRunScript(ctx, path)
					if err != nil {
						return err
					}
					e := &bespb.BuildEvent{
						Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_RunTargetAnalyzed{}},
						Payload: &bespb.BuildEvent_RunTargetAnalyzed{RunTargetAnalyzed: &bespb.RunTargetAnalyzed{
							Arguments:          runScriptInfo.args,
							RunfilesRoot:       runScriptInfo.runfilesRoot,
							Runfiles:           runScriptInfo.runfiles,
							RunfileDirectories: runScriptInfo.runfileDirs,
						}},
					}
					ar.reporter.Publish(e)
				}
				return nil
			})
			if err != nil {
				return err
			}

			// Clear the directory so it's in a clean state for future steps
			if err := os.RemoveAll(runScriptDir); err != nil {
				return err
			}
		}

		duration := time.Since(cmdStartTime)
		completedEvent := &bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_RemoteRunnerStepCompleted{
				RemoteRunnerStepCompleted: &bespb.BuildEventId_RemoteRunnerStepCompletedId{},
			}},
			Payload: &bespb.BuildEvent_RemoteRunnerStepCompleted{RemoteRunnerStepCompleted: &bespb.RemoteRunnerStepCompleted{
				ExitCode:  int32(exitCode),
				StartTime: timestamppb.New(cmdStartTime),
				Duration:  durationpb.New(duration),
			}},
		}
		if err := ar.reporter.Publish(completedEvent); err != nil {
			break
		}

		if exitCode != 0 {
			return runErr
		}

		// Flush progress after every command.
		// Stop execution early on BEP failure, but ignore error -- it will surface in `bep.Finish()`.
		if err := ar.reporter.FlushProgress(); err != nil {
			break
		}
	}

	return nil
}

func (ar *actionRunner) workspaceStatusEvent() *bespb.BuildEvent {
	buildUser := os.Getenv("BUILD_USER")
	if buildUser == "" {
		buildUser = ar.username
	}
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bespb.WorkspaceStatus{
			Item: []*bespb.WorkspaceStatus_Item{
				{Key: "BUILD_USER", Value: buildUser},
				{Key: "BUILD_HOST", Value: ar.hostname},
				{Key: "GIT_BRANCH", Value: *pushedBranch},
				{Key: "GIT_TREE_STATUS", Value: "Clean"},
				// Note: COMMIT_SHA may not actually reflect the current state
				// of the repo since we merge the target branch before running
				// the workflow; we set this for the purpose of reporting
				// statuses to GitHub.
				{Key: "COMMIT_SHA", Value: *commitSHA},
				{Key: "REPO_URL", Value: baseRepoURL()},
			},
		}},
	}
}

// This should only be used for WorkflowConfiguredEvents--it explicitly labels
// actions with no name so that they can be identified later on.
func getActionNameForWorkflowConfiguredEvent() (string, error) {
	if *serializedAction != "" {
		a, err := deserializeAction(*serializedAction)
		if err != nil {
			return "", err
		}
		return a.Name, nil
	}
	if *actionName != "" {
		return *actionName, nil
	}
	return "Unknown action", nil
}

func getActionToRun() (*config.Action, error) {
	if *serializedAction != "" {
		return deserializeAction(*serializedAction)
	}
	if *actionName != "" {
		cfg, err := readConfig()
		if err != nil {
			return nil, status.WrapError(err, "failed to read BuildBuddy config")
		}
		// If a specific action was specified, filter to configured
		// actions with a matching action name.
		return findAction(cfg.Actions, *actionName)
	}
	return nil, status.InvalidArgumentError("an action to run must be specified")
}

func deserializeAction(actionString string) (*config.Action, error) {
	actionYaml, err := base64.StdEncoding.DecodeString(actionString)
	if err != nil {
		return nil, err
	}
	a := &config.Action{}
	if err := yaml.Unmarshal(actionYaml, a); err != nil {
		return nil, err
	}
	return a, nil
}

type runInfo struct {
	args         []string
	runfiles     []*bespb.File
	runfileDirs  []*bespb.Tree
	runfilesRoot string
}

func collectRunfiles(runfilesDir string) (map[digest.Key]string, map[string]string, error) {
	fileDigestMap := make(map[digest.Key]string)
	dirsToUpload := make(map[string]string)
	err := filepath.WalkDir(runfilesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			t, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fi, err := os.Stat(t)
			if err != nil {
				return err
			}
			if fi.IsDir() {
				dirsToUpload[path] = t
				return nil
			}
		}
		rn, err := cachetools.ComputeFileDigest(path, *remoteInstanceName, repb.DigestFunction_SHA256)
		if err != nil {
			return err
		}
		fileDigestMap[digest.NewKey(rn.GetDigest())] = path
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, status.UnknownErrorf("could not setup runtime files: %s", err)
	}
	return fileDigestMap, dirsToUpload, err
}

func uploadRunfiles(ctx context.Context, workspaceRoot, runfilesDir string) ([]*bespb.File, []*bespb.Tree, error) {
	healthChecker := healthcheck.NewHealthChecker("ci-runner")
	env := real_environment.NewRealEnv(healthChecker)

	fileDigestMap, dirs, err := collectRunfiles(runfilesDir)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc_client.DialSimple(*cacheBackend)
	if err != nil {
		return nil, nil, err
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))

	backendURL, err := url.Parse(*cacheBackend)
	if err != nil {
		return nil, nil, err
	}
	bytestreamURIPrefix := "bytestream://" + backendURL.Host

	var digests []*repb.Digest
	var runfiles []*bespb.File
	for d, runfilePath := range fileDigestMap {
		digests = append(digests, d.ToDigest())
		relPath, err := filepath.Rel(workspaceRoot, runfilePath)
		if err != nil {
			return nil, nil, err
		}
		downloadString, err := digest.NewResourceName(d.ToDigest(), *remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).DownloadString()
		if err != nil {
			return nil, nil, err
		}

		runfiles = append(runfiles, &bespb.File{
			Name: relPath,
			File: &bespb.File_Uri{
				Uri: fmt.Sprintf("%s%s", bytestreamURIPrefix, downloadString),
			},
		})
	}
	rsp, err := env.GetContentAddressableStorageClient().FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName: *remoteInstanceName,
		BlobDigests:  digests,
	})
	if err != nil {
		return nil, nil, status.UnknownErrorf("could not check digest existence: %s", err)
	}
	missingDigests := rsp.GetMissingBlobDigests()

	eg, ctx := errgroup.WithContext(ctx)
	u := cachetools.NewBatchCASUploader(ctx, env, *remoteInstanceName, repb.DigestFunction_SHA256)

	for _, d := range missingDigests {
		runfilePath, ok := fileDigestMap[digest.NewKey(d)]
		if !ok {
			// not supposed to happen...
			return nil, nil, status.InternalErrorf("missing digest not in our digest map")
		}
		f, err := os.Open(runfilePath)
		if err != nil {
			return nil, nil, err
		}
		if err := u.Upload(d, f); err != nil {
			return nil, nil, err
		}
	}

	eg.Go(func() error {
		return u.Wait()
	})

	var runfileDirs []*bespb.Tree
	var mu sync.Mutex
	// Output directories in runfiles are symlinks to physical directories.
	// We upload the real directory, but return the logical directory that the binary expects.
	for placePath, realPath := range dirs {
		placePath := placePath
		realPath := realPath
		eg.Go(func() error {
			_, td, err := cachetools.UploadDirectoryToCAS(ctx, env, *remoteInstanceName, repb.DigestFunction_SHA256, realPath)
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(workspaceRoot, placePath)
			if err != nil {
				return err
			}
			downloadString, err := digest.NewResourceName(td, *remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).DownloadString()
			if err != nil {
				return err
			}
			mu.Lock()
			runfileDirs = append(runfileDirs, &bespb.Tree{
				Name: relPath,
				Uri:  fmt.Sprintf("%s%s", bytestreamURIPrefix, downloadString),
			})
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	return runfiles, runfileDirs, nil
}

// processRunScript processes the contents of a bazel run script (produced via bazel run --script_path) and extracts
// information about the default binary arguments as well as about supporting files (runfiles).
func processRunScript(ctx context.Context, runScript string) (*runInfo, error) {
	runScriptBytes, err := os.ReadFile(runScript)
	if err != nil {
		return nil, status.UnknownErrorf("error reading run script: %s", err)
	}
	runScriptLines := strings.Split(string(runScriptBytes), "\n")
	if len(runScriptLines) < 3 {
		return nil, status.UnknownErrorf("run script not in expected format, too short")
	}

	// The last line contains the name of the binary as well as the default arguments.
	binAndArgs, err := shlex.Split(runScriptLines[len(runScriptLines)-1])
	if err != nil {
		return nil, status.UnknownErrorf("error parsing run script: %s", err)
	}
	bin := binAndArgs[0]
	var args []string
	for _, a := range binAndArgs[1:] {
		if a != "$@" {
			args = append(args, a)
		}
	}

	wsFile, err := bazel.FindWorkspaceFile(filepath.Dir(bin))
	if err != nil {
		return nil, status.UnknownErrorf("could not detect binary workspace root: %s", err)
	}
	wsRoot := filepath.Dir(wsFile)

	// The second line changes the working directory to within the runfiles directory.
	cdLine := runScriptLines[1]
	l := shlex.NewLexer(bytes.NewReader([]byte(cdLine)))
	k, err := l.Next()
	if err != nil {
		return nil, status.UnknownErrorf("could not parse working directory line: %s", err)
	}
	if k != "cd" {
		return nil, status.UnknownErrorf("run script not in expected format: directory change not found")
	}
	k, err = l.Next()
	if err != nil {
		return nil, status.UnknownErrorf("could not parse working directory line: %s", err)
	}
	runfilesRoot, err := filepath.Rel(wsRoot, k)
	if err != nil {
		return nil, err
	}

	runfilesDir := bin + ".runfiles"
	runfiles, runfileDirs, err := uploadRunfiles(ctx, wsRoot, runfilesDir)
	if err != nil {
		return nil, err
	}

	return &runInfo{
		args:         args,
		runfiles:     runfiles,
		runfileDirs:  runfileDirs,
		runfilesRoot: runfilesRoot,
	}, nil
}

func printCommandLine(out io.Writer, command string, args ...string) error {
	cmdLine := command
	for _, arg := range args {
		cmdLine += " " + toShellToken(arg)
	}
	io.WriteString(out, ansiGray+formatNowUTC()+ansiReset+" ")
	io.WriteString(out, aurora.Sprintf("%s %s\n", aurora.Green("$"), cmdLine))
	return nil
}

func (ws *workspace) bazelWorkspacePath() (string, error) {
	action, err := getActionToRun()
	if err != nil {
		return "", err
	}
	return filepath.Join(ws.rootDir, repoDirName, action.BazelWorkspaceDir), nil
}

// Returns the tokenized bazel command with a startup option to use the custom
// baelrc written by the ci_runner.
func (ws *workspace) bazelArgsWithCustomBazelrc(cmd string) ([]string, error) {
	tokens, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}
	if tokens[0] == bazelBinaryName || tokens[0] == bazeliskBinaryName {
		tokens = tokens[1:]
	}
	bazelWorkspacePath, err := ws.bazelWorkspacePath()
	if err != nil {
		return nil, err
	}
	startupFlags, err := customBazelrcOptions(ws.rootDir, bazelWorkspacePath)
	if err != nil {
		return nil, err
	}
	return append(startupFlags, tokens...), nil
}

// Returns the startup options to use the custom bazelrc written by the ci_runner.
func customBazelrcOptions(rootAbsPath string, bazelWorkspaceAbsPath string) ([]string, error) {
	startupFlags := []string{"--bazelrc=" + filepath.Join(rootAbsPath, buildbuddyBazelrcPath)}

	// Bazel will treat the user's workspace .bazelrc file with lower precedence
	// than our --bazelrc, which is undesired. So instead, explicitly add the
	// workspace rc as a --bazelrc flag after ours, and also set --noworkspace_rc
	// to prevent the workspace rc from getting loaded twice.
	if bazelWorkspaceAbsPath != "" {
		workspaceRcPath := filepath.Join(bazelWorkspaceAbsPath, ".bazelrc")
		_, err := os.Stat(workspaceRcPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if exists := err == nil; exists {
			startupFlags = append(startupFlags, "--noworkspace_rc", "--bazelrc="+workspaceRcPath)
		}
	}
	return startupFlags, nil
}

func currentBazelWorkspaceAbsPath() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	wsPath, err := bazel.FindWorkspaceFile(dir)
	if err != nil {
		return "", err
	}
	wsDir := filepath.Dir(wsPath)
	return wsDir, nil
}

// appendBazelSubcommandArgs appends bazel arguments to a bazel command,
// *before* the arg separator ("--") if it exists, so that the arguments apply
// to the bazel subcommand ("build", "run", etc.) and not the binary being run
// (in the "bazel run" case).
func appendBazelSubcommandArgs(args []string, argsToAppend ...string) []string {
	splitIndex := len(args)
	for i, arg := range args {
		if arg == "--" {
			splitIndex = i
			break
		}
	}
	out := make([]string, 0, len(args)+len(argsToAppend))
	out = append(out, args[:splitIndex]...)
	out = append(out, argsToAppend...)
	out = append(out, args[splitIndex:]...)
	return out
}

func ensureHomeDir() error {
	// HOME dir is "/" when there is no user home dir created, which can happen
	// when running locally (due to docker_inherit_user_ids). Treat this the same
	// as HOME being unset.
	if os.Getenv("HOME") != "" && os.Getenv("HOME") != "/" {
		return nil
	}
	os.MkdirAll(".home", 0777)
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	os.Setenv("HOME", path.Join(wd, ".home"))
	return nil
}

func ensureUser() error {
	if os.Getenv("USER") != "" {
		return nil
	}
	return os.Setenv("USER", "buildbuddy")
}

func ensurePath() error {
	if os.Getenv("PATH") != "" {
		return nil
	}
	return os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func disableInteractivity() error {
	// Prevent git from asking for user input.
	if err := os.Setenv("GIT_TERMINAL_PROMPT", "0"); err != nil {
		return err
	}
	if runtime.GOOS == "linux" {
		// Prevent `apt-get install` from asking for input.
		if err := os.Setenv("DEBIAN_FRONTEND", "noninteractive"); err != nil {
			return err
		}
	}
	return nil
}

// extractBazelisk copies the embedded bazelisk to the given path if it does
// not already exist.
func extractBazelisk(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	f, err := bazelisk.Open()
	if err != nil {
		return err
	}
	defer f.Close()
	dst, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0555)
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, f); err != nil {
		return err
	}
	return nil
}

func (ws *workspace) getHostAndUserName() (string, string) {
	hostname, err := os.Hostname()
	if err != nil {
		ws.log.Printf("failed to get hostname: %s", err)
		hostname = ""
	}
	user, err := user.Current()
	username := ""
	if err != nil {
		ws.log.Printf("failed to get user: %s", err)
	} else {
		username = user.Username
	}
	return hostname, username
}

func findAction(actions []*config.Action, name string) (*config.Action, error) {
	for _, action := range actions {
		if action.Name == name {
			return action, nil
		}
	}
	return nil, status.NotFoundErrorf("action %q not found", name)
}

func (ws *workspace) setup(ctx context.Context) error {
	// Remove any existing artifacts from previous workflow invocations
	if err := disk.ForceRemove(ctx, artifactsRootPath(ws)); err != nil {
		return err
	}
	repoDirInfo, err := os.Stat(repoDirName)
	if err != nil && !os.IsNotExist(err) {
		return status.WrapErrorf(err, "stat %q", repoDirName)
	}
	if repoDirInfo != nil {
		writeCommandSummary(ws.log, "Syncing existing repo...")
		if err := os.Chdir(repoDirName); err != nil {
			return status.WrapError(err, "cd")
		}
		err := ws.sync(ctx)
		if err == nil {
			return nil
		}
		if !*fallbackToCleanCheckout {
			return err
		}
		ws.log.Printf(
			"Failed to sync existing repo (maybe due to destructive '.git' dir edit or incompatible remote update). "+
				"Deleting and initializing from scratch. Error: %s",
			err,
		)
		if err := os.Chdir(".."); err != nil {
			return status.WrapError(err, "cd")
		}
		if err := disk.ForceRemove(ctx, repoDirName); err != nil {
			return status.WrapErrorf(err, "rm -rf %q", repoDirName)
		}
	}

	if err := os.Mkdir(repoDirName, 0777); err != nil {
		return status.WrapErrorf(err, "mkdir %q", repoDirName)
	}
	if err := os.Chdir(repoDirName); err != nil {
		return status.WrapErrorf(err, "cd %q", repoDirName)
	}
	if err := ws.init(ctx); err != nil {
		return err
	}
	if err := ws.sync(ctx); err != nil {
		return err
	}
	ws.log.Write([]byte(fmt.Sprintf("Setup completed in %s\n", time.Since(ws.startTime))))
	return nil
}

func (ws *workspace) applyPatch(ctx context.Context, bsClient bspb.ByteStreamClient, patchURI string) error {
	rn, err := digest.ParseDownloadResourceName(patchURI)
	if err != nil {
		return err
	}
	patchFileName := rn.GetDigest().GetHash()
	f, err := os.OpenFile(patchFileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err := cachetools.GetBlob(ctx, bsClient, rn, f); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if _, err := git(ctx, ws.log, "apply", "--verbose", patchFileName); err != nil {
		return err
	}
	return nil
}

func (ws *workspace) sync(ctx context.Context) error {
	if *pushedBranch == "" && *commitSHA == "" {
		return status.InvalidArgumentError("expected at least one of `pushed_branch` or `commit_sha` to be set")
	}

	if err := ws.config(ctx); err != nil {
		return err
	}

	if err := ws.fetchPushedRef(ctx); err != nil {
		return status.WrapError(err, "fetch pushed ref")
	}

	// Clean up in case a previous workflow made a mess.
	cleanArgs := []string{
		"clean",
		"-x", /* include ignored files */
		"-d", /* recurse into directories */
		"--force",
	}
	for _, path := range *gitCleanExclude {
		cleanArgs = append(cleanArgs, "-e", path)
	}
	if _, err := git(ctx, ws.log, cleanArgs...); err != nil {
		return err
	}

	if err := ws.checkoutRef(ctx); err != nil {
		return status.WrapError(err, "checkout ref")
	}

	if err := ws.updateSubmodules(ctx); err != nil {
		return status.WrapError(err, "update submodules")
	}

	// If commit sha is not set, pull the sha that is checked out so that it can be used in Github Status reporting
	if *commitSHA == "" {
		headCommitSHA, err := git(ctx, ws.log, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		*commitSHA = headCommitSHA
	}
	// Unfortunately there's no performant way to find the branch name from
	// a commit sha, so if the branch name is not set, leave it empty

	action, err := getActionToRun()
	if err != nil {
		return err
	}
	// If enabled, merge the target branch (if different from the
	// pushed branch) so that the workflow can pick up any changes not yet
	// incorporated into the pushed branch.
	if ws.shouldMergeBranches(action.GetTriggers()) {
		if err := ws.fetchTargetRef(ctx); err != nil {
			return status.WrapError(err, "fetch target ref")
		}
		targetRef := fmt.Sprintf("%s/%s", gitRemoteName(*targetRepoURL), *targetBranch)
		if _, err := git(ctx, ws.log, "merge", "--no-edit", targetRef); err != nil && !isAlreadyUpToDate(err) {
			errMsg := err.Output
			if _, err := git(ctx, ws.log, "merge", "--abort"); err != nil {
				errMsg += "\n" + err.Output
			}
			// Make note of the merge conflict and abort. We'll run all actions and each
			// one will just fail with the merge conflict error.
			ws.setupError = status.FailedPreconditionErrorf(
				"Merge conflict between branches %q and %q.\n\n%s",
				*pushedBranch, *targetBranch, errMsg,
			)
		}
		mergedCommitSHA, err := git(ctx, io.Discard, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		writeCommandSummary(ws.log, "Merged into the target branch %s. HEAD is now at %s.", *targetBranch, mergedCommitSHA)
	}

	if len(*patchURIs) > 0 {
		conn, err := grpc_client.DialSimple(*cacheBackend)
		if err != nil {
			return err
		}
		bsClient := bspb.NewByteStreamClient(conn)
		for _, patchURI := range *patchURIs {
			if err := ws.applyPatch(ctx, bsClient, patchURI); err != nil {
				return err
			}
		}
	}

	return nil
}

func (ws *workspace) shouldMergeBranches(actionTriggers *config.Triggers) bool {
	return actionTriggers.GetPullRequestTrigger().GetMergeWithBase() &&
		ws.hasMultipleBranches()
}

func (ws *workspace) hasMultipleBranches() bool {
	return *targetRepoURL != "" &&
		*targetBranch != "" &&
		(*pushedRepoURL != *targetRepoURL || *pushedBranch != *targetBranch)
}

func (ws *workspace) fetchPushedRef(ctx context.Context) error {
	// By default, try to fetch with --depth=1, to minimize data fetched
	fetchDepth := 1
	// If fetch depth is explicitly set, respect it
	if *gitFetchDepth != smartFetchDepth {
		fetchDepth = *gitFetchDepth
	}

	refToFetch := *commitSHA
	if refToFetch == "" {
		refToFetch = *pushedBranch
	}

	// If the merge commit has not been generated, fetch the full history
	// to ensure the merge base commit is fetched, so we can manually merge the branches
	// TODO(Maggie): Only do this if merge_with_base is enabled
	// If we serialize the action in serializedAction, we won't need to checkout
	// the repo in the ci_runner to read the config
	if ws.hasMultipleBranches() && *gitFetchDepth == smartFetchDepth {
		fetchDepth = 0
	}

	if err := ws.fetch(ctx, *pushedRepoURL, []string{refToFetch}, fetchDepth); err != nil {
		if strings.Contains(err.Error(), "Server does not allow request for unadvertised object") {
			writeCommandSummary(ws.log, "Git does not support fetching non-HEAD commits by default."+
				" You must set the `uploadpack.allowAnySHA1InWant`"+
				" config option in the repo that is being fetched.")
			if refToFetch != *pushedBranch && *pushedBranch != "" {
				writeCommandSummary(ws.log, "Attempting to fetch the branch with --depth=0 instead...")
				refToFetch = *pushedBranch
				fetchDepth = 0
				return ws.fetch(ctx, *pushedRepoURL, []string{refToFetch}, fetchDepth)
			}
		}
		return err
	}
	return nil
}

func (ws *workspace) fetchTargetRef(ctx context.Context) error {
	// Fetch with --depth=0 to ensure the merge base commit is fetched
	fetchDepth := 0
	return ws.fetch(ctx, *targetRepoURL, []string{*targetBranch}, fetchDepth)
}

func (ws *workspace) updateSubmodules(ctx context.Context) error {
	if _, err := os.Stat(".gitmodules"); err != nil {
		if os.IsNotExist(err) {
			// Repo doesn't have submodules.
			return nil
		}
		return fmt.Errorf("stat .gitmodules: %w", err)
	}
	if _, err := git(ctx, ws.log, "submodule", "update", "--init", "--recursive"); err != nil {
		return err
	}
	return nil
}

// checkoutRef checks out a reference that the rest of the remote run should run off
func (ws *workspace) checkoutRef(ctx context.Context) error {
	checkoutLocalBranchName := *pushedBranch
	checkoutRef := *commitSHA
	if checkoutRef == "" {
		checkoutRef = fmt.Sprintf("%s/%s", gitRemoteName(*pushedRepoURL), *pushedBranch)
	}

	if checkoutLocalBranchName != "" {
		// Create the local branch if it doesn't already exist, then update it to point to the checkout ref
		if _, err := git(ctx, ws.log, "checkout", "--force", "-B", checkoutLocalBranchName, checkoutRef); err != nil {
			return err
		}
	} else {
		if _, err := git(ctx, ws.log, "checkout", checkoutRef); err != nil {
			return err
		}
	}
	return nil
}

func (ws *workspace) config(ctx context.Context) error {
	useSystemGitCredentials := os.Getenv("USE_SYSTEM_GIT_CREDENTIALS") == "1"

	// Set up repo-local config.
	cfg := [][]string{
		{"user.email", "ci-runner@buildbuddy.io"},
		{"user.name", "BuildBuddy"},
		{"advice.detachedHead", "false"},
		// With the version of git that we have installed in the CI runner
		// image, --filter=blob:none requires the partialClone extension to be
		// enabled.
		{"extensions.partialClone", "true"},
		// Disable this check for `git fetch` performance improvements
		{"fetch.showForcedUpdates", "false"},
		// Disable automatic gc - it can interfere with running `rm -rf .git` in
		// the case where we don't sync successfully.
		{"gc.auto", "0"},
		// Make "git gc --auto" runs in the foreground so that we don't snapshot
		// the microvm while it's running.
		{"gc.autoDetach", "false"},
	}
	if !useSystemGitCredentials {
		// Disable any credential helpers (in particular, osxkeychain which
		// displays a blocking popup dialog)
		cfg = append(cfg, []string{"credential.helper", ""})
	}

	writeCommandSummary(ws.log, "Configuring repository...")
	for _, kv := range cfg {
		// Don't show the config output.
		if _, err := git(ctx, io.Discard, "config", kv[0], kv[1]); err != nil {
			return err
		}
	}

	// Set up global config (~/.gitconfig) but only on Linux for now since Linux
	// workflows are isolated.
	// TODO(bduffany): find a solution that works for Mac workflows too.
	if !useSystemGitCredentials && runtime.GOOS == "linux" {
		// SSH URL rewrites and git credential helper are used for external git
		// deps fetched by bazel, so these need to be in the global config.
		if err := configureGlobalURLRewrites(ctx); err != nil {
			return err
		}
		if err := configureGlobalCredentialHelper(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (ws *workspace) init(ctx context.Context) error {
	if _, err := git(ctx, ws.log, "init"); err != nil {
		return status.UnknownError("git init failed")
	}
	return nil
}

func (ws *workspace) fetch(ctx context.Context, remoteURL string, refs []string, fetchDepth int) error {
	if len(refs) == 0 {
		return nil
	}

	fetchURL := remoteURL
	useSystemGitCredentials := os.Getenv("USE_SYSTEM_GIT_CREDENTIALS") == "1"
	if !useSystemGitCredentials {
		authURL, err := gitutil.AuthRepoURL(remoteURL, os.Getenv(repoUserEnvVarName), os.Getenv(repoTokenEnvVarName))
		if err != nil {
			return err
		}
		fetchURL = authURL
	}

	remoteName := gitRemoteName(remoteURL)
	writeCommandSummary(ws.log, "Configuring remote %q...", remoteName)

	// Don't show `git remote add` command or the error message since the URL may
	// contain the repo access token.
	if _, err := git(ctx, io.Discard, "remote", "add", remoteName, fetchURL); err != nil {
		// Rename the existing remote. Removing then re-adding would be simpler,
		// but unfortunately that drops the "partialclonefilter" options on the
		// existing remote.
		if isRemoteAlreadyExists(err) {
			if _, err := git(ctx, io.Discard, "remote", "set-url", remoteName, fetchURL); err != nil {
				return err
			}
		} else {
			return status.UnknownErrorf("Command `git remote add %q <url>` failed.", remoteName)
		}
	}
	fetchArgs := []string{"fetch", "--force"}
	for _, filter := range *gitFetchFilters {
		fetchArgs = append(fetchArgs, "--filter="+filter)
	}
	// The --depth option is sticky when fetching the same branch. If --depth
	// was set in a previous snapshot run, make sure to unset it if needed
	if fetchDepth == 0 {
		output, err := git(ctx, io.Discard, "rev-parse", "--is-shallow-repository")
		if err != nil {
			return err
		}
		// If you have never fetched a ref with limited depth, passing --unshallow
		// will fail. By default, it will pull the complete git history.
		if strings.Contains(output, "true") {
			fetchArgs = append(fetchArgs, "--unshallow")
		}
	} else if fetchDepth > 0 {
		fetchArgs = append(fetchArgs, fmt.Sprintf("--depth=%d", fetchDepth))
	}
	fetchArgs = append(fetchArgs, remoteName)
	fetchArgs = append(fetchArgs, refs...)
	if _, err := git(ctx, ws.log, fetchArgs...); err != nil {
		return status.WrapError(err, err.Output)
	}
	return nil
}

// Writes a wrapper script that invokes the ci_runner with the bazel_wrapper subcommand.
// Also adds it to the PATH so it will be invoked whenever `bazel` or `bazelisk` are called.
// The wrapper script adds a startup option for the custom ci_runner .bazelrc to
// all bazel commands.
func (ws *workspace) writeBazelWrapperScript() error {
	wrapperDir := filepath.Join(ws.rootDir, "wrappers")
	for _, c := range []string{bazelBinaryName, bazeliskBinaryName} {
		wrapperPath := filepath.Join(wrapperDir, c)
		_, err := os.Stat(wrapperPath)
		if err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(filepath.Dir(wrapperPath), 0755); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		cmd := fmt.Sprintf(
			"BAZEL_WRAPPER_MODE=1 BAZEL_BIN=%q CI_RUNNER_ROOT=%q exec %s \"$@\"",
			*bazelCommand,
			ws.rootDir,
			os.Getenv("BUILDBUDDY_CI_RUNNER_ABSPATH"),
		)
		contents := `#!/usr/bin/env sh
` + cmd + `
`
		if err := os.WriteFile(wrapperPath, []byte(contents), 0755); err != nil {
			return status.InternalErrorf("Failed to write to %s: %s", wrapperPath, err)
		}
	}

	prevPath := os.Getenv("PATH")
	if !strings.Contains(prevPath, wrapperDir) {
		if err := os.Setenv("PATH", fmt.Sprintf("%s:%s", wrapperDir, prevPath)); err != nil {
			return status.WrapError(err, "failed to include wrapper dir in PATH")
		}
	}

	return nil
}

type commandError struct {
	Err    error
	Output string
}

func (e *commandError) Error() string {
	return e.Err.Error()
}

func isRemoteAlreadyExists(err error) bool {
	gitErr, ok := err.(*commandError)
	return ok && strings.Contains(gitErr.Output, "already exists")
}
func isBranchNotFound(err error) bool {
	gitErr, ok := err.(*commandError)
	return ok && strings.Contains(gitErr.Output, "not found")
}
func isAlreadyUpToDate(err error) bool {
	gitErr, ok := err.(*commandError)
	return ok && strings.Contains(gitErr.Output, "up to date")
}

func git(ctx context.Context, out io.Writer, args ...string) (string, *commandError) {
	if err := printCommandLine(out, "git", args...); err != nil {
		return "", &commandError{err, ""}
	}
	return runCommandWithOutput(ctx, "git", args, map[string]string{} /*=env*/, "" /*=dir*/, out)
}

func isPushedRefInFork() bool {
	return *targetRepoURL != "" && *pushedRepoURL != *targetRepoURL
}

// Returns the base URL used for status reporting. For PRs from forks, this
// will be the repo URL into which the PR is being merged.
func baseRepoURL() string {
	if isPushedRefInFork() {
		return *targetRepoURL
	}
	return *pushedRepoURL
}

func formatNowUTC() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05.000 UTC")
}

func writeCommandSummary(out io.Writer, format string, args ...interface{}) {
	io.WriteString(out, ansiGray+formatNowUTC()+ansiReset+" ")
	io.WriteString(out, fmt.Sprintf(format, args...))
	io.WriteString(out, "\n")
}

func invocationURL(invocationID string) string {
	urlPrefix := *besResultsURL
	if !strings.HasSuffix(urlPrefix, "/") {
		urlPrefix = urlPrefix + "/"
	}
	return urlPrefix + invocationID
}

func writeBazelrc(path, invocationID, runID, rootDir string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0664)
	if err != nil {
		return status.InternalErrorf("Failed to open %s for writing: %s", path, err)
	}
	defer f.Close()

	lines := []string{
		"common --build_metadata=PARENT_INVOCATION_ID=" + invocationID,
		"common --build_metadata=PARENT_RUN_ID=" + runID,
		// Note: these pieces of metadata are set to match the WorkspaceStatus event
		// for the outer (workflow) invocation.
		"common --build_metadata=COMMIT_SHA=" + *commitSHA,
		"common --build_metadata=REPO_URL=" + baseRepoURL(),
		"common --build_metadata=BRANCH_NAME=" + *pushedBranch, // corresponds to GIT_BRANCH status key
		// Don't report commit statuses for individual bazel commands, since the
		// overall status of all bazel commands is reflected in the status reported
		// for the workflow invocation. In addition, for PRs, we first merge with
		// the target branch which causes the HEAD commit SHA to change, and this
		// SHA won't actually exist on GitHub.
		"common --build_metadata=DISABLE_COMMIT_STATUS_REPORTING=true",
		"common --bes_backend=" + *besBackend,
		"common --bes_results_url=" + *besResultsURL,
		// Dump Bazel's heap on OOM - we'll upload this file as a workflow
		// artifact for easier debugging.
		"common --heap_dump_on_oom",
	}
	isWorkflow := *workflowID != ""
	if isWorkflow {
		lines = append(lines, "common --build_metadata=WORKFLOW_ID="+*workflowID)
		lines = append(lines, "common --build_metadata=ROLE=CI")
	}
	if !isWorkflow || *prNumber != 0 {
		lines = append(lines, "common --build_metadata=DISABLE_TARGET_TRACKING=true")
	}
	if *prNumber != 0 {
		lines = append(lines, "common --build_metadata=PULL_REQUEST_NUMBER="+fmt.Sprintf("%d", *prNumber))
	}
	if isPushedRefInFork() {
		lines = append(lines, "common --build_metadata=FORK_REPO_URL="+*pushedRepoURL)
	}
	if apiKey := os.Getenv(buildbuddyAPIKeyEnvVarName); apiKey != "" {
		lines = append(lines, "common --remote_header=x-buildbuddy-api-key="+apiKey)
		lines = append(lines, "build:buildbuddy_api_key --remote_header=x-buildbuddy-api-key="+apiKey)
	}
	if origin := os.Getenv("BB_GRPC_CLIENT_ORIGIN"); origin != "" {
		lines = append(lines, "common --remote_header=x-buildbuddy-origin="+origin)
		lines = append(lines, "common --bes_header=x-buildbuddy-origin="+origin)
	}
	if identity := os.Getenv(clientIdentityEnvVar); identity != "" {
		lines = append(lines, "common --remote_header=x-buildbuddy-client-identity="+identity)
		lines = append(lines, "common --bes_header=x-buildbuddy-client-identity="+identity)
	}

	// These configs point to the same env that triggered the remote run
	// (i.e. if the remote run was triggered in dev, they point to the dev app).
	// They are all prefixed with "buildbuddy_" to avoid conflicting
	// with existing .bazelrc configs in the wild.
	lines = append(lines, []string{
		"common:buildbuddy_bes_backend --bes_backend=" + *besBackend,
		"common:buildbuddy_bes_results_url --bes_results_url=" + *besResultsURL,
	}...)
	if *cacheBackend != "" {
		lines = append(lines, "common:buildbuddy_remote_cache --remote_cache="+*cacheBackend)
		lines = append(lines, "common:buildbuddy_experimental_remote_downloader --experimental_remote_downloader="+*cacheBackend)
	}
	if *rbeBackend != "" {
		lines = append(lines, "common:buildbuddy_remote_executor --remote_executor="+*rbeBackend)
	}

	outputBase := filepath.Join(rootDir, outputBaseDirName)
	lines = append(lines, "startup --output_base="+outputBase)
	startupFlags, err := shlex.Split(*bazelStartupFlags)
	if err != nil {
		return status.WrapError(err, "failed to split --bazel_startup_flags")
	}
	for _, s := range startupFlags {
		lines = append(lines, "startup "+s)
	}

	extras, err := shlex.Split(*extraBazelArgs)
	if err != nil {
		return status.WrapError(err, "failed to split --extra_bazel_args")
	}
	for _, e := range extras {
		lines = append(lines, "common "+e)
	}

	contents := strings.Join(lines, "\n") + "\n"

	if _, err := io.WriteString(f, contents); err != nil {
		return status.InternalErrorf("Failed to append to %s: %s", path, err)
	}
	return nil
}

func readConfig() (*config.BuildBuddyConfig, error) {
	f, err := os.Open(config.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Note: targetRepoDefaultBranch is only used to compute triggers,
			// but we don't read triggers in the CI runner (since the runner is
			// already triggered). So we exclude the default branch here.
			return config.GetDefault("" /*=targetRepoDefaultBranch*/), nil
		}
		return nil, status.FailedPreconditionErrorf("open %q: %s", config.FilePath, err)
	}
	c, err := config.NewConfig(f)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("read %q: %s", config.FilePath, err)
	}
	return c, nil
}

func gitRemoteName(repoURL string) string {
	if repoURL == *targetRepoURL || *targetRepoURL == "" {
		return defaultGitRemoteName
	}
	return forkGitRemoteName
}

func runBashCommand(ctx context.Context, cmd string, env map[string]string, dir string, outputSink io.Writer) error {
	if err := printCommandLine(outputSink, cmd); err != nil {
		return err
	}

	return runCommand(ctx, "bash", []string{"-eo", "pipefail", "-c", cmd}, env, dir, outputSink)
}

func runCommandWithOutput(ctx context.Context, executable string, args []string, env map[string]string, dir string, outputSink io.Writer) (string, *commandError) {
	var buf bytes.Buffer
	w := io.MultiWriter(outputSink, &buf)

	if err := runCommand(ctx, executable, args, env, dir, w); err != nil {
		return "", &commandError{err, buf.String()}
	}
	output := buf.String()
	return strings.TrimSpace(output), nil
}

func runCommand(ctx context.Context, executable string, args []string, env map[string]string, dir string, outputSink io.Writer) error {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	if dir != "" {
		cmd.Dir = dir
	}
	size := &pty.Winsize{Rows: uint16(*ptyRows), Cols: uint16(*ptyCols)}
	f, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return err
	}
	defer f.Close()
	copyOutputDone := make(chan struct{})
	go func() {
		io.Copy(outputSink, f)
		copyOutputDone <- struct{}{}
	}()
	err = cmd.Wait()
	<-copyOutputDone

	if ctxErr := ctx.Err(); ctxErr == context.DeadlineExceeded {
		_, _ = outputSink.Write([]byte(fmt.Sprintf("Remote run exceeded timeout (%s). Aborting...", timeout.String())))
	}

	if err != nil {
		_, _ = outputSink.Write([]byte(aurora.Sprintf(aurora.Red("Command failed: %s\n"), err)))
	}

	return err
}

func getExitCode(err error) int {
	if err == nil {
		return 0
	}
	if commandError, ok := err.(*commandError); ok {
		if commandError == nil {
			return 0
		}
		err = commandError.Err
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode()
	}
	return noExitCode
}

func actionDebugString(action *config.Action) string {
	yamlBytes, err := yaml.Marshal(action)
	if err != nil {
		return fmt.Sprintf("<failed to marshal action: %s>", err)
	}
	return string(yamlBytes)
}

func bazelWorkspacePath(cfg *config.BuildBuddyConfig) string {
	for _, a := range cfg.Actions {
		if a.Name == *actionName {
			return a.BazelWorkspaceDir
		}
	}
	return ""
}

func newUUID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", status.UnavailableError("failed to generate UUID")
	}
	return id.String(), nil
}

func toShellToken(s string) string {
	if shellCharsRequiringQuote.MatchString(s) {
		s = "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
	}
	return s
}

func getStructuredCommandLine() *clpb.CommandLine {
	options := make([]*clpb.Option, 0, len(os.Args[1:]))
	for _, arg := range os.Args[1:] {
		// TODO: Handle other arg formats ("-name=value", "--name value",
		// "--bool_switch", etc). Ignore these for now since we don't set
		// them in practice.
		if !strings.HasPrefix(arg, "--") || !strings.Contains(arg, "=") {
			continue
		}
		arg = strings.TrimPrefix(arg, "--")
		parts := strings.SplitN(arg, "=", 2)
		options = append(options, &clpb.Option{
			CombinedForm: arg,
			OptionName:   parts[0],
			OptionValue:  parts[1],
		})
	}
	return &clpb.CommandLine{
		CommandLineLabel: "original",
		Sections: []*clpb.CommandLineSection{
			{
				SectionLabel: "executable",
				SectionType: &clpb.CommandLineSection_ChunkList{ChunkList: &clpb.ChunkList{
					Chunk: []string{os.Args[0]},
				}},
			},
			{
				SectionLabel: "command options",
				SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{
					Option: options,
				}},
			},
		},
	}
}

func configureGlobalCredentialHelper(ctx context.Context) error {
	if !strings.HasPrefix(baseRepoURL(), "https://") {
		return nil
	}
	repoToken := os.Getenv(repoTokenEnvVarName)
	if repoToken == "" {
		return nil
	}
	u, err := url.Parse(baseRepoURL())
	if err != nil {
		return nil // if URL is unparseable, do nothing
	}
	repoHostURL := fmt.Sprintf("https://%s", u.Host)
	configKey := fmt.Sprintf("credential.%s.helper", repoHostURL)
	configValue := fmt.Sprintf("!%s --credential_helper", os.Getenv("BUILDBUDDY_CI_RUNNER_ABSPATH"))
	if _, err := git(ctx, io.Discard, "config", "--global", configKey, configValue); err != nil {
		return err
	}
	return nil
}

// Replaces some known SSH git URLs with HTTPS equivalents, since we won't
// necessarily have any SSH private keys available that we can use to clone
// repos over SSH.
func configureGlobalURLRewrites(ctx context.Context) error {
	// Discard any existing rewrites initially, then append.
	flag := "--replace-all"
	for original, replacement := range map[string]string{
		"ssh://git@github.com:": "https://github.com/",
		"git@github.com:":       "https://github.com/",
	} {
		if _, err := git(ctx, io.Discard, "config", "--global", flag, "url."+replacement+".insteadOf", original); err != nil {
			return err
		}
		flag = "--add"
	}
	return nil
}

func runCredentialHelper() error {
	// Do nothing with request details on stdin for now. We only configure the
	// credential helper for the target repo URL, so the details should always
	// match the target repo.
	io.Copy(io.Discard, os.Stdin)

	cmd := flag.Args()[0]
	switch cmd {
	case "get":
		username := os.Getenv(repoUserEnvVarName)
		if username == "" {
			// git requires a username from the cred helper but GitHub doesn't
			// care what the username is (it just looks at the token), so just
			// set an arbitrary username.
			username = "x-access-token"
		}
		fmt.Printf("username=%s\npassword=%s\n", username, os.Getenv(repoTokenEnvVarName))
		return nil
	default:
		// Do nothing
		return nil
	}
}

func runBazelWrapper() error {
	rootPath := os.Getenv("CI_RUNNER_ROOT")
	bazelBin := os.Getenv("BAZEL_BIN")

	// These arguments are passed as env vars so we don't have to parse out flags
	// intended for the bazel wrapper from startup options intended to be passed through
	// to bazel.
	// Unset these env vars so we don't start depending on them unnecessarily.
	if err := os.Unsetenv("CI_RUNNER_ROOT"); err != nil {
		return err
	}
	if err := os.Unsetenv("BAZEL_BIN"); err != nil {
		return err
	}

	// Get the current bazel workspace path where we expect to find the
	// workspace rc file.
	workspacePath, err := currentBazelWorkspaceAbsPath()
	if err != nil && !status.IsNotFoundError(err) {
		return fmt.Errorf("find bazel workspace: %w", err)
	}

	originalArgs := os.Args[1:]

	// Pass the original command as metadata, stripping the custom flags we've set,
	// so that it can be displayed in the UI
	filteredOriginalArgs := make([]string, 0, len(originalArgs))
	for i, arg := range originalArgs {
		if i == 0 && (arg == bazelBinaryName || arg == bazeliskBinaryName) {
			continue
		}
		if strings.Contains(arg, "--invocation_id") ||
			strings.Contains(arg, "--remote_header=") ||
			strings.Contains(arg, "--config=buildbuddy_remote_cache") ||
			strings.Contains(arg, "--config=buildbuddy_bes_results_url") ||
			strings.Contains(arg, "--config=buildbuddy_bes_backend") {
			continue
		}
		filteredOriginalArgs = append(filteredOriginalArgs, arg)
	}
	originalArgsJSON, err := json.Marshal(filteredOriginalArgs)
	if err != nil {
		return err
	}
	metadataFlag := "--build_metadata=EXPLICIT_COMMAND_LINE=" + string(originalArgsJSON)

	startupArgs, err := customBazelrcOptions(rootPath, workspacePath)
	if err != nil {
		return err
	}

	bazelCmd := append([]string{bazelBin}, append(startupArgs, originalArgs...)...)
	bazelCmd = appendBazelSubcommandArgs(bazelCmd, metadataFlag)

	// Replace the process running the bazel wrapper with the process running bazel,
	// so there are no remaining traces of the wrapper script.
	return syscall.Exec(bazelBin, bazelCmd, os.Environ())
}

// Attempts to free up disk space.
func (ws *workspace) reclaimDiskSpace(ctx context.Context) error {
	// We should be in the git repo root at this point - run git gc.
	ws.log.Printf("Running git maintenance...")
	if err := ws.runGitMaintenance(ctx); err != nil {
		ws.log.Printf("WARNING: git maintenance failed: %s", err)
	}

	// TODO: attempt to clean up old bazel cache objects?

	// If we still have high disk usage after cleaning, print some debug info so
	// that we can see where the disk usage is coming from.
	usageStats, err := diskUsage()
	if err != nil {
		return fmt.Errorf("get disk usage: %s", err)
	}
	if usageStats.usageFraction < highDiskUsageThreshold {
		return nil
	}
	// Just print a few dirs for now so this doesn't take excessively long.
	ws.log.Printf("WARNING: high VM disk usage (%.2f%%)", usageStats.usageFraction*100)
	duArgs := []string{"--human-readable", "--max-depth=1", ".", filepath.Join("..", outputBaseDirName)}
	if err = runCommand(ctx, "du", duArgs, nil /*=env*/, "" /*=dir*/, ws.log); err != nil {
		return fmt.Errorf("du: %w", err)
	}

	return nil
}

// Creates a marker file that prevents the runner from being recycled if bazel
// still has the workspace lock.
func (ws *workspace) checkBazelWorkspaceLock(ctx context.Context) error {
	var buf bytes.Buffer
	bazelWorkspacePath, err := ws.bazelWorkspacePath()
	if err != nil {
		return fmt.Errorf("get bazel workspace path: %s", err)
	}
	startupArgs, err := customBazelrcOptions(ws.rootDir, bazelWorkspacePath)
	if err != nil {
		return fmt.Errorf("get bazel command: %w", err)
	}
	// 'bazel --noblock_for_lock info workspace' should either succeed quickly
	// if the workspace lock is not held, or fail quickly if it is held.
	bazelArgs := append(startupArgs, "--noblock_for_lock", "info", "workspace")
	if err := runCommand(ctx, *bazelCommand, bazelArgs, nil, bazelWorkspacePath, &buf); err != nil {
		return fmt.Errorf("%w: %s", err, buf.String())
	}
	return nil
}

func (ws *workspace) runGitMaintenance(ctx context.Context) error {
	// TODO: switch to git maintenance once it's more widely available.
	initialUsage, err := diskUsage()
	if err != nil {
		return fmt.Errorf("get disk usage: %s", err)
	}
	if _, err := git(ctx, ws.log, "gc", "--auto"); err != nil {
		return fmt.Errorf("git gc: %w", err)
	}
	postCleanupUsage, err := diskUsage()
	if err != nil {
		return fmt.Errorf("get disk usage: %s", err)
	}
	freedDiskMb := (initialUsage.usedBytes - postCleanupUsage.usedBytes) / 1e6
	ws.log.Printf("%sGit maintenance cleaned %vMB %s", ansiGray, freedDiskMb, ansiReset)
	return nil
}

type diskUsageStats struct {
	usageFraction float64
	usedBytes     int64
}

func diskUsage() (*diskUsageStats, error) {
	df, err := disk.GetDirUsage(".")
	if err != nil {
		return nil, err
	}
	if df.TotalBytes == 0 {
		return &diskUsageStats{
			usageFraction: 0,
			usedBytes:     0,
		}, nil
	}
	usedBytes := df.TotalBytes - df.AvailBytes
	return &diskUsageStats{
		usageFraction: float64(usedBytes) / float64(df.TotalBytes),
		usedBytes:     int64(usedBytes),
	}, nil
}
