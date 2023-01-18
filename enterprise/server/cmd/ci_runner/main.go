package main

import (
	"bytes"
	"context"
	"flag"
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
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/logrusorgru/aurora"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v2"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
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

	defaultGitRemoteName = "origin"
	forkGitRemoteName    = "fork"

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
	progressFlushInterval = 1 * time.Second
	// progressFlushThresholdBytes specifies how full the log buffer
	// should be before we force a flush, regardless of the flush interval.
	progressFlushThresholdBytes = 1_000

	// Bazel binary constants

	bazelBinaryName    = "bazel"
	bazeliskBinaryName = "bazelisk"

	// ANSI codes for cases where the aurora equivalent is not supported by our UI
	// (ex: aurora's "grayscale" mode results in some ANSI codes that we don't currently
	// parse correctly).

	ansiGray  = "\033[90m"
	ansiReset = "\033[0m"
)

var (
	besBackend         = flag.String("bes_backend", "", "gRPC endpoint for BuildBuddy's BES backend.")
	cacheBackend       = flag.String("cache_backend", "", "gRPC endpoint for BuildBuddy Cache.")
	rbeBackend         = flag.String("rbe_backend", "", "gRPC endpoint for BuildBuddy RBE.")
	besResultsURL      = flag.String("bes_results_url", "", "URL prefix for BuildBuddy invocation URLs.")
	remoteInstanceName = flag.String("remote_instance_name", "", "Remote instance name used to retrieve patches.")
	triggerEvent       = flag.String("trigger_event", "", "Event type that triggered the action runner.")
	pushedRepoURL      = flag.String("pushed_repo_url", "", "URL of the pushed repo.")
	pushedBranch       = flag.String("pushed_branch", "", "Branch name of the commit to be checked out.")
	commitSHA          = flag.String("commit_sha", "", "Commit SHA to report statuses for.")
	targetRepoURL      = flag.String("target_repo_url", "", "URL of the target repo.")
	targetBranch       = flag.String("target_branch", "", "Branch to check action triggers against.")
	targetCommitSHA    = flag.String("target_commit_sha", "", "If set, target repo URL is checked out at the given commit instead of the tip of the branch.")
	workflowID         = flag.String("workflow_id", "", "ID of the workflow associated with this CI run.")
	actionName         = flag.String("action_name", "", "If set, run the specified action and *only* that action, ignoring trigger conditions.")
	invocationID       = flag.String("invocation_id", "", "If set, use the specified invocation ID for the workflow action. Ignored if action_name is not set.")
	visibility         = flag.String("visibility", "", "If set, use the specified value for VISIBILITY build metadata for the workflow invocation.")
	bazelSubCommand    = flag.String("bazel_sub_command", "", "If set, run the bazel command specified by these args and ignore all triggering and configured actions.")
	patchDigests       = flagutil.New("patch_digest", []string{}, "Digests of patches to apply to the repo after checkout. Can be specified multiple times to apply multiple patches.")
	recordRunMetadata  = flag.Bool("record_run_metadata", false, "Instead of running a target, extract metadata about it and report it in the build event stream.")
	gitCleanExclude    = flagutil.New("git_clean_exclude", []string{}, "Directories to exclude from `git clean` while setting up the repo.")

	shutdownAndExit = flag.Bool("shutdown_and_exit", false, "If set, runs bazel shutdown with the configured bazel_command, and exits. No other commands are run.")

	bazelCommand      = flag.String("bazel_command", "", "Bazel command to use.")
	bazelStartupFlags = flag.String("bazel_startup_flags", "", "Startup flags to pass to bazel. The value can include spaces and will be properly tokenized.")
	extraBazelArgs    = flag.String("extra_bazel_args", "", "Extra flags to pass to the bazel command. The value can include spaces and will be properly tokenized.")
	debug             = flag.Bool("debug", false, "Print additional debug information in the action logs.")

	// Test-only flags
	fallbackToCleanCheckout = flag.Bool("fallback_to_clean_checkout", true, "Fallback to cloning the repo from scratch if sync fails (for testing purposes only).")

	shellCharsRequiringQuote = regexp.MustCompile(`[^\w@%+=:,./-]`)
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
	//         output-base/         (bazel output base)
	//         repo-root/           (cloned git repo)
	//             .git/
	//             WORKSPACE
	//             buildbuddy.yaml  (optional workflow config)
	//             ...
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

	// An error that occurred while setting up the workspace, which should be
	// reported for all action logs instead of actually executing the action.
	setupError error

	// The start time of the setup phase.
	startTime time.Time

	// log contains logs from the workspace setup phase (cloning the git repo and
	// deciding which actions to run), which are reported as part of the first
	// action's logs.
	log io.Writer
}

type buildEventReporter struct {
	isWorkflow bool
	apiKey     string
	bep        *build_event_publisher.Publisher
	log        *invocationLog

	invocationID          string
	startTime             time.Time
	cancelBackgroundFlush func()

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
	return &buildEventReporter{apiKey: apiKey, bep: bep, log: newInvocationLog(), invocationID: iid, isWorkflow: isWorkflow}, nil
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
	patterns := []string{}
	if !r.isWorkflow {
		parsedArgs, err := parseBazelArgs(*bazelSubCommand)
		if err != nil {
			return err
		}
		cmd = parsedArgs.cmd
		patterns = parsedArgs.patterns
	}
	startedEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
			{Id: &bespb.BuildEventId_ChildInvocationsConfigured{ChildInvocationsConfigured: &bespb.BuildEventId_ChildInvocationsConfiguredId{}}},
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 0}}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
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
	} else {
		startedEvent.Children = append(startedEvent.Children, &bespb.BuildEventId{Id: &bespb.BuildEventId_Pattern{Pattern: &bespb.BuildEventId_PatternExpandedId{Pattern: patterns}}})
	}
	if err := r.bep.Publish(startedEvent); err != nil {
		return err
	}
	if !r.isWorkflow {
		patternEvent := &bespb.BuildEvent{
			Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_Pattern{Pattern: &bespb.BuildEventId_PatternExpandedId{Pattern: patterns}}},
			Payload: &bespb.BuildEvent_Expanded{Expanded: &bespb.PatternExpanded{}},
		}
		if err := r.bep.Publish(patternEvent); err != nil {
			return err
		}
	}

	// Flush whenever the log buffer fills past a certain threshold.
	r.log.writeListener = func() {
		if size := r.log.Len(); size >= progressFlushThresholdBytes {
			r.FlushProgress() // ignore error; it will surface in `bep.Finish()`
		}
	}
	stopFlushingProgress := r.startBackgroundProgressFlush()
	r.cancelBackgroundFlush = stopFlushingProgress
	return nil
}

func (r *buildEventReporter) Stop(exitCode int, exitCodeName string) error {
	if r.cancelBackgroundFlush != nil {
		r.cancelBackgroundFlush()
		r.cancelBackgroundFlush = nil
	}

	r.FlushProgress()
	now := time.Now()

	r.Publish(&bespb.BuildEvent{
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
	elapsedTimeSeconds := float64(time.Since(r.startTime)) / float64(time.Second)
	// NB: This is the last message -- if more are added afterwards, be sure to
	// update the `LastMessage` flag
	r.Publish(&bespb.BuildEvent{
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
				break
			case <-time.After(progressFlushInterval):
				r.FlushProgress()
			}
		}
	}()
	return func() {
		stop <- struct{}{}
	}
}

func main() {
	if err := run(); err != nil {
		if result, ok := err.(*actionResult); ok {
			os.Exit(result.exitCode)
		}
		log.Errorf("%s", err)
		os.Exit(int(gstatus.Code(err)))
	}
}

func run() error {
	flag.Parse()

	ws := &workspace{
		startTime:          time.Now(),
		buildbuddyAPIKey:   os.Getenv(buildbuddyAPIKeyEnvVarName),
		forcedInvocationID: *invocationID,
	}

	ctx := context.Background()
	if ws.buildbuddyAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, ws.buildbuddyAPIKey)
	}

	buildEventReporter, err := newBuildEventReporter(ctx, *besBackend, ws.buildbuddyAPIKey, *invocationID, *workflowID != "" /*=isWorkflow*/)
	if err != nil {
		return err
	}

	// Write setup logs to the current task's stderr (to make debugging easier),
	// and also to the invocation.
	ws.log = io.MultiWriter(os.Stderr, buildEventReporter)
	ws.hostname, ws.username = getHostAndUserName()

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
	// Write default bazelrc
	if err := writeBazelrc(buildbuddyBazelrcPath, buildEventReporter.invocationID); err != nil {
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
			log.Error(err.Error())
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

	if *shutdownAndExit {
		log.Info("--shutdown_and_exit requested; will run bazel shutdown then exit.")
		if _, err := os.Stat(repoDirName); err != nil {
			log.Info("Workspace does not exist; exiting.")
			return nil
		}
		if err := os.Chdir(repoDirName); err != nil {
			return err
		}
		cfg, err := readConfig()
		if err != nil {
			log.Warningf("Failed to read BuildBuddy config; will run `bazel shutdown` from repo root: %s", err)
		}
		wsPath := ""
		if cfg != nil {
			wsPath = bazelWorkspacePath(cfg)
		}
		args, err := bazelArgs(rootDir, wsPath, "shutdown")
		if err != nil {
			return err
		}
		printCommandLine(os.Stderr, *bazelCommand, args...)
		if err := runCommand(ctx, *bazelCommand, args, nil, wsPath, os.Stderr); err != nil {
			return err
		}
		log.Info("Shutdown complete.")
		return nil
	}

	if err := buildEventReporter.Start(ws.startTime); err != nil {
		return status.WrapError(err, "could not publish started event")
	}
	if err := ws.setup(ctx); err != nil {
		_ = buildEventReporter.Stop(noExitCode, failedExitCodeName)
		return status.WrapError(err, "failed to set up git repo")
	}
	cfg, err := readConfig()
	if err != nil {
		_ = buildEventReporter.Stop(noExitCode, failedExitCodeName)
		return status.WrapError(err, "failed to read BuildBuddy config")
	}

	var action *config.Action
	if *bazelSubCommand != "" {
		action = &config.Action{
			Name: "run",
			BazelCommands: []string{
				*bazelSubCommand,
			},
		}
	} else if *actionName != "" {
		// If a specific action was specified, filter to configured
		// actions with a matching action name.
		action, err = findAction(cfg.Actions, *actionName)
		if err != nil {
			return err
		}
	} else {
		return status.InvalidArgumentError("One of --action or --bazel_sub_command must be specified.")
	}

	result, err := ws.RunAction(ctx, action, buildEventReporter)
	if err != nil {
		return err
	}
	if err := buildEventReporter.Stop(result.exitCode, result.exitCodeName); err != nil {
		return err
	}
	if result.exitCode != 0 {
		return result // as error
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
func (ws *workspace) RunAction(ctx context.Context, action *config.Action, buildEventReporter *buildEventReporter) (*actionResult, error) {
	// NB: Anything logged to `ar.log` gets output to both the stdout of this binary
	// and the logs uploaded to BuildBuddy for this action. Anything that we want
	// the user to see in the invocation UI needs to go in that log, instead of
	// the global `log.Print`.
	ar := &actionRunner{
		isWorkflow: *workflowID != "",
		action:     action,
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
	writeListener func()
}

func newInvocationLog() *invocationLog {
	invLog := &invocationLog{writeListener: func() {}}
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, os.Stderr)
	return invLog
}

func (invLog *invocationLog) Write(b []byte) (int, error) {
	n, err := invLog.writer.Write(b)
	invLog.writeListener()
	return n, err
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
	action     *config.Action
	reporter   *buildEventReporter
	rootDir    string
	username   string
	hostname   string
}

func (ar *actionRunner) Run(ctx context.Context, ws *workspace) error {
	ar.reporter.Printf("Running action %q", ar.action.Name)

	// Only print this to the local logs -- it's mostly useful for development purposes.
	log.Infof("Invocation URL:  %s", invocationURL(ar.reporter.InvocationID()))

	// NOTE: In this func we return immediately with an error of nil if event publishing fails,
	// because that error is instead surfaced in the caller func when calling
	// `buildEventPublisher.Wait()`

	wfc := &bespb.WorkflowConfigured{
		WorkflowId:         *workflowID,
		ActionName:         ar.action.Name,
		ActionTriggerEvent: *triggerEvent,
		PushedRepoUrl:      *pushedRepoURL,
		PushedBranch:       *pushedBranch,
		CommitSha:          *commitSHA,
		TargetRepoUrl:      *targetRepoURL,
		TargetBranch:       *targetBranch,
		Os:                 runtime.GOOS,
		Arch:               runtime.GOARCH,
		ContainerImage:     ar.action.ContainerImage,
	}
	wfcEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowConfigured{WorkflowConfigured: &bespb.BuildEventId_WorkflowConfiguredId{}}},
		Payload: &bespb.BuildEvent_WorkflowConfigured{WorkflowConfigured: wfc},
	}

	cic := &bespb.ChildInvocationsConfigured{}
	cicEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_ChildInvocationsConfigured{ChildInvocationsConfigured: &bespb.BuildEventId_ChildInvocationsConfiguredId{}}},
		Payload: &bespb.BuildEvent_ChildInvocationsConfigured{ChildInvocationsConfigured: cic},
	}
	// If the triggering commit merges cleanly with the target branch, the runner
	// will execute the configured bazel commands. Otherwise, the runner will
	// exit early without running those commands and does not need to create
	// invocation streams for them.
	if ws.setupError == nil {
		for _, bazelCmd := range ar.action.BazelCommands {
			iid, err := newUUID()
			if err != nil {
				return err
			}
			wfc.Invocation = append(wfc.Invocation, &bespb.WorkflowConfigured_InvocationMetadata{
				InvocationId: iid,
				BazelCommand: bazelCmd,
			})
			wfcEvent.Children = append(wfcEvent.Children, &bespb.BuildEventId{
				Id: &bespb.BuildEventId_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.BuildEventId_WorkflowCommandCompletedId{
					InvocationId: iid,
				}},
			})
			cic.Invocation = append(cic.Invocation, &bespb.ChildInvocationsConfigured_InvocationMetadata{
				InvocationId: iid,
				BazelCommand: bazelCmd,
			})
			cicEvent.Children = append(cicEvent.Children, &bespb.BuildEventId{
				Id: &bespb.BuildEventId_ChildInvocationCompleted{ChildInvocationCompleted: &bespb.BuildEventId_ChildInvocationCompletedId{
					InvocationId: iid,
				}},
			})
		}
	}
	if ar.isWorkflow {
		if err := ar.reporter.Publish(wfcEvent); err != nil {
			return nil
		}
	}
	if err := ar.reporter.Publish(cicEvent); err != nil {
		return nil
	}

	buildMetadata := &bespb.BuildMetadata{
		Metadata: map[string]string{},
	}
	if ar.isWorkflow {
		buildMetadata.Metadata["ROLE"] = "CI_RUNNER"
	} else {
		buildMetadata.Metadata["ROLE"] = "HOSTED_BAZEL"
	}
	if *visibility != "" {
		buildMetadata.Metadata["VISIBILITY"] = *visibility
	}
	buildMetadataEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
	}
	if err := ar.reporter.Publish(buildMetadataEvent); err != nil {
		return nil
	}

	workspaceStatusEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bespb.WorkspaceStatus{
			Item: []*bespb.WorkspaceStatus_Item{
				{Key: "BUILD_USER", Value: ar.username},
				{Key: "BUILD_HOST", Value: ar.hostname},
				{Key: "GIT_BRANCH", Value: *pushedBranch},
				{Key: "GIT_TREE_STATUS", Value: "Clean"},
				// Note: COMMIT_SHA may not actually reflect the current state of the
				// repo since we merge the target branch before running the workflow;
				// we set this for the purpose of reporting statuses to GitHub.
				{Key: "COMMIT_SHA", Value: *commitSHA},
				// REPO_URL is used to report statuses, so always set it to the
				// target repo URL (which should be the same URL on which the workflow
				// is configured).
				{Key: "REPO_URL", Value: *targetRepoURL},
			},
		}},
	}
	if err := ar.reporter.Publish(workspaceStatusEvent); err != nil {
		return nil
	}

	if ws.setupError != nil {
		return ws.setupError
	}

	for i, bazelCmd := range ar.action.BazelCommands {
		cmdStartTime := time.Now()

		if i >= len(wfc.GetInvocation()) {
			return status.InternalErrorf("No invocation metadata generated for bazel_commands[%d]; this should never happen", i)
		}
		iid := wfc.GetInvocation()[i].GetInvocationId()
		args, err := bazelArgs(ar.rootDir, ar.action.BazelWorkspaceDir, bazelCmd)
		if err != nil {
			return status.InvalidArgumentErrorf("failed to parse bazel command: %s", err)
		}
		printCommandLine(ar.reporter, *bazelCommand, args...)
		// Transparently set the invocation ID from the one we computed ahead of
		// time. The UI is expecting this invocation ID so that it can render a
		// BuildBuddy invocation URL for each bazel_command that is executed.
		args = appendBazelSubcommandArgs(args, fmt.Sprintf("--invocation_id=%s", iid))

		// Instead of actually running the target, have Bazel write out a run script using the --script_path flag and
		// extract run options (i.e. args, runfile information) from the generated run script.
		runScript := ""
		if *recordRunMetadata {
			tmpDir, err := os.MkdirTemp("", "bazel-run-script-*")
			if err != nil {
				return err
			}
			defer os.RemoveAll(tmpDir)
			runScript = filepath.Join(tmpDir, "run.sh")
			args = appendBazelSubcommandArgs(args, "--script_path="+runScript)
		}

		runErr := runCommand(ctx, *bazelCommand, expandEnv(args), ar.action.Env, ar.action.BazelWorkspaceDir, ar.reporter)
		exitCode := getExitCode(runErr)
		if exitCode != noExitCode {
			ar.reporter.Printf("%s(command exited with code %d)%s\n", ansiGray, exitCode, ansiReset)
		}

		// If this is a successfully "bazel run" invocation from which we are extracting run information via
		// --script_path, go ahead and extract run information from the script and send it via the event stream.
		if exitCode == 0 && runScript != "" {
			runInfo, err := processRunScript(ctx, runScript)
			if err != nil {
				return err
			}
			e := &bespb.BuildEvent{
				Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_RunTargetAnalyzed{}},
				Payload: &bespb.BuildEvent_RunTargetAnalyzed{RunTargetAnalyzed: &bespb.RunTargetAnalyzed{
					Arguments:          runInfo.args,
					RunfilesRoot:       runInfo.runfilesRoot,
					Runfiles:           runInfo.runfiles,
					RunfileDirectories: runInfo.runfileDirs,
				}},
			}
			if err := ar.reporter.Publish(e); err != nil {
				break
			}
		}

		// Publish the status of each command as well as the finish time.
		// Stop execution early on BEP failure, but ignore error -- it will surface in `bep.Finish()`.
		duration := time.Since(cmdStartTime)
		completedEvent := &bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.BuildEventId_WorkflowCommandCompletedId{
				InvocationId: iid,
			}}},
			Payload: &bespb.BuildEvent_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.WorkflowCommandCompleted{
				ExitCode:  int32(exitCode),
				StartTime: timestamppb.New(cmdStartTime),
				Duration:  durationpb.New(duration),
			}},
		}
		if err := ar.reporter.Publish(completedEvent); err != nil {
			break
		}
		duration = time.Since(cmdStartTime)
		childCompletedEvent := &bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_ChildInvocationCompleted{ChildInvocationCompleted: &bespb.BuildEventId_ChildInvocationCompletedId{
				InvocationId: iid,
			}}},
			Payload: &bespb.BuildEvent_ChildInvocationCompleted{ChildInvocationCompleted: &bespb.ChildInvocationCompleted{
				ExitCode:  int32(exitCode),
				StartTime: timestamppb.New(cmdStartTime),
				Duration:  durationpb.New(duration),
			}},
		}
		if err := ar.reporter.Publish(childCompletedEvent); err != nil {
			break
		}

		if runErr != nil {
			// Return early if the command failed.
			// Note, even though we don't hit the `FlushProgress` call below in this case,
			// we'll still flush progress before closing the BEP stream.
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
		rn, err := cachetools.ComputeFileDigest(path, *remoteInstanceName)
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
	conn, err := grpc_client.DialTarget(*cacheBackend)
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
		runfiles = append(runfiles, &bespb.File{
			Name: relPath,
			File: &bespb.File_Uri{
				Uri: fmt.Sprintf("%s%s", bytestreamURIPrefix, digest.NewResourceName(d.ToDigest(), *remoteInstanceName).DownloadString()),
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
	u := cachetools.NewBatchCASUploader(ctx, env, *remoteInstanceName)

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
			_, td, err := cachetools.UploadDirectoryToCAS(ctx, env, *remoteInstanceName, realPath)
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(workspaceRoot, placePath)
			if err != nil {
				return err
			}
			mu.Lock()
			runfileDirs = append(runfileDirs, &bespb.Tree{
				Name: relPath,
				Uri:  fmt.Sprintf("%s%s", bytestreamURIPrefix, digest.NewResourceName(td, *remoteInstanceName).DownloadString()),
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

func printCommandLine(out io.Writer, command string, args ...string) {
	cmdLine := command
	for _, arg := range args {
		cmdLine += " " + toShellToken(arg)
	}
	out.Write([]byte(aurora.Sprintf("%s %s\n", aurora.Green("$"), cmdLine)))
}

// TODO: Handle shell variable expansion. Probably want to run this with sh -c
func bazelArgs(rootAbsPath, bazelWorkspaceRelPath, cmd string) ([]string, error) {
	tokens, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}
	if tokens[0] == bazelBinaryName || tokens[0] == bazeliskBinaryName {
		tokens = tokens[1:]
	}
	startupFlags, err := shlex.Split(*bazelStartupFlags)
	if err != nil {
		return nil, err
	}
	startupFlags = append(startupFlags, "--output_base="+filepath.Join(rootAbsPath, outputBaseDirName))
	startupFlags = append(startupFlags, "--bazelrc="+filepath.Join(rootAbsPath, buildbuddyBazelrcPath))
	// Bazel will treat the user's workspace .bazelrc file with lower precedence
	// than our --bazelrc, which is undesired. So instead, explicitly add the
	// workspace rc as a --bazelrc flag after ours, and also set --noworkspace_rc
	// to prevent the workspace rc from getting loaded twice.
	workspacercPath := ".bazelrc"
	if bazelWorkspaceRelPath != "" {
		workspacercPath = filepath.Join(bazelWorkspaceRelPath, ".bazelrc")
	}
	_, err = os.Stat(workspacercPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if exists := (err == nil); exists {
		startupFlags = append(startupFlags, "--noworkspace_rc", "--bazelrc=.bazelrc")
	}
	if *extraBazelArgs != "" {
		extras, err := shlex.Split(*extraBazelArgs)
		if err != nil {
			return nil, err
		}
		tokens = appendBazelSubcommandArgs(tokens, extras...)
	}
	return append(startupFlags, tokens...), nil
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

func getHostAndUserName() (string, string) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Errorf("failed to get hostname: %s", err)
		hostname = ""
	}
	user, err := user.Current()
	username := ""
	if err != nil {
		log.Errorf("failed to get user: %s", err)
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
		log.Warningf(
			"Failed to sync existing repo (maybe due to destructive '.git' dir edit or incompatible remote update). "+
				"Deleting and initializing from scratch. Error: %s",
			err,
		)
		writeCommandSummary(ws.log, "Failed to sync existing git repo. Deleting repo and trying again.")
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
	if err := ws.clone(ctx, *targetRepoURL); err != nil {
		return err
	}
	if err := ws.config(ctx); err != nil {
		return err
	}
	if err := ws.sync(ctx); err != nil {
		return err
	}
	ws.log.Write([]byte(fmt.Sprintf("Setup completed in %s\n", time.Now().Sub(ws.startTime))))
	return nil
}

func (ws *workspace) applyPatch(ctx context.Context, bsClient bspb.ByteStreamClient, digestString string) error {
	d, err := digest.Parse(digestString)
	if err != nil {
		return err
	}
	patchFile := d.GetHash()
	f, err := os.OpenFile(patchFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err := cachetools.GetBlob(ctx, bsClient, digest.NewResourceName(d, *remoteInstanceName), f); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if err := git(ctx, ws.log, "apply", "--verbose", patchFile); err != nil {
		return err
	}
	return nil
}

func (ws *workspace) sync(ctx context.Context) error {
	// Fetch the pushed and target branches from their respective remotes.
	// "base" here is referring to the repo on which the workflow is configured.
	// "fork" is referring to the forked repo, if the runner was triggered by a
	// PR from a fork (forkBranches will be empty otherwise).
	baseRefs := []string{}
	if *targetBranch != "" {
		baseRefs = append(baseRefs, *targetBranch)
	}
	forkBranches := []string{}
	// Add the pushed branch to the appropriate list corresponding to the remote
	// to be fetched (base or fork).
	if *pushedRepoURL != "" {
		if isPushedBranchInFork := *pushedRepoURL != *targetRepoURL; isPushedBranchInFork {
			forkBranches = append(forkBranches, *pushedBranch)
		} else if *pushedBranch != *targetBranch {
			baseRefs = append(baseRefs, *pushedBranch)
		}
	}
	// TODO: Fetch from remotes in parallel
	if err := ws.fetch(ctx, *targetRepoURL, baseRefs); err != nil {
		return err
	}
	if err := ws.fetch(ctx, *pushedRepoURL, forkBranches); err != nil {
		return err
	}

	checkoutRef := ""
	checkoutLocalBranchName := ""
	if *pushedRepoURL != "" {
		checkoutRef = fmt.Sprintf("%s/%s", gitRemoteName(*pushedRepoURL), *pushedBranch)
		checkoutLocalBranchName = *pushedBranch
	} else {
		if *targetBranch != "" {
			checkoutRef = fmt.Sprintf("%s/%s", gitRemoteName(*targetRepoURL), *targetBranch)
			checkoutLocalBranchName = *targetBranch
		} else {
			checkoutRef = *targetCommitSHA
			checkoutLocalBranchName = "local"
		}
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
	if err := git(ctx, ws.log, cleanArgs...); err != nil {
		return err
	}
	// Create the branch if it doesn't already exist, then update it to point to
	// the pushed branch tip.
	if err := git(ctx, ws.log, "checkout", "--force", "-B", checkoutLocalBranchName, checkoutRef); err != nil {
		return err
	}
	// Merge the target branch (if different from the pushed branch) so that the
	// workflow can pick up any changes not yet incorporated into the pushed branch.
	if *pushedRepoURL != "" && (*pushedRepoURL != *targetRepoURL || *pushedBranch != *targetBranch) {
		targetRef := fmt.Sprintf("%s/%s", gitRemoteName(*targetRepoURL), *targetBranch)
		if err := git(ctx, ws.log, "merge", "--no-edit", targetRef); err != nil && !isAlreadyUpToDate(err) {
			errMsg := err.Output
			if err := git(ctx, ws.log, "merge", "--abort"); err != nil {
				errMsg += "\n" + err.Output
			}
			// Make note of the merge conflict and abort. We'll run all actions and each
			// one will just fail with the merge conflict error.
			ws.setupError = status.FailedPreconditionErrorf(
				"Merge conflict between branches %q and %q.\n\n%s",
				*pushedBranch, *targetBranch, errMsg,
			)
		}
	}

	if len(*patchDigests) > 0 {
		conn, err := grpc_client.DialTarget(*cacheBackend)
		if err != nil {
			return err
		}
		bsClient := bspb.NewByteStreamClient(conn)
		for _, digestString := range *patchDigests {
			if err := ws.applyPatch(ctx, bsClient, digestString); err != nil {
				return err
			}
		}
	}

	return nil
}

func (ws *workspace) config(ctx context.Context) error {
	cfg := [][]string{
		{"user.email", "ci-runner@buildbuddy.io"},
		{"user.name", "BuildBuddy"},
		{"advice.detachedHead", "false"},
	}
	writeCommandSummary(ws.log, "Configuring repository...")
	for _, kv := range cfg {
		// Don't show the config output.
		if err := git(ctx, io.Discard, "config", kv[0], kv[1]); err != nil {
			return err
		}
	}
	return nil
}

func (ws *workspace) clone(ctx context.Context, remoteURL string) error {
	authURL, err := gitutil.AuthRepoURL(remoteURL, os.Getenv(repoUserEnvVarName), os.Getenv(repoTokenEnvVarName))
	if err != nil {
		return err
	}
	writeCommandSummary(ws.log, "Cloning target repo...")
	// Don't show command since the URL may contain the repo access token.
	args := []string{"clone", "--config=credential.helper=", "--filter=blob:none", "--no-checkout", authURL, "."}
	if err := runCommand(ctx, "git", args, map[string]string{}, "" /*=dir*/, ws.log); err != nil {
		return status.UnknownError("Command `git clone --filter=blob:none --no-checkout <url>` failed.")
	}
	return nil
}

func (ws *workspace) fetch(ctx context.Context, remoteURL string, branches []string) error {
	if len(branches) == 0 {
		return nil
	}
	authURL, err := gitutil.AuthRepoURL(remoteURL, os.Getenv(repoUserEnvVarName), os.Getenv(repoTokenEnvVarName))
	if err != nil {
		return err
	}
	remoteName := gitRemoteName(remoteURL)
	writeCommandSummary(ws.log, "Configuring remote %q...", remoteName)
	// Don't show `git remote add` command or the error message since the URL may
	// contain the repo access token.
	if err := git(ctx, io.Discard, "remote", "add", remoteName, authURL); err != nil && !isRemoteAlreadyExists(err) {
		return status.UnknownErrorf("Command `git remote add %q <url>` failed.", remoteName)
	}
	fetchArgs := append([]string{"fetch", "--filter=blob:none", "--force", remoteName}, branches...)
	if err := git(ctx, ws.log, fetchArgs...); err != nil {
		return err
	}
	return nil
}

type gitError struct {
	error
	Output string
}

func isRemoteAlreadyExists(err error) bool {
	gitErr, ok := err.(*gitError)
	return ok && strings.Contains(gitErr.Output, "already exists")
}
func isBranchNotFound(err error) bool {
	gitErr, ok := err.(*gitError)
	return ok && strings.Contains(gitErr.Output, "not found")
}
func isAlreadyUpToDate(err error) bool {
	gitErr, ok := err.(*gitError)
	return ok && strings.Contains(gitErr.Output, "up to date")
}

func git(ctx context.Context, out io.Writer, args ...string) *gitError {
	var buf bytes.Buffer
	w := io.MultiWriter(out, &buf)
	printCommandLine(out, "git", args...)
	if err := runCommand(ctx, "git", args, map[string]string{} /*=env*/, "" /*=dir*/, w); err != nil {
		return &gitError{err, string(buf.Bytes())}
	}
	return nil
}

func writeCommandSummary(out io.Writer, format string, args ...interface{}) {
	io.WriteString(out, ansiGray)
	io.WriteString(out, fmt.Sprintf(format, args...))
	io.WriteString(out, ansiReset)
	io.WriteString(out, "\n")
}

func invocationURL(invocationID string) string {
	urlPrefix := *besResultsURL
	if !strings.HasSuffix(urlPrefix, "/") {
		urlPrefix = urlPrefix + "/"
	}
	return urlPrefix + invocationID
}

func writeBazelrc(path, invocationID string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0664)
	if err != nil {
		return status.InternalErrorf("Failed to open %s for writing: %s", path, err)
	}
	defer f.Close()

	lines := []string{
		"build --build_metadata=ROLE=CI",
		"build --build_metadata=PARENT_INVOCATION_ID=" + invocationID,
		// Note: these pieces of metadata are set to match the WorkspaceStatus event
		// for the outer (workflow) invocation.
		"build --build_metadata=COMMIT_SHA=" + *commitSHA,
		"build --build_metadata=REPO_URL=" + *targetRepoURL,
		"build --build_metadata=BRANCH_NAME=" + *pushedBranch, // corresponds to GIT_BRANCH status key
		// Don't report commit statuses for individual bazel commands, since the
		// overall status of all bazel commands is reflected in the status reported
		// for the workflow invocation. In addition, for PRs, we first merge with
		// the target branch which causes the HEAD commit SHA to change, and this
		// SHA won't actually exist on GitHub.
		"build --build_metadata=DISABLE_COMMIT_STATUS_REPORTING=true",
		"build --bes_backend=" + *besBackend,
		"build --bes_results_url=" + *besResultsURL,
	}
	if *workflowID != "" {
		lines = append(lines, "build --build_metadata=WORKFLOW_ID="+*workflowID)
	}
	if apiKey := os.Getenv(buildbuddyAPIKeyEnvVarName); apiKey != "" {
		lines = append(lines, "build --remote_header=x-buildbuddy-api-key="+apiKey)
	}

	// Primitive configs pointing to BB endpoints. These are purposely very
	// fine-grained and do not include any options other than the backend
	// URLs for now. They are all prefixed with "buildbuddy_" to avoid conflicting
	// with existing .bazelrc configs in the wild.
	lines = append(lines, []string{
		"build:buildbuddy_bes_backend --bes_backend=" + *besBackend,
		"build:buildbuddy_bes_results_url --bes_results_url=" + *besResultsURL,
	}...)
	if *cacheBackend != "" {
		lines = append(lines, "build:buildbuddy_remote_cache --remote_cache="+*cacheBackend)
	}
	if *rbeBackend != "" {
		lines = append(lines, "build:buildbuddy_remote_executor --remote_executor="+*rbeBackend)
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
			return config.GetDefault(), nil
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
	if repoURL == *targetRepoURL {
		return defaultGitRemoteName
	}
	return forkGitRemoteName
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
	f, err := pty.Start(cmd)
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
	return err
}

func expandEnv(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		out = append(out, os.ExpandEnv(arg))
	}
	return out
}

func getExitCode(err error) int {
	if err == nil {
		return 0
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
