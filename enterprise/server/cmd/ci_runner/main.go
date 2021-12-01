package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/logrusorgru/aurora"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v2"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

const (
	// Name of the dir into which the repo is cloned.
	repoDirName = "repo-root"

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
	besBackend                  = flag.String("bes_backend", "", "gRPC endpoint for BuildBuddy's BES backend.")
	cacheBackend                = flag.String("cache_backend", "", "gRPC endpoint for BuildBuddy Cache.")
	besResultsURL               = flag.String("bes_results_url", "", "URL prefix for BuildBuddy invocation URLs.")
	remoteInstanceName          = flag.String("remote_instance_name", "", "Remote instance name used to retrieve patches.")
	triggerEvent                = flag.String("trigger_event", "", "Event type that triggered the action runner.")
	pushedRepoURL               = flag.String("pushed_repo_url", "", "URL of the pushed repo.")
	pushedBranch                = flag.String("pushed_branch", "", "Branch name of the commit to be checked out.")
	commitSHA                   = flag.String("commit_sha", "", "Commit SHA to report statuses for.")
	targetRepoURL               = flag.String("target_repo_url", "", "URL of the target repo.")
	targetBranch                = flag.String("target_branch", "", "Branch to check action triggers against.")
	workflowID                  = flag.String("workflow_id", "", "ID of the workflow associated with this CI run.")
	actionName                  = flag.String("action_name", "", "If set, run the specified action and *only* that action, ignoring trigger conditions.")
	invocationID                = flag.String("invocation_id", "", "If set, use the specified invocation ID for the workflow action. Ignored if action_name is not set.")
	bazelSubCommand             = flag.String("bazel_sub_command", "", "If set, run the bazel command specified by these args and ignore all triggering and configured actions.")
	patchDigests                flagutil.StringSliceFlag
	reportLiveRepoSetupProgress = flag.Bool("report_live_repo_setup_progress", false, "If set, repo setup output will be streamed live to the invocation instead of being postponed until the action is run.")

	shutdownAndExit = flag.Bool("shutdown_and_exit", false, "If set, runs bazel shutdown with the configured bazel_command, and exits. No other commands are run.")

	bazelCommand      = flag.String("bazel_command", "", "Bazel command to use.")
	bazelStartupFlags = flag.String("bazel_startup_flags", "", "Startup flags to pass to bazel. The value can include spaces and will be properly tokenized.")
	debug             = flag.Bool("debug", false, "Print additional debug information in the action logs.")

	// Test-only flags
	fallbackToCleanCheckout = flag.Bool("fallback_to_clean_checkout", true, "Fallback to cloning the repo from scratch if sync fails (for testing purposes only).")

	shellCharsRequiringQuote = regexp.MustCompile(`[^\w@%+=:,./-]`)
)

func init() {
	flag.Var(&patchDigests, "patch_digest", "Digests of patches to apply to the repo after checkout. Can be specified multiple times to apply multiple patches.")
}

type workspace struct {
	// Whether the workspace setup phase duration and logs were reported as part
	// of any action's logs yet.
	reportedInitMetrics bool

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
	log    io.Writer
	logBuf bytes.Buffer
}

type buildEventReporter struct {
	apiKey string
	bep    *build_event_publisher.Publisher
	log    *invocationLog

	invocationID          string
	startTime             time.Time
	cancelBackgroundFlush func()

	mu            sync.Mutex // protects(progressCount)
	progressCount int32
}

func newBuildEventReporter(ctx context.Context, besBackend string, apiKey string, forcedInvocationID string) (*buildEventReporter, error) {
	iid := forcedInvocationID
	if iid == "" {
		iid = newUUID()
	}

	bep, err := build_event_publisher.New(besBackend, apiKey, iid)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to initialize build event publisher: %s", err)
	}
	bep.Start(ctx)
	return &buildEventReporter{apiKey: apiKey, bep: bep, log: newInvocationLog(), invocationID: iid}, nil
}

func (r *buildEventReporter) InvocationID() string {
	return r.invocationID
}

func (r *buildEventReporter) Publish(event *bespb.BuildEvent) error {
	return r.bep.Publish(event)
}

func (r *buildEventReporter) Start(startTime time.Time) error {
	if !r.startTime.IsZero() {
		// Already started.
		return nil
	}
	r.startTime = startTime

	optionsDescription := ""
	if r.apiKey != "" {
		optionsDescription = fmt.Sprintf("--remote_header='x-buildbuddy-api-key=%s'", r.apiKey)
	}

	startedEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
			{Id: &bespb.BuildEventId_WorkflowConfigured{WorkflowConfigured: &bespb.BuildEventId_WorkflowConfiguredId{}}},
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 0}}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
			Uuid:               r.invocationID,
			StartTimeMillis:    startTime.UnixMilli(),
			OptionsDescription: optionsDescription,
		}},
	}
	if err := r.bep.Publish(startedEvent); err != nil {
		return err
	}

	// Flush whenever the log buffer fills past a certain threshold.
	r.log.writeListener = func() {
		if size := r.log.Len(); size >= progressFlushThresholdBytes {
			r.FlushProgress() // ignore error; it will surface in `bep.Wait()`
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
	r.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		},
		Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
			OverallSuccess: exitCode == 0,
			ExitCode: &bespb.BuildFinished_ExitCode{
				Name: exitCodeName,
				Code: int32(exitCode),
			},
			FinishTimeMillis: time.Now().UnixMilli(),
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

	if err := r.bep.Wait(); err != nil {
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
	flag.Parse()

	ws := &workspace{
		startTime:          time.Now(),
		buildbuddyAPIKey:   os.Getenv(buildbuddyAPIKeyEnvVarName),
		forcedInvocationID: *invocationID,
	}

	// Write setup logs to the current task's stderr (to make debugging easier),
	// and also to a buffer to be flushed to the first workflow action's logs.
	ws.log = io.MultiWriter(os.Stderr, &ws.logBuf)
	ws.hostname, ws.username = getHostAndUserName()

	ctx := context.Background()
	if ws.buildbuddyAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, ws.buildbuddyAPIKey)
	}

	// Bazel needs a HOME dir; ensure that one is set.
	if err := ensureHomeDir(); err != nil {
		fatal(status.WrapError(err, "ensure HOME"))
	}
	// Make sure PATH is set.
	if err := ensurePath(); err != nil {
		fatal(status.WrapError(err, "ensure PATH"))
	}
	// Make sure we have a bazel / bazelisk binary available.
	if *bazelCommand == "" {
		wd, err := os.Getwd()
		if err != nil {
			fatal(err)
		}
		bazeliskPath := filepath.Join(wd, bazeliskBinaryName)
		if err := extractBazelisk(bazeliskPath); err != nil {
			fatal(status.WrapError(err, "failed to extract bazelisk"))
		}
		*bazelCommand = bazeliskPath
	}

	if *shutdownAndExit {
		log.Info("--shutdown_and_exit requested; will run bazel shutdown then exit.")
		if _, err := os.Stat(repoDirName); err != nil {
			log.Info("Workspace does not exist; exiting.")
			return
		}
		if err := os.Chdir(repoDirName); err != nil {
			fatal(err)
		}
		printCommandLine(os.Stderr, *bazelCommand, "shutdown")
		if err := runCommand(ctx, *bazelCommand, []string{"shutdown"}, nil, os.Stderr); err != nil {
			fatal(err)
		}
		log.Info("Shutdown complete.")
		return
	}

	var buildEventReporter *buildEventReporter
	if *reportLiveRepoSetupProgress {
		ber, err := newBuildEventReporter(ctx, *besBackend, ws.buildbuddyAPIKey, *invocationID)
		if err != nil {
			fatal(err)
		}
		buildEventReporter = ber
		if err := buildEventReporter.Start(ws.startTime); err != nil {
			fatal(status.WrapError(err, "could not publish started event"))
		}
	}
	if err := ws.setup(ctx, buildEventReporter); err != nil {
		if buildEventReporter != nil {
			_ = buildEventReporter.Stop(noExitCode, failedExitCodeName)
		}
		fatal(status.WrapError(err, "failed to set up git repo"))
	}
	cfg, err := readConfig()
	if err != nil {
		if buildEventReporter != nil {
			_ = buildEventReporter.Stop(noExitCode, failedExitCodeName)
		}
		fatal(status.WrapError(err, "failed to read BuildBuddy config"))
	}

	var actions []*config.Action
	if *bazelSubCommand != "" {
		actions = []*config.Action{{
			Name: "run",
			BazelCommands: []string{
				*bazelSubCommand,
			},
		}}
	} else if *actionName != "" {
		// If a specific action was specified, filter to configured
		// actions with a matching action name.
		actions = filterActions(cfg.Actions, func(action *config.Action) bool {
			return action.Name == *actionName
		})
	} else {
		// If no action was specified; filter to configured actions that
		// match any trigger.
		actions = filterActions(cfg.Actions, func(action *config.Action) bool {
			match := config.MatchesAnyTrigger(action, *triggerEvent, *targetBranch)
			if !match {
				log.Debugf("No triggers matched for %q event with target branch %q. Action config:\n===\n%s===", *triggerEvent, *targetBranch, actionDebugString(action))
			}
			return match
		})
	}
	if *invocationID != "" && len(actions) > 1 {
		fatal(status.InvalidArgumentError("Cannot specify --invocationID when running multiple actions"))
	}

	ws.RunAllActions(ctx, actions, buildEventReporter)
}

// RunAllActions runs the specified actions in serial, creating a synthetic
// invocation for each one.
func (ws *workspace) RunAllActions(ctx context.Context, actions []*config.Action, buildEventReporter *buildEventReporter) {
	for _, action := range actions {
		startTime := time.Now()

		if buildEventReporter == nil {
			ber, err := newBuildEventReporter(ctx, *besBackend, ws.buildbuddyAPIKey, *invocationID)
			if err != nil {
				fatal(err)
			}
			buildEventReporter = ber
		}

		// NB: Anything logged to `ar.log` gets output to both the stdout of this binary
		// and the logs uploaded to BuildBuddy for this action. Anything that we want
		// the user to see in the invocation UI needs to go in that log, instead of
		// the global `log.Print`.
		ar := &actionRunner{
			action:   action,
			reporter: buildEventReporter,
			hostname: ws.hostname,
			username: ws.username,
		}
		exitCode := 0
		exitCodeName := "OK"

		// Include the repo's download time as part of the first invocation.
		if !ws.reportedInitMetrics {
			ws.reportedInitMetrics = true
			// Print the setup logs to the first action's logs.
			ar.reporter.Printf("Setup completed in %s", startTime.Sub(ws.startTime))
			startTime = ws.startTime
			b, _ := io.ReadAll(&ws.logBuf)
			ws.logBuf.Reset()
			if len(b) > 0 {
				ar.reporter.Printf("Setup logs:")
				ar.reporter.Printf(string(b))
			}
		}

		if err := ar.reporter.Start(startTime); err != nil {
			if err := ar.reporter.Stop(noExitCode, failedExitCodeName); err != nil {
				fatal(err)
			}
			return
		}

		if err := ar.Run(ctx, ws, startTime); err != nil {
			ar.reporter.Printf(aurora.Sprintf(aurora.Red("\nAction failed: %s"), status.Message(err)))
			exitCode = getExitCode(err)
			// TODO: More descriptive exit code names, so people have a better
			// sense of what happened without even needing to open the invocation.
			exitCodeName = failedExitCodeName
		}

		if err := ar.reporter.Stop(exitCode, exitCodeName); err != nil {
			fatal(err)
		}
	}
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
	action   *config.Action
	reporter *buildEventReporter
	username string
	hostname string
}

func (ar *actionRunner) Run(ctx context.Context, ws *workspace, startTime time.Time) error {
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
	}
	configuredEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowConfigured{WorkflowConfigured: &bespb.BuildEventId_WorkflowConfiguredId{}}},
		Payload: &bespb.BuildEvent_WorkflowConfigured{WorkflowConfigured: wfc},
	}
	// If the triggering commit merges cleanly with the target branch, the runner
	// will execute the configured bazel commands. Otherwise, the runner will
	// exit early without running those commands and does not need to create
	// invocation streams for them.
	if ws.setupError == nil {
		for _, bazelCmd := range ar.action.BazelCommands {
			iid := newUUID()
			wfc.Invocation = append(wfc.Invocation, &bespb.WorkflowConfigured_InvocationMetadata{
				InvocationId: iid,
				BazelCommand: bazelCmd,
			})
			eventID := &bespb.BuildEventId{
				Id: &bespb.BuildEventId_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.BuildEventId_WorkflowCommandCompletedId{
					InvocationId: iid,
				}},
			}
			configuredEvent.Children = append(configuredEvent.Children, eventID)
		}
	}
	if err := ar.reporter.Publish(configuredEvent); err != nil {
		return nil
	}

	buildMetadataEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: &bespb.BuildMetadata{
			Metadata: map[string]string{
				"ROLE": "CI_RUNNER",
			},
		}},
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
		args, err := bazelArgs(bazelCmd)
		if err != nil {
			return status.InvalidArgumentErrorf("failed to parse bazel command: %s", err)
		}
		printCommandLine(ar.reporter, *bazelCommand, args...)
		// Transparently set the invocation ID from the one we computed ahead of
		// time. The UI is expecting this invocation ID so that it can render a
		// BuildBuddy invocation URL for each bazel_command that is executed.
		args = append(args, fmt.Sprintf("--invocation_id=%s", iid))

		runErr := runCommand(ctx, *bazelCommand, args, nil /*=env*/, ar.reporter)
		exitCode := getExitCode(runErr)
		if exitCode != noExitCode {
			ar.reporter.Printf("%s(command exited with code %d)%s\n", ansiGray, exitCode, ansiReset)
		}

		// Publish the status of each command as well as the finish time.
		// Stop execution early on BEP failure, but ignore error -- it will surface in `bep.Wait()`.
		completedEvent := &bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.BuildEventId_WorkflowCommandCompletedId{
				InvocationId: iid,
			}}},
			Payload: &bespb.BuildEvent_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.WorkflowCommandCompleted{
				ExitCode:        int32(exitCode),
				StartTimeMillis: cmdStartTime.UnixMilli(),
				DurationMillis:  int64(float64(time.Since(cmdStartTime)) / float64(time.Millisecond)),
			}},
		}
		if err := ar.reporter.Publish(completedEvent); err != nil {
			break
		}

		if runErr != nil {
			// Return early if the command failed.
			// Note, even though we don't hit the `FlushProgress` call below in this case,
			// we'll still flush progress before closing the BEP stream.
			return runErr
		}

		// Flush progress after every command.
		// Stop execution early on BEP failure, but ignore error -- it will surface in `bep.Wait()`.
		if err := ar.reporter.FlushProgress(); err != nil {
			break
		}
	}
	return nil
}

func printCommandLine(out io.Writer, command string, args ...string) {
	cmdLine := command
	for _, arg := range args {
		cmdLine += " " + toShellToken(arg)
	}
	out.Write([]byte(aurora.Sprintf("%s %s\n", aurora.Green("$"), cmdLine)))
}

// TODO: Handle shell variable expansion. Probably want to run this with sh -c
func bazelArgs(cmd string) ([]string, error) {
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
	return append(startupFlags, tokens...), nil
}

func ensureHomeDir() error {
	if os.Getenv("HOME") != "" {
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

func filterActions(actions []*config.Action, fn func(action *config.Action) bool) []*config.Action {
	matched := make([]*config.Action, 0)
	for _, action := range actions {
		if fn(action) {
			matched = append(matched, action)
		}
	}
	return matched
}

func (ws *workspace) setup(ctx context.Context, reporter *buildEventReporter) error {
	if reporter != nil {
		// If we have a reporter, stream the logs through it instead of buffering them until later.
		ws.log = reporter
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
		log.Warningf(
			"Failed to sync existing repo (maybe due to destructive '.git' dir edit or incompatible remote update). "+
				"Deleting and initializing from scratch. Error: %s",
			err,
		)
		writeCommandSummary(ws.log, "Failed to sync existing git repo. Deleting repo and trying again.")
		if err := os.Chdir(".."); err != nil {
			return status.WrapError(err, "cd")
		}
		if err := os.RemoveAll(repoDirName); err != nil {
			return status.WrapErrorf(err, "rm -r %q", repoDirName)
		}
	}

	if err := os.Mkdir(repoDirName, 0777); err != nil {
		return status.WrapErrorf(err, "mkdir %q", repoDirName)
	}
	if err := os.Chdir(repoDirName); err != nil {
		return status.WrapErrorf(err, "cd %q", repoDirName)
	}
	if err := git(ctx, ws.log, "init"); err != nil {
		return err
	}
	// Don't use a credential helper since we always use explicit credentials.
	if err := git(ctx, ws.log, "config", "--local", "credential.helper", ""); err != nil {
		return err
	}
	return ws.sync(ctx)
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
	if err := cachetools.GetBlob(ctx, bsClient, digest.NewInstanceNameDigest(d, *remoteInstanceName), f); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if err := git(ctx, ws.log, "apply", patchFile); err != nil {
		return err
	}
	return nil
}

func (ws *workspace) sync(ctx context.Context) error {
	// Setup config before we do anything.
	if err := ws.config(ctx); err != nil {
		return err
	}

	// Fetch the pushed and target branches from their respective remotes.
	// "base" here is referring to the repo on which the workflow is configured.
	// "fork" is referring to the forked repo, if the runner was triggered by a
	// PR from a fork (forkBranches will be empty otherwise).
	baseBranches := []string{*targetBranch}
	forkBranches := []string{}
	// Add the pushed branch to the appropriate list corresponding to the remote
	// to be fetched (base or fork).
	if isPushedBranchInFork := *pushedRepoURL != *targetRepoURL; isPushedBranchInFork {
		forkBranches = append(forkBranches, *pushedBranch)
	} else if *pushedBranch != *targetBranch {
		baseBranches = append(baseBranches, *pushedBranch)
	}
	// TODO: Fetch from remotes in parallel
	if err := ws.fetch(ctx, *targetRepoURL, baseBranches); err != nil {
		return err
	}
	if err := ws.fetch(ctx, *pushedRepoURL, forkBranches); err != nil {
		return err
	}
	// Clean up in case a previous workflow made a mess.
	if err := git(ctx, ws.log, "clean", "-d" /*directories*/, "--force"); err != nil {
		return err
	}
	if err := git(ctx, ws.log, "clean", "-X" /*ignored files*/, "--force"); err != nil {
		return err
	}
	// Create the branch if it doesn't already exist, then update it to point to
	// the pushed branch tip.
	if err := git(ctx, ws.log, "checkout", "-B", *pushedBranch); err != nil {
		return err
	}
	remotePushedBranchRef := fmt.Sprintf("%s/%s", gitRemoteName(*pushedRepoURL), *pushedBranch)
	if err := git(ctx, ws.log, "reset", "--hard", remotePushedBranchRef); err != nil {
		return err
	}
	// Merge the target branch (if different from the pushed branch) so that the
	// workflow can pick up any changes not yet incorporated into the pushed branch.
	if *pushedRepoURL != *targetRepoURL || *pushedBranch != *targetBranch {
		targetRef := fmt.Sprintf("%s/%s", gitRemoteName(*targetRepoURL), *targetBranch)
		if err := git(ctx, ws.log, "merge", targetRef); err != nil && !isAlreadyUpToDate(err) {
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

	if len(patchDigests) > 0 {
		conn, err := grpc_client.DialTarget(*cacheBackend)
		if err != nil {
			return err
		}
		bsClient := bspb.NewByteStreamClient(conn)
		for _, digestString := range patchDigests {
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
	fetchArgs := append([]string{"fetch", "--force", remoteName}, branches...)
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
	if err := runCommand(ctx, "git", args, map[string]string{} /*=env*/, w); err != nil {
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

func readConfig() (*config.BuildBuddyConfig, error) {
	f, err := os.Open(config.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return config.GetDefault(
				*targetBranch, *besBackend, *besResultsURL,
				os.Getenv(buildbuddyAPIKeyEnvVarName),
			), nil
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

func runCommand(ctx context.Context, executable string, args []string, env map[string]string, outputSink io.Writer) error {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Stdout = outputSink
	cmd.Stderr = outputSink
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	return cmd.Run()
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

func newUUID() string {
	id, err := uuid.NewRandom()
	if err != nil {
		fatal(status.UnavailableError("failed to generate UUID"))
	}
	return id.String()
}

func toShellToken(s string) string {
	if shellCharsRequiringQuote.MatchString(s) {
		s = "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
	}
	return s
}

func fatal(err error) {
	log.Errorf("%s", err)
	os.Exit(int(gstatus.Code(err)))
}
