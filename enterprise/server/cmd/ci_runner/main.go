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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/logrusorgru/aurora"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v2"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	git "github.com/go-git/go-git/v5"
	gitcfg "github.com/go-git/go-git/v5/config"
	gitplumbing "github.com/go-git/go-git/v5/plumbing"
	gstatus "google.golang.org/grpc/status"
)

const (
	// Name of the dir into which the repo is cloned.
	repoDirName = "repo-root"
	// Path where we expect to find actions config, relative to the repo root.
	actionsConfigPath = "buildbuddy.yaml"

	// Env vars set by workflow runner
	// NOTE: These env vars are not populated for non-private repos.

	buildbuddyAPIKeyEnvVarName = "BUILDBUDDY_API_KEY"
	repoUserEnvVarName         = "REPO_USER"
	repoTokenEnvVarName        = "REPO_TOKEN"

	// Exit code placeholder used when a command doesn't return an exit code on its own.
	noExitCode = -1

	// progressFlushInterval specifies how often we should flush
	// each Bazel command's output while it is running.
	progressFlushInterval = 1 * time.Second
	// progressFlushThresholdBytes specifies how full the log buffer
	// should be before we force a flush, regardless of the flush interval.
	progressFlushThresholdBytes = 1_000

	// Webhook event names

	pushEventName        = "push"
	pullRequestEventName = "pull_request"

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
	besBackend    = flag.String("bes_backend", "", "gRPC endpoint for BuildBuddy's BES backend.")
	besResultsURL = flag.String("bes_results_url", "", "URL prefix for BuildBuddy invocation URLs.")
	repoURL       = flag.String("repo_url", "", "URL of the Git repo to check out.")
	branch        = flag.String("branch", "", "Branch name of the commit to be checked out.")
	commitSHA     = flag.String("commit_sha", "", "SHA of the commit to be checked out.")
	triggerEvent  = flag.String("trigger_event", "", "Event type that triggered the action runner.")
	triggerBranch = flag.String("trigger_branch", "", "Branch to check action triggers against.")
	workflowID    = flag.String("workflow_id", "", "ID of the workflow associated with this CI run.")
	actionName    = flag.String("action_name", "", "If set, run the specified action and *only* that action, ignoring trigger conditions.")
	invocationID  = flag.String("invocation_id", "", "If set, use the specified invocation ID for the workflow action. Ignored if action_name is not set.")

	bazelCommand = flag.String("bazel_command", bazeliskBinaryName, "Bazel command to use.")
	debug        = flag.Bool("debug", false, "Print additional debug information in the action logs.")

	// Test-only flags
	fallbackToCleanCheckout = flag.Bool("fallback_to_clean_checkout", true, "Fallback to cloning the repo from scratch if sync fails (for testing purposes only).")

	shellCharsRequiringQuote = regexp.MustCompile(`[^\w@%+=:,./-]`)

	// initLogs contain informational logs from the setup phase (cloning the
	// git repo and deciding which actions to run) which are reported as part of
	// the first action's logs.
	initLog bytes.Buffer
)

func main() {
	im := &initMetrics{start: time.Now()}

	flag.Parse()

	if *bazelCommand == "" {
		*bazelCommand = bazeliskBinaryName
	}
	if (*actionName == "") != (*invocationID == "") {
		log.Fatalf("--action_name and --invocation_id must either be both present or both missing.")
	}

	ctx := context.Background()

	// Bazel needs a HOME dir; ensure that one is set.
	if err := ensureHomeDir(); err != nil {
		fatal(status.WrapError(err, "ensure HOME"))
	}
	// Make sure PATH is set.
	if err := ensurePath(); err != nil {
		fatal(status.WrapError(err, "ensure PATH"))
	}
	if err := setupGitRepo(ctx); err != nil {
		fatal(status.WrapError(err, "failed to set up git repo"))
	}
	if err := os.Chdir(repoDirName); err != nil {
		fatal(status.WrapErrorf(err, "cd %q", repoDirName))
	}
	runDebugCommand(&initLog, `pwd`)
	runDebugCommand(&initLog, `ls -la`)
	cfg, err := readConfig()
	if err != nil {
		fatal(status.WrapError(err, "failed to read BuildBuddy config"))
	}

	RunAllActions(ctx, cfg, im)
}

// initMetrics record the time spent between the start of main() and the
// instant just before running the first action.
type initMetrics struct {
	start time.Time
	// Whether these metrics were already reported and accounted for in an invocation.
	reported bool
}

// RunAllActions runs all triggered actions in the BuildBuddy config in serial, creating
// a synthetic invocation for each one.
func RunAllActions(ctx context.Context, cfg *config.BuildBuddyConfig, im *initMetrics) {
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

	for _, action := range cfg.Actions {
		startTime := time.Now()

		if *actionName != "" {
			if action.Name != *actionName {
				continue
			}
		} else if !matchesAnyTrigger(action, *triggerEvent, *triggerBranch) {
			log.Debugf("No triggers matched for %q event with target branch %q. Action config:\n===\n%s===", *triggerEvent, *triggerBranch, actionDebugString(action))
			continue
		}

		iid := ""
		// Respect the invocation ID flag only when running a single action
		// (via ExecuteWorkflow).
		if *actionName != "" {
			iid = *invocationID
		} else {
			iid = newUUID()
		}
		bep := newBuildEventPublisher(&bepb.StreamId{
			InvocationId: iid,
			BuildId:      newUUID(),
		})
		bep.Start(ctx)

		// NB: Anything logged to `ar.log` gets output to both the stdout of this binary
		// and the logs uploaded to BuildBuddy for this action. Anything that we want
		// the user to see in the invocation UI needs to go in that log, instead of
		// the global `log.Print`.
		ar := &actionRunner{
			action:   action,
			log:      newInvocationLog(),
			bep:      bep,
			hostname: hostname,
			username: username,
		}
		exitCode := 0
		exitCodeName := "OK"

		// Include the repo's download time as part of the first invocation.
		if !im.reported {
			im.reported = true
			ar.log.Printf("Fetched Git repository in %s\n", startTime.Sub(im.start))
			startTime = im.start
		}

		if err := ar.Run(ctx, startTime); err != nil {
			ar.log.Printf(aurora.Sprintf(aurora.Red("\nAction failed: %s"), err))
			exitCode = getExitCode(err)
			// TODO: More descriptive exit code names, so people have a better
			// sense of what happened without even needing to open the invocation.
			exitCodeName = "Failed"
		}

		// Ignore errors from the events published here; they'll be surfaced in `bep.Wait()`
		ar.flushProgress()
		bep.Publish(&bespb.BuildEvent{
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
				FinishTimeMillis: time.Now().UnixNano() / int64(time.Millisecond),
			}},
		})
		elapsedTimeSeconds := float64(time.Since(startTime)) / float64(time.Second)
		// NB: This is the last message -- if more are added afterwards, be sure to
		// update the `LastMessage` flag
		bep.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
			Payload: &bespb.BuildEvent_BuildToolLogs{BuildToolLogs: &bespb.BuildToolLogs{
				Log: []*bespb.File{
					{Name: "elapsed time", File: &bespb.File_Contents{Contents: []byte(string(fmt.Sprintf("%.6f", elapsedTimeSeconds)))}},
				},
			}},
			LastMessage: true,
		})

		if err := bep.Wait(); err != nil {
			// If we don't publish a build event successfully, then the status may not be
			// reported to the Git provider successfully. Terminate with a code indicating
			// that the executor can retry the action, so that we have another chance.
			fatal(status.UnavailableErrorf("failed to publish build event for action %q: %s", action.Name, err))
		}
	}
}

type invocationLog struct {
	writer        io.Writer
	writeListener func()
	lockingbuffer.LockingBuffer
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

// buildEventPublisher publishes Bazel build events for a single build event stream.
type buildEventPublisher struct {
	err      error
	streamID *bepb.StreamId
	done     chan struct{}
	events   chan *bespb.BuildEvent
	mu       sync.Mutex
}

func newBuildEventPublisher(streamID *bepb.StreamId) *buildEventPublisher {
	return &buildEventPublisher{
		streamID: streamID,
		// We probably won't ever saturate this buffer since we only need to
		// publish a few events for the actions themselves and progress events
		// are rate-limited. Also, events are sent to the server with low
		// latency compared to the rate limiting interval.
		events: make(chan *bespb.BuildEvent, 256),
		done:   make(chan struct{}, 1),
	}
}

// Start the event publishing loop in the background. Stops handling new events
// as soon as the first call to `Wait()` occurs.
func (bep *buildEventPublisher) Start(ctx context.Context) {
	go bep.run(ctx)
}
func (bep *buildEventPublisher) run(ctx context.Context) {
	defer func() {
		bep.done <- struct{}{}
	}()

	conn, err := grpc_client.DialTarget(*besBackend)
	if err != nil {
		bep.setError(status.WrapError(err, "error dialing bes_backend"))
		return
	}
	defer conn.Close()
	besClient := pepb.NewPublishBuildEventClient(conn)
	buildbuddyAPIKey := os.Getenv(buildbuddyAPIKeyEnvVarName)
	if buildbuddyAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, buildbuddyAPIKey)
	}
	stream, err := besClient.PublishBuildToolEventStream(ctx)
	if err != nil {
		bep.setError(status.WrapError(err, "error opening build event stream"))
		return
	}

	doneReceiving := make(chan struct{}, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err == nil {
				continue
			}
			if err == io.EOF {
				log.Debug("Received all acks from server.")
			} else {
				log.Errorf("Error receiving acks from the server: %s", err)
				bep.setError(err)
			}
			doneReceiving <- struct{}{}
			return
		}
	}()

	for seqNo := int64(1); ; seqNo++ {
		event := <-bep.events
		if event == nil {
			// Wait() was called, meaning no more events to publish.
			// Send ComponentStreamFinished event before closing the stream.
			start := time.Now()
			err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
				OrderedBuildEvent: &pepb.OrderedBuildEvent{
					StreamId:       bep.streamID,
					SequenceNumber: seqNo,
					Event: &bepb.BuildEvent{
						EventTime: ptypes.TimestampNow(),
						Event: &bepb.BuildEvent_ComponentStreamFinished{ComponentStreamFinished: &bepb.BuildEvent_BuildComponentStreamFinished{
							Type: bepb.BuildEvent_BuildComponentStreamFinished_FINISHED,
						}},
					},
				},
			})
			log.Debugf("BEP: published FINISHED event (#%d) in %s", seqNo, time.Since(start))

			if err != nil {
				bep.setError(err)
				return
			}
			break
		}

		bazelEvent, err := ptypes.MarshalAny(event)
		if err != nil {
			bep.setError(status.WrapError(err, "failed to marshal bazel event"))
			return
		}
		start := time.Now()
		err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
			OrderedBuildEvent: &pepb.OrderedBuildEvent{
				StreamId:       bep.streamID,
				SequenceNumber: seqNo,
				Event: &bepb.BuildEvent{
					EventTime: ptypes.TimestampNow(),
					Event:     &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEvent},
				},
			},
		})
		log.Debugf("BEP: published event (#%d) in %s", seqNo, time.Since(start))
		if err != nil {
			bep.setError(err)
			return
		}
	}
	// After successfully transmitting all events, close our side of the stream
	// and wait for server ACKs before closing the connection.
	if err := stream.CloseSend(); err != nil {
		bep.setError(status.WrapError(err, "failed to close build event stream"))
		return
	}
	<-doneReceiving
}
func (bep *buildEventPublisher) Publish(e *bespb.BuildEvent) error {
	bep.mu.Lock()
	defer bep.mu.Unlock()
	if bep.err != nil {
		return status.WrapError(bep.err, "cannot publish event due to previous error")
	}
	bep.events <- e
	return nil
}
func (bep *buildEventPublisher) Wait() error {
	bep.events <- nil
	<-bep.done
	return bep.err
}
func (bep *buildEventPublisher) getError() error {
	bep.mu.Lock()
	defer bep.mu.Unlock()
	return bep.err
}
func (bep *buildEventPublisher) setError(err error) {
	bep.mu.Lock()
	defer bep.mu.Unlock()
	bep.err = err
}

// actionRunner runs a single action in the BuildBuddy config.
type actionRunner struct {
	action   *config.Action
	log      *invocationLog
	bep      *buildEventPublisher
	username string
	hostname string

	mu            sync.Mutex // protects(progressCount)
	progressCount int32
}

func runDebugCommand(out io.Writer, script string) {
	if !*debug {
		return
	}
	io.WriteString(out, fmt.Sprintf("(debug) # %s\n", script))
	output, err := exec.Command("sh", "-c", script).CombinedOutput()
	out.Write(output)
	exitCode := getExitCode(err)
	if exitCode != noExitCode {
		io.WriteString(out, fmt.Sprintf("%s(command exited with code %d)%s\n", ansiGray, exitCode, ansiReset))
	}
	io.WriteString(out, "===\n")
}

func (ar *actionRunner) Run(ctx context.Context, startTime time.Time) error {
	// Print the initLogs to the first action's logs.
	b, _ := io.ReadAll(&initLog)
	initLog.Reset()
	ar.log.Printf(string(b))

	ar.log.Printf("Running action: %s", ar.action.Name)

	// Only print this to the local logs -- it's mostly useful for development purposes.
	log.Infof("Invocation URL:  %s", invocationURL(ar.bep.streamID.InvocationId))

	// NOTE: In this func we return immediately with an error of nil if event publishing fails,
	// because that error is instead surfaced in the caller func when calling
	// `buildEventPublisher.Wait()`

	bep := ar.bep

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
			Uuid:            ar.bep.streamID.InvocationId,
			StartTimeMillis: startTime.UnixNano() / int64(time.Millisecond),
		}},
	}
	if err := bep.Publish(startedEvent); err != nil {
		return nil
	}
	if err := ar.flushProgress(); err != nil {
		return nil
	}

	wfc := &bespb.WorkflowConfigured{
		WorkflowId:          *workflowID,
		ActionName:          ar.action.Name,
		ActionTriggerBranch: *triggerBranch,
		ActionTriggerEvent:  *triggerEvent,
	}
	configuredEvent := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowConfigured{WorkflowConfigured: &bespb.BuildEventId_WorkflowConfiguredId{}}},
		Payload: &bespb.BuildEvent_WorkflowConfigured{WorkflowConfigured: wfc},
	}
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
	if err := bep.Publish(configuredEvent); err != nil {
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
	if err := bep.Publish(buildMetadataEvent); err != nil {
		return nil
	}

	workspaceStatusEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bespb.WorkspaceStatus{
			Item: []*bespb.WorkspaceStatus_Item{
				{Key: "BUILD_USER", Value: ar.username},
				{Key: "BUILD_HOST", Value: ar.hostname},
				{Key: "REPO_URL", Value: *repoURL},
				{Key: "COMMIT_SHA", Value: *commitSHA},
				{Key: "GIT_BRANCH", Value: *branch},
				{Key: "GIT_TREE_STATUS", Value: "Clean"},
				// TODO: Consider parsing the `.bazelrc` and running the user's actual workspace
				// status command that they've configured for bazel.
			},
		}},
	}
	if err := bep.Publish(workspaceStatusEvent); err != nil {
		return nil
	}

	// Flush whenever the log buffer fills past a certain threshold.
	ar.log.writeListener = func() {
		if size := ar.log.Len(); size >= progressFlushThresholdBytes {
			ar.flushProgress() // ignore error; it will surface in `bep.Wait()`
		}
	}
	stopFlushingProgress := ar.startBackgroundProgressFlush()
	defer stopFlushingProgress()

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
		ar.printCommandLine(args)
		// Transparently set the invocation ID from the one we computed ahead of
		// time. The UI is expecting this invocation ID so that it can render a
		// BuildBuddy invocation URL for each bazel_command that is executed.
		args = append(args, fmt.Sprintf("--invocation_id=%s", iid))

		runErr := runCommand(ctx, *bazelCommand, args /*env=*/, nil, ar.log)
		exitCode := getExitCode(runErr)
		if exitCode != noExitCode {
			ar.log.Printf("%s(command exited with code %d)%s", ansiGray, exitCode, ansiReset)
		}

		runDebugCommand(ar.log, `ls -la`)

		// Publish the status of each command as well as the finish time.
		// Stop execution early on BEP failure, but ignore error -- it will surface in `bep.Wait()`.
		completedEvent := &bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.BuildEventId_WorkflowCommandCompletedId{
				InvocationId: iid,
			}}},
			Payload: &bespb.BuildEvent_WorkflowCommandCompleted{WorkflowCommandCompleted: &bespb.WorkflowCommandCompleted{
				ExitCode:        int32(exitCode),
				StartTimeMillis: int64(float64(cmdStartTime.UnixNano()) / float64(time.Millisecond)),
				DurationMillis:  int64(float64(time.Since(cmdStartTime)) / float64(time.Millisecond)),
			}},
		}
		if err := bep.Publish(completedEvent); err != nil {
			break
		}

		if runErr != nil {
			// Return early if the command failed.
			// Note, even though we don't hit the `flushProgress` call below in this case,
			// we'll still flush progress before closing the BEP stream.
			return runErr
		}

		// Flush progress after every command.
		// Stop execution early on BEP failure, but ignore error -- it will surface in `bep.Wait()`.
		if err := ar.flushProgress(); err != nil {
			break
		}
	}
	return nil
}

func (ar *actionRunner) startBackgroundProgressFlush() func() {
	stop := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-stop:
				break
			case <-time.After(progressFlushInterval):
				ar.flushProgress()
			}
		}
	}()
	return func() {
		stop <- struct{}{}
	}
}

func (ar *actionRunner) printCommandLine(bazelArgs []string) {
	ps1End := "$"
	if ar.username == "root" {
		ps1End = "#"
	}
	command := bazeliskBinaryName
	for _, arg := range bazelArgs {
		command += " " + toShellToken(arg)
	}
	userAtHost := fmt.Sprintf("%s@%s", ar.username, ar.hostname)
	ar.log.Printf(aurora.Sprintf("\n%s%s %s", aurora.Cyan(userAtHost), ps1End, command))
}

func (ar *actionRunner) flushProgress() error {
	event, err := ar.nextProgressEvent()
	if err != nil {
		return err
	}
	if event == nil {
		// No progress to flush.
		return nil
	}

	return ar.bep.Publish(event)
}

func (ar *actionRunner) nextProgressEvent() (*bespb.BuildEvent, error) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	buf, err := ar.log.ReadAll()
	if err != nil {
		return nil, status.WrapError(err, "failed to read action logs")
	}
	if len(buf) == 0 {
		return nil, nil
	}
	count := ar.progressCount
	ar.progressCount++

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

// TODO: Handle shell variable expansion. Probably want to run this with sh -c
func bazelArgs(cmd string) ([]string, error) {
	tokens, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}
	if tokens[0] == bazelBinaryName || tokens[0] == bazeliskBinaryName {
		tokens = tokens[1:]
	}
	return tokens, nil
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

func setupGitRepo(ctx context.Context) error {
	repoDirInfo, err := os.Stat(repoDirName)
	if err != nil && !os.IsNotExist(err) {
		return status.WrapErrorf(err, "stat %q", repoDirName)
	}
	if repoDirInfo != nil {
		initLog.WriteString("Syncing existing git repo.\n")
		err := syncExistingRepo(ctx, repoDirName)
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
		initLog.WriteString("Failed to sync existing git repo.\n")
		if err := os.RemoveAll(repoDirName); err != nil {
			return status.WrapErrorf(err, "rm -r %q", repoDirName)
		}
	}

	if err := os.Mkdir(repoDirName, 0777); err != nil {
		return status.WrapErrorf(err, "mkdir %q", repoDirName)
	}

	initLog.WriteString("Cloning git repo.\n")
	return setupNewGitRepo(ctx, repoDirName)
}

func fetchOrigin(ctx context.Context, remote *git.Remote) error {
	fetchOpts := &git.FetchOptions{
		RefSpecs: []gitcfg.RefSpec{gitcfg.RefSpec(fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", *branch, *branch))},
	}
	if err := remote.FetchContext(ctx, fetchOpts); err != nil && err != git.NoErrAlreadyUpToDate {
		return status.WrapErrorf(err, "fetch remote %q", git.DefaultRemoteName)
	}
	return nil
}

func setupNewGitRepo(ctx context.Context, path string) error {
	repo, err := git.PlainInit(path, false /*=isBare*/)
	if err != nil {
		return err
	}
	authURL, err := gitutil.AuthRepoURL(*repoURL, os.Getenv(repoUserEnvVarName), os.Getenv(repoTokenEnvVarName))
	if err != nil {
		return err
	}
	remote, err := repo.CreateRemote(&gitcfg.RemoteConfig{
		Name: git.DefaultRemoteName,
		URLs: []string{authURL},
	})
	if err != nil {
		return err
	}
	if err := fetchOrigin(ctx, remote); err != nil {
		return err
	}
	tree, err := repo.Worktree()
	if err != nil {
		return status.WrapErrorf(err, "get tree")
	}
	if err := tree.Checkout(&git.CheckoutOptions{Hash: gitplumbing.NewHash(*commitSHA)}); err != nil {
		return status.WrapErrorf(err, "checkout %s", *commitSHA)
	}
	return nil
}

func syncExistingRepo(ctx context.Context, path string) error {
	repo, err := git.PlainOpen(path)
	if err != nil {
		return err
	}
	tree, err := repo.Worktree()
	if err != nil {
		return status.WrapErrorf(err, "get worktree")
	}
	if err := tree.Clean(&git.CleanOptions{Dir: true}); err != nil {
		return status.WrapErrorf(err, "clean")
	}
	remote, err := repo.Remote(git.DefaultRemoteName)
	if err != nil {
		return status.WrapErrorf(err, "get existing remote %q", git.DefaultRemoteName)
	}
	if err := fetchOrigin(ctx, remote); err != nil {
		return err
	}
	checkoutOpts := &git.CheckoutOptions{
		Hash:  gitplumbing.NewHash(*commitSHA),
		Force: true, // remove local changes
	}
	if err := tree.Checkout(checkoutOpts); err != nil {
		return status.WrapErrorf(err, "checkout %s", *commitSHA)
	}
	return nil
}

func invocationURL(invocationID string) string {
	urlPrefix := *besResultsURL
	if !strings.HasSuffix(urlPrefix, "/") {
		urlPrefix = urlPrefix + "/"
	}
	return urlPrefix + invocationID
}

func readConfig() (*config.BuildBuddyConfig, error) {
	f, err := os.Open(actionsConfigPath)
	if err != nil {
		if os.IsNotExist(err) {
			return getDefaultConfig(), nil
		}
		return nil, status.FailedPreconditionErrorf("open %q: %s", actionsConfigPath, err)
	}
	c, err := config.NewConfig(f)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("read %q: %s", actionsConfigPath, err)
	}
	return c, nil
}

func getDefaultConfig() *config.BuildBuddyConfig {
	// By default, test all targets when any branch is pushed.
	// TODO: Consider running a bazel query to find only the targets that are
	// affected by the changed files.
	return &config.BuildBuddyConfig{
		Actions: []*config.Action{
			{
				Name: "Test all targets",
				Triggers: &config.Triggers{
					Push: &config.PushTrigger{Branches: []string{*triggerBranch}},
				},
				BazelCommands: []string{
					// TOOD: Consider enabling remote_cache and remote_executor along with
					// recommended RBE flags.
					fmt.Sprintf(
						"test //... "+
							"--build_metadata=ROLE=CI "+
							"--bes_backend=%s --bes_results_url=%s "+
							"--remote_header=x-buildbuddy-api-key=%s",
						*besBackend, *besResultsURL,
						os.Getenv(buildbuddyAPIKeyEnvVarName),
					),
				},
			},
		},
	}
}

func matchesAnyTrigger(action *config.Action, event, branch string) bool {
	if action.Triggers == nil {
		return false
	}
	if pushCfg := action.Triggers.Push; pushCfg != nil && event == pushEventName {
		return matchesAnyBranch(pushCfg.Branches, branch)
	}

	if prCfg := action.Triggers.PullRequest; prCfg != nil && event == pullRequestEventName {
		return matchesAnyBranch(prCfg.Branches, branch)
	}
	return false
}

func matchesAnyBranch(branches []string, branch string) bool {
	for _, b := range branches {
		if b == branch {
			return true
		}
	}
	return false
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
