package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
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

	// Env vars
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

	// Binary constants

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

	shellCharsRequiringQuote = regexp.MustCompile(`[^\w@%+=:,./-]`)
)

func main() {
	im := &initMetrics{start: time.Now()}

	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()

	if err := setupGitRepo(ctx); err != nil {
		fatal(status.WrapError(err, "failed to clone repo"))
	}
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
		log.Printf("failed to get hostname: %s", err)
		hostname = ""
	}
	user, err := user.Current()
	username := ""
	if err != nil {
		log.Printf("failed to get user: %s", err)
	} else {
		username = user.Username
	}

	for _, action := range cfg.Actions {
		startTime := time.Now()

		if !matchesAnyTrigger(action, *triggerEvent, *triggerBranch) {
			log.Printf("No triggers matched for %q event with target branch %q. Action config:\n===\n%s===", *triggerEvent, *triggerBranch, actionDebugString(action))
			continue
		}

		bep := newBuildEventPublisher(&bepb.StreamId{
			InvocationId: newUUID(),
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
	lockingbuffer.LockingBuffer

	writer        io.Writer
	writeListener func()
}

func newInvocationLog() *invocationLog {
	invLog := &invocationLog{writeListener: func() {}}
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, os.Stdout)
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
	streamID *bepb.StreamId
	done     chan struct{}
	events   chan *bespb.BuildEvent

	mu  sync.Mutex
	err error
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
				log.Println("Received all acks from server.")
			} else {
				log.Printf("Error receiving acks from the server: %s", err)
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
			log.Printf("BEP: published FINISHED event (#%d) in %s", seqNo, time.Since(start))

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
		log.Printf("BEP: published event (#%d) in %s", seqNo, time.Since(start))
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
	action        *config.Action
	log           *invocationLog
	bep           *buildEventPublisher
	progressCount int32
	username      string
	hostname      string
}

func (ar *actionRunner) Run(ctx context.Context, startTime time.Time) error {
	ar.log.Printf("Running action: %s", ar.action.Name)

	// Only print this to the local logs -- it's mostly useful for development purposes.
	log.Printf("Invocation URL:  %s", invocationURL(ar.bep.streamID.InvocationId))

	// NOTE: In this func we return immediately with an error of nil if event publishing fails,
	// because that error is instead surfaced in the caller func when calling
	// `buildEventPublisher.Wait()`

	bep := ar.bep

	startedEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
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
	buildMetadataEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: &bespb.BuildMetadata{
			Metadata: map[string]string{
				"ROLE":                             "CI_RUNNER",
				"BUILDBUDDY_WORKFLOW_ID":           *workflowID,
				"BUILDBUDDY_ACTION_NAME":           ar.action.Name,
				"BUILDBUDDY_ACTION_TRIGGER_EVENT":  *triggerEvent,
				"BUILDBUDDY_ACTION_TRIGGER_BRANCH": *triggerBranch,
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

	for _, bazelCmd := range ar.action.BazelCommands {
		args, err := bazelArgs(bazelCmd)
		if err != nil {
			return status.InvalidArgumentErrorf("failed to parse bazel command: %s", err)
		}
		ar.printCommandLine(args)
		err = runCommand(ctx, bazeliskBinaryName, args /*env=*/, nil, ar.log)
		if exitCode := getExitCode(err); exitCode != noExitCode {
			ar.log.Printf("%s(command exited with code %d)%s", ansiGray, exitCode, ansiReset)
		}
		if err != nil {
			// Note, even though we don't hit the `flushProgress` call below in this case,
			// we'll still flush progress before closing the BEP stream.
			return err
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
	buf, err := ioutil.ReadAll(ar.log)
	if err != nil {
		return status.WrapError(err, "failed to read action logs")
	}
	if len(buf) == 0 {
		return nil
	}
	count := ar.progressCount
	ar.progressCount++
	output := string(buf)

	return ar.bep.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count + 1}}},
		},
		Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{
			// Only outputting to stderr for now, like Bazel does.
			Stderr: output,
		}},
	})
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

func setupGitRepo(ctx context.Context) error {
	if err := os.Mkdir(repoDirName, 0o775); err != nil {
		return status.WrapErrorf(err, "mkdir %q", repoDirName)
	}
	if err := os.Chdir(repoDirName); err != nil {
		return status.WrapErrorf(err, "cd %q", repoDirName)
	}

	repo, err := git.PlainInit("." /*isBare=*/, false)
	if err != nil {
		return err
	}
	authURL, err := gitutil.AuthRepoURL(*repoURL, os.Getenv(repoUserEnvVarName), os.Getenv(repoTokenEnvVarName))
	if err != nil {
		return err
	}
	remote, err := repo.CreateRemote(&gitcfg.RemoteConfig{
		Name: "origin",
		URLs: []string{authURL},
	})
	if err != nil {
		return err
	}

	fetchOpts := &git.FetchOptions{
		RefSpecs: []gitcfg.RefSpec{gitcfg.RefSpec(fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", *branch, *branch))},
	}
	if err := remote.FetchContext(ctx, fetchOpts); err != nil {
		return err
	}
	tree, err := repo.Worktree()
	if err != nil {
		return err
	}
	if err := tree.Checkout(&git.CheckoutOptions{Hash: gitplumbing.NewHash(*commitSHA)}); err != nil {
		return err
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
		return nil, status.FailedPreconditionErrorf("open %q: %s", actionsConfigPath, err)
	}
	c, err := config.NewConfig(f)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("read %q: %s", actionsConfigPath, err)
	}
	return c, nil
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
	log.Printf("%s", err)
	os.Exit(int(gstatus.Code(err)))
}
