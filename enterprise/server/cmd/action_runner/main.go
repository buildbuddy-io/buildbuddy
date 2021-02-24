package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflowconf"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

const (
	// Name of the dir into which the repo is cloned.
	repoDirName = "repo-root"
	// Path where we expect to find actions config, relative to the repo root.
	actionsConfigPath = "buildbuddy.yaml"

	// env vars
	// TODO: Allow populating BUILDBUDDY_API_KEY for private repos,
	// in case folks would rather have it passed via BB than have
	// to check it in to their repo.

	// NOTE: These env vars are not populated for non-private repos.
	repoUserEnvVarName  = "REPO_USER"
	repoTokenEnvVarName = "REPO_TOKEN"

	// "Unique"-ish exit code that indicates the runner should be retried
	// due to an error that may be temporary.
	retryableExitCode = 21

	// Webhook event names

	pushEventName        = "push"
	pullRequestEventName = "pull_request"

	// ANSI codes for nicer output

	textGreen  = "\033[32m"
	textYellow = "\033[33m"
	textBlue   = "\033[34m"
	textPurple = "\033[35m"
	textCyan   = "\033[36m"
	textGray   = "\033[90m"
	textReset  = "\033[0m"
)

var (
	besBackend    = flag.String("bes_backend", "", "gRPC endpoint for BuildBuddy's BES backend.")
	besResultsURL = flag.String("bes_results_url", "", "URL prefix for BuildBuddy invocation URLs.")
	repoURL       = flag.String("repo_url", "", "URL of the Git repo to check out.")
	commitSHA     = flag.String("commit_sha", "", "SHA of the commit to be checked out.")
	triggerEvent  = flag.String("trigger_event", "", "Event type that triggered the action runner.")
	triggerBranch = flag.String("trigger_branch", "", "Branch to check action triggers against.")

	emptyEnv = map[string]string{}
)

func main() {
	flag.Parse()
	if err := validateFlags(); err != nil {
		log.Printf("invalid flags: %s", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err := cloneRepo(ctx); err != nil {
		log.Printf("failed to clone repo: %s", err)
		os.Exit(retryableExitCode)
	}
	cfg, err := readConfig()
	if err != nil {
		log.Printf("failed to read BuildBuddy config: %s", err)
		os.Exit(1)
	}

	// For now, we run all actions in serial, creating a synthetic invocation for each one.
	// Git provider statuses will be published by the build event handler, as we stream
	// synthetic build events to the build event stream associated with the invocation.
	RunAllActions(ctx, cfg)
}

func RunAllActions(ctx context.Context, cfg *workflowconf.BuildBuddyConfig) {
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
			log.Printf("No triggers matched for %q event with target branch %q. Action config:\n<<<\n%s>>>", *triggerEvent, *triggerBranch, actionDebugString(action))
			continue
		}

		invocationID, err := uuid.NewRandom()
		if err != nil {
			log.Printf("failed to generate invocation ID: %s", err)
			os.Exit(retryableExitCode)
		}
		buildID, err := uuid.NewRandom()
		if err != nil {
			log.Printf("failed to generate build ID: %s", err)
			os.Exit(retryableExitCode)
		}
		bep := newBuildEventPublisher(&bepb.StreamId{
			InvocationId: invocationID.String(),
			BuildId:      buildID.String(),
		})
		bep.Start(ctx)

		// NB: Anything logged to `ar.log` gets output to both the stdout of this binary
		// and the logs uploaded to BuildBuddy for this action. Anything that we want
		// the user to see in the invocation UI needs to go in that log, instead of
		// the global `log.Print`.
		ar := &actionRunner{
			action:   action,
			log:      newActionLog(),
			bep:      bep,
			hostname: hostname,
			username: username,
		}
		exitCode := 0
		if err := ar.Run(ctx); err != nil {
			ar.log.Printf("action runner failed: %s", err)
			exitCode = getExitCode(err)
			// Treat "command not found", "process killed" etc. as exit code 1 so the build event
			// handler will treat it as a failure.
			if exitCode == -1 {
				exitCode = 1
			}
		}

		// Ignore errors from the events published here; they'll be surfaced in `bep.Wait()`
		ar.flushProgress()
		bep.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
			Children: []*bespb.BuildEventId{
				{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
			},
			Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
				ExitCode:         &bespb.BuildFinished_ExitCode{Code: int32(exitCode)},
				FinishTimeMillis: time.Now().UnixNano() / int64(time.Millisecond),
			}},
		})
		elapsedTimeSeconds := float64(time.Since(startTime)) / float64(time.Second)
		// NB: This is the last message
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
			log.Printf("failed to publish build event for action %q: %s", action.Name, err)
			// If we don't publish a build event successfully, then the status may not be
			// reported to the Git provider successfully. Terminate with a code indicating
			// that the executor can retry the action, so that we have another chance.
			os.Exit(retryableExitCode)
		}
	}
}

type actionLog struct {
	Buffer bytes.Buffer
	writer io.Writer
}

func newActionLog() *actionLog {
	al := &actionLog{}
	al.writer = io.MultiWriter(os.Stdout, &al.Buffer)
	return al
}
func (al *actionLog) Println(vals ...interface{}) {
	al.writer.Write([]byte(fmt.Sprintln(vals...)))
}
func (al *actionLog) Printf(format string, vals ...interface{}) {
	al.writer.Write([]byte(fmt.Sprintf(format+"\n", vals...)))
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
		// publish a few events and they should get flushed quickly. The large
		// size is just to be safe.
		events: make(chan *bespb.BuildEvent, 128),
		done:   make(chan struct{}, 1),
	}
}

// Start the event publishing loop. Stops handling new events as soon as the first
// call to `Wait()` occurs.
func (ep *buildEventPublisher) Start(ctx context.Context) {
	go func() {
		defer func() {
			ep.done <- struct{}{}
		}()

		opts := []grpc.DialOption{}
		backendURL, err := url.Parse(*besBackend)
		if err != nil {
			ep.setError(fmt.Errorf("invalid bes_backend %q: %s", *besBackend, err))
			return
		}
		if backendURL.Scheme == "grpc" {
			opts = append(opts, grpc.WithInsecure())
		}
		backendURL.Scheme = ""
		target := strings.TrimPrefix(backendURL.String(), "//")
		conn, err := grpc.Dial(target, opts...)
		if err != nil {
			ep.setError(fmt.Errorf("dialing bes_backend failed: %s", err))
			return
		}
		defer conn.Close()

		besClient := pepb.NewPublishBuildEventClient(conn)
		stream, err := besClient.PublishBuildToolEventStream(ctx)
		if err != nil {
			ep.setError(fmt.Errorf("error opening build event stream: %s", err))
		}

		doneReceiving := make(chan struct{}, 1)
		go func() {
			for {
				_, err := stream.Recv()
				if err == nil {
					continue
				}
				if err == io.EOF {
					log.Println("BEP: Received all acks from server.")
				} else {
					log.Printf("BEP: error receiving acks from the server: %s", err)
					ep.setError(err)
				}
				doneReceiving <- struct{}{}
				return
			}
		}()

		// After transmitting all events, close the stream and then wait for
		// acks from the server.
		defer func() {
			ep.mu.Lock()
			err := ep.err
			ep.mu.Unlock()
			if err != nil {
				log.Printf("BEP: cannot close stream due to previous error: %s", err)
				return
			}

			if err := stream.CloseSend(); err != nil {
				ep.setError(fmt.Errorf("failed to close build event stream: %s", err))
				return
			}
			<-doneReceiving
		}()

		for seqNo := int64(1); ; seqNo++ {
			event := <-ep.events
			if event == nil {
				// Wait() was called, meaning no more events to publish.
				// Send final event before closing the stream.
				log.Printf("BEP: publishing FINISHED event (sequence number = %d)", seqNo)
				err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
					OrderedBuildEvent: &pepb.OrderedBuildEvent{
						StreamId:       ep.streamID,
						SequenceNumber: seqNo,
						Event: &bepb.BuildEvent{
							EventTime: ptypes.TimestampNow(),
							Event: &bepb.BuildEvent_ComponentStreamFinished{ComponentStreamFinished: &bepb.BuildEvent_BuildComponentStreamFinished{
								Type: bepb.BuildEvent_BuildComponentStreamFinished_FINISHED,
							}},
						},
					},
				})
				if err != nil {
					ep.setError(err)
				}
				return
			}

			bazelEvent, err := ptypes.MarshalAny(event)
			if err != nil {
				ep.setError(err)
				return
			}
			log.Printf("BEP: publishing event (sequence number = %d)", seqNo)
			err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
				OrderedBuildEvent: &pepb.OrderedBuildEvent{
					StreamId:       ep.streamID,
					SequenceNumber: seqNo,
					Event: &bepb.BuildEvent{
						Event: &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEvent},
					},
				},
			})
			if err != nil {
				ep.setError(err)
				return
			}
		}
	}()
}
func (ep *buildEventPublisher) Publish(e *bespb.BuildEvent) error {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.err != nil {
		return fmt.Errorf("cannot publish event due to previous error: %s", ep.err)
	}
	ep.events <- e
	return nil
}
func (ep *buildEventPublisher) Wait() error {
	ep.events <- nil
	<-ep.done
	return ep.err
}
func (ep *buildEventPublisher) setError(err error) {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	ep.err = err
}

// actionRunner runs a single action in the BuildBuddy config.
type actionRunner struct {
	action        *workflowconf.Action
	log           *actionLog
	bep           *buildEventPublisher
	progressCount int32
	username      string
	hostname      string
}

func (ar *actionRunner) Run(ctx context.Context) error {
	ar.log.Printf("Action:          %s", ar.action.Name)
	ar.log.Printf("Triggered by:    %s to branch %q", *triggerEvent, *triggerBranch)
	ar.log.Printf("Invocation ID:   %s", ar.bep.streamID.InvocationId)
	ar.log.Printf("Invocation URL:  %s", invocationURL(ar.bep.streamID.InvocationId))

	// NOTE: In this func we return immediately with an error of nil if event publishing fails,
	// because that error is instead surfaced in the caller func when calling
	// `buildEventPublisher.Wait()`

	bep := ar.bep

	startedEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 0}}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
			Uuid:            ar.bep.streamID.InvocationId,
			StartTimeMillis: time.Now().UnixNano() / int64(time.Millisecond),
		}},
	}
	if err := bep.Publish(startedEvent); err != nil {
		return nil
	}
	if err := ar.flushProgress(); err != nil {
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
				{Key: "GIT_TREE_STATUS", Value: "Clean"},
				// TODO: Populate GIT_BRANCH. Can't source this from the `trigger_branch` flag
				// in the PR case, because that refers to the branch into which the PR would be
				// merged, which doesn't reflect the currently checked out branch.
			},
		}},
	}
	if err := bep.Publish(workspaceStatusEvent); err != nil {
		return nil
	}

	ps1End := "$"
	if ar.username == "root" {
		ps1End = "#"
	}
	for _, bazelCmd := range ar.action.BazelCommands {
		args, err := bazelArgs(bazelCmd)
		// Provide some info to help make it clear which output is coming
		// from which bazel commands.
		ar.log.Printf("\n%s%s@%s%s%s bazelisk %s", textCyan, ar.username, ar.hostname, textReset, ps1End, strings.Join(args, " "))
		if err != nil {
			return fmt.Errorf("failed to parse bazel command: %s", err)
		}
		// TODO: Use sh -c 'bazelisk ...' to support env var expansion for private repos.
		err = runCommand(ctx, "bazelisk", args, emptyEnv, ar.log.writer)
		if exitCode := getExitCode(err); exitCode >= 0 {
			ar.log.Printf("%s(command exited with code %d)%s", textGray, exitCode, textReset)
		}
		if err := ar.flushProgress(); err != nil {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (ar *actionRunner) flushProgress() error {
	if ar.log.Buffer.Len() == 0 {
		return nil
	}

	count := ar.progressCount
	ar.progressCount++
	output := string(ar.log.Buffer.Bytes())
	ar.log.Buffer.Reset()

	return ar.bep.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count + 1}}},
		},
		Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{
			// Only outputting to stdout for now.
			Stdout: output,
		}},
	})
}

// TODO: Handle shell variable expansion. Probably want to run this with sh -c
func bazelArgs(cmd string) ([]string, error) {
	tokens, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}
	if tokens[0] == "bazel" || tokens[0] == "bazelisk" {
		tokens = tokens[1:]
	}
	return tokens, nil
}

func cloneRepo(ctx context.Context) error {
	if err := os.Mkdir(repoDirName, 0o775); err != nil {
		return fmt.Errorf("mkdir %q: %s", repoDirName, err)
	}
	if err := os.Chdir(repoDirName); err != nil {
		return fmt.Errorf("cd %q: %s", repoDirName, err)
	}
	if err := runCommand(ctx, "git", []string{"init"}, emptyEnv, os.Stdout); err != nil {
		return err
	}
	authURL, err := authRepoURL()
	if err != nil {
		return err
	}
	if err := runCommand(ctx, "git", []string{"remote", "add", "origin", authURL}, emptyEnv, os.Stdout); err != nil {
		return err
	}
	if err := runCommand(ctx, "git", []string{"fetch", "origin", *commitSHA}, emptyEnv, os.Stdout); err != nil {
		return err
	}
	if err := runCommand(ctx, "git", []string{"checkout", *commitSHA}, emptyEnv, os.Stdout); err != nil {
		return err
	}
	return nil
}

func authRepoURL() (string, error) {
	user := os.Getenv(repoUserEnvVarName)
	token := os.Getenv(repoTokenEnvVarName)
	if user == "" && token == "" {
		return *repoURL, nil
	}
	u, err := url.Parse(*repoURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse repo URL %q: %s", *repoURL, err)
	}
	u.User = url.UserPassword(user, token)
	return u.String(), nil
}

func invocationURL(invocationID string) string {
	urlPrefix := *besResultsURL
	if !strings.HasSuffix(urlPrefix, "/") {
		urlPrefix = urlPrefix + "/"
	}
	return urlPrefix + invocationID
}

func readConfig() (*workflowconf.BuildBuddyConfig, error) {
	f, err := os.Open(actionsConfigPath)
	if err != nil {
		return nil, fmt.Errorf("open %q: %s", actionsConfigPath, err)
	}
	c, err := workflowconf.NewConfig(f)
	if err != nil {
		return nil, fmt.Errorf("read %q: %s", actionsConfigPath, err)
	}
	return c, nil
}

func matchesAnyTrigger(action *workflowconf.Action, event, branch string) bool {
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
	if branches == nil {
		return false
	}
	for _, b := range branches {
		if b == branch {
			return true
		}
	}
	return false
}

func runCommand(ctx context.Context, executable string, args []string, env map[string]string, log io.Writer) error {
	cmd := exec.CommandContext(ctx, executable, args...)
	// Stream command output to the log.
	// No need to differentiate between stdout and stderr for now.
	cmd.Stdout = log
	cmd.Stderr = log
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
	return -1
}

func validateFlags() error {
	errs := []string{}
	for requiredFlag, value := range map[string]string{
		"repo_url":        *repoURL,
		"commit_sha":      *commitSHA,
		"trigger_branch":  *triggerBranch,
		"trigger_event":   *triggerEvent,
		"bes_backend":     *besBackend,
		"bes_results_url": *besResultsURL,
	} {
		if value == "" {
			errs = append(errs, fmt.Sprintf("--%s unset", requiredFlag))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf(strings.Join(errs, "; "))
}

func actionDebugString(action *workflowconf.Action) string {
	yamlBytes, err := yaml.Marshal(action)
	if err != nil {
		return fmt.Sprintf("<failed to marshal action: %s>", err)
	}
	return string(yamlBytes)
}
