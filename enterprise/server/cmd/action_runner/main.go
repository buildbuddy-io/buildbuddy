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
	repoDirName         = "repo-root"
	repoUserEnvVarName  = "REPO_USER"
	repoTokenEnvVarName = "REPO_TOKEN"

	// "Unique"-ish exit code that indicates the runner should be retried
	// due to an error that may be temporary.
	retryableExitCode = 21

	pushEventName        = "push"
	pullRequestEventName = "pull_request"

	// ANSI codes for easier-to-read output
	textDim    = "\033[0;2m"
	textGreen  = "\033[0;32m"
	textYellow = "\033[0;33m"
	textBlue   = "\033[0;34m"
	textReset  = "\033[0m"
)

var (
	besBackend    = flag.String("bes_backend", "", "gRPC endpoint for BES backend")
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
	for _, action := range cfg.Actions {
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
			action: action,
			log:    newActionLog(),
			bep:    bep,
		}
		exitCode := 0
		if err := ar.Run(ctx); err != nil {
			ar.log.Printf("action runner failed: %s", err)
			if err, ok := err.(*exec.ExitError); ok {
				// Exit with the error code of the last command that failed.
				exitCode = err.ExitCode()
			}
			exitCode = 1
		}

		// ignore errors returned here; they'll be surfaced in `bep.Wait()` below
		bep.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
			Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
				ExitCode:         &bespb.BuildFinished_ExitCode{Code: int32(exitCode)},
				FinishTimeMillis: time.Now().UnixNano() / int64(time.Millisecond),
			}},
			// NB: This is the last message! If we want to add more messages later
			// such as BuildToolLogs, we'll need to move this flag.
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
		// After transmitting all events, swallow all acks from the server and
		// then close the stream, if there haven't been any errors yet.
		defer func() {
			ep.mu.Lock()
			err := ep.err
			ep.mu.Unlock()
			if err == nil {
				return
			}
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					fmt.Printf("error receiving acks from the server: %s", err)
					ep.setError(err)
					return
				}
			}
			if err := stream.CloseSend(); err != nil {
				ep.setError(fmt.Errorf("failed to close build event stream: %s", err))
			}
		}()

		for seqNo := int64(1); ; seqNo++ {
			event := <-ep.events
			if event == nil { // Wait() was called, meaning no more events to publish
				return
			}

			bazelEvent, err := ptypes.MarshalAny(event)
			if err != nil {
				ep.setError(err)
				return
			}
			log.Printf("BEP: publishing event #%d", seqNo)
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
	action *workflowconf.Action
	log    *actionLog
	bep    *buildEventPublisher
}

func (ar *actionRunner) Run(ctx context.Context) error {
	ar.log.Printf("=== BuildBuddy Action Logs ===")
	ar.log.Printf("Action:        %s", ar.action.Name)
	ar.log.Printf("InvocationID:  %s", ar.bep.streamID.InvocationId)
	ar.log.Printf("")

	// NOTE: In this func we return immediately with an error of nil if event publishing fails,
	// because that error is instead surfaced in the caller func when calling
	// `buildEventPublisher.Wait()`

	bep := ar.bep

	startedEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
			Uuid:            ar.bep.streamID.InvocationId,
			StartTimeMillis: time.Now().UnixNano() / int64(time.Millisecond),
		}},
	}
	if err := bep.Publish(startedEvent); err != nil {
		return nil
	}
	workspaceStatusEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bespb.WorkspaceStatus{
			Item: []*bespb.WorkspaceStatus_Item{
				{Key: "REPO_URL", Value: *repoURL},
				{Key: "COMMIT_SHA", Value: *commitSHA},
				{Key: "GIT_TREE_STATUS", Value: "Clean"},
				// TODO: Populate GIT_BRANCH. Can't source this from the `trigger_branch` flag
				// in the PR case, because that refers to the branch into which the PR would be
				// merged, which doesn't reflect the currently checked out branch.
			}},
		},
	}
	if err := bep.Publish(workspaceStatusEvent); err != nil {
		return nil
	}

	for _, bazelCmd := range ar.action.BazelCommands {
		// Provide some info to help make it clear which output is coming
		// from which bazel commands.
		ar.log.Printf("%s@buildbuddy-action-runner%s# %s", textBlue, textReset, bazelCmd)
		args, err := bazelArgs(bazelCmd)
		if err != nil {
			return fmt.Errorf("failed to parse bazel command: %s", err)
		}
		// TODO: Use sh -c 'bazelisk ...' to support env var expansion for private repos.
		err = runCommand(ctx, "bazelisk", args, emptyEnv, ar.log.writer)
		if exitCode := getExitCode(err); exitCode >= 0 {
			ar.log.Printf("%s(command exited with code %d)%s", textDim, exitCode, textReset)
		}
		if err != nil {
			return err
		}
	}
	return nil
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

func readConfig() (*workflowconf.BuildBuddyConfig, error) {
	f, err := os.Open(workflowconf.PathRelativeToRepoRoot)
	if err != nil {
		return nil, fmt.Errorf("open %q: %s", workflowconf.PathRelativeToRepoRoot, err)
	}
	c, err := workflowconf.NewConfig(f)
	if err != nil {
		return nil, fmt.Errorf("read %q: %s", workflowconf.PathRelativeToRepoRoot, err)
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
	if *repoURL == "" {
		errs = append(errs, "--repo_url unset")
	}
	if *commitSHA == "" {
		errs = append(errs, "--commit_sha unset")
	}
	if *triggerBranch == "" {
		errs = append(errs, "--trigger_branch unset")
	}
	if *triggerEvent == "" {
		errs = append(errs, "--trigger_event unset")
	}
	if *besBackend == "" {
		errs = append(errs, "--bes_backend unset")
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
