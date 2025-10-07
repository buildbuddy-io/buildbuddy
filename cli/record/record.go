package record

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

var (
	flags = flag.NewFlagSet("record", flag.ContinueOnError)

	besBackend   = flags.String("bes_backend", "remote.buildbuddy.io", "BuildBuddy BES backend target")
	resultsURL   = flags.String("results_url", "https://app.buildbuddy.io", "BuildBuddy results URL")
	invocationID = flags.String("invocation_id", "", "Invocation ID to use (auto-generated if not specified)")

	usage = `
usage: bb record [options] <command> [args...]

Records command output and streams it to BuildBuddy.

Example:
  $ bb record npm install
  $ bb record make build
  $ bb record --invocation_id=my-build ./build.sh

The command will be executed locally and its output will be streamed to
BuildBuddy, where you can view it in the UI.
`
)

func HandleRecord(args []string) (int, error) {
	// Parse flags until we hit a non-flag argument
	flags.SetOutput(io.Discard)
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			flags.SetOutput(os.Stderr)
			flags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}

	cmdArgs := flags.Args()
	if len(cmdArgs) == 0 {
		log.Print("error: must provide a command to execute")
		log.Print(usage)
		return 1, nil
	}

	exitCode, err := record(cmdArgs)
	if err != nil {
		log.Printf("Failed to record command: %s", err)
		return exitCode, err
	}
	return exitCode, nil
}

func record(cmdArgs []string) (int, error) {
	ctx := context.Background()

	iid := *invocationID
	if iid == "" {
		iid = uuid.New()
	}

	log.Debugf("Starting recording session with invocation ID: %s", iid)

	invocationURL := fmt.Sprintf("%s/invocation/%s", *resultsURL, iid)
	streamingLog := fmt.Sprintf("\033[32mINFO:\033[0m Streaming results to: \033[4;34m%s\033[0m\n", invocationURL)

	fmt.Fprintf(os.Stderr, "%s\n\n", streamingLog)

	publisher, err := NewPublisher(ctx, *besBackend, iid)
	if err != nil {
		return 1, status.WrapError(err, "failed to create publisher")
	}

	if err := publisher.Start(ctx); err != nil {
		return 1, status.WrapError(err, "failed to start publisher")
	}

	if err := publisher.PublishStarted(cmdArgs); err != nil {
		log.Warnf("Failed to publish started event: %s", err)
	}

	if err := publisher.PublishStructuredCommandLine(cmdArgs); err != nil {
		log.Warnf("Failed to publish structured command line: %s", err)
	}

	if err := publisher.PublishBuildMetadata(); err != nil {
		log.Warnf("Failed to publish build metadata: %s", err)
	}

	if err := publisher.PublishWorkspaceStatus(); err != nil {
		log.Warnf("Failed to publish workspace status: %s", err)
	}

	if err := publisher.PublishConfiguration(); err != nil {
		log.Warnf("Failed to publish configuration: %s", err)
	}

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 1, status.WrapError(err, "failed to create stdout pipe")
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return 1, status.WrapError(err, "failed to create stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return 1, status.WrapError(err, "failed to start command")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		streamOutput(stdout, os.Stdout, publisher, bepb.ConsoleOutputStream_STDOUT)
	}()

	go func() {
		defer wg.Done()
		streamOutput(stderr, os.Stderr, publisher, bepb.ConsoleOutputStream_STDERR)
	}()

	wg.Wait()

	exitCode := 0
	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			log.Warnf("Command failed: %s", err)
			exitCode = 1
		}
	}

	if err := publisher.PublishFinished(exitCode); err != nil {
		log.Warnf("Failed to publish finished event: %s", err)
	}

	if err := publisher.Finish(); err != nil {
		log.Warnf("Failed to finish publishing events: %s", err)
	}

	fmt.Fprintf(os.Stderr, "\n%s\n", streamingLog)

	return exitCode, nil
}

func streamOutput(src io.Reader, dst io.Writer, publisher *Publisher, streamType bepb.ConsoleOutputStream) {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(nil, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(dst, line)
		if err := publisher.PublishConsoleOutput(line+"\n", streamType); err != nil {
			log.Debugf("Failed to publish console output: %s", err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Debugf("Error reading output: %s", err)
	}
}

type Publisher struct {
	streamID   *bepb.StreamId
	besBackend string
	ctx        context.Context
	startTime  time.Time

	mu             sync.Mutex
	sequenceNumber int64
	stream         pepb.PublishBuildEvent_PublishBuildToolEventStreamClient
	streamErr      error
	finished       bool
	recvDone       chan error
}

func NewPublisher(ctx context.Context, besBackend, invocationID string) (*Publisher, error) {
	buildID := uuid.New()
	streamID := &bepb.StreamId{
		InvocationId: invocationID,
		BuildId:      buildID,
	}

	return &Publisher{
		streamID:       streamID,
		besBackend:     besBackend,
		ctx:            ctx,
		sequenceNumber: 0,
		startTime:      time.Now(),
		recvDone:       make(chan error, 1),
	}, nil
}

func (p *Publisher) Start(ctx context.Context) error {
	apiKey := ""
	if key, err := login.GetAPIKey(); err == nil {
		apiKey = strings.TrimSpace(key)
	}

	conn, err := grpc_client.DialSimple(p.besBackend)
	if err != nil {
		return status.WrapError(err, "error dialing bes_backend")
	}

	besClient := pepb.NewPublishBuildEventClient(conn)

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	stream, err := besClient.PublishBuildToolEventStream(ctx)
	if err != nil {
		return status.WrapError(err, "error creating build event stream")
	}

	p.stream = stream

	// Start receiving responses in the background
	go func() {
		defer close(p.recvDone)
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				p.mu.Lock()
				p.streamErr = err
				p.mu.Unlock()
				p.recvDone <- err
				return
			}
		}
	}()

	return nil
}

func (p *Publisher) publish(bazelEvent *bespb.BuildEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.finished {
		return status.FailedPreconditionError("publisher already finished")
	}

	if p.streamErr != nil {
		return p.streamErr
	}

	p.sequenceNumber++

	bazelEventAny, err := anypb.New(bazelEvent)
	if err != nil {
		return status.WrapError(err, "failed to marshal bazel event")
	}

	event := &bepb.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEventAny},
	}

	obe := &pepb.OrderedBuildEvent{
		StreamId:       p.streamID,
		SequenceNumber: p.sequenceNumber,
		Event:          event,
	}

	req := &pepb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: obe,
	}

	if err := p.stream.Send(req); err != nil {
		p.streamErr = err
		return status.WrapError(err, "failed to send event")
	}

	return nil
}

func (p *Publisher) PublishStarted(cmdArgs []string) error {
	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{},
		},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildMetadata{}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{}},
			{Id: &bespb.BuildEventId_Configuration{Configuration: &bespb.BuildEventId_ConfigurationId{Id: "host"}}},
			{Id: &bespb.BuildEventId_BuildFinished{}},
			{Id: &bespb.BuildEventId_StructuredCommandLine{
				StructuredCommandLine: &bespb.BuildEventId_StructuredCommandLineId{
					CommandLineLabel: "original",
				},
			}},
		},
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{
				Uuid:               p.streamID.InvocationId,
				StartTime:          timestamppb.New(p.startTime),
				Command:            strings.Join(cmdArgs[:min(2, len(cmdArgs))], " "),
				OptionsDescription: strings.Join(cmdArgs, " "),
			},
		},
	}

	return p.publish(bazelEvent)
}

func (p *Publisher) PublishStructuredCommandLine(cmdArgs []string) error {
	sections := []*clpb.CommandLineSection{
		{
			SectionLabel: "command",
			SectionType: &clpb.CommandLineSection_ChunkList{
				ChunkList: &clpb.ChunkList{
					Chunk: []string{cmdArgs[0]},
				},
			},
		},
	}

	if len(cmdArgs) > 1 {
		sections = append(sections, &clpb.CommandLineSection{
			SectionLabel: "arguments",
			SectionType: &clpb.CommandLineSection_ChunkList{
				ChunkList: &clpb.ChunkList{
					Chunk: cmdArgs[1:],
				},
			},
		})
	}

	commandLine := &clpb.CommandLine{
		CommandLineLabel: "original",
		Sections:         sections,
	}

	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_StructuredCommandLine{
				StructuredCommandLine: &bespb.BuildEventId_StructuredCommandLineId{
					CommandLineLabel: "original",
				},
			},
		},
		Payload: &bespb.BuildEvent_StructuredCommandLine{
			StructuredCommandLine: commandLine,
		},
	}


	return p.publish(bazelEvent)
}

func (p *Publisher) PublishBuildMetadata() error {
	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_BuildMetadata{},
		},
		Payload: &bespb.BuildEvent_BuildMetadata{
			BuildMetadata: &bespb.BuildMetadata{
				Metadata: map[string]string{
					"ROLE": "RECORD",
				},
			},
		},
	}
	return p.publish(bazelEvent)
}

func (p *Publisher) PublishWorkspaceStatus() error {
	user := os.Getenv("USER")
	if user == "" {
		user = "unknown"
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	cwd, err := os.Getwd()
	if err != nil {
		cwd = "unknown"
	}

	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_WorkspaceStatus{},
		},
		Payload: &bespb.BuildEvent_WorkspaceStatus{
			WorkspaceStatus: &bespb.WorkspaceStatus{
				Item: []*bespb.WorkspaceStatus_Item{
					{Key: "BUILD_USER", Value: user},
					{Key: "BUILD_HOST", Value: hostname},
					{Key: "BUILD_WORKING_DIRECTORY", Value: cwd},
				},
			},
		},
	}
	
	return p.publish(bazelEvent)
}

func (p *Publisher) PublishConfiguration() error {
	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Configuration{
				Configuration: &bespb.BuildEventId_ConfigurationId{
					Id: "host",
				},
			},
		},
		Payload: &bespb.BuildEvent_Configuration{
			Configuration: &bespb.Configuration{
				Mnemonic:     "host",
				PlatformName: runtime.GOOS,
				Cpu:          runtime.GOARCH,
				MakeVariable: map[string]string{
					"TARGET_CPU": runtime.GOARCH,
				},
			},
		},
	}

	return p.publish(bazelEvent)
}

func (p *Publisher) PublishFinished(exitCode int) error {
	exitCodeName := "SUCCESS"
	if exitCode != 0 {
		exitCodeName = "FAILED"
	}

	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_BuildFinished{},
		},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildToolLogs{}},
		},
		LastMessage: true,
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{
				ExitCode: &bespb.BuildFinished_ExitCode{
					Name: exitCodeName,
					Code: int32(exitCode),
				},
				FinishTime: timestamppb.Now(),
			},
		},
	}

	return p.publish(bazelEvent)
}

func (p *Publisher) PublishConsoleOutput(output string, streamType bepb.ConsoleOutputStream) error {
	progress := &bespb.Progress{}
	if streamType == bepb.ConsoleOutputStream_STDOUT {
		progress.Stdout = output
	} else {
		progress.Stderr = output
	}

	bazelEvent := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Progress{
				Progress: &bespb.BuildEventId_ProgressId{
					OpaqueCount: int32(p.sequenceNumber),
				},
			},
		},
		Payload: &bespb.BuildEvent_Progress{
			Progress: progress,
		},
	}
	return p.publish(bazelEvent)
}

func (p *Publisher) Finish() error {
	p.mu.Lock()
	p.finished = true
	p.mu.Unlock()

	if err := p.stream.CloseSend(); err != nil {
		return status.WrapError(err, "failed to close stream")
	}

	if err := <-p.recvDone; err != nil {
		return status.WrapError(err, "failed to receive all acknowledgements")
	}

	return nil
}