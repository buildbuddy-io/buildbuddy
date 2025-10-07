package record

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
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

	apiKey := ""
	if key, err := login.GetAPIKey(); err == nil {
		apiKey = strings.TrimSpace(key)
	}

	log.Debugf("Starting recording session with invocation ID: %s", iid)

	invocationURL := fmt.Sprintf("%s/invocation/%s", *resultsURL, iid)
	streamingLog := fmt.Sprintf("\033[32mINFO:\033[0m Streaming results to: \033[4;34m%s\033[0m\n", invocationURL)

	fmt.Fprintf(os.Stderr, "%s\n", streamingLog)

	publisher, err := build_event_publisher.New(*besBackend, apiKey, iid)
	if err != nil {
		return 1, status.WrapError(err, "failed to create publisher")
	}
	publisher.Start(ctx)

	if err := publisher.Publish(startedEvent(cmdArgs, iid, time.Now())); err != nil {
		log.Warnf("Failed to publish started event: %s", err)
	}

	if err := publisher.Publish(structuredCommandLineEvent(cmdArgs)); err != nil {
		log.Warnf("Failed to publish structured command line: %s", err)
	}

	if err := publisher.Publish(buildMetadataEvent()); err != nil {
		log.Warnf("Failed to publish build metadata: %s", err)
	}

	if err := publisher.Publish(workspaceStatusEvent()); err != nil {
		log.Warnf("Failed to publish workspace status: %s", err)
	}

	if err := publisher.Publish(configurationEvent()); err != nil {
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

	if err := publisher.Publish(finishedEvent(exitCode)); err != nil {
		log.Warnf("Failed to publish finished event: %s", err)
	}

	if err := publisher.Finish(); err != nil {
		log.Warnf("Failed to finish publishing events: %s", err)
	}

	fmt.Fprintf(os.Stderr, "\n%s", streamingLog)

	return exitCode, nil
}

func streamOutput(src io.Reader, dst io.Writer, publisher *build_event_publisher.Publisher, streamType bepb.ConsoleOutputStream) {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(nil, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(dst, line)
		if err := publisher.Publish(consoleOutputEvent(line+"\n", streamType)); err != nil {
			log.Debugf("Failed to publish console output: %s", err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Debugf("Error reading output: %s", err)
	}
}

func startedEvent(cmdArgs []string, invocationID string, startTime time.Time) *bespb.BuildEvent {
	return &bespb.BuildEvent{
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
				Uuid:               invocationID,
				StartTime:          timestamppb.New(startTime),
				Command:            strings.Join(cmdArgs[:min(2, len(cmdArgs))], " "),
				OptionsDescription: strings.Join(cmdArgs, " "),
			},
		},
	}
}

func structuredCommandLineEvent(cmdArgs []string) *bespb.BuildEvent {
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

	return &bespb.BuildEvent{
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
}

func buildMetadataEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
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
}

func workspaceStatusEvent() *bespb.BuildEvent {
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

	return &bespb.BuildEvent{
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

}

func configurationEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
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

}

func finishedEvent(exitCode int) *bespb.BuildEvent {
	exitCodeName := "SUCCESS"
	if exitCode != 0 {
		exitCodeName = "FAILED"
	}

	return &bespb.BuildEvent{
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

}

func consoleOutputEvent(output string, streamType bepb.ConsoleOutputStream) *bespb.BuildEvent {
	progress := &bespb.Progress{}
	if streamType == bepb.ConsoleOutputStream_STDOUT {
		progress.Stdout = output
	} else {
		progress.Stderr = output
	}

	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Progress{
				Progress: &bespb.BuildEventId_ProgressId{
					OpaqueCount: int32(time.Now().UnixNano()),
				},
			},
		},
		Payload: &bespb.BuildEvent_Progress{
			Progress: progress,
		},
	}
}
