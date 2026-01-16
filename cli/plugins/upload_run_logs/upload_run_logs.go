package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/exec"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

var (
	target       = flag.String("target", "grpcs://remote.buildbuddy.io", "BuildBuddy server to stream logs to.")
	apiKey       = flag.String("api_key", "", "BuildBuddy API key.")
	invocationID = flag.String("invocation_id", "", "Invocation ID for the associated build. Logs will be sent to this invocation's URL.")
)

const (
	bufferSize = 1 << 20 // 1MB
)

func main() {
	log.Printf("args: %v", os.Args)
	flag.Parse()

	log.Printf("target: %s", *target)

	apiKey := *apiKey
	if apiKey == "" {
		var err error
		apiKey, err = login.GetAPIKey()
		if err != nil || apiKey == "" {
			log.Warnf("Authenticate your request with `bb login` or add an API key to your run with `--remote_header=x-buildbuddy-api-key=XXX`")
			handleErr(status.WrapError(err, "failed to read API key from .git/config"))
		}
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		handleErr(status.WrapError(err, "failed to dial BuildBuddy"))
	}
	defer conn.Close()
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	// Challenge: This script
	if _, err := bbClient.UpdateRunStatus(ctx, &elpb.UpdateRunStatusRequest{
		InvocationId: *invocationID,
		Status:       inspb.OverallStatus_IN_PROGRESS,
	}); err != nil {
		handleErr(status.WrapError(err, "failed to update run status"))
	}

	exitCode := runCommand(ctx, bbClient, flag.Args())

	invStatus := inspb.OverallStatus_SUCCESS
	if exitCode != 0 {
		invStatus = inspb.OverallStatus_FAILURE
	}
	if _, err := bbClient.UpdateRunStatus(ctx, &elpb.UpdateRunStatusRequest{
		InvocationId: *invocationID,
		Status:       invStatus,
	}); err != nil {
		log.Warnf("Failed to update run status: %s", err)
	}

	os.Exit(exitCode)
}

func runCommand(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, args []string) int {
	stream, err := bbClient.WriteEventLog(ctx)
	if err != nil {
		handleErr(status.WrapError(err, "failed to create stream"))
	}

	cmd := exec.Command(args[0], args[1:]...)
	ptmx, err := pty.Start(cmd)
	if err != nil {
		handleErr(status.WrapError(err, "failed to start command with PTY"))
	}
	defer ptmx.Close()

	copyOutputDone := make(chan error)
	go func() {
		copyOutputDone <- streamOutput(ctx, bbClient, ptmx, stream)
	}()

	cmdErr := cmd.Wait()
	<-copyOutputDone

	_, err = stream.CloseAndRecv()
	if err != nil {
		log.Warnf("Failed to close stream: %s", err)
	}

	if cmdErr != nil {
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}

// streamOutput streams output to both stdout and uploads them to the BuildBuddy server.
func streamOutput(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, r io.Reader, stream bbspb.BuildBuddyService_WriteEventLogClient) error {
	uploadRunLogs := true
	buf := make([]byte, bufferSize)
	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		os.Stdout.Write(buf[:n])

		// TODO(Maggie): Add retries and a server-side mechanism to ensure idempotency.
		if uploadRunLogs {
			if err := stream.Send(&elpb.WriteEventLogRequest{
				Type: elpb.LogType_RUN_LOG,
				Metadata: &elpb.LogMetadata{
					InvocationId: *invocationID,
				},
				Data: buf[:n],
			}); err != nil {
				log.Warnf("Failed to stream run logs: %s", err)
				if _, err := bbClient.UpdateRunStatus(ctx, &elpb.UpdateRunStatusRequest{
					InvocationId: *invocationID,
					Status:       inspb.OverallStatus_DISCONNECTED,
				}); err != nil {
					log.Warnf("Failed to update run status: %s", err)
				}
				uploadRunLogs = false
			}
		}
	}
}

// If uploading run logs fails, run the original command normally.
func handleErr(err error) {
	log.Warnf("Failed to stream run logs: %s", err)

	args := flag.Args()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		log.Fatalf("Failed to run command: %v", err)
	}
	os.Exit(0)
}
