package stream_run_logs

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	guuid "github.com/google/uuid"
)

const (
	// Even if not enough data has been written to flush a chunk, flush at least once every interval to ensure
	// the UI is relatively up-to-date.
	flushChunkTimeout = 15 * time.Second
)

var (
	// Size of the buffer to use for streaming logs.
	UploadBufferSize = 1 << 20 // 1MB

	enabled = false
)

type Opts struct {
	BesBackend   string
	InvocationID string
	ApiKey       string
}

func Enable(e bool) {
	enabled = e
}

// If streaming run logs is requested with --stream_run_logs, parse required args.
func Configure(args []string) ([]string, *Opts, error) {
	if !enabled || arg.GetCommand(args) != "run" {
		return args, nil, nil
	}

	apiKey := arg.Get(args, "remote_header=x-buildbuddy-api-key")
	if apiKey == "" {
		log.Warnf("To stream run logs, authenticate your request with `bb login` or add an API key to your run with " +
			"`--remote_header=x-buildbuddy-api-key=XXX`")
		return args, nil, status.UnauthenticatedError("unauthenticated request")
	}

	besBackend := arg.Get(args, "bes_backend")
	if besBackend == "" {
		return args, nil, status.FailedPreconditionError("bes_backend is required for streaming run logs")
	}

	// In order to stream run logs to the same invocation URL as the build, we must pre-generate the
	// invocation ID to pass it to `Execute`.
	iid := arg.Get(args, "invocation_id")
	if iid == "" {
		invocationUUID, err := guuid.NewRandom()
		if err != nil {
			return args, nil, status.InternalErrorf("failed to generate invocation ID: %s", err)
		}
		iid = invocationUUID.String()
		args = arg.Append(args, "--invocation_id="+iid)
	}

	return args, &Opts{
		BesBackend:   besBackend,
		InvocationID: iid,
		ApiKey:       apiKey,
	}, nil
}

// Execute executes the run script and streams its output to the server.
// If streaming fails, it falls back to running the script normally.
func Execute(runScriptPath string, opts Opts) (int, error) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", opts.ApiKey)

	if err := os.Chmod(runScriptPath, 0o755); err != nil {
		return 1, status.InternalErrorf("failed to chmod script %s: %s", runScriptPath, err)
	}

	conn, err := grpc_client.DialSimple(opts.BesBackend)
	if err != nil {
		log.Warnf("Failed to dial %s for streaming `run` executable logs: %s", opts.BesBackend, err)
		return runScriptDirectly(runScriptPath)
	}
	defer conn.Close()
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	if _, err := bbClient.UpdateRunStatus(ctx, &elpb.UpdateRunStatusRequest{
		InvocationId: opts.InvocationID,
		Status:       inspb.OverallStatus_IN_PROGRESS,
	}); err != nil {
		log.Warnf("Failed to update run status: %s", err)
		return runScriptDirectly(runScriptPath)
	}

	exitCode, err := runScriptWithStreaming(ctx, bbClient, opts.InvocationID, runScriptPath)

	// TODO(Maggie): Forward signals to the child process and set status=DISCONNECTED if the command is interrupted.
	invStatus := inspb.OverallStatus_SUCCESS
	if exitCode != 0 {
		invStatus = inspb.OverallStatus_FAILURE
	}
	if _, err := bbClient.UpdateRunStatus(ctx, &elpb.UpdateRunStatusRequest{
		InvocationId: opts.InvocationID,
		Status:       invStatus,
	}); err != nil {
		log.Warnf("Failed to update run status: %s", err)
	}

	return exitCode, err
}

// runScriptDirectly runs the script without streaming logs. It's used as a fallback if streaming fails.
func runScriptDirectly(scriptPath string) (int, error) {
	cmd := exec.Command(scriptPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return 1, status.InternalErrorf("failed to run %s: %s", scriptPath, err)
	}
	return 0, nil
}

func runScriptWithStreaming(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID, scriptPath string) (int, error) {
	stream, err := bbClient.WriteEventLog(ctx)
	if err != nil {
		log.Warnf("Failed to create log stream: %s", err)
		return runScriptDirectly(scriptPath)
	}
	defer func() {
		if _, err := stream.CloseAndRecv(); err != nil {
			log.Warnf("Failed to close stream: %s", err)
		}
	}()

	cmd := exec.Command(scriptPath)

	var outputReader io.Reader
	var cleanup func()

	if terminal.IsTTY(os.Stdout) && terminal.IsTTY(os.Stderr) {
		ptmx, err := pty.Start(cmd)
		if err != nil {
			log.Warnf("Failed to start command with PTY: %s", err)
			return runScriptDirectly(scriptPath)
		}
		outputReader = ptmx
		cleanup = func() { ptmx.Close() }
	} else {
		pr, pw := io.Pipe()
		cmd.Stdin = os.Stdin
		cmd.Stdout = pw
		cmd.Stderr = pw
		outputReader = pr
		cleanup = func() { pw.Close() }

		if err := cmd.Start(); err != nil {
			pr.Close()
			pw.Close()
			log.Warnf("Failed to start command: %s", err)
			return runScriptDirectly(scriptPath)
		}
	}
	copyOutputDone := make(chan error)
	go func() {
		copyOutputDone <- streamOutput(ctx, bbClient, invocationID, outputReader, stream)
	}()

	cmdErr := cmd.Wait()
	cleanup() // Close PTY/pipe to signal EOF to streamOutput
	copyErr := <-copyOutputDone
	if copyErr != nil {
		log.Warnf("Failed to stream output: %s", copyErr)
	}

	if cmdErr != nil {
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return 1, status.InternalErrorf("failed to run %s: %s", scriptPath, cmdErr)
	}
	return 0, nil
}

// streamOutput streams output to both stdout and uploads them to the BuildBuddy server.
func streamOutput(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string, outputReader io.Reader, writeStream bbspb.BuildBuddyService_WriteEventLogClient) error {
	uploadRunLogs := true
	flushTimer := time.NewTimer(flushChunkTimeout)
	defer flushTimer.Stop()

	writeBuf := make([]byte, 0, UploadBufferSize)
	readBuf := make([]byte, UploadBufferSize)
	for {
		n, err := outputReader.Read(readBuf)
		// When a PTY is closed, it returns EIO.
		if err == io.EOF || errors.Is(err, syscall.EIO) {
			if len(writeBuf) > 0 {
				_ = uploadLogs(ctx, bbClient, invocationID, writeStream, writeBuf)
			}
			return nil
		}
		if err != nil {
			return err
		}

		os.Stdout.Write(readBuf[:n])

		if !uploadRunLogs {
			continue
		}

		forceFlush := false
		select {
		case <-flushTimer.C:
			forceFlush = true
		default:
		}

		// Flush writes to the server once we've accumulated enough data.
		if len(writeBuf) > 0 && (len(writeBuf)+n > UploadBufferSize || forceFlush) {
			if err := uploadLogs(ctx, bbClient, invocationID, writeStream, writeBuf); err != nil {
				uploadRunLogs = false
				continue
			}

			// Reset the flush timer.
			if !flushTimer.Stop() {
				select {
				case <-flushTimer.C:
				default:
				}
			}
			flushTimer.Reset(flushChunkTimeout)

			// Reset the write buffer.
			writeBuf = writeBuf[:0]
		}
		writeBuf = append(writeBuf, readBuf[:n]...)
	}
}

func uploadLogs(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string, stream bbspb.BuildBuddyService_WriteEventLogClient, data []byte) error {
	// TODO(Maggie): Add retries and a server-side mechanism to ensure idempotency.
	if err := stream.Send(&elpb.WriteEventLogRequest{
		Type: elpb.LogType_RUN_LOG,
		Metadata: &elpb.LogMetadata{
			InvocationId: invocationID,
		},
		Data: data,
	}); err != nil {
		log.Warnf("Failed to stream run logs: %s", err)
		if _, err := bbClient.UpdateRunStatus(ctx, &elpb.UpdateRunStatusRequest{
			InvocationId: invocationID,
			Status:       inspb.OverallStatus_DISCONNECTED,
		}); err != nil {
			log.Warnf("Failed to update run status: %s", err)
		}
		return err
	}
	return nil
}
