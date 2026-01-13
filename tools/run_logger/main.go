package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/creack/pty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

var (
	target       = flag.String("target", "grpcs://remote.buildbuddy.io", "BuildBuddy server target")
	apiKey       = flag.String("api_key", "", "BuildBuddy API key (defaults to $BUILDBUDDY_API_KEY)")
	invocationID = flag.String("invocation_id", "", "Invocation ID (defaults to $BAZEL_INVOCATION_ID or generated)")
	attemptID    = flag.String("attempt_id", "", "Attempt ID (defaults to timestamp)")
	usePTY       = flag.Bool("use_pty", true, "Use PTY to capture ANSI codes")
	bufferSize   = flag.Int("buffer_size", 32*1024, "Buffer size for streaming")
)

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <command> [args...]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Get configuration
	iid := getInvocationID()
	aid := getAttemptID()
	key := getAPIKey()

	// Connect to BuildBuddy
	conn, err := dialBuildBuddy(*target)
	if err != nil {
		log.Errorf("Failed to connect to BuildBuddy: %s", err)
		// Continue anyway - just run the command without logging
		os.Exit(runCommand(flag.Args(), nil))
	}
	defer conn.Close()

	// Create context with API key if provided
	ctx := context.Background()
	if key != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)
	}

	client := bbspb.NewBuildBuddyServiceClient(conn)
	stream, err := client.WriteEventLog(ctx)
	if err != nil {
		log.Errorf("Failed to create stream: %s", err)
		os.Exit(runCommand(flag.Args(), nil))
	}

	// Send initial metadata message
	err = stream.Send(&elpb.WriteEventLogRequest{
		Type: elpb.LogType_RUN_LOG,
		Metadata: &elpb.LogMetadata{
			InvocationId: iid,
			AttemptId:    aid,
		},
	})
	if err != nil {
		log.Errorf("Failed to send metadata: %s", err)
		os.Exit(runCommand(flag.Args(), nil))
	}

	// Run command and stream output
	exitCode := runCommand(flag.Args(), stream)

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Warningf("Failed to close stream: %s", err)
	} else {
		log.Infof("Logs streamed successfully to BuildBuddy")
		log.Infof("View at: %s/invocation/%s?attempt=%s#run-log", *target, iid, aid)
	}
	_ = resp

	os.Exit(exitCode)
}

func getInvocationID() string {
	if *invocationID != "" {
		return *invocationID
	}
	if id := os.Getenv("BAZEL_INVOCATION_ID"); id != "" {
		return id
	}
	// Generate a unique ID
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func getAttemptID() string {
	if *attemptID != "" {
		return *attemptID
	}
	// Use timestamp as attempt ID
	return fmt.Sprintf("%d", time.Now().Unix())
}

func getAPIKey() string {
	if *apiKey != "" {
		return *apiKey
	}
	return os.Getenv("BUILDBUDDY_API_KEY")
}

func dialBuildBuddy(target string) (*grpc_client.ClientConnPool, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(50 * 1024 * 1024), // 50MB
		),
	}

	return grpc_client.DialSimple(target, opts...)
}

func runCommand(args []string, stream bbspb.BuildBuddyService_WriteEventLogClient) int {
	cmd := exec.Command(args[0], args[1:]...)

	if *usePTY {
		return runWithPTY(cmd, stream)
	}
	return runWithPipes(cmd, stream)
}

func runWithPTY(cmd *exec.Cmd, stream bbspb.BuildBuddyService_WriteEventLogClient) int {
	// Start command with PTY to capture ANSI codes
	ptmx, err := pty.Start(cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start command with PTY: %s\n", err)
		return 1
	}
	defer ptmx.Close()

	// Handle terminal size updates
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	go func() {
		for range ch {
			if err := pty.InheritSize(os.Stdin, ptmx); err != nil {
				log.Warningf("Failed to resize PTY: %s", err)
			}
		}
	}()
	ch <- syscall.SIGWINCH // Initial resize

	// Copy stdin to PTY
	go func() {
		io.Copy(ptmx, os.Stdin)
	}()

	// Stream PTY output to both stdout and BuildBuddy
	done := make(chan error)
	go func() {
		done <- streamOutput(ptmx, stream)
	}()

	// Wait for command to finish
	cmdErr := cmd.Wait()

	// Wait for streaming to finish
	<-done

	// Return exit code
	if cmdErr != nil {
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}

func runWithPipes(cmd *exec.Cmd, stream bbspb.BuildBuddyService_WriteEventLogClient) int {
	// Get stdout and stderr pipes
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stdout pipe: %s\n", err)
		return 1
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stderr pipe: %s\n", err)
		return 1
	}

	// Pass through stdin
	cmd.Stdin = os.Stdin

	// Start command
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start command: %s\n", err)
		return 1
	}

	// Stream both stdout and stderr
	done := make(chan error, 2)
	go func() {
		done <- streamOutput(stdout, stream)
	}()
	go func() {
		done <- streamOutput(stderr, stream)
	}()

	// Wait for both streams to finish
	<-done
	<-done

	// Wait for command to finish
	cmdErr := cmd.Wait()
	if cmdErr != nil {
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}

func streamOutput(r io.Reader, stream bbspb.BuildBuddyService_WriteEventLogClient) error {
	buf := make([]byte, *bufferSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			// Write to stdout for local viewing
			os.Stdout.Write(buf[:n])

			// Stream to BuildBuddy if available
			if stream != nil {
				sendErr := stream.Send(&elpb.WriteEventLogRequest{
					Buffer: buf[:n],
				})
				if sendErr != nil {
					log.Warningf("Failed to stream to BuildBuddy: %s", sendErr)
					// Continue anyway - don't fail the build
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
