// sandboxd is a small binary that runs as a "pseudo-root" user in order to get
// mount() permissions. It exposes a gRPC service that sets up mounts for
// sandboxed processes, executes sandboxed commands, then takes care of
// unmounting afterwards. Unmounting adds extra latency, so it is done after
// returning the response to the user.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	sbdpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/sandboxd"
	gstatus "google.golang.org/grpc/status"
)

var (
	socketFlag = flag.String("socket", "", "Path to the Unix socket where the server will listen")
)

func main() {
	flag.Parse()
	if *socketFlag == "" {
		log.Fatalf("sandboxd: missing -socket flag")
	}

	lis, err := net.Listen("unix", *socketFlag)
	if err != nil {
		log.Fatalf("sandboxd: failed to listen on %s: %s", *socketFlag, err)
	}
	log.Printf("sandboxd: listening on unix://%s", *socketFlag)

	// When the parent executor exits, we should exit too (since each executor
	// should have ownership of a single sandboxd process)
	go func() {
		_, _ = os.Stdin.Read([]byte{0})
		log.Infof("sandboxd: parent exited")
		if err := os.RemoveAll(*socketFlag); err != nil {
			log.Warningf("sandboxd: failed to remove socket: %s", err)
		}
		os.Exit(1)
	}()

	server := grpc.NewServer()
	sbdpb.RegisterSandboxServer(server, &sandboxd{})
	if err := server.Serve(lis); err != nil {
		log.Fatalf("sandboxd: failed to serve: %s", err)
	}
}

type sandboxd struct{}

var _ sbdpb.SandboxServer = (*sandboxd)(nil)

type message struct {
	Response *sbdpb.ExecResponse
	Err      error
}

func (s *sandboxd) Exec(stream sbdpb.Sandbox_ExecServer) error {
	ctx := stream.Context()

	msgs := make(chan *message, 128)

	go func() {
		var cmd *command
		var cmdFinished chan struct{}
		defer func() {
			if cmdFinished != nil {
				// If a command is running, then it may still be streaming
				// messages. Wait for all messages to be streamed before we
				// close the channel.
				<-cmdFinished
			}
			close(msgs)
		}()
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				// If the client hasn't yet started a command, they shouldn't have
				// closed the stream yet. Report an error.
				if cmd == nil {
					msgs <- &message{Err: status.InvalidArgumentError("stream was closed without receiving exec start request")}
					return
				}
				// Otherwise if a command was started and we have an open stdin
				// pipe, the client closing their end of the stream means we
				// should close the stdin pipe.
				if cmd.stdin != nil {
					if err := cmd.stdin.Close(); err != nil {
						msgs <- &message{Err: status.InternalErrorf("failed to close stdin pipe: %s", err)}
						return
					}
				}
				// Done receiving messages; return.
				return
			}
			if err != nil {
				msgs <- &message{Err: err}
				return
			}
			if msg.Start != nil {
				if cmd != nil {
					msgs <- &message{Err: status.InvalidArgumentError("received multiple exec start requests")}
					return
				}

				// Setup mounts
				mounts := msg.Start.GetMounts()
				for _, m := range mounts {
					if err := setupMount(m); err != nil {
						msgs <- &message{Err: status.WrapError(err, "setup mount")}
						return
					}
				}

				cmd, err = newCommand(msg.Start)
				if err != nil {
					msgs <- &message{Err: err}
					return
				}
				cmdFinished = make(chan struct{})
				go func() {
					defer close(cmdFinished)
					res, err := cmd.Run(ctx, msgs)
					if err != nil {
						msgs <- &message{Err: err}
						return
					}
					msgs <- &message{Response: res}
				}()
			}
			if len(msg.Stdin) > 0 {
				if cmd == nil {
					msgs <- &message{Err: status.InvalidArgumentError("received stdin before exec start request")}
					return
				}
				if cmd.stdin == nil {
					msgs <- &message{Err: status.InvalidArgumentError("received stdin without specifying open_stdin in exec start request")}
					return
				}
				if _, err := cmd.stdin.Write(msg.Stdin); err != nil {
					msgs <- &message{Err: status.InternalErrorf("failed to write stdin: %s", err)}
					return
				}
			}
		}
	}()
	for msg := range msgs {
		if msg.Err != nil {
			return msg.Err
		}
		if err := stream.Send(msg.Response); err != nil {
			return status.InternalErrorf("failed to send response: %s", err)
		}
	}
	return nil
}

func (s *sandboxd) Clean(ctx context.Context, req *sbdpb.CleanRequest) (*sbdpb.CleanResponse, error) {
	var lastErr error
	for _, path := range req.GetUnmountPaths() {
		if err := syscall.Unmount(path, 0); err != nil {
			lastErr = status.WrapErrorf(err, "unmount %s", path)
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return &sbdpb.CleanResponse{}, nil
}

func setupMount(m *sbdpb.Mount) error {
	// log.Infof("sandboxd: mounting %+v", m)
	if err := syscall.Mount(m.Source, m.Target, m.Filesystemtype, uintptr(m.Flags), m.Data); err != nil {
		return status.WrapErrorf(err, "mount source=%s,target=%s,data=%s", m.Source, m.Target, m.Data)
	}
	return nil
}

type command struct {
	cmd *exec.Cmd

	stdin        io.WriteCloser
	stdoutWriter *io.PipeWriter
	stdoutReader *io.PipeReader
	stderrWriter *io.PipeWriter
	stderrReader *io.PipeReader
}

func newCommand(start *sbdpb.Command) (*command, error) {
	if len(start.GetArguments()) == 0 {
		return nil, status.InvalidArgumentError("arguments not specified")
	}
	cmd := exec.Command(start.GetArguments()[0], start.GetArguments()[1:]...)
	for _, envVar := range start.GetEnvironmentVariables() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if start.GetUser() != "" {
		err := commandutil.SetCredential(cmd, start.GetUser())
		if err != nil {
			return nil, err
		}
	}

	stdoutReader, stdoutWriter := io.Pipe()
	cmd.Stdout = stdoutWriter
	stderrReader, stderrWriter := io.Pipe()
	cmd.Stderr = stderrWriter
	var stdin io.WriteCloser
	if start.GetOpenStdin() {
		stdinPipe, err := cmd.StdinPipe()
		if err != nil {
			return nil, status.InternalErrorf("failed to open stdin: %s", err)
		}
		stdin = stdinPipe
	}
	return &command{
		cmd:          cmd,
		stdin:        stdin,
		stdoutReader: stdoutReader,
		stdoutWriter: stdoutWriter,
		stderrReader: stderrReader,
		stderrWriter: stderrWriter,
	}, nil
}

func (c *command) Run(ctx context.Context, msgs chan *message) (*sbdpb.ExecResponse, error) {
	log.Infof("sandboxd: running command %q", c.cmd.String())
	stdoutErrCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(&stdoutWriter{msgs}, c.stdoutReader)
		stdoutErrCh <- err
	}()
	stderrErrCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(&stderrWriter{msgs}, c.stderrReader)
		stderrErrCh <- err
	}()

	// TODO: stats?
	_, err := commandutil.RunWithProcessTreeCleanup(ctx, c.cmd, nil /*=statsListener*/)

	exitCode, err := commandutil.ExitCode(ctx, c.cmd, err)
	s := &sbdpb.CommandStatus{
		ExitCode: int32(exitCode),
		Status:   gstatus.Convert(err).Proto(),
	}
	c.stdoutWriter.Close()
	if err := <-stdoutErrCh; err != nil {
		return nil, status.InternalErrorf("failed to copy stdout: %s", err)
	}
	c.stderrWriter.Close()
	if err := <-stderrErrCh; err != nil {
		return nil, status.InternalErrorf("failed to copy stderr: %s", err)
	}
	return &sbdpb.ExecResponse{Status: s}, nil
}

// TODO: use byte buffer pool for these

type stdoutWriter struct{ msgs chan *message }

func (w *stdoutWriter) Write(b []byte) (int, error) {
	c := make([]byte, len(b))
	copy(c, b)
	w.msgs <- &message{Response: &sbdpb.ExecResponse{Stdout: c}}
	return len(b), nil
}

type stderrWriter struct{ msgs chan *message }

func (w *stderrWriter) Write(b []byte) (int, error) {
	c := make([]byte, len(b))
	copy(c, b)
	w.msgs <- &message{Response: &sbdpb.ExecResponse{Stderr: c}}
	return len(b), nil
}
