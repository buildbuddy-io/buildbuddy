package sandboxd_client

import (
	"bytes"
	"context"
	"flag"
	"io"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/nsutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	sbdpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/sandboxd"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

var (
	debugStreamSandboxdLogs = flag.Bool("debug_stream_sandboxd_logs", false, "Whether to stream the debug logs from sandboxd.")
)

// Sandboxd is a handle on a sanbdoxd process.
type Sandboxd struct {
	cmd    *exec.Cmd
	stdin  io.Closer
	conn   *grpc_client.ClientConnPool
	client sbdpb.SandboxClient

	closed chan struct{}
	exited chan struct{}
	err    error
}

func StartSandboxd(executablePath, socket string) (_ *Sandboxd, err error) {
	s := &Sandboxd{
		closed: make(chan struct{}),
		exited: make(chan struct{}),
	}
	start := func() error {
		s.cmd = exec.Command(executablePath, "-socket", socket)
		// Prevent sandboxd from becoming orphaned if the executor process
		// terminates: have it read from a pipe that we create, but never write
		// anything to. The pipe will return an error only if we get terminated.
		stdin, err := s.cmd.StdinPipe()
		if err != nil {
			return status.WrapError(err, "get stdin pipe")
		}
		s.stdin = stdin
		// TODO: hide these behind a flag
		if *debugStreamSandboxdLogs {
			s.cmd.Stdout = os.Stderr
			s.cmd.Stderr = os.Stderr
		}
		s.cmd.SysProcAttr = &syscall.SysProcAttr{
			// Don't receive signals that are sent to the executor, like
			// SIGINT / SIGTERM.
			Setpgid: true,
		}
		// Run sandboxd in "pseudo-root" mode, unsharing its mount namespace and
		// user namespace, and mapping the current user to uid 0. Note: this is
		// the whole reason why sandboxd exists - it can run as uid 0 in its own
		// user namespace while the executor can just run happily as the current
		// user.
		nsutil.SetOptions(s.cmd.SysProcAttr, nsutil.MapID(0, 0), nsutil.UnshareMount)
		if err := s.cmd.Start(); err != nil {
			return status.WrapError(err, "start sandboxd")
		}
		return nil
	}
	if err := commandutil.RetryIfTextFileBusy(start); err != nil {
		return nil, err
	}
	go func() {
		err := s.cmd.Wait()
		select {
		case <-s.closed:
		default:
			// TODO: restart automatically
			log.Errorf("sandboxd exited unexpectedly: %s", err)
		}
		s.err = err
		close(s.exited)
	}()
	defer func() {
		if err != nil {
			// Kill sandboxd if we failed to connect.
			s.stdin.Close()
		}
	}()

	// Wait for the socket to be created.
	const timeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	r := retry.DefaultWithContext(ctx)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if !r.Next() {
			return nil, status.InternalError("timed out waiting for sandboxd socket to be created")
		}
	}
	// Connect to sandboxd (blocking)
	conn, err := grpc_client.DialSimple(
		"passthrough:///unix/"+socket,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			unixAddr, err := net.ResolveUnixAddr("unix", socket)
			if err != nil {
				return nil, status.WrapError(err, "resolve unix addr")
			}
			dialer := net.Dialer{}
			conn, err := dialer.DialContext(ctx, "unix", unixAddr.String())
			if err != nil {
				select {
				case <-s.exited:
					// If sandboxd exited then don't keep retrying.
					return nil, &permanentError{Err: s.err}
				default:
					return nil, err
				}
			}
			return conn, nil
		}),
	)
	if err != nil {
		return nil, status.WrapError(err, "dial sandboxd")
	}
	s.conn = conn
	s.client = sbdpb.NewSandboxClient(conn)
	return s, nil
}

func (s *Sandboxd) GetClient() sbdpb.SandboxClient {
	return s.client
}

func (s *Sandboxd) Close() error {
	close(s.closed)
	_ = s.conn.Close()
	_ = s.stdin.Close()
	<-s.exited
	return nil
}

// Execute executes the command using the Exec API.
func (s *Sandboxd) Exec(ctx context.Context, cmd *repb.Command, mounts []*sbdpb.Mount, user string, statsListener procstats.Listener, stdio *interfaces.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	var stderr, stdout bytes.Buffer
	if stdio == nil {
		stdio = &interfaces.Stdio{}
	}
	stdoutw := io.Writer(&stdout)
	if stdio.Stdout != nil {
		stdoutw = stdio.Stdout
	}
	stderrw := io.Writer(&stderr)
	if stdio.Stderr != nil {
		stderrw = stdio.Stderr
	}
	if *commandutil.DebugStreamCommandOutputs {
		stdoutw = io.MultiWriter(os.Stdout, stdoutw)
		stderrw = io.MultiWriter(os.Stderr, stderrw)
	}
	start := &sbdpb.Command{
		User:      user,
		Arguments: cmd.GetArguments(),
		Mounts:    mounts,
		OpenStdin: stdio.Stdin != nil,
	}
	for _, ev := range cmd.GetEnvironmentVariables() {
		start.EnvironmentVariables = append(start.EnvironmentVariables, &sbdpb.Command_EnvironmentVariable{
			Name:  ev.GetName(),
			Value: ev.GetValue(),
		})
	}

	stream, err := s.client.Exec(ctx)
	if err != nil {
		return commandutil.ErrorResult(err)
	}
	startMsg := &sbdpb.ExecRequest{Start: start}
	if err := stream.Send(startMsg); err != nil {
		return commandutil.ErrorResult(err)
	}
	var res *sbdpb.CommandStatus
	var stats *repb.UsageStats
	eg, ctx := errgroup.WithContext(ctx)
	if stdio.Stdin != nil {
		eg.Go(func() error {
			if _, err := io.Copy(&stdinWriter{stream}, stdio.Stdin); err != nil {
				return status.InternalErrorf("failed to write stdin: %s", err)
			}
			if err := stream.CloseSend(); err != nil {
				return status.InternalErrorf("failed to close send direction of stream: %s", err)
			}
			return nil
		})
	} else {
		if err := stream.CloseSend(); err != nil {
			return commandutil.ErrorResult(status.InternalErrorf("failed to close send direction of stream: %s", err))
		}
	}

	eg.Go(func() error {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				if res == nil {
					return status.InternalErrorf("unexpected EOF before receiving command result: %s", err)
				}
				return gstatus.ErrorProto(res.GetStatus())
			}
			if err != nil {
				if ctx.Err() == context.DeadlineExceeded {
					return status.DeadlineExceededError("context deadline exceeded")
				}
				if ctx.Err() == context.Canceled {
					return status.CanceledError("context canceled")
				}
				return status.InternalErrorf("failed to receive from stream: %s", status.Message(err))
			}
			if _, err := stdoutw.Write(msg.Stdout); err != nil {
				return status.InternalErrorf("failed to write stdout: %s", status.Message(err))
			}
			if _, err := stderrw.Write(msg.Stderr); err != nil {
				return status.InternalErrorf("failed to write stderr: %s", status.Message(err))
			}
			if msg.Status != nil {
				res = msg.Status
			}
			// TODO: stats
			// if msg.UsageStats != nil {
			// 	stats = msg.UsageStats
			// 	if statsListener != nil {
			// 		statsListener(stats)
			// 	}
			// }
		}
	})

	err = eg.Wait()
	exitCode := commandutil.NoExitCode
	if res != nil {
		exitCode = int(res.GetExitCode())
	}
	result := &interfaces.CommandResult{
		ExitCode:   exitCode,
		Stderr:     stderr.Bytes(),
		Stdout:     stdout.Bytes(),
		Error:      err,
		UsageStats: stats,
	}
	return result
}

type stdinWriter struct {
	stream sbdpb.Sandbox_ExecClient
}

func (w *stdinWriter) Write(b []byte) (int, error) {
	msg := &sbdpb.ExecRequest{Stdin: b}
	if err := w.stream.Send(msg); err != nil {
		return 0, err
	}
	return len(b), nil
}

type permanentError struct{ Err error }

func (e *permanentError) Error() string {
	return e.Err.Error()
}

func (e *permanentError) Temporary() bool {
	return false
}

func (e *permanentError) Unwrap() error {
	return e.Err
}
