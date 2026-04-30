package vmexec_client

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	gstatus "google.golang.org/grpc/status"
)

const (
	// Timeout used for the explicit Sync() call in the case where the command
	// is cancelled or times out.
	syncTimeout = 5 * time.Second

	// Timeout applied to each exec stream recv, after which we assume that
	// something has gone wrong in the VM and cancel execution.
	streamRecvTimeout = 1 * time.Minute
)

var (
	errRecvTimeout = status.UnavailableErrorf("stream recv timed out after %s", streamRecvTimeout)
)

// Execute executes the command using the ExecStreamed API.
func Execute(ctx context.Context, client vmxpb.ExecClient, cmd *repb.Command, workDir, user string, statsListener procstats.Listener, stdio *interfaces.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	ctx, cancelExec := context.WithCancelCause(ctx)
	defer cancelExec(nil)

	if stdio == nil {
		stdio = &interfaces.Stdio{}
	}
	var stdoutBuf, stderrBuf bytes.Buffer
	makeWriter := func(primary io.Writer, buf *bytes.Buffer, collect bool, extras ...io.Writer) io.Writer {
		writers := make([]io.Writer, 0, 2+len(extras))
		if collect {
			writers = append(writers, buf)
		}
		if primary != nil {
			writers = append(writers, primary)
		}
		writers = append(writers, extras...)
		switch len(writers) {
		case 0:
			if stdio.DisableOutputLimits {
				return io.Discard
			}
			return commandutil.LimitStdOutErrWriter(io.Discard)
		case 1:
			if stdio.DisableOutputLimits {
				return writers[0]
			}
			return commandutil.LimitStdOutErrWriter(writers[0])
		default:
			sink := io.MultiWriter(writers...)
			if stdio.DisableOutputLimits {
				return sink
			}
			return commandutil.LimitStdOutErrWriter(sink)
		}
	}
	extraStdout := []io.Writer{}
	extraStderr := []io.Writer{}
	if *commandutil.DebugStreamCommandOutputs {
		extraStdout = append(extraStdout, os.Stdout)
		extraStderr = append(extraStderr, os.Stderr)
	}
	stdoutw := makeWriter(stdio.Stdout, &stdoutBuf, stdio.Stdout == nil, extraStdout...)
	stderrw := makeWriter(stdio.Stderr, &stderrBuf, stdio.Stderr == nil, extraStderr...)
	req := &vmxpb.ExecRequest{
		WorkingDirectory: workDir,
		User:             user,
		Arguments:        cmd.GetArguments(),
		OpenStdin:        stdio.Stdin != nil,
	}
	for _, ev := range cmd.GetEnvironmentVariables() {
		req.EnvironmentVariables = append(req.EnvironmentVariables, &vmxpb.ExecRequest_EnvironmentVariable{
			Name: ev.GetName(), Value: ev.GetValue(),
		})
	}

	stream, err := client.ExecStreamed(ctx)
	if err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("create execution stream: %s", err))
	}
	startMsg := &vmxpb.ExecStreamedRequest{Start: req}
	if err := stream.Send(startMsg); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("send execution start request: %s", err))
	}
	var res *vmxpb.ExecResponse
	var stats *repb.UsageStats
	eg, egCtx := errgroup.WithContext(ctx)
	if stdio.Stdin != nil {
		eg.Go(func() error {
			if _, err := io.Copy(&stdinWriter{stream}, stdio.Stdin); err != nil {
				return status.UnavailableErrorf("failed to write stdin: %s", err)
			}
			if err := stream.CloseSend(); err != nil {
				return status.UnavailableErrorf("failed to close send direction of stream: %s", err)
			}
			return nil
		})
	} else {
		if err := stream.CloseSend(); err != nil {
			return commandutil.ErrorResult(status.UnavailableErrorf("failed to close send direction of stream: %s", err))
		}
	}

	eg.Go(func() error {
		receiver := rpcutil.NewReceiver[*vmxpb.ExecStreamedResponse](egCtx, stream)
		for {
			msg, err := receiver.RecvWithTimeoutCause(streamRecvTimeout, errRecvTimeout)
			if err == io.EOF {
				if res == nil {
					return status.UnavailableErrorf("unexpected EOF before receiving command result: %s", err)
				}
				// Trust the error code returned by the vmexec server, but
				// temporarily log the error here for debugging purposes.
				err := gstatus.ErrorProto(res.GetStatus())
				if err != nil {
					log.CtxInfof(ctx, "vmexec stream returned error: %s", err)
				}
				return err
			}
			if err != nil {
				if egCtx.Err() == context.DeadlineExceeded {
					return status.DeadlineExceededError("context deadline exceeded")
				}
				if egCtx.Err() == context.Canceled {
					return status.CanceledError("context canceled")
				}
				return status.UnavailableErrorf("failed to receive from stream: %s", status.Message(err))
			}
			if _, err := stdoutw.Write(msg.Stdout); err != nil {
				if status.IsResourceExhaustedError(err) {
					err = status.WrapError(err, "failed to write stdout")
					cancelExec(err)
					return err
				}
				return status.UnavailableErrorf("failed to write stdout: %s", status.Message(err))
			}
			if _, err := stderrw.Write(msg.Stderr); err != nil {
				if status.IsResourceExhaustedError(err) {
					err = status.WrapError(err, "failed to write stderr")
					cancelExec(err)
					return err
				}
				return status.UnavailableErrorf("failed to write stderr: %s", status.Message(err))
			}
			if msg.Response != nil {
				res = msg.Response
			}
			if msg.UsageStats != nil {
				stats = msg.UsageStats
				if statsListener != nil {
					statsListener(stats)
				}
			}
		}
	})

	err = eg.Wait()
	if cause := context.Cause(ctx); status.IsResourceExhaustedError(cause) {
		err = cause
	}
	exitCode := commandutil.NoExitCode
	if res != nil {
		exitCode = int(res.GetExitCode())
	}
	stdoutBytes := stdoutBuf.Bytes()
	stderrBytes := stderrBuf.Bytes()
	if stdio.Stdout != nil {
		stdoutBytes = nil
	}
	if stdio.Stderr != nil {
		stderrBytes = nil
	}
	result := &interfaces.CommandResult{
		ExitCode:   exitCode,
		Stderr:     stderrBytes,
		Stdout:     stdoutBytes,
		Error:      err,
		UsageStats: stats,
	}
	// The vmexec server normally calls unix.Sync() after the command is
	// terminated, but if the context was cancelled then the Sync() call may not
	// have completed yet. Explicitly sync here so that we have a better chance
	// of collecting output files in this case.
	if err := egCtx.Err(); err != nil {
		ctxErr := status.FromContextError(egCtx)
		if cause := context.Cause(ctx); status.IsResourceExhaustedError(cause) {
			ctxErr = cause
		}
		ctx, cancel := background.ExtendContextForFinalization(egCtx, syncTimeout)
		defer cancel()
		_, err := client.Sync(ctx, &vmxpb.SyncRequest{})
		if err != nil {
			result.Error = status.WrapErrorf(
				ctxErr,
				"failed to sync filesystem following interrupted command; some output files may be missing from the workspace: %s. command interrupted due to", err)
		}
	}
	return result
}

type stdinWriter struct {
	stream vmxpb.Exec_ExecStreamedClient
}

func (w *stdinWriter) Write(b []byte) (int, error) {
	msg := &vmxpb.ExecStreamedRequest{Stdin: b}
	if err := w.stream.Send(msg); err != nil {
		return 0, err
	}
	return len(b), nil
}
