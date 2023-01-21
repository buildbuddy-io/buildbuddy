package vmexec_client

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

const (
	// Timeout used for the explicit Sync() call in the case where the command
	// is cancelled or times out.
	syncTimeout = 5 * time.Second
)

// Execute executes the command using the ExecStreamed API.
func Execute(ctx context.Context, client vmxpb.ExecClient, cmd *repb.Command, workDir, user string, statsListener procstats.Listener, stdio *container.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	var stderr, stdout bytes.Buffer
	if stdio == nil {
		stdio = &container.Stdio{}
	}
	stdoutw := io.Writer(&stdout)
	if stdio.Stdout != nil {
		stdoutw = stdio.Stdout
	}
	stderrw := io.Writer(&stderr)
	if stdio.Stderr != nil {
		stderrw = stdio.Stderr
	}
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
		return commandutil.ErrorResult(err)
	}
	startMsg := &vmxpb.ExecStreamedRequest{Start: req}
	if err := stream.Send(startMsg); err != nil {
		return commandutil.ErrorResult(err)
	}
	var res *vmxpb.ExecResponse
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
				return nil
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
	if res == nil {
		res = &vmxpb.ExecResponse{ExitCode: commandutil.NoExitCode}
	}
	result := &interfaces.CommandResult{
		ExitCode:   int(res.ExitCode),
		Stderr:     stderr.Bytes(),
		Stdout:     stdout.Bytes(),
		Error:      err,
		UsageStats: stats,
	}
	// The vmexec server normally calls unix.Sync() after the command is
	// terminated, but if the context was cancelled then the Sync() call may not
	// have completed yet. Explicitly sync here so that we have a better chance
	// of collecting output files in this case.
	if err := ctx.Err(); err != nil {
		ctxErr := status.FromContextError(ctx)
		ctx, cancel := background.ExtendContextForFinalization(ctx, syncTimeout)
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
