package commandutil

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ps "github.com/mitchellh/go-ps"
)

const (
	// KilledExitCode is a special exit code value used by the "os/exec" package
	// when a process is killed.
	KilledExitCode = -1
	// NoExitCode indicates a missing exit code value, usually because the process
	// never started, or its actual exit code could not be determined because of an
	// error.
	NoExitCode = -2
)

var (
	// ErrSIGKILL is a special error used to indicate that a command was terminated
	// by SIGKILL and may be retried.
	ErrSIGKILL = status.UnavailableErrorf("command was terminated by SIGKILL, likely due to executor shutdown or OOM")

	DebugStreamCommandOutputs = flag.Bool("debug_stream_command_outputs", false, "If true, stream command outputs to the terminal. Intended for debugging purposes only and should not be used in production.")
)

var (
	// Regexp matching a string consisting solely of digits (0-9).
	allDigits = regexp.MustCompile(`^\d+$`)
)

// Stdio specifies standard input / output readers for a command.
type Stdio struct {
	// Stdin is an optional stdin source for the executed process.
	Stdin io.Reader
	// Stdout is an optional stdout sink for the executed process.
	Stdout io.Writer
	// Stderr is an optional stderr sink for the executed process.
	Stderr io.Writer
}

func constructExecCommand(command *repb.Command, workDir string, stdio *Stdio) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, error) {
	if stdio == nil {
		stdio = &Stdio{}
	}
	executable, args := splitExecutableArgs(command.GetArguments())
	// Note: we don't use CommandContext here because the default behavior of
	// CommandContext is to kill just the top-level process when the context is
	// canceled. Instead, we would rather kill the entire process group to ensure
	// that child processes are killed too.
	cmd := exec.Command(executable, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	if stdio.Stdout != nil {
		cmd.Stdout = stdio.Stdout
	}
	cmd.Stderr = &stderr
	if stdio.Stderr != nil {
		cmd.Stderr = stdio.Stderr
	}
	// Note: We are using StdinPipe() instead of cmd.Stdin here, because the
	// latter approach results in a bug where cmd.Wait() can hang indefinitely if
	// the process doesn't consume its stdin. See
	// https://go.dev/play/p/DpKaVrx8d8G
	if stdio.Stdin != nil {
		inp, err := cmd.StdinPipe()
		if err != nil {
			return nil, nil, nil, status.InternalErrorf("failed to get stdin pipe: %s", err)
		}
		go func() {
			defer inp.Close()
			io.Copy(inp, stdio.Stdin)
		}()
	}
	if *DebugStreamCommandOutputs {
		logWriter := log.Writer(fmt.Sprintf("[%s] ", executable))
		cmd.Stdout = io.MultiWriter(cmd.Stdout, logWriter)
		cmd.Stderr = io.MultiWriter(cmd.Stderr, logWriter)
	}
	cmd.SysProcAttr = getDefaultSysProcAttr()
	for _, envVar := range command.GetEnvironmentVariables() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	return cmd, &stdout, &stderr, nil
}

// RetryIfTextFileBusy runs a function, retrying "text file busy" errors up to
// 3 times. This is a workaround for https://github.com/golang/go/issues/22315
func RetryIfTextFileBusy(fn func() error) error {
	nBusy := 0
	for {
		err := fn()
		if err != nil && nBusy < 3 && strings.Contains(err.Error(), "text file busy") {
			nBusy++
			time.Sleep(100 * time.Millisecond << uint(nBusy))
			continue
		}
		return err
	}
}

// Run a command, retrying "text file busy" errors and killing the process tree
// when the context is cancelled.
//
// If statsListener is non-nil, stats will be enabled and the callback will be
// invoked each time stats are measured. In addition, the last recorded stats
// will be returned in CommandResult.Stats. Note that enabling stats incurs some
// overhead, so a nil callback should be used if stats aren't needed.
func Run(ctx context.Context, command *repb.Command, workDir string, statsListener procstats.Listener, stdio *Stdio) *interfaces.CommandResult {
	var cmd *exec.Cmd
	var stdoutBuf, stderrBuf *bytes.Buffer
	var stats *repb.UsageStats

	err := RetryIfTextFileBusy(func() error {
		// Create a new command on each attempt since commands can only be run once.
		var err error
		cmd, stdoutBuf, stderrBuf, err = constructExecCommand(command, workDir, stdio)
		if err != nil {
			return err
		}
		stats, err = RunWithProcessTreeCleanup(ctx, cmd, statsListener)
		return err
	})

	exitCode, err := ExitCode(ctx, cmd, err)
	return &interfaces.CommandResult{
		ExitCode:           exitCode,
		Error:              err,
		Stdout:             stdoutBuf.Bytes(),
		Stderr:             stderrBuf.Bytes(),
		CommandDebugString: cmd.String(),
		UsageStats:         stats,
	}
}

func startNewProcess(ctx context.Context, cmd *exec.Cmd) (*process, error) {
	p := &process{
		cmd:        cmd,
		terminated: make(chan struct{}),
	}
	if err := p.preStart(); err != nil {
		return nil, fmt.Errorf("fail to setup preStart: %w", err)
	}
	if err := p.cmd.Start(); err != nil {
		return nil, err
	}
	if err := p.postStart(); err != nil {
		return nil, fmt.Errorf("fail to setup postStart: %w", err)
	}

	// Cleanup goroutine: kill the process tree when the context is canceled.
	go func() {
		select {
		case <-p.terminated:
			return
		case <-ctx.Done():
			if err := p.killProcessTree(); err != nil {
				log.Warningf("Failed to kill process tree: %s", err)
			}
		}
	}()

	return p, nil
}

func (p *process) monitor(statsListener procstats.Listener) chan *repb.UsageStats {
	statsCh := make(chan *repb.UsageStats, 1)
	// Monitor goroutine: periodically record process stats.
	go func() {
		defer close(statsCh)
		if statsListener == nil {
			return
		}
		statsCh <- procstats.Monitor(p.cmd.Process.Pid, statsListener, p.terminated)
	}()

	return statsCh
}

func (p *process) wait() error {
	defer close(p.terminated)
	return p.cmd.Wait()
}

// RunWithProcessTreeCleanup runs the given command, ensuring that child
// processes are killed if the command times out.
//
// It is intended to be used with a command created via exec.Command(), not
// exec.CommandContext(). Unlike exec.CommandContext.Run(), it kills the process
// tree when the context is done, instead of just killing the top-level process.
// This helps ensure that orphaned child processes aren't left running after the
// command completes.
//
// For an example command that can be passed to this func, see
// constructExecCommand.
//
// If statsListener is non-nil, stats will be enabled and the callback will be
// invoked each time stats are measured. In addition, the stats returned will
// be non-nil. Note that enabling stats incurs some overhead, so a nil callback
// should be used if stats aren't needed.
func RunWithProcessTreeCleanup(ctx context.Context, cmd *exec.Cmd, statsListener procstats.Listener) (*repb.UsageStats, error) {
	p, err := startNewProcess(ctx, cmd)
	if err != nil {
		return nil, err
	}
	statsCh := p.monitor(statsListener)

	err = p.wait()
	return <-statsCh, err
}

// ChildPids returns all *direct* child pids of a process identified by pid.
func ChildPids(pid int) ([]int, error) {
	procs, err := ps.Processes()
	if err != nil {
		return nil, err
	}
	var out []int
	for _, proc := range procs {
		if proc.PPid() != pid {
			continue
		}
		out = append(out, proc.Pid())
	}
	return out, nil
}

func ErrorResult(err error) *interfaces.CommandResult {
	return &interfaces.CommandResult{
		Error:    err,
		ExitCode: NoExitCode,
	}
}

func splitExecutableArgs(commandTokens []string) (executable string, args []string) {
	executable = commandTokens[0]
	if len(commandTokens) > 1 {
		args = commandTokens[1:]
	} else {
		args = []string{}
	}
	return
}

// exitCode returns the exit code from the given command, based on the error returned.
// If the command could not be started or did not exit cleanly, an error is returned.
func ExitCode(ctx context.Context, cmd *exec.Cmd, err error) (int, error) {
	if err == nil {
		return 0, nil
	}
	// exec.Error is only returned when `exec.LookPath` fails to classify a file as an executable.
	// This could be a "not found" error or a permissions error, but we just report it as "not found".
	//
	// See:
	// - https://golang.org/pkg/os/exec/#Error
	// - https://github.com/golang/go/blob/fcb9d6b5d0ba6f5606c2b5dfc09f75e2dc5fc1e5/src/os/exec/lp_unix.go#L35
	if notFoundErr, ok := err.(*exec.Error); ok {
		return NoExitCode, status.NotFoundError(notFoundErr.Error())
	}

	// If we fail to get the exit code of the process for any other reason, it might
	// be a transient error that the client can retry, so return UNAVAILABLE for now.
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		return NoExitCode, status.UnavailableError(err.Error())
	}
	processState := exitErr.ProcessState
	if processState == nil {
		return NoExitCode, status.UnavailableError(err.Error())
	}

	exitCode := processState.ExitCode()

	// TODO(bduffany): Extract syscall.WaitStatus from exitErr.Sys(), and set
	// ErrSIGKILL if waitStatus.Signal() == syscall.SIGKILL, so that the command
	// can be retried if it was OOM killed. Note that KilledExitCode does not
	// imply that SIGKILL was received.

	if exitCode == KilledExitCode {
		if dl, ok := ctx.Deadline(); ok && time.Now().After(dl) {
			return exitCode, status.DeadlineExceededErrorf("Command timed out: %s", err.Error())
		}
		// If the command didn't time out, it was probably killed by the kernel due to OOM.
		return exitCode, status.ResourceExhaustedErrorf("Command was killed: %s", err.Error())
	}

	return exitCode, nil
}

// EnvStringList returns the command's environment variables as a list of string
// assignments. (Example: {"KEY1=VAL1", "KEY2=VAL2"})
func EnvStringList(command *repb.Command) []string {
	env := make([]string, 0, len(command.GetEnvironmentVariables()))
	for _, envVar := range command.GetEnvironmentVariables() {
		env = append(env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	return env
}
