package commandutil

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
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

func constructExecCommand(command *repb.Command, workDir string, stdio *container.Stdio) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, error) {
	if stdio == nil {
		stdio = &container.Stdio{}
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
func Run(ctx context.Context, command *repb.Command, workDir string, statsListener procstats.Listener, stdio *container.Stdio) *interfaces.CommandResult {
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
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pid := cmd.Process.Pid
	processTerminated := make(chan struct{})
	// Cleanup goroutine: kill the process tree when the context is canceled.
	go func() {
		select {
		case <-processTerminated:
			return
		case <-ctx.Done():
			if err := KillProcessTree(pid); err != nil {
				log.Warningf("Failed to kill process tree: %s", err)
			}
		}
	}()
	statsCh := make(chan *repb.UsageStats, 1)
	// Monitor goroutine: periodically record process stats.
	go func() {
		defer close(statsCh)
		if statsListener == nil {
			return
		}
		statsCh <- procstats.Monitor(pid, statsListener, processTerminated)
	}()
	wait := func() error {
		defer close(processTerminated)
		return cmd.Wait()
	}
	if err := wait(); err != nil {
		return nil, err
	}
	return <-statsCh, nil
}

// KillProcessTree kills the given pid as well as any descendant processes.
//
// It tries to kill as many processes in the tree as possible. If it encounters
// an error along the way, it proceeds to kill subsequent pids in the tree. It
// returns the last error encountered, if any.
func KillProcessTree(pid int) error {
	var lastErr error

	// Run a BFS on the process tree to build up a list of processes to kill.
	// Before listing child processes for each pid, send SIGSTOP to prevent it
	// from spawning new child processes. Otherwise the child process list has a
	// chance to become stale if the pid forks a new child just after we list
	// processes but before we send SIGKILL.

	pidsToExplore := []int{pid}
	pidsToKill := []int{}
	for len(pidsToExplore) > 0 {
		pid := pidsToExplore[0]
		pidsToExplore = pidsToExplore[1:]
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			lastErr = err
			// If we fail to SIGSTOP, proceed anyway; the more we can clean up,
			// the better.
		}
		pidsToKill = append(pidsToKill, pid)

		childPids, err := ChildPids(pid)
		if err != nil {
			lastErr = err
			continue
		}
		pidsToExplore = append(pidsToExplore, childPids...)
	}
	for _, pid := range pidsToKill {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			lastErr = err
		}
	}

	return lastErr
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

// LookupCredential resolves a "USER[:GROUP]" string to a credential with both
// uid and gid populated. Both numeric IDs and non-numeric names can be
// specified for either USER or GROUP. If no group is specified, then the user's
// primary group is used.
//
// NOTE: This function does not authenticate that the user is part of the
// specified group.
func LookupCredential(spec string) (*syscall.Credential, error) {
	parts := strings.Split(spec, ":")
	if len(parts) == 0 {
		return nil, status.InvalidArgumentError("credential spec is empty: expected USER[:GROUP]")
	}
	if len(parts) > 2 {
		return nil, status.InvalidArgumentError("credential spec had too many parts: expected USER[:GROUP]")
	}
	userSpec := parts[0]
	var u *user.User
	var g *user.Group
	var err error
	if allDigits.MatchString(userSpec) {
		u, err = user.LookupId(userSpec)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("uid lookup failed: %s", err)
		}
	} else {
		u, err = user.Lookup(userSpec)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("user lookup failed: %s", err)
		}
	}

	groupSpec := u.Gid
	if len(parts) > 1 {
		groupSpec = parts[1]
	}
	if allDigits.MatchString(groupSpec) {
		g, err = user.LookupGroupId(groupSpec)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("gid lookup failed: %s", err)
		}
	} else {
		g, err = user.LookupGroup(groupSpec)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("group lookup failed: %s", err)
		}
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return nil, status.InternalErrorf("failed to parse uid: %s", err)
	}
	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return nil, status.InternalErrorf("failed to parse gid: %s", err)
	}

	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, nil
}
