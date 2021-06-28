package commandutil

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

func constructExecCommand(ctx context.Context, command *repb.Command, workDir string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	executable, args := splitExecutableArgs(command.GetArguments())
	cmd := exec.CommandContext(ctx, executable, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	for _, envVar := range command.GetEnvironmentVariables() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	return cmd, &stdout, &stderr
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

// Run a command, retrying "text file busy" errors.
func Run(ctx context.Context, command *repb.Command, workDir string) *interfaces.CommandResult {
	var cmd *exec.Cmd
	var stdoutBuf, stderrBuf *bytes.Buffer

	err := RetryIfTextFileBusy(func() error {
		// Create a new command on each attempt since commands can only be run once.
		cmd, stdoutBuf, stderrBuf = constructExecCommand(ctx, command, workDir)
		return cmd.Run()
	})

	exitCode, err := ExitCode(ctx, cmd, err)

	return &interfaces.CommandResult{
		ExitCode:           exitCode,
		Error:              err,
		Stdout:             stdoutBuf.Bytes(),
		Stderr:             stderrBuf.Bytes(),
		CommandDebugString: cmd.String(),
	}
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

	if exitCode == KilledExitCode {
		if dl, ok := ctx.Deadline(); ok && time.Now().After(dl) {
			return exitCode, status.DeadlineExceededErrorf("Command timed out: %s", err.Error())
		}
		// If the command didn't time out, it was probably killed by the kernel due to OOM.
		return exitCode, status.ResourceExhaustedErrorf("Command `%s` was killed: %s", cmd.String(), err.Error())
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
