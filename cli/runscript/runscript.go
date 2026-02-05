package runscript

import (
	"os"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/stream_run_logs"
)

// Configure adds `--script_path` to a bazel run command so that we can
// invoke the build and the run separately.
func Configure(args []string) (newArgs []string, scriptPath string, err error) {
	if arg.GetCommand(args) != "run" {
		return args, "", nil
	}
	// If --script_path is already set, don't create a run script ourselves,
	// since the caller probably has the intention to invoke it on their own.
	existingScript := arg.Get(args, "script_path")
	if existingScript != "" {
		return args, "", nil
	}
	script, err := os.CreateTemp("", "bb-run-*")
	if err != nil {
		return nil, "", err
	}
	defer script.Close()
	scriptPath = script.Name()
	args = arg.Append(args, "--script_path="+scriptPath)
	return args, scriptPath, nil
}

// Invoke executes the run script, replacing the current process.
func Invoke(path string) (exitCode int, err error) {
	if err := os.Chmod(path, 0o755); err != nil {
		return -1, err
	}
	// TODO: Exec() replaces the current process, so it prevents us from running
	// post-run hooks (if we decide those will be supported). If we want to use
	// exec.Command() here instead of exec(), then we might need to manually
	// forward signals from the parent file watcher process.
	if err := syscall.Exec(path, nil, os.Environ()); err != nil {
		return -1, err
	}
	panic("unreachable")
}

// InvokeWithLogStreaming executes the run script while streaming its output to the server.
// Unlike Invoke, this does not replace the current process, so it returns after execution.
func InvokeWithLogStreaming(path string, opts stream_run_logs.Opts) (exitCode int, err error) {
	return stream_run_logs.Execute(path, opts)
}
