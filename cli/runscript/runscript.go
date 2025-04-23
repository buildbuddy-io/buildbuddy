package runscript

import (
	"os"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
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
	args = append(args, "--script_path="+scriptPath)
	return args, scriptPath, nil
}

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
