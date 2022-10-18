package watcher

import (
	"os"
	"syscall"

	"github.com/bduffany/godemon"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/shlex"
)

// Watch looks for the -w or --watch flag, and if present it reinvokes the
// CLI as a subprocess on changes to source files.
func Watch() (exitCode int, err error) {
	args := os.Args

	idx := -1
	for i, arg := range args {
		if arg == "-w" || arg == "--watch" {
			idx = i
			break
		}
	}
	if idx == -1 {
		return -1, nil
	}
	args = append(args[:idx], args[idx+1:]...)

	// Allow specifying --watcher_flags to forward args to the watcher.
	// Mostly useful for debugging, e.g. --watcher_flags='--verbose'
	watcherFlagsRaw, args := arg.Pop(args, "watcher_flags")
	watcherFlags, err := shlex.Split(watcherFlagsRaw)
	if err != nil {
		return -1, status.InvalidArgumentErrorf("failed to parse --watcher_flags: %s", err)
	}

	// Notes on FS watcher solutions:
	// https://docs.google.com/document/d/1tbe7lAX6OEYe5_1FRLG8RPG3lXGUrT4_Vv9UCx6_Vwo

	workspaceDir, err := workspace.Path()
	if err != nil {
		return -1, err
	}

	_ = os.Setenv("GODEMON_LOG_PREFIX", "--- ")
	argv := append([]string{"godemon", "--watch", workspaceDir}, watcherFlags...)
	argv = append(argv, args...)

	// Optionally invoke a specific godemon binary.
	// Especially useful for development but can also be used to pull in newer
	// godemon features.
	if bin := os.Getenv("GODEMON_BINARY_PATH"); bin != "" {
		if err := syscall.Exec(bin, argv, os.Environ()); err != nil {
			return -1, err
		}
		panic("unreachable")
	}

	godemon.Main(argv)

	return 0, nil
}
