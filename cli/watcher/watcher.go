package watcher

import (
	"fmt"
	"os"
	"syscall"

	"github.com/bduffany/godemon"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

const lockfilePathEnvVar = "BB_WATCHER_LOCKFILE_PATH"

var (
	settings = struct {
		watch        bool
		watcherFlags []string
	}{}
)

func Configure(watch bool, watcherFlags []string) {
	settings.watch = watch
	settings.watcherFlags = watcherFlags
}

// If enabled through Configure, Watch reinvokes the
// CLI as a subprocess on changes to source files.
func Watch(args []string) (exitCode int, err error) {
	if !settings.watch {
		return -1, nil
	}
	// Godemon's child inherits the lockfile path from the original bb process.
	// Do not recursively start another watcher if the child re-enables --watch
	// (can happen on accident through a rc file, for example).
	if os.Getenv(lockfilePathEnvVar) != "" {
		return -1, nil
	}
	// Notes on FS watcher solutions:
	// https://docs.google.com/document/d/1tbe7lAX6OEYe5_1FRLG8RPG3lXGUrT4_Vv9UCx6_Vwo

	workspaceDir, err := workspace.Path()
	if err != nil {
		return -1, err
	}

	lockfile, err := initLockfile()
	if err != nil {
		return -1, fmt.Errorf("failed to initialize watcher lockfile: %s", err)
	}

	_ = os.Setenv("GODEMON_LOG_PREFIX", "--- ")
	argv := append([]string{
		"godemon",
		"--watch", workspaceDir,
		"--lockfile", lockfile,
	}, settings.watcherFlags...)
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

// Prepares a lockfile path that can be used to Pause and Unpause the watcher
// by creating or removing it, respectively.
func initLockfile() (string, error) {
	f, err := os.CreateTemp("", "watcher-*.lock")
	if err != nil {
		return "", err
	}
	f.Close()
	if err := os.Remove(f.Name()); err != nil {
		return "", err
	}
	os.Setenv(lockfilePathEnvVar, f.Name())
	return f.Name(), nil
}

// Pause prevents the file watcher from triggering restarts until Unpause()
// is called. Any FS events received while paused will be buffered, then flushed
// when unpaused.
func Pause() {
	lockfilePath := os.Getenv(lockfilePathEnvVar)
	if lockfilePath == "" {
		// We're not running in watch mode.
		return
	}
	f, err := os.Create(lockfilePath)
	if err != nil {
		log.Printf("Warning: Failed to pause file watcher: %s", err)
		return
	}
	f.Close()
}

// Unpause resumes watcher restart-on-update functionality. A restart will be
// triggered immediately if any events were buffered while the watcher was
// paused.
func Unpause() {
	lockfilePath := os.Getenv(lockfilePathEnvVar)
	if lockfilePath == "" {
		// We're not running in watch mode.
		return
	}
	if err := os.Remove(lockfilePath); err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("Warning: Failed to unpause file watcher: %s", err)
		return
	}
}
