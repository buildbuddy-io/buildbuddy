//go:build !windows

package update

import (
	"os"
	"syscall"
)

// Reexec replaces the current process with the (just updated) executable,
// keeping the arguments. NoUpdateEnv is set so the new process does not
// check again.
func Reexec() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	return syscall.Exec(exe, os.Args, append(os.Environ(), NoUpdateEnv+"=1"))
}
