//go:build unix

package tunnel

import "syscall"

// detachAttr puts the daemon in its own session, so that closing the terminal
// it was started from does not hang it up.
func detachAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

// processAlive reports whether a process with this pid exists. One we may not
// signal still exists.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
