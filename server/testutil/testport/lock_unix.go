//go:build unix

package testport

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var portLockFiles sync.Map

// lockPort acquires an exclusive cross-process lock for the given port to
// prevent parallel test processes from choosing the same port. The lock is
// released automatically when the process exits.
func lockPort(port int) bool {
	// Bazel sandboxes can set TMPDIR to a sandbox-local directory, but these
	// locks need to coordinate all test processes on the host.
	lockDir := filepath.Join("/tmp", fmt.Sprintf("buildbuddy-testport-locks-%d", os.Getuid()))
	if err := os.MkdirAll(lockDir, 0700); err != nil {
		return false
	}
	lockPath := filepath.Join(lockDir, fmt.Sprintf("testport.%d.lock", port))
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return false
	}
	if _, loaded := portLockFiles.LoadOrStore(port, f); loaded {
		f.Close()
		return false
	}
	return true
}
