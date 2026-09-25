//go:build unix

package testport

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var portLocks struct {
	sync.Mutex
	files []*os.File
}

// lockPort acquires an exclusive cross-process lock for the given port to
// prevent parallel test processes from choosing the same port. The lock is
// released automatically when the process exits.
func lockPort(port int) bool {
	lockPath := filepath.Join(os.TempDir(), fmt.Sprintf("testport.%d.lock", port))
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return false
	}
	// Keep the file reachable for the lifetime of the process. Merely omitting
	// Close is insufficient: os.File's finalizer closes unreachable files and
	// releases their locks, allowing another test to claim a live port lease.
	portLocks.Lock()
	portLocks.files = append(portLocks.files, f)
	portLocks.Unlock()
	return true
}
