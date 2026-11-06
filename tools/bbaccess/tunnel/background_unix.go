//go:build unix

package tunnel

import (
	"os"
	"syscall"
)

// detachAttr puts the daemon in its own session, so that closing the terminal
// it was started from does not hang it up.
func detachAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

// lockExclusive takes the lock on f for the life of the process, or returns
// errLocked if another process holds it.
func lockExclusive(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return errLocked
	}
	return err
}

// lockHeld reports whether another process holds the lock on f.
func lockHeld(f *os.File) (bool, error) {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
