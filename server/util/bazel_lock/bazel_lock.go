package bazel_lock

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sys/unix"
)

const readFile = true

// IsOutputBaseLocked returns whether the given output base is locked by any
// bazel process.
//
// This is not safe to call concurrently, since it temporarily acquires an
// exclusive lock on the output base to test whether it is locked.
func IsOutputBaseLocked(outputBase string) (bool, error) {
	lockPath := filepath.Join(outputBase, "lock")
	if readFile {
		b, err := os.ReadFile(lockPath)
		log.Errorf("lockfile contents: %s", string(b))
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
		}
		for _, f := range strings.Fields(string(b)) {
			k, v, _ := strings.Cut(f, "=")
			if k != "pid" {
				continue
			}
			// Iterate through all open fds of pid
			// XXX: this only works on Linux
			pid := v
			entries, err := os.ReadDir("/proc/" + pid + "/fd")
			log.Infof("readdir /proc/%s/fd: %d entries; err=%s", pid, len(entries), err)
			{
				b, _ := exec.Command("sh", "-c", "ps aux | grep "+pid+"; ps aux | grep bazel").CombinedOutput()
				log.Infof("ps: %s", string(b))
			}
			if err != nil {
				if os.IsNotExist(err) {
					return false, nil
				}
				return false, fmt.Errorf("read /proc/%s/fd: %w", pid, err)
			}
			for _, entry := range entries {
				fd := entry.Name()
				target, _ := os.Readlink("/proc/" + pid + "/fd/" + fd)
				log.Infof("fd %s -> %s", fd, target)
				if target == lockPath {
					// Lock path is still open, so the output base is locked.
					return true, nil
				}
			}
		}
		// return false, nil
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("open lock file: %w", err)
	}
	defer f.Close()
	// Try acquiring the lock. If we succeed, the output base is not locked.
	// Note: deferred f.Close() will release the lock if we acquire it below.
	ok, err := tryLock(f.Fd(), false)
	if err != nil {
		return false, fmt.Errorf("try lock: %w", err)
	}
	log.Errorf("tryLock(%q) ok=%v", lockPath, ok)
	return !ok, nil
}

// See https://github.com/bazelbuild/bazel/blob/18129ba6a5653878d2699aec7b1ec18ac94b0dca/src/main/cpp/blaze_util_posix.cc#L615-L648
func tryLock(fd uintptr, shared bool) (bool, error) {
	lock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: unix.SEEK_SET,
		Start:  0,
		Len:    1,
	}
	if shared {
		lock.Type = unix.F_RDLCK
	}
	// F_OFD_SETLK is only available on Linux and requires kernel 3.15 or newer,
	// so on non-Linux platforms we skip the F_OFD_SETLK codepath, and on Linux
	// kernels older than 3.15 we will hit the EINVAL case below and fall back
	// to the general POSIX codepath.
	if runtime.GOOS == "linux" {
		const F_OFD_SETLK = 0x25
		if err := unix.FcntlFlock(fd, F_OFD_SETLK, &lock); err != nil {
			if err == unix.EACCES || err == unix.EAGAIN {
				// Already locked by another process.
				return false, nil
			}
			if err != unix.EINVAL {
				// Unexpected error.
				return false, fmt.Errorf("fcntl: %w", err)
			}
			// Fall back to POSIX locks on EINVAL.
		} else {
			log.Errorf("F_OFD_SETLK succeeded")
			return true, nil
		}
	}
	if err := unix.FcntlFlock(fd, unix.F_SETLK, &lock); err != nil {
		if err == unix.EACCES || err == unix.EAGAIN {
			// Already locked by another process.
			return false, nil
		}
		// Unexpected error.
		return false, fmt.Errorf("fcntl: %w", err)
	}
	return true, nil
}
