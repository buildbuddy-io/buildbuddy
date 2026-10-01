//go:build linux

package dirtools

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sys/unix"
)

var executableReadinessUnsupportedWarning sync.Once

// waitForExecutableReady waits until the kernel reports that no writable file
// description remains for path. A concurrent fork can inherit a download's
// writer even when it is close-on-exec, and the child's final release can
// outlive the downloader's Close. Publishing the file during that interval can
// cause execve to fail with ETXTBSY.
//
// The caller must ensure that no new writer can open the inode once the
// download is complete and that path keeps referring to that inode through
// publication. A read lease checks existing writers; it does not make the
// file immutable after the lease is released. If file leases are unsupported,
// the check logs a warning and preserves the previous publication behavior.
func waitForExecutableReady(ctx context.Context, path string) error {
	return waitForExecutableReadyWithFcntl(ctx, path, unix.FcntlInt)
}

func waitForExecutableReadyWithFcntl(ctx context.Context, path string, fcntl func(uintptr, int, int) (int, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open executable for readiness check: %w", err)
	}
	defer f.Close()

	delay := time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := fcntl(f.Fd(), unix.F_SETLEASE, unix.F_RDLCK)
		if err == nil {
			if _, err := fcntl(f.Fd(), unix.F_SETLEASE, unix.F_UNLCK); err != nil {
				return fmt.Errorf("release executable read lease: %w", err)
			}
			return nil
		}
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
			// EINVAL also means the file is not regular. Only treat it as an
			// unsupported lease when the downloaded file is regular.
			if errors.Is(err, unix.EINVAL) {
				info, statErr := f.Stat()
				if statErr != nil {
					return fmt.Errorf("stat executable for readiness check: %w", statErr)
				}
				if !info.Mode().IsRegular() {
					return fmt.Errorf("acquire executable read lease: %w", err)
				}
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			executableReadinessUnsupportedWarning.Do(func() {
				log.CtxWarningf(ctx, "Cannot check executable readiness for %q because file leases are unsupported (%s); continuing with the downloaded file. Further unsupported-lease warnings will be suppressed.", path, err)
			})
			return nil
		}
		if !errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("acquire executable read lease: %w", err)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, 10*time.Millisecond)
	}
}
