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
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sys/unix"
)

const (
	// Max wait time of waitForExecutableReady.
	executableReadyTimeout = 5 * time.Second
)

var (
	executableReadinessLeaseErrorWarning sync.Once
)

// waitForExecutableReady waits until the kernel reports that no writable file
// description remains for the given path.
//
// This is useful for working around an issue with how child processes work on
// Linux. Let's say we have two actions, A and B. The following sequence can
// happen:
//  1. Action A opens a file descriptor, say fd 37, for writing an executable
//     input file.
//  2. Action B, an arbitrary different action, calls fork, e.g. to start crun.
//     Because of how fork works, this copies fd 37 to the forked process, and
//     keeps it open for writing. Actually starting crun requires both fork()
//     and exec(), but in this scenario, it takes a little while to get to the
//     exec() syscall, due to increased load on the system.
//  3. Action A finishes downloading the executable and closes it. It then tries
//     to execute the file. This fails with ETXTBSY, because Linux does not allow
//     executing an executable file if *any* process has it open for writing,
//     and the forked process for action B still has fd 37 open. Note that
//     Action B does not even need this executable - the fact that it has fd 37
//     open is purely a side effect of the fork() syscall copying the fd table
//     to the new process.
//  4. Action B then calls exec, which closes fd 37 (since files in Go are opened
//     with O_CLOEXEC). However, it's too late - Action A has already failed.
//
// With this function, Action A can explicitly wait for Action B to release fd
// 37 before it starts executing. It does this by repeatedly attempting to
// acquire a read lease on the file using the F_SETLEASE fcntl, which fails with
// EAGAIN if any process has the file open for writing. This is best-effort; if
// F_SETLEASE is unsupported or not allowed by permissions, or the file is still
// open for writing after executableReadyTimeout, it logs a warning and returns
// without error.
func waitForExecutableReady(ctx context.Context, path string) error {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return waitForExecutableReadyWithFcntl(ctx, path, executableReadyTimeout, unix.FcntlInt)
}

func waitForExecutableReadyWithFcntl(ctx context.Context, path string, timeout time.Duration, fcntl func(uintptr, int, int) (int, error)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open executable for readiness check: %w", err)
	}
	defer f.Close()

	deadline := time.Now().Add(timeout)
	delay := time.Millisecond
	for {
		_, err := fcntl(f.Fd(), unix.F_SETLEASE, unix.F_RDLCK)
		if err == nil {
			if _, err := fcntl(f.Fd(), unix.F_SETLEASE, unix.F_UNLCK); err != nil {
				return fmt.Errorf("release executable read lease: %w", err)
			}
			return nil
		}
		if !errors.Is(err, unix.EAGAIN) {
			// F_SETLEASE is not supported or not allowed.
			executableReadinessLeaseErrorWarning.Do(func() {
				log.CtxWarningf(ctx, "Cannot check executable readiness for %q because a read lease could not be acquired (%s); continuing with the downloaded file. Further lease warnings will be suppressed.", path, err)
			})
			return nil
		}
		if time.Now().After(deadline) {
			log.CtxWarningf(ctx, "Gave up waiting for writers of executable %q to close after %s; continuing with the downloaded file.", path, timeout)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(2*delay, 10*time.Millisecond)
	}
}
