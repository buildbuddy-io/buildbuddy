// Package testleak checks that tests don't leave goroutines running or file
// descriptors open.
package testleak

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// Check fails the test if any goroutine started after Check is called is still
// running once the test's other cleanups have finished. Cleanups run in
// reverse order, so call Check before setting up anything that registers a
// cleanup, such as servers or test environments.
//
// opts are passed to goleak, and are typically goleak.IgnoreTopFunction or
// goleak.IgnoreAnyFunction options listing known leaks.
func Check(t testing.TB, opts ...goleak.Option) {
	opts = append(slices.Clip(opts), goleak.IgnoreCurrent())
	t.Cleanup(func() {
		if err := goleak.Find(opts...); err != nil {
			t.Errorf("goroutines leaked by %s: %s", t.Name(), err)
		}
	})
}

const (
	// fdDir lists the open file descriptors of the current process.
	fdDir = "/proc/self/fd"

	// How long CheckFDs waits for file descriptors to be closed, since some
	// are closed by goroutines that are still finishing up, or by finalizers.
	fdWaitTimeout = 5 * time.Second
	fdPollPeriod  = 20 * time.Millisecond
)

// FDOption configures CheckFDs.
type FDOption func(*fdOptions)

type fdOptions struct {
	ignoredPrefixes []string
}

// IgnoreFDTarget makes CheckFDs ignore file descriptors whose link target in
// /proc/self/fd starts with prefix, such as "anon_inode:[eventfd]" or
// "socket:". Use it to list known leaks.
func IgnoreFDTarget(prefix string) FDOption {
	return func(o *fdOptions) {
		o.ignoredPrefixes = append(o.ignoredPrefixes, prefix)
	}
}

// CheckFDs fails the test if any file descriptor opened after CheckFDs is
// called is still open once the test's other cleanups have finished. Like
// Check, call it before setting up anything that registers a cleanup.
//
// CheckFDs only works on Linux. Elsewhere it does nothing.
func CheckFDs(t testing.TB, opts ...FDOption) {
	o := &fdOptions{}
	for _, opt := range opts {
		opt(o)
	}
	before, err := openFDs()
	if err != nil {
		t.Logf("Not checking for leaked file descriptors: %s", err)
		return
	}
	t.Cleanup(func() {
		var leaked []string
		deadline := time.Now().Add(fdWaitTimeout)
		for {
			// Some file descriptors are only closed by finalizers once their
			// owners are garbage collected, such as the pipes that the net
			// package caches in a sync.Pool for splice(). Clearing a sync.Pool
			// takes two GCs.
			runtime.GC()
			runtime.GC()
			after, err := openFDs()
			if err != nil {
				t.Errorf("list open file descriptors: %s", err)
				return
			}
			leaked = leaked[:0]
			for fd, target := range after {
				if before[fd] == target || o.ignored(target) {
					continue
				}
				leaked = append(leaked, fmt.Sprintf("%s -> %s", fd, target))
			}
			if len(leaked) == 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(fdPollPeriod)
		}
		if len(leaked) > 0 {
			sort.Strings(leaked)
			t.Errorf("file descriptors leaked by %s:\n%s", t.Name(), strings.Join(leaked, "\n"))
		}
	})
}

func (o *fdOptions) ignored(target string) bool {
	for _, prefix := range o.ignoredPrefixes {
		if strings.HasPrefix(target, prefix) {
			return true
		}
	}
	return false
}

// openFDs returns the open file descriptors of the current process, mapped to
// their link targets, e.g. "socket:[12345]". A file descriptor that is closed
// and reused for a different file has a different target.
func openFDs() (map[string]string, error) {
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil, err
	}
	fds := make(map[string]string, len(entries))
	for _, e := range entries {
		target, err := os.Readlink(fdDir + "/" + e.Name())
		if err != nil {
			// The fd used by ReadDir itself is gone by now.
			continue
		}
		fds[e.Name()] = target
	}
	return fds, nil
}
