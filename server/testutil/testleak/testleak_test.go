package testleak_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testleak"
	"github.com/stretchr/testify/require"
)

// fakeT records cleanups and errors, so a test can run a check's cleanup
// itself and see whether it fails.
type fakeT struct {
	testing.TB
	cleanups []func()
	errors   []string
}

func (f *fakeT) Cleanup(fn func()) {
	f.cleanups = append(f.cleanups, fn)
}

func (f *fakeT) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeT) runCleanups() {
	for _, fn := range slices.Backward(f.cleanups) {
		fn()
	}
}

func TestCheck_PassesWhenGoroutinesExit(t *testing.T) {
	ft := &fakeT{TB: t}
	testleak.Check(ft)
	done := make(chan struct{})
	go func() { <-done }()
	close(done)

	ft.runCleanups()

	require.Empty(t, ft.errors)
}

func TestCheck_FailsWhenGoroutineLeaks(t *testing.T) {
	ft := &fakeT{TB: t}
	testleak.Check(ft)
	stop := make(chan struct{})
	defer close(stop)
	go func() { <-stop }()

	ft.runCleanups()

	require.Len(t, ft.errors, 1)
	require.Contains(t, ft.errors[0], "TestCheck_FailsWhenGoroutineLeaks")
}

func TestCheck_IgnoresGoroutinesStartedBeforeCheck(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	go func() { <-stop }()
	ft := &fakeT{TB: t}
	testleak.Check(ft)

	ft.runCleanups()

	require.Empty(t, ft.errors)
}
