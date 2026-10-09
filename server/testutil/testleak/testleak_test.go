package testleak

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeTB records errors instead of failing the real test.
type fakeTB struct {
	testing.TB
	mu       sync.Mutex
	errors   []string
	cleanups []func()
}

func (f *fakeTB) Cleanup(fn func())   { f.cleanups = append(f.cleanups, fn) }
func (f *fakeTB) Name() string        { return "fake" }
func (f *fakeTB) Logf(string, ...any) {}
func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeTB) runCleanups() {
	for _, fn := range slices.Backward(f.cleanups) {
		fn()
	}
}

func TestCheckFDs_NoLeak(t *testing.T) {
	ft := &fakeTB{TB: t}
	CheckFDs(ft)
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	ft.runCleanups()
	require.Empty(t, ft.errors)
}

func TestCheckFDs_Leak(t *testing.T) {
	ft := &fakeTB{TB: t}
	CheckFDs(ft)
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer f.Close()
	ft.runCleanups()
	require.Len(t, ft.errors, 1)
	require.True(t, strings.HasPrefix(ft.errors[0], "file descriptors leaked"))
}

func TestCheckFDs_IgnoredTarget(t *testing.T) {
	ft := &fakeTB{TB: t}
	CheckFDs(ft, IgnoreFDTarget(os.DevNull))
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer f.Close()
	ft.runCleanups()
	require.Empty(t, ft.errors)
}

func TestCheckFDs_ClosedByCleanup(t *testing.T) {
	ft := &fakeTB{TB: t}
	CheckFDs(ft)
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	// Registered after CheckFDs, so it runs first.
	ft.Cleanup(func() { f.Close() })
	ft.runCleanups()
	require.Empty(t, ft.errors)
}

func TestCheckFDs_RenamedFileOpenedBefore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	ft := &fakeTB{TB: t}
	CheckFDs(ft)
	require.NoError(t, os.Rename(path, path+".renamed"))
	ft.runCleanups()
	require.Empty(t, ft.errors)
}

func TestCheckFDs_DeletedFileOpenedBefore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	ft := &fakeTB{TB: t}
	CheckFDs(ft)
	require.NoError(t, os.Remove(path))
	ft.runCleanups()
	require.Empty(t, ft.errors)
}

func TestCheckFDs_ReportsLeakedTarget(t *testing.T) {
	ft := &fakeTB{TB: t}
	CheckFDs(ft)
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer f.Close()
	ft.runCleanups()
	require.Len(t, ft.errors, 1)
	require.Contains(t, ft.errors[0], "-> "+os.DevNull)
}
