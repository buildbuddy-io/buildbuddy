package flock_test

import (
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/flock"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestFlock(t *testing.T) {
	path := testfs.MakeTempDir(t)
	f1, err := flock.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f1.Close() })
	f2, err := flock.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f2.Close() })
	{
		err := f1.Lock()
		require.NoError(t, err)
	}
	{
		// Same process should be able to lock the same file descriptor twice
		err := f1.Lock()
		require.NoError(t, err)
	}
	{
		// TryLock should also work when using the same file descriptor.
		err := f1.TryLock()
		require.NoError(t, err)
	}
	{
		// f2 is within the same process but is using a different file
		// descriptor, so TryLock should fail.
		err := f2.TryLock()
		require.Equal(t, syscall.EAGAIN, err)
	}
	{
		// Unlock f1, should be able to lock f2 after.
		err := f1.Unlock()
		require.NoError(t, err)
	}
	{
		err := f2.TryLock()
		require.NoError(t, err)
	}
}
