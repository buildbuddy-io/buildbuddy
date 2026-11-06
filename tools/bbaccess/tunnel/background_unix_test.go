//go:build unix

package tunnel

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPidFileLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tunnel.pid")
	_, held := lockedPid(path)
	require.False(t, held)

	pf, err := holdPidFile(path)
	require.NoError(t, err)
	pid, held := lockedPid(path)
	require.True(t, held)
	require.Equal(t, os.Getpid(), pid)

	_, err = holdPidFile(path)
	require.Error(t, err, "a second daemon must not get the lock")

	pf.release()
	_, held = lockedPid(path)
	require.False(t, held)
	require.NoFileExists(t, path)

	// A file left behind by a dead daemon is not a running daemon.
	require.NoError(t, os.WriteFile(path, []byte("4242\n"), 0o644))
	_, held = lockedPid(path)
	require.False(t, held)
}
