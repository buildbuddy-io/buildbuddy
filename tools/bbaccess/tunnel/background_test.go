package tunnel

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadPid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tunnel.pid")
	_, err := readPid(path)
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, os.WriteFile(path, []byte("4242\n"), 0o644))
	pid, err := readPid(path)
	require.NoError(t, err)
	require.Equal(t, 4242, pid)

	for _, bad := range []string{"", "abc", "-1", "0"} {
		require.NoError(t, os.WriteFile(path, []byte(bad), 0o644))
		_, err := readPid(path)
		require.Error(t, err, "%q", bad)
	}
}

func TestLastLines(t *testing.T) {
	require.Equal(t, "a\nb", lastLines("a\nb\n", 5))
	require.Equal(t, "d\ne", lastLines("a\nb\nc\nd\ne\n", 2))
	require.Equal(t, "", lastLines("", 2))
}
