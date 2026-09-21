//go:build !android

package install

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnitWord(t *testing.T) {
	for _, ok := range []string{"bbtun0", "someone", "some.user", "a-b_c"} {
		require.True(t, unitWord.MatchString(ok), ok)
	}
	// Anything that could change the unit's meaning is refused.
	for _, bad := range []string{"", "bb tun0", "bbtun0\nExecStart=/bin/true", "50%", "tun/0", "a=b"} {
		require.False(t, unitWord.MatchString(bad), "%q", bad)
	}
}

func TestDeviceOwner(t *testing.T) {
	dir := t.TempDir()
	sysClassNet = dir
	t.Cleanup(func() { sysClassNet = "/sys/class/net" })
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "bbtun0"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bbtun0", "owner"), []byte("1000\n"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "eth0"), 0o755))

	owner, err := deviceOwner("bbtun0")
	require.NoError(t, err)
	require.Equal(t, 1000, owner)
	owner, err = deviceOwner("eth0")
	require.NoError(t, err)
	require.Equal(t, -1, owner, "only persistent TUN/TAP devices have an owner")
}

func TestDeviceUnitRenders(t *testing.T) {
	unit, err := deviceUnit(deviceUnitParams{
		IPCommand: "/usr/sbin/ip",
		User:      "someone",
		Dev:       "bbtun0",
		Addr:      netip.MustParsePrefix("198.18.0.1/16"),
		MTU:       tunMTU,
	})
	require.NoError(t, err)
	require.Contains(t, unit, "mode tun user someone")
}
