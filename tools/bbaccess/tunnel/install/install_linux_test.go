//go:build !android

package install

import (
	"net/netip"
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
