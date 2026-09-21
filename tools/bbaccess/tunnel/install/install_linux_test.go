//go:build !android

package install

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

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
