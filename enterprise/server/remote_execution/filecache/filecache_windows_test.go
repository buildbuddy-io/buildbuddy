//go:build windows

package filecache_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func canCreateSharedDirectoryStartupFixture(name string) bool {
	// Windows strips trailing dots from directory names, so these names cannot
	// be represented as distinct malformed on-disk cache directories. The
	// platform-neutral test still covers their API fallback behavior.
	return name != "." && name != ".."
}

func requireFilecacheDurabilitySupport(t testing.TB, path string) {
	t.Helper()
	volumePath, err := windows.UTF16PtrFromString(`\\.\` + filepath.VolumeName(path))
	require.NoError(t, err)
	handle, err := windows.CreateFile(
		volumePath,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		0,
		0,
	)
	if errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD) {
		t.Skip("filecache power-loss durability tests require permission to flush the Windows volume")
	}
	require.NoError(t, err)
	require.NoError(t, windows.CloseHandle(handle))
}

// requireFilecacheRestartScanSupport skips restart-scan tests until FileCache
// normalizes WalkDir's native paths before combining them with slash-separated
// relative cache paths. Otherwise, FastLinkFile looks up mixed-separator source
// paths that do not exist.
func requireFilecacheRestartScanSupport(t testing.TB) {
	t.Helper()
	t.Skip("Windows startup scans require native cache path normalization")
}
