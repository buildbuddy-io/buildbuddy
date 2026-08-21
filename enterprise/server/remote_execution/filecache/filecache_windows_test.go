//go:build windows

package filecache_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
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
func requireFilecacheRestartScanSupport(t testing.TB) {}

func TestFileCacheRestartScanWindows(t *testing.T) {
	ctx := context.Background()
	// Deliberately use slash separators so this remains a regression test even
	// though MakeTempDir returns a canonical native Windows path.
	cacheRoot := filepath.ToSlash(testfs.MakeTempDir(t))
	workspace := testfs.MakeTempDir(t)
	node := nodeFromString("restart-scan", true)
	sourcePath := writeFileContent(t, workspace, "source", "restart-scan", true)

	fc, err := filecache.NewFileCache(cacheRoot, 100_000, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	require.NoError(t, fc.AddFile(ctx, node, sourcePath))
	require.NoError(t, fc.Close())

	fc, err = filecache.NewFileCache(cacheRoot, 100_000, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fc.Close()) })
	fc.WaitForDirectoryScanToComplete()

	linkedPath := filepath.Join(workspace, "linked")
	require.True(t, fc.FastLinkFile(ctx, node, linkedPath), "startup scan should preserve the cached file")
}
