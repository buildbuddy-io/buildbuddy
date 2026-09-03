//go:build windows

package filecache_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

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
