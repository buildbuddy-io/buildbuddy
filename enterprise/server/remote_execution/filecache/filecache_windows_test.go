//go:build windows

package filecache_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

const devDrivePath = "E:"

func TestFileCacheUncleanShutdownStartupOnWindows(t *testing.T) {
	flags.Set(t, "executor.delete_filecache_on_unclean_shutdown", true)

	rootDir := windowsFileCacheRoot(t)
	require.NoError(t, os.MkdirAll(rootDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(rootDir, "filecache.dirty"), nil, 0644))

	fc, err := filecache.NewFileCache(rootDir, 100_000, false)
	require.NoError(t, err)
	require.NotNil(t, fc)
	t.Cleanup(func() { require.NoError(t, fc.Close()) })
	fc.WaitForDirectoryScanToComplete()
}

func windowsFileCacheRoot(t *testing.T) string {
	rootDir, err := os.MkdirTemp(devDrivePath, "buildbuddy-filecache-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(rootDir)) })
	return filepath.Join(rootDir, "cache")
}
