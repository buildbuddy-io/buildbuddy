package fsync_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/fsync"
	"github.com/stretchr/testify/require"
)

func TestSyncPath(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create a file
	err := os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Sync should succeed
	err = fsync.SyncPath(testFile)
	require.NoError(t, err)
}

func TestMkdirAllAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "a", "b", "c")

	err := fsync.MkdirAllAndSync(testPath, 0755)
	require.NoError(t, err)

	// Verify directory exists
	info, err := os.Stat(testPath)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestCreateFileAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	content := []byte("test content")
	err := fsync.CreateFileAndSync(testFile, 0644, bytes.NewReader(content), os.Getuid(), os.Getgid())
	require.NoError(t, err)

	// Verify file contents
	data, err := os.ReadFile(testFile)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestSymlinkAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	target := filepath.Join(tmpDir, "target.txt")
	link := filepath.Join(tmpDir, "link.txt")

	// Create target file
	err := os.WriteFile(target, []byte("test"), 0644)
	require.NoError(t, err)

	// Create symlink
	err = fsync.SymlinkAndSync(target, link)
	require.NoError(t, err)

	// Verify symlink
	linkTarget, err := os.Readlink(link)
	require.NoError(t, err)
	require.Equal(t, target, linkTarget)
}

func TestLinkAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	original := filepath.Join(tmpDir, "original.txt")
	link := filepath.Join(tmpDir, "link.txt")

	// Create original file
	err := os.WriteFile(original, []byte("test"), 0644)
	require.NoError(t, err)

	// Create hard link
	err = fsync.LinkAndSync(original, link)
	require.NoError(t, err)

	// Verify link exists and has same content
	data, err := os.ReadFile(link)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), data)
}

func TestRenameAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	oldPath := filepath.Join(tmpDir, "old.txt")
	newPath := filepath.Join(tmpDir, "new.txt")

	// Create file
	err := os.WriteFile(oldPath, []byte("test"), 0644)
	require.NoError(t, err)

	// Rename
	err = fsync.RenameAndSync(oldPath, newPath)
	require.NoError(t, err)

	// Verify old path doesn't exist
	_, err = os.Stat(oldPath)
	require.True(t, os.IsNotExist(err))

	// Verify new path exists
	data, err := os.ReadFile(newPath)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), data)
}
