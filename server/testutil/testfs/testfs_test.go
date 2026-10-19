package testfs_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestWriteRandomTree(t *testing.T) {
	const numFiles = 500
	dir := testfs.MakeTempDir(t)
	testfs.WriteRandomTree(t, dir, numFiles, 1<<20)

	// The tree has the requested number of entries. Some are hard links,
	// which show up as regular files sharing an inode, and some are symlinks
	// whose targets resolve.
	entries, symlinks := 0, 0
	inodes := map[uint64]bool{}
	var bytes int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			return nil
		}
		entries++
		info, err := os.Lstat(path)
		require.NoError(t, err)
		if info.Mode()&fs.ModeSymlink != 0 {
			symlinks++
			_, err := os.Stat(path)
			require.NoError(t, err, "symlink target of %s", path)
			return nil
		}
		st := info.Sys().(*syscall.Stat_t)
		if !inodes[st.Ino] {
			inodes[st.Ino] = true
			bytes += info.Size()
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, numFiles, entries)
	require.Greater(t, symlinks, 0)
	require.Less(t, len(inodes), entries-symlinks, "expected some hard links")
	require.InDelta(t, 1<<20, bytes, 0.2*(1<<20))

	// The same arguments produce the same tree.
	again := testfs.MakeTempDir(t)
	testfs.WriteRandomTree(t, again, numFiles, 1<<20)
	require.Equal(t, listTree(t, dir), listTree(t, again))
}

// listTree returns each entry of the tree with its type, size and symlink
// target, relative to root.
func listTree(t *testing.T, root string) map[string]string {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		rel, err := filepath.Rel(root, path)
		require.NoError(t, err)
		info, err := os.Lstat(path)
		require.NoError(t, err)
		desc := info.Mode().Type().String()
		if info.Mode().IsRegular() {
			desc += fmt.Sprintf(" %d", info.Size())
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			require.NoError(t, err)
			desc += " " + target
		}
		out[rel] = desc
		return nil
	})
	require.NoError(t, err)
	return out
}
