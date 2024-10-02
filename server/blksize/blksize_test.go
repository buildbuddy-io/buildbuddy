package blksize_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

const fileSize = 4096 * 16

func TestBlockSize_Write(t *testing.T) {
	tmp := testfs.MakeTempDir(t)
	f, err := os.Create(filepath.Join(tmp, "file"))
	require.NoError(t, err)
	defer f.Close()
	err = f.Truncate(fileSize)
	require.NoError(t, err)

	_, err = f.Write([]byte{1})
	require.NoError(t, err)

	require.Equal(t, int64(1), numIOBlocks(t, f.Name()))
}

func TestBlockSize_Mmap(t *testing.T) {
	tmp := testfs.MakeTempDir(t)
	f, err := os.Create(filepath.Join(tmp, "file"))
	require.NoError(t, err)
	defer f.Close()
	err = f.Truncate(fileSize)
	require.NoError(t, err)
	data, err := syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	require.NoError(t, err)
	defer syscall.Munmap(data)

	data[0] = 1

	require.Equal(t, int64(1), numIOBlocks(t, f.Name()))
}

func numIOBlocks(t *testing.T, path string) int64 {
	s := &syscall.Stat_t{}
	err := syscall.Stat(path, s)
	require.NoError(t, err)
	// stat() always uses 512 bytes for its block size, which may not match the
	// IO block size (block size used by the filesystem).
	// See https://askubuntu.com/a/1308745
	statBlocksPerIOBlock := int64(s.Blksize) / 512
	return s.Blocks / statBlocksPerIOBlock
}
