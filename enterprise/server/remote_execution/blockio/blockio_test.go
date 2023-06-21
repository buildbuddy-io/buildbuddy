package blockio_test

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/stretchr/testify/require"
)

const (
	backingFileSizeBytes int64 = 32 * 1024
)

func TestFile(t *testing.T) {
	path := makeEmptyTempFile(t, backingFileSizeBytes)
	s, err := blockio.NewFile(path)
	require.NoError(t, err)
	testStore(t, s, path)
}

func TestMmap(t *testing.T) {
	path := makeEmptyTempFile(t, backingFileSizeBytes)
	s, err := blockio.NewMmap(path)
	require.NoError(t, err)
	testStore(t, s, path)
}

func TestCOW_Basic(t *testing.T) {
	path := makeEmptyTempFile(t, backingFileSizeBytes)
	dataDir := testfs.MakeTempDir(t)
	chunkSizeBytes := backingFileSizeBytes / 2
	s, err := blockio.ConvertFileToCOW(path, chunkSizeBytes, dataDir)
	require.NoError(t, err)
	// Don't validate against the backing file, since COWFromFile makes a copy
	// of the underlying file.
	testStore(t, s, "" /*=path*/)
}

func TestCOW_SparseData(t *testing.T) {
	if runtime.GOOS == "darwin" {
		// Sparse files work a bit differently on macOS, just skip for now.
		t.SkipNow()
	}

	// Figure out the IO block size (number of bytes transferred to/from disk
	// for each IO operation). This is the minimum seek size when using seek()
	// with SEEK_DATA.
	tmp := testfs.MakeTempDir(t)
	stat, err := os.Stat(tmp)
	require.NoError(t, err)
	ioBlockSize := int64(stat.Sys().(*syscall.Stat_t).Blksize)
	// Use a chunk size that is a few times larger than the IO block size.
	const blocksPerChunk = 4
	chunkSize := ioBlockSize * blocksPerChunk
	// Write a file consisting of the following chunks:
	// - 0: completely empty
	// - 1: data block at the beginning of the chunk
	// - 2: data block somewhere in the middle of the chunk
	// - 3: data block at the end of the chunk
	chunks := make([][]byte, 4)
	for i := 0; i < len(chunks); i++ {
		chunks[i] = make([]byte, chunkSize)
	}
	// chunkData[0]: empty
	chunks[1][0] = 1
	chunks[2][ioBlockSize] = 1
	chunks[3][chunkSize-1] = 1
	data := concatBytes(chunks...)
	dataFilePath := filepath.Join(tmp, "data.bin")
	writeSparseFile(t, dataFilePath, data, ioBlockSize)
	outDir := testfs.MakeTempDir(t)

	// Now split the file.
	c, err := blockio.ConvertFileToCOW(dataFilePath, chunkSize, outDir)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	// Make sure the full content matches our buffer.
	dataOut := make([]byte, len(data))
	n, err := c.ReadAt(dataOut, 0)
	require.NoError(t, err)
	require.Equal(t, len(dataOut), n)

	// Inspect the chunk files and ensure they have the expected physical size.
	for i := 0; i < len(chunks); i++ {
		chunkPath := filepath.Join(outDir, strconv.Itoa(i*int(chunkSize)))
		// We wrote one data block per chunk except for the one chunk that was
		// all empty. The empty chunk should not have written a file.
		if i == 0 {
			exists, err := disk.FileExists(context.TODO(), chunkPath)
			require.NoError(t, err)
			require.False(t, exists, "chunk 0 should not exist on disk")
			continue
		}
		nb := numIOBlocks(t, chunkPath)
		require.Equal(t, int64(1), nb, "chunk %d IO block count", i)
	}

	// Now write a single data byte to the empty chunk (0), and sync it to disk.
	n, err = c.WriteAt([]byte{1}, 0)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	err = c.Chunks[0].Sync()
	require.NoError(t, err)

	// Make sure we wrote a dirty chunk with the expected contents, and only a
	// single IO block on disk (since we only wrote 1 byte).
	dirtyPath := filepath.Join(outDir, "0.dirty")
	b, err := os.ReadFile(dirtyPath)
	expectedContent := make([]byte, len(chunks[0]))
	expectedContent[0] = 1
	require.Equal(t, expectedContent, b)
	require.Equal(t, int64(1), numIOBlocks(t, dirtyPath))
}

func testStore(t *testing.T, s blockio.Store, path string) {
	size, err := s.SizeBytes()
	require.NoError(t, err, "SizeBytes failed")
	require.Equal(t, backingFileSizeBytes, size, "unexpected SizeBytes")

	expectedContent := make([]byte, int(size))
	buf := make([]byte, int(size))
	n := 1 + rand.Intn(50)
	for i := 0; i < n; i++ {
		// With equal probability, either (a) read a random range and make sure
		// it matches expectedContent, or (b) write a random range and update
		// our expectedContent for subsequent reads.
		offset := rand.Int63n(int64(len(expectedContent)))
		length := rand.Int63n(int64(len(expectedContent)) - offset)
		if rand.Float64() > 0.5 {
			n, err := s.ReadAt(buf[:length], offset)
			require.NoError(t, err)
			require.Equal(t, length, int64(n))
			require.Equal(t, expectedContent[offset:offset+length], buf[:length])
		} else {
			_, err := rand.Read(buf[:length])
			require.NoError(t, err)

			n, err := s.WriteAt(buf[:length], offset)
			require.NoError(t, err)
			require.Equal(t, length, int64(n))

			copy(expectedContent[offset:offset+length], buf[:length])
		}
	}
	// Make sure we can sync and close without error.
	err = s.Sync()
	require.NoError(t, err, "Sync failed")

	// Read back the raw backing file using read(2); it should match our
	// expected contents. This is especially important for testing mmap, since
	// Sync() should guarantee that the contents are flushed from memory back
	// to disk.
	if path != "" {
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, expectedContent, b)
	}

	err = s.Close()
	require.NoError(t, err, "Close failed")
}

func makeTempFile(t *testing.T, content []byte) string {
	root := testfs.MakeTempDir(t)
	path := filepath.Join(root, "f")
	err := os.WriteFile(path, content, 0644)
	require.NoError(t, err, "write empty file")
	return path
}

func makeEmptyTempFile(t *testing.T, sizeBytes int64) string {
	return makeTempFile(t, make([]byte, sizeBytes))
}

// writeSparseFile writes only the data blocks from b to the given path, so
// that the physical size of the file is minimal while still representing the
// same underlying bytes.
func writeSparseFile(t *testing.T, path string, b []byte, ioBlockSize int64) {
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	// Truncate to ensure the file has the correct size in case it ends with one
	// or more empty blocks.
	err = f.Truncate(int64(len(b)))
	require.NoError(t, err)
	for off := int64(0); off < int64(len(b)); off += ioBlockSize {
		data := b[off:]
		if int64(len(data)) > ioBlockSize {
			data = data[:ioBlockSize]
		}
		if blockio.IsEmptyOrAllZero(data) {
			continue
		}
		_, err := f.WriteAt(data, off)
		require.NoError(t, err)
	}
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

func concatBytes(chunks ...[]byte) []byte {
	var out []byte
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}
