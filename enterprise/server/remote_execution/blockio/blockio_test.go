package blockio_test

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/boljen/go-bitmap"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

const (
	backingFileSizeBytes int64 = 8192
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

func TestOverlayBasic(t *testing.T) {
	basePath := makeEmptyTempFile(t, backingFileSizeBytes)
	base, err := blockio.NewMmap(basePath)
	require.NoError(t, err)
	topPath := makeEmptyTempFile(t, backingFileSizeBytes)
	top, err := blockio.NewMmap(topPath)
	opts := &blockio.OverlayOpts{}
	s, err := blockio.NewOverlay(base, top, nil /*=dirty*/, opts)
	require.NoError(t, err)
	// Use the top layer path for validating the expected store contents. Since
	// both layers are initially empty and all writes should be applied only to
	// the top layer, the top layer should contain the expected contents for the
	// store.
	testStore(t, s, topPath)
	// Make sure the bottom layer was unchanged.
	b, err := os.ReadFile(basePath)
	require.NoError(t, err)
	require.Equal(t, make([]byte, backingFileSizeBytes), b)
}

func TestOverlayEdgeCases(t *testing.T) {
	// Run a bunch of tests where we write
	for _, test := range []struct {
		name string
		// Base layer starting contents
		base []byte
		// Block size
		blockSize int64
		// Bytes to be written (contents and offset)
		writeBuf []byte
		writeOff int64
		// Expected contents for the overlay store (combined view of base + top)
		expectedContent []byte
		// Expected contents for the top layer
		expectedTop []byte
	}{
		{
			name:      "PartialBlockWriteAtStart",
			blockSize: 2,
			base: []byte{
				10, 11,
				12, 13,
			},
			writeBuf: []byte{101}, writeOff: 1,
			expectedContent: []byte{
				10, 101, // base block 0 with partial overwrite
				12, 13, // base block 1 unmodified
			},
			expectedTop: []byte{
				10, 101, // base block 0 with partial overwrite
				0, 0, // block 1 still empty
			},
		},
		{
			name:      "PartialBlockWriteAtEnd",
			blockSize: 2,
			base: []byte{
				10, 11,
				12, 13,
			},
			writeBuf: []byte{102}, writeOff: 2,
			expectedContent: []byte{
				10, 11, // base block 0 unmodified
				102, 13, // base block 1 with partial overwrite
			},
			expectedTop: []byte{
				0, 0, // block 0 empty
				102, 13, // base block 1 with partial overwrite
			},
		},
		{
			name:      "PartialBlockWriteAtStartAndEndSpanningMultipleBlocks",
			blockSize: 2,
			base: []byte{
				10, 11,
				12, 13,
				14, 15,
			},
			writeBuf: []byte{101, 102, 103, 104}, writeOff: 1,
			expectedContent: []byte{
				10, 101, // base block 0 with partial overwrite
				102, 103, // block 1 completely overwritten
				104, 15, // base block 2 with partial overwrite
			},
			expectedTop: []byte{
				10, 101, // base block 0 with partial overwrite
				102, 103, // block 1 completely overwritten
				104, 15, // base block 2 with partial overwrite
			},
		},
		{
			name:      "PartialBlockWriteAtStartAndEndSpanningOneBlock",
			blockSize: 4,
			base: []byte{
				10, 11, 12, 13,
			},
			writeBuf: []byte{101, 102}, writeOff: 1,
			expectedContent: []byte{
				10, 101, 102, 13, // base block 0 with partial overwrite
			},
			expectedTop: []byte{
				10, 101, 102, 13, // base block 0 with partial overwrite
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			basePath := makeTempFile(t, test.base)
			base, err := blockio.NewMmap(basePath)
			require.NoError(t, err)
			topPath := makeEmptyTempFile(t, int64(len(test.base)))
			top, err := blockio.NewMmap(topPath)
			require.NoError(t, err)
			opts := &blockio.OverlayOpts{BlockSizeBytes: test.blockSize}
			overlay, err := blockio.NewOverlay(base, top, nil /*=dirty*/, opts)

			// Make sure we can write without error
			n, err := overlay.WriteAt(test.writeBuf, test.writeOff)

			require.NoError(t, err)
			require.Equal(t, len(test.writeBuf), n)

			// Read back the full combined contents, make sure they match what
			// we expect.
			b := make([]byte, len(test.base))
			n, err = overlay.ReadAt(b, 0)

			require.NoError(t, err)
			require.Equal(t, len(b), n)
			require.Equal(t, test.expectedContent, b)

			// Sync any in-memory contents
			err = overlay.Sync()
			require.NoError(t, err)
			bb, err := os.ReadFile(basePath)
			require.NoError(t, err)
			require.Equal(t, test.base, bb, "base file should be unmodified")
			tb, err := os.ReadFile(topPath)
			require.NoError(t, err)
			require.Equal(t, test.expectedTop, tb, "top file should match expected contents")
		})
	}
}

func TestOverlayRestore(t *testing.T) {
	// Set up an overlay with one dirty block.
	const blockSizeBytes = 2
	baseBytes := []byte{
		10, 11,
		12, 13,
	}
	topBytes := []byte{
		0, 0, // block 0 clean
		102, 103, // block 1 dirty
	}
	expectedContent := []byte{
		10, 11, // block 0 from base
		102, 103, // block 1 from top
	}
	dirty := bitmap.New(2) // 2 blocks total, so we need 2 dirty tracking bits
	dirty.Set(1, true)     // mark block 1 dirty
	basePath := makeTempFile(t, baseBytes)
	base, err := blockio.NewMmap(basePath)
	require.NoError(t, err)
	topPath := makeTempFile(t, topBytes)
	top, err := blockio.NewMmap(topPath)
	require.NoError(t, err)
	opts := &blockio.OverlayOpts{BlockSizeBytes: blockSizeBytes}
	overlay, err := blockio.NewOverlay(base, top, dirty, opts)
	require.NoError(t, err)

	// Read back the full overlay contents, should match what we expect.
	b := make([]byte, len(baseBytes))
	n, err := overlay.ReadAt(b, 0)

	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, expectedContent, b)
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
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, expectedContent, b)

	err = s.Close()
	require.NoError(t, err, "Close failed")
}
