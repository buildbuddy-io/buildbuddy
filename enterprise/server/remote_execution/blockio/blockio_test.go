package blockio_test

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

const (
	backingFileSizeBytes int64 = 1_000_000
)

func TestFile(t *testing.T) {
	path := makeEmptyBackingFile(t)
	s, err := blockio.NewFile(path)
	require.NoError(t, err)
	testStore(t, s)
}

func TestMmap(t *testing.T) {
	path := makeEmptyBackingFile(t)
	s, err := blockio.NewMmap(path)
	require.NoError(t, err)
	testStore(t, s)
}

func makeEmptyBackingFile(t *testing.T) string {
	root := testfs.MakeTempDir(t)
	path := filepath.Join(root, "f")
	err := os.WriteFile(path, make([]byte, backingFileSizeBytes), 0644)
	require.NoError(t, err, "write empty file")
	return path
}

func testStore(t *testing.T, s blockio.Store) {
	size, err := s.SizeBytes()
	require.NoError(t, err, "SizeBytes failed")
	require.Equal(t, backingFileSizeBytes, size, "unexpected SizeBytes")

	expectedContent := make([]byte, int(size))
	buf := make([]byte, int(size))
	for i := 0; i < 100; i++ {
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
	err = s.Close()
	require.NoError(t, err, "Close failed")
}
