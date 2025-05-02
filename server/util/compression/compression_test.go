package compression_test

import (
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// CompressZstd -> DecompressZstd, NewZstdDecompressor, NewZStdDecompressingReader
// NewZstdCompressingReader -> DecompressZstd, NewZstdDecompressor, NewZStdDecompressingReader

func TestLossless_CompressZstd(t *testing.T) {
	src := []byte(strings.Repeat("Please compress me.", 1024))
	dst := make([]byte, len(src))
	compressed := compression.CompressZstd(dst, src)
	require.LessOrEqual(t, len(compressed), len(src))

	decompressed := make([]byte, len(src))
	decompressed, err := compression.DecompressZstd(decompressed, compressed)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(src, decompressed))
}

func Test_NewZstdDecompressingReader(t *testing.T) {
	blob := "AAAAAAAAAAAAA"
	compressedBlob := compression.CompressZstd(nil, []byte(blob))
	compressedReader := strings.NewReader(string(compressedBlob))
	decompressedReader, err := compression.NewZstdDecompressingReader(io.NopCloser(compressedReader))
	require.NoError(t, err)

	b, err := io.ReadAll(decompressedReader)
	require.NoError(t, err)
	require.Equal(t, blob, string(b))
}
