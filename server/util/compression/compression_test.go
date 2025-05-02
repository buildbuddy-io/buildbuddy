package compression_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestLossless(t *testing.T) {
	for _, tc := range []struct {
		name       string
		compress   func(*testing.T, []byte) []byte
		decompress func(*testing.T, int, []byte) []byte
	}{
		{
			name:       "CompressZstd -> DecompressZstd",
			compress:   compressWithCompressZstd,
			decompress: decompressWithDecompressZstd,
		},
		{
			name:       "CompressZstd -> NewZstdDecompressor",
			compress:   compressWithCompressZstd,
			decompress: decompressWithNewZstdDecompressor,
		},
		{
			name:       "CompressZstd -> NewZstdDecompressingReader",
			compress:   compressWithCompressZstd,
			decompress: decompressWithNewZstdDecompressingReader,
		},
		{
			name:       "NewZstdCompressingReader -> DecompressZstd",
			compress:   compressWithNewZstdCompressingReader,
			decompress: decompressWithDecompressZstd,
		},
		{
			name:       "NewZstdCompressingReader -> NewZstdDecompressor",
			compress:   compressWithNewZstdCompressingReader,
			decompress: decompressWithNewZstdDecompressor,
		},
		{
			name:       "NewZstdCompressingReader -> NewZstdDecompressingReader",
			compress:   compressWithNewZstdCompressingReader,
			decompress: decompressWithNewZstdDecompressingReader,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := []byte(strings.Repeat("Please compress me.", 1024))
			compressed := tc.compress(t, src)

			decompressed := tc.decompress(t, len(src), compressed)
			require.Empty(t, cmp.Diff(src, decompressed))
		})
	}
}

func compressWithCompressZstd(t *testing.T, src []byte) []byte {
	dst := make([]byte, len(src))
	compressed := compression.CompressZstd(dst, src)
	require.LessOrEqual(t, len(compressed), len(src))
	return compressed
}

func compressWithNewZstdCompressingReader(t *testing.T, src []byte) []byte {
	readBuf := make([]byte, len(src))
	compressBuf := make([]byte, len(src))
	rc := io.NopCloser(bytes.NewReader(src))
	c, err := compression.NewZstdCompressingReader(rc, readBuf, compressBuf)
	require.NoError(t, err)

	compressed, err := io.ReadAll(c)
	require.NoError(t, err)
	err = c.Close()
	require.NoError(t, err)
	return compressed
}

func decompressWithDecompressZstd(t *testing.T, srclen int, compressed []byte) []byte {
	decompressed := make([]byte, srclen)
	decompressed, err := compression.DecompressZstd(decompressed, compressed)
	require.NoError(t, err)
	return decompressed
}

func decompressWithNewZstdDecompressor(t *testing.T, _ int, compressed []byte) []byte {
	buf := &bytes.Buffer{}
	d, err := compression.NewZstdDecompressor(buf)
	require.NoError(t, err)
	n, err := d.Write(compressed)
	require.NoError(t, err)
	require.Equal(t, len(compressed), n)
	err = d.Close()
	require.NoError(t, err)
	return buf.Bytes()
}

func decompressWithNewZstdDecompressingReader(t *testing.T, srclen int, compressed []byte) []byte {
	rc := io.NopCloser(bytes.NewReader(compressed))
	d, err := compression.NewZstdDecompressingReader(rc)
	require.NoError(t, err)
	buf := make([]byte, srclen)
	n, err := d.Read(buf)
	require.NoError(t, err)
	require.Equal(t, srclen, n)
	err = d.Close()
	require.NoError(t, err)
	err = rc.Close()
	require.NoError(t, err)
	return buf
}
