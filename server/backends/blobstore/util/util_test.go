package util_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

var testBlob = []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 10_000))

func TestCompressDecompressRoundTrip(t *testing.T) {
	for _, zstdEnabled := range []bool{false, true} {
		flags.Set(t, "storage.zstd_compression", zstdEnabled)

		compressed, err := util.Compress(testBlob)
		require.NoError(t, err)
		require.Less(t, len(compressed), len(testBlob))

		decompressed, err := util.Decompress(compressed, nil)
		require.NoError(t, err)
		require.Equal(t, testBlob, decompressed)
	}
}

func TestStreamingRoundTrip(t *testing.T) {
	for _, zstdEnabled := range []bool{false, true} {
		flags.Set(t, "storage.zstd_compression", zstdEnabled)

		var buf bytes.Buffer
		zw := util.NewCompressWriter(&buf)
		_, err := zw.Write(testBlob)
		require.NoError(t, err)
		require.NoError(t, zw.Close())
		require.Less(t, buf.Len(), len(testBlob))

		zr, err := util.NewCompressReader(&buf)
		require.NoError(t, err)
		decompressed, err := io.ReadAll(zr)
		require.NoError(t, err)
		require.NoError(t, zr.Close())
		require.Equal(t, testBlob, decompressed)
	}
}

// Blobs written in one format must remain readable after the write format
// changes, in both directions.
func TestCrossFormatReads(t *testing.T) {
	for _, writeZstd := range []bool{false, true} {
		flags.Set(t, "storage.zstd_compression", writeZstd)
		compressed, err := util.Compress(testBlob)
		require.NoError(t, err)

		for _, readZstd := range []bool{false, true} {
			flags.Set(t, "storage.zstd_compression", readZstd)

			decompressed, err := util.Decompress(compressed, nil)
			require.NoError(t, err)
			require.Equal(t, testBlob, decompressed)

			zr, err := util.NewCompressReader(bytes.NewReader(compressed))
			require.NoError(t, err)
			streamed, err := io.ReadAll(zr)
			require.NoError(t, err)
			require.NoError(t, zr.Close())
			require.Equal(t, testBlob, streamed)
		}
	}
}

// Records written before compression was enabled are stored uncompressed and
// must be returned as-is.
func TestUncompressedLegacyReads(t *testing.T) {
	decompressed, err := util.Decompress(testBlob, nil)
	require.NoError(t, err)
	require.Equal(t, testBlob, decompressed)

	zr, err := util.NewCompressReader(bytes.NewReader(testBlob))
	require.NoError(t, err)
	streamed, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.NoError(t, zr.Close())
	require.Equal(t, testBlob, streamed)
}

func TestShortAndEmptyInputs(t *testing.T) {
	for _, in := range [][]byte{{}, []byte("a"), []byte("abc")} {
		decompressed, err := util.Decompress(in, nil)
		require.NoError(t, err)
		require.Equal(t, in, decompressed)

		zr, err := util.NewCompressReader(bytes.NewReader(in))
		require.NoError(t, err)
		streamed, err := io.ReadAll(zr)
		require.NoError(t, err)
		require.NoError(t, zr.Close())
		require.Equal(t, in, streamed)
	}
}
