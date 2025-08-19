package compression_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
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
		for i := 1; i <= 5; i++ {
			srclen := int(math.Pow10(i))
			name := tc.name + "_" + strconv.Itoa(srclen) + "_bytes"
			t.Run(name, func(t *testing.T) {
				_, r := testdigest.NewReader(t, int64(srclen))
				src, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, srclen, len(src))
				compressed := tc.compress(t, src)

				decompressed := tc.decompress(t, len(src), compressed)
				require.Empty(t, cmp.Diff(src, decompressed))
			})
		}
	}
}

func compressWithCompressZstd(t *testing.T, src []byte) []byte {
	dst := make([]byte, len(src))
	compressed := compression.CompressZstd(dst, src)
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

func TestCompressingReader_EmptyReadBuf(t *testing.T) {
	_, r := testdigest.NewReader(t, 16)
	zrc, err := compression.NewZstdCompressingReader(io.NopCloser(r), make([]byte, 0), make([]byte, 16))
	require.Error(t, err)
	require.Nil(t, zrc)
}

func TestCompressingReader_BufferSizes(t *testing.T) {
	for _, totalBytes := range []int{9, 99, 999} {
		for _, readBufSize := range []int{1, 63, 64, 65, 128} {
			for _, compressBufSize := range []int{1, 63, 64, 65, 128} {
				for _, pSize := range []int{1, 63, 64, 65, 128} {
					name := fmt.Sprintf("%d_total_bytes_%d_read_buf_%d_compress_buf_%d_p", totalBytes, readBufSize, compressBufSize, pSize)
					t.Run(name, func(t *testing.T) {
						_, in := testdigest.RandomCASResourceBuf(t, int64(totalBytes))
						zrc, err := compression.NewZstdCompressingReader(
							io.NopCloser(bytes.NewReader(in)),
							make([]byte, readBufSize),
							make([]byte, compressBufSize),
						)
						require.NoError(t, err)
						p := make([]byte, pSize)
						var compressed bytes.Buffer
						for {
							n, err := zrc.Read(p)
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							require.LessOrEqual(t, n, len(p))
							_, err = compressed.Write(p[:n])
							require.NoError(t, err)
						}
						decompressed := make([]byte, totalBytes)
						decompressed, err = compression.DecompressZstd(decompressed, compressed.Bytes())
						require.NoError(t, err)
						require.Empty(t, cmp.Diff(in, decompressed))
					})
				}
			}
		}
	}
}

type erroringReader struct {
	inputReader   io.Reader
	bytesToAllow  int
	errorToReturn error

	bytesRead int
}

func (er *erroringReader) Read(p []byte) (int, error) {
	n, err := er.inputReader.Read(p)
	if err != nil {
		return n, err
	}
	er.bytesRead += n
	if er.bytesRead > er.bytesToAllow {
		return n, er.errorToReturn
	}
	return n, nil
}

// TestCompressingReader_BufferSizes tests that a CompressingReader
// will hold onto any error from its underlying reader until it has exhausted
// all the successfully-read bytes.
func TestCompressingReader_HoldErrors(t *testing.T) {
	totalBytes := 65
	bytesToAllow := 33
	errorToReturn := io.ErrUnexpectedEOF
	readBufSize := 64
	compressBufSize := 64

	in := make([]byte, totalBytes)
	for i := range totalBytes {
		in[i] = byte(i % 256)
	}
	er := &erroringReader{
		inputReader:   bytes.NewReader(in),
		bytesToAllow:  bytesToAllow,
		errorToReturn: errorToReturn,
	}
	zrc, err := compression.NewZstdCompressingReader(
		io.NopCloser(er),
		make([]byte, readBufSize),
		make([]byte, compressBufSize),
	)
	require.NoError(t, err)

	p := make([]byte, 8)
	for i := 0; i < 4; i++ {
		n, err := zrc.Read(p)
		require.NoError(t, err)
		require.Equal(t, len(p), n)
	}

	p = make([]byte, 64)
	_, err = zrc.Read(p)
	require.Error(t, err)
	require.ErrorIs(t, err, errorToReturn)
}
