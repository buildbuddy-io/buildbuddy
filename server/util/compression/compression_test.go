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
		{
			name:       "NewZstdCompressingWriter -> DecompressZstd",
			compress:   compressWithNewZstdCompressingWriter,
			decompress: decompressWithDecompressZstd,
		},
		{
			name:       "NewZstdCompressingWriter -> NewZstdDecompressor",
			compress:   compressWithNewZstdCompressingWriter,
			decompress: decompressWithNewZstdDecompressor,
		},
		{
			name:       "NewZstdCompressingWriter -> NewZstdDecompressingReader",
			compress:   compressWithNewZstdCompressingWriter,
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

func compressWithNewZstdCompressingWriter(t *testing.T, src []byte) []byte {
	var out bytes.Buffer
	w, err := compression.NewZstdCompressingWriter(&out, 10*int64(len(src)))
	require.NoError(t, err)
	n, err := w.Write(src)
	require.NoError(t, err)
	require.Equal(t, len(src), n)
	require.NoError(t, w.Close())
	return out.Bytes()
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

// TestCompressingReader_HoldErrors tests that a CompressingReader
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

func TestCompressingWriter_EmptyBuffer(t *testing.T) {
	_, err := compression.NewZstdCompressingWriter(nil, 0)
	require.Error(t, err)
}

func TestCompressingWriter_BufferSizes(t *testing.T) {
	src := []byte{1, 2, 3, 4, 5}
	size := int64(len(src))
	for _, bufSize := range []int64{1, size - 1, size, size + 1, 2 * size} {
		var out bytes.Buffer
		w, err := compression.NewZstdCompressingWriter(&out, bufSize)
		require.NoError(t, err)
		n, err := w.Write(src)
		require.NoError(t, err)
		require.Equal(t, len(src), n)
		if bufSize > int64(len(src)) {
			// If the buffer is bigger than the source, we should not have
			// written anything yet.
			require.Zero(t, w.CompressedBytesWritten)
		} else {
			// We should have skipped the buffer and wrote directly.
			require.Greater(t, w.CompressedBytesWritten, 0)
		}
		require.NoError(t, w.Close())
		require.Greater(t, w.CompressedBytesWritten, 0)

		decompressed := decompressWithDecompressZstd(t, len(src), out.Bytes())
		require.Equal(t, src, decompressed)
	}
}

func TestCompressingWriter_Flush(t *testing.T) {
	src := []byte{1, 2, 3, 4, 5}
	var out bytes.Buffer
	w, err := compression.NewZstdCompressingWriter(&out, 2*int64(len(src)))
	require.NoError(t, err)
	n, err := w.Write(src)
	require.NoError(t, err)
	require.Equal(t, len(src), n)
	// Check that we didn't write anything yet.
	require.Zero(t, w.CompressedBytesWritten)

	err = w.Flush()
	require.NoError(t, err)
	// Check that we wrote something on Flush.
	require.Greater(t, w.CompressedBytesWritten, 0)

	require.NoError(t, w.Close())

	decompressed := decompressWithDecompressZstd(t, len(src), out.Bytes())
	require.Equal(t, src, decompressed)
}

func TestCompressingWriter_ReadFrom(t *testing.T) {
	src := []byte{1, 2, 3, 4, 5}
	var out bytes.Buffer
	w, err := compression.NewZstdCompressingWriter(&out, int64(len(src)))
	require.NoError(t, err)

	// Write 2 bytes with Write
	n, err := w.Write(src[:2])
	require.NoError(t, err)
	require.Equal(t, 2, n)
	// Check that we didn't write anything yet.
	require.Zero(t, w.CompressedBytesWritten)

	// Write the rest with ReadFrom
	n2, err := w.ReadFrom(bytes.NewReader(src[2:]))
	require.NoError(t, err)
	require.Equal(t, 3, int(n2))

	// Check that ReadFrom flushed.
	require.Greater(t, w.CompressedBytesWritten, 0)
	require.NoError(t, w.Close())

	decompressed := decompressWithDecompressZstd(t, len(src), out.Bytes())
	require.Equal(t, src, decompressed)
}

func TestCompressingWriter_HoldsErrors(t *testing.T) {
	src := []byte{1, 2, 3, 4, 5}
	out := &erroringWriter{}
	w, err := compression.NewZstdCompressingWriter(out, int64(len(src)))
	require.NoError(t, err)
	_, err = w.Write(src)
	require.Equal(t, "error #1", err.Error())
	_, err = w.Write(src)
	require.Equal(t, "error #1", err.Error())
	err = w.Flush()
	require.Equal(t, "error #1", err.Error())
	_, err = w.ReadFrom(nil)
	require.Equal(t, "error #1", err.Error())
	err = w.Close()
	require.Equal(t, "error #1", err.Error())
}

type erroringWriter struct {
	errCount int
}

func (ew *erroringWriter) Write(p []byte) (int, error) {
	ew.errCount++
	return 0, fmt.Errorf("error #%d", ew.errCount)
}
