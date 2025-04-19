package compression

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewZstdDecompressingReader(t *testing.T) {
	blob := "AAAAAAAAAAAAA"
	compressedBlob := CompressZstd(nil, []byte(blob))
	compressedReader := strings.NewReader(string(compressedBlob))
	decompressedReader, err := NewZstdDecompressingReader(io.NopCloser(compressedReader))
	require.NoError(t, err)

	b, err := io.ReadAll(decompressedReader)
	require.NoError(t, err)
	require.Equal(t, blob, string(b))
}

func Test_NewZstdCompressingWriteCloser(t *testing.T) {
	blob := []byte("AAAAAAAAAAAAA")
	compressedBlob := CompressZstd(nil, blob)
	buf := &bytes.Buffer{}
	c, err := NewZstdCompressingWriteCloser(&testWriteCloser{Writer: buf})
	require.NoError(t, err)
	written, err := io.Copy(c, bytes.NewReader(blob))
	require.NoError(t, err)
	require.Equal(t, int64(len(blob)), written)

	err = c.Close()
	require.NoError(t, err)
	require.Equal(t, compressedBlob, buf.Bytes())
}

type testWriteCloser struct {
	io.Writer
}

func (twc *testWriteCloser) Close() error {
	return nil
}
