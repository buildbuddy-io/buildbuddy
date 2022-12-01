package compression

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewZstdDecompressingReader(t *testing.T) {
	blob := "AAAAAAAAAAAAA"
	compressedBlob := CompressZstd(nil, []byte(blob))
	compressedReader := strings.NewReader(string(compressedBlob))
	decompressedReader, err := NewZstdDecompressingReader(compressedReader)
	require.NoError(t, err)

	b, err := io.ReadAll(decompressedReader)
	require.NoError(t, err)
	require.Equal(t, blob, string(b))
}
