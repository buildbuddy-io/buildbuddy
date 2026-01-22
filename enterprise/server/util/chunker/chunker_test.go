package chunker_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker"
	"github.com/stretchr/testify/require"
)

func TestChunker_ReassemblesOriginalData(t *testing.T) {
	ctx := context.Background()

	const dataSize = 1 << 20 // 1MB
	originalData := make([]byte, dataSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err)

	var chunks [][]byte
	writeChunkFn := func(data []byte) error {
		// Copy since the underlying buffer may be reused.
		chunk := make([]byte, len(data))
		copy(chunk, data)
		chunks = append(chunks, chunk)
		return nil
	}

	const averageSize = 64 * 1024 // 64KB
	c, err := chunker.New(ctx, averageSize, writeChunkFn)
	require.NoError(t, err)

	_, err = c.Write(originalData)
	require.NoError(t, err)

	err = c.Close()
	require.NoError(t, err)

	require.Greater(t, len(chunks), 1, "expected multiple chunks for 1MB of data")

	minSize := averageSize / 4
	maxSize := averageSize * 4
	for i, chunk := range chunks {
		if i < len(chunks)-1 {
			require.GreaterOrEqual(t, len(chunk), minSize, "chunk %d is smaller than min size", i)
		}
		require.LessOrEqual(t, len(chunk), maxSize, "chunk %d is larger than max size", i)
	}

	var reassembled bytes.Buffer
	for _, chunk := range chunks {
		reassembled.Write(chunk)
	}
	require.Equal(t, originalData, reassembled.Bytes(), "reassembled data should match original")
}

func TestChunker_DeterministicChunking(t *testing.T) {
	ctx := context.Background()

	const dataSize = 256 * 1024 // 256KB
	originalData := make([]byte, dataSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err)

	const averageSize = 16 * 1024 // 16KB

	var runs [2][][]byte
	for run := 0; run < 2; run++ {
		var chunks [][]byte
		writeChunkFn := func(data []byte) error {
			chunk := make([]byte, len(data))
			copy(chunk, data)
			chunks = append(chunks, chunk)
			return nil
		}

		c, err := chunker.New(ctx, averageSize, writeChunkFn)
		require.NoError(t, err)

		_, err = c.Write(originalData)
		require.NoError(t, err)

		err = c.Close()
		require.NoError(t, err)

		runs[run] = chunks
	}

	require.Equal(t, len(runs[0]), len(runs[1]), "should produce the same number of chunks")
	for i := range runs[0] {
		require.Equal(t, runs[0][i], runs[1][i], "chunk %d should be identical across runs", i)
	}
}

func TestChunker_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	writeChunkFn := func(data []byte) error {
		return nil
	}

	const averageSize = 16 * 1024
	c, err := chunker.New(ctx, averageSize, writeChunkFn)
	require.NoError(t, err)

	cancel()

	largeData := make([]byte, 1<<20)
	_, err = c.Write(largeData)
	require.ErrorIs(t, err, context.Canceled)

	err = c.Close()
	require.ErrorIs(t, err, context.Canceled)
}
