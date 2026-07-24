package firecracker

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/stretchr/testify/require"
)

func TestObserve(t *testing.T) {
	var finalized []int64
	tracker := &sequentialChunkWriteTracker{
		chunkSizeBytes: 1,
		finalizeChunk: func(chunkOffset int64) error {
			finalized = append(finalized, chunkOffset)
			return nil
		},
	}

	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 0, Length: 50, ChunkIndex: 0}))
	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 50, Length: 25, ChunkIndex: 0}))
	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 100, Length: 20, ChunkIndex: 100}))
	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 300, Length: 10, ChunkIndex: 300}))
	require.NoError(t, tracker.Finish())

	require.Equal(t, []int64{0, 100, 300}, finalized)
}

func TestObserve_RejectsNonMonotonicWrites(t *testing.T) {
	tracker := &sequentialChunkWriteTracker{
		chunkSizeBytes: 1,
		finalizeChunk: func(chunkOffset int64) error {
			return nil
		},
	}

	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 100, Length: 10, ChunkIndex: 100}))
	require.Error(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 50, Length: 10, ChunkIndex: 0}))
}
