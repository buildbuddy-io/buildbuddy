package firecracker

import (
	"errors"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/stretchr/testify/require"
)

func TestSequentialChunkWriteTracker(t *testing.T) {
	var finalized []int64
	tracker := &sequentialChunkWriteTracker{
		chunkSizeBytes: 100,
		finalizeChunk: func(chunkOffset int64) error {
			finalized = append(finalized, chunkOffset)
			return nil
		},
	}

	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 0, Length: 50, ChunkOffset: 0}))
	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 50, Length: 25, ChunkOffset: 0}))
	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 100, Length: 20, ChunkOffset: 100}))
	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 300, Length: 10, ChunkOffset: 300}))
	require.NoError(t, tracker.Finish())

	require.Equal(t, []int64{0, 100, 300}, finalized)
}

func TestSequentialChunkWriteTrackerRejectsNonMonotonicWrites(t *testing.T) {
	tracker := &sequentialChunkWriteTracker{
		chunkSizeBytes: 100,
		finalizeChunk: func(chunkOffset int64) error {
			return nil
		},
	}

	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 100, Length: 10, ChunkOffset: 100}))
	require.Error(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 50, Length: 10, ChunkOffset: 0}))
}

func TestSequentialChunkWriteTrackerRejectsOutOfBoundsChunkEvent(t *testing.T) {
	tracker := &sequentialChunkWriteTracker{
		chunkSizeBytes: 100,
		finalizeChunk: func(chunkOffset int64) error {
			return nil
		},
	}

	require.Error(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 90, Length: 20, ChunkOffset: 0}))
}

func TestSequentialChunkWriteTrackerPropagatesFinalizeError(t *testing.T) {
	finalizeErr := errors.New("finalize failed")
	tracker := &sequentialChunkWriteTracker{
		chunkSizeBytes: 100,
		finalizeChunk: func(chunkOffset int64) error {
			return finalizeErr
		},
	}

	require.NoError(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 0, Length: 10, ChunkOffset: 0}))
	require.ErrorIs(t, tracker.Observe(copy_on_write.WriteEvent{Offset: 100, Length: 10, ChunkOffset: 100}), finalizeErr)
}
