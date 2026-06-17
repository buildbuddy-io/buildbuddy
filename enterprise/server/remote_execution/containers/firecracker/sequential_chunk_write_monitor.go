package firecracker

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// sequentialChunkWriteTracker watches sequential chunk writes.
// When writes advance from one chunk to a later chunk, the previous chunk is
// finalized under the invariant that writes are observed in non-decreasing
// order.
type sequentialChunkWriteTracker struct {
	// chunkSizeBytes converts COWStore chunk indexes to chunk start offsets.
	chunkSizeBytes int64

	// finalizeChunk is called when no more writes are expected for the given chunk.
	finalizeChunk func(chunkOffset int64) error

	// currentChunkIndex is the chunk currently receiving writes.
	currentChunkIndex int64
	// haveCurrentChunk distinguishes "no writes observed yet" from chunk 0.
	haveCurrentChunk bool
}

// Observe records a chunk write and finalizes the previous chunk when the write
// stream advances to a later chunk.
func (t *sequentialChunkWriteTracker) Observe(event copy_on_write.WriteEvent) error {
	err := validateSequentialWriteEvent(event, t.currentChunkIndex, t.chunkSizeBytes)
	if err != nil {
		return err
	}

	if !t.haveCurrentChunk {
		t.currentChunkIndex = event.ChunkIndex
		t.haveCurrentChunk = true
		return nil
	}

	// If the write is to the same chunk, do nothing.
	if event.ChunkIndex == t.currentChunkIndex {
		return nil
	}

	// In a sequential write, if the write has moved to a new chunk, finalize the previous chunk.
	if err := t.finalizeChunk(t.currentChunkOffset()); err != nil {
		return err
	}
	t.currentChunkIndex = event.ChunkIndex
	return nil
}

func validateSequentialWriteEvent(event copy_on_write.WriteEvent, currentChunkIndex, chunkSizeBytes int64) error {
	if chunkSizeBytes <= 0 {
		return status.FailedPreconditionErrorf("chunk size must be positive: %d", chunkSizeBytes)
	}
	if event.Offset < 0 {
		return status.InvalidArgumentErrorf("write offset must be non-negative: %d", event.Offset)
	}
	if event.Length <= 0 {
		return status.InvalidArgumentErrorf("write length must be positive: %d", event.Length)
	}
	if event.ChunkIndex < 0 {
		return status.InvalidArgumentErrorf("chunk offset must be non-negative: %d", event.ChunkIndex)
	}
	if event.ChunkIndex < currentChunkIndex {
		return status.FailedPreconditionErrorf("memory snapshot write moved backwards from chunk %d to chunk %d", currentChunkIndex, event.ChunkIndex)
	}
	return nil
}

// After there are no more writes to observe, Finish() ensures final bookkeeping of all written chunks.
func (t *sequentialChunkWriteTracker) Finish() error {
	if !t.haveCurrentChunk {
		return nil
	}
	// After the last write, there is no subsequent write to trigger finalization
	// of the last chunk. Finalize it here.
	chunkOffset := t.currentChunkOffset()
	t.haveCurrentChunk = false
	return t.finalizeChunk(chunkOffset)
}

func (t *sequentialChunkWriteTracker) currentChunkOffset() int64 {
	return t.currentChunkIndex * t.chunkSizeBytes
}
