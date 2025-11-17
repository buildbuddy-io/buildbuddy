package cdc_proxy

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

// ChunkInfo contains information about a single chunk
type ChunkInfo struct {
	Digest *repb.Digest
	Data   []byte
}

// ChunkedBlobInfo contains all chunks and metadata for a blob
type ChunkedBlobInfo struct {
	Chunks   []*ChunkInfo
	Metadata *sgpb.StorageMetadata_ChunkedMetadata
}

// ChunkBlob chunks a blob using content-defined chunking and returns
// chunk information and metadata.
func ChunkBlob(ctx context.Context, data []byte, averageChunkSize int, digestFunction repb.DigestFunction_Value) (*ChunkedBlobInfo, error) {
	if averageChunkSize <= 0 {
		return nil, status.InvalidArgumentError("averageChunkSize must be positive")
	}

	result := &ChunkedBlobInfo{
		Chunks: make([]*ChunkInfo, 0),
	}

	var mu sync.Mutex
	writeChunk := func(chunkData []byte) error {
		// Compute digest for chunk using the specified digest function
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), digestFunction)
		if err != nil {
			return status.InternalErrorf("failed to compute chunk digest: %s", err)
		}

		// Store chunk info
		chunkDataCopy := make([]byte, len(chunkData))
		copy(chunkDataCopy, chunkData)

		mu.Lock()
		defer mu.Unlock()
		result.Chunks = append(result.Chunks, &ChunkInfo{
			Digest: chunkDigest,
			Data:   chunkDataCopy,
		})
		return nil
	}

	// Create chunker and process data
	c, err := chunker.New(ctx, averageChunkSize, writeChunk)
	if err != nil {
		return nil, status.InternalErrorf("failed to create chunker: %s", err)
	}

	_, err = c.Write(data)
	if err != nil {
		return nil, status.InternalErrorf("failed to write to chunker: %s", err)
	}

	err = c.Close()
	if err != nil {
		return nil, status.InternalErrorf("failed to close chunker: %s", err)
	}

	// Build metadata with chunk resource names
	chunkResources := make([]*rspb.ResourceName, 0, len(result.Chunks))
	for _, chunk := range result.Chunks {
		chunkRN := &rspb.ResourceName{
			Digest:         chunk.Digest,
			DigestFunction: digestFunction,
			CacheType:      rspb.CacheType_CAS,
		}
		chunkResources = append(chunkResources, chunkRN)
	}

	result.Metadata = &sgpb.StorageMetadata_ChunkedMetadata{
		Resource: chunkResources,
	}

	return result, nil
}

// ChunkBlobStream chunks a blob from a stream using content-defined chunking.
// This is useful for large blobs that shouldn't be buffered entirely in memory.
func ChunkBlobStream(ctx context.Context, reader io.Reader, averageChunkSize int, digestFunction repb.DigestFunction_Value, writeChunk func(*ChunkInfo) error) (*sgpb.StorageMetadata_ChunkedMetadata, error) {
	if averageChunkSize <= 0 {
		return nil, status.InvalidArgumentError("averageChunkSize must be positive")
	}

	chunkResources := make([]*rspb.ResourceName, 0)
	var mu sync.Mutex

	writeChunkWrapper := func(chunkData []byte) error {
		// Compute digest for chunk using the specified digest function
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), digestFunction)
		if err != nil {
			return status.InternalErrorf("failed to compute chunk digest: %s", err)
		}

		// Make a copy since chunkData will be reused by chunker
		chunkDataCopy := make([]byte, len(chunkData))
		copy(chunkDataCopy, chunkData)

		chunkInfo := &ChunkInfo{
			Digest: chunkDigest,
			Data:   chunkDataCopy,
		}

		// Call user's write function
		if err := writeChunk(chunkInfo); err != nil {
			return err
		}

		// Track chunk resource name
		mu.Lock()
		defer mu.Unlock()
		chunkRN := &rspb.ResourceName{
			Digest:         chunkDigest,
			DigestFunction: digestFunction,
			CacheType:      rspb.CacheType_CAS,
		}
		chunkResources = append(chunkResources, chunkRN)
		return nil
	}

	// Create chunker and process stream
	c, err := chunker.New(ctx, averageChunkSize, writeChunkWrapper)
	if err != nil {
		return nil, status.InternalErrorf("failed to create chunker: %s", err)
	}

	// Copy from reader to chunker
	_, err = io.Copy(c, reader)
	if err != nil {
		return nil, status.InternalErrorf("failed to read from stream: %s", err)
	}

	err = c.Close()
	if err != nil {
		return nil, status.InternalErrorf("failed to close chunker: %s", err)
	}

	metadata := &sgpb.StorageMetadata_ChunkedMetadata{
		Resource: chunkResources,
	}

	return metadata, nil
}

// ReconstructBlob reconstructs a complete blob from its chunks.
// Chunks must be provided in the correct order.
func ReconstructBlob(chunks [][]byte) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, status.InvalidArgumentError("no chunks provided")
	}

	// Calculate total size
	totalSize := int64(0)
	for _, chunk := range chunks {
		totalSize += int64(len(chunk))
	}

	// Allocate buffer for full blob
	result := make([]byte, 0, totalSize)

	// Concatenate chunks
	for _, chunk := range chunks {
		result = append(result, chunk...)
	}

	return result, nil
}

// ValidateChunkMetadata validates that chunk metadata is well-formed
func ValidateChunkMetadata(metadata *sgpb.StorageMetadata_ChunkedMetadata) error {
	if metadata == nil {
		return status.InvalidArgumentError("metadata is nil")
	}

	if len(metadata.GetResource()) == 0 {
		return status.InvalidArgumentError("metadata has no chunks")
	}

	for i, rn := range metadata.GetResource() {
		if rn.GetDigest() == nil {
			return status.InvalidArgumentErrorf("chunk %d has no digest", i)
		}
		if rn.GetDigest().GetHash() == "" {
			return status.InvalidArgumentErrorf("chunk %d has empty hash", i)
		}
		if rn.GetDigest().GetSizeBytes() <= 0 {
			return status.InvalidArgumentErrorf("chunk %d has invalid size: %d", i, rn.GetDigest().GetSizeBytes())
		}
	}

	return nil
}

// ShouldChunkBlob determines whether a blob should be chunked based on
// configuration and blob size.
func ShouldChunkBlob(blobSizeBytes int64, minBlobSizeToChunk int64, averageChunkSize int) bool {
	if averageChunkSize <= 0 {
		return false
	}
	if minBlobSizeToChunk <= 0 {
		return false
	}
	return blobSizeBytes >= minBlobSizeToChunk
}

// GetChunkDigests extracts digest list from chunk metadata
func GetChunkDigests(metadata *sgpb.StorageMetadata_ChunkedMetadata) []*repb.Digest {
	if metadata == nil {
		return nil
	}

	digests := make([]*repb.Digest, 0, len(metadata.GetResource()))
	for _, rn := range metadata.GetResource() {
		if rn.GetDigest() != nil {
			digests = append(digests, rn.GetDigest())
		}
	}
	return digests
}
