package chunk_metadata_store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const chunkMetadataKeyPrefix = "/chunk-metadata-v1/"

// ChunkMetadataStore creates a way to store information on
// chunked CAS objects using REv2 API. This is done so that the cache proxy
// can chunk using CDC, and the remote can store chunks, while also being
// able to reconstruct them for execution on the remote.
type ChunkMetadataStore struct {
	cache interfaces.Cache
}

func NewChunkMetadataStore(cache interfaces.Cache) *ChunkMetadataStore {
	return &ChunkMetadataStore{
		cache: cache,
	}
}

// Get returns the chunks used to compose a blob if the blob is stored using CDC given its digest.
// If not found, a NotFound error is returned.
// Get does not validate the existence of the individual chunks.
//
// This uses a two-level lookup:
// 1. Blob digest -> Metadata digest (pointer stored in cache)
// 2. Metadata digest -> ChunkedMetadata proto (stored in CAS)
func (s *ChunkMetadataStore) Get(ctx context.Context, instanceName string, blobDigest *repb.Digest, digestFunction repb.DigestFunction_Value) (*sgpb.StorageMetadata_ChunkedMetadata, error) {
	pointerKey := s.MakeMetadataPointerKey(instanceName, blobDigest, digestFunction)
	pointerBlob, err := s.cache.Get(ctx, pointerKey)
	if err != nil {
		return nil, err
	}

	pointer := sgpb.StorageMetadata_ChunkedMetadataPointer{}
	if err := proto.Unmarshal(pointerBlob, &pointer); err != nil {
		return nil, status.InternalErrorf("unmarshal chunk metadata pointer: %w", err)
	}

	metadataDigest := pointer.GetMetadataDigest()
	if metadataDigest == nil {
		return nil, status.InternalError("chunk metadata pointer has nil digest")
	}

	metadataRN := digest.NewCASResourceName(metadataDigest, instanceName, digestFunction)
	metadataBlob, err := s.cache.Get(ctx, metadataRN.ToProto())
	if err != nil {
		return nil, status.InternalErrorf("fetch chunk metadata from CAS: %w", err)
	}

	metadata := sgpb.StorageMetadata_ChunkedMetadata{}
	if err := proto.Unmarshal(metadataBlob, &metadata); err != nil {
		return nil, status.InternalErrorf("unmarshal chunk metadata: %w", err)
	}
	return &metadata, nil
}

// Set stores chunked metadata for a given Blob digest.
//
// This uses a two-level storage:
// 1. ChunkedMetadata proto -> stored in CAS with its own digest
// 2. Blob digest -> Metadata digest pointer (stored in cache)
func (s *ChunkMetadataStore) Set(ctx context.Context, instanceName string, blobDigest *repb.Digest, digestFunction repb.DigestFunction_Value, chunkMetadata *sgpb.StorageMetadata_ChunkedMetadata) error {
	metadataBlob, err := proto.Marshal(chunkMetadata)
	if err != nil {
		return status.InternalErrorf("marshalling chunk metadata: %w", err)
	}

	metadataDigest, err := digest.Compute(bytes.NewReader(metadataBlob), digestFunction)
	if err != nil {
		return status.InternalErrorf("computing chunk metadata digest: %w", err)
	}

	metadataRN := digest.NewCASResourceName(metadataDigest, instanceName, digestFunction)
	if err := s.cache.Set(ctx, metadataRN.ToProto(), metadataBlob); err != nil {
		return status.InternalErrorf("storing chunk metadata in CAS: %w", err)
	}

	pointer := &sgpb.StorageMetadata_ChunkedMetadataPointer{
		MetadataDigest: metadataDigest,
	}
	pointerBlob, err := proto.Marshal(pointer)
	if err != nil {
		return status.InternalErrorf("marshalling chunk metadata pointer: %w", err)
	}

	pointerKey := s.MakeMetadataPointerKey(instanceName, blobDigest, digestFunction)
	return s.cache.Set(ctx, pointerKey, pointerBlob)
}

// MakeMetadataPointerKey creates a cache key for storing the metadata pointer.
// Uses a prefix + blob hash, then hashes it to create a valid SHA256 hash.
// This avoids collisions with actual CAS blobs that might have the same digest.
// This method is public so it can be used by ByteStreamServerProxy to maintain consistency.
func (s *ChunkMetadataStore) MakeMetadataPointerKey(instanceName string, blobDigest *repb.Digest, digestFunction repb.DigestFunction_Value) *rspb.ResourceName {
	// Hash the prefix + blob hash to create a valid SHA256 hash (64 hex characters)
	keyData := chunkMetadataKeyPrefix + blobDigest.GetHash()
	hash := sha256.Sum256([]byte(keyData))
	hashStr := hex.EncodeToString(hash[:])

	metadataDigest := &repb.Digest{
		Hash:      hashStr,
		SizeBytes: 1, // Size is not meaningful for pointer keys, but must be non-zero to pass validation
	}
	return digest.NewCASResourceName(metadataDigest, instanceName, digestFunction).ToProto()
}

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
