package chunking

import (
	"context"
	"encoding/hex"
	"io"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// manifestVersion can be changed to invalidate old manifests.
	manifestVersion = "v1"

	chunkedManifestPrefix = "_bb_chunked_manifest_" + manifestVersion + "_/"
)

type ChunkedManifest struct {
	BlobDigest     *repb.Digest
	ChunkDigests   []*repb.Digest
	InstanceName   string
	DigestFunction repb.DigestFunction_Value
}

func (cm *ChunkedManifest) encodeToActionResult() (*repb.ActionResult, error) {
	if len(cm.ChunkDigests) == 0 {
		return nil, status.InvalidArgumentError("chunked manifest must have at least one chunk")
	}

	ar := &repb.ActionResult{
		OutputFiles: make([]*repb.OutputFile, 0, len(cm.ChunkDigests)),
	}

	for i, chunkDigest := range cm.ChunkDigests {
		ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{
			Path:   chunkedManifestPrefix + "chunk_" + strconv.Itoa(i),
			Digest: chunkDigest,
		})
	}

	return ar, nil
}

func decodeFromActionResult(ar *repb.ActionResult, blobDigest *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) (*ChunkedManifest, error) {
	if len(ar.GetOutputFiles()) == 0 {
		return nil, status.InvalidArgumentError("chunked manifest is empty")
	}

	chunkDigests := make([]*repb.Digest, len(ar.GetOutputFiles()))
	for i, outputFile := range ar.GetOutputFiles() {
		indexStr := strings.TrimPrefix(outputFile.GetPath(), chunkedManifestPrefix+"chunk_")
		expectedIndex := i
		actualIndex, err := strconv.Atoi(indexStr)
		if err != nil || actualIndex != expectedIndex {
			return nil, status.InvalidArgumentErrorf(
				"invalid chunked manifest: expected %q but got path %q",
				chunkedManifestPrefix+"chunk_"+strconv.Itoa(expectedIndex),
				outputFile.GetPath())
		}

		chunkDigests[i] = outputFile.GetDigest()
	}

	return &ChunkedManifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}, nil
}

// Store saves the chunked manifest to the Action Cache using the blob digest as
// the AC key.
func (cm *ChunkedManifest) Store(ctx context.Context, cache interfaces.Cache) error {
	log.Debugf("Storing chunked manifest: blob=%s size=%d chunks=%d instance=%s",
		cm.BlobDigest.GetHash(), cm.BlobDigest.GetSizeBytes(), len(cm.ChunkDigests), cm.InstanceName)

	ar, err := cm.encodeToActionResult()
	if err != nil {
		return err
	}

	arBytes, err := proto.Marshal(ar)
	if err != nil {
		return status.InternalErrorf("marshal chunked manifest to ActionResult: %w", err)
	}

	acInstanceName := chunkedManifestPrefix + cm.InstanceName
	acRN := digest.NewResourceName(cm.BlobDigest, acInstanceName, rspb.CacheType_AC, cm.DigestFunction)
	if err := acRN.Validate(); err != nil {
		return err
	}

	log.Debugf("Storing chunked manifest with AC key: blob=%s/%d digest_func=%s instance=%q ac_key=%s",
		cm.BlobDigest.GetHash(), cm.BlobDigest.GetSizeBytes(), cm.DigestFunction, acInstanceName, acRN.ToProto().String())

	if err := cache.Set(ctx, acRN.ToProto(), arBytes); err != nil {
		return status.InternalErrorf("store chunked manifest in AC: %w", err)
	}

	log.Debugf("Stored chunked manifest in AC: blob=%s manifest_size=%d", cm.BlobDigest.GetHash(), len(arBytes))
	return nil
}

// Load retrieves a chunked manifest from the Action Cache.
func Load(ctx context.Context, cache interfaces.Cache, blobDigest *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) (*ChunkedManifest, error) {
	chunkedManifestInstanceName := chunkedManifestPrefix + instanceName
	acRN := digest.NewResourceName(blobDigest, chunkedManifestInstanceName, rspb.CacheType_AC, digestFunction)
	if err := acRN.Validate(); err != nil {
		return nil, err
	}

	log.Debugf("Looking up chunked manifest: blob=%s/%d digest_func=%s instance=%q ac_key=%s",
		blobDigest.GetHash(), blobDigest.GetSizeBytes(), digestFunction, chunkedManifestInstanceName, acRN.ToProto().String())

	arBytes, err := cache.Get(ctx, acRN.ToProto())
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, status.NotFoundErrorf("chunked manifest not found for blob %s", blobDigest.GetHash())
		}
		return nil, status.InternalErrorf("retrieve chunked manifest from AC: %w", err)
	}

	ar := &repb.ActionResult{}
	if err := proto.Unmarshal(arBytes, ar); err != nil {
		return nil, status.InternalErrorf("unmarshal chunked manifest from ActionResult: %w", err)
	}

	manifest, err := decodeFromActionResult(ar, blobDigest, instanceName, digestFunction)
	if err != nil {
		return nil, err
	}

	log.Debugf("Loaded chunked manifest: blob=%s size=%d chunks=%d instance=%s",
		blobDigest.GetHash(), blobDigest.GetSizeBytes(), len(manifest.ChunkDigests), instanceName)

	return manifest, nil
}

// VerifyChunks validates that all chunks exist in CAS and their concatenated
// digest matches the expected blob digest. Expensive for large blobs.
func (cm *ChunkedManifest) VerifyChunks(ctx context.Context, cache interfaces.Cache) error {
	hasher, err := digest.HashForDigestType(cm.DigestFunction)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid digest function: %s", err)
	}

	var totalSize int64
	for i, chunkDigest := range cm.ChunkDigests {
		chunkRN := digest.NewResourceName(chunkDigest, cm.InstanceName, rspb.CacheType_CAS, cm.DigestFunction)
		if err := chunkRN.Validate(); err != nil {
			return status.InvalidArgumentErrorf("invalid chunk digest at index %d for blob %s: %s", i, cm.BlobDigest.GetHash(), err)
		}

		reader, err := cache.Reader(ctx, chunkRN.ToProto(), 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) {
				return status.NotFoundErrorf("chunk %d not found: %s", i, chunkDigest.GetHash())
			}
			return status.InternalErrorf("read chunk %d for blob %s from CAS: %w", i, cm.BlobDigest.GetHash(), err)
		}

		n, err := io.Copy(hasher, reader)
		if err != nil {
			return status.InternalErrorf("hash chunk %d for blob %s: %w", i, cm.BlobDigest.GetHash(), err)
		}
		totalSize += n
	}

	computedDigest := &repb.Digest{
		Hash:      hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes: totalSize,
	}

	if computedDigest.GetHash() != cm.BlobDigest.GetHash() {
		return status.InvalidArgumentErrorf(
			"computed digest hash %s does not match expected hash %s",
			computedDigest.GetHash(), cm.BlobDigest.GetHash())
	}

	if computedDigest.GetSizeBytes() != cm.BlobDigest.GetSizeBytes() {
		return status.InvalidArgumentErrorf(
			"computed size %d does not match expected size %d",
			computedDigest.GetSizeBytes(), cm.BlobDigest.GetSizeBytes())
	}

	return nil
}
