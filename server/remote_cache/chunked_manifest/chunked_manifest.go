// Package chunked_manifest stores and loads content-defined chunking (CDC)
// mappings, which map a blob digest to its ordered list of chunk digests.
// Storing these mappings allows similar blobs to share chunks, reducing
// storage and transfer costs. Manifests are stored in the cache as AC entries.
package chunked_manifest

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var chunkedManifestSalt = flag.String("cache.chunking.ac_key_salt", "", "If set, salt the AC key with this value.")

const (
	chunkedManifestPrefix = "_bb_chunked_manifest_v1_/"
	chunkOutputFilePrefix = "chunk_"
)

type ChunkedManifest struct {
	BlobDigest     *repb.Digest
	ChunkDigests   []*repb.Digest
	InstanceName   string
	DigestFunction repb.DigestFunction_Value
}

func (cm *ChunkedManifest) ToSplitBlobResponse() *repb.SplitBlobResponse {
	return &repb.SplitBlobResponse{
		ChunkDigests: cm.ChunkDigests,
	}
}

func FromSplitResponse(req *repb.SplitBlobRequest, resp *repb.SplitBlobResponse) *ChunkedManifest {
	return &ChunkedManifest{
		BlobDigest:     req.GetBlobDigest(),
		ChunkDigests:   resp.GetChunkDigests(),
		InstanceName:   req.GetInstanceName(),
		DigestFunction: req.GetDigestFunction(),
	}
}

func (cm *ChunkedManifest) ToFindMissingBlobsRequest() *repb.FindMissingBlobsRequest {
	return &repb.FindMissingBlobsRequest{
		InstanceName:   cm.InstanceName,
		BlobDigests:    cm.ChunkDigests,
		DigestFunction: cm.DigestFunction,
	}
}

func (cm *ChunkedManifest) ToSpliceBlobRequest() *repb.SpliceBlobRequest {
	return &repb.SpliceBlobRequest{
		BlobDigest:     cm.BlobDigest,
		ChunkDigests:   cm.ChunkDigests,
		InstanceName:   cm.InstanceName,
		DigestFunction: cm.DigestFunction,
	}
}

// Store saves the chunked manifest to the cache as an AC entry, keyed by the
// blob digest.
func (cm *ChunkedManifest) Store(ctx context.Context, cache interfaces.Cache) error {
	if len(cm.ChunkDigests) == 0 {
		return status.InvalidArgumentError("chunked manifest must have at least one chunk")
	}

	// Run validations concurrently so we can fail fast if chunks are missing,
	// avoiding the cost of reading all chunk data for verification.
	g, goCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rns := make([]*rspb.ResourceName, 0, len(cm.ChunkDigests))
		for _, chunkDigest := range cm.ChunkDigests {
			rns = append(rns, digest.NewCASResourceName(chunkDigest, cm.InstanceName, cm.DigestFunction).ToProto())
		}

		missing, err := cache.FindMissing(goCtx, rns)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return status.InvalidArgumentErrorf("required chunks not found in CAS: %+v", toString(missing))
		}
		return nil
	})
	g.Go(func() error {
		// TODO(buildbuddy-internal#6426): This could be skipped if this manifest was previously verified, since AC is not shared between
		// groups, but the result validity could be shared.
		return cm.verifyChunks(goCtx, cache)
	})

	ar := &repb.ActionResult{
		OutputFiles: make([]*repb.OutputFile, 0, len(cm.ChunkDigests)),
	}
	for i, chunkDigest := range cm.ChunkDigests {
		ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{
			Path:   chunkOutputFilePrefix + strconv.Itoa(i),
			Digest: chunkDigest,
		})
	}

	arBytes, err := proto.Marshal(ar)
	if err != nil {
		return status.InternalErrorf("marshal chunked manifest to ActionResult: %w", err)
	}

	acRNProto, err := acResourceName(cm.BlobDigest, cm.InstanceName, cm.DigestFunction)
	if err != nil {
		return err
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return cache.Set(ctx, acRNProto, arBytes)
}

// Load retrieves a chunked manifest from the cache, and verifies all chunks exist in the CAS.
func Load(ctx context.Context, cache interfaces.Cache, blobDigest *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) (*ChunkedManifest, error) {
	acRNProto, err := acResourceName(blobDigest, instanceName, digestFunction)
	if err != nil {
		return nil, err
	}

	arBytes, err := cache.Get(ctx, acRNProto)
	if err != nil {
		if status.IsNotFoundError(err) {
			blobRN := digest.NewCASResourceName(blobDigest, instanceName, digestFunction).ToProto()
			if exists, existsErr := cache.Contains(ctx, blobRN); existsErr != nil {
				return nil, errors.Join(existsErr, err)
			} else if exists {
				return nil, status.UnimplementedErrorf("blob %s exists but was not stored with chunking", blobDigest.GetHash())
			}
			return nil, err
		}
		return nil, status.InternalErrorf("retrieve chunked manifest from AC: %w", err)
	}

	ar := &repb.ActionResult{}
	if err := proto.Unmarshal(arBytes, ar); err != nil {
		return nil, status.InternalErrorf("unmarshal chunked manifest from ActionResult: %w", err)
	}

	if len(ar.GetOutputFiles()) == 0 {
		return nil, status.InvalidArgumentError("chunked manifest is empty")
	}

	chunkDigests := make([]*repb.Digest, 0, len(ar.GetOutputFiles()))
	rns := make([]*rspb.ResourceName, 0, len(ar.GetOutputFiles()))
	for _, outputFile := range ar.GetOutputFiles() {
		outputDigest := outputFile.GetDigest()
		chunkDigests = append(chunkDigests, outputDigest)
		rns = append(rns, digest.NewCASResourceName(outputDigest, instanceName, digestFunction).ToProto())
	}

	// TODO(buildbuddy-internal#6426): We could eventually return the missing chunks, and have the client
	// check other places for those, before failing. This adds complexity, and
	// will be avoided in the MVP.
	missing, err := cache.FindMissing(ctx, rns)
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		return nil, status.NotFoundErrorf("required chunks not found in CAS: %+v", toString(missing))
	}
	return &ChunkedManifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}, nil
}

// TODO(buildbuddy-internal#6426): Consider prefetching readers using a
// buffered channel of size 1 to reduce latency between chunk reads.
func (cm *ChunkedManifest) verifyChunks(ctx context.Context, cache interfaces.Cache) error {
	hasher, err := digest.HashForDigestType(cm.DigestFunction)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid digest function: %s", err)
	}

	var totalSize int64
	for i, chunkDigest := range cm.ChunkDigests {
		chunkRN := digest.NewCASResourceName(chunkDigest, cm.InstanceName, cm.DigestFunction)
		if err := chunkRN.Validate(); err != nil {
			return status.InvalidArgumentErrorf("invalid chunk digest at index %d for blob %s: %s", i, cm.BlobDigest.GetHash(), err)
		}

		reader, err := cache.Reader(ctx, chunkRN.ToProto(), 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) {
				return status.InvalidArgumentErrorf("invalid manifest: chunk %d not found in the CAS: %s", i, chunkDigest.GetHash())
			}
			return status.InternalErrorf("read chunk %d for blob %s from CAS: %w", i, cm.BlobDigest.GetHash(), err)
		}

		n, err := io.Copy(hasher, reader)
		if err != nil {
			reader.Close()
			return status.InternalErrorf("hash chunk %d for blob %s: %w", i, cm.BlobDigest.GetHash(), err)
		}
		if err := reader.Close(); err != nil {
			return status.InternalErrorf("close chunk %d reader for blob %s: %w", i, cm.BlobDigest.GetHash(), err)
		}
		totalSize += n
	}

	computedDigest := &repb.Digest{
		Hash:      hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes: totalSize,
	}

	if !digest.Equal(computedDigest, cm.BlobDigest) {
		return status.InvalidArgumentErrorf("computed digest %s does not match expected %s", digest.String(computedDigest), digest.String(cm.BlobDigest))
	}
	return nil
}

func acResourceName(blobDigest *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) (*rspb.ResourceName, error) {
	acInstanceName := chunkedManifestPrefix + instanceName
	acDigest := blobDigest

	// Optionally salt the AC key with a salt value. This is used to prevent someone uploading
	// an invalid chunked manifest directly to the AC, which could be used to bypass the chunk
	// verification.
	// TODO(buildbuddy-internal#6426): Consider using [HMAC](https://pkg.go.dev/crypto/hmac) for more robust keyed hashing.
	if *chunkedManifestSalt != "" {
		saltedDigest, err := digest.Compute(strings.NewReader(*chunkedManifestSalt+":"+blobDigest.GetHash()), digestFunction)
		if err != nil {
			return nil, err
		}
		saltedDigest.SizeBytes = blobDigest.GetSizeBytes()
		acDigest = saltedDigest
	}

	acRN := digest.NewACResourceName(acDigest, acInstanceName, digestFunction)
	if err := acRN.Validate(); err != nil {
		return nil, err
	}
	return acRN.ToProto(), nil
}

func toString(digests []*repb.Digest) string {
	return strings.Join(digestStrings(digests...), ", ")
}

func digestStrings(digests ...*repb.Digest) []string {
	strings := make([]string, 0, len(digests))
	for _, d := range digests {
		strings = append(strings, digest.String(d))
	}
	return strings
}
