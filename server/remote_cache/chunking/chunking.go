// Package chunking provides content-defined chunking (CDC) for large blobs.
package chunking

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/fastcdc2020/fastcdc"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

var (
	chunkedManifestSalt = flag.String("cache.chunking.ac_key_salt", "", "If set, salt the AC key with this value.")
	avgChunkSizeBytes   = flag.Int64("cache.avg_chunk_size_bytes", 512*1024, "This is the average size of a chunk. Only blobs larger (non-inclusive) than 4x this value will be chunked. The maximum chunk size will be 4x this value, and the minimum will be 1/4 this value (default 512KB).")
)

const (
	chunkedManifestPrefix = "_bb_chunked_manifest_v2_/"
	chunkOutputFilePrefix = "chunk_"
)

func AvgChunkSizeBytes() int64 {
	return *avgChunkSizeBytes
}

func MaxChunkSizeBytes() int64 {
	return *avgChunkSizeBytes * 4
}

func ValidateConfig() error {
	v := *avgChunkSizeBytes
	if v < 1024 || v > 1024*1024 {
		return fmt.Errorf("cache.avg_chunk_size_bytes must be between 1024 and 1048576, got %d", v)
	}
	return nil
}

func Enabled(ctx context.Context, efp interfaces.ExperimentFlagProvider) bool {
	return efp != nil &&
		efp.Boolean(ctx, "cache.chunking_enabled", false)
}

func ShouldReadChunked(ctx context.Context, efp interfaces.ExperimentFlagProvider, digestSizeBytes, offset, limit int64) bool {
	// Check digest first since it's faster than reading efp flag
	// and very likely to be false.
	return digestSizeBytes > MaxChunkSizeBytes() &&
		offset == 0 &&
		limit == 0 &&
		Enabled(ctx, efp)
}

type WriteFunc func([]byte) error

type Chunker struct {
	pw *io.PipeWriter

	done chan struct{}

	mu  sync.Mutex // protects err
	err error
}

func (c *Chunker) Write(buf []byte) (int, error) {
	return c.pw.Write(buf)
}

// Close blocks until all chunks have been processed.
func (c *Chunker) Close() error {
	if err := c.pw.Close(); err != nil {
		return status.InternalErrorf("failed to close chunker: %s", err)
	}

	<-c.done

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// NewChunker returns an io.WriteCloser that split file into chunks of average size.
// averageSize is typically a power of 2. It must be in the range 256B to 256MB.
// The minimum allowed chunk size is averageSize / 4, and the maximum allowed
// chunk size is averageSize * 4.
func NewChunker(ctx context.Context, averageSize int, writeChunkFn WriteFunc) (*Chunker, error) {
	pr, pw := io.Pipe()
	c := &Chunker{
		pw:   pw,
		done: make(chan struct{}),
	}
	chunker, err := fastcdc.NewChunker(
		pr,
		averageSize,

		// Min and Max size should always be 1/4x and 4x of the
		// avg size respectively. Explicitly declare these to prevent
		// the library modifying them unknowningly.
		fastcdc.WithMinSize(averageSize/4),
		fastcdc.WithMaxSize(averageSize*4),

		// We want to keep the rolling hash the same to ensure that given the same
		// file, the library will chunk the file in the same way.
		fastcdc.WithSeed(0),

		// Normalization defaults to 2 from testing using Bazel build
		// artifacts, since it provided the best balance of deduplication
		// and chunk size consistency.
		//
		// Stats:
		// Algorithm         │ Dedup%   │ Saved        │ Chunks/File avg
		// ─────────────────────────────────────────────────────────────
		// normalization-0  │   30.37% │     93.87 GB │     33.4 │
		// normalization-1  │   31.30% │     96.73 GB │     34.4 │
		// normalization-2  │   32.09% │     99.19 GB │     38.3 │
		// normalization-3  │   32.07% │     99.10 GB │     41.4 │
		fastcdc.WithNormalization(2),
	)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(c.done)
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				err = status.InternalErrorf("failed to get the next chunk: %s", err)
				pr.CloseWithError(err)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.err == nil {
					c.err = err
				}
				return
			}
			if err := writeChunkFn(chunk.Data); err != nil {
				err = status.InternalErrorf("writeChunkFn failed: %s", err)
				pr.CloseWithError(err)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.err == nil {
					c.err = err
				}
				return
			}
		}
	}()

	go func() {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
		}
		pr.CloseWithError(ctx.Err())
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.err == nil {
			c.err = ctx.Err()
		}
	}()

	return c, nil
}

type Manifest struct {
	BlobDigest     *repb.Digest
	ChunkDigests   []*repb.Digest
	InstanceName   string
	DigestFunction repb.DigestFunction_Value
}

func (cm *Manifest) ToSplitBlobResponse() *repb.SplitBlobResponse {
	return &repb.SplitBlobResponse{
		ChunkDigests:     cm.ChunkDigests,
		ChunkingFunction: repb.ChunkingFunction_FAST_CDC_2020,
	}
}

func (cm *Manifest) ToFindMissingBlobsRequest() *repb.FindMissingBlobsRequest {
	return &repb.FindMissingBlobsRequest{
		InstanceName:   cm.InstanceName,
		BlobDigests:    cm.ChunkDigests,
		DigestFunction: cm.DigestFunction,
	}
}

func (cm *Manifest) ToSpliceBlobRequest() *repb.SpliceBlobRequest {
	return &repb.SpliceBlobRequest{
		BlobDigest:       cm.BlobDigest,
		ChunkDigests:     cm.ChunkDigests,
		InstanceName:     cm.InstanceName,
		DigestFunction:   cm.DigestFunction,
		ChunkingFunction: repb.ChunkingFunction_FAST_CDC_2020,
	}
}

func (cm *Manifest) ChunkResourceNames() []*rspb.ResourceName {
	rns := make([]*rspb.ResourceName, 0, len(cm.ChunkDigests))
	for _, chunkDigest := range cm.ChunkDigests {
		rns = append(rns, digest.NewCASResourceName(chunkDigest, cm.InstanceName, cm.DigestFunction).ToProto())
	}
	return rns
}

// Store saves the chunked manifest to the cache as an AC entry, keyed by the
// blob digest. It validates that all chunks exist and their combined hash
// matches the blob digest.
func (cm *Manifest) Store(ctx context.Context, cache interfaces.Cache) error {
	if len(cm.ChunkDigests) == 0 {
		return status.InvalidArgumentError("chunked manifest must have at least one chunk")
	}

	// Run validations concurrently so we can fail fast if chunks are missing,
	// avoiding the cost of reading all chunk data for verification.
	g, goCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		missing, err := cache.FindMissing(goCtx, cm.ChunkResourceNames())
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return status.InvalidArgumentErrorf("required chunks not found in CAS: %+v", DigestsSummary(missing))
		}
		return nil
	})
	g.Go(func() error {
		// TODO(buildbuddy-internal#6426): This could be skipped if this manifest was previously verified, since AC is not shared between
		// groups, but the result validity could be shared.
		return cm.verifyChunks(goCtx, cache)
	})

	if err := g.Wait(); err != nil {
		return err
	}

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

	if err := cache.Set(ctx, acRNProto, arBytes); err != nil {
		if status.IsInternalError(err) {
			log.CtxInfof(ctx, "Failed to set chunking manifest (blob %s, manifest key %s): %v", cm.BlobDigest.GetHash(), acRNProto.GetDigest().GetHash(), err)
		}
		return sanitizeManifestError(err, acRNProto.GetDigest().GetHash(), cm.BlobDigest.GetHash())
	}
	return nil
}

// LoadManifest retrieves a chunked manifest from the cache. It does NOT validate existence of the chunks.
func LoadManifest(ctx context.Context, cache interfaces.Cache, blobDigest *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) (*Manifest, error) {
	acRNProto, err := acResourceName(blobDigest, instanceName, digestFunction)
	if err != nil {
		return nil, err
	}

	arBytes, err := cache.Get(ctx, acRNProto)
	if err != nil {
		if status.IsInternalError(err) {
			log.CtxInfof(ctx, "Failed to get chunking manifest (blob %s, manifest key %s): %v", blobDigest.GetHash(), acRNProto.GetDigest().GetHash(), err)
		}
		err = sanitizeManifestError(err, acRNProto.GetDigest().GetHash(), blobDigest.GetHash())
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
	for _, outputFile := range ar.GetOutputFiles() {
		chunkDigests = append(chunkDigests, outputFile.GetDigest())
	}

	return &Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}, nil
}

// TODO(buildbuddy-internal#6426): Consider prefetching readers using a
// buffered channel of size 1 to reduce latency between chunk reads.
func (cm *Manifest) verifyChunks(ctx context.Context, cache interfaces.Cache) error {
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

// sanitizeManifestError replaces the salted AC key hash with the original blob hash in
// the error message, so callers see the blob digest rather than the internal salted key.
// The gRPC status code is preserved.
func sanitizeManifestError(err error, saltedHash, blobHash string) error {
	if err == nil || saltedHash == "" || saltedHash == blobHash {
		return err
	}
	grpcStatus, ok := gstatus.FromError(err)
	if !ok {
		return fmt.Errorf("%s", sanitizeMsg(err.Error(), saltedHash, blobHash))
	}
	return gstatus.Error(grpcStatus.Code(), sanitizeMsg(grpcStatus.Message(), saltedHash, blobHash))
}

func sanitizeMsg(msg, saltedHash, blobHash string) string {
	replacement := fmt.Sprintf("manifestKey(%s)", blobHash)
	if replaced := strings.ReplaceAll(msg, saltedHash, replacement); replaced != msg {
		return replaced
	}
	return fmt.Sprintf("%s (blob %s)", msg, blobHash)
}

func DigestsSummary(digests []*repb.Digest) string {
	const maxShown = 3
	strs := digestsStrings(digests...)
	if len(strs) <= maxShown {
		return strings.Join(strs, ", ")
	}
	return strings.Join(strs[:maxShown], ", ") + " (" + strconv.Itoa(len(strs)) + " total)"
}

func digestsStrings(digests ...*repb.Digest) []string {
	strings := make([]string, 0, len(digests))
	for _, d := range digests {
		strings = append(strings, digest.String(d))
	}
	return strings
}

// MissingChunkChecker is used to check to make sure all of the chunks that make up a blob
// are present in the cache, and to de-duplicate excess calls to FindMissing.
type MissingChunkChecker struct {
	cache        interfaces.Cache
	chunkPresent map[string]bool
}

func NewMissingChunkChecker(cache interfaces.Cache) *MissingChunkChecker {
	return &MissingChunkChecker{
		cache:        cache,
		chunkPresent: make(map[string]bool),
	}
}

// AnyChunkMissing checks to make sure all of the chunks that make up a blob
// are present in the cache. To de-duplicate excess calls to FindMissing,
// it keeps a map of chunks that have already been checked.
//
// When a new manifest is checked, we mark all of its chunks as present, and then
// update them as missing if they're returned from FindMissing.
func (c *MissingChunkChecker) AnyChunkMissing(ctx context.Context, manifest *Manifest) (bool, error) {
	var unknownChunks []*rspb.ResourceName
	for _, rn := range manifest.ChunkResourceNames() {
		if present, known := c.chunkPresent[rn.GetDigest().GetHash()]; known {
			if !present {
				return true, nil
			}
			continue
		}
		unknownChunks = append(unknownChunks, rn)
	}

	if len(unknownChunks) == 0 {
		return false, nil
	}

	missingDigests, err := c.cache.FindMissing(ctx, unknownChunks)
	if err != nil {
		return false, err
	}

	// To prevent unbounded growth, just clear the chunk
	// cache if its >1000 entries. Checking the len(map)
	// is O(1) since Go stores the map length in the map
	// header
	if len(c.chunkPresent) >= 1000 {
		clear(c.chunkPresent)
	}
	for _, rn := range unknownChunks {
		c.chunkPresent[rn.GetDigest().GetHash()] = true
	}
	for _, d := range missingDigests {
		c.chunkPresent[d.GetHash()] = false
	}

	return len(missingDigests) > 0, nil
}
