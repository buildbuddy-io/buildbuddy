package ocicache

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/anypb"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheSecret = flag.String("oci.cache.secret", "", "Secret to add to OCI image cache keys.", flag.Secret)
)

const (
	blobOutputFilePath          = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath  = "_bb_ociregistry_blob_metadata_"
	actionResultInstanceName    = interfaces.OCIImageInstanceNamePrefix
	manifestContentInstanceName = interfaces.OCIImageInstanceNamePrefix + "_manifest_content_"

	maxManifestSize = 10000000

	cacheDigestFunction = repb.DigestFunction_SHA256
)

func WriteManifestToAC(ctx context.Context, raw []byte, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, originalRef gcrname.Reference) error {
	arRN, err := manifestACKey(repo, hash)
	if err != nil {
		log.CtxWarningf(ctx, "Error creating key for manifest %s@%s (original ref %q): %s", repo, hash, originalRef, err)
		return err
	}

	m := &ocipb.OCIManifestContent{
		Raw:         raw,
		ContentType: contentType,
	}
	any, err := anypb.New(m)
	if err != nil {
		log.CtxWarningf(ctx, "Error constructing manifest contents %s@%s (original ref %q): %s", repo, hash, originalRef, err)
		return err
	}
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				any,
			},
		},
	}
	if err := cachetools.UploadActionResult(ctx, acClient, arRN, ar); err != nil {
		log.CtxWarningf(ctx, "Error writing manifest %s@%s (original ref %q) to AC: %s", repo, hash, originalRef, err)
		return err
	}
	log.CtxInfof(ctx, "Successfully wrote manifest %s@%s (original ref %q)", repo, hash, originalRef)
	return nil
}

func updateCacheEventMetric(ociResourceTypeLabel, cacheEventType string) {
	metrics.OCIRegistryCacheEvents.With(prometheus.Labels{
		metrics.OCIResourceTypeLabel: ociResourceTypeLabel,
		metrics.CacheEventTypeLabel:  cacheEventType,
	}).Inc()
}

func manifestMiss(ctx context.Context, repo gcrname.Repository, hash gcr.Hash, originalRef gcrname.Reference) {
	log.CtxInfof(ctx, "OCI cache manifest miss %s@%s (original ref %q)", repo, hash, originalRef)
	updateCacheEventMetric(metrics.OCIManifestResourceTypeLabel, metrics.MissStatusLabel)
}

func manifestHit(ctx context.Context, repo gcrname.Repository, hash gcr.Hash, originalRef gcrname.Reference) {
	log.CtxInfof(ctx, "OCI cache manifest hit %s@%s (original ref %q)", repo, hash, originalRef)
	updateCacheEventMetric(metrics.OCIManifestResourceTypeLabel, metrics.HitStatusLabel)
}

// FetchManifestFromAC fetches the given manifest from the AC if present.
// TODO(dan) remote originalRef argument once we've debugged frequency of manifest cache misses.
func FetchManifestFromAC(ctx context.Context, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, originalRef gcrname.Reference) (*ocipb.OCIManifestContent, error) {
	arRN, err := manifestACKey(repo, hash)
	if err != nil {
		manifestMiss(ctx, repo, hash, originalRef)
		log.CtxWarningf(ctx, "Error creating key for manifest %s@%s: %s", repo, hash, err)
		return nil, err
	}
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		manifestMiss(ctx, repo, hash, originalRef)
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error getting action result for manifest %s@%s: %s", repo, hash, err)
		}
		return nil, err
	}
	meta := ar.GetExecutionMetadata()
	if meta == nil {
		manifestMiss(ctx, repo, hash, originalRef)
		log.CtxWarningf(ctx, "Missing execution metadata for manifest %s@%s", repo, hash)
		return nil, status.InternalErrorf("missing execution metadata for manifest in %q", repo)
	}
	aux := meta.GetAuxiliaryMetadata()
	if aux == nil || len(aux) != 1 {
		manifestMiss(ctx, repo, hash, originalRef)
		log.CtxWarningf(ctx, "Missing auxiliary metadata for manifest %s@%s", repo, hash)
		return nil, status.InternalErrorf("missing auxiliary metadata for manifest %s@%s", repo, hash)
	}
	any := aux[0]
	var mc ocipb.OCIManifestContent
	err = any.UnmarshalTo(&mc)
	if err != nil {
		manifestMiss(ctx, repo, hash, originalRef)
		log.CtxWarningf(ctx, "Error unmarshalling manifest content %s@%s: %s", repo, hash, err)
		return nil, status.InternalErrorf("could not unmarshal metadata for manifest %s@%s: %s", repo, hash, err)
	}
	manifestHit(ctx, repo, hash, originalRef)
	return &mc, nil
}

func manifestACKey(repo gcrname.Repository, refhash gcr.Hash) (*digest.ACResourceName, error) {
	s := hash.Strings(
		repo.RegistryStr(),
		repo.RepositoryStr(),
		ocipb.OCIResourceType_MANIFEST.String(),
		refhash.Algorithm,
		refhash.Hex,
		*cacheSecret,
	)
	arDigest, err := digest.Compute(bytes.NewBufferString(s), cacheDigestFunction)
	if err != nil {
		return nil, err
	}
	return digest.NewACResourceName(
		arDigest,
		manifestContentInstanceName,
		cacheDigestFunction,
	), nil
}

func blobMetadataMiss(ctx context.Context) {
	log.CtxDebug(ctx, "oci cache blob metadata miss")
	updateCacheEventMetric(metrics.OCIBlobMetadataResourceTypeLabel, metrics.MissStatusLabel)
}

func blobMetadataHit(ctx context.Context) {
	log.CtxDebug(ctx, "oci cache blob metadata hit")
	updateCacheEventMetric(metrics.OCIBlobMetadataResourceTypeLabel, metrics.HitStatusLabel)
}

func FetchBlobMetadataFromCache(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash) (*ocipb.OCIBlobMetadata, error) {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      repo.RegistryStr(),
		Repository:    repo.RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		blobMetadataMiss(ctx)
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		blobMetadataMiss(ctx)
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		blobMetadataMiss(ctx)
		return nil, err
	}

	var blobMetadataCASDigest *repb.Digest
	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case blobMetadataOutputFilePath:
			blobMetadataCASDigest = outputFile.GetDigest()
		case blobOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "Unknown output file path %q in ActionResult for %q", outputFile.GetPath(), repo)
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		blobMetadataMiss(ctx)
		return nil, status.NotFoundErrorf("missing blob metadata digest or blob digest for %s", repo)
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		cacheDigestFunction,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsClient, blobMetadataRN, blobMetadata)
	if err != nil {
		blobMetadataMiss(ctx)
		return nil, err
	}
	blobMetadataHit(ctx)
	return blobMetadata, nil
}

func blobMiss(ctx context.Context) {
	log.CtxDebug(ctx, "oci cache blob miss")
	updateCacheEventMetric(metrics.OCIBlobResourceTypeLabel, metrics.MissStatusLabel)
}

func blobHit(ctx context.Context) {
	log.CtxDebug(ctx, "oci cache blob hit")
	updateCacheEventMetric(metrics.OCIBlobResourceTypeLabel, metrics.HitStatusLabel)
}

func FetchBlobFromCache(ctx context.Context, w io.Writer, bsClient bspb.ByteStreamClient, hash gcr.Hash, contentLength int64) error {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		cacheDigestFunction,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	counter := &ioutil.Counter{}
	mw := io.MultiWriter(w, counter)
	defer func() {
		metrics.OCIRegistryCacheDownloadSizeBytes.With(prometheus.Labels{
			metrics.OCIResourceTypeLabel: metrics.OCIBlobResourceTypeLabel,
		}).Observe(float64(counter.Count()))
	}()
	if err := cachetools.GetBlob(ctx, bsClient, blobRN, mw); err != nil {
		blobMiss(ctx)
		return err
	}
	blobHit(ctx)
	return nil
}

func writeBlobMetadataToCache(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, contentLength int64) error {
	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", cacheDigestFunction, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      repo.RegistryStr(),
		Repository:    repo.RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   blobOutputFilePath,
				Digest: blobCASDigest,
			},
			{
				Path:   blobMetadataOutputFilePath,
				Digest: blobMetadataCASDigest,
			},
		},
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	return cachetools.UploadActionResult(ctx, acClient, arRN, ar)
}

func WriteBlobToCache(ctx context.Context, r io.Reader, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, contentLength int64) error {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		cacheDigestFunction,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, r)
	if err != nil {
		return err
	}
	return writeBlobMetadataToCache(ctx, bsClient, acClient, repo, hash, contentType, contentLength)
}

// newBlobUploader creates a CommittedWriteCloser that writes OCI blobs to the CAS.
//
// Once contentLength bytes have been written, the blobUploader will commit the blob.
// It is an error to attempt to Write after commit, and to write more than contentLength bytes.
func newBlobUploader(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, contentLength int64) (interfaces.CommittedWriteCloser, error) {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		cacheDigestFunction,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	uw, err := cachetools.NewUploadWriter(ctx, bsClient, blobRN)
	if err != nil {
		return nil, err
	}

	return &blobUploader{
		uw:            uw,
		ctx:           ctx,
		bsClient:      bsClient,
		acClient:      acClient,
		repo:          repo,
		hash:          hash,
		contentType:   contentType,
		contentLength: contentLength,
	}, nil
}

type blobUploader struct {
	uw *cachetools.UploadWriter

	ctx      context.Context
	bsClient bspb.ByteStreamClient
	acClient repb.ActionCacheClient

	repo          gcrname.Repository
	hash          gcr.Hash
	contentType   string
	contentLength int64

	committed bool
}

func (b *blobUploader) Write(p []byte) (int, error) {
	if b.committed {
		return 0, status.FailedPreconditionError("blobUploader already committed, cannot receive writes")
	}
	return b.uw.Write(p)
}

func (b *blobUploader) Commit() error {
	if b.committed {
		return status.FailedPreconditionError("blobUploader already committed, cannot commit again")
	}
	b.committed = true
	if err := b.uw.Commit(); err != nil {
		return err
	}
	return writeBlobMetadataToCache(
		b.ctx,
		b.bsClient,
		b.acClient,
		b.repo,
		b.hash,
		b.contentType,
		b.contentLength,
	)
}

func (b *blobUploader) Close() error {
	return b.uw.Close()
}

// newBlobReadThroughCacher creates a ReadCloser that will write bytes to the CAS as they are read from the input ReadCloser.
// Any errors writing to the CAS will be logged and ignored.
//
// Closing the ReadThroughCacher closes the input ReadCloser and the underlying BlobUploader.
func newBlobReadThroughCacher(ctx context.Context, rc io.ReadCloser, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, contentLength int64) (io.ReadCloser, error) {
	cache, err := newBlobUploader(ctx, bsClient, acClient, repo, hash, contentType, contentLength)
	if err != nil {
		return nil, err
	}
	return &readThroughCacher{
		rc:    rc,
		cache: cache,
	}, nil
}

type readThroughCacher struct {
	rc    io.ReadCloser
	cache interfaces.CommittedWriteCloser

	cacheErr error
}

func (r *readThroughCacher) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if r.cacheErr != nil {
		return n, err
	}

	if n > 0 {
		written, writeErr := r.cache.Write(p[:n])
		r.cacheErr = writeErr
		if r.cacheErr == nil {
			if written < n {
				log.Warningf("Short write to cache. Wanted %v, wrote %v", n, written)
				r.cacheErr = io.ErrShortWrite
			}
		} else if !status.IsAlreadyExistsError(r.cacheErr) {
			log.Warningf("Error writing to cache: %s", r.cacheErr)
		}
	}

	if err == io.EOF && r.cacheErr == nil {
		r.cacheErr = r.cache.Commit()
		if r.cacheErr != nil {
			log.Warningf("Error committing blob to cache: %s", r.cacheErr)
		}
	}

	return n, err
}

func (r *readThroughCacher) Close() error {
	err := r.rc.Close()
	if err := r.cache.Close(); err != nil {
		log.Warningf("Error closing cache writer: %s", err)
	}
	return err
}

func WriteBlobOrManifestToCacheAndWriter(ctx context.Context, upstream io.Reader, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, ociResourceType ocipb.OCIResourceType, hash gcr.Hash, contentType string, contentLength int64, originalRef gcrname.Reference) error {
	if ociResourceType == ocipb.OCIResourceType_MANIFEST {
		if contentLength > maxManifestSize {
			return status.FailedPreconditionErrorf("manifest too large (%d bytes) to write to cache (limit %d bytes)", contentLength, maxManifestSize)
		}
		buf := bytes.NewBuffer(make([]byte, 0, contentLength))
		mw := io.MultiWriter(w, buf)
		written, err := io.Copy(mw, io.LimitReader(upstream, contentLength))
		if err != nil {
			return err
		}
		if written != contentLength {
			return status.DataLossErrorf("expected manifest of length %d, only able to write %d bytes", contentLength, written)
		}
		return WriteManifestToAC(ctx, buf.Bytes(), acClient, repo, hash, contentType, originalRef)
	}
	tr := io.TeeReader(upstream, w)
	return WriteBlobToCache(ctx, tr, bsClient, acClient, repo, hash, contentType, contentLength)
}

// OCICache provides an interface for reading OCI blobs from the CAS with fallback to upstream registry.
type OCICache interface {
	// TeeBlob reads the blob from the CAS, falling back to the upstream registry if not present.
	// The reference should be in the format: registry/repository@sha256:hash
	// Returns an io.ReadCloser for the blob content.
	// When fetching from upstream, TeeBlob automatically caches the blob to the CAS.
	// Authentication and transport are handled by the LayerFetcher provided to NewOCICache.
	TeeBlob(ctx context.Context, reference string) (io.ReadCloser, error)
}

// LayerFetcher provides a way to fetch layers from an upstream registry.
// The reference must be a digest reference (not a tag).
type LayerFetcher interface {
	Layer(context.Context, gcrname.Digest) (gcr.Layer, error)
}

// ociCache implements the OCICache interface.
type ociCache struct {
	bsClient     bspb.ByteStreamClient
	acClient     repb.ActionCacheClient
	layerFetcher LayerFetcher
}

// NewOCICache creates a new OCICache instance.
func NewOCICache(bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, layerFetcher LayerFetcher) OCICache {
	return &ociCache{
		bsClient:     bsClient,
		acClient:     acClient,
		layerFetcher: layerFetcher,
	}
}

// TeeBlob reads the blob from the CAS, falling back to the upstream registry if not present.
func (c *ociCache) TeeBlob(ctx context.Context, reference string) (io.ReadCloser, error) {
	// Parse the reference to extract repository and digest information
	ref, err := gcrname.ParseReference(reference)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid reference %q: %s", reference, err)
	}

	// Extract the digest from the reference
	digest, ok := ref.(gcrname.Digest)
	if !ok {
		return nil, status.InvalidArgumentErrorf("reference %q must be a digest, not a tag", reference)
	}

	hash, err := gcr.NewHash(digest.DigestStr())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid digest in reference %q: %s", reference, err)
	}

	// Create a pipe for streaming the blob content
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		// Try to get metadata first to know the blob size
		metadata, metaErr := FetchBlobMetadataFromCache(ctx, c.bsClient, c.acClient, ref.Context(), hash)
		if metaErr == nil {
			// Metadata found - try to read the blob from CAS
			err := FetchBlobFromCache(ctx, pw, c.bsClient, hash, metadata.GetContentLength())
			if err == nil {
				// CAS hit - blob was successfully fetched
				blobHit(ctx)
				return
			}
			// Blob not in CAS even though metadata exists - fall through to upstream fetch
			log.CtxWarningf(ctx, "Blob metadata found but blob not in CAS: %s", err)
		}

		// Cache miss or error - fall back to upstream
		if status.IsNotFoundError(metaErr) {
			blobMiss(ctx)
		} else if metaErr != nil {
			log.CtxDebugf(ctx, "Error fetching blob metadata from cache: %s", metaErr)
		}

		// Fetch layer from upstream using the layer fetcher (which handles auth and transport)
		remoteLayer, layerErr := c.layerFetcher.Layer(ctx, digest)
		if layerErr != nil {
			// Preserve specific error types (auth failures, not found, etc.) so callers
			// can distinguish them from transient unavailability
			if t, ok := layerErr.(*transport.Error); ok {
				switch t.StatusCode {
				case http.StatusUnauthorized, http.StatusForbidden:
					pw.CloseWithError(status.PermissionDeniedErrorf("not authorized to fetch layer: %s", layerErr))
				case http.StatusNotFound:
					pw.CloseWithError(status.NotFoundErrorf("layer not found: %s", layerErr))
				default:
					pw.CloseWithError(status.UnavailableErrorf("failed to fetch layer from upstream: %s", layerErr))
				}
			} else {
				// For non-transport errors, wrap as unavailable
				pw.CloseWithError(status.UnavailableErrorf("failed to fetch layer from upstream: %s", layerErr))
			}
			return
		}

		// Get the compressed blob content
		upstreamReader, readerErr := remoteLayer.Compressed()
		if readerErr != nil {
			pw.CloseWithError(status.UnavailableErrorf("failed to get compressed layer: %s", readerErr))
			return
		}
		defer upstreamReader.Close()

		// Get metadata about the layer
		size, sizeErr := remoteLayer.Size()
		if sizeErr != nil {
			log.CtxWarningf(ctx, "Failed to get layer size: %s", sizeErr)
			size = 0
		}

		mediaType, mediaTypeErr := remoteLayer.MediaType()
		if mediaTypeErr != nil {
			log.CtxWarningf(ctx, "Failed to get layer media type: %s", mediaTypeErr)
		}
		contentType := string(mediaType)
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		// Wrap with read-through cacher to cache as we stream
		cachedReader, cacheErr := newBlobReadThroughCacher(
			ctx,
			upstreamReader,
			c.bsClient,
			c.acClient,
			ref.Context(),
			hash,
			contentType,
			size,
		)
		if cacheErr != nil {
			log.CtxWarningf(ctx, "Error creating read-through cacher: %s", cacheErr)
			// Fall back to direct copy without caching
			_, copyErr := io.Copy(pw, upstreamReader)
			if copyErr != nil {
				pw.CloseWithError(copyErr)
			}
			return
		}
		defer cachedReader.Close()

		// Copy the cached reader content to the pipe
		_, copyErr := io.Copy(pw, cachedReader)
		if copyErr != nil {
			pw.CloseWithError(copyErr)
			return
		}
	}()

	return pr, nil
}
