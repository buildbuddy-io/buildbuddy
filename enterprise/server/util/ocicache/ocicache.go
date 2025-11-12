package ocicache

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
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
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/anypb"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"

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

// Fetcher is an interface for fetching OCI blobs and manifests from an upstream registry.
type Fetcher interface {
	// Head makes a HEAD request to fetch metadata about a manifest.
	Head(ctx context.Context, ref gcrname.Reference) (*gcr.Descriptor, error)
	// Get fetches a manifest from the upstream registry.
	Get(ctx context.Context, ref gcrname.Reference) (*remote.Descriptor, error)
	// Layer fetches a layer (blob) from the upstream registry.
	Layer(ctx context.Context, ref gcrname.Digest) (gcr.Layer, error)
}

type readThroughFetcher struct {
	puller   *remote.Puller
	acClient repb.ActionCacheClient
	bsClient bspb.ByteStreamClient
	useCache bool
}

// NewFetcher creates a Fetcher that fetches OCI manifest from the AC and blobs from the CAS.
// If a manifest is not in the AC or a blob is not in the CAS, it is fetched from the upstream registry
// and written to the AC or CAS.
// If useCache is false, caching is disabled and all requests go directly to the upstream registry.
func NewFetcher(env environment.Env, useCache bool, opts ...remote.Option) (Fetcher, error) {
	puller, err := remote.NewPuller(opts...)
	if err != nil {
		return nil, err
	}
	return &readThroughFetcher{
		puller:   puller,
		acClient: env.GetActionCacheClient(),
		bsClient: env.GetByteStreamClient(),
		useCache: useCache,
	}, nil
}

func (c *readThroughFetcher) Head(ctx context.Context, ref gcrname.Reference) (*gcr.Descriptor, error) {
	return c.puller.Head(ctx, ref)
}

func (c *readThroughFetcher) Get(ctx context.Context, ref gcrname.Reference) (*remote.Descriptor, error) {
	digestRef := ref
	hash, err := gcr.NewHash(ref.Identifier())
	if err != nil {
		desc, err := c.puller.Head(ctx, ref)
		if err != nil {
			return nil, err
		}
		hash = desc.Digest
		digestRef = ref.Context().Digest(hash.String())
	}

	if c.useCache {
		mc, err := FetchManifestFromAC(ctx, c.acClient, digestRef.Context(), hash, ref)
		if err == nil {
			desc := &remote.Descriptor{
				Descriptor: gcr.Descriptor{
					MediaType: types.MediaType(mc.ContentType),
					Size:      int64(len(mc.Raw)),
					Digest:    hash,
				},
				Manifest: mc.Raw,
			}
			return desc, nil
		}
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error fetching manifest from AC for %s: %s", ref, err)
		}
	}

	desc, err := c.puller.Get(ctx, digestRef)
	if err != nil {
		return nil, err
	}

	if c.useCache {
		if err := WriteManifestToAC(ctx, desc.Manifest, c.acClient, digestRef.Context(), hash, string(desc.MediaType), ref); err != nil {
			log.CtxWarningf(ctx, "Error writing manifest to AC for %s: %s", ref, err)
		}
	}

	return desc, nil
}

func (c *readThroughFetcher) Layer(ctx context.Context, ref gcrname.Digest) (gcr.Layer, error) {
	return c.puller.Layer(ctx, ref)
}

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

// NewBlobUploader creates a CommittedWriteCloser that writes OCI blobs to the CAS.
//
// Once contentLength bytes have been written, the blobUploader will commit the blob.
// It is an error to attempt to Write after commit, and to write more than contentLength bytes.
func NewBlobUploader(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, contentLength int64) (interfaces.CommittedWriteCloser, error) {
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

// NewBlobReadThroughCacher creates a ReadCloser that will write bytes to the CAS as they are read from the input ReadCloser.
// Any errors writing to the CAS will be logged and ignored.
//
// Closing the ReadThroughCacher closes the input ReadCloser and the underlying BlobUploader.
func NewBlobReadThroughCacher(ctx context.Context, rc io.ReadCloser, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string, contentLength int64) (io.ReadCloser, error) {
	cache, err := NewBlobUploader(ctx, bsClient, acClient, repo, hash, contentType, contentLength)
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
