package ocicache

import (
	"bytes"
	"context"
	"io"

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

func WriteManifestToAC(ctx context.Context, raw []byte, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, contentType string) error {
	arRN, err := manifestACKey(repo, hash)
	if err != nil {
		return err
	}

	m := &ocipb.OCIManifestContent{
		Raw:         raw,
		ContentType: contentType,
	}
	any, err := anypb.New(m)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				any,
			},
		},
	}
	return cachetools.UploadActionResult(ctx, acClient, arRN, ar)
}

func updateCacheEventMetric(ociResourceTypeLabel, cacheEventType string) {
	metrics.OCIRegistryCacheEvents.With(prometheus.Labels{
		metrics.OCIResourceTypeLabel: ociResourceTypeLabel,
		metrics.CacheEventTypeLabel:  cacheEventType,
	}).Inc()
}

func manifestMiss(ctx context.Context, repo gcrname.Repository, hash gcr.Hash) {
	log.CtxInfof(ctx, "OCI cache manifest miss in %s:%s", repo, hash)
	updateCacheEventMetric(metrics.OCIManifestResourceTypeLabel, metrics.MissStatusLabel)
}

func manifestHit(ctx context.Context, repo gcrname.Repository, hash gcr.Hash) {
	log.CtxInfof(ctx, "OCI cache manifest hit %s:%s", repo, hash)
	updateCacheEventMetric(metrics.OCIManifestResourceTypeLabel, metrics.HitStatusLabel)
}

func FetchManifestFromAC(ctx context.Context, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash) (*ocipb.OCIManifestContent, error) {
	arRN, err := manifestACKey(repo, hash)
	if err != nil {
		manifestMiss(ctx, repo, hash)
		log.CtxWarningf(ctx, "Error creating key for manifest in %q: %s", repo, err)
		return nil, err
	}
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		manifestMiss(ctx, repo, hash)
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error getting action result for manifest in %q: %s", repo, err)
		}
		return nil, err
	}
	meta := ar.GetExecutionMetadata()
	if meta == nil {
		manifestMiss(ctx, repo, hash)
		log.CtxWarningf(ctx, "Missing execution metadata for manifest in %q", repo)
		return nil, status.InternalErrorf("missing execution metadata for manifest in %q", repo)
	}
	aux := meta.GetAuxiliaryMetadata()
	if aux == nil || len(aux) != 1 {
		manifestMiss(ctx, repo, hash)
		log.CtxWarningf(ctx, "Missing auxiliary metadata for manifest in %q", repo)
		return nil, status.InternalErrorf("missing auxiliary metadata for manifest in %q", repo)
	}
	any := aux[0]
	var mc ocipb.OCIManifestContent
	err = any.UnmarshalTo(&mc)
	if err != nil {
		manifestMiss(ctx, repo, hash)
		log.CtxWarningf(ctx, "Error unmarshalling manifest content in %q: %s", repo, err)
		return nil, status.InternalErrorf("could not unmarshal metadata for manifest in %q: %s", repo, err)
	}
	manifestHit(ctx, repo, hash)
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
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
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
		return nil, err
	}
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
		if writeErr != nil {
			log.Warningf("Error writing to cache: %s", writeErr)
			r.cacheErr = writeErr
			return n, err
		}
		if written < n {
			r.cacheErr = io.ErrShortWrite
			return n, err
		}
	}

	if err == io.EOF {
		if err := r.cache.Commit(); err != nil {
			log.Warningf("Error committing blob to cache: %s", err)
			r.cacheErr = err
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

func WriteBlobOrManifestToCacheAndWriter(ctx context.Context, upstream io.Reader, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, ociResourceType ocipb.OCIResourceType, hash gcr.Hash, contentType string, contentLength int64) error {
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
		return WriteManifestToAC(ctx, buf.Bytes(), acClient, repo, hash, contentType)
	}
	tr := io.TeeReader(upstream, w)
	return WriteBlobToCache(ctx, tr, bsClient, acClient, repo, hash, contentType, contentLength)
}
