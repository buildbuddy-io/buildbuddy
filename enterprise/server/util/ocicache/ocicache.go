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
	"github.com/google/go-containerregistry/pkg/authn"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
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

// Credentials encapsulates optional registry credentials used to authenticate
// requests to upstream registries.
type Credentials struct {
	Username string
	Password string
}

// IsEmpty reports whether the credential set contains any authentication data.
func (c Credentials) IsEmpty() bool {
	return c.Username == "" && c.Password == ""
}

type remoteFetcher func(ctx context.Context, ref gcrname.Digest, creds Credentials) (io.ReadCloser, string, int64, error)

// OCICache coordinates reading and populating OCI blobs in the CAS.
type OCICache struct {
	bsClient    bspb.ByteStreamClient
	acClient    repb.ActionCacheClient
	fetchRemote remoteFetcher
}

// NewOCICache returns an OCICache backed by the provided CAS clients.
func NewOCICache(bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) *OCICache {
	return (&OCICache{
		bsClient: bsClient,
		acClient: acClient,
	}).WithRemoteFetcher(defaultRemoteFetcher)
}

// WithRemoteFetcher configures a custom function used to fetch blobs from the
// upstream registry when they are not present in the cache.
func (c *OCICache) WithRemoteFetcher(fetcher remoteFetcher) *OCICache {
	if c == nil {
		return nil
	}
	clone := &OCICache{
		bsClient:    c.bsClient,
		acClient:    c.acClient,
		fetchRemote: c.fetchRemote,
	}
	if fetcher != nil {
		clone.fetchRemote = fetcher
	}
	return clone
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

func defaultRemoteFetcher(ctx context.Context, ref gcrname.Digest, creds Credentials) (io.ReadCloser, string, int64, error) {
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if !creds.IsEmpty() {
		remoteOpts = append(remoteOpts, remote.WithAuth(&authn.Basic{
			Username: creds.Username,
			Password: creds.Password,
		}))
	}

	layer, err := remote.Layer(ref, remoteOpts...)
	if err != nil {
		return nil, "", 0, err
	}
	mediaType, err := layer.MediaType()
	if err != nil {
		return nil, "", 0, err
	}
	contentLength, err := layer.Size()
	if err != nil {
		return nil, "", 0, err
	}
	upstream, err := layer.Compressed()
	if err != nil {
		return nil, "", 0, err
	}
	return upstream, string(mediaType), contentLength, nil
}

// TeeBlob returns a reader for the requested blob, writing bytes into the CAS
// as they are consumed. If the blob is already cached it is streamed directly
// from the cache; otherwise it is fetched from the upstream registry using the
// provided credentials and written back to the cache.
func (c *OCICache) TeeBlob(ctx context.Context, ref gcrname.Digest, creds Credentials) (io.ReadCloser, error) {
	if c == nil || c.bsClient == nil || c.acClient == nil {
		return nil, status.FailedPreconditionError("OCICache requires configured ByteStream and ActionCache clients")
	}

	repo := ref.Context()
	hash, err := gcr.NewHash(ref.DigestStr())
	if err != nil {
		return nil, status.WrapError(err, "invalid digest reference")
	}

	metadata, err := FetchBlobMetadataFromCache(ctx, c.bsClient, c.acClient, repo, hash)
	if err == nil {
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			if fetchErr := FetchBlobFromCache(ctx, pw, c.bsClient, hash, metadata.GetContentLength()); fetchErr != nil {
				pw.CloseWithError(fetchErr)
			}
		}()
		return pr, nil
	}
	if !status.IsNotFoundError(err) {
		return nil, err
	}

	if c.fetchRemote == nil {
		return nil, status.UnimplementedError("no remote fetcher configured")
	}

	upstream, mediaType, contentLength, err := c.fetchRemote(ctx, ref, creds)
	if err != nil {
		return nil, err
	}

	rc, err := NewBlobReadThroughCacher(
		ctx,
		upstream,
		c.bsClient,
		c.acClient,
		repo,
		hash,
		mediaType,
		contentLength,
	)
	if err != nil {
		upstream.Close()
		return nil, err
	}
	return rc, nil
}
