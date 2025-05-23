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

	hitLabel    = "hit"
	missLabel   = "miss"
	uploadLabel = "upload"

	actionCacheLabel = "action_cache"
	casLabel         = "cas"

	cacheDigestFunction = repb.DigestFunction_SHA256
)

func WriteManifestToAC(ctx context.Context, raw []byte, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string) error {
	arRN, err := manifestACKey(ref, hash)
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
	updateCacheEventMetric(actionCacheLabel, uploadLabel)
	return cachetools.UploadActionResult(ctx, acClient, arRN, ar)
}

func updateCacheEventMetric(cacheType, eventType string) {
	metrics.OCIRegistryCacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      cacheType,
		metrics.CacheEventTypeLabel: eventType,
	}).Inc()
}

func FetchManifestFromAC(ctx context.Context, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash) (*ocipb.OCIManifestContent, error) {
	arRN, err := manifestACKey(ref, hash)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	meta := ar.GetExecutionMetadata()
	if meta == nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		log.CtxWarningf(ctx, "Missing execution metadata for manifest in %q", ref.Context())
		return nil, status.InternalErrorf("missing execution metadata for manifest in %q", ref.Context())
	}
	aux := meta.GetAuxiliaryMetadata()
	if aux == nil || len(aux) != 1 {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		log.CtxWarningf(ctx, "Missing auxiliary metadata for manifest in %q", ref.Context())
		return nil, status.InternalErrorf("missing auxiliary metadata for manifest in %q", ref.Context())
	}
	any := aux[0]
	var mc ocipb.OCIManifestContent
	err = any.UnmarshalTo(&mc)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, status.InternalErrorf("could not unmarshal metadata for manifest in %q: %s", ref.Context(), err)
	}
	updateCacheEventMetric(actionCacheLabel, hitLabel)
	return &mc, nil
}

func manifestACKey(ref gcrname.Reference, refhash gcr.Hash) (*digest.ACResourceName, error) {
	s := hash.Strings(
		ref.Context().RegistryStr(),
		ref.Context().RepositoryStr(),
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

func FetchBlobMetadataFromCache(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference) (*ocipb.OCIBlobMetadata, error) {
	hash, err := gcr.NewHash(ref.Identifier())
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	updateCacheEventMetric(actionCacheLabel, hitLabel)

	var blobMetadataCASDigest *repb.Digest
	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case blobMetadataOutputFilePath:
			blobMetadataCASDigest = outputFile.GetDigest()
		case blobOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "Unknown output file path %q in ActionResult for %q", outputFile.GetPath(), ref.Context())
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		updateCacheEventMetric(casLabel, missLabel)
		return nil, status.NotFoundErrorf("missing blob metadata digest or blob digest for %s", ref.Context())
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		cacheDigestFunction,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsClient, blobMetadataRN, blobMetadata)
	if err != nil {
		updateCacheEventMetric(casLabel, missLabel)
		return nil, err
	}
	updateCacheEventMetric(casLabel, hitLabel)
	return blobMetadata, nil
}

func FetchBlobFromCache(ctx context.Context, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, hash gcr.Hash, contentLength int64) error {
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
			metrics.CacheTypeLabel: casLabel,
		}).Observe(float64(counter.Count()))
	}()
	if err := cachetools.GetBlob(ctx, bsClient, blobRN, mw); err != nil {
		updateCacheEventMetric(casLabel, missLabel)
		return err
	}
	updateCacheEventMetric(casLabel, hitLabel)
	return nil
}

// WriteManifestToCacheAndResponse reads the entire manifest from the upstream remote registry,
// and writes the contents to both the response and to the AC.
func WriteManifestToCacheAndResponse(ctx context.Context, upstream io.Reader, response io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string, contentLength int64) error {
	if contentLength > maxManifestSize {
		return status.FailedPreconditionErrorf("manifest too large (%d bytes) to write to cache (limit %d bytes)", contentLength, maxManifestSize)
	}
	buf := bytes.NewBuffer(make([]byte, 0, contentLength))
	mw := io.MultiWriter(response, buf)
	written, err := io.Copy(mw, io.LimitReader(upstream, contentLength))
	if err != nil {
		return err
	}
	if written != contentLength {
		return status.DataLossErrorf("expected manifest of length %d, only able to write %d bytes", contentLength, written)
	}
	return WriteManifestToAC(ctx, buf.Bytes(), acClient, ref, hash, contentType)
}

// WriteBlobToCacheAndResponse reads an OCI blob (an OCI image layer) from the upstream remote registry
// and writes those bytes to the response. It also makes a best-effort attempt to write the blob to the CAS.
// Failure to write the blob to the CAS should not impact writing the blob to the response.
func WriteBlobToCacheAndResponse(ctx context.Context, upstream io.Reader, response io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string, contentLength int64) error {
	uploader, err := NewBlobUploader(ctx, bsClient, acClient, ref, hash, contentType, contentLength)
	if err != nil {
		return err
	}
	defer uploader.Close()
	dst := &writeThroughCacher{
		primary: response,
		cacher:  uploader,
	}
	if _, err := io.Copy(dst, upstream); err != nil {
		return err
	}
	return uploader.Commit()
}

type blobUploader struct {
	ref           gcrname.Reference
	hash          gcr.Hash
	contentType   string
	contentLength int64

	writer *cachetools.UploadWriter

	ctx      context.Context
	bsClient bspb.ByteStreamClient
	acClient repb.ActionCacheClient
}

func (up *blobUploader) Write(p []byte) (int, error) {
	return up.writer.Write(p)
}

func (up *blobUploader) Commit() error {
	if err := up.writer.Commit(); err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: up.contentLength,
		ContentType:   up.contentType,
	}
	updateCacheEventMetric(casLabel, uploadLabel)
	blobMetadataCASDigest, err := cachetools.UploadProto(up.ctx, up.bsClient, "", cacheDigestFunction, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      up.ref.Context().RegistryStr(),
		Repository:    up.ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: up.hash.Algorithm,
		HashHex:       up.hash.Hex,
	}
	blobCASDigest := &repb.Digest{
		Hash:      up.hash.Hex,
		SizeBytes: up.contentLength,
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
	updateCacheEventMetric(actionCacheLabel, uploadLabel)
	return cachetools.UploadActionResult(up.ctx, up.acClient, arRN, ar)
}

func (up *blobUploader) Close() error {
	return up.writer.Close()
}

func NewBlobUploader(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string, contentLength int64) (interfaces.CommittedWriteCloser, error) {
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
	updateCacheEventMetric(casLabel, uploadLabel)
	uw, err := cachetools.NewUploadWriter(ctx, bsClient, blobRN)
	if err != nil {
		return nil, err
	}
	return &blobUploader{
		ref:           ref,
		hash:          hash,
		contentType:   contentType,
		contentLength: contentLength,

		writer: uw,

		ctx:      ctx,
		bsClient: bsClient,
		acClient: acClient,
	}, nil
}

type writeThroughCacher struct {
	primary io.Writer
	cacher  io.Writer
}

func (w *writeThroughCacher) Write(p []byte) (int, error) {
	n, err := w.primary.Write(p)
	if n > 0 {
		w.cacher.Write(p[:n]) // Writing to the cache is best-effort, so ignore any errors.
	}
	return n, err
}
