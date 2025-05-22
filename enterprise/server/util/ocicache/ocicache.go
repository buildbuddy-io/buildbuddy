package ocicache

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
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

	chunkBufSizeBytes = 1000000 // 1MB
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

func WriteBlobToCache(ctx context.Context, r io.Reader, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string, contentLength int64) error {
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
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, r)
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	updateCacheEventMetric(casLabel, uploadLabel)
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", cacheDigestFunction, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
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
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

func WriteBlobOrManifestToCacheAndWriter(ctx context.Context, upstream io.Reader, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, hash gcr.Hash, contentType string, contentLength int64) error {
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
		return WriteManifestToAC(ctx, buf.Bytes(), acClient, ref, hash, contentType)
	}
	tr := io.TeeReader(upstream, w)
	return WriteBlobToCache(ctx, tr, bsClient, acClient, ref, hash, contentType, contentLength)
}

type blobUploadWriter struct {
	writer interfaces.CommittedWriteCloser

	ctx      context.Context
	acClient repb.ActionCacheClient
	bsClient bspb.ByteStreamClient

	contentLength int64
	contentType   string
	ref           gcrname.Reference
	hash          gcr.Hash
	blobCASDigest *repb.Digest
}

func (w *blobUploadWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *blobUploadWriter) Commit() error {
	if err := w.writer.Commit(); err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: w.contentLength,
		ContentType:   w.contentType,
	}
	updateCacheEventMetric(casLabel, uploadLabel)
	blobMetadataCASDigest, err := cachetools.UploadProto(w.ctx, w.bsClient, "", cacheDigestFunction, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      w.ref.Context().RegistryStr(),
		Repository:    w.ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: w.hash.Algorithm,
		HashHex:       w.hash.Hex,
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   blobOutputFilePath,
				Digest: w.blobCASDigest,
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
	err = cachetools.UploadActionResult(w.ctx, w.acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

func (w *blobUploadWriter) Close() error {
	return w.writer.Close()
}

type cachingReadCloser struct {
	reader io.ReadCloser
	writer interfaces.CommittedWriteCloser

	readErr  error
	writeErr error
}

func (rc *cachingReadCloser) Read(p []byte) (int, error) {
	if rc.readErr != nil {
		return 0, status.WrapError(rc.readErr, "Reader already encountered error, cannot read further")
	}
	n, err := rc.reader.Read(p)
	if err != nil {
		rc.readErr = err
		return n, err
	}
	if rc.writeErr != nil {
		return n, err
	}
	written, err := rc.writer.Write(p[:n])
	if err != nil {
		rc.writeErr = err
		log.Warningf("Error writing OCI image blob to the cache: %s", err)
		return n, nil
	}
	if written < n {
		rc.writeErr = status.DataLossErrorf("attempted to write %d OCI image blob bytes to the cache, only wrote %d bytes", n, written)
	}
	return n, nil
}

func (rc *cachingReadCloser) Close() error {
	err := rc.reader.Close()
	if err := rc.writer.Commit(); err != nil {
		log.Warningf("Could not commit OCI image blob to the cache: %s", err)
	}
	if err := rc.writer.Close(); err != nil {
		log.Warningf("Could not close cache upload: %s", err)
	}
	return err
}

func NewCachingReadCloser(ctx context.Context, upstream io.ReadCloser, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string, contentLength int64) (io.ReadCloser, error) {
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

	rbuf := make([]byte, 0, chunkBufSizeBytes)
	cbuf := make([]byte, 0, chunkBufSizeBytes)
	reader, err := compression.NewZstdCompressingReader(upstream, rbuf[:chunkBufSizeBytes], cbuf[:chunkBufSizeBytes])
	if err != nil {
		return nil, status.InternalErrorf("cannot compress OCI image blob for caching: %s", err)
	}

	writer, err := cachetools.NewUploadWriter(ctx, bsClient, blobRN)
	if err != nil {
		return nil, err
	}
	uploader := &blobUploadWriter{
		writer: writer,

		ctx:      ctx,
		acClient: acClient,
		bsClient: bsClient,

		contentLength: contentLength,
		contentType:   contentType,
		ref:           ref,
		hash:          hash,
		blobCASDigest: blobCASDigest,
	}
	return &cachingReadCloser{
		reader: reader,
		writer: uploader,
	}, nil
}
