package imgcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	headerAccept              = "Accept"
	headerContentType         = "Content-Type"
	headerDockerContentDigest = "Docker-Content-Digest"
	headerContentLength       = "Content-Length"
	headerAuthorization       = "Authorization"
	headerWWWAuthenticate     = "WWW-Authenticate"
	headerRange               = "Range"

	blobOutputFilePath         = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath = "_bb_ociregistry_blob_metadata_"
	actionResultInstanceName   = "_bb_ociregistry_"
)

func fetchBlobOrManifestFromCache(ctx context.Context, w http.ResponseWriter, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, writeBody bool) error {
	hash, err := gcr.NewHash(ref.Identifier())
	if err != nil {
		return err
	}
	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ociResourceType,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		return err
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
			log.CtxErrorf(ctx, "unknown output file path '%s' in ActionResult for %s", outputFile.GetPath(), ref)
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		return fmt.Errorf("missing blob metadata digest or blob digest for %s", ref)
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsClient, blobMetadataRN, blobMetadata)
	if err != nil {
		return err
	}
	w.Header().Add(headerDockerContentDigest, hash.String())
	w.Header().Add(headerContentLength, strconv.FormatInt(blobMetadata.GetContentLength(), 10))
	w.Header().Add(headerContentType, blobMetadata.GetContentType())
	w.WriteHeader(http.StatusOK)

	if writeBody {
		blobRN := digest.NewCASResourceName(
			blobCASDigest,
			"",
			repb.DigestFunction_SHA256,
		)
		blobRN.SetCompressor(repb.Compressor_ZSTD)
		return cachetools.GetBlob(ctx, bsClient, blobRN, w)
	} else {
		return nil
	}
}

func writeBlobOrManifestToCacheAndResponse(ctx context.Context, upstream io.Reader, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, hash gcr.Hash, contentType string, contentLength int64) error {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	tr := io.TeeReader(upstream, w)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, tr)
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ociResourceType,
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
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}
