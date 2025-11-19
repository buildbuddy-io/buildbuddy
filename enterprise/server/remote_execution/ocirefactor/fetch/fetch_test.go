package fetch_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/ocirefactor/fetch"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	actionResultInstanceName    = interfaces.OCIImageInstanceNamePrefix
	manifestContentInstanceName = interfaces.OCIImageInstanceNamePrefix + "_manifest_content_"
	cacheDigestFunction         = repb.DigestFunction_SHA256
	blobOutputFilePath          = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath  = "_bb_ociregistry_blob_metadata_"
)

func setupTestEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, interfaces.ClientIdentityApp)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te
}

// writeTestManifest writes a manifest to the Action Cache for testing.
// Copied and adapted from ocicache.WriteManifestToAC.
func writeTestManifest(t *testing.T, ctx context.Context, acClient repb.ActionCacheClient, ref string, manifestBytes []byte, contentType string, secret string) {
	digestRef, err := ctrname.NewDigest(ref)
	require.NoError(t, err)

	repo := digestRef.Context()
	refhash, err := ctr.NewHash(digestRef.DigestStr())
	require.NoError(t, err)

	// Build cache key
	s := hash.Strings(
		repo.RegistryStr(),
		repo.RepositoryStr(),
		ocipb.OCIResourceType_MANIFEST.String(),
		refhash.Algorithm,
		refhash.Hex,
		secret,
	)
	arDigest, err := digest.Compute(bytes.NewBufferString(s), cacheDigestFunction)
	require.NoError(t, err)
	arRN := digest.NewACResourceName(
		arDigest,
		manifestContentInstanceName,
		cacheDigestFunction,
	)

	// Create manifest content proto
	m := &ocipb.OCIManifestContent{
		Raw:         manifestBytes,
		ContentType: contentType,
	}
	any, err := anypb.New(m)
	require.NoError(t, err)

	// Upload to AC
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{any},
		},
	}
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	require.NoError(t, err)
}

// writeTestBlob writes a blob and its metadata to the cache for testing.
// Copied and adapted from ocicache.WriteBlobToCache and writeBlobMetadataToCache.
func writeTestBlob(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref string, blobData []byte, contentType string, secret string) {
	digestRef, err := ctrname.NewDigest(ref)
	require.NoError(t, err)

	repo := digestRef.Context()
	refhash, err := ctr.NewHash(digestRef.DigestStr())
	require.NoError(t, err)

	contentLength := int64(len(blobData))

	// Write blob data to CAS with ZSTD compression
	blobCASDigest := &repb.Digest{
		Hash:      refhash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		cacheDigestFunction,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, blobRN, bytes.NewReader(blobData))
	require.NoError(t, err)

	// Write blob metadata to CAS
	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", cacheDigestFunction, blobMetadata)
	require.NoError(t, err)

	// Write ActionResult with blob and metadata references
	arKey := &ocipb.OCIActionResultKey{
		Registry:      repo.RegistryStr(),
		Repository:    repo.RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: refhash.Algorithm,
		HashHex:       refhash.Hex,
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
	require.NoError(t, err)
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	require.NoError(t, err)
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	require.NoError(t, err)
}

// createTestManifest creates a test OCI manifest using crane.
func createTestManifest(t *testing.T, imageName string) ([]byte, string, string) {
	image, err := crane.Image(map[string][]byte{
		"/tmp/" + imageName: []byte(imageName),
	})
	require.NoError(t, err)

	raw, err := image.RawManifest()
	require.NoError(t, err)

	mediaType, err := image.MediaType()
	require.NoError(t, err)
	contentType := string(mediaType)

	hash, err := image.Digest()
	require.NoError(t, err)

	ref, err := ctrname.ParseReference("buildbuddy.io/" + imageName)
	require.NoError(t, err)

	digestRef := ref.Context().Digest(hash.String())
	return raw, digestRef.String(), contentType
}

// createTestBlob creates a test OCI blob (layer) using crane.
func createTestBlob(t *testing.T, imageName string, filesize int64) ([]byte, string, string) {
	_, filebuf := testdigest.RandomCASResourceBuf(t, filesize)
	filename, err := random.RandomString(32)
	require.NoError(t, err)

	image, err := crane.Image(map[string][]byte{
		"/tmp/" + filename: filebuf,
	})
	require.NoError(t, err)

	imageRef, err := ctrname.ParseReference("buildbuddy.io/" + imageName)
	require.NoError(t, err)

	layers, err := image.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)

	layer := layers[0]
	hash, err := layer.Digest()
	require.NoError(t, err)

	layerRef := imageRef.Context().Digest(hash.String())

	mediaType, err := layer.MediaType()
	require.NoError(t, err)
	contentType := string(mediaType)

	rc, err := layer.Compressed()
	require.NoError(t, err)
	defer rc.Close()

	layerBuf, err := io.ReadAll(rc)
	require.NoError(t, err)

	return layerBuf, layerRef.String(), contentType
}

func TestCacheFetcher_FetchManifest(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	manifestBytes, ref, contentType := createTestManifest(t, "test_fetch_manifest")
	secret := "test-secret"

	// Write manifest to cache
	writeTestManifest(t, ctx, te.GetActionCacheClient(), ref, manifestBytes, contentType, secret)

	// Create fetcher and fetch manifest
	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), secret)
	fetchedBytes, err := fetcher.FetchManifest(ctx, ref, nil, nil)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(manifestBytes, fetchedBytes))
}

func TestCacheFetcher_FetchManifest_NotFound(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	// Create a ref that doesn't exist in cache
	_, ref, _ := createTestManifest(t, "nonexistent_manifest")

	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), "")
	_, err := fetcher.FetchManifest(ctx, ref, nil, nil)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestCacheFetcher_FetchManifest_SecretMismatch(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	manifestBytes, ref, contentType := createTestManifest(t, "test_secret_mismatch")

	// Write with one secret
	writeTestManifest(t, ctx, te.GetActionCacheClient(), ref, manifestBytes, contentType, "secret-a")

	// Try to fetch with different secret
	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), "secret-b")
	_, err := fetcher.FetchManifest(ctx, ref, nil, nil)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestCacheFetcher_FetchBlobMetadata(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	blobData, ref, contentType := createTestBlob(t, "test_fetch_blob_metadata", 1024)
	secret := ""

	// Write blob to cache
	writeTestBlob(t, ctx, te.GetByteStreamClient(), te.GetActionCacheClient(), ref, blobData, contentType, secret)

	// Fetch metadata
	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), secret)
	size, mediaType, err := fetcher.FetchBlobMetadata(ctx, ref, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(blobData)), size)
	require.Equal(t, contentType, mediaType)
}

func TestCacheFetcher_FetchBlobMetadata_NotFound(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	// Create a ref that doesn't exist in cache
	_, ref, _ := createTestBlob(t, "nonexistent_blob", 1024)

	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), "")
	_, _, err := fetcher.FetchBlobMetadata(ctx, ref, nil)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestCacheFetcher_FetchBlob(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	blobData, ref, contentType := createTestBlob(t, "test_fetch_blob", 2048)
	secret := ""

	// Write blob to cache
	writeTestBlob(t, ctx, te.GetByteStreamClient(), te.GetActionCacheClient(), ref, blobData, contentType, secret)

	// Fetch blob
	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), secret)
	rc, err := fetcher.FetchBlob(ctx, ref, nil)
	require.NoError(t, err)
	defer rc.Close()

	fetchedData, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(blobData, fetchedData))
}

func TestCacheFetcher_FetchBlob_NotFound(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()

	// Create a ref that doesn't exist in cache
	_, ref, _ := createTestBlob(t, "nonexistent_blob_data", 1024)

	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), "")
	_, err := fetcher.FetchBlob(ctx, ref, nil)
	require.Error(t, err)
	// Note: FetchBlob calls FetchBlobMetadata first, so we expect NotFound from that
	require.True(t, status.IsNotFoundError(err))
}

func TestCacheFetcher_RoundTrip(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()
	secret := "round-trip-secret"

	// Test manifest round-trip
	manifestBytes, manifestRef, manifestContentType := createTestManifest(t, "roundtrip_manifest")
	writeTestManifest(t, ctx, te.GetActionCacheClient(), manifestRef, manifestBytes, manifestContentType, secret)

	fetcher := fetch.NewCacheFetcher(te.GetActionCacheClient(), te.GetByteStreamClient(), secret)
	fetchedManifest, err := fetcher.FetchManifest(ctx, manifestRef, nil, nil)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(manifestBytes, fetchedManifest))

	// Test blob round-trip
	blobData, blobRef, blobContentType := createTestBlob(t, "roundtrip_blob", 4096)
	writeTestBlob(t, ctx, te.GetByteStreamClient(), te.GetActionCacheClient(), blobRef, blobData, blobContentType, secret)

	size, mediaType, err := fetcher.FetchBlobMetadata(ctx, blobRef, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(blobData)), size)
	require.Equal(t, blobContentType, mediaType)

	rc, err := fetcher.FetchBlob(ctx, blobRef, nil)
	require.NoError(t, err)
	defer rc.Close()
	fetchedBlob, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(blobData, fetchedBlob))
}
