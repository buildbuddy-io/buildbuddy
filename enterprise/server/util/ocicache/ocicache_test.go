package ocicache_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/stretchr/testify/require"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestCacheSecret(t *testing.T) {
	for _, tc := range []struct {
		name        string
		imageName   string
		writeSecret string
		fetchSecret string
		canFetch    bool
	}{
		{
			name:        "secrets match, can fetch",
			imageName:   "secrets_match_can_fetch",
			writeSecret: "not very secret secret",
			fetchSecret: "not very secret secret",
			canFetch:    true,
		},
		{
			name:        "secrets do not match, cannot fetch",
			imageName:   "secrets_do_not_match_cannot_fetch",
			writeSecret: "not very secret secret",
			fetchSecret: "completey different not very secret secret",
			canFetch:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image, err := crane.Image(map[string][]byte{
				"/tmp/" + tc.imageName: []byte(tc.imageName),
			})
			require.NoError(t, err)
			raw, err := image.RawManifest()
			require.NoError(t, err)

			te := setupTestEnv(t)

			ctx := context.Background()
			mediaType, err := image.MediaType()
			require.NoError(t, err)
			contentType := string(mediaType)
			hash, err := image.Digest()
			require.NoError(t, err)

			acClient := te.GetActionCacheClient()
			ref, err := gcrname.ParseReference("buildbuddy.io/" + tc.imageName)
			require.NoError(t, err)

			flags.Set(t, "oci.cache.secret", tc.writeSecret)
			err = ocicache.WriteManifestToAC(ctx, raw, acClient, ref.Context(), hash, contentType, ref)
			require.NoError(t, err)

			flags.Set(t, "oci.cache.secret", tc.fetchSecret)
			mc, err := ocicache.FetchManifestFromAC(ctx, acClient, ref.Context(), hash, ref)
			if !tc.canFetch {
				require.Error(t, err)
				require.Nil(t, mc)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, mc)

			require.Equal(t, contentType, mc.ContentType)
			require.Empty(t, cmp.Diff(raw, mc.Raw))
		})
	}
}

func setupTestEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, interfaces.ClientIdentityApp)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te
}

// TestManifestWrittenOnlyToAC sets the byte stream server and client to nil,
// writes a manifest to the AC, and fetches it. If that path were to touch the CAS
// at all, there would be an error trying to write to a nil client or server.
func TestManifestWrittenOnlyToAC(t *testing.T) {
	te := setupTestEnv(t)

	imageName := "test_manifest_written_only_to_ac"
	image, err := crane.Image(map[string][]byte{
		"/tmp/" + imageName: []byte(imageName),
	})
	require.NoError(t, err)
	raw, err := image.RawManifest()
	require.NoError(t, err)

	ctx := context.Background()
	mediaType, err := image.MediaType()
	require.NoError(t, err)
	contentType := string(mediaType)
	hash, err := image.Digest()
	require.NoError(t, err)

	acClient := te.GetActionCacheClient()
	ref, err := gcrname.ParseReference("buildbuddy.io/" + imageName)
	require.NoError(t, err)

	var out bytes.Buffer
	err = ocicache.WriteBlobOrManifestToCacheAndWriter(ctx,
		bytes.NewReader(raw),
		&out,
		nil, // explicitly pass nil bytestream client
		acClient,
		ref.Context(),
		ocipb.OCIResourceType_MANIFEST,
		hash,
		contentType,
		int64(len(raw)),
		ref,
	)
	require.NoError(t, err)
	require.Equal(t, len(raw), out.Len())
	require.Empty(t, cmp.Diff(raw, out.Bytes()))

	mc, err := ocicache.FetchManifestFromAC(ctx, acClient, ref.Context(), hash, ref)
	require.NoError(t, err)
	require.NotNil(t, mc)

	require.Equal(t, contentType, mc.ContentType)
	require.Empty(t, cmp.Diff(raw, mc.Raw))
}

func TestWriteAndFetchBlob(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "write_and_fetch_blob", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()
	r := bytes.NewReader(layerBuf)

	err := ocicache.WriteBlobToCache(ctx, r, bsClient, acClient, repo, hash, contentType, contentLength)
	require.NoError(t, err)

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
}

func TestBlobUploader(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "blob_uploader", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	up, err := ocicache.NewBlobUploader(ctx, bsClient, acClient, repo, hash, contentType, contentLength)
	require.NoError(t, err)

	written, err := up.Write(layerBuf[:256])
	require.NoError(t, err)
	require.Equal(t, 256, written)
	blobDoesNotExist(t, ctx, te, repo, hash, contentLength)

	written, err = up.Write(layerBuf[256:])
	require.NoError(t, err)
	require.Equal(t, len(layerBuf)-256, written)
	blobDoesNotExist(t, ctx, te, repo, hash, contentLength)

	err = up.Commit()
	require.NoError(t, err)
	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	// Second commit fails, but can still fetch blob.
	err = up.Commit()
	require.Error(t, err)
	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	err = up.Close()
	require.NoError(t, err)
	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	// Second close fails, but can still fetch blob.
	err = up.Close()
	require.Error(t, err)
	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
}

func TestBlobUploader_PartialWriteFails(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "blob_uploader_partial_write", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	up, err := ocicache.NewBlobUploader(ctx, bsClient, acClient, repo, hash, contentType, contentLength)
	require.NoError(t, err)
	written, err := up.Write(layerBuf[:256])
	require.NoError(t, err)
	require.Equal(t, 256, written)
	err = up.Close()
	require.NoError(t, err)

	blobDoesNotExist(t, ctx, te, repo, hash, contentLength)
}

func blobDoesNotExist(t *testing.T, ctx context.Context, te *testenv.TestEnv, repo gcrname.Repository, hash gcr.Hash, contentLength int64) {
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()
	metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, bsClient, acClient, repo, hash)
	require.Error(t, err)
	require.Nil(t, metadata)

	out := &bytes.Buffer{}
	err = ocicache.FetchBlobFromCache(ctx, out, bsClient, hash, contentLength)
	require.Error(t, err)
}

func TestBlobUploader_BlobExists(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "blob_uploader_blob_exists", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	for i := 0; i < 2; i++ {
		up, err := ocicache.NewBlobUploader(ctx, bsClient, acClient, repo, hash, contentType, contentLength)
		require.NoError(t, err)

		written, err := up.Write(layerBuf)
		require.NoError(t, err)
		require.Equal(t, len(layerBuf), written)

		err = up.Commit()
		require.NoError(t, err)

		fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

		err = up.Close()
		require.NoError(t, err)

		fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
	}
}

func TestReadThroughCacher(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "read_through_cacher", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()
	r := io.NopCloser(bytes.NewReader(layerBuf))

	cacher, err := ocicache.NewBlobReadThroughCacher(ctx, r, bsClient, acClient, repo, hash, contentType, contentLength)
	require.NoError(t, err)

	readout, err := io.ReadAll(cacher)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(layerBuf, readout))

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	err = cacher.Close()
	require.NoError(t, err)

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
}

func TestReadThroughCacher_PartialReadPreventsCommit(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "read_through_cacher_partial_read", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()
	rc := io.NopCloser(bytes.NewReader(layerBuf))

	cacher, err := ocicache.NewBlobReadThroughCacher(ctx, rc, bsClient, acClient, repo, hash, contentType, contentLength)
	require.NoError(t, err)

	readbuf := make([]byte, 256)
	n, err := cacher.Read(readbuf)
	require.NoError(t, err)
	require.Equal(t, 256, n)

	err = cacher.Close()
	require.NoError(t, err)

	blobDoesNotExist(t, ctx, te, repo, hash, contentLength)
}

func TestReadThroughCacher_ReturnsEOF(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "read_through_cacher", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()
	r := io.NopCloser(bytes.NewReader(layerBuf))

	cacher, err := ocicache.NewBlobReadThroughCacher(ctx, r, bsClient, acClient, repo, hash, contentType, contentLength)
	require.NoError(t, err)

	readout := &bytes.Buffer{}
	readbuf := make([]byte, 128)
	for {
		n, err := cacher.Read(readbuf)
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
			break
		}
		require.Greater(t, n, 0)
		blobDoesNotExist(t, ctx, te, repo, hash, contentLength)
		_, err = readout.Write(readbuf[:n])
		require.NoError(t, err)
	}
	require.Empty(t, cmp.Diff(layerBuf, readout.Bytes()))

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	err = cacher.Close()
	require.NoError(t, err)

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
}

func createLayer(t *testing.T, imageName string, filesize int64) ([]byte, gcrname.Repository, gcr.Hash, string) {
	_, filebuf := testdigest.RandomCASResourceBuf(t, filesize)
	filename, err := random.RandomString(32)
	require.NoError(t, err)
	image, err := crane.Image(map[string][]byte{
		"/tmp/" + filename: filebuf,
	})
	require.NoError(t, err)
	imageRef, err := gcrname.ParseReference("buildbuddy.io/" + imageName)
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
	return layerBuf, layerRef.Context(), hash, contentType
}

func fetchAndCheckBlob(t *testing.T, te *testenv.TestEnv, layerBuf []byte, repo gcrname.Repository, hash gcr.Hash, contentType string) {
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, bsClient, acClient, repo, hash)
	require.NoError(t, err)
	require.Equal(t, contentType, metadata.GetContentType())
	require.Equal(t, contentLength, metadata.GetContentLength())

	out := &bytes.Buffer{}
	err = ocicache.FetchBlobFromCache(ctx, out, bsClient, hash, contentLength)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(layerBuf, out.Bytes()))
}

func TestOCICache_TeeBlob(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "oci_cache_tee_blob", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	cache := ocicache.NewOCICache(bsClient, acClient)
	upstream := io.NopCloser(bytes.NewReader(layerBuf))

	ref := ocicache.BlobReference{
		Repo:          repo,
		Hash:          hash,
		ContentType:   types.MediaType(contentType),
		ContentLength: contentLength,
	}

	tee, err := cache.TeeBlob(ctx, upstream, ref)
	require.NoError(t, err)

	readout, err := io.ReadAll(tee)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(layerBuf, readout))

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	err = tee.Close()
	require.NoError(t, err)

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
}

func TestOCICache_TeeBlob_PartialReadPreventsCommit(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "oci_cache_tee_blob_partial", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	cache := ocicache.NewOCICache(bsClient, acClient)
	upstream := io.NopCloser(bytes.NewReader(layerBuf))

	ref := ocicache.BlobReference{
		Repo:          repo,
		Hash:          hash,
		ContentType:   types.MediaType(contentType),
		ContentLength: contentLength,
	}

	tee, err := cache.TeeBlob(ctx, upstream, ref)
	require.NoError(t, err)

	readbuf := make([]byte, 256)
	n, err := tee.Read(readbuf)
	require.NoError(t, err)
	require.Equal(t, 256, n)

	err = tee.Close()
	require.NoError(t, err)

	blobDoesNotExist(t, ctx, te, repo, hash, contentLength)
}

func TestOCICache_TeeBlob_ReturnsEOF(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, repo, hash, contentType := createLayer(t, "oci_cache_tee_blob_eof", 1024)
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	cache := ocicache.NewOCICache(bsClient, acClient)
	upstream := io.NopCloser(bytes.NewReader(layerBuf))

	ref := ocicache.BlobReference{
		Repo:          repo,
		Hash:          hash,
		ContentType:   types.MediaType(contentType),
		ContentLength: contentLength,
	}

	tee, err := cache.TeeBlob(ctx, upstream, ref)
	require.NoError(t, err)

	readout := &bytes.Buffer{}
	readbuf := make([]byte, 128)
	for {
		n, err := tee.Read(readbuf)
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
			break
		}
		require.Greater(t, n, 0)
		blobDoesNotExist(t, ctx, te, repo, hash, contentLength)
		_, err = readout.Write(readbuf[:n])
		require.NoError(t, err)
	}
	require.Empty(t, cmp.Diff(layerBuf, readout.Bytes()))

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)

	err = tee.Close()
	require.NoError(t, err)

	fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
}

func TestOCICache_TeeBlob_MultipleBlobs(t *testing.T) {
	te := setupTestEnv(t)
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	cache := ocicache.NewOCICache(bsClient, acClient)

	// Create and cache multiple blobs using the same cache instance
	for i := 0; i < 3; i++ {
		layerBuf, repo, hash, contentType := createLayer(t, "oci_cache_tee_blob_multi", 512)
		contentLength := int64(len(layerBuf))

		upstream := io.NopCloser(bytes.NewReader(layerBuf))
		ref := ocicache.BlobReference{
			Repo:          repo,
			Hash:          hash,
			ContentType:   types.MediaType(contentType),
			ContentLength: contentLength,
		}

		tee, err := cache.TeeBlob(ctx, upstream, ref)
		require.NoError(t, err)

		readout, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(layerBuf, readout))

		err = tee.Close()
		require.NoError(t, err)

		fetchAndCheckBlob(t, te, layerBuf, repo, hash, contentType)
	}
}
