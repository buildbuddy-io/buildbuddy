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
