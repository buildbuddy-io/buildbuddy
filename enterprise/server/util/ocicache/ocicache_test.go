package ocicache_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/stretchr/testify/require"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
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
			ref, err := name.ParseReference("buildbuddy.io/" + tc.imageName)
			require.NoError(t, err)

			flags.Set(t, "oci.cache.secret", tc.writeSecret)
			err = ocicache.WriteManifestToAC(ctx, raw, acClient, ref, hash, contentType)
			require.NoError(t, err)

			flags.Set(t, "oci.cache.secret", tc.fetchSecret)
			mc, err := ocicache.FetchManifestFromAC(ctx, acClient, ref, hash)
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
	flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityApp)
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	require.NoError(t, err)
	err = clientidentity.Register(te)
	require.NoError(t, err)
	require.NotNil(t, te.GetClientIdentityService())

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
	ref, err := name.ParseReference("buildbuddy.io/" + imageName)
	require.NoError(t, err)

	var out bytes.Buffer
	err = ocicache.WriteBlobOrManifestToCacheAndWriter(ctx,
		bytes.NewReader(raw),
		&out,
		nil, // explicitly pass nil bytestream client
		acClient,
		ref,
		ocipb.OCIResourceType_MANIFEST,
		hash,
		contentType,
		int64(len(raw)),
	)
	require.NoError(t, err)
	require.Equal(t, len(raw), out.Len())
	require.Empty(t, cmp.Diff(raw, out.Bytes()))

	mc, err := ocicache.FetchManifestFromAC(ctx, acClient, ref, hash)
	require.NoError(t, err)
	require.NotNil(t, mc)

	require.Equal(t, contentType, mc.ContentType)
	require.Empty(t, cmp.Diff(raw, mc.Raw))
}

func TestWriteAndFetchBlob(t *testing.T) {
	te := setupTestEnv(t)

	layerBuf, layerRef, hash, contentType := createLayer(t)
	contentLength := int64(len(layerBuf))

	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()

	err := ocicache.WriteBlobToCache(ctx, bytes.NewReader(layerBuf), bsClient, acClient, layerRef, hash, contentType, contentLength)
	require.NoError(t, err)

	fetchAndCheckBlob(t, te, layerBuf, layerRef, hash, contentType)
}

func createLayer(t *testing.T) ([]byte, name.Reference, gcr.Hash, string) {
	imageName := "create_layer"
	image, err := crane.Image(map[string][]byte{
		"/tmp/" + imageName: []byte(imageName),
	})
	require.NoError(t, err)
	imageRef, err := name.ParseReference("buildbuddy.io/" + imageName)
	require.NoError(t, err)
	layers, err := image.Layers()
	require.NoError(t, err)
	require.Equal(t, 1, len(layers))
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

	return layerBuf, layerRef, hash, contentType
}

func fetchAndCheckBlob(t *testing.T, te *testenv.TestEnv, layerBuf []byte, layerRef name.Reference, hash gcr.Hash, contentType string) {
	contentLength := int64(len(layerBuf))
	ctx := context.Background()
	bsClient := te.GetByteStreamClient()
	acClient := te.GetActionCacheClient()
	blobMetadata, err := ocicache.FetchBlobMetadataFromCache(ctx, bsClient, acClient, layerRef)
	require.NoError(t, err)
	require.NotNil(t, blobMetadata)
	require.Equal(t, contentLength, blobMetadata.ContentLength)
	require.Equal(t, contentType, blobMetadata.ContentType)

	out := &bytes.Buffer{}
	err = ocicache.FetchBlobFromCache(ctx, out, bsClient, hash, contentLength)
	require.NoError(t, err)

	require.Empty(t, cmp.Diff(layerBuf, out.Bytes()))
}
