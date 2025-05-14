package ocicache_test

import (
	"context"
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
