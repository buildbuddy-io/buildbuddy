package cache_proxy_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func enableEncryption(t *testing.T, rbe *rbetest.Env, userID string) {
	ctx := rbe.WithUserID(t.Context(), userID)
	client := rbe.GetBuildBuddyServiceClient()
	_, err := client.SetEncryptionConfig(ctx, &enpb.SetEncryptionConfigRequest{
		Enabled: true,
		KmsConfig: &enpb.KMSConfig{
			LocalInsecureKmsConfig: &enpb.LocalInsecureKMSConfig{KeyId: "groupKey"},
		},
	})
	require.NoError(t, err, "Could not enable encryption")

	getResp, err := client.GetEncryptionConfig(ctx, &enpb.GetEncryptionConfigRequest{})
	require.NoError(t, err)
	require.True(t, getResp.Enabled, "Encryption should be enabled")
}

func TestFindMissing_ES256Auth(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	require.NoError(t, claims.Init())

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()

	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	req := repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{
			{Hash: strings.Repeat("a", 64), SizeBytes: 1},
		},
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-buildbuddy-api-key", rbe.APIKey1)
	resp, err := cas.FindMissingBlobs(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))
}

func TestFindMissing_ES256Auth_Encryption(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	require.NoError(t, claims.Init())

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()

	enableEncryption(t, rbe, rbe.UserID1)

	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	req := repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{
			{Hash: strings.Repeat("a", 64), SizeBytes: 1},
		},
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-buildbuddy-api-key", rbe.APIKey1)
	resp, err := cas.FindMissingBlobs(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))
}

func TestFindMissing_Encryption(t *testing.T) {
	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()

	enableEncryption(t, rbe, rbe.UserID1)

	cas := rbe.AddCacheProxy().GetContentAddressableStorageClient()
	req := repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{
			&repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1},
		},
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-buildbuddy-api-key", rbe.APIKey1)
	resp, err := cas.FindMissingBlobs(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))
}
