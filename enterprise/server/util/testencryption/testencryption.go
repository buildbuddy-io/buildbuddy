package testencryption

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_service"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
)

const (
	masterKeyID = "masterKey"
)

// KMS holds local KMS state configured for a test env.
type KMS struct {
	env    *real_environment.RealEnv
	keyDir string
}

// Setup registers local test KMS keys and the crypter service on the env.
//
// The local KMS provider is called insecure because it reads raw key bytes from
// disk. That makes it useful for hermetic tests but unsuitable for production.
func Setup(t testing.TB, env *real_environment.RealEnv) *KMS {
	kmsDir := testfs.MakeTempDir(t)
	writeLocalInsecureKMSKey(t, kmsDir, masterKeyID)

	flags.Set(t, "keystore.master_key_uri", "local-insecure-kms://"+masterKeyID)
	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	err := kms.Register(env)
	require.NoError(t, err)

	err = crypter_service.Register(env)
	require.NoError(t, err)

	return &KMS{
		env:    env,
		keyDir: kmsDir,
	}
}

// EnableForAuthenticatedGroup enables cache encryption using the local group
// key for the authenticated user's group in ctx.
func EnableForAuthenticatedGroup(t testing.TB, ctx context.Context, env environment.Env) {
	c, err := claims.ClaimsFromContext(ctx)
	require.NoError(t, err)

	keyDir, err := flagutil.GetDereferencedValue[string]("keystore.local_insecure_kms_directory")
	require.NoError(t, err)

	groupKeyID := "groupKey-" + c.GetGroupID()
	writeLocalInsecureKMSKey(t, keyDir, groupKeyID)
	_, err = env.GetCrypter().SetEncryptionConfig(ctx, &enpb.SetEncryptionConfigRequest{
		Enabled: true,
		KmsConfig: &enpb.KMSConfig{
			LocalInsecureKmsConfig: &enpb.LocalInsecureKMSConfig{KeyId: groupKeyID},
		},
	})
	require.NoError(t, err)
}

func writeLocalInsecureKMSKey(t testing.TB, kmsDir, id string) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(kmsDir, id), key, 0600))
}
