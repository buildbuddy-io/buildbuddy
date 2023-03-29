package keystore_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	"github.com/stretchr/testify/require"
)

func generateKMSKey(t *testing.T, kmsDir string, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	keyFile := filepath.Join(kmsDir, id)
	err = os.WriteFile(keyFile, key, 0644)
	require.NoError(t, err)
	return "local-insecure-kms://" + id
}

func TestTestKMS(t *testing.T) {
	kmsDir := testfs.MakeTempDir(t)
	masterKeyURI := generateKMSKey(t, kmsDir, "masterKey")
	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	flags.Set(t, "keystore.master_key_uri", masterKeyURI)

	kmsClient, err := kms.New(context.Background())
	require.NoError(t, err)
	plaintext := []byte("woo boy wouldn't want this to get out")
	associatedData := []byte("big_secret")

	masterKey, err := kmsClient.FetchMasterKey()
	require.NoError(t, err)

	ciphertext, err := masterKey.Encrypt(plaintext, associatedData)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, ciphertext)

	decrypted, err := masterKey.Decrypt(ciphertext, associatedData)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func TestGenerateKey(t *testing.T) {
	kmsDir := testfs.MakeTempDir(t)
	masterKeyURI := generateKMSKey(t, kmsDir, "masterKey")
	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	flags.Set(t, "keystore.master_key_uri", masterKeyURI)

	te := testenv.GetTestEnv(t)
	err := kms.Register(te)
	require.NoError(t, err)

	pubKey64, encPrivKey64, err := keystore.GenerateSealedBoxKeys(te)
	require.NoError(t, err)
	require.NotNil(t, pubKey64)
	require.NotNil(t, encPrivKey64)

	pubKeySlice, err := base64.StdEncoding.DecodeString(pubKey64)
	require.NoError(t, err)
	require.Equal(t, 32, len(pubKeySlice))

	encPrivKeySlice, err := base64.StdEncoding.DecodeString(encPrivKey64)
	require.NoError(t, err)
	require.Equal(t, 60, len(encPrivKeySlice))
}

func TestBoxSealAndOpen(t *testing.T) {
	kmsDir := testfs.MakeTempDir(t)
	masterKeyURI := generateKMSKey(t, kmsDir, "masterKey")
	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	flags.Set(t, "keystore.master_key_uri", masterKeyURI)

	te := testenv.GetTestEnv(t)
	err := kms.Register(te)
	require.NoError(t, err)

	pubKey, encPrivKey, err := keystore.GenerateSealedBoxKeys(te)
	require.NoError(t, err)

	{
		anonSealedBox, err := keystore.NewAnonymousSealedBox(pubKey, "sEcRet")
		require.NoError(t, err)
		require.NotNil(t, anonSealedBox)

		plainText, err := keystore.OpenAnonymousSealedBox(te, pubKey, encPrivKey, anonSealedBox)
		require.NoError(t, err)
		require.Equal(t, "sEcRet", plainText)
	}

	{
		anonSealedBox, err := keystore.NewAnonymousSealedBox(pubKey, "sEcRet")
		require.NoError(t, err)
		require.NotNil(t, anonSealedBox)

		// Using different pub/priv keys to open this box should fail.
		pubKey2, encPrivKey2, err := keystore.GenerateSealedBoxKeys(te)
		require.NoError(t, err)
		plainText, err := keystore.OpenAnonymousSealedBox(te, pubKey2, encPrivKey2, anonSealedBox)
		require.Error(t, err)
		require.NotEqual(t, "sEcRet", plainText)
	}

	{
		anonSealedBox, err := keystore.NewAnonymousSealedBox(pubKey, "sEcRet")
		require.NoError(t, err)
		require.NotNil(t, anonSealedBox)

		// Using a different master key to open this box should fail.
		generateKMSKey(t, kmsDir, "masterKey")
		plainText, err := keystore.OpenAnonymousSealedBox(te, pubKey, encPrivKey, anonSealedBox)
		require.Error(t, err)
		require.NotEqual(t, "sEcRet", plainText)
	}
}
