package crypter_service

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	mrand "math/rand"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func generateKMSKey(t *testing.T, kmsDir string, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(kmsDir, id), key, 0644)
	require.NoError(t, err)
	return "local-insecure-kms://" + id
}

func writeInRandomChunks(t *testing.T, w io.Writer, data []byte) {
	idx := 0
	for idx < len(data) {
		n := mrand.Intn(2048)
		if idx+n > len(data) {
			n = len(data) - idx
		}
		_, err := w.Write(data[idx : idx+n])
		require.NoError(t, err)
		idx += n
	}
}

func createKeyVersion(t *testing.T, env environment.Env, customerKeyURI string) *tables.EncryptionKeyVersion {
	kmsClient := env.GetKMS()

	masterKeyPart := make([]byte, 32)
	_, err := rand.Read(masterKeyPart)
	require.NoError(t, err)
	customerKeyPart := make([]byte, 32)
	_, err = rand.Read(customerKeyPart)
	require.NoError(t, err)

	masterAEAD, err := kmsClient.FetchMasterKey()
	require.NoError(t, err)
	encMasterKeyPart, err := masterAEAD.Encrypt(masterKeyPart, nil)
	require.NoError(t, err)

	customerAEAD, err := kmsClient.FetchKey(customerKeyURI)
	require.NoError(t, err)
	encCustomerKeyPart, err := customerAEAD.Encrypt(customerKeyPart, nil)

	return &tables.EncryptionKeyVersion{
		KeyID:                "EK123",
		Version:              1,
		MasterEncryptedKey:   encMasterKeyPart,
		CustomerKeyURI:       customerKeyURI,
		CustomerEncryptedKey: encCustomerKeyPart,
	}
}

func getEnv(t *testing.T) (*testenv.TestEnv, string) {
	mrand.Seed(time.Now().UnixMicro())

	kmsDir := testfs.MakeTempDir(t)
	masterKeyURI := generateKMSKey(t, kmsDir, "masterKey")

	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	flags.Set(t, "keystore.master_key_uri", masterKeyURI)
	env := testenv.GetTestEnv(t)
	err := kms.Register(env)
	require.NoError(t, err)
	return env, kmsDir
}

func TestEncryptDecrypt(t *testing.T) {
	env, kmsDir := getEnv(t)
	customerKeyURI := generateKMSKey(t, kmsDir, "customerKey")
	key := createKeyVersion(t, env, customerKeyURI)

	groupID := "GR123"
	crypter := New(env)
	out := bytes.NewBuffer(nil)
	for _, size := range []int64{1, 10, 100, 1000, 1000 * 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			e, err := crypter.newEncryptorWithKey(ioutil.NewCustomCommitWriteCloser(out), groupID, key, 1024)
			require.NoError(t, err)

			testData := make([]byte, size)
			_, err = rand.Read(testData)
			require.NoError(t, err)

			// Write the test data in random chunk sizes. The input chunk sizes should
			// not affect the final result.
			writeInRandomChunks(t, e, testData)

			err = e.Commit()
			require.NoError(t, err)

			d, err := crypter.newDecryptorWithKey(io.NopCloser(out), groupID, key, 1024)
			require.NoError(t, err)
			decrypted, err := io.ReadAll(d)
			require.NoError(t, err)

			require.Equal(t, decrypted, testData)
		})
	}
}

func TestDecryptWrongGroup(t *testing.T) {
	env, kmsDir := getEnv(t)
	customerKeyURI := generateKMSKey(t, kmsDir, "customerKey")
	key := createKeyVersion(t, env, customerKeyURI)

	groupID := "GR123"
	crypter := New(env)
	out := bytes.NewBuffer(nil)

	e, err := crypter.newEncryptorWithKey(ioutil.NewCustomCommitWriteCloser(out), groupID, key, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	writeInRandomChunks(t, e, testData)

	err = e.Commit()
	require.NoError(t, err)

	// Reading with the correct groupID should be OK.
	d, err := crypter.newDecryptorWithKey(io.NopCloser(bytes.NewReader(out.Bytes())), groupID, key, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)

	// If group ID doesn't match, authentication should fail.
	d, err = crypter.newDecryptorWithKey(io.NopCloser(bytes.NewReader(out.Bytes())), "GRBAD", key, 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
}
