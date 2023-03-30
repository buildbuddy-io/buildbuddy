package crypter_service

import (
	"bytes"
	"context"
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
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// The digest contents is not important as long as the same value is passed
	// to the encrypt and decrypt functions.
	dummyDigest = &repb.Digest{Hash: "foo", SizeBytes: 123}
)

func generateKMSKey(t *testing.T, kmsDir string, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(kmsDir, id), key, 0644)
	require.NoError(t, err)
	return "local-insecure-kms://" + id
}

func writeInRandomChunks(t *testing.T, w interfaces.Encryptor, data []byte) {
	for len(data) > 0 {
		n := mrand.Intn(2048)
		if n > len(data) {
			n = len(data)
		}
		_, err := w.Write(data[:n])
		require.NoError(t, err)
		data = data[n:]
	}
	err := w.Commit()
	require.NoError(t, err)
}

func createKey(t *testing.T, env environment.Env, keyID, groupID, groupKeyURI string) (*tables.EncryptionKey, *tables.EncryptionKeyVersion) {
	kmsClient := env.GetKMS()

	masterKeyPart := make([]byte, 32)
	_, err := rand.Read(masterKeyPart)
	require.NoError(t, err)
	groupKeyPart := make([]byte, 32)
	_, err = rand.Read(groupKeyPart)
	require.NoError(t, err)

	masterAEAD, err := kmsClient.FetchMasterKey()
	require.NoError(t, err)
	encMasterKeyPart, err := masterAEAD.Encrypt(masterKeyPart, nil)
	require.NoError(t, err)

	groupAEAD, err := kmsClient.FetchKey(groupKeyURI)
	require.NoError(t, err)
	encGroupKeyPart, err := groupAEAD.Encrypt(groupKeyPart, nil)

	key := &tables.EncryptionKey{
		EncryptionKeyID: keyID,
		GroupID:         groupID,
	}
	keyVersion := &tables.EncryptionKeyVersion{
		EncryptionKeyID:    keyID,
		Version:            1,
		MasterEncryptedKey: encMasterKeyPart,
		GroupKeyURI:        groupKeyURI,
		GroupEncryptedKey:  encGroupKeyPart,
	}
	return key, keyVersion
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

	groupID := "GR123"
	_, keyVersion := createKey(t, env, "EK123", groupID, customerKeyURI)

	crypter := New(env)
	out := bytes.NewBuffer(nil)
	for _, size := range []int64{1, 10, 100, 1000, 1000 * 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			e, err := crypter.newEncryptorWithKey(dummyDigest, ioutil.NewCustomCommitWriteCloser(out), groupID, keyVersion, 1024)
			require.NoError(t, err)

			testData := make([]byte, size)
			_, err = rand.Read(testData)
			require.NoError(t, err)

			// Write the test data in random chunk sizes. The input chunk sizes should
			// not affect the final result.
			writeInRandomChunks(t, e, testData)

			d, err := crypter.newDecryptorWithKey(dummyDigest, io.NopCloser(out), groupID, keyVersion, 1024)
			require.NoError(t, err)
			decrypted, err := io.ReadAll(d)
			require.NoError(t, err)

			if !bytes.Equal(decrypted, testData) {
				require.FailNow(t, "original plaintext and decrypted plaintext do not match")
			}
		})
	}
}

func TestDecryptWrongGroup(t *testing.T) {
	env, kmsDir := getEnv(t)
	customerKeyURI := generateKMSKey(t, kmsDir, "customerKey")

	groupID := "GR123"
	_, keyVersion := createKey(t, env, "EK123", groupID, customerKeyURI)

	crypter := New(env)
	out := bytes.NewBuffer(nil)

	e, err := crypter.newEncryptorWithKey(dummyDigest, ioutil.NewCustomCommitWriteCloser(out), groupID, keyVersion, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	writeInRandomChunks(t, e, testData)

	// Reading with the correct groupID should be OK.
	d, err := crypter.newDecryptorWithKey(dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, keyVersion, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)

	// If group ID doesn't match, authentication should fail.
	d, err = crypter.newDecryptorWithKey(dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), "GRBAD", keyVersion, 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
}

func TestDecryptWrongDigest(t *testing.T) {
	env, kmsDir := getEnv(t)
	customerKeyURI := generateKMSKey(t, kmsDir, "customerKey")

	groupID := "GR123"
	_, keyVersion := createKey(t, env, "EK123", groupID, customerKeyURI)

	crypter := New(env)
	out := bytes.NewBuffer(nil)

	e, err := crypter.newEncryptorWithKey(dummyDigest, ioutil.NewCustomCommitWriteCloser(out), groupID, keyVersion, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	writeInRandomChunks(t, e, testData)

	d, err := crypter.newDecryptorWithKey(dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, keyVersion, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)

	wrongHashDigest := &repb.Digest{Hash: "badhash", SizeBytes: dummyDigest.SizeBytes}
	d, err = crypter.newDecryptorWithKey(wrongHashDigest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, keyVersion, 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")

	wrongSizeDigest := &repb.Digest{Hash: dummyDigest.Hash, SizeBytes: 9999999999999999}
	d, err = crypter.newDecryptorWithKey(wrongSizeDigest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, keyVersion, 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
}

func TestKeyLookup(t *testing.T) {
	flags.Set(t, "database.enable_encryption_schema", true)

	env, kmsDir := getEnv(t)

	userID1 := "US123"
	groupID1 := "GR123"
	group1KeyID := "EK123"
	userID2 := "US456"
	groupID2 := "GR456"
	group2KeyID := "EK456"
	userID3 := "US999"
	groupID3 := "GR999"
	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID1, groupID1, userID2, groupID2, userID3, groupID3))
	env.SetAuthenticator(auther)

	group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
	group2KeyURI := generateKMSKey(t, kmsDir, "group2Key")
	ctx := context.Background()

	// Add separate keys for the first and second users.
	// Third user doesn't have a key configured.

	{
		// user 1
		key, keyVersion := createKey(t, env, group1KeyID, groupID1, group1KeyURI)
		err := env.GetDBHandle().DB(ctx).Create(key).Error
		require.NoError(t, err)
		err = env.GetDBHandle().DB(ctx).Create(keyVersion).Error
		require.NoError(t, err)
	}
	{
		// user 2
		key, keyVersion := createKey(t, env, group2KeyID, groupID2, group2KeyURI)
		err := env.GetDBHandle().DB(ctx).Create(key).Error
		require.NoError(t, err)
		err = env.GetDBHandle().DB(ctx).Create(keyVersion).Error
		require.NoError(t, err)
	}

	crypter := New(env)

	// user1 should be able to encrypt and decrypt using their own key.
	{
		out := bytes.NewBuffer(nil)
		ctx, err := auther.WithAuthenticatedUser(ctx, userID1)
		require.NoError(t, err)
		c, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
		require.NoError(t, err)
		require.Equal(t, c.Metadata().GetEncryptionKeyId(), group1KeyID)
		require.EqualValues(t, c.Metadata().GetVersion(), 1)

		input := []byte("hello world")
		writeInRandomChunks(t, c, input)
		d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), c.Metadata())
		require.NoError(t, err)
		decrypted := make([]byte, len(input))
		_, err = d.Read(decrypted)
		require.NoError(t, err)
		require.Equal(t, input, decrypted)
	}

	// user2 should be able to encrypt and decrypt using their own key.
	{
		out := bytes.NewBuffer(nil)
		ctx, err := auther.WithAuthenticatedUser(ctx, userID2)
		require.NoError(t, err)
		c, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
		require.NoError(t, err)
		require.Equal(t, c.Metadata().GetEncryptionKeyId(), group2KeyID)
		require.EqualValues(t, c.Metadata().GetVersion(), 1)

		input := []byte("hello universe")
		writeInRandomChunks(t, c, input)
		d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), c.Metadata())
		require.NoError(t, err)
		decrypted := make([]byte, len(input))
		_, err = d.Read(decrypted)
		require.NoError(t, err)
		require.Equal(t, input, decrypted)
	}

	// user2 should not be able to decrypt using user1's key even if the
	// supplied metadata references their key.
	{
		ctx, err := auther.WithAuthenticatedUser(ctx, userID1)
		require.NoError(t, err)
		c, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(bytes.NewBuffer(nil)))
		user1MD := c.Metadata()

		ctx, err = auther.WithAuthenticatedUser(ctx, userID2)
		_, err = crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(nil)), user1MD)
		require.True(t, status.IsNotFoundError(err))
	}

	// user3 key lookup should fail since they don't have a key setup
	{
		ctx, err := auther.WithAuthenticatedUser(ctx, userID3)
		require.NoError(t, err)
		_, err = crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(bytes.NewBuffer(nil)))
		require.True(t, status.IsNotFoundError(err))

		ctx, err = auther.WithAuthenticatedUser(ctx, userID2)
		_, err = crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(nil)), &rfpb.EncryptionMetadata{EncryptionKeyId: group1KeyID, Version: 1})
		require.True(t, status.IsNotFoundError(err))
	}
}
