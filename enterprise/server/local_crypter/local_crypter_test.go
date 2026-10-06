package local_crypter_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/local_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdata"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	user1  = "user1"
	user2  = "user2"
	group1 = "group1"
	group2 = "group2"
)

var fooDigest = &repb.Digest{Hash: "foo", SizeBytes: 123}

func randomKey(t *testing.T) []byte {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

func setup(t *testing.T, key []byte, version int64) (*testauth.TestAuthenticator, *local_crypter.LocalCrypter) {
	authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers(user1, group1, user2, group2))
	c, err := local_crypter.New(authenticator, key, version)
	require.NoError(t, err)
	return authenticator, c
}

func encrypt(t *testing.T, ctx context.Context, c interfaces.Crypter, data []byte) ([]byte, *sgpb.EncryptionMetadata) {
	md, err := c.ActiveKey(ctx)
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)
	encryptor, err := c.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
	require.NoError(t, err)
	testdata.WriteInRandomChunks(t, encryptor, data)
	return out.Bytes(), encryptor.Metadata()
}

func decrypt(ctx context.Context, c interfaces.Crypter, d *repb.Digest, ciphertext []byte, md *sgpb.EncryptionMetadata) ([]byte, error) {
	decryptor, err := c.NewDecryptor(ctx, d, io.NopCloser(bytes.NewReader(ciphertext)), md)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(decryptor)
}

func TestRegister(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		localKey                    string
		localKeyVersion             int64
		disableLocalCacheEncryption bool
		wantErr                     bool
	}{
		{name: "no key", localKey: "", localKeyVersion: 1},
		{name: "valid key", localKey: base64.StdEncoding.EncodeToString(randomKey(t)), localKeyVersion: 1},
		{name: "valid key with later version", localKey: base64.StdEncoding.EncodeToString(randomKey(t)), localKeyVersion: 7},
		{name: "invalid base64", localKey: "not base64!", localKeyVersion: 1, wantErr: true},
		{name: "key too short", localKey: base64.StdEncoding.EncodeToString(make([]byte, 16)), localKeyVersion: 1, wantErr: true},
		{name: "key too long", localKey: base64.StdEncoding.EncodeToString(make([]byte, 64)), localKeyVersion: 1, wantErr: true},
		{name: "zero version", localKey: base64.StdEncoding.EncodeToString(randomKey(t)), localKeyVersion: 0, wantErr: true},
		{name: "negative version", localKey: base64.StdEncoding.EncodeToString(randomKey(t)), localKeyVersion: -1, wantErr: true},
		{
			name:                        "local cache encryption disabled",
			localKey:                    base64.StdEncoding.EncodeToString(randomKey(t)),
			localKeyVersion:             1,
			disableLocalCacheEncryption: true,
			wantErr:                     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "crypter.local_key", tc.localKey)
			flags.Set(t, "crypter.local_key_version", tc.localKeyVersion)
			flags.Set(t, "crypter.enable_local_cache_encryption", !tc.disableLocalCacheEncryption)
			te := testenv.GetTestEnv(t)
			authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers(user1, group1))
			te.SetAuthenticator(authenticator)
			ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
			require.NoError(t, err)

			err = local_crypter.Register(te)
			if tc.wantErr {
				require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
				require.Nil(t, te.GetCrypter())
				return
			}
			require.NoError(t, err)
			if tc.localKey == "" {
				require.Nil(t, te.GetCrypter())
				return
			}
			require.IsType(t, &local_crypter.LocalCrypter{}, te.GetCrypter())
			md, err := te.GetCrypter().ActiveKey(ctx)
			require.NoError(t, err)
			require.Equal(t, "local", md.GetEncryptionKeyId())
			require.Equal(t, tc.localKeyVersion, md.GetVersion())
			require.True(t, remote_crypter.SupportsEncryption(te))
		})
	}
}

func TestRegisterWithExistingCrypter(t *testing.T) {
	flags.Set(t, "crypter.local_key", base64.StdEncoding.EncodeToString(randomKey(t)))
	te := testenv.GetTestEnv(t)
	_, existing := setup(t, randomKey(t), 1)
	te.SetCrypter(existing)

	err := local_crypter.Register(te)
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got %v", err)
	require.Same(t, existing, te.GetCrypter())
}

func TestRegisterWithRemoteTarget(t *testing.T) {
	flags.Set(t, "crypter.local_key", base64.StdEncoding.EncodeToString(randomKey(t)))
	flags.Set(t, "crypter.remote_target", "grpc://localhost:1234")
	te := testenv.GetTestEnv(t)

	// The cache proxy registers the local crypter first, so registering the
	// remote crypter afterwards fails.
	require.NoError(t, local_crypter.Register(te))
	err := remote_crypter.Register(te)
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got %v", err)
	require.IsType(t, &local_crypter.LocalCrypter{}, te.GetCrypter())
}

func TestEncryptDecrypt(t *testing.T) {
	authenticator, c := setup(t, randomKey(t), 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	ciphertext, md := encrypt(t, ctx, c, testData)
	require.NotContains(t, string(ciphertext), string(testData))

	decrypted, err := decrypt(ctx, c, fooDigest, ciphertext, md)
	require.NoError(t, err)
	require.Equal(t, testData, decrypted)

	// The wrong digest should fail message authentication.
	wrongDigest := &repb.Digest{Hash: "badhash", SizeBytes: fooDigest.SizeBytes}
	_, err = decrypt(ctx, c, wrongDigest, ciphertext, md)
	require.Error(t, err)
}

func TestGroupsCannotDecryptEachOthersData(t *testing.T) {
	authenticator, c := setup(t, randomKey(t), 1)
	user1Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	user2Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user2)
	require.NoError(t, err)

	ciphertext, md := encrypt(t, user1Ctx, c, []byte("group1 data"))
	_, err = decrypt(user2Ctx, c, fooDigest, ciphertext, md)
	require.Error(t, err)
}

func TestKeyChangeWithoutVersionChange(t *testing.T) {
	authenticator, oldCrypter := setup(t, randomKey(t), 1)
	_, newCrypter := setup(t, randomKey(t), 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	// The key ID and version don't depend on the key, so data written with
	// the old key is found under the same cache keys, but fails to decrypt.
	ciphertext, md := encrypt(t, ctx, oldCrypter, []byte("hello"))
	_, err = decrypt(ctx, newCrypter, fooDigest, ciphertext, md)
	require.Error(t, err)
}

func TestKeyRotation(t *testing.T) {
	oldKey := randomKey(t)
	authenticator, oldCrypter := setup(t, oldKey, 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	ciphertext, md := encrypt(t, ctx, oldCrypter, []byte("hello"))

	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{name: "new key", key: randomKey(t)},
		{name: "same key", key: oldKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, newCrypter := setup(t, tc.key, 2)
			newMD, err := newCrypter.ActiveKey(ctx)
			require.NoError(t, err)
			require.Equal(t, "local", newMD.GetEncryptionKeyId())
			require.EqualValues(t, 2, newMD.GetVersion())

			// Entries written with the old version are reported as not
			// found, so that the proxy re-fetches them.
			_, err = decrypt(ctx, newCrypter, fooDigest, ciphertext, md)
			require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
		})
	}
}

func TestUnknownKeyID(t *testing.T) {
	authenticator, c := setup(t, randomKey(t), 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	_, err = decrypt(ctx, c, fooDigest, nil, &sgpb.EncryptionMetadata{EncryptionKeyId: "EK123", Version: 1})
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
}
