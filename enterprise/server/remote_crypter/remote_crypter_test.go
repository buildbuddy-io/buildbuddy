package remote_crypter_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdata"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	permittedClient = "permitted-client"
)

var (
	fooDigest = &repb.Digest{Hash: "foo", SizeBytes: 123}
)

const (
	user1  = "user1"
	user2  = "user2"
	user3  = "user3"
	group1 = "group1"
	group2 = "group2"
	group3 = "group3"
)

type groupID string
type keyID string

type fakeEncryptionService struct {
	requests              atomic.Int32
	authenticator         interfaces.Authenticator
	clientIdentityService interfaces.ClientIdentityService
	seq                   int // internal ordering, a la time.Now()
	keys                  map[groupID]map[keyID][]*fakeKey
}

type fakeKey struct {
	id      keyID
	seq     int
	version int64
	key     []byte
}

func newFakeEncryptionService(authenticator interfaces.Authenticator, clientIdentityService interfaces.ClientIdentityService) *fakeEncryptionService {
	return &fakeEncryptionService{
		authenticator:         authenticator,
		clientIdentityService: clientIdentityService,
		keys:                  make(map[groupID]map[keyID][]*fakeKey),
	}
}

func (f *fakeEncryptionService) add(t *testing.T, group string, newKey *fakeKey) {
	gid := groupID(group)
	for id, keys := range f.keys[gid] {
		for _, key := range keys {
			if id == newKey.id && key.version == newKey.version {
				t.FailNow()
			}
		}
	}

	newKey.seq = f.seq
	f.seq++
	if f.keys[gid] == nil {
		f.keys[gid] = map[keyID][]*fakeKey{}
	}
	f.keys[gid][newKey.id] = append(f.keys[gid][newKey.id], newKey)
}

func (f *fakeEncryptionService) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	f.requests.Add(1)
	ctx, err := f.clientIdentityService.ValidateIncomingIdentity(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := f.clientIdentityService.IdentityFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if identity.Client != permittedClient {
		return nil, status.InvalidArgumentErrorf("Client %s may not access EncryptionService", identity.Client)
	}
	userInfo, err := f.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	gid := groupID(userInfo.GetGroupID())

	// Figure out which key is being requested.
	var kid keyID
	if req.GetMetadata().GetId() != "" {
		kid = keyID(req.GetMetadata().GetId())
	} else {
		// If no Key ID was specified, select the key with the highest seq
		seq := -1
		for _, keys := range f.keys[gid] {
			for _, key := range keys {
				if key.seq > seq {
					kid = key.id
					seq = key.seq
				}
			}
		}
	}

	if kid == "" {
		return nil, status.NotFoundError("No encryption key available")
	}

	keys := f.keys[gid][kid]
	if req.GetMetadata() != nil && req.GetMetadata().GetVersion() >= 0 {
		for _, key := range keys {
			if key.version == req.GetMetadata().GetVersion() {
				return &enpb.GetEncryptionKeyResponse{
					Key: &enpb.EncryptionKey{
						Metadata: &enpb.EncryptionKeyMetadata{
							Id:      string(kid),
							Version: key.version,
						},
						Key: key.key,
					},
				}, nil
			}
		}
		return nil, status.NotFoundError("Encryption key version not found")
	}

	var keyToReturn *fakeKey
	for _, key := range keys {
		if keyToReturn == nil {
			keyToReturn = key
		} else if key.version > keyToReturn.version {
			keyToReturn = key
		}
	}

	return &enpb.GetEncryptionKeyResponse{
		Key: &enpb.EncryptionKey{
			Metadata: &enpb.EncryptionKeyMetadata{
				Id:      string(kid),
				Version: keyToReturn.version,
			},
			Key: keyToReturn.key,
		},
	}, nil
}

func setup(t *testing.T) (*testauth.TestAuthenticator, interfaces.Crypter, *clockwork.FakeClock, *fakeEncryptionService) {
	return setupWithIdentity(t, permittedClient)
}

func setupWithIdentity(t *testing.T, identity string) (*testauth.TestAuthenticator, interfaces.Crypter, *clockwork.FakeClock, *fakeEncryptionService) {
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers(user1, group1, user2, group2, user3, group3))
	te.SetAuthenticator(authenticator)
	clock := clockwork.NewFakeClock()
	flags.Set(t, "app.client_identity.key", "key")
	flags.Set(t, "app.client_identity.client", identity)
	flags.Set(t, "app.client_identity.origin", "origin")
	clientIdentityService, err := clientidentity.New(clock)
	require.NoError(t, err)
	encryptionService := newFakeEncryptionService(authenticator, clientIdentityService)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	enpb.RegisterEncryptionServiceServer(grpcServer, encryptionService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(t.Context(), lis)
	require.NoError(t, err)
	crypter := remote_crypter.New(te, authenticator, clientIdentityService, clock, conn)
	return authenticator, crypter, clock, encryptionService
}

func TestEncryptDecrypt(t *testing.T) {
	authenticator, crypter, _, service := setup(t)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	service.add(t, group1, &fakeKey{id: "1", version: 1, key: []byte(strings.Repeat("1", 32))})

	for _, size := range []int64{1, 10, 100, 1000, 1000 * 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			out := bytes.NewBuffer(nil)
			md, err := crypter.ActiveKey(ctx)
			require.NoError(t, err)
			encryptor, err := crypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
			require.NoError(t, err)

			testData := make([]byte, size)
			_, err = rand.Read(testData)
			require.NoError(t, err)

			// Write the test data in random chunk sizes
			testdata.WriteInRandomChunks(t, encryptor, testData)

			decryptor, err := crypter.NewDecryptor(ctx, fooDigest, io.NopCloser(out), encryptor.Metadata())
			require.NoError(t, err)
			decrypted, err := io.ReadAll(decryptor)
			require.NoError(t, err)

			require.Equal(t, testData, decrypted, "original plaintext and decrypted plaintext do not match")
		})
	}
}

func TestDecryptWrongDigest(t *testing.T) {
	authenticator, crypter, _, service := setup(t)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	service.add(t, group1, &fakeKey{id: "1", version: 1, key: []byte(strings.Repeat("1", 32))})

	out := bytes.NewBuffer(nil)

	md, err := crypter.ActiveKey(ctx)
	require.NoError(t, err)
	encryptor, err := crypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	testdata.WriteInRandomChunks(t, encryptor, testData)

	// Correct digest should work
	decryptor, err := crypter.NewDecryptor(ctx, fooDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
	require.NoError(t, err)
	decrypted, err := io.ReadAll(decryptor)
	require.NoError(t, err)
	require.Equal(t, testData, decrypted)

	// Wrong hash should fail message authentication
	wrongHashDigest := &repb.Digest{Hash: "badhash", SizeBytes: fooDigest.SizeBytes}
	decryptor, err = crypter.NewDecryptor(ctx, wrongHashDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
	require.NoError(t, err)
	_, err = io.ReadAll(decryptor)
	require.Error(t, err)

	// Wrong size should fail message authentication
	wrongSizeDigest := &repb.Digest{Hash: fooDigest.Hash, SizeBytes: 9999999999999999}
	decryptor, err = crypter.NewDecryptor(ctx, wrongSizeDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
	require.NoError(t, err)
	_, err = io.ReadAll(decryptor)
	require.Error(t, err)
}

func TestAuth(t *testing.T) {
	authenticator, crypter, _, service := setup(t)
	group1Key := "group1key"
	group2Key := "group2key"
	service.add(t, group1, &fakeKey{id: keyID(group1Key), version: 1, key: []byte(strings.Repeat("1", 32))})
	service.add(t, group2, &fakeKey{id: keyID(group2Key), version: 1, key: []byte(strings.Repeat("2", 32))})
	user1Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	user2Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user2)
	require.NoError(t, err)
	user3Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user3)
	require.NoError(t, err)

	in := []byte("123456789")
	out := bytes.NewBuffer(nil)

	// user1 should be able to encrypt and decrypt using their own key
	{
		md, err := crypter.ActiveKey(user1Ctx)
		require.NoError(t, err)
		encryptor, err := crypter.NewEncryptor(user1Ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
		require.NoError(t, err)
		require.Equal(t, group1Key, encryptor.Metadata().GetEncryptionKeyId())
		require.EqualValues(t, 1, encryptor.Metadata().GetVersion())

		encryptor.Write(in)
		encryptor.Commit()

		decryptor, err := crypter.NewDecryptor(user1Ctx, fooDigest, io.NopCloser(out), encryptor.Metadata())
		require.NoError(t, err)
		decrypted, err := io.ReadAll(decryptor)
		require.NoError(t, err)
		require.Equal(t, in, decrypted)
	}

	// user2 should not be able to decrypt using user1's key
	{
		md, err := crypter.ActiveKey(user2Ctx)
		require.NoError(t, err)
		encryptor, err := crypter.NewEncryptor(user2Ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
		require.NoError(t, err)
		require.Equal(t, group2Key, encryptor.Metadata().GetEncryptionKeyId())
		require.EqualValues(t, 1, encryptor.Metadata().GetVersion())

		encryptor.Write(in)
		encryptor.Commit()

		_, err = crypter.NewDecryptor(user1Ctx, fooDigest, io.NopCloser(out), encryptor.Metadata())
		require.True(t, status.IsNotFoundError(err))
	}

	// user3 key lookup should fail since they don't have a key setup
	{
		_, err = crypter.ActiveKey(user3Ctx)
		require.True(t, status.IsNotFoundError(err))
	}
}

func TestActiveKey(t *testing.T) {
	authenticator, crypter, _, service := setup(t)
	group1Key := "group1key"
	service.add(t, group1, &fakeKey{id: keyID(group1Key), version: 1, key: []byte(strings.Repeat("1", 32))})
	user1Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	user2Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user2)
	require.NoError(t, err)

	metadata, err := crypter.ActiveKey(user1Ctx)
	require.NoError(t, err)
	require.Equal(t, group1Key, metadata.GetEncryptionKeyId())
	require.EqualValues(t, 1, metadata.GetVersion())

	_, err = crypter.ActiveKey(user2Ctx)
	require.True(t, status.IsNotFoundError(err))
}

func randomLocalKey(t *testing.T) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(key)
}

func setupLocalKey(t *testing.T, localKey string, version int64) (*testauth.TestAuthenticator, interfaces.Crypter) {
	flags.Set(t, "crypter.local_key", localKey)
	flags.Set(t, "crypter.local_key_version", version)
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers(user1, group1, user2, group2))
	te.SetAuthenticator(authenticator)
	require.NoError(t, remote_crypter.Register(te))
	require.NotNil(t, te.GetCrypter())
	return authenticator, te.GetCrypter()
}

func TestRegisterLocalKey(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		localKey                    string
		localKeyVersion             int64
		remoteTarget                string
		disableLocalCacheEncryption bool
		wantErr                     bool
	}{
		{name: "valid key", localKey: randomLocalKey(t), localKeyVersion: 1},
		{name: "valid key with later version", localKey: randomLocalKey(t), localKeyVersion: 7},
		{name: "invalid base64", localKey: "not base64!", localKeyVersion: 1, wantErr: true},
		{name: "key too short", localKey: base64.StdEncoding.EncodeToString(make([]byte, 16)), localKeyVersion: 1, wantErr: true},
		{name: "key too long", localKey: base64.StdEncoding.EncodeToString(make([]byte, 64)), localKeyVersion: 1, wantErr: true},
		{name: "zero version", localKey: randomLocalKey(t), localKeyVersion: 0, wantErr: true},
		{name: "negative version", localKey: randomLocalKey(t), localKeyVersion: -1, wantErr: true},
		{name: "remote target set", localKey: randomLocalKey(t), localKeyVersion: 1, remoteTarget: "grpc://localhost:1234", wantErr: true},
		{name: "local cache encryption disabled", localKey: randomLocalKey(t), localKeyVersion: 1, disableLocalCacheEncryption: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "crypter.local_key", tc.localKey)
			flags.Set(t, "crypter.local_key_version", tc.localKeyVersion)
			flags.Set(t, "crypter.remote_target", tc.remoteTarget)
			flags.Set(t, "crypter.enable_local_cache_encryption", !tc.disableLocalCacheEncryption)
			te := testenv.GetTestEnv(t)
			authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers(user1, group1))
			te.SetAuthenticator(authenticator)
			ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
			require.NoError(t, err)

			err = remote_crypter.Register(te)
			if tc.wantErr {
				require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
				require.Nil(t, te.GetCrypter())
				return
			}
			require.NoError(t, err)
			require.NotNil(t, te.GetCrypter())
			md, err := te.GetCrypter().ActiveKey(ctx)
			require.NoError(t, err)
			require.Equal(t, "local", md.GetEncryptionKeyId())
			require.Equal(t, tc.localKeyVersion, md.GetVersion())
			// Encryption with a local key doesn't depend on the remote
			// encryption experiment.
			require.True(t, remote_crypter.SupportsEncryption(te)(ctx))
		})
	}
}

func TestLocalKeyEncryptDecrypt(t *testing.T) {
	authenticator, crypter := setupLocalKey(t, randomLocalKey(t), 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	md, err := crypter.ActiveKey(ctx)
	require.NoError(t, err)
	require.Equal(t, "local", md.GetEncryptionKeyId())
	require.EqualValues(t, 1, md.GetVersion())

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)
	encryptor, err := crypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
	require.NoError(t, err)
	testdata.WriteInRandomChunks(t, encryptor, testData)
	require.NotContains(t, out.String(), string(testData))

	decryptor, err := crypter.NewDecryptor(ctx, fooDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
	require.NoError(t, err)
	decrypted, err := io.ReadAll(decryptor)
	require.NoError(t, err)
	require.Equal(t, testData, decrypted)
}

func TestLocalKeyAuth(t *testing.T) {
	authenticator, crypter := setupLocalKey(t, randomLocalKey(t), 1)
	user1Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)
	user2Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user2)
	require.NoError(t, err)

	md, err := crypter.ActiveKey(user1Ctx)
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)
	encryptor, err := crypter.NewEncryptor(user1Ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
	require.NoError(t, err)
	_, err = encryptor.Write([]byte("123456789"))
	require.NoError(t, err)
	require.NoError(t, encryptor.Commit())

	// Each group gets its own derived key, so user2 can't decrypt user1's
	// data.
	decryptor, err := crypter.NewDecryptor(user2Ctx, fooDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
	require.NoError(t, err)
	_, err = io.ReadAll(decryptor)
	require.Error(t, err)
}

func TestLocalKeyChange(t *testing.T) {
	authenticator, oldCrypter := setupLocalKey(t, randomLocalKey(t), 1)
	_, newCrypter := setupLocalKey(t, randomLocalKey(t), 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	md, err := oldCrypter.ActiveKey(ctx)
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)
	encryptor, err := oldCrypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
	require.NoError(t, err)
	_, err = encryptor.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, encryptor.Commit())

	// The key ID doesn't depend on the key, so data written with the old key
	// is found under the same cache keys, but fails to decrypt.
	decryptor, err := newCrypter.NewDecryptor(ctx, fooDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
	require.NoError(t, err)
	_, err = io.ReadAll(decryptor)
	require.Error(t, err)
}

func TestLocalKeyRotation(t *testing.T) {
	oldKey := randomLocalKey(t)
	authenticator, oldCrypter := setupLocalKey(t, oldKey, 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	md, err := oldCrypter.ActiveKey(ctx)
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)
	encryptor, err := oldCrypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out), md)
	require.NoError(t, err)
	_, err = encryptor.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, encryptor.Commit())

	for _, tc := range []struct {
		name string
		key  string
	}{
		{name: "new key", key: randomLocalKey(t)},
		{name: "same key", key: oldKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, newCrypter := setupLocalKey(t, tc.key, 2)
			newMD, err := newCrypter.ActiveKey(ctx)
			require.NoError(t, err)
			require.Equal(t, "local", newMD.GetEncryptionKeyId())
			require.EqualValues(t, 2, newMD.GetVersion())

			// Entries written with the old version are reported as not
			// found, so that the proxy re-fetches them.
			_, err = newCrypter.NewDecryptor(ctx, fooDigest, io.NopCloser(bytes.NewReader(out.Bytes())), encryptor.Metadata())
			require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
		})
	}
}

func TestLocalKeyUnknownKeyID(t *testing.T) {
	authenticator, crypter := setupLocalKey(t, randomLocalKey(t), 1)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	_, err = crypter.NewDecryptor(ctx, fooDigest, io.NopCloser(bytes.NewReader(nil)), &sgpb.EncryptionMetadata{EncryptionKeyId: "EK123", Version: 1})
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
}

func TestUnauthorizedIdentity(t *testing.T) {
	authenticator, crypter, clock, service := setupWithIdentity(t, "some-other-client")
	group1Key := "group1key"
	service.add(t, group1, &fakeKey{id: keyID(group1Key), version: 1, key: []byte(strings.Repeat("1", 32))})
	user1Ctx, err := authenticator.WithAuthenticatedUser(context.Background(), user1)
	require.NoError(t, err)

	// Advance the clock to use up retries.
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
				clock.Advance(1 * time.Second)
			}
		}
	}()

	_, err = crypter.ActiveKey(user1Ctx)
	require.Error(t, err)
}
