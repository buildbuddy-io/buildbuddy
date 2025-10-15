package remote_crypter_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdata"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	requests      atomic.Int32
	authenticator interfaces.Authenticator
	seq           int // internal ordering, a la time.Now()
	keys          map[groupID]map[keyID][]*fakeKey
}

type fakeKey struct {
	id      keyID
	seq     int
	version int64
	key     []byte
}

func newFakeEncryptionService(authenticator interfaces.Authenticator) *fakeEncryptionService {
	return &fakeEncryptionService{
		authenticator: authenticator,
		keys:          make(map[groupID]map[keyID][]*fakeKey),
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

func setup(t *testing.T) (*testauth.TestAuthenticator, interfaces.Crypter, clockwork.FakeClock, *fakeEncryptionService) {
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1, user2, group2, user3, group3))
	te.SetAuthenticator(authenticator)
	encryptionService := newFakeEncryptionService(authenticator)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	enpb.RegisterEncryptionServiceServer(grpcServer, encryptionService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(t.Context(), lis)
	require.NoError(t, err)
	clock := clockwork.NewFakeClock()
	crypter := remote_crypter.New(te, authenticator, clock, conn)
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
			encryptor, err := crypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out))
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

	encryptor, err := crypter.NewEncryptor(ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out))
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
		encryptor, err := crypter.NewEncryptor(user1Ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out))
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
		encryptor, err := crypter.NewEncryptor(user2Ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out))
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
		_, err = crypter.NewEncryptor(user3Ctx, fooDigest, ioutil.NewCustomCommitWriteCloser(out))
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
