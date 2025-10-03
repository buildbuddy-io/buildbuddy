package remote_crypter

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// The digest contents is not important as long as the same value is passed
	// to the encrypt and decrypt functions.
	dummyDigest = &repb.Digest{Hash: "foo", SizeBytes: 123}
)

type groupID string
type keyID string

type fakeEncryptionService struct {
	authenticator interfaces.Authenticator
	mu            sync.Mutex
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
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, keys := range f.keys[gid] {
		for _, key := range keys {
			if id == newKey.id && key.version == newKey.version {
				t.FailNow()
			}
		}
	}

	newKey.seq = f.seq
	f.seq++
	f.keys[gid][newKey.id] = append(f.keys[gid][newKey.id], newKey)
}

func (f *fakeEncryptionService) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
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

func setup(t *testing.T) (interfaces.Authenticator, interfaces.Crypter, clockwork.Clock, *fakeEncryptionService) {
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers())
	te.SetAuthenticator(authenticator)
	encryptionService := newFakeEncryptionService(authenticator)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	enpb.RegisterEncryptionServiceServer(grpcServer, encryptionService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(t.Context(), lis)
	require.NoError(t, err)
	clock := clockwork.NewFakeClock()
	crypter, err := new(te, clock, conn)
	require.NoError(t, err)
	return authenticator, crypter, clock, encryptionService
}

func TestEncryptDecrypt(t *testing.T) {
	// env, fakeService := getTestEnv(t)
	// fakeService.add("EK123", 1)

	// userID := "US123"
	// groupID := "GR123"
	// auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
	// env.SetAuthenticator(auther)

	// ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	// require.NoError(t, err)

	// crypter := setup(t, env, fakeService)

	// for _, size := range []int64{1, 10, 100, 1000, 1000 * 1000} {
	// 	t.Run(fmt.Sprint(size), func(t *testing.T) {
	// 		out := bytes.NewBuffer(nil)
	// 		e, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
	// 		require.NoError(t, err)

	// 		testData := make([]byte, size)
	// 		_, err = rand.Read(testData)
	// 		require.NoError(t, err)

	// 		// Write the test data in random chunk sizes
	// 		testdata.WriteInRandomChunks(t, e, testData)

	// 		d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(out), e.Metadata())
	// 		require.NoError(t, err)
	// 		decrypted, err := io.ReadAll(d)
	// 		require.NoError(t, err)

	// 		require.Equal(t, testData, decrypted, "original plaintext and decrypted plaintext do not match")
	// 	})
	// }
}

// func TestDecryptWrongDigest(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	fakeService.addKey("EK123", 1)

// 	userID := "US123"
// 	groupID := "GR123"
// 	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
// 	env.SetAuthenticator(auther)

// 	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
// 	require.NoError(t, err)

// 	crypter := setup(t, env, fakeService)
// 	out := bytes.NewBuffer(nil)

// 	e, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
// 	require.NoError(t, err)

// 	testData := make([]byte, 1000)
// 	_, err = rand.Read(testData)
// 	require.NoError(t, err)

// 	testdata.WriteInRandomChunks(t, e, testData)

// 	// Correct digest should work
// 	d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata())
// 	require.NoError(t, err)
// 	decrypted, err := io.ReadAll(d)
// 	require.NoError(t, err)
// 	require.Equal(t, testData, decrypted)

// 	// Wrong hash should fail authentication
// 	wrongHashDigest := &repb.Digest{Hash: "badhash", SizeBytes: dummyDigest.SizeBytes}
// 	d, err = crypter.NewDecryptor(ctx, wrongHashDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata())
// 	require.NoError(t, err)
// 	_, err = io.ReadAll(d)
// 	require.ErrorContains(t, err, "authentication failed")

// 	// Wrong size should fail authentication
// 	wrongSizeDigest := &repb.Digest{Hash: dummyDigest.Hash, SizeBytes: 9999999999999999}
// 	d, err = crypter.NewDecryptor(ctx, wrongSizeDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata())
// 	require.NoError(t, err)
// 	_, err = io.ReadAll(d)
// 	require.ErrorContains(t, err, "authentication failed")
// }

// func TestKeyLookup(t *testing.T) {
// 	env, fakeService := getTestEnv(t)

// 	userID1 := "US123"
// 	groupID1 := "GR123"
// 	group1KeyID := "EK123"
// 	userID2 := "US456"
// 	groupID2 := "GR456"
// 	group2KeyID := "EK456"
// 	userID3 := "US999"
// 	groupID3 := "GR999"

// 	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID1, groupID1, userID2, groupID2, userID3, groupID3))
// 	env.SetAuthenticator(auther)

// 	// Add keys for group1 and group2, but not group3
// 	fakeService.addKey(group1KeyID, 1)
// 	fakeService.addKey(group2KeyID, 1)

// 	ctx := context.Background()
// 	crypter := setup(t, env, fakeService)

// 	// user1 should be able to encrypt and decrypt using their own key
// 	{
// 		out := bytes.NewBuffer(nil)
// 		ctx, err := auther.WithAuthenticatedUser(ctx, userID1)
// 		require.NoError(t, err)
// 		c, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
// 		require.NoError(t, err)
// 		require.Equal(t, group1KeyID, c.Metadata().GetEncryptionKeyId())
// 		require.EqualValues(t, 1, c.Metadata().GetVersion())

// 		input := []byte("hello world")
// 		testdata.WriteInRandomChunks(t, c, input)
// 		d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), c.Metadata())
// 		require.NoError(t, err)
// 		decrypted := make([]byte, len(input))
// 		_, err = d.Read(decrypted)
// 		require.NoError(t, err)
// 		require.Equal(t, input, decrypted)
// 	}

// 	// user2 should be able to encrypt and decrypt using their own key
// 	{
// 		out := bytes.NewBuffer(nil)
// 		ctx, err := auther.WithAuthenticatedUser(ctx, userID2)
// 		require.NoError(t, err)
// 		c, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
// 		require.NoError(t, err)
// 		require.Equal(t, group2KeyID, c.Metadata().GetEncryptionKeyId())
// 		require.EqualValues(t, 1, c.Metadata().GetVersion())

// 		input := []byte("hello universe")
// 		testdata.WriteInRandomChunks(t, c, input)
// 		d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), c.Metadata())
// 		require.NoError(t, err)
// 		decrypted := make([]byte, len(input))
// 		_, err = d.Read(decrypted)
// 		require.NoError(t, err)
// 		require.Equal(t, input, decrypted)
// 	}

// 	// user3 key lookup should fail since they don't have a key setup
// 	{
// 		ctx, err := auther.WithAuthenticatedUser(ctx, userID3)
// 		require.NoError(t, err)
// 		_, err = crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(bytes.NewBuffer(nil)))
// 		require.True(t, status.IsNotFoundError(err) || status.IsUnavailableError(err))
// 	}
// }

// func TestActiveKey(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	fakeService.addKey("EK123", 1)

// 	userID := "US123"
// 	groupID := "GR123"
// 	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
// 	env.SetAuthenticator(auther)

// 	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
// 	require.NoError(t, err)

// 	crypter := setup(t, env, fakeService)

// 	metadata, err := crypter.ActiveKey(ctx)
// 	require.NoError(t, err)
// 	require.Equal(t, "EK123", metadata.GetEncryptionKeyId())
// 	require.EqualValues(t, 1, metadata.GetVersion())
// }

// func TestSetEncryptionConfig_Unimplemented(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	crypter := setup(t, env, fakeService)

// 	_, err := crypter.SetEncryptionConfig(context.Background(), &enpb.SetEncryptionConfigRequest{})
// 	require.True(t, status.IsUnimplementedError(err))
// }

// func TestGetEncryptionConfig_Unimplemented(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	crypter := setup(t, env, fakeService)

// 	_, err := crypter.GetEncryptionConfig(context.Background(), &enpb.GetEncryptionConfigRequest{})
// 	require.True(t, status.IsUnimplementedError(err))
// }

// func TestGetEncryptionKey_Unimplemented(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	crypter := setup(t, env, fakeService)

// 	_, err := crypter.GetEncryptionKey(context.Background(), &enpb.GetEncryptionKeyRequest{})
// 	require.True(t, status.IsUnimplementedError(err))
// }

// func TestRefreshKey_SpecificVersion(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	fakeService.addKey("EK123", 1)

// 	userID := "US123"
// 	groupID := "GR123"
// 	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
// 	env.SetAuthenticator(auther)

// 	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
// 	require.NoError(t, err)

// 	crypter := setup(t, env, fakeService)

// 	// Encrypt some data with version 1
// 	out := bytes.NewBuffer(nil)
// 	e, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
// 	require.NoError(t, err)

// 	testData := []byte("test data")
// 	testdata.WriteInRandomChunks(t, e, testData)

// 	metadata := e.Metadata()

// 	// Should be able to decrypt with the specific version
// 	d, err := crypter.NewDecryptor(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), metadata)
// 	require.NoError(t, err)
// 	decrypted, err := io.ReadAll(d)
// 	require.NoError(t, err)
// 	require.Equal(t, testData, decrypted)
// }

// func TestRefreshKey_KeyNotFound(t *testing.T) {
// 	env, fakeService := getTestEnv(t)
// 	// Don't add any keys

// 	userID := "US123"
// 	groupID := "GR123"
// 	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
// 	env.SetAuthenticator(auther)

// 	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
// 	require.NoError(t, err)

// 	crypter := setup(t, env, fakeService)

// 	// Should fail to create encryptor when no key exists
// 	_, err = crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(bytes.NewBuffer(nil)))
// 	require.Error(t, err)
// 	require.True(t, status.IsNotFoundError(err) || status.IsUnavailableError(err))
// }
