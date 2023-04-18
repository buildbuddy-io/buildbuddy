package crypter_service

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	mrand "math/rand"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// The digest contents is not important as long as the same value is passed
	// to the encrypt and decrypt functions.
	dummyDigest = &repb.Digest{Hash: "foo", SizeBytes: 123}
)

type fakeKmsKey struct {
	key        []byte
	err        error
	fetchCount int
}

type fakeKMS struct {
	t    *testing.T
	mu   sync.Mutex
	keys map[string]*fakeKmsKey
}

func newFakeKMS(t *testing.T) *fakeKMS {
	return &fakeKMS{
		t:    t,
		keys: make(map[string]*fakeKmsKey),
	}
}

func (f *fakeKMS) SetKey(uri string, key []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[uri] = &fakeKmsKey{key: key}
}

func (f *fakeKMS) RemoveKey(uri string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, uri)
}

func (f *fakeKMS) ReturnError(uri string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[uri] = &fakeKmsKey{err: err}
}

func (f *fakeKMS) FetchMasterKey() (interfaces.AEAD, error) {
	return f.FetchKey("master")
}

func (f *fakeKMS) ResetFetchCount(uri string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keyData, ok := f.keys[uri]
	if !ok {
		require.FailNowf(f.t, "key not found", "key %q not in fake KMS", uri)
	}
	keyData.fetchCount = 0
}

func (f *fakeKMS) GetFetchCount(uri string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	keyData, ok := f.keys[uri]
	if !ok {
		require.FailNowf(f.t, "key not found", "key %q not in fake KMS", uri)
	}
	return keyData.fetchCount
}

type gcmAESAEAD struct {
	ciph cipher.AEAD
}

func (g *gcmAESAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	nonce := make([]byte, g.ciph.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := g.ciph.Seal(nil, nonce, plaintext, associatedData)
	return append(nonce, out...), nil
}

func (g *gcmAESAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < g.ciph.NonceSize() {
		return nil, status.InvalidArgumentErrorf("input ciphertext too short")
	}
	nonce := ciphertext[:g.ciph.NonceSize()]
	return g.ciph.Open(nil, nonce, ciphertext[g.ciph.NonceSize():], associatedData)
}

func (f *fakeKMS) FetchKey(uri string) (interfaces.AEAD, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keyData, ok := f.keys[uri]
	if !ok {
		return nil, status.NotFoundErrorf("key %q not found", uri)
	}
	keyData.fetchCount++
	if keyData.err != nil {
		return nil, keyData.err
	}

	bc, err := aes.NewCipher(keyData.key)
	if err != nil {
		return nil, err
	}
	ciph, err := cipher.NewGCM(bc)
	if err != nil {
		return nil, err
	}
	return &gcmAESAEAD{ciph: ciph}, nil
}

func generateKMSKey(t *testing.T, f *fakeKMS, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	f.SetKey(id, key)
	return id
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

func createKey(t *testing.T, env environment.Env, keyID, groupID, groupKeyURI string) {
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
	encGroupKeyPart, err := groupAEAD.Encrypt(groupKeyPart, []byte(groupID))

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
	ctx := context.Background()
	err = env.GetDBHandle().DB(ctx).Create(key).Error
	require.NoError(t, err)
	err = env.GetDBHandle().DB(ctx).Create(keyVersion).Error
	require.NoError(t, err)
}

func getEnv(t *testing.T) (*testenv.TestEnv, *fakeKMS) {
	mrand.Seed(time.Now().UnixMicro())

	flags.Set(t, "database.enable_encryption_schema", true)

	kms := newFakeKMS(t)

	generateKMSKey(t, kms, "master")

	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{})
	env.SetKMS(kms)
	return env, kms
}

func TestEncryptDecrypt(t *testing.T) {
	env, kms := getEnv(t)
	customerKeyURI := generateKMSKey(t, kms, "customerKey")

	userID := "US123"
	groupID := "GR123"
	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
	env.SetAuthenticator(auther)
	createKey(t, env, "EK123", groupID, customerKeyURI)

	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	crypter, err := New(env, clockwork.NewRealClock())
	defer crypter.Stop()
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)
	for _, size := range []int64{1, 10, 100, 1000, 1000 * 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			e, err := crypter.newEncryptorWithChunkSize(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
			require.NoError(t, err)

			testData := make([]byte, size)
			_, err = rand.Read(testData)
			require.NoError(t, err)

			// Write the test data in random chunk sizes. The input chunk sizes should
			// not affect the final result.
			writeInRandomChunks(t, e, testData)

			d, err := crypter.newDecryptorWithChunkSize(ctx, dummyDigest, io.NopCloser(out), e.Metadata(), groupID, 1024)
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
	env, kms := getEnv(t)
	customerKeyURI := generateKMSKey(t, kms, "customerKey")

	userID := "US123"
	groupID := "GR123"
	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
	env.SetAuthenticator(auther)
	createKey(t, env, "EK123", groupID, customerKeyURI)

	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	crypter, err := New(env, clockwork.NewRealClock())
	defer crypter.Stop()
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)

	e, err := crypter.newEncryptorWithChunkSize(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	writeInRandomChunks(t, e, testData)

	// Reading with the correct groupID should be OK.
	d, err := crypter.newDecryptorWithChunkSize(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)

	// If group ID doesn't match, authentication should fail.
	d, err = crypter.newDecryptorWithChunkSize(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), "GRBAD", 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
}

func TestDecryptWrongDigest(t *testing.T) {
	env, kms := getEnv(t)
	customerKeyURI := generateKMSKey(t, kms, "customerKey")

	userID := "US123"
	groupID := "GR123"
	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID, groupID))
	env.SetAuthenticator(auther)

	createKey(t, env, "EK123", groupID, customerKeyURI)

	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	crypter, err := New(env, clockwork.NewRealClock())
	defer crypter.Stop()
	require.NoError(t, err)
	out := bytes.NewBuffer(nil)

	e, err := crypter.newEncryptorWithChunkSize(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	writeInRandomChunks(t, e, testData)

	d, err := crypter.newDecryptorWithChunkSize(ctx, dummyDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)

	wrongHashDigest := &repb.Digest{Hash: "badhash", SizeBytes: dummyDigest.SizeBytes}
	d, err = crypter.newDecryptorWithChunkSize(ctx, wrongHashDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")

	wrongSizeDigest := &repb.Digest{Hash: dummyDigest.Hash, SizeBytes: 9999999999999999}
	d, err = crypter.newDecryptorWithChunkSize(ctx, wrongSizeDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	decrypted, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
}

func TestKeyLookup(t *testing.T) {
	env, kms := getEnv(t)

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

	group1KeyURI := generateKMSKey(t, kms, "group1Key")
	group2KeyURI := generateKMSKey(t, kms, "group2Key")
	ctx := context.Background()

	// Add separate keys for the first and second users.
	// Third user doesn't have a key configured.
	createKey(t, env, group1KeyID, groupID1, group1KeyURI)
	createKey(t, env, group2KeyID, groupID2, group2KeyURI)

	crypter, err := New(env, clockwork.NewRealClock())
	defer crypter.Stop()
	require.NoError(t, err)

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

func testEncryptDecrypt(ctx context.Context, t *testing.T, auther *testauth.TestAuthenticator, crypter *Crypter, userID string, expectedKeyID string) {
	out := bytes.NewBuffer(nil)
	ctx, err := auther.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	c, err := crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
	require.NoError(t, err)
	require.Equal(t, c.Metadata().GetEncryptionKeyId(), expectedKeyID)
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

func testKeyError(ctx context.Context, t *testing.T, auther *testauth.TestAuthenticator, crypter *Crypter, userID string, clock clockwork.FakeClock) error {
	out := bytes.NewBuffer(nil)
	ctx, err := auther.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)

	// If we are expecting an error, keep advancing the clock to run out the
	// retries.
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
				clock.Advance(1 * time.Second)
			}
		}
	}()
	_, err = crypter.NewEncryptor(ctx, dummyDigest, ioutil.NewCustomCommitWriteCloser(out))
	require.Error(t, err)
	return err
}

// advances the fake clock once and waits for the refresh to be done.
func advanceTimeAndWaitForRefresh(clock clockwork.FakeClock, crypter *Crypter, dur time.Duration) {
	lastRun := crypter.testGetLastCacheRefreshRun()
	clock.Advance(dur)
	for !crypter.testGetLastCacheRefreshRun().After(lastRun) {
		time.Sleep(5 * time.Millisecond)
	}
	for crypter.testGetCacheActiveRefreshOps() > 0 {
		time.Sleep(5 * time.Millisecond)
	}
}

// similar to above, but continuously moves fake clock forward while refresh in
// progress for testing error scenarios where retries may be attempted.
func contAdvanceTimeAndWaitForRefresh(clock clockwork.FakeClock, crypter *Crypter, dur time.Duration) {
	lastRun := crypter.testGetLastCacheRefreshRun()
	clock.Advance(dur)
	for !crypter.testGetLastCacheRefreshRun().After(lastRun) {
		time.Sleep(5 * time.Millisecond)
	}
	for crypter.testGetCacheActiveRefreshOps() > 0 {
		time.Sleep(5 * time.Millisecond)
		clock.Advance(1 * time.Second)
	}
}

func TestKeyCaching(t *testing.T) {
	env, kms := getEnv(t)

	userID1 := "US123"
	groupID1 := "GR123"
	group1KeyID := "EK123"
	userID2 := "US456"
	groupID2 := "GR456"
	group2KeyID := "EK456"
	auther := testauth.NewTestAuthenticator(testauth.TestUsers(userID1, groupID1, userID2, groupID2))
	env.SetAuthenticator(auther)

	group1KeyURI := generateKMSKey(t, kms, "group1Key")
	group2KeyURI := generateKMSKey(t, kms, "group2Key")
	ctx := context.Background()

	// Add separate keys for the first and second users.
	// Third user doesn't have a key configured.
	createKey(t, env, group1KeyID, groupID1, group1KeyURI)
	createKey(t, env, group2KeyID, groupID2, group2KeyURI)

	// Note that encryption and decryption keys are cached separately so a
	// encryption/decryption round trip requires two key lookups.

	// Back to back operations should only fetch the keys once.
	{
		crypter, err := New(env, clockwork.NewRealClock())
		require.NoError(t, err)
		kms.ResetFetchCount(group1KeyURI)
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))
		crypter.Stop()
	}

	// Various cache refresh conditions.
	{
		clock := clockwork.NewFakeClock()
		crypter, err := New(env, clock)
		require.NoError(t, err)
		kms.ResetFetchCount(group1KeyURI)
		kms.ResetFetchCount(group2KeyURI)
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))

		// If we're not close enough to expiration, no refresh should be
		// triggered.
		advanceTimeAndWaitForRefresh(clock, crypter, keyRefreshScanFrequency)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))

		// If we are getting closer to expiration, but the keys have not been
		// used recently then we should not have tried to refresh them.
		advanceTimeAndWaitForRefresh(clock, crypter, 6*time.Minute)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))

		// Perform encryption/decryption ops which should mark the key as
		// recently used.
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))
		// On the next fresh, we should attempt to fetch the keys from KMS.
		advanceTimeAndWaitForRefresh(clock, crypter, keyRefreshScanFrequency)
		require.Equal(t, 4, kms.GetFetchCount(group1KeyURI))

		// If we are past expiration, the keys should be removed from the cache
		// and the next ops should trigger a new fetch.
		advanceTimeAndWaitForRefresh(clock, crypter, 20*time.Minute)
		require.Equal(t, 4, kms.GetFetchCount(group1KeyURI))
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		require.Equal(t, 6, kms.GetFetchCount(group1KeyURI))

		// There shouldn't have been any fetches of the group2 keys.
		require.Equal(t, 0, kms.GetFetchCount(group2KeyURI))

		crypter.Stop()
	}

	// Test refresh error handling.
	{
		clock := clockwork.NewFakeClock()
		crypter, err := New(env, clock)
		require.NoError(t, err)
		kms.ResetFetchCount(group1KeyURI)
		kms.ResetFetchCount(group2KeyURI)
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		require.Equal(t, 2, kms.GetFetchCount(group1KeyURI))

		kms.ReturnError(group1KeyURI, status.UnavailableError("mainframe down"))

		// If we are getting closer to expiration, we should try to refresh.
		advanceTimeAndWaitForRefresh(clock, crypter, 6*time.Minute)
		// Need to use the key so it's marked as recently used.
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)
		contAdvanceTimeAndWaitForRefresh(clock, crypter, keyRefreshScanFrequency)
		require.Greater(t, kms.GetFetchCount(group1KeyURI), 2)

		// Next scan loop shouldn't try to refresh the key since it has already
		// been recently checked.
		oldCount := kms.GetFetchCount(group1KeyURI)
		advanceTimeAndWaitForRefresh(clock, crypter, keyRefreshScanFrequency)
		require.Equal(t, oldCount, kms.GetFetchCount(group1KeyURI))

		// But we should try again once we get past the retry interval.
		oldCount = kms.GetFetchCount(group1KeyURI)
		contAdvanceTimeAndWaitForRefresh(clock, crypter, keyRefreshRetryInterval)
		require.Greater(t, kms.GetFetchCount(group1KeyURI), oldCount)

		// Encryption should continue to use the cached key until we reach
		// expiration time.
		testEncryptDecrypt(ctx, t, auther, crypter, userID1, group1KeyID)

		advanceTimeAndWaitForRefresh(clock, crypter, 20*time.Minute)
		err = testKeyError(ctx, t, auther, crypter, userID1, clock)
		require.True(t, status.IsUnavailableError(err))
	}
}

func TestConfigAPI(t *testing.T) {
	env, kms := getEnv(t)

	//groupKeyID := "EK123"

	auther := enterprise_testauth.Configure(t, env)
	users := enterprise_testauth.CreateRandomGroups(t, env)
	var userID, groupID string
	for _, u := range users {
		if len(u.Groups) != 1 || u.Groups[0].Role != uint32(role.Admin) {
			continue
		}
		userID = u.UserID
		groupID = u.Groups[0].Group.GroupID
		break
	}

	groupKMSKeyID := "groupKey"
	groupKMSKey := make([]byte, 32)
	_, err := rand.Read(groupKMSKey)
	require.NoError(t, err)
	kms.SetKey(groupKMSKeyID, groupKMSKey)

	userCtx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	apiKeys, err := env.GetUserDB().GetAPIKeys(userCtx, groupID)
	require.NoError(t, err)
	apiKeyCtx := auther.AuthContextFromAPIKey(context.Background(), apiKeys[0].Value)

	rootDir := testfs.MakeTempDir(t)
	cacheSizeBytes := int64(1000000)
	customPartID := "CPART"
	opts := &pebble_cache.Options{
		RootDirectory: rootDir,
		Partitions: []disk.Partition{
			{ID: "default", MaxSizeBytes: cacheSizeBytes},
			{ID: customPartID, MaxSizeBytes: cacheSizeBytes, EncryptionSupported: true},
		},
		PartitionMappings: []disk.PartitionMapping{
			{GroupID: groupID, PartitionID: customPartID},
		},
	}
	pc, err := pebble_cache.NewPebbleCache(env, opts)
	env.SetCache(pc)
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)
	defer pc.Stop()

	clock := clockwork.NewFakeClock()
	crypter, err := New(env, clock)
	require.NoError(t, err)
	env.SetCrypter(crypter)

	// Write unencrypted data, this should remain readable even after keys
	// are deleted.
	plaintextResource, plaintextBuf := testdigest.RandomCASResourceBuf(t, 10)
	err = pc.Set(apiKeyCtx, plaintextResource, plaintextBuf)
	require.NoError(t, err)

	// Enable encryption.
	_, err = crypter.SetEncryptionConfig(userCtx, &enpb.SetEncryptionConfigRequest{
		Enabled: true,
		KeyUri:  groupKMSKeyID,
	})
	require.NoError(t, err)
	apiKeyCtx = auther.AuthContextFromAPIKey(context.Background(), apiKeys[0].Value)

	// Write an encrypted resource. This resource should become unreadable
	// after encryption keys become unavailable.
	encryptedResource, encryptedBuf := testdigest.RandomACResourceBuf(t, 10)
	err = pc.Set(apiKeyCtx, encryptedResource, encryptedBuf)

	// Remove the key from the KMS and let the cached key expire. Previously
	// encrypted data should become unreadable.
	kms.RemoveKey(groupKMSKeyID)
	advanceTimeAndWaitForRefresh(clock, crypter, 11*time.Minute)

	// Should succeed since this digest was written before encryption was
	// enabled.
	plaintextReadBuf, err := pc.Get(apiKeyCtx, plaintextResource)
	require.NoError(t, err)
	require.Equal(t, plaintextBuf, plaintextReadBuf)

	// Shouldn't be able to read the encrypted resource anymore.
	_, err = pc.Get(apiKeyCtx, encryptedResource)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))

	// Restore the key so we can test disabling encryption via the API.
	kms.SetKey(groupKMSKeyID, groupKMSKey)
	_, err = pc.Get(apiKeyCtx, encryptedResource)
	require.NoError(t, err)

	// Disable encryption, this deletes the encryption keys making previously
	// written encrypted data unreadable.
	_, err = crypter.SetEncryptionConfig(userCtx, &enpb.SetEncryptionConfigRequest{
		Enabled: false,
	})
	require.NoError(t, err)
	apiKeyCtx = auther.AuthContextFromAPIKey(context.Background(), apiKeys[0].Value)
	advanceTimeAndWaitForRefresh(clock, crypter, 11*time.Minute)
	_, err = pc.Get(apiKeyCtx, encryptedResource)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}
