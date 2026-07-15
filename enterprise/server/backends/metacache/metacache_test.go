package metacache_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/metacache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/mockmetadata"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockgcs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const MB = 1_000_000

var emptyUserMap = testauth.TestUsers()

func getAnonContext(t testing.TB, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	return ctx
}

// gcsObjectCounter observes the number of blobs stored in the mock GCS, so
// tests can detect duplicate writes that should have been deduped.
type gcsObjectCounter interface {
	ObjectCount() int
}

func runMetacache(t testing.TB, te *real_environment.RealEnv, clock clockwork.Clock, partialOpts metacache.Options) *metacache.Cache {
	bc, _ := runMetacacheWithGCS(t, te, clock, partialOpts)
	return bc
}

func runMetacacheWithGCS(t testing.TB, te *real_environment.RealEnv, clock clockwork.Clock, partialOpts metacache.Options) (*metacache.Cache, gcsObjectCounter) {
	t.Helper()

	if partialOpts.GCSTTLDays == 0 {
		t.Fatal("opts.GCSTTLDays must be set")
	}
	opts := partialOpts

	mockGCS := mockgcs.New(clock)
	mockGCS.SetBucketCustomTimeTTL(context.TODO(), int64(opts.GCSTTLDays))
	opts.FileStorer = filestore.New(filestore.WithGCSBlobstore(mockGCS, "one-bucket"))

	mm, err := mockmetadata.NewServer(1e6 /*maxSizeBytes*/, opts.FileStorer)
	require.NoError(t, err)

	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	mdspb.RegisterMetadataServiceServer(te.GetGRPCServer(), mm)
	conn, err := testenv.LocalGRPCConn(
		te.GetServerContext(),
		lis,
		interceptors.GetUnaryClientIdentityInterceptor(te),
		interceptors.GetStreamClientIdentityInterceptor(te),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	go runServer()

	opts.Clock = clock
	opts.MetadataClient = mdspb.NewMetadataServiceClient(conn)

	bc, err := metacache.New(te, opts)
	require.NoError(t, err)
	return bc, mockGCS
}

func TestReadWrite(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	options := metacache.Options{
		Name: "TestReadWrite",

		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,

		GCSTTLDays: 1,
	}
	bc := runMetacache(t, te, clock, options)

	testSizes := []int64{
		1, 10, 100, 256, 512, 1000, 1024, 2 * 1024, 10000, 1000000,
	}
	for _, testSize := range testSizes {
		desc := fmt.Sprintf("size_%d", testSize)
		t.Run(desc, func(t *testing.T) {
			rn, buf := testdigest.RandomCASResourceBuf(t, testSize)
			// Use Writer() to set the bytes in the cache.
			wc, err := bc.Writer(ctx, rn)
			require.NoError(t, err, "Error getting %q writer", rn.GetDigest().GetHash())
			_, err = wc.Write(buf)
			require.NoError(t, err)
			err = wc.Commit()
			require.NoError(t, err)
			err = wc.Close()
			require.NoError(t, err)

			// Use Reader() to get the bytes from the cache.
			reader, err := bc.Reader(ctx, rn, 0, 0)
			require.NoError(t, err, "Error getting %q reader", rn.GetDigest().GetHash())
			d2 := testdigest.ReadDigestAndClose(t, reader)
			require.Equal(t, rn.GetDigest().GetHash(), d2.GetHash())
		})
	}
}

func TestGetSet(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	options := metacache.Options{
		Name: "TestGetSet",

		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,

		GCSTTLDays: 1,
	}
	bc := runMetacache(t, te, clock, options)

	testSizes := []int64{
		1, 10, 100, 256, 512, 1000,
	}
	for _, testSize := range testSizes {
		desc := fmt.Sprintf("size_%d", testSize)
		t.Run(desc, func(t *testing.T) {
			rn, buf := testdigest.RandomCASResourceBuf(t, testSize)
			err := bc.Set(ctx, rn, buf)
			require.NoError(t, err)

			gotBuf, err := bc.Get(ctx, rn)
			require.NoError(t, err)
			require.Equal(t, buf, gotBuf)
		})
	}

}

// dedupOptions returns metacache options with an inline threshold low enough
// that blobs of a couple KB land in GCS, so the dedup tests can observe the
// GCS object count.
func dedupOptions(name string) metacache.Options {
	return metacache.Options{
		Name:                        name,
		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,
		GCSTTLDays:                  1,
	}
}

func TestSetDeduplicatesExistingCAS(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc, gcs := runMetacacheWithGCS(t, te, clock, dedupOptions("TestSetDeduplicatesExistingCAS"))

	rn, buf := testdigest.RandomCASResourceBuf(t, 2000)

	require.NoError(t, bc.Set(ctx, rn, buf))
	require.Equal(t, 1, gcs.ObjectCount())

	// Setting the same CAS blob again must be deduped: no second GCS object.
	require.NoError(t, bc.Set(ctx, rn, buf))
	require.Equal(t, 1, gcs.ObjectCount())

	got, err := bc.Get(ctx, rn)
	require.NoError(t, err)
	require.Equal(t, buf, got)
}

func TestWriterDeduplicatesExistingCAS(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc, gcs := runMetacacheWithGCS(t, te, clock, dedupOptions("TestWriterDeduplicatesExistingCAS"))

	rn, buf := testdigest.RandomCASResourceBuf(t, 2000)
	require.NoError(t, bc.Set(ctx, rn, buf))
	require.Equal(t, 1, gcs.ObjectCount())

	// Writing the same CAS blob via Writer() must be deduped: the bytes are
	// dropped and no second GCS object is created.
	wc, err := bc.Writer(ctx, rn)
	require.NoError(t, err)
	_, err = wc.Write(buf)
	require.NoError(t, err)
	require.NoError(t, wc.Commit())
	require.NoError(t, wc.Close())
	require.Equal(t, 1, gcs.ObjectCount())

	got, err := bc.Get(ctx, rn)
	require.NoError(t, err)
	require.Equal(t, buf, got)
}

func TestSetMultiDeduplicatesExistingCAS(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc, gcs := runMetacacheWithGCS(t, te, clock, dedupOptions("TestSetMultiDeduplicatesExistingCAS"))

	rnA, bufA := testdigest.RandomCASResourceBuf(t, 2000)
	require.NoError(t, bc.Set(ctx, rnA, bufA))
	require.Equal(t, 1, gcs.ObjectCount())

	// A is already present and must be skipped; only B should be written.
	rnB, bufB := testdigest.RandomCASResourceBuf(t, 2000)
	require.NoError(t, bc.SetMulti(ctx, map[*rspb.ResourceName][]byte{
		rnA: bufA,
		rnB: bufB,
	}))
	require.Equal(t, 2, gcs.ObjectCount())

	gotB, err := bc.Get(ctx, rnB)
	require.NoError(t, err)
	require.Equal(t, bufB, gotB)
}

func TestACWritesAreNotDeduplicated(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc, _ := runMetacacheWithGCS(t, te, clock, dedupOptions("TestACWritesAreNotDeduplicated"))

	rn, buf1 := testdigest.RandomACResourceBuf(t, 2000)
	require.NoError(t, bc.Set(ctx, rn, buf1))

	// AC entries are mutable: a second Set with different data must overwrite,
	// not be deduped away.
	buf2 := make([]byte, len(buf1))
	_, err := rand.Read(buf2)
	require.NoError(t, err)
	require.NoError(t, bc.Set(ctx, rn, buf2))

	got, err := bc.Get(ctx, rn)
	require.NoError(t, err)
	require.Equal(t, buf2, got)
}

func TestFindMissing(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	options := metacache.Options{
		Name: "TestFindMissing",

		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,

		GCSTTLDays: 1,
	}
	bc := runMetacache(t, te, clock, options)

	testSizes := []int64{50, 100, 1000, 1500, 10000}
	for _, testSize := range testSizes {
		desc := fmt.Sprintf("size_%d", testSize)
		t.Run(desc, func(t *testing.T) {
			r, buf := testdigest.RandomCASResourceBuf(t, testSize)
			notSetR1, _ := testdigest.RandomCASResourceBuf(t, testSize)
			notSetR2, _ := testdigest.RandomCASResourceBuf(t, testSize)

			err := bc.Set(ctx, r, buf)
			require.NoError(t, err)

			rns := []*rspb.ResourceName{r, notSetR1, notSetR2}
			missing, err := bc.FindMissing(ctx, rns)
			require.NoError(t, err)
			require.ElementsMatch(t, []*repb.Digest{notSetR1.GetDigest(), notSetR2.GetDigest()}, missing)

			rns = []*rspb.ResourceName{r}
			missing, err = bc.FindMissing(ctx, rns)
			require.NoError(t, err)
			require.Empty(t, missing)
		})
	}
}

func generateKMSKey(t *testing.T, kmsDir string, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(kmsDir, id), key, 0644)
	require.NoError(t, err)
	return "local-insecure-kms://" + id
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
	encMasterKeyPart, err := masterAEAD.Encrypt(masterKeyPart, []byte(groupID))
	require.NoError(t, err)

	groupAEAD, err := kmsClient.FetchKey(groupKeyURI)
	require.NoError(t, err)
	encGroupKeyPart, err := groupAEAD.Encrypt(groupKeyPart, []byte(groupID))
	require.NoError(t, err)

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

func getCrypterEnv(t *testing.T) (*testenv.TestEnv, string) {
	kmsDir := testfs.MakeTempDir(t)
	masterKeyURI := generateKMSKey(t, kmsDir, "masterKey")

	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	flags.Set(t, "keystore.master_key_uri", masterKeyURI)
	env := testenv.GetTestEnv(t)
	err := kms.Register(env)
	require.NoError(t, err)
	err = crypter_service.Register(env)
	require.NoError(t, err)
	return env, kmsDir
}

func TestEncryption(t *testing.T) {
	testCases := []struct {
		desc                   string
		maxInlineFileSizeBytes int64
		digestSize             int64
	}{
		{
			desc:                   "inline",
			maxInlineFileSizeBytes: 200,
			digestSize:             100,
		},
		{
			desc:                   "non-inlined",
			maxInlineFileSizeBytes: 100,
			digestSize:             1000,
		},
	}

	withKey := []bool{false, true}

	for _, tc := range testCases {
		for _, isKeyAvailable := range withKey {
			desc := fmt.Sprintf("%s_is_key_available_%t", tc.desc, isKeyAvailable)
			t.Run(desc, func(t *testing.T) {
				te, kmsDir := getCrypterEnv(t)

				userID := "US123"
				groupID := "GR123"
				groupKeyID := "EK123"
				user := testauth.User(userID, groupID)
				user.CacheEncryptionEnabled = true
				users := map[string]interfaces.UserInfo{userID: user}
				auther := testauth.NewTestAuthenticator(t, users)
				te.SetAuthenticator(auther)

				ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
				require.NoError(t, err)

				if isKeyAvailable {
					group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
					key, keyVersion := createKey(t, te, groupKeyID, groupID, group1KeyURI)
					err = te.GetDBHandle().NewQuery(ctx, "create_key").Create(key)
					require.NoError(t, err)
					err = te.GetDBHandle().NewQuery(ctx, "create_key_version").Create(keyVersion)
					require.NoError(t, err)
				}

				clock := clockwork.NewFakeClock()
				options := metacache.Options{
					Name:                        desc,
					MaxInlineFileSizeBytes:      tc.maxInlineFileSizeBytes,
					MinBytesAutoZstdCompression: 100,
					GCSTTLDays:                  1,
				}
				bc := runMetacache(t, te, clock, options)

				rn, buf := testdigest.RandomCASResourceBuf(t, tc.digestSize)
				err = bc.Set(ctx, rn, buf)
				if !isKeyAvailable {
					require.ErrorContains(t, err, "no key available")
				} else {
					readBuf, err := bc.Get(ctx, rn)
					require.NoError(t, err)
					require.Equal(t, buf, readBuf)
				}
			})
		}
	}
}

func randomDigests(t *testing.T, sizes ...int64) map[*rspb.ResourceName][]byte {
	m := make(map[*rspb.ResourceName][]byte)
	for _, size := range sizes {
		rn, buf := testdigest.RandomCASResourceBuf(t, size)
		m[rn] = buf
	}
	return m
}

func TestMultiGetSet(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	options := metacache.Options{
		Name: "TestGetSet",

		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,

		GCSTTLDays: 1,
	}
	bc := runMetacache(t, te, clock, options)

	digests := randomDigests(t, 10, 20, 11, 30, 1024, 40)
	err := bc.SetMulti(ctx, digests)
	require.NoError(t, err)

	resourceNames := make([]*rspb.ResourceName, 0, len(digests))
	for d := range digests {
		resourceNames = append(resourceNames, d)
	}

	m, err := bc.GetMulti(ctx, resourceNames)
	require.NoError(t, err)
	for rn := range digests {
		d := rn.GetDigest()
		rbuf, ok := m[d]
		require.True(t, ok, "Multi-get failed to return expected digest: %q", d.GetHash())
		d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
		require.NoError(t, err)
		require.Equal(t, d.GetHash(), d2.GetHash(), "d=%v; d2=%v", d, d2)
	}
}
