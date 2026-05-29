package metacache_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const MB = 1_000_000

var emptyUserMap = testauth.TestUsers()

func getAnonContext(t testing.TB, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	return ctx
}

func runMetacache(t testing.TB, te *real_environment.RealEnv, clock clockwork.Clock, partialOpts metacache.Options) *metacache.Cache {
	t.Helper()
	bc, _ := runMetacacheWithMetadata(t, te, clock, partialOpts)
	return bc
}

func runMetacacheWithMetadata(t testing.TB, te *real_environment.RealEnv, clock clockwork.Clock, partialOpts metacache.Options) (*metacache.Cache, *mockmetadata.Server) {
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
	return bc, mm
}

func metadataForResource(t testing.TB, ctx context.Context, mm *mockmetadata.Server, rn *rspb.ResourceName, partitionID string) *sgpb.FileMetadata {
	t.Helper()
	r := digest.ResourceNameFromProto(rn)
	fr := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:          r.GetCacheType(),
			RemoteInstanceName: r.GetInstanceName(),
			PartitionId:        partitionID,
			GroupId:            interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: r.GetDigestFunction(),
		Compressor:     r.GetCompressor(),
	}
	rsp, err := mm.Get(ctx, &mdpb.GetRequest{FileRecords: []*sgpb.FileRecord{fr}})
	require.NoError(t, err)
	require.Len(t, rsp.GetFileMetadatas(), 1)
	md := rsp.GetFileMetadatas()[0]
	require.NotNil(t, md.GetFileRecord(), "metadata not found for %s", rn.GetDigest().GetHash())
	return md
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

func TestPartitionMappings(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc := runMetacache(t, te, clock, metacache.Options{
		Name:                        "TestPartitionMappings",
		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,
		GCSTTLDays:                  1,
		PartitionMappings: []disk.PartitionMapping{
			{GroupID: interfaces.AuthAnonymousUser, Prefix: "anon/", PartitionID: "anon"},
			{GroupID: interfaces.AuthAnonymousUser, Prefix: "anon/special/", PartitionID: "special"},
			{GroupID: "other-group", Prefix: "anon/", PartitionID: "other"},
		},
	})

	for _, tc := range []struct {
		name         string
		instanceName string
		want         string
	}{
		{name: "no match", instanceName: "default/instance", want: metacache.DefaultPartitionID},
		{name: "anonymous prefix", instanceName: "anon/instance", want: "anon"},
		{name: "first matching mapping wins", instanceName: "anon/special/instance", want: "anon"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bc.Partition(ctx, tc.instanceName)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStorageThresholdBoundaries(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc, mm := runMetacacheWithMetadata(t, te, clock, metacache.Options{
		Name:                        "TestStorageThresholdBoundaries",
		MaxInlineFileSizeBytes:      100,
		MinBytesAutoZstdCompression: 1000,
		GCSTTLDays:                  1,
	})

	for _, tc := range []struct {
		name       string
		size       int64
		wantInline bool
	}{
		{name: "one byte below inline threshold", size: 99, wantInline: true},
		{name: "exactly inline threshold", size: 100, wantInline: false},
		{name: "one byte above inline threshold", size: 101, wantInline: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn, buf := testdigest.RandomCASResourceBuf(t, tc.size)
			require.NoError(t, bc.Set(ctx, rn, buf))

			md := metadataForResource(t, ctx, mm, rn, metacache.DefaultPartitionID)
			if tc.wantInline {
				require.NotNil(t, md.GetStorageMetadata().GetInlineMetadata())
				require.Nil(t, md.GetStorageMetadata().GetGcsMetadata())
			} else {
				require.Nil(t, md.GetStorageMetadata().GetInlineMetadata())
				require.NotNil(t, md.GetStorageMetadata().GetGcsMetadata())
			}
			require.Equal(t, repb.Compressor_IDENTITY, md.GetFileRecord().GetCompressor())

			got, err := bc.Get(ctx, rn)
			require.NoError(t, err)
			require.Equal(t, buf, got)
		})
	}
}

func TestCompressionThresholdBoundaries(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc, mm := runMetacacheWithMetadata(t, te, clock, metacache.Options{
		Name:                        "TestCompressionThresholdBoundaries",
		MaxInlineFileSizeBytes:      1,
		MinBytesAutoZstdCompression: 100,
		GCSTTLDays:                  1,
	})

	for _, tc := range []struct {
		name           string
		size           int64
		wantCompressor repb.Compressor_Value
	}{
		{name: "one byte below compression threshold", size: 99, wantCompressor: repb.Compressor_IDENTITY},
		{name: "exactly compression threshold", size: 100, wantCompressor: repb.Compressor_ZSTD},
		{name: "one byte above compression threshold", size: 101, wantCompressor: repb.Compressor_ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn, buf := testdigest.RandomCASResourceBuf(t, tc.size)
			require.NoError(t, bc.Set(ctx, rn, buf))

			storedRN := rn.CloneVT()
			storedRN.Compressor = tc.wantCompressor
			md := metadataForResource(t, ctx, mm, storedRN, metacache.DefaultPartitionID)
			require.Equal(t, tc.wantCompressor, md.GetFileRecord().GetCompressor())

			got, err := bc.Get(ctx, rn)
			require.NoError(t, err)
			require.Equal(t, buf, got)
		})
	}
}

func TestDeleteRemovesMetadata(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc := runMetacache(t, te, clock, metacache.Options{
		Name:                        "TestDeleteRemovesMetadata",
		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,
		GCSTTLDays:                  1,
	})

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	require.NoError(t, bc.Set(ctx, rn, buf))
	contains, err := bc.Contains(ctx, rn)
	require.NoError(t, err)
	require.True(t, contains)

	require.NoError(t, bc.Delete(ctx, rn))
	contains, err = bc.Contains(ctx, rn)
	require.NoError(t, err)
	require.False(t, contains)
	_, err = bc.Get(ctx, rn)
	require.Error(t, err)
}

func TestExpiredGCSMetadataIsNotReadable(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc := runMetacache(t, te, clock, metacache.Options{
		Name:                        "TestExpiredGCSMetadataIsNotReadable",
		MaxInlineFileSizeBytes:      1,
		MinBytesAutoZstdCompression: 1000,
		GCSTTLDays:                  1,
	})

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	require.NoError(t, bc.Set(ctx, rn, buf))
	got, err := bc.Get(ctx, rn)
	require.NoError(t, err)
	require.Equal(t, buf, got)

	clock.Advance(25 * time.Hour)
	_, err = bc.Get(ctx, rn)
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
}

func TestACInstanceIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, emptyUserMap))
	ctx := getAnonContext(t, te)
	clock := clockwork.NewFakeClock()

	bc := runMetacache(t, te, clock, metacache.Options{
		Name:                        "TestACInstanceIsolation",
		MaxInlineFileSizeBytes:      1000,
		MinBytesAutoZstdCompression: 100,
		GCSTTLDays:                  1,
	})

	actionDigest, err := digest.Compute(bytes.NewReader([]byte("same action key")), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	rn1 := digest.NewACResourceName(actionDigest, "instance-one", repb.DigestFunction_SHA256).ToProto()
	rn2 := digest.NewACResourceName(actionDigest, "instance-two", repb.DigestFunction_SHA256).ToProto()
	buf1 := []byte("action result one")
	buf2 := []byte("action result two")

	require.NoError(t, bc.Set(ctx, rn1, buf1))
	require.NoError(t, bc.Set(ctx, rn2, buf2))
	got1, err := bc.Get(ctx, rn1)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)
	got2, err := bc.Get(ctx, rn2)
	require.NoError(t, err)
	require.Equal(t, buf2, got2)
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
