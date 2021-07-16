package disk_cache_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func getAnonContext(t *testing.T, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func getTmpDir(t *testing.T) string {
	dir, err := ioutil.TempDir("/tmp", "buildbuddy_diskcache_*")
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.EnsureDirectoryExists(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	})
	return dir
}

func TestGetSet(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		ctx := getAnonContext(t, te)
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		// Set() the bytes in the cache.
		err := dc.Set(ctx, d, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		// Get() the bytes from the cache.
		rbuf, err := dc.Get(ctx, d)
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func randomDigests(t *testing.T, sizes ...int64) map[*repb.Digest][]byte {
	m := make(map[*repb.Digest][]byte)
	for _, size := range sizes {
		d, buf := testdigest.NewRandomDigestBuf(t, size)
		m[d] = buf
	}
	return m
}

func TestMultiGetSet(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)
	digests := randomDigests(t, 10, 20, 11, 30, 40)
	if err := dc.SetMulti(ctx, digests); err != nil {
		t.Fatalf("Error multi-setting digests: %s", err.Error())
	}
	digestKeys := make([]*repb.Digest, 0, len(digests))
	for d := range digests {
		digestKeys = append(digestKeys, d)
	}
	m, err := dc.GetMulti(ctx, digestKeys)
	if err != nil {
		t.Fatalf("Error multi-getting digests: %s", err.Error())
	}
	for d := range digests {
		rbuf, ok := m[d]
		if !ok {
			t.Fatalf("Multi-get failed to return expected digest: %q", d.GetHash())
		}
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if err != nil {
			t.Fatal(err)
		}
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match multi-set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestReadWrite(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		ctx := getAnonContext(t, te)
		d, r := testdigest.NewRandomDigestReader(t, testSize)
		// Use Writer() to set the bytes in the cache.
		wc, err := dc.Writer(ctx, d)
		if err != nil {
			t.Fatalf("Error getting %q writer: %s", d.GetHash(), err.Error())
		}
		if _, err := io.Copy(wc, r); err != nil {
			t.Fatalf("Error copying bytes to cache: %s", err.Error())
		}
		if err := wc.Close(); err != nil {
			t.Fatalf("Error closing writer: %s", err.Error())
		}
		// Use Reader() to get the bytes from the cache.
		reader, err := dc.Reader(ctx, d, 0)
		if err != nil {
			t.Fatalf("Error getting %q reader: %s", d.GetHash(), err.Error())
		}
		d2 := testdigest.ReadDigestAndClose(t, reader)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestSizeLimit(t *testing.T) {
	maxSizeBytes := int64(1000) // 1000 bytes
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)
	digestBufs := randomDigests(t, 400, 400, 400)
	digestKeys := make([]*repb.Digest, 0, len(digestBufs))
	for d, buf := range digestBufs {
		if err := dc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		digestKeys = append(digestKeys, d)
	}
	// Expect the last *2* digests to be present.
	// The first digest should have been evicted.
	for i, d := range digestKeys {
		rbuf, err := dc.Get(ctx, d)
		if i == 0 {
			if err == nil {
				t.Fatalf("%q should have been evicted from cache", d.GetHash())
			}
			continue
		}
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}
		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestLRU(t *testing.T) {
	maxSizeBytes := int64(1000) // 1000 bytes
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)
	digestBufs := randomDigests(t, 400, 400)
	digestKeys := make([]*repb.Digest, 0, len(digestBufs))
	for d, buf := range digestBufs {
		if err := dc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		digestKeys = append(digestKeys, d)
	}
	// Now "use" the first digest written so it is most recently used.
	ok, err := dc.Contains(ctx, digestKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Key %q was not present in cache, it should have been.", digestKeys[0].GetHash())
	}
	// Now write one more digest, which should evict the oldest digest,
	// (the second one we wrote).
	d, buf := testdigest.NewRandomDigestBuf(t, 400)
	if err := dc.Set(ctx, d, buf); err != nil {
		t.Fatal(err)
	}
	digestKeys = append(digestKeys, d)

	// Expect the first and third digests to be present.
	// The second digest should have been evicted.
	for i, d := range digestKeys {
		rbuf, err := dc.Get(ctx, d)
		if i == 1 {
			if err == nil {
				t.Fatalf("%q should have been evicted from cache", d.GetHash())
			}
			continue
		}
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}
		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestFileAtomicity(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)

	eg, gctx := errgroup.WithContext(ctx)
	d, buf := testdigest.NewRandomDigestBuf(t, 100000)
	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			for n := 0; n < 100; n++ {
				if err := dc.Set(gctx, d, buf); err != nil {
					return err
				}
				_, err := dc.Get(gctx, d)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		t.Fatalf("Error reading/writing digest %q from goroutine: %s", d.GetHash(), err.Error())
	}
}

func TestAsyncLoading(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	anonPath := filepath.Join(rootDir, interfaces.AuthAnonymousUser)
	if err := disk.EnsureDirectoryExists(anonPath); err != nil {
		t.Fatal(err)
	}

	// Write some pre-existing data.
	digests := make([]*repb.Digest, 0)
	for i := 0; i < 10000; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		dest := filepath.Join(anonPath, d.GetHash())
		err := os.WriteFile(dest, buf, 0644)
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, d)
	}
	// Create a new disk cache (this will start async processing of
	// the data we just wrote above ^)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure that files on disk exist *immediately*, even though
	// they may not have been async processed yet.
	for _, d := range digests {
		exists, err := dc.Contains(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("%q was not found in cache.", d.GetHash())
		}
	}
	// Write some more files, just to ensure the LRU is appended to.
	for i := 0; i < 1000; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		if err := dc.Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digests = append(digests, d)
	}

	// Wait for loading to async loading to finish.
	// Yeah yeah a sleep is lame.
	time.Sleep(100 * time.Millisecond)

	// Check that everything still exists.
	for _, d := range digests {
		exists, err := dc.Contains(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("%q was not found in cache.", d.GetHash())
		}
	}
}

func TestJanitorThread(t *testing.T) {
	maxSizeBytes := int64(10_000_000) // 10MB
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	rootDir := getTmpDir(t)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Fill the cache.
	digests := make([]*repb.Digest, 0)
	for i := 0; i < 999; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		err := dc.Set(ctx, d, buf)
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, d)
	}

	// Make a new disk cache with a smaller size. The
	// janitor should clean extra data up, oldest first.
	dc, err = disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes/2) // 5MB
	if err != nil {
		t.Fatal(err)
	}
	// GC runs after 100ms, so give it a little time to delete the files.
	time.Sleep(500 * time.Millisecond)
	for i, d := range digests {
		if i > 500 {
			break
		}
		contains, err := dc.Contains(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		if contains {
			t.Fatalf("Expected oldest digest %+v to be deleted", d)
		}
	}
}

func TestZeroLengthFiles(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := getTmpDir(t)
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	anonPath := filepath.Join(rootDir, interfaces.AuthAnonymousUser)
	if err := disk.EnsureDirectoryExists(anonPath); err != nil {
		t.Fatal(err)
	}
	// Write a single zero length file.
	badDigest, _ := testdigest.NewRandomDigestBuf(t, 1000)
	buf := []byte{}
	dest := filepath.Join(anonPath, badDigest.GetHash())
	if err := os.WriteFile(dest, buf, 0644); err != nil {
		t.Fatal(err)
	}

	// Write a valid pre-existing file
	goodDigest, buf := testdigest.NewRandomDigestBuf(t, 1000)
	dest = filepath.Join(anonPath, goodDigest.GetHash())
	if err := os.WriteFile(dest, buf, 0644); err != nil {
		t.Fatal(err)
	}

	// Create a new disk cache (this will start async processing of
	// the data we just wrote above ^)
	dc, err := disk_cache.NewDiskCache(te, &config.DiskConfig{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure that the goodDigest exists and the zero length one does not.
	exists, err := dc.Contains(ctx, badDigest)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("%q (empty file) should not be mapped in cache.", badDigest.GetHash())
	}

	exists, err = dc.Contains(ctx, goodDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("%q was not found in cache.", goodDigest.GetHash())
	}
}

func TestNonDefaultPartition(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := getTmpDir(t)

	// First API key is from a group that is not mapped to a custom partition.
	testAPIKey1 := "AK1111"
	testGroup1 := "GR1234"
	// Second API key is for a group mapped to the "other" custom partition.
	testAPIKey2 := "AK2222"
	testGroup2 := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey1, testGroup1, testAPIKey2, testGroup2)
	te := getTestEnv(t, testUsers)

	otherPartitionID := "other"
	otherPartitionPrefix := "myteam/"
	diskConfig := &config.DiskConfig{
		RootDirectory: rootDir,
		Partitions: []config.DiskCachePartition{
			{
				ID:           "default",
				MaxSizeBytes: 10_000_000,
			},
			{
				ID:           otherPartitionID,
				MaxSizeBytes: 10_000_000,
			},
		},
		PartitionMappings: []config.DiskCachePartitionMapping{
			{
				GroupID:     testGroup2,
				Prefix:      otherPartitionPrefix,
				PartitionID: otherPartitionID,
			},
		},
	}

	dc, err := disk_cache.NewDiskCache(te, diskConfig, maxSizeBytes)
	require.NoError(t, err)

	// Anonymous user on default partition.
	{
		ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)

		err = dc.Set(ctx, d, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, interfaces.AuthAnonymousUser)
		dPath := filepath.Join(userRoot, d.GetHash())
		require.FileExists(t, dPath)
	}

	// Authenticated user on default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey1)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)

		err = dc.Set(ctx, d, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, testGroup1)
		dPath := filepath.Join(userRoot, d.GetHash())
		require.FileExists(t, dPath)
	}

	// Authenticated user with group ID that matches custom partition, but without a matching instance name prefix.
	// Data should go to the default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey2)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)

		instanceName := "nonmatchingprefix"
		c, err := dc.WithIsolation(ctx, interfaces.CASCacheType, instanceName)
		require.NoError(t, err)
		err = c.Set(ctx, d, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, testGroup2)
		dPath := filepath.Join(userRoot, instanceName, d.GetHash())
		require.FileExists(t, dPath)
	}

	// Authenticated user with group ID that matches custom partition and instance name prefix that matches non-default
	// partition.
	// Data should go to the matching partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey2)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)

		instanceName := otherPartitionPrefix + "hello"
		c, err := dc.WithIsolation(ctx, interfaces.CASCacheType, instanceName)
		require.NoError(t, err)
		err = c.Set(ctx, d, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, disk_cache.PartitionDirectoryPrefix+otherPartitionID, testGroup2)
		dPath := filepath.Join(userRoot, instanceName, d.GetHash())
		require.FileExists(t, dPath)
	}
}
