package disk_cache_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	emptyUserMap = testauth.TestUsers()
)

const (
	// All files on disk will be a multiple of this block size, assuming a
	// filesystem with default settings.
	defaultExt4BlockSize = 4096
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
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

func TestGetSet(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)

	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	instanceName := "remoteInstanceName"
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: instanceName,
		}
		// Set() the bytes in the cache.
		err := dc.Set(ctx, r, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		// Get() the bytes from the cache.
		rbuf, err := dc.Get(ctx, r)
		require.NoError(t, err)

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestMetadata(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)

	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_AC,
			InstanceName: "remoteInstanceName",
		}
		// Set() the bytes in the cache.
		err := dc.Set(ctx, r, buf)
		require.NoError(t, err)

		// Metadata should return true size of the blob, regardless of queried size.
		digestWrongSize := &repb.Digest{Hash: d.GetHash(), SizeBytes: 1}
		rn := &resource.ResourceName{
			Digest:       digestWrongSize,
			CacheType:    resource.CacheType_AC,
			InstanceName: "remoteInstanceName",
		}

		md, err := dc.Metadata(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, testSize, md.StoredSizeBytes)
		lastAccessTime1 := md.LastAccessTimeUsec
		lastModifyTime1 := md.LastModifyTimeUsec
		require.NotZero(t, lastAccessTime1)
		require.NotZero(t, lastModifyTime1)

		// Last access time should not update since last call to Metadata()
		md, err = dc.Metadata(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, testSize, md.StoredSizeBytes)
		lastAccessTime2 := md.LastAccessTimeUsec
		lastModifyTime2 := md.LastModifyTimeUsec
		require.Equal(t, lastAccessTime1, lastAccessTime2)
		require.Equal(t, lastModifyTime1, lastModifyTime2)

		// After updating data, last access and modify time should update
		time.Sleep(1 * time.Second) // Sleep to guarantee timestamps change
		err = dc.Set(ctx, rn, buf)
		md, err = dc.Metadata(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, testSize, md.StoredSizeBytes)
		lastAccessTime3 := md.LastAccessTimeUsec
		lastModifyTime3 := md.LastModifyTimeUsec
		require.Greater(t, lastAccessTime3, lastAccessTime1)
		require.Greater(t, lastModifyTime3, lastModifyTime1)
	}
}

func TestMetadataFileDoesNotExist(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)

	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}

	testSize := int64(100)
	d, _ := testdigest.NewRandomDigestBuf(t, testSize)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

	md, err := dc.Metadata(ctx, r)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, md)
}

func randomDigests(t *testing.T, sizes ...int64) map[*resource.ResourceName][]byte {
	m := make(map[*resource.ResourceName][]byte)
	for _, size := range sizes {
		d, buf := testdigest.NewRandomDigestBuf(t, size)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		m[rn] = buf
	}
	return m
}

func TestFindMissing(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(defaultExt4BlockSize * 1)
	rootDir := testfs.MakeTempDir(t)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	require.NoError(t, err)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	notSetD1, _ := testdigest.NewRandomDigestBuf(t, 100)
	notSetD2, _ := testdigest.NewRandomDigestBuf(t, 100)

	remoteInstanceName := "farFarAway"
	r := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_AC,
		InstanceName: remoteInstanceName,
	}
	err = dc.Set(ctx, r, buf)
	require.NoError(t, err)

	digests := []*repb.Digest{d, notSetD1, notSetD2}
	rns := digest.ResourceNames(resource.CacheType_AC, remoteInstanceName, digests)
	missing, err := dc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetD1, notSetD2}, missing)

	digests = []*repb.Digest{d}
	rns = digest.ResourceNames(resource.CacheType_AC, remoteInstanceName, digests)
	missing, err = dc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestMultiGetSet(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)
	digests := randomDigests(t, 10, 20, 11, 30, 40)
	if err := dc.SetMulti(ctx, digests); err != nil {
		t.Fatalf("Error multi-setting digests: %s", err.Error())
	}
	resourceNames := make([]*resource.ResourceName, 0, len(digests))
	for d := range digests {
		resourceNames = append(resourceNames, d)
	}
	m, err := dc.GetMulti(ctx, resourceNames)
	if err != nil {
		t.Fatalf("Error multi-getting digests: %s", err.Error())
	}
	for r := range digests {
		d := r.GetDigest()
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
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		ctx := getAnonContext(t, te)
		d, r := testdigest.NewRandomDigestReader(t, testSize)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		// Use Writer() to set the bytes in the cache.
		wc, err := dc.Writer(ctx, rn)
		if err != nil {
			t.Fatalf("Error getting %q writer: %s", d.GetHash(), err.Error())
		}
		if _, err := io.Copy(wc, r); err != nil {
			t.Fatalf("Error copying bytes to cache: %s", err.Error())
		}
		if err := wc.Commit(); err != nil {
			t.Fatalf("Error committing writer: %s", err.Error())
		}
		if err := wc.Close(); err != nil {
			t.Fatalf("Error closing writer: %s", err.Error())
		}
		// Use Reader() to get the bytes from the cache.
		reader, err := dc.Reader(ctx, rn, 0, 0)
		if err != nil {
			t.Fatalf("Error getting %q reader: %s", d.GetHash(), err.Error())
		}
		d2 := testdigest.ReadDigestAndClose(t, reader)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestReadOffset(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)
	d, r := testdigest.NewRandomDigestReader(t, 100)
	rn := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	// Use Writer() to set the bytes in the cache.
	wc, err := dc.Writer(ctx, rn)
	if err != nil {
		t.Fatalf("Error getting %q writer: %s", d.GetHash(), err.Error())
	}
	if _, err := io.Copy(wc, r); err != nil {
		t.Fatalf("Error copying bytes to cache: %s", err.Error())
	}
	if err := wc.Commit(); err != nil {
		t.Fatalf("Error committing writer: %s", err.Error())
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Error closing writer: %s", err.Error())
	}
	// Use Reader() to get the bytes from the cache.
	reader, err := dc.Reader(ctx, rn, d.GetSizeBytes(), 0)
	if err != nil {
		t.Fatalf("Error getting %q reader: %s", d.GetHash(), err.Error())
	}
	d2 := testdigest.ReadDigestAndClose(t, reader)
	if "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" != d2.GetHash() {
		t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
	}

}

func TestReadOffsetLimit(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, defaultExt4BlockSize)
	require.NoError(t, err)

	ctx := getAnonContext(t, te)
	size := int64(10)
	d, buf := testdigest.NewRandomDigestBuf(t, size)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = dc.Set(ctx, r, buf)
	require.NoError(t, err)

	offset := int64(2)
	limit := int64(3)
	reader, err := dc.Reader(ctx, r, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, size)
	n, err := reader.Read(readBuf)
	require.EqualValues(t, limit, n)
	require.Equal(t, buf[offset:offset+limit], readBuf[:limit])
}

func TestSizeLimit(t *testing.T) {
	// Enough space for 2 small digests.
	maxSizeBytes := int64(defaultExt4BlockSize * 2)
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	dc.WaitUntilMapped()
	ctx := getAnonContext(t, te)
	digestBufs := randomDigests(t, 400, 400, 400)
	digestKeys := make([]*resource.ResourceName, 0, len(digestBufs))
	for d, buf := range digestBufs {
		if err := dc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetDigest().GetHash(), err.Error())
		}
		digestKeys = append(digestKeys, d)
	}
	for i, r := range digestKeys {
		d := r.GetDigest()
		rbuf, err := dc.Get(ctx, r)

		// The first digest should have been evicted.
		if i == 0 {
			if err == nil {
				t.Fatalf("%q should have been evicted from cache", d.GetHash())
			}
			continue
		}

		// Expect the last *2* digests to be present.
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
	// Enough room for two small digests.
	maxSizeBytes := int64(defaultExt4BlockSize * 2)
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	dc.WaitUntilMapped()
	ctx := getAnonContext(t, te)
	digestBufs := randomDigests(t, 400, 400)
	digestKeys := make([]*resource.ResourceName, 0, len(digestBufs))
	for r, buf := range digestBufs {
		if err := dc.Set(ctx, r, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", r.GetDigest().GetHash(), err.Error())
		}
		digestKeys = append(digestKeys, r)
	}
	// Now "use" the first digest written so it is most recently used.
	ok, err := dc.Contains(ctx, digestKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Key %q was not present in cache, it should have been.", digestKeys[0].GetDigest().GetHash())
	}
	// Now write one more digest, which should evict the oldest digest,
	// (the second one we wrote).
	d, buf := testdigest.NewRandomDigestBuf(t, 400)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	if err := dc.Set(ctx, r, buf); err != nil {
		t.Fatal(err)
	}
	digestKeys = append(digestKeys, r)

	for i, r := range digestKeys {
		d := r.GetDigest()
		rbuf, err := dc.Get(ctx, r)

		// The second digest should have been evicted.
		if i == 1 {
			if err == nil {
				t.Fatalf("%q should have been evicted from cache", d.GetHash())
			}
			continue
		}

		// Expect the first and third digests to be present.
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
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t, te)

	lock := sync.RWMutex{}
	eg, gctx := errgroup.WithContext(ctx)
	d, buf := testdigest.NewRandomDigestBuf(t, 100000)
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			lock.Lock()
			defer lock.Unlock()
			r := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			if err := dc.Set(gctx, r, buf); err != nil {
				return err
			}
			_, err = dc.Get(ctx, r)
			if err != nil {
				return err
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
	rootDir := testfs.MakeTempDir(t)
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
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure that files on disk exist *immediately*, even though
	// they may not have been async processed yet.
	for _, d := range digests {
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		exists, err := dc.Contains(ctx, r)
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
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		if err := dc.Set(ctx, r, buf); err != nil {
			t.Fatal(err)
		}
		digests = append(digests, d)
	}

	dc.WaitUntilMapped()

	// Check that everything still exists.
	for _, d := range digests {
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		exists, err := dc.Contains(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("%q was not found in cache.", d.GetHash())
		}
	}
}

func waitForEviction(t *testing.T, dc *disk_cache.DiskCache) {
	start := time.Now()
	for time.Since(start) < 1*time.Second {
		if dc.WithinTargetSize() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.FailNow(t, "eviction did not happen in time")
}

func expectedDefaultV1DiskPath(rootDir string, r *resource.ResourceName) string {
	return filepath.Join(rootDir, interfaces.AuthAnonymousUser, r.GetDigest().GetHash())
}

func testEviction(t *testing.T, rootDir string) {
	maxSizeBytes := int64(5_000_000) // 10MB
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir, ForceV1Layout: true}, maxSizeBytes)
	require.NoError(t, err)

	// Fill the cache.
	resources := make([]*resource.ResourceName, 0)
	for i := 0; i < 999; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		err := dc.Set(ctx, r, buf)
		require.NoError(t, err)
		resources = append(resources, r)
	}

	waitForEviction(t, dc)

	for i, r := range resources {
		if i > 500 {
			break
		}
		contains, err := dc.Contains(ctx, r)
		require.NoError(t, err)
		if contains {
			t.Fatalf("Expected oldest digest %+v to be deleted", r)
		}
		fp := expectedDefaultV1DiskPath(rootDir, r)
		if _, err := os.Stat(fp); err == nil || !os.IsNotExist(err) {
			require.FailNow(t, "file should not exist", "%q should not exist", fp)
		}
	}

	// Simulate a "restart" of the cache by creating a new instance.

	dc, err = disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	require.NoError(t, err)
	dc.WaitUntilMapped()

	// Write more data to push out what's left of the original digests.

	for i := 0; i < 500; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		r := &resource.ResourceName{Digest: d, CacheType: resource.CacheType_CAS}
		err := dc.Set(ctx, r, buf)
		require.NoError(t, err)
	}

	for _, r := range resources {
		contains, err := dc.Contains(ctx, r)
		require.NoError(t, err)
		if contains {
			t.Fatalf("Expected oldest digest %+v to be deleted", r)
		}
		fp := expectedDefaultV1DiskPath(rootDir, r)
		if _, err := os.Stat(fp); err == nil || !os.IsNotExist(err) {
			require.FailNow(t, "file should not exist", "%q should not exist", fp)
		}
	}
}

func TestEviction_V1Layout(t *testing.T) {
	rootDir := filepath.Clean(testfs.MakeTempDir(t))
	testEviction(t, rootDir)
}

func TestEviction_V1Layout_RootDirEndsInSlash(t *testing.T) {
	rootDir := filepath.Clean(testfs.MakeTempDir(t))
	testEviction(t, rootDir+"/")
}

func newCacheAndContext(t *testing.T, opts *disk_cache.Options, maxSizeBytes int64) (*disk_cache.DiskCache, context.Context) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	optsCopy := *opts
	if optsCopy.RootDirectory == "" {
		rootDir := testfs.MakeTempDir(t)
		optsCopy.RootDirectory = rootDir
	}
	dc, err := disk_cache.NewDiskCache(te, &optsCopy, maxSizeBytes)
	require.NoError(t, err)
	return dc, ctx
}

func TestJanitorThread(t *testing.T) {
	maxSizeBytes := int64(10_000_000) // 10MB
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	rootDir := testfs.MakeTempDir(t)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Fill the cache.
	digests := make([]*resource.ResourceName, 0)
	for i := 0; i < 999; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		err := dc.Set(ctx, r, buf)
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, r)
	}

	// Make a new disk cache with a smaller size. The
	// janitor should clean extra data up, oldest first.
	dc, err = disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes/2) // 5MB
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
			t.Fatalf("Expected oldest digest %+v to be deleted", d.GetDigest())
		}
	}
}

func TestDefaultToV2Layout(t *testing.T) {
	// Setup a v1 cache and verify it doesn't get switched to v2.
	{
		rootDir := testfs.MakeTempDir(t)

		c, ctx := newCacheAndContext(t, &disk_cache.Options{ForceV1Layout: true, RootDirectory: rootDir}, 10_000_000)
		d, data := testdigest.NewRandomDigestBuf(t, 1000)
		err := c.Set(ctx, &resource.ResourceName{Digest: d, CacheType: resource.CacheType_CAS}, data)
		require.NoError(t, err)

		c, ctx = newCacheAndContext(t, &disk_cache.Options{RootDirectory: rootDir}, 10_000_000)
		require.False(t, c.IsV2Layout(), "cache should have remained on v1 layout")
	}

	// New cache on an empty directory should default to v2.
	{
		c, _ := newCacheAndContext(t, &disk_cache.Options{}, 10_000_000)
		require.True(t, c.IsV2Layout(), "cache should have been auto-updated to v2 layout")
	}
}

func TestZeroLengthFiles(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)
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
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure that the goodDigest exists and the zero length one does not.
	exists, err := dc.Contains(ctx, digest.NewCASResourceName(badDigest, "").ToProto())
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("%q (empty file) should not be mapped in cache.", badDigest.GetHash())
	}

	exists, err = dc.Contains(ctx, digest.NewCASResourceName(goodDigest, "").ToProto())
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("%q was not found in cache.", goodDigest.GetHash())
	}
}

func TestDeleteStaleTempFiles(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	anonPath := filepath.Join(rootDir, interfaces.AuthAnonymousUser)
	err := disk.EnsureDirectoryExists(anonPath)
	require.NoError(t, err)

	// Create a temp file for a write that should be deleted.
	badDigest, _ := testdigest.NewRandomDigestBuf(t, 1000)
	yesterday := time.Now().AddDate(0, 0, -1)
	writeTempFile := filepath.Join(anonPath, badDigest.GetHash()+".ababababab.tmp")
	err = os.WriteFile(writeTempFile, []byte("hello"), 0644)
	require.NoError(t, err)
	err = os.Chtimes(writeTempFile, yesterday, yesterday)
	require.NoError(t, err)

	// Create an unexpected file that should not be deleted.
	unexpectedFile := filepath.Join(anonPath, "some_other_file.txt")
	err = os.WriteFile(unexpectedFile, []byte("hello"), 0644)
	require.NoError(t, err)
	err = os.Chtimes(unexpectedFile, yesterday, yesterday)
	require.NoError(t, err)

	// Create a temp file that was recently modified and should not be deleted.
	// (Could've been created for a legitimate purpose - for example, we use tmp files when writing data)
	badDigest2, _ := testdigest.NewRandomDigestBuf(t, 1000)
	writeTempFile2 := filepath.Join(anonPath, badDigest2.GetHash()+".ababababab.tmp")
	err = os.WriteFile(writeTempFile2, []byte("hello"), 0644)
	require.NoError(t, err)

	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDir}, maxSizeBytes)
	require.NoError(t, err)

	dc.WaitUntilMapped()

	exists, err := disk.FileExists(ctx, writeTempFile)
	require.NoError(t, err)
	require.False(t, exists, "temp file %q should have been deleted", writeTempFile)

	exists, err = disk.FileExists(ctx, unexpectedFile)
	require.NoError(t, err)
	require.True(t, exists, "unexpected file %q should not have been deleted", unexpectedFile)

	exists, err = disk.FileExists(ctx, writeTempFile2)
	require.NoError(t, err)
	require.True(t, exists, "recent temp file %q should not have been deleted", writeTempFile2)
}

func TestNonDefaultPartition(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)

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
	diskConfig := &disk_cache.Options{
		ForceV1Layout: true,
		RootDirectory: rootDir,
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: 10_000_000,
			},
			{
				ID:           otherPartitionID,
				MaxSizeBytes: 10_000_000,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
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
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}

		err = dc.Set(ctx, r, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, interfaces.AuthAnonymousUser)
		dPath := filepath.Join(userRoot, d.GetHash())
		require.FileExists(t, dPath)

		c, err := dc.Contains(ctx, r)
		require.NoError(t, err)
		require.True(t, c)
	}

	// Authenticated user on default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey1)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}

		err = dc.Set(ctx, r, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, testGroup1)
		dPath := filepath.Join(userRoot, d.GetHash())
		require.FileExists(t, dPath)

		c, err := dc.Contains(ctx, r)
		require.NoError(t, err)
		require.True(t, c)
	}

	// Authenticated user with group ID that matches custom partition, but without a matching instance name prefix.
	// Data should go to the default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey2)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		instanceName := "nonmatchingprefix"
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: instanceName,
		}

		err = dc.Set(ctx, r, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, testGroup2)
		dPath := filepath.Join(userRoot, instanceName, d.GetHash())
		require.FileExists(t, dPath)

		contains, err := dc.Contains(ctx, r)
		require.NoError(t, err)
		require.True(t, contains)
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
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: instanceName,
		}

		err = dc.Set(ctx, r, buf)
		require.NoError(t, err)

		userRoot := filepath.Join(rootDir, disk_cache.PartitionDirectoryPrefix+otherPartitionID, testGroup2)
		dPath := filepath.Join(userRoot, instanceName, d.GetHash())
		require.FileExists(t, dPath)

		contains, err := dc.Contains(ctx, r)
		require.NoError(t, err)
		require.True(t, contains)
	}
}

func TestV2Layout(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)
	te := getTestEnv(t, emptyUserMap)

	diskConfig := &disk_cache.Options{
		RootDirectory: rootDir,
		UseV2Layout:   true,
	}
	dc, err := disk_cache.NewDiskCache(te, diskConfig, maxSizeBytes)
	require.NoError(t, err)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

	err = dc.Set(ctx, r, buf)
	require.NoError(t, err)

	userRoot := filepath.Join(rootDir, disk_cache.V2Dir, disk_cache.PartitionDirectoryPrefix+disk_cache.DefaultPartitionID, interfaces.AuthAnonymousUser)
	dPath := filepath.Join(userRoot, d.GetHash()[0:disk_cache.HashPrefixDirPrefixLen], d.GetHash())
	require.FileExists(t, dPath)

	ok, err := dc.Contains(ctx, r)
	require.NoError(t, err)
	require.Truef(t, ok, "digest should be in the cache")

	_, err = dc.Get(ctx, &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	})
	require.NoError(t, err)
}

func TestV2LayoutMigration(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te := getTestEnv(t, testUsers)

	type test struct {
		oldPath string
		newPath string
		data    string
	}

	tests := []test{
		{
			oldPath: "ANON/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			newPath: "v2/PTdefault/ANON/7e09/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			data:    "test1",
		},
		{
			oldPath: "ANON/prefix/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			newPath: "v2/PTdefault/ANON/prefix/7e09/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			data:    "test2",
		},
		{
			oldPath: "PTFOO/GR7890/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			newPath: "v2/PTFOO/GR7890/7e09/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			data:    "test3",
		},
		{
			oldPath: "PTFOO/GR7890/prefix/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			newPath: "v2/PTFOO/GR7890/prefix/7e09/7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620",
			data:    "test4",
		},
	}

	expectedContents := make(map[string]string)
	for _, test := range tests {
		p := filepath.Join(rootDir, test.oldPath)
		err := os.MkdirAll(filepath.Dir(p), 0755)
		require.NoError(t, err)
		err = os.WriteFile(p, []byte(test.data), 0644)
		require.NoError(t, err)
		expectedContents[test.newPath] = test.data
	}
	err := disk_cache.MigrateToV2Layout(rootDir)
	require.NoError(t, err)
	testfs.AssertExactFileContents(t, rootDir, expectedContents)

	// Now create a cache on top of the migrated files and verify it works as expected.
	diskConfig := &disk_cache.Options{
		RootDirectory: rootDir,
		UseV2Layout:   true,
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: 10_000_000,
			},
			{
				ID:           "FOO",
				MaxSizeBytes: 10_000_000,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: "FOO",
			},
		},
	}
	dc, err := disk_cache.NewDiskCache(te, diskConfig, maxSizeBytes)
	require.NoError(t, err)
	dc.WaitUntilMapped()

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	testHash := "7e09daa1b85225442942ff853d45618323c56b85a553c5188cec2fd1009cd620"
	{
		d := &repb.Digest{Hash: testHash, SizeBytes: 5}
		buf, err := dc.Get(ctx, &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		})
		require.NoError(t, err)
		require.Equal(t, []byte("test1"), buf)
	}

	{
		d := &repb.Digest{Hash: testHash, SizeBytes: 5}
		rn := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: "prefix",
		}
		ok, err := dc.Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, ok, "digest should be in the cache")

		buf, err := dc.Get(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, []byte("test2"), buf)
	}

	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)

		d := &repb.Digest{Hash: testHash, SizeBytes: 5}
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		ok, err := dc.Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, ok, "digest should be in the cache")

		buf, err := dc.Get(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, []byte("test3"), buf)
	}

	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		require.NoError(t, err)

		d := &repb.Digest{Hash: testHash, SizeBytes: 5}
		rn := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: "prefix",
		}
		ok, err := dc.Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, ok, "digest should be in the cache")

		buf, err := dc.Get(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, []byte("test4"), buf)
	}

	// Run the migration again, nothing should happen.
	err = disk_cache.MigrateToV2Layout(rootDir)
	require.NoError(t, err)
	testfs.AssertExactFileContents(t, rootDir, expectedContents)
}

func TestScanDiskDirectoryV1(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te := getTestEnv(t, testUsers)

	diskConfig := &disk_cache.Options{
		RootDirectory: rootDir,
		UseV2Layout:   false,
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: 10_000_000,
			},
			{
				ID:           "FOO",
				MaxSizeBytes: 10_000_000,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: "FOO",
			},
		},
	}
	dc, err := disk_cache.NewDiskCache(te, diskConfig, maxSizeBytes)
	require.NoError(t, err)

	tests := []struct {
		remoteInstanceName string
		cacheType          resource.CacheType
		apiKey             string
		expectedPartition  string
	}{
		{
			remoteInstanceName: "",
			cacheType:          resource.CacheType_CAS,
			apiKey:             "",
			expectedPartition:  "default",
		},
		{
			remoteInstanceName: "prefix",
			cacheType:          resource.CacheType_AC,
			apiKey:             "",
			expectedPartition:  "default",
		},
		{
			remoteInstanceName: "prefix",
			cacheType:          resource.CacheType_CAS,
			apiKey:             testAPIKey,
			expectedPartition:  "FOO",
		},
		{
			remoteInstanceName: "prefix",
			cacheType:          resource.CacheType_AC,
			apiKey:             testAPIKey,
			expectedPartition:  "FOO",
		},
	}

	expectedFileRecords := make([]*rfpb.FileRecord, 0)

	for _, test := range tests {
		var ctx context.Context
		var err error
		if test.apiKey != "" {
			ctx = te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
			ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		} else {
			ctx, err = prefix.AttachUserPrefixToContext(context.Background(), te)
		}
		if err != nil {
			t.Fatalf("error attaching context: %s", err)
		}

		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    test.cacheType,
			InstanceName: test.remoteInstanceName,
		}
		err = dc.Set(ctx, r, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		isolation := &rfpb.Isolation{}
		isolation.CacheType = test.cacheType
		isolation.RemoteInstanceName = test.remoteInstanceName
		isolation.PartitionId = test.expectedPartition
		fr := &rfpb.FileRecord{
			Digest:    d,
			Isolation: isolation,
		}
		expectedFileRecords = append(expectedFileRecords, fr)
	}

	fmChan := disk_cache.ScanDiskDirectory(rootDir)
	for fm := range fmChan {
		found := false
		for _, fr := range expectedFileRecords {
			if proto.Equal(fr, fm.GetFileRecord()) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("FileMetadata: %+v was not in expected: %+v", fm, expectedFileRecords)
		}
	}
}

func TestScanDiskDirectoryV2(t *testing.T) {
	maxSizeBytes := int64(100_000_000) // 100MB
	rootDir := testfs.MakeTempDir(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te := getTestEnv(t, testUsers)

	diskConfig := &disk_cache.Options{
		RootDirectory: rootDir,
		UseV2Layout:   true,
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: 10_000_000,
			},
			{
				ID:           "FOO",
				MaxSizeBytes: 10_000_000,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: "FOO",
			},
		},
	}
	dc, err := disk_cache.NewDiskCache(te, diskConfig, maxSizeBytes)
	require.NoError(t, err)

	tests := []struct {
		remoteInstanceName string
		cacheType          resource.CacheType
		apiKey             string
		expectedPartition  string
	}{
		{
			remoteInstanceName: "",
			cacheType:          resource.CacheType_CAS,
			apiKey:             "",
			expectedPartition:  "default",
		},
		{
			remoteInstanceName: "prefix",
			cacheType:          resource.CacheType_AC,
			apiKey:             "",
			expectedPartition:  "default",
		},
		{
			remoteInstanceName: "prefix",
			cacheType:          resource.CacheType_CAS,
			apiKey:             testAPIKey,
			expectedPartition:  "FOO",
		},
		{
			remoteInstanceName: "prefix",
			cacheType:          resource.CacheType_AC,
			apiKey:             testAPIKey,
			expectedPartition:  "FOO",
		},
	}

	expectedFileRecords := make([]*rfpb.FileRecord, 0)

	for _, test := range tests {
		var ctx context.Context
		var err error
		if test.apiKey != "" {
			ctx = te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
			ctx, err = prefix.AttachUserPrefixToContext(ctx, te)
		} else {
			ctx, err = prefix.AttachUserPrefixToContext(context.Background(), te)
		}
		if err != nil {
			t.Fatalf("error attaching context: %s", err)
		}

		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    test.cacheType,
			InstanceName: test.remoteInstanceName,
		}
		err = dc.Set(ctx, r, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		isolation := &rfpb.Isolation{}
		isolation.CacheType = test.cacheType
		isolation.RemoteInstanceName = test.remoteInstanceName
		isolation.PartitionId = test.expectedPartition
		fr := &rfpb.FileRecord{
			Digest:    d,
			Isolation: isolation,
		}
		expectedFileRecords = append(expectedFileRecords, fr)
	}

	fmChan := disk_cache.ScanDiskDirectory(rootDir)
	for fm := range fmChan {
		found := false
		for _, fr := range expectedFileRecords {
			if proto.Equal(fr, fm.GetFileRecord()) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("FileMetadata: %+v was not in expected: %+v", fm, expectedFileRecords)
		}
	}
}
