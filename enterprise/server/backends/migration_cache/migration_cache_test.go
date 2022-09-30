package migration_cache_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/migration_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func getAnonContext(t *testing.T, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err, "error ataching user prefix")
	return ctx
}

// waitForCopy keeps checking the destination cache to see whether the background process has
// copied the given digest over
func waitForCopy(t *testing.T, ctx context.Context, destCache interfaces.Cache, digest *repb.Digest) {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
		contains, err := destCache.ContainsDeprecated(ctx, digest)
		require.NoError(t, err, "error calling contains on dest cache")

		if contains {
			return
		}

		// Data has not been copied yet... Keep waiting
		time.Sleep(delay)
	}

	require.FailNowf(t, "timeout", "Timed out waiting for data to be copied to dest cache")
}

// errorCache lets us mock errors to test error handling
type errorCache struct {
	interfaces.Cache
}

func (c *errorCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	return errors.New("error cache set err")
}

func (c *errorCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	return nil, errors.New("error cache get err")
}

func (c *errorCache) Delete(ctx context.Context, d *repb.Digest) error {
	return errors.New("error cache delete err")
}

func (c *errorCache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	return false, errors.New("error cache contains err")
}

func (c *errorCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	return nil, errors.New("error cache metadata err")
}

func (c *errorCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	return nil, errors.New("error cache findmissing err")
}

func (c *errorCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	return nil, errors.New("error cache writer err")
}

func (c *errorCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	return nil, errors.New("error cache reader err")
}

func TestACIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)

	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	c1, err := mc.WithIsolation(ctx, interfaces.ActionCacheType, "")
	require.NoError(t, err)

	d1, buf1 := testdigest.NewRandomDigestBuf(t, 100)
	require.NoError(t, c1.Set(ctx, d1, buf1))

	got1, err := c1.Get(ctx, d1)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	// Data should not be in CAS cache
	gotCAS, err := mc.Get(ctx, d1)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, gotCAS)
}

func TestACIsolation_RemoteInstanceName(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	c1, err := mc.WithIsolation(ctx, interfaces.ActionCacheType, "remote")
	require.NoError(t, err)

	d1, buf1 := testdigest.NewRandomDigestBuf(t, 100)
	require.NoError(t, c1.Set(ctx, d1, buf1))

	got1, err := c1.Get(ctx, d1)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	// Data should not be in CAS cache
	gotCAS, err := mc.Get(ctx, d1)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, gotCAS)
}

func TestSet_DoubleWrite(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Verify data was written to both caches
	srcData, err := srcCache.Get(ctx, d)
	require.NoError(t, err)
	require.NotNil(t, srcData)

	destData, err := destCache.Get(ctx, d)
	require.NoError(t, err)
	require.NotNil(t, destData)

	require.True(t, bytes.Equal(srcData, destData))
}

func TestSet_DestWriteErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, int64(1000))
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Verify data was successfully written to src cache
	srcData, err := srcCache.Get(ctx, d)
	require.NoError(t, err)
	require.Equal(t, buf, srcData)
}

func TestSet_SrcWriteErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache := &errorCache{}
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, int64(1000))
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.Error(t, err)

	// Verify data was deleted from the dest cache
	destData, err := destCache.Get(ctx, d)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, destData)
}

func TestSet_SrcAndDestWriteErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)

	srcCache := &errorCache{}
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err := mc.Set(ctx, d, buf)
	require.Error(t, err)
}

func TestGetSet(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		err = mc.Set(ctx, d, buf)
		require.NoError(t, err, "error setting digest in cache")

		// Get() the bytes from the cache.
		rbuf, err := mc.Get(ctx, d)
		require.NoError(t, err, "error getting from cache")

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		require.True(t, d.GetHash() == d2.GetHash())
	}
}

func TestGet_DoubleRead(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	data, err := mc.Get(ctx, d)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))
}

func TestGet_DestReadErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	data, err := mc.Get(ctx, d)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))
}

func TestGet_SrcReadErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache := &errorCache{}
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}, srcCache, destCache)

	// Write data to dest cache only
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = destCache.Set(ctx, d, buf)
	require.NoError(t, err)

	// Should return error
	data, err := mc.Get(ctx, d)
	require.Error(t, err)
	require.Nil(t, data)
}

func TestGetSet_EmptyData(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}, srcCache, destCache)

	d, _ := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, []byte{})
	require.NoError(t, err)

	data, err := mc.Get(ctx, d)
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte{}, data))
}

func TestCopyDataInBackground(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	numTests := 1000
	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize: numTests + 1,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	for i := 0; i < numTests; i++ {
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, d, buf)
			require.NoError(t, err)

			// Get should queue copy in background
			data, err := mc.Get(ctx, d)
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))

			// Expect copy
			waitForCopy(t, ctx, destCache, d)
			return nil
		})
	}
	eg.Wait()
}

func TestCopyDataInBackground_ExceedsCopyChannelSize(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize:             1,
		CopyChanFullWarningIntervalMin: 1,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, d, buf)
			require.NoError(t, err)

			// We should exceed the copy channel size, but should not prevent us from continuing
			// to read from the cache
			data, err := mc.Get(ctx, d)
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))
			return nil
		})
	}
	eg.Wait()
}

func TestCopyDataInBackground_RateLimit(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    1,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	start := time.Now()

	for i := 0; i < 2; i++ {
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, d, buf)
			require.NoError(t, err)

			// Get should queue copy in background
			data, err := mc.Get(ctx, d)
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))

			// Expect copy
			waitForCopy(t, ctx, destCache, d)
			return nil
		})
	}
	eg.Wait()

	// Should wait at least 1 second before the second digest is copied
	require.True(t, time.Since(start) >= 1*time.Second)
}

func TestContains(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	contains, err := mc.ContainsDeprecated(ctx, d)
	require.NoError(t, err)
	require.True(t, contains)

	notWrittenDigest, _ := testdigest.NewRandomDigestBuf(t, 100)
	contains, err = mc.ContainsDeprecated(ctx, notWrittenDigest)
	require.NoError(t, err)
	require.False(t, contains)
}

func TestContains_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	contains, err := mc.ContainsDeprecated(ctx, d)
	require.NoError(t, err)
	require.True(t, contains)
}

func TestMetadata(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	md, err := mc.Metadata(ctx, d)
	require.NoError(t, err)
	require.Equal(t, int64(100), md.SizeBytes)

	notWrittenDigest, _ := testdigest.NewRandomDigestBuf(t, 100)
	md, err = mc.Metadata(ctx, notWrittenDigest)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, md)
}

func TestMetadata_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	md, err := mc.Metadata(ctx, d)
	require.NoError(t, err)
	require.Equal(t, int64(100), md.SizeBytes)
}

func TestFindMissing(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	notSetD1, _ := testdigest.NewRandomDigestBuf(t, 100)
	notSetD2, _ := testdigest.NewRandomDigestBuf(t, 100)

	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	missing, err := mc.FindMissing(ctx, []*repb.Digest{d, notSetD1, notSetD2})
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetD1, notSetD2}, missing)

	missing, err = mc.FindMissing(ctx, []*repb.Digest{d})
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestFindMissing_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	notSetD1, _ := testdigest.NewRandomDigestBuf(t, 100)
	notSetD2, _ := testdigest.NewRandomDigestBuf(t, 100)

	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	missing, err := mc.FindMissing(ctx, []*repb.Digest{d, notSetD1, notSetD2})
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetD1, notSetD2}, missing)
}

func TestGetMultiWithCopying(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(10000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	config := &migration_cache.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	digests := make([]*repb.Digest, 50)
	expected := make(map[*repb.Digest][]byte, 50)
	for i := 0; i < 50; i++ {
		idx := i
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			lock.Lock()
			defer lock.Unlock()
			err = srcCache.Set(ctx, d, buf)
			require.NoError(t, err)

			digests[idx] = d
			expected[d] = buf
			return nil
		})
	}
	eg.Wait()

	r, err := mc.GetMulti(ctx, digests)
	require.NoError(t, err)
	require.Equal(t, expected, r)

	for _, digest := range digests {
		waitForCopy(t, ctx, destCache, digest)
	}
}

func TestSetMulti(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(10000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	config := &migration_cache.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	dataToSet := make(map[*repb.Digest][]byte, 50)
	for i := 0; i < 50; i++ {
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			lock.Lock()
			defer lock.Unlock()
			dataToSet[d] = buf
			return nil
		})
	}
	eg.Wait()

	err = mc.SetMulti(ctx, dataToSet)
	require.NoError(t, err)

	for d, expected := range dataToSet {
		data, err := mc.Get(ctx, d)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, data))

		data, err = srcCache.Get(ctx, d)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, data))

		data, err = destCache.Get(ctx, d)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, data))
	}

}

func TestDelete(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	// Check data exists before delete
	data, err := mc.Get(ctx, d)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	data, err = srcCache.Get(ctx, d)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	data, err = destCache.Get(ctx, d)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	// After delete, data should no longer exist
	err = mc.Delete(ctx, d)
	require.NoError(t, err)

	data, err = mc.Get(ctx, d)
	require.True(t, status.IsNotFoundError(err))

	data, err = srcCache.Get(ctx, d)
	require.True(t, status.IsNotFoundError(err))

	data, err = destCache.Get(ctx, d)
	require.True(t, status.IsNotFoundError(err))
}

func TestReadWrite(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	require.NoError(t, err)
	destCache.Start()
	config := &migration_cache.MigrationConfig{
		DoubleReadPercentage: 1.0,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		w, err := mc.Writer(ctx, d)
		require.NoError(t, err)

		_, err = w.Write(buf)
		require.NoError(t, err)

		err = w.Close()
		require.NoError(t, err)

		reader, err := mc.Reader(ctx, d, 0, 0)
		require.NoError(t, err)

		actualBuf := make([]byte, len(buf))
		n, err := reader.Read(actualBuf)
		require.NoError(t, err)
		require.Equal(t, int(testSize), n)
		require.True(t, bytes.Equal(buf, actualBuf))

		err = reader.Close()
		require.NoError(t, err)

		// Verify data was written to both caches
		srcReader, err := srcCache.Reader(ctx, d, 0, 0)
		require.NoError(t, err)

		actualBuf = make([]byte, len(buf))
		n, err = srcReader.Read(actualBuf)
		require.NoError(t, err)
		require.Equal(t, int(testSize), n)
		require.True(t, bytes.Equal(buf, actualBuf))

		err = srcReader.Close()
		require.NoError(t, err)

		destReader, err := destCache.Reader(ctx, d, 0, 0)
		require.NoError(t, err)

		actualBuf = make([]byte, len(buf))
		n, err = destReader.Read(actualBuf)
		require.NoError(t, err)
		require.Equal(t, int(testSize), n)
		require.True(t, bytes.Equal(buf, actualBuf))

		err = destReader.Close()
		require.NoError(t, err)
	}
}

func TestReaderWriter_DestFails(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	config := &migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	// Will fail to set a dest writer
	w, err := mc.Writer(ctx, d)
	require.NoError(t, err)

	// Should still write data to src cache without error
	_, err = w.Write(buf)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	// Will fail to set a dest reader
	reader, err := mc.Reader(ctx, d, 0, 0)
	require.NoError(t, err)

	// Should still read from src cache without error
	actualBuf := make([]byte, len(buf))
	n, err := reader.Read(actualBuf)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.True(t, bytes.Equal(buf, actualBuf))
}
