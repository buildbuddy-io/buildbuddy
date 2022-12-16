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
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
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

const (
	// All files on disk will be a multiple of this block size, assuming a
	// filesystem with default settings.
	defaultExt4BlockSize = 4096
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
func waitForCopy(t *testing.T, ctx context.Context, destCache interfaces.Cache, r *resource.ResourceName) {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
		contains, err := destCache.Contains(ctx, r)
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

func (c *errorCache) Set(ctx context.Context, r *resource.ResourceName, data []byte) error {
	return errors.New("error cache set err")
}

func (c *errorCache) Get(ctx context.Context, r *resource.ResourceName) ([]byte, error) {
	return nil, errors.New("error cache get err")
}

func (c *errorCache) DeleteDeprecated(ctx context.Context, d *repb.Digest) error {
	return errors.New("error cache delete err")
}

func (c *errorCache) Contains(ctx context.Context, r *resource.ResourceName) (bool, error) {
	return false, errors.New("error cache contains err")
}

func (c *errorCache) Metadata(ctx context.Context, r *resource.ResourceName) (*interfaces.CacheMetadata, error) {
	return nil, errors.New("error cache metadata err")
}

func (c *errorCache) Writer(ctx context.Context, r *resource.ResourceName) (interfaces.CommittedWriteCloser, error) {
	return nil, errors.New("error cache writer err")
}

func (c *errorCache) Reader(ctx context.Context, r *resource.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	return nil, errors.New("error cache reader err")
}

func TestACIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 1)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)

	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d1, buf1 := testdigest.NewRandomDigestBuf(t, 100)
	r1 := &resource.ResourceName{
		Digest:    d1,
		CacheType: resource.CacheType_AC,
	}
	require.NoError(t, mc.Set(ctx, r1, buf1))

	got1, err := mc.Get(ctx, r1)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	// Data should not be in CAS cache
	gotCAS, err := mc.Get(ctx, &resource.ResourceName{
		Digest:    d1,
		CacheType: resource.CacheType_CAS,
	})
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, gotCAS)
}

func TestACIsolation_RemoteInstanceName(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 1)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d1, buf1 := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:       d1,
		CacheType:    resource.CacheType_AC,
		InstanceName: "remote",
	}
	require.NoError(t, mc.Set(ctx, r, buf1))

	got1, err := mc.Get(ctx, r)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	// Data should not be in CAS cache
	gotCAS, err := mc.Get(ctx, &resource.ResourceName{
		Digest:       d1,
		CacheType:    resource.CacheType_CAS,
		InstanceName: "remote",
	})
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, gotCAS)
}

func TestSet_DoubleWrite(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Verify data was written to both caches
	srcData, err := srcCache.Get(ctx, r)
	require.NoError(t, err)
	require.NotNil(t, srcData)

	destData, err := destCache.Get(ctx, r)
	require.NoError(t, err)
	require.NotNil(t, destData)

	require.True(t, bytes.Equal(srcData, destData))
}

func TestSet_DestWriteErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, int64(defaultExt4BlockSize*10))
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Verify data was successfully written to src cache
	srcData, err := srcCache.Get(ctx, r)
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
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.Error(t, err)

	// Verify data was deleted from the dest cache
	destData, err := destCache.Get(ctx, r)
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
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err := mc.Set(ctx, r, buf)
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
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		err = mc.Set(ctx, r, buf)
		require.NoError(t, err, "error setting digest in cache")

		// Get() the bytes from the cache.
		rbuf, err := mc.Get(ctx, r)
		require.NoError(t, err)
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		require.True(t, d.GetHash() == d2.GetHash())
	}
}

func TestGet_DoubleRead(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	data, err := mc.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))
}

func TestGet_DestReadErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 1)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}

	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{DoubleReadPercentage: 1.0}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	data, err := mc.Get(ctx, &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	})
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
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = destCache.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return error
	data, err := mc.Get(ctx, &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	})
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
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, []byte{})
	require.NoError(t, err)

	data, err := mc.Get(ctx, &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	})
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
			r := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, r, buf)
			require.NoError(t, err)

			// Get should queue copy in background
			data, err := mc.Get(ctx, r)
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))

			// Expect copy
			waitForCopy(t, ctx, destCache, r)
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
			r := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, r, buf)
			require.NoError(t, err)

			// We should exceed the copy channel size, but should not prevent us from continuing
			// to read from the cache
			data, err := mc.Get(ctx, &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			})
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))
			return nil
		})
	}
	eg.Wait()
}

func TestCopyDataInBackground_RateLimitMax(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
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
			r := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, r, buf)
			require.NoError(t, err)

			// Get should queue copy in background
			data, err := mc.Get(ctx, &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			})
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))

			// Expect copy
			waitForCopy(t, ctx, destCache, r)
			return nil
		})
	}
	eg.Wait()

	// Should wait at least 1 second before the second digest is copied
	require.True(t, time.Since(start) >= 1*time.Second)
}

func TestCopyDataInBackground_RateLimitMin(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    10,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	start := time.Now()

	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			r := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, r, buf)
			require.NoError(t, err)

			// Get should queue copy in background
			data, err := mc.Get(ctx, &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			})
			require.NoError(t, err)
			require.True(t, bytes.Equal(buf, data))

			// Expect copy
			waitForCopy(t, ctx, destCache, r)
			return nil
		})
	}
	eg.Wait()

	// Copies should not be rate limited, and should complete within 1 second
	require.True(t, time.Since(start) <= 1*time.Second)
}

func TestCopyDataInBackground_DrainOnShutdown(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	// Set slow rate of copying
	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    1,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	d2, buf2 := testdigest.NewRandomDigestBuf(t, 100)
	r2 := &resource.ResourceName{
		Digest:    d2,
		CacheType: resource.CacheType_CAS,
	}
	err = srcCache.Set(ctx, r, buf)
	require.NoError(t, err)
	err = srcCache.Set(ctx, r2, buf2)
	require.NoError(t, err)

	// Queue copy on read and then immediately stop the cache
	eg := errgroup.Group{}
	eg.Go(func() error {
		_, err := mc.Get(ctx, r)
		require.NoError(t, err)
		return nil
	})
	eg.Go(func() error {
		_, err := mc.Get(ctx, r2)
		require.NoError(t, err)
		return nil
	})
	eg.Wait()
	err = mc.Stop()
	require.NoError(t, err)

	// Make sure copy queue was drained after shutdown
	waitForCopy(t, ctx, destCache, r)
	waitForCopy(t, ctx, destCache, r2)
}

func TestCopyDataInBackground_AuthenticatedUser(t *testing.T) {
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)

	te := getTestEnv(t, testUsers)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	pebbleOptions := &pebble_cache.Options{
		RootDirectory:     rootDirDest,
		MaxSizeBytes:      maxSizeBytes,
		IsolateByGroupIDs: true,
	}
	destCache, err := pebble_cache.NewPebbleCache(te, pebbleOptions)
	require.NoError(t, err)
	destCache.Start()

	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    10,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	authenticatedCtx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
	authenticatedCtx, err = prefix.AttachUserPrefixToContext(authenticatedCtx, te)
	require.NoError(t, err)

	// Save data to different isolations in src cache
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = srcCache.Set(authenticatedCtx, r, buf)
	require.NoError(t, err)

	d2, buf2 := testdigest.NewRandomDigestBuf(t, 100)
	r2 := &resource.ResourceName{
		Digest:       d2,
		CacheType:    resource.CacheType_AC,
		InstanceName: "dog",
	}
	err = srcCache.Set(authenticatedCtx, r2, buf2)
	require.NoError(t, err)

	//Call get so the digests are copied to the destination cache
	data, err := mc.Get(authenticatedCtx, r2)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf2, data))

	data, err = mc.Get(authenticatedCtx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	// Verify data was copied to correct isolation in destination cache
	eg := errgroup.Group{}
	eg.Go(func() error {
		waitForCopy(t, authenticatedCtx, destCache, r)
		return nil
	})
	eg.Go(func() error {
		waitForCopy(t, authenticatedCtx, destCache, r2)
		return nil
	})
	eg.Wait()
}

func TestCopyDataInBackground_MultipleIsolations(t *testing.T) {
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)

	te := getTestEnv(t, testUsers)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    10,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	// Save data to different isolations in src cache
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_AC,
		InstanceName: "cat",
	}
	err = srcCache.Set(ctx, r, buf)
	require.NoError(t, err)

	d2, buf2 := testdigest.NewRandomDigestBuf(t, 100)
	r2 := &resource.ResourceName{
		Digest:       d2,
		CacheType:    resource.CacheType_CAS,
		InstanceName: "cow",
	}
	err = srcCache.Set(ctx, r2, buf2)
	require.NoError(t, err)

	// Call get so the digests are copied to the destination cache
	data, err := mc.Get(ctx, r2)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf2, data))

	data, err = mc.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	// Verify data was copied to correct isolation in destination cache
	eg := errgroup.Group{}
	eg.Go(func() error {
		waitForCopy(t, ctx, destCache, r)
		return nil
	})
	eg.Go(func() error {
		waitForCopy(t, ctx, destCache, r2)
		return nil
	})
	eg.Wait()
}

func TestCopyDataInBackground_FindMissing(t *testing.T) {
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
		CopyChanBufferSize:   10,
		DoubleReadPercentage: 1.0,
		LogNotFoundErrors:    true,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	// Save digest to both caches - does not need to be copied to dest, but should be there
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r1 := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r1, buf)
	require.NoError(t, err)

	// Save digest to only src cache - should be copied to dest cache
	d2, buf2 := testdigest.NewRandomDigestBuf(t, 100)
	r2 := &resource.ResourceName{
		Digest:    d2,
		CacheType: resource.CacheType_CAS,
	}
	err = srcCache.Set(ctx, r2, buf2)
	require.NoError(t, err)

	// Save digest to only dest cache - should not be copied to src cache
	d3, buf3 := testdigest.NewRandomDigestBuf(t, 100)
	r3 := &resource.ResourceName{
		Digest:    d3,
		CacheType: resource.CacheType_CAS,
	}
	err = destCache.Set(ctx, r3, buf3)
	require.NoError(t, err)

	// FindMissing should queue copies in background
	missing, err := mc.FindMissing(ctx, []*resource.ResourceName{r1, r2, r3})
	require.NoError(t, err)
	require.Equal(t, []*repb.Digest{d3}, missing)

	// Expect data to have been copied
	waitForCopy(t, ctx, destCache, r1)
	waitForCopy(t, ctx, destCache, r2)
}

func TestContains(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 1)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)
	remoteInstanceName := "cloud"

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_AC,
		InstanceName: remoteInstanceName,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	contains, err := mc.Contains(ctx, r)
	require.NoError(t, err)
	require.True(t, contains)

	notWrittenDigest, _ := testdigest.NewRandomDigestBuf(t, 100)
	contains, err = mc.Contains(ctx, &resource.ResourceName{
		Digest:       notWrittenDigest,
		InstanceName: "",
		CacheType:    resource.CacheType_CAS,
	})
	require.NoError(t, err)
	require.False(t, contains)
}

func TestContains_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 1)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	contains, err := mc.Contains(ctx, r)
	require.NoError(t, err)
	require.True(t, contains)
}

func TestMetadata(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	md, err := mc.Metadata(ctx, r)
	require.NoError(t, err)
	require.Equal(t, int64(100), md.StoredSizeBytes)

	notWrittenDigest, _ := testdigest.NewRandomDigestBuf(t, 100)
	md, err = mc.Metadata(ctx, &resource.ResourceName{
		Digest:    notWrittenDigest,
		CacheType: resource.CacheType_CAS,
	})
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, md)
}

func TestMetadata_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	md, err := mc.Metadata(ctx, r)
	require.NoError(t, err)
	require.Equal(t, int64(100), md.StoredSizeBytes)
}

func TestFindMissing(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{LogNotFoundErrors: true, DoubleReadPercentage: 1}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	notSetD1, _ := testdigest.NewRandomDigestBuf(t, 100)
	notSetD2, _ := testdigest.NewRandomDigestBuf(t, 100)

	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	digests := []*repb.Digest{d, notSetD1, notSetD2}
	rns := digest.ResourceNames(resource.CacheType_CAS, "", digests)
	missing, err := mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetD1, notSetD2}, missing)

	digests = []*repb.Digest{d}
	rns = digest.ResourceNames(resource.CacheType_CAS, "", digests)
	missing, err = mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestFindMissing_DestSrcMismatch(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{LogNotFoundErrors: true, DoubleReadPercentage: 1}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	d2, buf2 := testdigest.NewRandomDigestBuf(t, 100)
	r2 := &resource.ResourceName{
		Digest:    d2,
		CacheType: resource.CacheType_CAS,
	}
	d3, buf3 := testdigest.NewRandomDigestBuf(t, 100)
	r3 := &resource.ResourceName{
		Digest:    d3,
		CacheType: resource.CacheType_CAS,
	}

	// Set d in both caches, but set d2 and d3 in only one of the caches
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)
	err = srcCache.Set(ctx, r2, buf2)
	require.NoError(t, err)
	err = destCache.Set(ctx, r3, buf3)
	require.NoError(t, err)

	digests := []*repb.Digest{d, d2, d3}
	rns := digest.ResourceNames(resource.CacheType_CAS, "", digests)
	missing, err := mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	// Even though d3 is written to the dest cache, expect output to reflect that it's missing from src cache
	require.ElementsMatch(t, []*repb.Digest{d3}, missing)
}

func TestFindMissing_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	notSetD1, _ := testdigest.NewRandomDigestBuf(t, 100)
	notSetD2, _ := testdigest.NewRandomDigestBuf(t, 100)

	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	rns := digest.ResourceNames(resource.CacheType_CAS, "", []*repb.Digest{d, notSetD1, notSetD2})
	missing, err := mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetD1, notSetD2}, missing)
}

func TestGetMultiWithCopying(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 100)
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
	resourceNames := make([]*resource.ResourceName, 50)
	expected := make(map[*repb.Digest][]byte, 50)
	for i := 0; i < 50; i++ {
		idx := i
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			r := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			lock.Lock()
			defer lock.Unlock()
			err = srcCache.Set(ctx, r, buf)
			require.NoError(t, err)

			resourceNames[idx] = &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			expected[d] = buf
			return nil
		})
	}
	eg.Wait()

	r, err := mc.GetMulti(ctx, resourceNames)
	require.NoError(t, err)
	require.Equal(t, expected, r)

	for _, rn := range resourceNames {
		waitForCopy(t, ctx, destCache, rn)
	}
}

func TestSetMulti(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 100)
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
	dataToSet := make(map[*resource.ResourceName][]byte, 50)
	for i := 0; i < 50; i++ {
		eg.Go(func() error {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			rn := &resource.ResourceName{
				Digest:    d,
				CacheType: resource.CacheType_CAS,
			}
			lock.Lock()
			defer lock.Unlock()
			dataToSet[rn] = buf
			return nil
		})
	}
	eg.Wait()

	err = mc.SetMulti(ctx, dataToSet)
	require.NoError(t, err)

	for r, expected := range dataToSet {
		data, err := mc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, data))

		data, err = srcCache.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, data))

		data, err = destCache.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, data))
	}
}

func TestDelete(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	mc := migration_cache.NewMigrationCache(&migration_cache.MigrationConfig{}, srcCache, destCache)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Check data exists before delete
	data, err := mc.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	data, err = srcCache.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	data, err = destCache.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))

	// After delete, data should no longer exist
	err = mc.Delete(ctx, r)
	require.NoError(t, err)

	data, err = mc.Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))

	data, err = srcCache.Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))

	data, err = destCache.Get(ctx, r)
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
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		w, err := mc.Writer(ctx, r)
		require.NoError(t, err)

		_, err = w.Write(buf)
		require.NoError(t, err)

		err = w.Commit()
		require.NoError(t, err)

		err = w.Close()
		require.NoError(t, err)

		reader, err := mc.Reader(ctx, r, 0, 0)
		require.NoError(t, err)

		actualBuf := make([]byte, len(buf))
		n, err := reader.Read(actualBuf)
		require.NoError(t, err)
		require.Equal(t, int(testSize), n)
		require.True(t, bytes.Equal(buf, actualBuf))

		err = reader.Close()
		require.NoError(t, err)

		// Verify data was written to both caches
		srcReader, err := srcCache.Reader(ctx, r, 0, 0)
		require.NoError(t, err)

		actualBuf = make([]byte, len(buf))
		n, err = srcReader.Read(actualBuf)
		require.NoError(t, err)
		require.Equal(t, int(testSize), n)
		require.True(t, bytes.Equal(buf, actualBuf))

		err = srcReader.Close()
		require.NoError(t, err)

		destReader, err := destCache.Reader(ctx, r, 0, 0)
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
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

	// Will fail to set a dest writer
	w, err := mc.Writer(ctx, r)
	require.NoError(t, err)

	// Should still write data to src cache without error
	_, err = w.Write(buf)
	require.NoError(t, err)

	err = w.Commit()
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	// Will fail to set a dest reader
	reader, err := mc.Reader(ctx, r, 0, 0)
	require.NoError(t, err)

	// Should still read from src cache without error
	actualBuf := make([]byte, len(buf))
	n, err := reader.Read(actualBuf)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.True(t, bytes.Equal(buf, actualBuf))
}
