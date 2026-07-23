package migration_cache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/cache_config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/migration_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, users))

	setMigrationState(t, migration_cache.SrcPrimary)
	fp, err := experiments.NewFlagProvider("test-name")
	require.NoError(t, err)
	te.SetExperimentFlagProvider(fp)
	return te
}

func setMigrationState(t *testing.T, migrationState string) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		migration_cache.MigrationCacheConfigFlag: {
			State:          memprovider.Enabled,
			DefaultVariant: "singleton",
			Variants: map[string]any{"singleton": map[string]any{
				migration_cache.MigrationStateField:           migrationState,
				migration_cache.AsyncDestWriteField:           false,
				migration_cache.DoubleReadPercentageField:     1.0,
				migration_cache.DecompressReadPercentageField: 0.0,
			}},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))
}

func setDoubleReadPercentage(t *testing.T, doubleRead float64) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		migration_cache.MigrationCacheConfigFlag: {
			State:          memprovider.Enabled,
			DefaultVariant: "singleton",
			Variants: map[string]any{"singleton": map[string]any{
				migration_cache.MigrationStateField:           migration_cache.SrcPrimary,
				migration_cache.AsyncDestWriteField:           false,
				migration_cache.DoubleReadPercentageField:     doubleRead,
				migration_cache.DecompressReadPercentageField: 0.0,
			}},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))
}

func setAsyncWrite(t *testing.T, asyncWrite bool) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		migration_cache.MigrationCacheConfigFlag: {
			State:          memprovider.Enabled,
			DefaultVariant: "singleton",
			Variants: map[string]any{"singleton": map[string]any{
				migration_cache.MigrationStateField:           migration_cache.SrcPrimary,
				migration_cache.AsyncDestWriteField:           asyncWrite,
				migration_cache.DoubleReadPercentageField:     0.0,
				migration_cache.DecompressReadPercentageField: 0.0,
			}},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))
}

func getAnonContext(t *testing.T, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err, "error ataching user prefix")
	return ctx
}

// waitForCopy keeps checking the destination cache to see whether the background process has
// copied the given digest over
func waitForCopy(t *testing.T, ctx context.Context, destCache interfaces.Cache, r *rspb.ResourceName) {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
		contains, err := destCache.Contains(ctx, r)
		require.NoError(t, err, "error calling contains on dest cache")

		if contains {
			return
		}

		// Data has not been copied yet... Keep waiting
		time.Sleep(delay)
	}

	require.FailNowf(t, "timeout", "Timed out waiting for %v to be copied to dest cache", r)
}

func waitForValue(ctx context.Context, destCache interfaces.Cache, r *rspb.ResourceName, expected []byte) error {
	for delay := 50 * time.Millisecond; delay < 3*time.Second; delay *= 2 {
		actual, err := destCache.Get(ctx, r)
		if err != nil && !status.IsNotFoundError(err) {
			return err
		}

		if bytes.Equal(actual, expected) {
			return nil
		}

		// Data has not been copied yet... Keep waiting
		time.Sleep(delay)
	}
	return fmt.Errorf("Timed out waiting for %v to be copied to dest cache", r)
}

// errorCache lets us mock errors to test error handling
type errorCache struct {
	interfaces.Cache
	calls atomic.Int64
}

func (c *errorCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	c.calls.Add(1)
	return errors.New("error cache set err")
}

func (c *errorCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	c.calls.Add(1)
	return errors.New("error cache set multi err")
}

func (c *errorCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	c.calls.Add(1)
	return nil, errors.New("error cache get err")
}

func (c *errorCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	c.calls.Add(1)
	return nil, errors.New("error cache get multi err")
}

func (c *errorCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	c.calls.Add(1)
	return errors.New("error cache delete err")
}

func (c *errorCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	c.calls.Add(1)
	return false, errors.New("error cache contains err")
}

func (c *errorCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	c.calls.Add(1)
	return nil, errors.New("error cache find missing err")
}

func (c *errorCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	c.calls.Add(1)
	return nil, errors.New("error cache metadata err")
}

func (c *errorCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	c.calls.Add(1)
	return nil, errors.New("error cache writer err")
}

func (c *errorCache) Reader(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	c.calls.Add(1)
	return nil, errors.New("error cache reader err")
}

func TestACIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)

	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)

	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r1, buf1 := testdigest.RandomACResourceBuf(t, 10)
	require.NoError(t, mc.Set(ctx, r1, buf1))

	got1, err := mc.Get(ctx, r1)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	// Data should not be in CAS cache
	gotCAS, err := mc.Get(ctx, &rspb.ResourceName{
		Digest:    r1.GetDigest(),
		CacheType: rspb.CacheType_CAS,
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf1 := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_AC, "remote")
	require.NoError(t, mc.Set(ctx, r, buf1))

	got1, err := mc.Get(ctx, r)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	// Data should not be in CAS cache
	gotCAS, err := mc.Get(ctx, digest.NewResourceName(r.GetDigest(), "remote", rspb.CacheType_CAS, r.GetDigestFunction()).ToProto())
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = mc.Set(ctx, r, buf)
	require.Error(t, err)

	// Verify data wasn't committed to the dest cache.
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err := mc.Set(ctx, r, buf)
	require.Error(t, err)
}

func TestAsyncWrite_ACValueChanged(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	setAsyncWrite(t, true)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	require.NoError(t, mc.Start())
	defer mc.Stop()

	r, buf := testdigest.RandomACResourceBuf(t, 100)
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	srcData, err := srcCache.Get(ctx, r)
	require.NoError(t, err)
	require.Equal(t, buf, srcData)

	require.NoError(t, waitForValue(ctx, destCache, r, buf))

	// Write a new value and make sure it gets copied too.
	_, buf = testdigest.RandomACResourceBuf(t, 100)
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	srcData, err = srcCache.Get(ctx, r)
	require.NoError(t, err)
	require.Equal(t, buf, srcData)

	require.NoError(t, waitForValue(ctx, destCache, r, buf))
}

// delayedWriterCache wraps a Cache and delays commits until waitFunc returns.
type delayedWriterCache struct {
	interfaces.Cache
	waitFunc func()
}

type delayedWriter struct {
	interfaces.CommittedWriteCloser
	waitFunc func()
}

func (dw *delayedWriter) Commit() error {
	dw.waitFunc()
	return dw.CommittedWriteCloser.Commit()
}

func (c *delayedWriterCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	w, err := c.Cache.Writer(ctx, r)
	if err != nil {
		return nil, err
	}
	return &delayedWriter{w, c.waitFunc}, nil
}

func TestAsyncWrite_SrcIsSlow(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	setAsyncWrite(t, true)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	delayedSrcCache := &delayedWriterCache{srcCache, func() { time.Sleep(100 * time.Millisecond) }}
	mc := migration_cache.NewMigrationCache(te, config, delayedSrcCache, destCache)
	require.NoError(t, mc.Start())
	defer mc.Stop()

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	srcData, err := srcCache.Get(ctx, r)
	require.NoError(t, err)
	require.Equal(t, buf, srcData)

	require.NoError(t, waitForValue(ctx, destCache, r, buf))
}

type failWritesCache struct {
	interfaces.Cache
}

func (c *failWritesCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	return nil, errors.New("simulated writer failure")
}

func TestAsyncWrite_SrcFails(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	setAsyncWrite(t, true)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, &failWritesCache{srcCache}, destCache)
	require.NoError(t, mc.Start())
	defer mc.Stop()

	r, buf := testdigest.RandomACResourceBuf(t, 100)
	// Write to the src cache directly to avoid the simulated write failure.
	err = srcCache.Set(ctx, r, buf)
	require.NoError(t, err)

	// This will fail to write to src cache, and so shouldn't copy to dest cache.
	err = mc.Set(ctx, r, buf)
	require.Error(t, err)

	require.Error(t, waitForValue(ctx, destCache, r, buf))
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		r, buf := testdigest.RandomCASResourceBuf(t, testSize)
		err = mc.Set(ctx, r, buf)
		require.NoError(t, err, "error setting digest in cache")

		// Get() the bytes from the cache.
		rbuf, err := mc.Get(ctx, r)
		require.NoError(t, err)
		d2, err := digest.Compute(bytes.NewReader(rbuf), r.GetDigestFunction())
		require.NoError(t, err)
		require.True(t, r.GetDigest().GetHash() == d2.GetHash())
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

	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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

	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return data from src cache without error
	data, err := mc.Get(ctx, r)
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

	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	// Write data to dest cache only
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = destCache.Set(ctx, r, buf)
	require.NoError(t, err)

	// Should return error
	data, err := mc.Get(ctx, r)
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

	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, _ := testdigest.RandomCASResourceBuf(t, 100)
	err = mc.Set(ctx, r, []byte{})
	require.NoError(t, err)

	data, err := mc.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte{}, data))
}

// TestCopyDataInBackground ensures that the copy queue works even when there
// are many parallel requests.
func TestCopyDataInBackground(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			te := getTestEnv(t, emptyUserMap)
			ctx := getAnonContext(t, te)
			maxSizeBytes := int64(1_000_000_000) // 1GB
			rootDirSrc := testfs.MakeTempDir(t)
			rootDirDest := testfs.MakeTempDir(t)

			srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
			require.NoError(t, err)
			destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
			require.NoError(t, err)

			numTests := 100
			config := &cache_config.MigrationConfig{
				CopyChanBufferSize: numTests + 1,
				NumCopyWorkers:     2,
			}
			config.SetConfigDefaults()
			var mc *migration_cache.MigrationCache
			if reverse {
				setMigrationState(t, migration_cache.DestPrimary)
				mc = migration_cache.NewMigrationCache(te, config, destCache, srcCache)
			} else {
				mc = migration_cache.NewMigrationCache(te, config, srcCache, destCache)
			}
			mc.Start() // Starts copying in background
			defer mc.Stop()

			eg, ctx := errgroup.WithContext(ctx)
			for i := 0; i < numTests; i++ {
				eg.Go(func() error {
					r, buf := testdigest.RandomCASResourceBuf(t, 100)
					err := srcCache.Set(ctx, r, buf)
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
		})
	}
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

	config := &cache_config.MigrationConfig{
		CopyChanBufferSize:             1,
		CopyChanFullWarningIntervalMin: 1,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			r, buf := testdigest.RandomCASResourceBuf(t, 100)
			lock.Lock()
			defer lock.Unlock()

			err = srcCache.Set(ctx, r, buf)
			require.NoError(t, err)

			// We should exceed the copy channel size, but should not prevent us from continuing
			// to read from the cache
			data, err := mc.Get(ctx, r)
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

	config := &cache_config.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    1,
		NumCopyWorkers:     2,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	start := time.Now()

	for i := 0; i < 2; i++ {
		eg.Go(func() error {
			r, buf := testdigest.RandomCASResourceBuf(t, 100)
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

	// Should wait at least 1 second before the second digest is copied
	require.True(t, time.Since(start) >= 1*time.Second)
}

func TestCopyDataInBackground_RateLimitMin(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 100)

	srcCache, err := memory_cache.NewMemoryCache(maxSizeBytes)
	require.NoError(t, err)
	destCache, err := memory_cache.NewMemoryCache(maxSizeBytes)
	require.NoError(t, err)

	const writeCount = 10
	config := &cache_config.MigrationConfig{
		CopyChanBufferSize: writeCount,
		MaxCopiesPerSec:    writeCount,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	resources := make(map[*rspb.ResourceName][]byte, writeCount)
	for range writeCount {
		r, buf := testdigest.RandomCASResourceBuf(t, 100)
		err := srcCache.Set(ctx, r, buf)
		require.NoError(t, err)
		resources[r] = buf
	}
	start := time.Now()
	for r, buf := range resources {
		// Get should queue copy in background
		data, err := mc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, data))

		// Expect copy
		waitForCopy(t, ctx, destCache, r)
	}

	// Copies should not be rate limited, and should complete within 1 second
	require.LessOrEqual(t, time.Since(start), 1*time.Second)
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
		RootDirectory: rootDirDest,
		MaxSizeBytes:  maxSizeBytes,
	}
	destCache, err := pebble_cache.NewPebbleCache(te, pebbleOptions)
	require.NoError(t, err)
	destCache.Start()

	config := &cache_config.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    10,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	authenticatedCtx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
	authenticatedCtx, err = prefix.AttachUserPrefixToContext(authenticatedCtx, te.GetAuthenticator())
	require.NoError(t, err)

	// Save data to different isolations in src cache
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = srcCache.Set(authenticatedCtx, r, buf)
	require.NoError(t, err)

	r2, buf2 := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_AC, "dog")
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

	config := &cache_config.MigrationConfig{
		CopyChanBufferSize: 10,
		MaxCopiesPerSec:    10,
	}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	// Save data to different isolations in src cache
	r, buf := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_AC, "cat")
	err = srcCache.Set(ctx, r, buf)
	require.NoError(t, err)

	r2, buf2 := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_CAS, "cow")
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
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			te := getTestEnv(t, emptyUserMap)
			ctx := getAnonContext(t, te)
			maxSizeBytes := int64(1_000_000_000) // 1GB
			rootDirSrc := testfs.MakeTempDir(t)
			rootDirDest := testfs.MakeTempDir(t)

			srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
			require.NoError(t, err)
			destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
			require.NoError(t, err)

			config := &cache_config.MigrationConfig{
				CopyChanBufferSize: 10,
				LogNotFoundErrors:  true,
			}
			config.SetConfigDefaults()
			var mc *migration_cache.MigrationCache
			if reverse {
				setMigrationState(t, migration_cache.DestPrimary)
				mc = migration_cache.NewMigrationCache(te, config, destCache, srcCache)
			} else {
				mc = migration_cache.NewMigrationCache(te, config, srcCache, destCache)
			}
			mc.Start() // Starts copying in background
			defer mc.Stop()

			// Save digest to both caches - does not need to be copied to dest, but should be there
			r1, buf := testdigest.RandomCASResourceBuf(t, 100)
			err = mc.Set(ctx, r1, buf)
			require.NoError(t, err)

			// Save digest to only src cache - should be copied to dest cache
			r2, buf2 := testdigest.RandomCASResourceBuf(t, 100)
			err = srcCache.Set(ctx, r2, buf2)
			require.NoError(t, err)

			// Save digest to only dest cache - should not be copied to src cache
			r3, buf3 := testdigest.RandomCASResourceBuf(t, 100)
			err = destCache.Set(ctx, r3, buf3)
			require.NoError(t, err)

			// FindMissing should queue copies in background
			missing, err := mc.FindMissing(ctx, []*rspb.ResourceName{r1, r2, r3})
			require.NoError(t, err)
			require.Equal(t, []*repb.Digest{r3.GetDigest()}, missing)

			// Expect data to have been copied
			waitForCopy(t, ctx, destCache, r1)
			waitForCopy(t, ctx, destCache, r2)
		})
	}
}

func TestContains(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 100)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)
	remoteInstanceName := "cloud"

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	require.NoError(t, err)
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start()
	defer mc.Stop()

	r, buf := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_AC, remoteInstanceName)
	err = srcCache.Set(ctx, r, buf)
	require.NoError(t, err)

	contains, err := mc.Contains(ctx, r)
	require.NoError(t, err)
	require.True(t, contains)

	waitForCopy(t, ctx, destCache, r)

	notWrittenResource, _ := testdigest.RandomCASResourceBuf(t, 100)
	contains, err = mc.Contains(ctx, notWrittenResource)
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start()
	defer mc.Stop()

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = srcCache.Set(ctx, r, buf)
	require.NoError(t, err)

	md, err := mc.Metadata(ctx, r)
	require.NoError(t, err)
	require.Equal(t, int64(100), md.StoredSizeBytes)

	notWrittenResource, _ := testdigest.RandomCASResourceBuf(t, 100)
	md, err = mc.Metadata(ctx, notWrittenResource)
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{LogNotFoundErrors: true}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)

	notSetR1, _ := testdigest.RandomCASResourceBuf(t, 100)
	notSetR2, _ := testdigest.RandomCASResourceBuf(t, 100)

	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	rns := []*rspb.ResourceName{r, notSetR1, notSetR2}
	missing, err := mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetR1.GetDigest(), notSetR2.GetDigest()}, missing)

	rns = []*rspb.ResourceName{r}
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{LogNotFoundErrors: true}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	r2, buf2 := testdigest.RandomCASResourceBuf(t, 100)
	r3, buf3 := testdigest.RandomCASResourceBuf(t, 100)

	// Set d in both caches, but set d2 and d3 in only one of the caches
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)
	err = srcCache.Set(ctx, r2, buf2)
	require.NoError(t, err)
	err = destCache.Set(ctx, r3, buf3)
	require.NoError(t, err)

	rns := []*rspb.ResourceName{r, r2, r3}
	missing, err := mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	// Even though d3 is written to the dest cache, expect output to reflect that it's missing from src cache
	require.ElementsMatch(t, []*repb.Digest{r3.GetDigest()}, missing)
}

func TestFindMissing_DestErr(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 10)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	notSetR1, _ := testdigest.RandomCASResourceBuf(t, 100)
	notSetR2, _ := testdigest.RandomCASResourceBuf(t, 100)

	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)

	// Hack to fix a data race below. When the destination cache fails,
	// it logs the request resource names using proto text formatting, which
	// calls ProtoReflect(), which stores an atomic pointer. This happens in
	// parallel with require.ElementsMatch() below, which uses deepequal to
	// compare values which reads the pointer without using atomic operations.
	// Calling ProtoReflect() initializes the pointer here instead of in the
	// goroutine in FindMissing.
	// TODO(vanja) try using the synctest package once we upgrade to 1.25
	_ = notSetR1.GetDigest().ProtoReflect()
	_ = notSetR2.GetDigest().ProtoReflect()

	// Should return data from src cache without error
	rns := digest.ResourceNames(rspb.CacheType_CAS, "", []*repb.Digest{r.GetDigest(), notSetR1.GetDigest(), notSetR2.GetDigest()})
	missing, err := mc.FindMissing(ctx, rns)
	require.NoError(t, err)
	for range 5 {
		if destCache.calls.Load() > 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, int64(5), destCache.calls.Load(), "Expected dest cache to be called twice (Set, FindMissing, 3x Contains)")
	require.ElementsMatch(t, []*repb.Digest{notSetR1.GetDigest(), notSetR2.GetDigest()}, missing)
}

func TestGetMultiWithCopying(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			te := getTestEnv(t, emptyUserMap)
			ctx := getAnonContext(t, te)
			maxSizeBytes := int64(defaultExt4BlockSize * 100)
			rootDirSrc := testfs.MakeTempDir(t)
			rootDirDest := testfs.MakeTempDir(t)

			srcCache, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDirSrc, MaxSizeBytes: maxSizeBytes})
			require.NoError(t, err)
			err = srcCache.Start()
			require.NoError(t, err)
			destCache, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDirDest, MaxSizeBytes: maxSizeBytes})
			require.NoError(t, err)
			err = destCache.Start()
			require.NoError(t, err)
			config := &cache_config.MigrationConfig{}
			config.SetConfigDefaults()
			var mc *migration_cache.MigrationCache
			if reverse {
				setMigrationState(t, migration_cache.DestPrimary)
				mc = migration_cache.NewMigrationCache(te, config, destCache, srcCache)
			} else {
				mc = migration_cache.NewMigrationCache(te, config, srcCache, destCache)
			}
			mc.Start() // Starts copying in background
			defer mc.Stop()

			eg, ctx := errgroup.WithContext(ctx)
			lock := sync.RWMutex{}
			resourceNames := make([]*rspb.ResourceName, 50)
			expected := make(map[*repb.Digest][]byte, 50)
			for i := 0; i < 50; i++ {
				idx := i
				eg.Go(func() error {
					r, buf := testdigest.RandomCASResourceBuf(t, 100)
					lock.Lock()
					defer lock.Unlock()
					err = srcCache.Set(ctx, r, buf)
					require.NoError(t, err)

					resourceNames[idx] = r
					expected[r.GetDigest()] = buf
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
		})
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
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)

	eg, ctx := errgroup.WithContext(ctx)
	lock := sync.RWMutex{}
	dataToSet := make(map[*rspb.ResourceName][]byte, 50)
	for i := 0; i < 50; i++ {
		eg.Go(func() error {
			rn, buf := testdigest.RandomCASResourceBuf(t, 100)
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
	mc := migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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

	_, err = mc.Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))

	_, err = srcCache.Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))

	_, err = destCache.Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))
}

// TestReadsCopyData ensures that methods that actually read data (as opposed to
// just checking presence) trigger copies even when double read percentage is 0.
func TestReadsCopyData(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDirSrc := testfs.MakeTempDir(t)

	setDoubleReadPercentage(t, 0)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	require.NoError(t, err)
	destCache.Start()
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	t.Run("Reader", func(t *testing.T) {
		r, buf := testdigest.RandomCASResourceBuf(t, 10)
		require.NoError(t, srcCache.Set(ctx, r, buf))
		reader, err := mc.Reader(ctx, r, 0, 0)
		require.NoError(t, err)
		reader.Close()
		waitForCopy(t, ctx, destCache, r)
	})
	t.Run("Get", func(t *testing.T) {
		r, buf := testdigest.RandomCASResourceBuf(t, 10)
		require.NoError(t, srcCache.Set(ctx, r, buf))
		_, err = mc.Get(ctx, r)
		require.NoError(t, err)
		waitForCopy(t, ctx, destCache, r)
	})
	t.Run("GetMulti", func(t *testing.T) {
		r, buf := testdigest.RandomCASResourceBuf(t, 10)
		require.NoError(t, srcCache.Set(ctx, r, buf))
		_, err = mc.GetMulti(ctx, []*rspb.ResourceName{r})
		require.NoError(t, err)
		waitForCopy(t, ctx, destCache, r)
	})
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
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		t.Run(fmt.Sprintf("size=%d", testSize), func(t *testing.T) {
			r, buf := testdigest.RandomCASResourceBuf(t, testSize)
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

			actualBuf, err = ioutil.ReadAll(srcReader)
			require.NoError(t, err)
			require.Equal(t, int(testSize), len(actualBuf))
			require.True(t, bytes.Equal(buf, actualBuf))

			err = srcReader.Close()
			require.NoError(t, err)

			destReader, err := destCache.Reader(ctx, r, 0, 0)
			require.NoError(t, err)

			actualBuf, err = ioutil.ReadAll(destReader)
			require.NoError(t, err)
			require.Equal(t, int(testSize), len(actualBuf))
			require.True(t, bytes.Equal(buf, actualBuf))

			err = destReader.Close()
			require.NoError(t, err)
		})
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
	config := &cache_config.MigrationConfig{}
	config.SetConfigDefaults()
	mc := migration_cache.NewMigrationCache(te, config, srcCache, destCache)
	mc.Start() // Starts copying in background
	defer mc.Stop()

	r, buf := testdigest.RandomCASResourceBuf(t, 100)

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

func testOnlyOneCache(t *testing.T, reverse bool) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(defaultExt4BlockSize * 100)
	rootDirSrc := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	require.NoError(t, err)
	destCache := &errorCache{}
	var mc *migration_cache.MigrationCache
	if reverse {
		setMigrationState(t, migration_cache.DestOnly)
		mc = migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, destCache, srcCache)
	} else {
		setMigrationState(t, migration_cache.SrcOnly)
		mc = migration_cache.NewMigrationCache(te, &cache_config.MigrationConfig{}, srcCache, destCache)
	}

	// Write using all the write methods
	// set
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = mc.Set(ctx, r, buf)
	require.NoError(t, err)
	// setmulti
	err = mc.SetMulti(ctx, map[*rspb.ResourceName][]byte{r: buf})
	require.NoError(t, err)
	// writer
	w, err := mc.Writer(ctx, r)
	require.NoError(t, err)
	_, err = w.Write(buf)
	require.NoError(t, err)
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	// Read using all the read methods
	// contains
	contains, err := mc.Contains(ctx, r)
	require.NoError(t, err)
	require.True(t, contains)
	// metadata
	md, err := mc.Metadata(ctx, r)
	require.NoError(t, err)
	require.Equal(t, int64(100), md.StoredSizeBytes)
	// findmissing
	missing, err := mc.FindMissing(ctx, []*rspb.ResourceName{r})
	require.NoError(t, err)
	require.Empty(t, missing)
	// get
	data, err := mc.Get(ctx, r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf, data))
	// getmulti
	dataMulti, err := mc.GetMulti(ctx, []*rspb.ResourceName{r})
	require.NoError(t, err)
	require.Equal(t, map[*repb.Digest][]byte{r.GetDigest(): buf}, dataMulti)
	// reader
	reader, err := mc.Reader(ctx, r, 0, 0)
	require.NoError(t, err)
	readBuf := make([]byte, len(buf))
	_, err = reader.Read(readBuf)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, buf, readBuf)

	contains, err = srcCache.Contains(ctx, r)
	require.NoError(t, err)
	require.True(t, contains)

	// delete
	require.NoError(t, mc.Delete(ctx, r))

	contains, err = srcCache.Contains(ctx, r)
	require.NoError(t, err)
	require.False(t, contains)

	// destination should never have been called
	require.Equal(t, int64(0), destCache.calls.Load())
}

func TestOnlySrc(t *testing.T) {
	testOnlyOneCache(t, false)
}

func TestOnlyDest(t *testing.T) {
	testOnlyOneCache(t, true)
}

func TestValidateCacheConfig(t *testing.T) {
	testCases := []struct {
		name        string
		config      cache_config.CacheConfig
		expectError bool
	}{
		{
			name: "valid disk config only",
			config: cache_config.CacheConfig{
				DiskConfig: &cache_config.DiskCacheConfig{
					RootDirectory: "/tmp/disk",
				},
			},
			expectError: false,
		},
		{
			name: "valid pebble config only",
			config: cache_config.CacheConfig{
				PebbleConfig: &cache_config.PebbleCacheConfig{
					RootDirectory: "/tmp/pebble",
				},
			},
			expectError: false,
		},
		{
			name: "valid meta config only",
			config: cache_config.CacheConfig{
				MetaConfig: &cache_config.MetaCacheConfig{
					MetadataBackend: "mysql",
				},
			},
			expectError: false,
		},
		{
			name: "valid disk + distributed",
			config: cache_config.CacheConfig{
				DiskConfig: &cache_config.DiskCacheConfig{
					RootDirectory: "/tmp/disk",
				},
				DistributedConfig: &cache_config.DistributedCacheConfig{
					ListenAddr: "localhost:1985",
				},
			},
			expectError: false,
		},
		{
			name: "valid pebble + distributed",
			config: cache_config.CacheConfig{
				PebbleConfig: &cache_config.PebbleCacheConfig{
					RootDirectory: "/tmp/pebble",
				},
				DistributedConfig: &cache_config.DistributedCacheConfig{
					ListenAddr: "localhost:1985",
				},
			},
			expectError: false,
		},
		{
			name:        "invalid no configs",
			config:      cache_config.CacheConfig{},
			expectError: true,
		},
		{
			name: "invalid both disk and pebble",
			config: cache_config.CacheConfig{
				DiskConfig: &cache_config.DiskCacheConfig{
					RootDirectory: "/tmp/disk",
				},
				PebbleConfig: &cache_config.PebbleCacheConfig{
					RootDirectory: "/tmp/pebble",
				},
			},
			expectError: true,
		},
		{
			name: "invalid meta + disk",
			config: cache_config.CacheConfig{
				MetaConfig: &cache_config.MetaCacheConfig{
					MetadataBackend: "mysql",
				},
				DiskConfig: &cache_config.DiskCacheConfig{
					RootDirectory: "/tmp/disk",
				},
			},
			expectError: true,
		},
		{
			name: "invalid meta + pebble",
			config: cache_config.CacheConfig{
				MetaConfig: &cache_config.MetaCacheConfig{
					MetadataBackend: "mysql",
				},
				PebbleConfig: &cache_config.PebbleCacheConfig{
					RootDirectory: "/tmp/pebble",
				},
			},
			expectError: true,
		},
		{
			name: "invalid meta + distributed",
			config: cache_config.CacheConfig{
				MetaConfig: &cache_config.MetaCacheConfig{
					MetadataBackend: "mysql",
				},
				DistributedConfig: &cache_config.DistributedCacheConfig{
					ListenAddr: "localhost:1985",
				},
			},
			expectError: true,
		},
		{
			name: "invalid only distributed",
			config: cache_config.CacheConfig{
				DistributedConfig: &cache_config.DistributedCacheConfig{
					ListenAddr: "localhost:1985",
				},
			},
			expectError: true,
		},
		{
			name: "invalid all configs",
			config: cache_config.CacheConfig{
				DiskConfig: &cache_config.DiskCacheConfig{
					RootDirectory: "/tmp/disk",
				},
				PebbleConfig: &cache_config.PebbleCacheConfig{
					RootDirectory: "/tmp/pebble",
				},
				DistributedConfig: &cache_config.DistributedCacheConfig{
					ListenAddr: "localhost:1985",
				},
				MetaConfig: &cache_config.MetaCacheConfig{
					MetadataBackend: "mysql",
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := migration_cache.ValidateCacheConfig(tc.config)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
