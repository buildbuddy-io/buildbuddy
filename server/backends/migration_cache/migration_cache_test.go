package migration_cache_test

import (
	"bytes"
	"context"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/migration_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/errorcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"testing"
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
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func TestSet_DoubleWrite(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1000)
	rootDirSrc := testfs.MakeTempDir(t)
	rootDirDest := testfs.MakeTempDir(t)

	srcCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirDest}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	mcCache := migration_cache.NewMigrationCache(srcCache, destCache, 0)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mcCache.Set(ctx, d, buf)
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
	if err != nil {
		t.Fatal(err)
	}
	destCache := &errorcache.ErrorCache{}
	mcCache := migration_cache.NewMigrationCache(srcCache, destCache, 0)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mcCache.Set(ctx, d, buf)
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

	srcCache := &errorcache.ErrorCache{}
	destCache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: rootDirSrc}, int64(1000))
	if err != nil {
		t.Fatal(err)
	}
	mcCache := migration_cache.NewMigrationCache(srcCache, destCache, 0)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	err = mcCache.Set(ctx, d, buf)
	require.Error(t, err)

	// Verify data was deleted from the dest cache
	destData, err := destCache.Get(ctx, d)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, destData)
}
