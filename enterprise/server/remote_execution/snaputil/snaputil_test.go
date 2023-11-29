package snaputil_test

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func TestCacheAndFetchArtifact(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	env := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)

	fc, err := filecache.NewFileCache(tmpDir, 100000, false)
	require.NoError(t, err)
	testcache.Setup(t, env)

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	// Test caching and fetching
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, d, "", b)
	require.NoError(t, err)
	outputPath := filepath.Join(tmpDir, "fetch")
	chunkSrc, err := snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), true, d, "", outputPath)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceLocalFilecache, chunkSrc)

	// Read bytes from outputPath and validate with original bytes
	fetchedStr := testfs.ReadFileAsString(t, tmpDir, "fetch")
	require.Equal(t, randomStr, fetchedStr)

	// Test rewriting same digest
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, d, "", b)
	require.NoError(t, err)

	// Delete from remote cache, make sure we can still read
	rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
	err = env.GetCache().Delete(ctx, rn)
	require.NoError(t, err)
	outputPathLocalFetch := filepath.Join(tmpDir, "fetch_local")
	chunkSrc, err = snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), true, d, "", outputPathLocalFetch)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceLocalFilecache, chunkSrc)
	fetchedStr = testfs.ReadFileAsString(t, tmpDir, "fetch_local")
	require.Equal(t, randomStr, fetchedStr)

	// Rewrite artifact and delete from local cache, make sure we can still read
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, d, "", b)
	require.NoError(t, err)
	deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: d})
	require.True(t, deleted)
	outputPathRemoteFetch := filepath.Join(tmpDir, "fetch_remote")
	chunkSrc, err = snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), true, d, "", outputPathRemoteFetch)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceRemoteCache, chunkSrc)
	fetchedStr = testfs.ReadFileAsString(t, tmpDir, "fetch_remote")
	require.Equal(t, randomStr, fetchedStr)

	// Test writing bogus digest
	bogusDigest, _ := testdigest.NewRandomResourceAndBuf(t, 1000, rspb.CacheType_CAS, "")
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, bogusDigest.Digest, "", b)
	require.Error(t, err)
}

func TestCacheAndFetchArtifact_LocalOnly(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	env := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)

	fc, err := filecache.NewFileCache(tmpDir, 10000, false)
	require.NoError(t, err)
	testcache.Setup(t, env)

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	// Read and write bytes from local cache
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), false /*remoteEnabled*/, d, "", b)
	require.NoError(t, err)
	outputPath := filepath.Join(tmpDir, "fetch")
	chunkSrc, err := snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), false /*remoteEnabled*/, d, "", outputPath)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceLocalFilecache, chunkSrc)
	// Read bytes from outputPath and validate with original bytes
	fetchedStr := testfs.ReadFileAsString(t, tmpDir, "fetch")
	require.Equal(t, randomStr, fetchedStr)

	// Delete artifact from local cache, make sure we can no longer read it
	deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: d})
	require.True(t, deleted)
	outputPathRemoteFetch := filepath.Join(tmpDir, "fetch_err")
	_, err = snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), false /*remoteEnabled*/, d, "", outputPathRemoteFetch)
	require.True(t, status.IsUnavailableError(err))
}

func TestCacheAndFetchBytes(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	env := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)

	fc, err := filecache.NewFileCache(tmpDir, 100000, false)
	require.NoError(t, err)
	testcache.Setup(t, env)

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	// Test caching and fetching
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, d, "", b)
	require.NoError(t, err)
	fetchedBytes, err := snaputil.GetBytes(ctx, fc, env.GetByteStreamClient(), true, d, "", tmpDir)
	require.NoError(t, err)
	require.Equal(t, randomStr, string(fetchedBytes))

	// Delete from local cache, make sure we can still read
	deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: d})
	require.True(t, deleted)
	fetchedBytes, err = snaputil.GetBytes(ctx, fc, env.GetByteStreamClient(), true, d, "", tmpDir)
	require.NoError(t, err)
	require.Equal(t, randomStr, string(fetchedBytes))
}
