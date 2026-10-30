package snaputil_test

import (
	"bytes"
	"context"
	"math/rand"
	"os"
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

func setupEnv(t *testing.T) *testenv.TestEnv {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
	flags.Set(t, "executor.enable_local_snapshot_sharing", true)

	env := testenv.GetTestEnv(t)
	fc, err := filecache.NewFileCache(testfs.MakeTempDir(t), 100_000, false)
	require.NoError(t, err)
	env.SetFileCache(fc)
	_, run, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	go run()
	return env
}

func TestCacheAndFetchArtifact(t *testing.T) {
	env := setupEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)
	fc := env.GetFileCache()

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	remoteInstanceName := ""
	fileTypeLabel := "test"

	// Test caching and fetching
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, true, d, remoteInstanceName, b, fileTypeLabel)
	require.NoError(t, err)
	outputPath := filepath.Join(tmpDir, "fetch")
	chunkSrc, err := snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), true, d, remoteInstanceName, outputPath)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceLocalFilecache, chunkSrc)

	// Read bytes from outputPath and validate with original bytes
	fetchedStr := testfs.ReadFileAsString(t, tmpDir, "fetch")
	require.Equal(t, randomStr, fetchedStr)

	// Test rewriting same digest
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, true, d, remoteInstanceName, b, fileTypeLabel)
	require.NoError(t, err)

	// Delete from remote cache, make sure we can still read
	rn := digest.NewResourceName(d, remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
	err = env.GetCache().Delete(ctx, rn)
	require.NoError(t, err)
	outputPathLocalFetch := filepath.Join(tmpDir, "fetch_local")
	chunkSrc, err = snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), true, d, remoteInstanceName, outputPathLocalFetch)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceLocalFilecache, chunkSrc)
	fetchedStr = testfs.ReadFileAsString(t, tmpDir, "fetch_local")
	require.Equal(t, randomStr, fetchedStr)

	// Rewrite artifact and delete from local cache, make sure we can still read
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, true, d, remoteInstanceName, b, fileTypeLabel)
	require.NoError(t, err)
	deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: d})
	require.True(t, deleted)
	outputPathRemoteFetch := filepath.Join(tmpDir, "fetch_remote")
	chunkSrc, err = snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), true, d, remoteInstanceName, outputPathRemoteFetch)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceRemoteCache, chunkSrc)
	fetchedStr = testfs.ReadFileAsString(t, tmpDir, "fetch_remote")
	require.Equal(t, randomStr, fetchedStr)

	// Test writing bogus digest
	bogusDigest, _ := testdigest.NewRandomResourceAndBuf(t, 1000, rspb.CacheType_CAS, remoteInstanceName)
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, true, bogusDigest.Digest, remoteInstanceName, b, fileTypeLabel)
	require.Error(t, err)
}

func TestCacheAndFetchArtifact_LocalOnly(t *testing.T) {
	env := setupEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)
	fc := env.GetFileCache()

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	remoteInstanceName := ""
	fileTypeLabel := "test"

	// Read and write bytes from local cache
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), false /*remoteEnabled*/, true, d, remoteInstanceName, b, fileTypeLabel)
	require.NoError(t, err)
	outputPath := filepath.Join(tmpDir, "fetch")
	chunkSrc, err := snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), false /*remoteEnabled*/, d, remoteInstanceName, outputPath)
	require.NoError(t, err)
	require.Equal(t, snaputil.ChunkSourceLocalFilecache, chunkSrc)
	// Read bytes from outputPath and validate with original bytes
	fetchedStr := testfs.ReadFileAsString(t, tmpDir, "fetch")
	require.Equal(t, randomStr, fetchedStr)

	// Delete artifact from local cache, make sure we can no longer read it
	deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: d})
	require.True(t, deleted)
	outputPathRemoteFetch := filepath.Join(tmpDir, "fetch_err")
	_, err = snaputil.GetArtifact(ctx, fc, env.GetByteStreamClient(), false /*remoteEnabled*/, d, remoteInstanceName, outputPathRemoteFetch)
	require.True(t, status.IsUnavailableError(err))
}

func TestCacheAndFetchBytes(t *testing.T) {
	env := setupEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)
	fc := env.GetFileCache()

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	remoteInstanceName := ""
	fileTypeLabel := "test"

	// Test caching and fetching
	err = snaputil.CacheBytes(ctx, fc, env.GetByteStreamClient(), true, true, d, remoteInstanceName, b, fileTypeLabel)
	require.NoError(t, err)
	fetchedBytes, err := snaputil.GetBytes(ctx, fc, env.GetByteStreamClient(), true, d, remoteInstanceName, tmpDir)
	require.NoError(t, err)
	require.Equal(t, randomStr, string(fetchedBytes))

	// Delete from local cache, make sure we can still read
	deleted := fc.DeleteFile(ctx, &repb.FileNode{Digest: d})
	require.True(t, deleted)
	fetchedBytes, err = snaputil.GetBytes(ctx, fc, env.GetByteStreamClient(), true, d, remoteInstanceName, tmpDir)
	require.NoError(t, err)
	require.Equal(t, randomStr, string(fetchedBytes))
}

func TestGetArtifactRejectsInvalidDigest(t *testing.T) {
	env := setupEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
	require.NoError(t, err)
	rootDir := testfs.MakeTempDir(t)
	d := &repb.Digest{Hash: "../../x", SizeBytes: 10}

	const sentinel = "sentinel-content"
	outputPath := filepath.Join(rootDir, "out")
	require.NoError(t, os.WriteFile(outputPath, []byte(sentinel), 0644))
	_, err = snaputil.GetArtifact(ctx, env.GetFileCache(), env.GetByteStreamClient(), true, d, "", outputPath)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
	b, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, sentinel, string(b))

	// GetBytes removes its temp file, so point at a missing dir: without
	// validation this fails with a different error.
	tmpDir := testfs.MakeDirAll(t, rootDir, "a/b")
	d = &repb.Digest{Hash: "../../missing/x", SizeBytes: 10}
	_, err = snaputil.GetBytes(ctx, env.GetFileCache(), env.GetByteStreamClient(), true, d, "", tmpDir)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
	entries, err := os.ReadDir(rootDir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.ElementsMatch(t, []string{"a", "out"}, names)
}
