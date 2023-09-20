package snaploader_utils_test

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader_utils"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func TestCacheAndFetchArtifact(t *testing.T) {
	flags.Set(t, "executor.enable_remote_snapshot_sharing", true)

	env := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err)
	tmpDir := testfs.MakeTempDir(t)

	fc, err := filecache.NewFileCache(tmpDir, 100000, false)
	require.NoError(t, err)
	clientConn := runByteStreamServer(ctx, env, t)
	require.NoError(t, err)
	bsClient := bspb.NewByteStreamClient(clientConn)

	length := rand.Intn(1000)
	randomStr, err := random.RandomString(length)
	require.NoError(t, err)
	b := []byte(randomStr)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	// Test caching and fetching
	err = snaploader_utils.CacheBytes(ctx, fc, bsClient, d, b, tmpDir)
	require.NoError(t, err)
	outputPath := filepath.Join(tmpDir, "fetch")
	err = snaploader_utils.FetchArtifact(ctx, fc, bsClient, d, outputPath)
	require.NoError(t, err)

	// Read bytes from outputPath and validate with original bytes
	fetchedStr := testfs.ReadFileAsString(t, tmpDir, "fetch")
	require.Equal(t, randomStr, fetchedStr)

	// Test rewriting same digest
	err = snaploader_utils.CacheBytes(ctx, fc, bsClient, d, b, tmpDir)
	require.NoError(t, err)

	// Delete from remote cache, make sure we can still read
	rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_BLAKE3).ToProto()
	err = env.GetCache().Delete(ctx, rn)
	require.NoError(t, err)
	outputPathLocalFetch := filepath.Join(tmpDir, "fetch_local")
	err = snaploader_utils.FetchArtifact(ctx, fc, bsClient, d, outputPathLocalFetch)
	require.NoError(t, err)
	fetchedStr = testfs.ReadFileAsString(t, tmpDir, "fetch_local")
	require.Equal(t, randomStr, fetchedStr)

	// Rewrite artifact and delete from local cache, make sure we can still read
	err = snaploader_utils.CacheBytes(ctx, fc, bsClient, d, b, tmpDir)
	require.NoError(t, err)
	deleted := fc.DeleteFile(&repb.FileNode{Digest: d})
	require.True(t, deleted)
	outputPathRemoteFetch := filepath.Join(tmpDir, "fetch_remote")
	err = snaploader_utils.FetchArtifact(ctx, fc, bsClient, d, outputPathRemoteFetch)
	require.NoError(t, err)
	fetchedStr = testfs.ReadFileAsString(t, tmpDir, "fetch_remote")
	require.Equal(t, randomStr, fetchedStr)

	// Test writing bogus digest
	bogusDigest, _ := testdigest.NewRandomResourceAndBuf(t, 1000, rspb.CacheType_CAS, "")
	err = snaploader_utils.CacheBytes(ctx, fc, bsClient, bogusDigest.Digest, b, tmpDir)
	require.Error(t, err)
}

func runByteStreamServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)

	grpcServer, runFunc := env.LocalGRPCServer()
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	require.NoError(t, err)

	return clientConn
}
