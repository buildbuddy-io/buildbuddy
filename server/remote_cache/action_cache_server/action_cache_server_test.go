package action_cache_server_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func TestInlineSingleFile(t *testing.T) {
	resetMetrics()

	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	clientConn := runACServer(ctx, t, te)
	acClient := repb.NewActionCacheClient(clientConn)
	bsClient := bspb.NewByteStreamClient(clientConn)

	digestA, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte("hello world"))
	require.NoError(t, err)

	update(t, ctx, acClient, []*repb.OutputFile{
		{
			Path:   "my/pkg/file",
			Digest: digestA,
		},
	})

	actionResult := getWithInlining(t, ctx, acClient, []string{"my/pkg/file"})
	require.Len(t, actionResult.OutputFiles, 1)
	assert.Equal(t, "my/pkg/file", actionResult.OutputFiles[0].Path)
	assert.Equal(t, digestA, actionResult.OutputFiles[0].Digest)
	assert.Equal(t, []byte("hello world"), actionResult.OutputFiles[0].Contents)

	testmetrics.AssertHistogramSamples(t, metrics.CacheRequestedInlineSizeBytes, float64(len("hello world")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.CacheEvents.With(
		prometheus.Labels{
			metrics.CacheTypeLabel:      "action_cache",
			metrics.CacheEventTypeLabel: "hit",
		},
	)))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.CacheEvents.With(
		prometheus.Labels{
			metrics.CacheTypeLabel:      "cas",
			metrics.CacheEventTypeLabel: "hit",
		},
	)))
}

func TestInlineSingleFileTooLarge(t *testing.T) {
	resetMetrics()

	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	clientConn := runACServer(ctx, t, te)
	acClient := repb.NewActionCacheClient(clientConn)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Choose a size that is just under the limit to verify that the proto size is factored in.
	size := 4*1024*1024 - 1
	digestA, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte(strings.Repeat("a", size)))
	require.NoError(t, err)

	update(t, ctx, acClient, []*repb.OutputFile{
		{
			Path:   "my/pkg/file",
			Digest: digestA,
		},
	})

	actionResult := getWithInlining(t, ctx, acClient, []string{"my/pkg/file"})
	require.Len(t, actionResult.OutputFiles, 1)
	assert.Equal(t, "my/pkg/file", actionResult.OutputFiles[0].Path)
	assert.Equal(t, digestA, actionResult.OutputFiles[0].Digest)
	assert.Empty(t, actionResult.OutputFiles[0].Contents)

	testmetrics.AssertHistogramSamples(t, metrics.CacheRequestedInlineSizeBytes, float64(size))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.CacheEvents.With(
		prometheus.Labels{
			metrics.CacheTypeLabel:      "action_cache",
			metrics.CacheEventTypeLabel: "hit",
		},
	)))
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.CacheEvents.With(
		prometheus.Labels{
			metrics.CacheTypeLabel:      "cas",
			metrics.CacheEventTypeLabel: "hit",
		},
	)))
}

func TestInlineMultipleFiles(t *testing.T) {
	resetMetrics()

	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	clientConn := runACServer(ctx, t, te)
	acClient := repb.NewActionCacheClient(clientConn)
	bsClient := bspb.NewByteStreamClient(clientConn)

	digestA, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte("hello world"))
	require.NoError(t, err)
	digestB, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte("hello bb"))
	require.NoError(t, err)
	digestC, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte(strings.Repeat("a", 4*1024*1024-1)))
	require.NoError(t, err)

	update(t, ctx, acClient, []*repb.OutputFile{
		{
			Path:   "my/pkg/file",
			Digest: digestA,
		},
		{
			Path:   "my/pkg/file2",
			Digest: digestB,
		},
		{
			Path:   "my/pkg/file3",
			Digest: digestC,
		},
	})

	actionResult := getWithInlining(t, ctx, acClient, []string{"my/pkg/file", "my/pkg/file2", "my/pkg/file3"})
	require.Len(t, actionResult.OutputFiles, 3)
	assert.Equal(t, "my/pkg/file", actionResult.OutputFiles[0].Path)
	assert.Equal(t, digestA, actionResult.OutputFiles[0].Digest)
	assert.Equal(t, []byte("hello world"), actionResult.OutputFiles[0].Contents)
	assert.Equal(t, "my/pkg/file2", actionResult.OutputFiles[1].Path)
	assert.Equal(t, digestB, actionResult.OutputFiles[1].Digest)
	assert.Equal(t, []byte("hello bb"), actionResult.OutputFiles[1].Contents)
	assert.Equal(t, "my/pkg/file3", actionResult.OutputFiles[2].Path)
	assert.Equal(t, digestC, actionResult.OutputFiles[2].Digest)
	assert.Empty(t, actionResult.OutputFiles[2].Contents)

	testmetrics.AssertHistogramSamples(t, metrics.CacheRequestedInlineSizeBytes, float64(len("hello world")))
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.CacheRequestedInlineSizeBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.CacheEvents.With(
		prometheus.Labels{
			metrics.CacheTypeLabel:      "action_cache",
			metrics.CacheEventTypeLabel: "hit",
		},
	)))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.CacheEvents.With(
		prometheus.Labels{
			metrics.CacheTypeLabel:      "cas",
			metrics.CacheEventTypeLabel: "hit",
		},
	)))
}

func update(t *testing.T, ctx context.Context, client repb.ActionCacheClient, outputFiles []*repb.OutputFile) {
	req := repb.UpdateActionResultRequest{
		ActionDigest: &repb.Digest{
			Hash:      strings.Repeat("a", 64),
			SizeBytes: 1024,
		},
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult: &repb.ActionResult{
			OutputFiles: outputFiles,
		},
	}
	_, err := client.UpdateActionResult(ctx, &req)
	require.NoError(t, err)
}

func getWithInlining(t *testing.T, ctx context.Context, client repb.ActionCacheClient, inline []string) *repb.ActionResult {
	req := &repb.GetActionResultRequest{
		ActionDigest: &repb.Digest{
			Hash:      strings.Repeat("a", 64),
			SizeBytes: 1024,
		},
		DigestFunction:    repb.DigestFunction_SHA256,
		InlineOutputFiles: inline,
	}
	resp, err := client.GetActionResult(ctx, req)
	require.NoError(t, err)
	return resp
}

func runACServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	acServer, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	repb.RegisterActionCacheServer(grpcServer, acServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func resetMetrics() {
	metrics.CacheRequestedInlineSizeBytes.Reset()
	metrics.CacheEvents.Reset()
}
