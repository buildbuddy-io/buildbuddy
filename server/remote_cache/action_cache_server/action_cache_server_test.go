package action_cache_server_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gcodes "google.golang.org/grpc/codes"
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

func TestHitTracking(t *testing.T) {
	for _, test := range []struct {
		name                     string
		actionResultProtoPresent bool
		outputsPresent           bool
		expectHit                bool
	}{
		{
			name:                     "MissingProto_CacheMiss",
			actionResultProtoPresent: false,
			outputsPresent:           true,
			expectHit:                false,
		},
		{
			name:                     "MissingOutputs_CacheMiss",
			actionResultProtoPresent: true,
			outputsPresent:           false,
			expectHit:                false,
		},
		{
			name:                     "ProtoAndOutputsPresent_CacheHit",
			actionResultProtoPresent: true,
			outputsPresent:           true,
			expectHit:                true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "cache.detailed_stats_enabled", true)
			resetMetrics()

			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			clientConn := runACServer(ctx, t, te)
			acClient := repb.NewActionCacheClient(clientConn)
			bsClient := bspb.NewByteStreamClient(clientConn)
			metricsCollector, err := memory_metrics_collector.NewMemoryMetricsCollector()
			require.NoError(t, err)
			te.SetMetricsCollector(metricsCollector)

			actionDigest := &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1}
			invocationID := "f5b5e1f7-7e91-4e3f-88f6-2f925e521aa0"
			// Upload action result and/or the referenced output file as
			// applicable
			instanceName := "test"
			digestFn := repb.DigestFunction_SHA256
			outputDigest, err := digest.Compute(strings.NewReader("hello world"), digestFn)
			require.NoError(t, err)
			if test.actionResultProtoPresent {
				_, err := acClient.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
					InstanceName:   instanceName,
					DigestFunction: digestFn,
					ActionDigest:   actionDigest,
					ActionResult: &repb.ActionResult{
						OutputFiles: []*repb.OutputFile{
							{
								Path:   "hello.txt",
								Digest: outputDigest,
							},
						},
					},
				})
				require.NoError(t, err)
			}
			if test.outputsPresent {
				_, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte("hello world"))
				require.NoError(t, err)
			}

			// Make an AC request, setting request metadata for hit tracking
			acCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
				ActionId:         actionDigest.GetHash(),
				ToolInvocationId: invocationID,
			})
			require.NoError(t, err)
			actionResult, err := acClient.GetActionResult(acCtx, &repb.GetActionResultRequest{
				InstanceName:   instanceName,
				DigestFunction: digestFn,
				ActionDigest:   actionDigest,
			})
			if test.expectHit {
				require.NoError(t, err)
			} else {
				require.True(t, status.IsNotFoundError(err), "expected NotFound, got %T", err)
			}
			scorecard := hit_tracker.ScoreCard(ctx, te, invocationID)
			expectedStatus := &statuspb.Status{Code: int32(gcodes.NotFound)}
			if test.expectHit {
				expectedStatus = &statuspb.Status{Code: int32(gcodes.OK)}
			}
			expectedResults := []*capb.ScoreCard_Result{
				{
					ActionId:             actionDigest.GetHash(),
					CacheType:            rspb.CacheType_AC,
					Digest:               actionDigest,
					RequestType:          capb.RequestType_READ,
					Status:               expectedStatus,
					TransferredSizeBytes: int64(proto.Size(actionResult)),
				},
			}
			assert.Empty(t, cmp.Diff(
				expectedResults,
				scorecard.GetResults(),
				protocmp.Transform(),
				protocmp.IgnoreFields(
					&capb.ScoreCard_Result{},
					"start_time",
					"duration",
				),
			))
		})
	}
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
