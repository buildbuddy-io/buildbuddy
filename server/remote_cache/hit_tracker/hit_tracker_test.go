package hit_tracker_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestHitTracker_RecordsDetailedStats(t *testing.T) {
	env := testenv.GetTestEnv(t)
	flags.Set(t, "cache.detailed_stats_enabled", true)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	env.SetMetricsCollector(mc)
	actionCache := false
	ctx := context.Background()
	iid := "d42f4cd1-6963-4a5a-9680-cb77cfaad9bd"
	rmd := &repb.RequestMetadata{
		ToolInvocationId: iid,
		ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
		ActionMnemonic:   "GoCompile",
		TargetId:         "//foo:bar",
	}
	d := &repb.Digest{
		Hash:      "c9c111006b30ffe6ce309fd64c44da651bffa068d530c7b1898698186b4afe2b",
		SizeBytes: 1234,
	}
	compressedSize := int64(123)
	ctx = withRequestMetadata(t, ctx, rmd)
	require.NoError(t, err)
	ht := hit_tracker.NewHitTracker(ctx, env, actionCache)

	dl := ht.TrackDownload(d)
	dl.CloseWithBytesTransferred(compressedSize, repb.Compressor_ZSTD)

	sc := hit_tracker.ScoreCard(ctx, env, iid)
	require.Len(t, sc.Results, 1, "expected exactly one cache result")
	actual := sc.Results[0]
	assert.Greater(t, actual.GetStartTime().AsTime().UnixNano(), int64(0), "missing timestamp")
	assert.Greater(t, actual.GetDuration().AsDuration().Nanoseconds(), int64(0), "missing duration")
	assert.Equal(t, "GoCompile", actual.ActionMnemonic)
	assert.Equal(t, "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4", actual.ActionId)
	assert.Equal(t, "//foo:bar", actual.TargetId)
	assert.Equal(t, capb.CacheType_CAS, actual.CacheType)
	assert.Equal(t, capb.RequestType_READ, actual.RequestType)
	assert.Equal(t, d.Hash, actual.GetDigest().GetHash())
	assert.Equal(t, d.SizeBytes, actual.GetDigest().GetSizeBytes())
	assert.Equal(t, repb.Compressor_ZSTD, actual.GetCompressor())
	assert.Equal(t, compressedSize, actual.GetTransferredSizeBytes())
	stats := hit_tracker.CollectCacheStats(ctx, env, iid)
	assert.Equal(t, int64(1), stats.GetCasCacheHits())
	assert.Equal(t, d.SizeBytes, stats.GetTotalDownloadSizeBytes())
	assert.Equal(t, compressedSize, stats.GetTotalDownloadTransferredSizeBytes())
	assert.Equal(t, int64(0), stats.GetCasCacheUploads())
	assert.Equal(t, int64(0), stats.GetTotalUploadSizeBytes())
	assert.Equal(t, int64(0), stats.GetTotalUploadTransferredSizeBytes())
}

// Note: Can't use bazel_request.WithRequestMetadata here since it sets the
// metadata on the outgoing context, but the hit tracker reads the metadata
// from the incoming context.
func withRequestMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	md := metadata.Pairs(bazel_request.RequestMetadataKey, string(b))
	return metadata.NewIncomingContext(ctx, md)
}
