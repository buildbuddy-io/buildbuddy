package hit_tracker_service_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func TestHitTrackerService_DetailedStats(t *testing.T) {
	flags.Set(t, "cache.detailed_stats_enabled", true)

	env := testenv.GetTestEnv(t)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	env.SetMetricsCollector(mc)
	hit_tracker.Register(env)
	require.NoError(t, hit_tracker_service.Register(env))
	ctx := context.Background()

	iid := "d42f4cd1-6963-4a5a-9680-cb77cfaad9bd"

	// Track one cache hit using the hit_tracker directly.
	rmd := &repb.RequestMetadata{
		ToolInvocationId: iid,
		ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
		ActionMnemonic:   "GoCompile",
		TargetId:         "//foo:bar",
	}
	d1 := &repb.Digest{
		Hash:      "c9c111006b30ffe6ce309fd64c44da651bffa068d530c7b1898698186b4afe2b",
		SizeBytes: 1234,
	}
	compressedSize := int64(123)
	ctx = withRequestMetadata(t, ctx, rmd)
	require.NoError(t, err)
	ht := env.GetHitTrackerFactory().NewCASHitTracker(ctx, iid)

	dl := ht.TrackDownload(d1)
	dl.CloseWithBytesTransferred(compressedSize, compressedSize, repb.Compressor_ZSTD, "test")

	// Track two more cache hits using the hit_tracker_service.
	d2 := repb.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 456,
	}
	rn := digest.NewResourceName(&d2, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	hits := hitpb.CacheHits{
		InvocationId: iid,
		EmptyHits:    1,
		Downloads: []*hitpb.Download{&hitpb.Download{
			Resource:  rn.ToProto(),
			SizeBytes: 456,
			Duration:  durationpb.New(time.Minute),
		}},
	}
	req := hitpb.TrackRequest{Hits: []*hitpb.CacheHits{&hits}}

	_, err = env.GetHitTrackerServiceServer().Track(ctx, &req)
	require.NoError(t, err)

	sc := hit_tracker.ScoreCard(ctx, env, iid)
	require.Len(t, sc.Results, 3, "expected exactly three cache results")
	actual1 := sc.Results[0]
	require.Equal(t, rspb.CacheType_CAS, actual1.CacheType)
	require.Equal(t, capb.RequestType_READ, actual1.RequestType)
	require.Equal(t, d1.Hash, actual1.GetDigest().GetHash())
	require.Equal(t, d1.SizeBytes, actual1.GetDigest().GetSizeBytes())
	require.Equal(t, repb.Compressor_ZSTD, actual1.GetCompressor())
	require.Equal(t, compressedSize, actual1.GetTransferredSizeBytes())

	actual2 := sc.Results[1]
	require.Equal(t, rspb.CacheType_CAS, actual2.CacheType)
	require.Equal(t, capb.RequestType_READ, actual2.RequestType)
	require.Equal(t, digest.EmptySha256, actual2.GetDigest().GetHash())
	require.Equal(t, int64(0), actual2.GetDigest().GetSizeBytes())
	require.Equal(t, repb.Compressor_IDENTITY, actual2.GetCompressor())
	require.Equal(t, int64(0), actual2.GetTransferredSizeBytes())

	actual3 := sc.Results[2]
	require.Equal(t, rspb.CacheType_CAS, actual3.CacheType)
	require.Equal(t, capb.RequestType_READ, actual3.RequestType)
	require.Equal(t, d2.Hash, actual3.GetDigest().GetHash())
	require.Equal(t, d2.SizeBytes, actual3.GetDigest().GetSizeBytes())
	require.Equal(t, repb.Compressor_IDENTITY, actual3.GetCompressor())
	require.Equal(t, int64(456), actual3.GetTransferredSizeBytes())
}

type fakeUsageTracker struct {
	interfaces.UsageTracker
	Increments []*tables.UsageCounts
}

func (ut *fakeUsageTracker) Increment(ctx context.Context, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	ut.Increments = append(ut.Increments, counts)
	return nil
}

func TestHitTrackerService_Usage(t *testing.T) {
	flags.Set(t, "cache.detailed_stats_enabled", true)

	env := testenv.GetTestEnv(t)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	env.SetMetricsCollector(mc)
	ut := &fakeUsageTracker{}
	env.SetUsageTracker(ut)
	hit_tracker.Register(env)
	require.NoError(t, hit_tracker_service.Register(env))
	require.NotNil(t, env.GetHitTrackerServiceServer())

	ctx := context.Background()
	iid := "d42f4cd1-6963-4a5a-9680-cb77cfaad9bd"

	d2 := repb.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 456,
	}
	rn := digest.NewResourceName(&d2, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	hits := hitpb.CacheHits{
		InvocationId: iid,
		EmptyHits:    1,
		Downloads: []*hitpb.Download{&hitpb.Download{
			Resource:  rn.ToProto(),
			SizeBytes: 456,
			Duration:  durationpb.New(time.Minute),
		}},
	}
	req := hitpb.TrackRequest{Hits: []*hitpb.CacheHits{&hits}}
	_, err = env.GetHitTrackerServiceServer().Track(ctx, &req)
	require.NoError(t, err)

	require.Len(t, ut.Increments, 1)
	require.Equal(t, []*tables.UsageCounts{{
		CASCacheHits:           1,
		TotalDownloadSizeBytes: 456,
	}}, ut.Increments)
	ut.Increments = nil
}

// Note: Can't use bazel_request.WithRequestMetadata here since it sets the
// metadata on the outgoing context, but the hit tracker reads the metadata
// from the incoming context, and in this test the server implementation is
// called directly; not over RPC.
func withRequestMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	md := metadata.Pairs(bazel_request.RequestMetadataKey, string(b))
	return metadata.NewIncomingContext(ctx, md)
}
