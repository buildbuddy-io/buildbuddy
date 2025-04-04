package hit_tracker_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func TestHitTracker_RecordsDetailedStats(t *testing.T) {
	env := testenv.GetTestEnv(t)
	flags.Set(t, "cache.detailed_stats_enabled", true)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	env.SetMetricsCollector(mc)
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
	ht := env.GetHitTrackerFactory().NewCASHitTracker(ctx, rmd)

	dl := ht.TrackDownload(d)
	dl.CloseWithBytesTransferred(compressedSize, compressedSize, repb.Compressor_ZSTD, "test")

	sc := hit_tracker.ScoreCard(ctx, env, iid)
	require.Len(t, sc.Results, 1, "expected exactly one cache result")
	actual := sc.Results[0]
	assert.Greater(t, actual.GetStartTime().AsTime().UnixNano(), int64(0), "missing timestamp")
	assert.Greater(t, actual.GetDuration().AsDuration().Nanoseconds(), int64(0), "missing duration")
	assert.Equal(t, "GoCompile", actual.ActionMnemonic)
	assert.Equal(t, "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4", actual.ActionId)
	assert.Equal(t, "//foo:bar", actual.TargetId)
	assert.Equal(t, rspb.CacheType_CAS, actual.CacheType)
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

func TestHitTracker_RecordsUsage(t *testing.T) {
	env := testenv.GetTestEnv(t)
	flags.Set(t, "cache.detailed_stats_enabled", true)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	env.SetMetricsCollector(mc)
	ut := &fakeUsageTracker{}
	env.SetUsageTracker(ut)
	ctx := context.Background()
	iid := "d42f4cd1-6963-4a5a-9680-cb77cfaad9bd"

	{
		// Bazel CAS cache hit
		rmd := &repb.RequestMetadata{
			ToolInvocationId: iid,
			ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
			ActionMnemonic:   "GoCompile",
			TargetId:         "//foo:bar",
		}
		d := &repb.Digest{
			Hash:      "c9c111006b30ffe6ce309fd64c44da651bffa068d530c7b1898698186b4afe2b",
			SizeBytes: 1000,
		}
		compressedSize := int64(100)
		ht := env.GetHitTrackerFactory().NewCASHitTracker(ctx, rmd)

		dl := ht.TrackDownload(d)
		dl.CloseWithBytesTransferred(compressedSize, compressedSize, repb.Compressor_ZSTD, "test")

		require.Len(t, ut.Increments, 1)
		assert.Equal(t, []*tables.UsageCounts{{
			CASCacheHits:           1,
			TotalDownloadSizeBytes: 1000,
		}}, ut.Increments)
		ut.Increments = nil
	}
	{
		// Executor CAS cache hit
		rmd := &repb.RequestMetadata{
			ToolInvocationId: iid,
			ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
			ActionMnemonic:   "GoCompile",
			TargetId:         "//foo:bar",
			ExecutorDetails:  &repb.ExecutorDetails{ExecutorHostId: "1234"},
		}
		d := &repb.Digest{
			Hash:      "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c",
			SizeBytes: 2000,
		}
		compressedSize := int64(100)
		ht := env.GetHitTrackerFactory().NewCASHitTracker(ctx, rmd)

		dl := ht.TrackDownload(d)
		dl.CloseWithBytesTransferred(compressedSize, compressedSize, repb.Compressor_ZSTD, "test")

		assert.Equal(t, []*tables.UsageCounts{{
			CASCacheHits:           1,
			TotalDownloadSizeBytes: 2000,
		}}, ut.Increments)
		ut.Increments = nil
	}
	{
		// Bazel CAS empty cache hit
		rmd := &repb.RequestMetadata{
			ToolInvocationId: iid,
			ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
			ActionMnemonic:   "GoCompile",
			TargetId:         "//foo:bar",
		}
		d := &repb.Digest{
			Hash:      digest.EmptySha256,
			SizeBytes: 0,
		}
		ht := env.GetHitTrackerFactory().NewCASHitTracker(ctx, rmd)

		dl := ht.TrackDownload(d)
		dl.CloseWithBytesTransferred(0, 0, repb.Compressor_ZSTD, "test")

		assert.Equal(t, []*tables.UsageCounts{{CASCacheHits: 1}}, ut.Increments)
		ut.Increments = nil
	}
	{
		// Bazel AC hit
		rmd := &repb.RequestMetadata{
			ToolInvocationId: iid,
			ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
			ActionMnemonic:   "GoCompile",
			TargetId:         "//foo:bar",
		}
		d := &repb.Digest{
			Hash:      "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730",
			SizeBytes: 111,
		}
		ht := env.GetHitTrackerFactory().NewACHitTracker(ctx, rmd)

		dl := ht.TrackDownload(d)
		dl.CloseWithBytesTransferred(d.SizeBytes, d.SizeBytes, repb.Compressor_IDENTITY, "test")

		assert.Equal(t, []*tables.UsageCounts{{
			ActionCacheHits:        1,
			TotalDownloadSizeBytes: 111,
		}}, ut.Increments)
		ut.Increments = nil
	}
}

type fakeUsageTracker struct {
	interfaces.UsageTracker
	Increments []*tables.UsageCounts
}

func (ut *fakeUsageTracker) Increment(ctx context.Context, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	ut.Increments = append(ut.Increments, counts)
	return nil
}
