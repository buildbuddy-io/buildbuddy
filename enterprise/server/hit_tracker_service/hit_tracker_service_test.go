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

	for _, tc := range []struct {
		name        string
		cacheType   rspb.CacheType
		requestType capb.RequestType
	}{
		{
			name:        "CAS upload",
			cacheType:   rspb.CacheType_CAS,
			requestType: capb.RequestType_WRITE,
		},
		{
			name:        "CAS download",
			cacheType:   rspb.CacheType_CAS,
			requestType: capb.RequestType_READ,
		},
		{
			name:        "AC upload",
			cacheType:   rspb.CacheType_AC,
			requestType: capb.RequestType_WRITE,
		},
		{
			name:        "AC download",
			cacheType:   rspb.CacheType_AC,
			requestType: capb.RequestType_READ,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
			require.NoError(t, err)
			env.SetMetricsCollector(mc)
			hit_tracker.Register(env)
			require.NoError(t, hit_tracker_service.Register(env))
			ctx := context.Background()

			iid := "d42f4cd1-6963-4a5a-9680-cb77cfaad9bd"

			// Track one cache hit using the hit_tracker directly.
			emptyRmd := &repb.RequestMetadata{
				ToolInvocationId: iid,
				ActionId:         "4bcc407e49739ff0ea6394ba83efe63ad934c65bb4655db3fe5282d6e005894f",
				ActionMnemonic:   "EmptyAction",
				TargetId:         "//empty",
			}
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

			var ht interfaces.HitTracker
			if tc.cacheType == rspb.CacheType_AC {
				ht = env.GetHitTrackerFactory().NewACHitTracker(ctx, rmd)
			} else {
				ht = env.GetHitTrackerFactory().NewCASHitTracker(ctx, rmd)
			}

			var dl interfaces.TransferTimer
			if tc.requestType == capb.RequestType_READ {
				dl = ht.TrackDownload(d1)
			} else {
				dl = ht.TrackUpload(d1)
			}
			dl.CloseWithBytesTransferred(compressedSize, compressedSize, repb.Compressor_ZSTD, "test")

			// Track two more cache hits using the hit_tracker_service.
			emptyDigest := repb.Digest{Hash: digest.EmptySha256, SizeBytes: 0}
			emptyRn := digest.NewResourceName(&emptyDigest, "", tc.cacheType, repb.DigestFunction_SHA256)
			emptyHit := hitpb.CacheHit{
				RequestMetadata:  emptyRmd,
				Resource:         emptyRn.ToProto(),
				CacheRequestType: tc.requestType,
			}
			d := repb.Digest{
				Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				SizeBytes: 456,
			}
			rn := digest.NewResourceName(&d, "", tc.cacheType, repb.DigestFunction_SHA256)
			hit := hitpb.CacheHit{
				RequestMetadata:  rmd,
				Resource:         rn.ToProto(),
				SizeBytes:        456,
				Duration:         durationpb.New(time.Minute),
				CacheRequestType: tc.requestType,
			}
			req := hitpb.TrackRequest{Hits: []*hitpb.CacheHit{&emptyHit, &hit}}

			_, err = env.GetHitTrackerServiceServer().Track(ctx, &req)
			require.NoError(t, err)

			sc := hit_tracker.ScoreCard(ctx, env, iid)
			require.Len(t, sc.Results, 3, "expected exactly three cache results")
			actual1 := sc.Results[0]
			require.Equal(t, tc.cacheType, actual1.CacheType)
			require.Equal(t, tc.requestType, actual1.RequestType)
			require.Equal(t, d1.Hash, actual1.GetDigest().GetHash())
			require.Equal(t, d1.SizeBytes, actual1.GetDigest().GetSizeBytes())
			require.Equal(t, repb.Compressor_ZSTD, actual1.GetCompressor())
			require.Equal(t, compressedSize, actual1.GetTransferredSizeBytes())

			actual2 := sc.Results[1]
			require.Equal(t, tc.cacheType, actual2.CacheType)
			require.Equal(t, tc.requestType, actual2.RequestType)
			require.Equal(t, digest.EmptySha256, actual2.GetDigest().GetHash())
			require.Equal(t, int64(0), actual2.GetDigest().GetSizeBytes())
			require.Equal(t, repb.Compressor_IDENTITY, actual2.GetCompressor())
			require.Equal(t, int64(0), actual2.GetTransferredSizeBytes())

			actual3 := sc.Results[2]
			require.Equal(t, tc.cacheType, actual3.CacheType)
			require.Equal(t, tc.requestType, actual3.RequestType)
			require.Equal(t, d.Hash, actual3.GetDigest().GetHash())
			require.Equal(t, d.SizeBytes, actual3.GetDigest().GetSizeBytes())
			require.Equal(t, repb.Compressor_IDENTITY, actual3.GetCompressor())
			require.Equal(t, int64(456), actual3.GetTransferredSizeBytes())
		})
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

func TestHitTrackerService_Usage(t *testing.T) {
	flags.Set(t, "cache.detailed_stats_enabled", true)

	for _, tc := range []struct {
		name          string
		cacheType     rspb.CacheType
		requestType   capb.RequestType
		expectedUsage []*tables.UsageCounts
	}{
		{
			name:        "CAS upload",
			cacheType:   rspb.CacheType_CAS,
			requestType: capb.RequestType_WRITE,
			expectedUsage: []*tables.UsageCounts{
				{},
				{TotalUploadSizeBytes: 456},
			},
		},
		{
			name:        "CAS download",
			cacheType:   rspb.CacheType_CAS,
			requestType: capb.RequestType_READ,
			expectedUsage: []*tables.UsageCounts{
				{CASCacheHits: 1},
				{
					CASCacheHits:           1,
					TotalDownloadSizeBytes: 456,
				},
			},
		},
		{
			name:        "AC upload",
			cacheType:   rspb.CacheType_AC,
			requestType: capb.RequestType_WRITE,
			expectedUsage: []*tables.UsageCounts{
				{},
				{TotalUploadSizeBytes: 456},
			},
		},
		{
			name:        "AC download",
			cacheType:   rspb.CacheType_AC,
			requestType: capb.RequestType_READ,
			expectedUsage: []*tables.UsageCounts{
				{ActionCacheHits: 1},
				{ActionCacheHits: 1, TotalDownloadSizeBytes: 456},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			emptyRmd := &repb.RequestMetadata{
				ToolInvocationId: iid,
				ActionId:         "4bcc407e49739ff0ea6394ba83efe63ad934c65bb4655db3fe5282d6e005894f",
				ActionMnemonic:   "EmptyAction",
				TargetId:         "//empty",
			}
			rmd := &repb.RequestMetadata{
				ToolInvocationId: iid,
				ActionId:         "f498500e6d2825ef3bd5564bb56c439da36efe38ab4936ae0ff93794e704ccb4",
				ActionMnemonic:   "GoCompile",
				TargetId:         "//foo:bar",
			}

			emptyDigest := repb.Digest{Hash: digest.EmptySha256, SizeBytes: 0}
			emptyRn := digest.NewResourceName(&emptyDigest, "", tc.cacheType, repb.DigestFunction_SHA256)
			emptyHit := hitpb.CacheHit{
				RequestMetadata:  emptyRmd,
				Resource:         emptyRn.ToProto(),
				CacheRequestType: tc.requestType,
			}
			d := repb.Digest{
				Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				SizeBytes: 456,
			}
			rn := digest.NewResourceName(&d, "", tc.cacheType, repb.DigestFunction_SHA256)
			hit := hitpb.CacheHit{
				RequestMetadata:  rmd,
				Resource:         rn.ToProto(),
				SizeBytes:        456,
				Duration:         durationpb.New(time.Minute),
				CacheRequestType: tc.requestType,
			}
			req := hitpb.TrackRequest{Hits: []*hitpb.CacheHit{&emptyHit, &hit}}
			_, err = env.GetHitTrackerServiceServer().Track(ctx, &req)
			require.NoError(t, err)

			require.Len(t, ut.Increments, 2)
			require.Equal(t, tc.expectedUsage, ut.Increments)
			ut.Increments = nil
		})
	}
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
