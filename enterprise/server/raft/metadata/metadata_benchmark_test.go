package metadata_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/metadata"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	dto "github.com/prometheus/client_model/go"
)

// benchPoolSize is the number of distinct records each benchmark cycles
// through. Large enough to avoid always hitting warm per-key state, small
// enough that setup stays quick.
const benchPoolSize = 512

// setUpBenchmarkCluster starts a 3-node metadata cluster, writes benchPoolSize
// records owned by group1, and returns a leader Server, a context authenticated
// as the records' owner, and the file records that were written. The test
// partitions are configured with NumRanges: 1, so every request below maps to a
// single range.
func setUpBenchmarkCluster(b *testing.B) (*metadata.Server, context.Context, []*sgpb.FileRecord) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	b.Helper()
	configs := getTestConfigs(b, 3)
	caches := startNodes(b, configs)
	rc := caches[0]

	ctx, err := configs[0].ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(b, err)

	records := make([]*sgpb.FileRecord, 0, benchPoolSize)
	var ops []*mdpb.SetRequest_SetOperation
	flush := func() {
		if len(ops) == 0 {
			return
		}
		_, err := rc.Set(ctx, &mdpb.SetRequest{SetOperations: ops})
		require.NoError(b, err)
		ops = ops[:0]
	}
	for range benchPoolSize {
		md := randomFileMetadata(b, 100, "group1")
		records = append(records, md.GetFileRecord())
		ops = append(ops, &mdpb.SetRequest_SetOperation{FileMetadata: md})
		// Write in chunks to keep individual raft proposals small.
		if len(ops) == 64 {
			flush()
		}
	}
	flush()

	return rc, ctx, records
}

// BenchmarkGet measures single-key Get, a RANGELEASE read served by the
// leaseholder (no consensus round trip).
func BenchmarkGet(b *testing.B) {
	rc, ctx, records := setUpBenchmarkCluster(b)
	req := &mdpb.GetRequest{FileRecords: make([]*sgpb.FileRecord, 1)}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		req.FileRecords[0] = records[i%len(records)]
		i++
		_, err := rc.Get(ctx, req)
		require.NoError(b, err)
	}
}

// BenchmarkFind measures single-key Find (existence check), also a
// RANGELEASE read.
func BenchmarkFind(b *testing.B) {
	rc, ctx, records := setUpBenchmarkCluster(b)
	req := &mdpb.FindRequest{FileRecords: make([]*sgpb.FileRecord, 1)}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		req.FileRecords[0] = records[i%len(records)]
		i++
		_, err := rc.Find(ctx, req)
		require.NoError(b, err)
	}
}

// atimeFlushCount reads the number of atime flush batches proposed so far.
// With atime_write_batch_size=1, batches == individual updates.
func atimeFlushCount(b *testing.B) uint64 {
	m := &dto.Metric{}
	require.NoError(b, metrics.RaftBatchAtimeUpdateDurationUsec.Write(m))
	return m.GetHistogram().GetSampleCount()
}

// BenchmarkFindWithAtimeUpdates measures Find while every hit also enqueues
// an atime update (threshold 0) -- the write side effect FindMissing traffic
// generates in production once records age past atime_update_threshold.
// atime_write_batch_size=1 makes the flusher propose each update immediately,
// so nothing waits on the 10s flush timer and the flush-histogram count
// equals the number of updates actually proposed (b.N minus buffer drops).
//
// ns/op covers only Find; the async pipeline's debt is paid in the drain step
// and reported separately:
//
//	atime-updates/op: updates proposed per Find (1.0 = pipeline kept up, no drops)
//	drain-ms:         time to flush the backlog after Finds stopped
func BenchmarkFindWithAtimeUpdates(b *testing.B) {
	flags.Set(b, "cache.raft.atime_update_threshold", time.Duration(0))
	flags.Set(b, "cache.raft.atime_write_batch_size", 1)
	rc, ctx, records := setUpBenchmarkCluster(b)
	req := &mdpb.FindRequest{FileRecords: make([]*sgpb.FileRecord, 1)}
	before := atimeFlushCount(b)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		req.FileRecords[0] = records[i%len(records)]
		i++
		_, err := rc.Find(ctx, req)
		require.NoError(b, err)
	}
	b.StopTimer()

	drainStart := time.Now()
	rc.TestingFlushAtimeUpdates()
	drainDur := time.Since(drainStart)

	proposed := atimeFlushCount(b) - before
	b.ReportMetric(float64(proposed)/float64(i), "atime-updates/op")
	b.ReportMetric(float64(drainDur.Milliseconds()), "drain-ms")
	if proposed == 0 {
		b.Fatal("no atime updates were proposed; benchmark is misconfigured")
	}
}

// BenchmarkGetParallel runs single-key Get from many goroutines at
// once.
func BenchmarkGetParallel(b *testing.B) {
	rc, ctx, records := setUpBenchmarkCluster(b)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		req := &mdpb.GetRequest{FileRecords: make([]*sgpb.FileRecord, 1)}
		i := 0
		for pb.Next() {
			req.FileRecords[0] = records[i%len(records)]
			i++
			_, err := rc.Get(ctx, req)
			require.NoError(b, err)
		}
	})
}
