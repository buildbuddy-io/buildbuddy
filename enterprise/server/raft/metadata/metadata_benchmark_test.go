package metadata_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/metadata"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
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
