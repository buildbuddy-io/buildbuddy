package usagetracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	pebblev1 "github.com/cockroachdb/pebble"
)

// BenchmarkEvictionCandidateDiscovery measures how long the eviction loop
// takes to discover every eligible record: M records total, K of them older
// than min_eviction_age (spread evenly through the keyspace), timed until K
// distinct candidates arrive on the deletes channel.
//
// The atime-index sweep's cost is proportional to K (read the index's cold
// front, stop at the age cutoff); the sampling designs it replaced read
// through record values across the whole keyspace, costing ~M.
func BenchmarkEvictionCandidateDiscovery(b *testing.B) {
	for _, tc := range []struct{ m, k int }{
		{10_000, 100},
		{100_000, 100},
		{100_000, 1_000},
	} {
		b.Run(fmt.Sprintf("m=%d/k=%d", tc.m, tc.k), func(b *testing.B) {
			dir := testfs.MakeTempDir(b)
			db, err := pebble.Open(dir, "test", &pebblev1.Options{})
			require.NoError(b, err)
			leaser := pebble.NewDBLeaser(db)
			b.Cleanup(func() {
				leaser.Close()
				db.Close()
			})

			// K eligible records (2h old, > minEvictionAge below) spread every
			// m/k-th write; the rest are fresh and ineligible.
			eligibleAtime := time.Now().Add(-2 * time.Hour).UnixMicro()
			freshAtime := time.Now().UnixMicro()
			stride := tc.m / tc.k
			for i := 0; i < tc.m; i++ {
				atime := freshAtime
				if i%stride == 0 && i/stride < tc.k {
					atime = eligibleAtime
				}
				writeRecord(b, db, atime, true /*=withIndex*/)
			}
			require.NoError(b, db.Flush())

			for b.Loop() {
				pu := &partitionUsage{
					part:     disk.Partition{ID: "FOO", MaxSizeBytes: 1 << 20},
					dbGetter: leaser,
					clock:    clockwork.NewRealClock(),
					// Far over the eviction threshold, so the loop sweeps.
					nodes:   map[string]*nodePartitionUsage{"n1": {sizeBytes: 1 << 40}},
					deletes: make(chan *evictionCandidate, 128),

					evictionRateLimit: 1_000_000_000,
					idleSleepDuration: time.Hour,
					minEvictionAge:    time.Hour,

					metrics: metricSet{
						atimeIndexSweepSeek:      metrics.RaftAtimeIndexSweepSeekDurationUsec.With(prometheus.Labels{metrics.PartitionID: "FOO"}),
						atimeIndexOrphansDropped: metrics.RaftAtimeIndexOrphansDropped.With(prometheus.Labels{metrics.PartitionID: "FOO"}),
					},
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					pu.evictionLoop(ctx)
					close(done)
				}()

				seen := make(map[string]bool, tc.k)
				for len(seen) < tc.k {
					select {
					case c := <-pu.deletes:
						seen[string(c.keyBytes)] = true
					case <-time.After(120 * time.Second):
						cancel()
						<-done
						b.Fatalf("timed out with %d/%d candidates discovered", len(seen), tc.k)
					}
				}
				cancel()
				<-done
			}
		})
	}
}
