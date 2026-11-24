package usagetracker

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pebble "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebblev2"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	gstatus "google.golang.org/grpc/status"
)

// fakeAPIClient returns a fixed response and records the request.
type fakeAPIClient struct {
	rfspb.ApiClient

	rsp *rfpb.SyncProposeResponse
	err error
	req *rfpb.SyncProposeRequest
}

func (c *fakeAPIClient) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest, opts ...grpc.CallOption) (*rfpb.SyncProposeResponse, error) {
	c.req = req
	return c.rsp, c.err
}

func deleteUnion(err error) *rfpb.ResponseUnion {
	s, _ := gstatus.FromError(err)
	return &rfpb.ResponseUnion{
		Status: s.Proto(),
		Value:  &rfpb.ResponseUnion_Delete{Delete: &rfpb.DeleteResponse{}},
	}
}

func missingUnion() *rfpb.ResponseUnion {
	return &rfpb.ResponseUnion{
		Value: &rfpb.ResponseUnion_Delete{Delete: &rfpb.DeleteResponse{Missing: true}},
	}
}

func testCandidates(n int) ([]*evictionCandidate, []*evictionKeyMeta) {
	candidates := make([]*evictionCandidate, n)
	keyMetas := make([]*evictionKeyMeta, n)
	for i := range n {
		candidates[i] = &evictionCandidate{
			keyBytes: []byte{byte('a' + i)},
			atime:    time.UnixMicro(int64(1000 + i)),
		}
		keyMetas[i] = sender.NewKeyMeta(candidates[i].keyBytes, candidates[i])
	}
	return candidates, keyMetas
}

func TestDeleteBatch(t *testing.T) {
	pu := &partitionUsage{}
	mismatch := status.FailedPreconditionError("Atime mismatch")

	for _, tc := range []struct {
		name          string
		rsp           *rfpb.BatchCmdResponse
		wantEvicted   []int
		wantMissing   []int
		wantNumFailed int
	}{
		{
			name: "all deleted",
			rsp: &rfpb.BatchCmdResponse{Union: []*rfpb.ResponseUnion{
				deleteUnion(nil), deleteUnion(nil), deleteUnion(nil),
			}},
			wantEvicted: []int{0, 1, 2},
		},
		{
			// Keep successful deletes when another key fails.
			name: "partial failure",
			rsp: &rfpb.BatchCmdResponse{Union: []*rfpb.ResponseUnion{
				deleteUnion(nil), deleteUnion(mismatch), deleteUnion(nil),
			}},
			wantEvicted:   []int{0, 2},
			wantNumFailed: 1,
		},
		{
			// A record that was already gone is not an eviction.
			name: "missing",
			rsp: &rfpb.BatchCmdResponse{Union: []*rfpb.ResponseUnion{
				deleteUnion(nil), missingUnion(), deleteUnion(nil),
			}},
			wantEvicted: []int{0, 2},
			wantMissing: []int{1},
		},
		{
			name: "batch status error",
			rsp: &rfpb.BatchCmdResponse{
				Union:  []*rfpb.ResponseUnion{{}, {}, {}},
				Status: gstatus.Convert(mismatch).Proto(),
			},
			wantNumFailed: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidates, keyMetas := testCandidates(3)
			c := &fakeAPIClient{rsp: &rfpb.SyncProposeResponse{Batch: tc.rsp}}

			res, err := pu.deleteBatch(context.Background(), c, &rfpb.Header{}, keyMetas)
			require.NoError(t, err)

			var wantEvicted, wantMissing []*evictionCandidate
			for _, i := range tc.wantEvicted {
				wantEvicted = append(wantEvicted, candidates[i])
			}
			for _, i := range tc.wantMissing {
				wantMissing = append(wantMissing, candidates[i])
			}
			require.Equal(t, wantEvicted, res.evicted)
			require.Equal(t, wantMissing, res.missing)
			require.Equal(t, tc.wantNumFailed, res.numFailed)
			if tc.wantNumFailed > 0 {
				require.True(t, status.IsFailedPreconditionError(res.lastErr), "lastErr: %v", res.lastErr)
			}

			// Deletes must match the candidates' atimes.
			reqs := c.req.GetBatch().GetUnion()
			require.Len(t, reqs, len(candidates))
			for i, r := range reqs {
				require.Equal(t, candidates[i].keyBytes, r.GetDelete().GetKey())
				require.Equal(t, candidates[i].atime.UnixMicro(), r.GetDelete().GetMatchAtime())
			}
		})
	}
}

func TestDeleteBatchRPCError(t *testing.T) {
	pu := &partitionUsage{}
	_, keyMetas := testCandidates(2)
	c := &fakeAPIClient{err: status.UnavailableError("replica down")}

	_, err := pu.deleteBatch(context.Background(), c, &rfpb.Header{}, keyMetas)
	require.True(t, status.IsUnavailableError(err), "err: %v", err)
}

func testMetricSet() metricSet {
	return metricSet{
		cacheEvictionAgeMsec:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "age"}),
		cacheLastEvictionAgeUsec: prometheus.NewGauge(prometheus.GaugeOpts{Name: "last_age"}),
		cacheNumEvictions:        prometheus.NewCounter(prometheus.CounterOpts{Name: "num"}),
		cacheBytesEvicted:        prometheus.NewCounter(prometheus.CounterOpts{Name: "bytes"}),
		atimeIndexBuilt:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "built"}),
		atimeIndexRepairs:        prometheus.NewCounter(prometheus.CounterOpts{Name: "repairs"}),
		atimeIndexSizeBytes:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "index_size"}),
	}
}

func TestUpdateEvictionMetricsScalesRemoteSizes(t *testing.T) {
	for _, tc := range []struct {
		name           string
		localSizeBytes int64
		wantRemote     int64
	}{
		// Evicting 10% locally shrinks the remote estimate by 10%.
		{name: "local size known", localSizeBytes: 1000, wantRemote: 450},
		// Without a local estimate, leave the remote estimate unchanged.
		{name: "local size unknown", localSizeBytes: 0, wantRemote: 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pu := &partitionUsage{
				sizeBytes: tc.localSizeBytes,
				nodes:     map[string]*nodePartitionUsage{"remote": {sizeBytes: 500}},
				metrics:   testMetricSet(),
			}
			candidates := []*evictionCandidate{{sizeBytes: 100, atime: time.Now()}}

			require.NoError(t, pu.updateEvictionMetrics(candidates))
			require.Equal(t, tc.wantRemote, pu.nodes["remote"].sizeBytes)
		})
	}
}

const testPartition = "FOO"

// newAtimeIndexTestPU's passes scan one record per chunk.
func newAtimeIndexTestPU(t *testing.T) (*partitionUsage, pebble.IPebbleDB) {
	db, err := pebble.Open(testfs.MakeTempDir(t), "test", &pebble.Options{})
	require.NoError(t, err)
	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})
	return &partitionUsage{
		part:                      disk.Partition{ID: testPartition},
		dbGetter:                  leaser,
		clock:                     clockwork.NewRealClock(),
		atimeIndexBuildRateLimit:  1_000_000,
		atimeIndexVerifyRateLimit: 1_000_000,
		metrics:                   testMetricSet(),
	}, db
}

// writeRecord writes a file record and returns the key of its index entry.
func writeRecord(t *testing.T, db pebble.IPebbleDB, atimeUsec int64, withEntry bool) []byte {
	rn, _ := testdigest.RandomCASResourceBuf(t, 100)
	pk, err := filestore.New().PebbleKey(&sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: testPartition,
		},
		Digest:         rn.GetDigest(),
		DigestFunction: rn.GetDigestFunction(),
	})
	require.NoError(t, err)
	fileKey, err := pk.Bytes(filestore.Version5)
	require.NoError(t, err)

	val, err := (&sgpb.FileMetadata{LastAccessUsec: atimeUsec}).MarshalVT()
	require.NoError(t, err)
	require.NoError(t, db.Set(fileKey, val, pebble.NoSync))
	entryKey := keys.AtimeIndexKey(testPartition, atimeUsec, fileKey)
	if withEntry {
		require.NoError(t, db.Set(entryKey, nil, pebble.NoSync))
	}
	return entryKey
}

// atimeIndexEntries returns the keys of the partition's index entries.
func atimeIndexEntries(t *testing.T, db pebble.IPebbleDB) [][]byte {
	start, end := keys.AtimeIndexPartitionRange(testPartition)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	require.NoError(t, err)
	defer iter.Close()
	var entries [][]byte
	for valid := iter.First(); valid; valid = iter.Next() {
		entries = append(entries, bytes.Clone(iter.Key()))
	}
	return entries
}

func backfillMarkerValue(t *testing.T, db pebble.IPebbleDB) []byte {
	val, closer, err := db.Get(keys.AtimeIndexBuiltKey(testPartition))
	if err == pebble.ErrNotFound {
		return nil
	}
	require.NoError(t, err)
	defer closer.Close()
	return bytes.Clone(val)
}

// A build adds entries without counting repairs, and writes the marker only
// if it completes.
func TestAtimeIndexBuild(t *testing.T) {
	for _, tc := range []struct {
		name   string
		marker []byte
	}{
		{name: "no marker"},
		{name: "marker from another version", marker: []byte{atimeIndexVersion + 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pu, db := newAtimeIndexTestPU(t)
			if tc.marker != nil {
				require.NoError(t, db.Set(keys.AtimeIndexBuiltKey(testPartition), tc.marker, pebble.NoSync))
			}
			var want [][]byte
			for i := range 5 {
				want = append(want, writeRecord(t, db, int64(1_000_000+i), i == 0 /*=withEntry*/))
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.Error(t, pu.atimeIndexPass(ctx))
			require.Equal(t, tc.marker, backfillMarkerValue(t, db))

			require.NoError(t, pu.atimeIndexPass(context.Background()))
			require.ElementsMatch(t, want, atimeIndexEntries(t, db))
			require.Equal(t, []byte{atimeIndexVersion}, backfillMarkerValue(t, db))
			require.Equal(t, 1.0, testutil.ToFloat64(pu.metrics.atimeIndexBuilt))
			require.Equal(t, 0.0, testutil.ToFloat64(pu.metrics.atimeIndexRepairs))
		})
	}
}

// On a built partition, each added entry counts as a repair.
func TestAtimeIndexVerify(t *testing.T) {
	pu, db := newAtimeIndexTestPU(t)
	require.NoError(t, db.Set(keys.AtimeIndexBuiltKey(testPartition), []byte{atimeIndexVersion}, pebble.NoSync))
	want := [][]byte{
		writeRecord(t, db, 1_000_000, true /*=withEntry*/),
		writeRecord(t, db, 2_000_000, false /*=withEntry*/),
		writeRecord(t, db, 3_000_000, true /*=withEntry*/),
	}
	// Orphans are left alone.
	orphan := keys.AtimeIndexKey(testPartition, 500_000, []byte("gone"))
	require.NoError(t, db.Set(orphan, nil, pebble.NoSync))
	want = append(want, orphan)

	// The second pass finds nothing to repair.
	for range 2 {
		require.NoError(t, pu.atimeIndexPass(context.Background()))
		require.ElementsMatch(t, want, atimeIndexEntries(t, db))
		require.Equal(t, 1.0, testutil.ToFloat64(pu.metrics.atimeIndexRepairs))
	}
}

// Only unconfigured partitions lose their atime index.
func TestDeleteStaleAtimeIndexes(t *testing.T) {
	pu, db := newAtimeIndexTestPU(t)
	set := func(key []byte) []byte {
		require.NoError(t, db.Set(key, nil, pebble.NoSync))
		return key
	}
	kept := [][]byte{
		set(keys.AtimeIndexBuiltKey(testPartition)),
		set(keys.AtimeIndexKey(testPartition, 1, []byte("a"))),
		set(keys.AtimeIndexKey(testPartition, 2, []byte("b"))),
		set(keys.AtimeIndexBuiltKey("SOFT")),
		set(keys.AtimeIndexKey("SOFT", 1, []byte("a"))),
		// Records are kept.
		set([]byte("PTBAR/a")),
	}
	// These sort before, between and after the kept partitions.
	deleted := [][]byte{
		set(keys.AtimeIndexBuiltKey("BAR")),
		set(keys.AtimeIndexKey("BAR", 1, []byte("a"))),
		set(keys.AtimeIndexKey("BAR", 2, []byte("b"))),
		// Marker only.
		set(keys.AtimeIndexBuiltKey("FOO-OLD")),
		// Entries only.
		set(keys.AtimeIndexKey("FOOD", 1, []byte("a"))),
		set(keys.AtimeIndexKey("ZZZ", 1, []byte("a"))),
	}

	partitions := []disk.Partition{{ID: testPartition}, {ID: "SOFT", SoftDeleted: true}}
	require.NoError(t, deleteStaleAtimeIndexes(pu.dbGetter, partitions))

	has := func(key []byte) bool {
		_, closer, err := db.Get(key)
		if err == pebble.ErrNotFound {
			return false
		}
		require.NoError(t, err)
		closer.Close()
		return true
	}
	for _, key := range kept {
		require.True(t, has(key), "%q was deleted", key)
	}
	for _, key := range deleted {
		require.False(t, has(key), "%q was kept", key)
	}
}
