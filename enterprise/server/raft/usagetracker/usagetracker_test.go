package usagetracker

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pebblev1 "github.com/cockroachdb/pebble"

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

			var wantEvicted []*evictionCandidate
			for _, i := range tc.wantEvicted {
				wantEvicted = append(wantEvicted, candidates[i])
			}
			require.Equal(t, wantEvicted, res.evicted)
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

// writeRecord writes one CAS file record with the given atime; withIndex also
// writes its atime-index entry, mirroring what the replica apply path
// maintains. Returns the record's file key.
func writeRecord(t testing.TB, db pebble.IPebbleDB, atimeUsec int64, withIndex bool) []byte {
	fs := filestore.New()
	// 100 random bytes so digests don't collide across records (a collision
	// would overwrite a record and orphan its older index entry).
	rn, _ := testdigest.RandomCASResourceBuf(t, 100)
	fr := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "FOO",
		},
		Digest:         rn.GetDigest(),
		DigestFunction: rn.GetDigestFunction(),
	}
	pk, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	fileKey, err := pk.Bytes(filestore.Version5)
	require.NoError(t, err)

	md := &sgpb.FileMetadata{
		LastAccessUsec:  atimeUsec,
		StorageMetadata: &sgpb.StorageMetadata{},
	}
	val, err := md.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, db.Set(fileKey, val, &pebblev1.WriteOptions{}))
	if withIndex {
		require.NoError(t, db.Set(keys.AtimeIndexKey("FOO", atimeUsec, fileKey), nil, &pebblev1.WriteOptions{}))
	}
	return fileKey
}

// newTestPU builds a partitionUsage whose eviction gate is controlled through
// globalSizeBytes (the eviction threshold is 0.9 * 1MiB).
func newTestPU(t *testing.T, db pebble.IPebbleDB, globalSizeBytes int64, minEvictionAge time.Duration) *partitionUsage {
	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})
	return &partitionUsage{
		part:     disk.Partition{ID: "FOO", MaxSizeBytes: 1 << 20},
		dbGetter: leaser,
		clock:    clockwork.NewRealClock(),
		nodes:    map[string]*nodePartitionUsage{"n1": {sizeBytes: globalSizeBytes}},
		deletes:  make(chan *evictionCandidate, 128),

		evictionRateLimit:    1_000_000_000, // don't rate-limit unit tests
		samplerSleepDuration: time.Hour,
		minEvictionAge:       minEvictionAge,
	}
}

func (pu *partitionUsage) setGlobalSizeForTest(sizeBytes int64) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	pu.nodes["n1"].sizeBytes = sizeBytes
}

func startEvictionLoopForTest(t *testing.T, pu *partitionUsage) (context.CancelFunc, chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pu.evictionLoop(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel, done
}

// Over budget, the eviction loop must emit this node's records coldest-first:
// the index is (partition, atime)-ordered, so the sweep is an exact
// oldest-to-newest walk.
func TestEvictionColdestFirst(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	// Insertion order is unrelated to atime order (i*7919 mod 100 walks the
	// atimes pseudo-randomly), so ordered output proves index ordering.
	const n = 100
	written := map[int64]bool{}
	for i := range n {
		atime := int64(1_000_000 + (i*7919)%100)
		writeRecord(t, db, atime, true /*=withIndex*/)
		written[atime] = true
	}
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40 /*=global size, over budget*/, 0 /*=minEvictionAge*/)
	startEvictionLoopForTest(t, pu)

	var got []int64
	for i := range n {
		select {
		case c := <-pu.deletes:
			got = append(got, c.atime.UnixMicro())
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out after %d/%d candidates", i, n)
		}
	}
	for i := 1; i < n; i++ {
		assert.LessOrEqual(t, got[i-1], got[i], "candidates not in ascending atime order")
	}
	seen := map[int64]bool{}
	for _, atime := range got {
		assert.True(t, written[atime], "candidate atime %d was never written", atime)
		assert.False(t, seen[atime], "candidate atime %d offered twice", atime)
		seen[atime] = true
	}
	// After a full sweep the loop must sleep (1h here) rather than restart at
	// the front and re-enqueue candidates whose deletes are still in flight.
	select {
	case c := <-pu.deletes:
		t.Fatalf("candidate re-offered after a full sweep: %q", c.keyBytes)
	case <-time.After(300 * time.Millisecond):
	}
}

// Orphaned index entries -- the record is gone, or its atime moved on -- must
// be dropped in place and never offered for eviction.
func TestEvictionDropsOrphans(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	valid := writeRecord(t, db, 2_000_000, true /*=withIndex*/)

	// Orphan 1: index entry whose record does not exist.
	missing := append([]byte(nil), valid...)
	missing[len(missing)-4] = 'x' // corrupt the digest portion
	missingEntry := keys.AtimeIndexKey("FOO", 1_000_000, missing)
	require.NoError(t, db.Set(missingEntry, nil, &pebblev1.WriteOptions{}))

	// Orphan 2: stale index entry at the record's old atime.
	staleEntry := keys.AtimeIndexKey("FOO", 1_500_000, valid)
	require.NoError(t, db.Set(staleEntry, nil, &pebblev1.WriteOptions{}))
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40, 0)
	startEvictionLoopForTest(t, pu)

	// Orphans sort before the valid entry, so the first candidate proves both
	// were skipped.
	select {
	case c := <-pu.deletes:
		assert.Equal(t, int64(2_000_000), c.atime.UnixMicro())
		assert.Equal(t, string(valid), string(c.keyBytes))
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for candidate")
	}

	// Both orphaned entries were deleted from the index.
	for _, k := range [][]byte{missingEntry, staleEntry} {
		_, closer, err := db.Get(k)
		if err == nil {
			closer.Close()
		}
		assert.ErrorIs(t, err, pebblev1.ErrNotFound, "orphaned index entry %q still present", k)
	}
}

// Below the eviction threshold nothing may be offered, even with old records
// present.
func TestNoEvictionBelowThreshold(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)
	for i := range 10 {
		writeRecord(t, db, int64(1_000_000+i), true /*=withIndex*/)
	}
	require.NoError(t, db.Flush())

	// Global size (80% of MaxSizeBytes) just under the 90% eviction cutoff.
	pu := newTestPU(t, db, (1<<20)*8/10, 0)
	startEvictionLoopForTest(t, pu)

	select {
	case c := <-pu.deletes:
		t.Fatalf("unexpected eviction candidate below threshold: %q", c.keyBytes)
	case <-time.After(300 * time.Millisecond):
	}
}

// Records younger than min_eviction_age must not be offered even when the
// partition is over budget.
func TestEvictionRespectsMinEvictionAge(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)
	old := writeRecord(t, db, time.Now().Add(-2*time.Hour).UnixMicro(), true /*=withIndex*/)
	writeRecord(t, db, time.Now().UnixMicro(), true /*=withIndex*/)
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40, time.Hour)
	startEvictionLoopForTest(t, pu)

	// Only the old record is offered, exactly once: the sweep stops at the
	// age boundary and the loop then sleeps (1h here) instead of restarting
	// at the front and re-enqueueing the in-flight candidate.
	select {
	case c := <-pu.deletes:
		require.Equal(t, string(old), string(c.keyBytes))
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the old record")
	}
	select {
	case c := <-pu.deletes:
		t.Fatalf("unexpected second candidate %q (fresh record, or in-flight re-offer)", c.keyBytes)
	case <-time.After(300 * time.Millisecond):
	}
}

// The eviction loop must stop offering candidates once the global size drops
// below the threshold.
func TestEvictionStopsBelowThreshold(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)
	const n = 50
	for i := range n {
		writeRecord(t, db, int64(1_000_000+i), true /*=withIndex*/)
	}
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40, 0)
	// A tiny channel keeps the sweep backpressured, so only ~2 candidates can
	// be in flight when the size drops -- otherwise the whole index would
	// already be buffered and the assertion would be vacuous.
	pu.deletes = make(chan *evictionCandidate, 1)
	startEvictionLoopForTest(t, pu)

	// Consume a few candidates, then drop the size below the cutoff.
	for i := range 5 {
		select {
		case <-pu.deletes:
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out after %d candidates", i)
		}
	}
	pu.setGlobalSizeForTest(1)

	// Drain what was already in flight (the sweep re-checks the size once per
	// entry, and the channel buffers), then expect silence.
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case <-pu.deletes:
		case <-deadline:
			break drain
		}
	}
	select {
	case c := <-pu.deletes:
		t.Fatalf("candidate offered after size dropped below threshold: %q", c.keyBytes)
	case <-time.After(300 * time.Millisecond):
	}
}

// backfillAtimeIndex must create entries for pre-existing records exactly
// once: the completion marker makes later startups skip the scan.
func TestBackfill(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	// Force the backfill to commit in multiple chunks.
	prev := backfillCommitSizeBytes
	backfillCommitSizeBytes = 64
	defer func() { backfillCommitSizeBytes = prev }()

	const n = 20
	fileKeys := make(map[string]int64, n)
	for i := range n {
		atime := int64(1_000_000 + i)
		fk := writeRecord(t, db, atime, false /*=withIndex*/)
		fileKeys[string(fk)] = atime
	}
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40, 0)
	require.NoError(t, pu.backfillAtimeIndex(context.Background()))

	// Every record got an index entry at its stored atime.
	start, end := keys.AtimeIndexPartitionRange("FOO")
	iter, err := db.NewIter(&pebblev1.IterOptions{LowerBound: start, UpperBound: end})
	require.NoError(t, err)
	entries := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		_, atime, fileKey, err := keys.ParseAtimeIndexKey(iter.Key())
		require.NoError(t, err)
		require.Equal(t, fileKeys[string(fileKey)], atime)
		entries++
	}
	iter.Close()
	require.Equal(t, n, entries)

	// A second run is a no-op: the marker short-circuits it, so a record
	// written without an index entry stays unindexed.
	writeRecord(t, db, 42, false /*=withIndex*/)
	require.NoError(t, pu.backfillAtimeIndex(context.Background()))
	iter, err = db.NewIter(&pebblev1.IterOptions{LowerBound: start, UpperBound: end})
	require.NoError(t, err)
	entries = 0
	for valid := iter.First(); valid; valid = iter.Next() {
		entries++
	}
	iter.Close()
	require.Equal(t, n, entries, "backfill ran again despite the completion marker")

	// A marker with a stale index version forces a full wipe and rebuild,
	// which picks up the record the previous run skipped.
	markerKey := keys.AtimeIndexBackfillMarkerKey("FOO")
	require.NoError(t, db.Set(markerKey, []byte{atimeIndexVersion - 1}, &pebblev1.WriteOptions{}))
	require.NoError(t, pu.backfillAtimeIndex(context.Background()))
	iter, err = db.NewIter(&pebblev1.IterOptions{LowerBound: start, UpperBound: end})
	require.NoError(t, err)
	entries = 0
	for valid := iter.First(); valid; valid = iter.Next() {
		entries++
	}
	iter.Close()
	require.Equal(t, n+1, entries, "version bump did not rebuild the index")
	val, closer, err := db.Get(markerKey)
	require.NoError(t, err)
	require.Equal(t, []byte{atimeIndexVersion}, val)
	closer.Close()
}
