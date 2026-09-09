package usagetracker

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	pebblev1 "github.com/cockroachdb/pebble"

	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

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

		evictionRateLimit: 1_000_000_000, // don't rate-limit unit tests
		idleSleepDuration: time.Hour,
		minEvictionAge:    minEvictionAge,

		metrics: metricSet{
			atimeIndexSweepSeek:      metrics.RaftAtimeIndexSweepSeekDurationUsec.With(prometheus.Labels{metrics.PartitionID: "FOO"}),
			atimeIndexOrphansDropped: metrics.RaftAtimeIndexOrphansDropped.With(prometheus.Labels{metrics.PartitionID: "FOO"}),
		},
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
	for i := 0; i < n; i++ {
		atime := int64(1_000_000 + (i*7919)%100)
		writeRecord(t, db, atime, true /*=withIndex*/)
		written[atime] = true
	}
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40 /*=global size, over budget*/, 0 /*=minEvictionAge*/)
	startEvictionLoopForTest(t, pu)

	var got []int64
	for i := 0; i < n; i++ {
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
	for i := 0; i < 10; i++ {
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
	for i := 0; i < n; i++ {
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
	for i := 0; i < 5; i++ {
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

// A sweep that reaches the age boundary while the partition stays over budget
// must keep its cursor rather than restarting from the front: the candidates
// it already enqueued have deletes still in flight (never applied here, since
// no delete pipeline runs), and a front restart would re-enqueue all of them.
// Regression test for
// https://github.com/buildbuddy-io/buildbuddy/pull/12877#discussion_r3678539378.
func TestNoReEnqueueWhileDeletesInFlight(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	const n = 20
	for i := 0; i < n; i++ {
		writeRecord(t, db, time.Now().Add(-2*time.Hour).UnixMicro(), true /*=withIndex*/)
	}
	// One fresh record so the sweep stops at the age boundary.
	writeRecord(t, db, time.Now().UnixMicro(), true /*=withIndex*/)
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 1<<40, time.Hour)
	// Disable sleeping so the loop spins: a front-restarting loop would
	// re-offer the same candidates many times within the test window.
	pu.idleSleepDuration = 0
	startEvictionLoopForTest(t, pu)

	seen := map[string]int{}
	total := 0
	deadline := time.After(1 * time.Second)
drain:
	for {
		select {
		case c := <-pu.deletes:
			seen[string(c.keyBytes)]++
			total++
		case <-deadline:
			break drain
		}
	}
	require.Equal(t, n, total, "each eligible record must be offered exactly once")
	for key, count := range seen {
		require.Equal(t, 1, count, "candidate %q re-enqueued", key)
	}
}

// Index state of partitions no longer in the config -- entries (including
// orphans the hard-delete flow can't derive from records) and the backfill
// marker -- must be wiped at startup; configured partitions' state stays.
func TestCleanupStaleAtimeIndexState(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)
	defer db.Close()

	// Configured partition: a record with its entry, plus the marker.
	fooKey := writeRecord(t, db, 1_000_000, true /*=withIndex*/)
	fooEntry := keys.AtimeIndexKey("FOO", 1_000_000, fooKey)
	fooMarker := keys.AtimeIndexBackfillMarkerKey("FOO")
	require.NoError(t, db.Set(fooMarker, []byte{atimeIndexVersion}, &pebblev1.WriteOptions{}))

	// Stale partition: orphaned entries (no records) plus a marker.
	goneEntry := keys.AtimeIndexKey("GONE", 1_000_000, []byte("PTGONE/orphan"))
	goneMarker := keys.AtimeIndexBackfillMarkerKey("GONE")
	require.NoError(t, db.Set(goneEntry, nil, &pebblev1.WriteOptions{}))
	require.NoError(t, db.Set(goneMarker, []byte{atimeIndexVersion}, &pebblev1.WriteOptions{}))

	// Stale partition with a marker but no entries.
	emptyMarker := keys.AtimeIndexBackfillMarkerKey("EMPTY")
	require.NoError(t, db.Set(emptyMarker, []byte{atimeIndexVersion}, &pebblev1.WriteOptions{}))

	require.NoError(t, cleanupStaleAtimeIndexState(db, set.From("FOO")))

	for _, k := range [][]byte{fooEntry, fooMarker} {
		_, closer, err := db.Get(k)
		require.NoError(t, err, "configured partition state %q must survive", k)
		closer.Close()
	}
	for _, k := range [][]byte{goneEntry, goneMarker, emptyMarker} {
		_, closer, err := db.Get(k)
		if err == nil {
			closer.Close()
		}
		require.ErrorIs(t, err, pebblev1.ErrNotFound, "stale state %q must be removed", k)
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
	for i := 0; i < n; i++ {
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

// The background verifier must repair exactly the records whose index entry is
// missing (the violation that would otherwise leave a record permanently
// unevictable), and must not touch consistent records or sweep-owned orphans.
func TestAtimeIndexVerifierRepairsMissingEntries(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	// Normal records: entry present.
	for i := 0; i < 5; i++ {
		writeRecord(t, db, int64(1_000_000+i), true /*=withIndex*/)
	}
	// The violation: a record with no index entry.
	missingKey := writeRecord(t, db, 2_000_000, false /*=withIndex*/)
	// Decoy: a consistent record that also has a stale orphan entry at an old
	// atime. The verifier must leave the orphan alone (dropping orphans is the
	// sweep's job) and must not count the record as needing repair.
	decoyKey := writeRecord(t, db, 3_000_000, true /*=withIndex*/)
	orphanEntry := keys.AtimeIndexKey("FOO", 2_999_999, decoyKey)
	require.NoError(t, db.Set(orphanEntry, nil, &pebblev1.WriteOptions{}))
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db, 0, time.Hour)
	pu.metrics.atimeIndexRepairs = metrics.RaftAtimeIndexMissingEntriesRepaired.With(prometheus.Labels{metrics.PartitionID: "FOO"})

	// Force the pass to reopen its iterator every few records, so chunk
	// boundaries are exercised.
	prev := verifyChunkRecords
	verifyChunkRecords = 2
	defer func() { verifyChunkRecords = prev }()

	limiter := rate.NewLimiter(rate.Inf, 1)
	repaired, err := pu.verifyAtimeIndexPass(context.Background(), limiter)
	require.NoError(t, err)
	require.Equal(t, 1, repaired)

	// The missing entry now exists, and the orphan was left in place.
	for _, k := range [][]byte{keys.AtimeIndexKey("FOO", 2_000_000, missingKey), orphanEntry} {
		_, closer, err := db.Get(k)
		require.NoError(t, err, "index entry %q missing after verify pass", k)
		closer.Close()
	}

	// A second pass finds nothing to do.
	repaired, err = pu.verifyAtimeIndexPass(context.Background(), limiter)
	require.NoError(t, err)
	require.Equal(t, 0, repaired)
}
