package usagetracker

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pebblev1 "github.com/cockroachdb/pebble"

	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

// writeRecordWithIndex writes one CAS v7 file record with the given atime plus
// its atime-index entry, mirroring what the replica apply path maintains.
// Returns the record's file key.
func writeRecordWithIndex(t *testing.T, db pebble.IPebbleDB, groupID string, atimeUsec int64) []byte {
	fs := filestore.New()
	// 100 random bytes so digests don't collide across records (a collision
	// would overwrite a record and legitimately orphan its older index entry).
	rn, _ := testdigest.RandomCASResourceBuf(t, 100)
	fr := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "FOO",
			GroupId:     groupID,
		},
		Digest:         rn.GetDigest(),
		DigestFunction: rn.GetDigestFunction(),
	}
	pk, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	fileKey, err := pk.Bytes(filestore.Version7)
	require.NoError(t, err)

	md := &sgpb.FileMetadata{
		LastAccessUsec:  atimeUsec,
		StorageMetadata: &sgpb.StorageMetadata{},
	}
	val, err := md.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, db.Set(fileKey, val, &pebblev1.WriteOptions{}))
	require.NoError(t, db.Set(keys.AtimeIndexKey("FOO", atimeUsec, fileKey), nil, &pebblev1.WriteOptions{}))
	return fileKey
}

func newTestPU(t *testing.T, db pebble.IPebbleDB) *partitionUsage {
	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})
	return &partitionUsage{
		part:     disk.Partition{ID: "FOO", MaxSizeBytes: 1 << 20},
		dbGetter: leaser,
		clock:    clockwork.NewRealClock(),
		// GlobalSizeBytes (1<<40) stays above the sleep threshold
		// (0.2 * MaxSizeBytes) so the sampler never sleeps and blocks the test.
		nodes:   map[string]*nodePartitionUsage{"n1": {sizeBytes: 1 << 40}},
		samples: make(chan *approxlru.Sample[*evictionKey], 128),

		samplerSleepDuration: time.Hour,
		minEvictionAge:       0,
	}
}

// The scanner must yield this node's records coldest-first: the index is
// (partition, atime)-ordered, so a sweep from the front is an exact
// oldest-to-newest walk.
func TestIndexScannerYieldsColdestFirst(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	// Insertion order is unrelated to atime order (i*7919 mod 100 walks the
	// atimes pseudo-randomly), so ordered output proves index ordering.
	const n = 100
	written := map[int64]bool{}
	for i := 0; i < n; i++ {
		atime := int64(1_000_000 + (i*7919)%100)
		writeRecordWithIndex(t, db, "GR1", atime)
		written[atime] = true
	}
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pu.generateSamplesForEviction(ctx)
		close(done)
	}()

	var got []int64
	for i := 0; i < n; i++ {
		select {
		case s := <-pu.samples:
			got = append(got, s.Timestamp.UnixMicro())
		case <-time.After(30 * time.Second):
			cancel()
			<-done
			t.Fatalf("timed out after %d/%d samples", i, n)
		}
	}
	cancel()
	<-done

	require.Len(t, got, n)
	for i := 1; i < n; i++ {
		assert.LessOrEqual(t, got[i-1], got[i], "samples not in ascending atime order")
	}
	for _, atime := range got {
		assert.True(t, written[atime], "sample atime %d was never written", atime)
	}
}

// Orphaned index entries -- the record is gone, or its atime moved on (e.g. a
// blind Set overwrite) -- must be dropped in place and never offered for
// eviction.
func TestIndexScannerDropsOrphans(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	valid := writeRecordWithIndex(t, db, "GR1", 2_000_000)

	// Orphan 1: index entry whose record does not exist.
	missing := append([]byte(nil), valid...)
	missing[len(missing)-4] = 'x' // corrupt the digest portion
	missingEntry := keys.AtimeIndexKey("FOO", 1_000_000, missing)
	require.NoError(t, db.Set(missingEntry, nil, &pebblev1.WriteOptions{}))

	// Orphan 2: stale index entry at the record's old atime.
	staleEntry := keys.AtimeIndexKey("FOO", 1_500_000, valid)
	require.NoError(t, db.Set(staleEntry, nil, &pebblev1.WriteOptions{}))
	require.NoError(t, db.Flush())

	pu := newTestPU(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pu.generateSamplesForEviction(ctx)
		close(done)
	}()

	// Orphans sort before the valid entry, so the first sample proves both
	// were skipped.
	select {
	case s := <-pu.samples:
		assert.Equal(t, int64(2_000_000), s.Timestamp.UnixMicro())
		assert.Equal(t, string(valid), string(s.Key.bytes))
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("timed out waiting for sample")
	}
	cancel()
	<-done

	// Both orphaned entries were deleted from the index.
	for _, k := range [][]byte{missingEntry, staleEntry} {
		_, closer, err := db.Get(k)
		if err == nil {
			closer.Close()
		}
		assert.ErrorIs(t, err, pebblev1.ErrNotFound, "orphaned index entry %q still present", k)
	}
}
