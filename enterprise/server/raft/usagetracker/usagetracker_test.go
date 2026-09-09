package usagetracker

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pebblev1 "github.com/cockroachdb/pebble"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func TestRandomKeyInRange(t *testing.T) {
	const draws = 2000

	// Single-group range: bounds share PT/GR<group>/ and differ in the digest.
	// randomKeyInRange should spread across the digest span (not stick to the
	// head), because the first differing byte is dense/uniform hex.
	t.Run("single_group_spreads_over_digests", func(t *testing.T) {
		gp := []byte("PTFOO/" + filestore.FixedWidthGroupID("GR1") + "/")
		start := append(append([]byte(nil), gp...), []byte("20000000")...)
		end := append(append([]byte(nil), gp...), []byte("80000000")...)
		lo, hi := byte(0xff), byte(0x00)
		for i := 0; i < draws; i++ {
			k := randomKeyInRange(start, end)
			require.True(t, bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) < 0,
				"key %q out of [%q, %q)", k, start, end)
			// The byte just after the group prefix is the randomized digest nibble.
			b := k[len(gp)]
			if b < lo {
				lo = b
			}
			if b > hi {
				hi = b
			}
		}
		// Over 2000 draws it should cover most of ['2','8'], not a single value.
		assert.Less(t, lo, byte('4'), "low end of digest range never sampled")
		assert.Greater(t, hi, byte('6'), "high end of digest range never sampled")
	})

	// Narrow range (bounds differ only in the last byte, digest sub-slot 0..4):
	// every draw must stay strictly in [start, end) and spread across the slots,
	// not collapse to the head the way clamping out-of-range draws would.
	t.Run("narrow_range_stays_in_bounds", func(t *testing.T) {
		gp := []byte("PTFOO/" + filestore.FixedWidthGroupID("GR1") + "/")
		start := append(append([]byte(nil), gp...), []byte("abcd000000000000000000000000000000000000000000000000000000000000")...)
		end := append(append([]byte(nil), gp...), []byte("abcd000000000000000000000000000000000000000000000000000000000005")...)
		seen := map[string]bool{}
		for i := 0; i < draws; i++ {
			k := randomKeyInRange(start, end)
			require.True(t, bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) < 0,
				"key %q out of [%q, %q)", k, start, end)
			seen[string(k)] = true
		}
		assert.Greater(t, len(seen), 1, "narrow range collapsed to a single key (head bias)")
	})

	// Multi-group range: bounds differ in the group field. Keys stay within the
	// group span and vary.
	t.Run("multi_group_stays_in_group_span", func(t *testing.T) {
		// Full group field is "PTFOO/" + 22-char GR id = 28 bytes.
		groupFieldLen := len("PTFOO/") + len(filestore.FixedWidthGroupID("GR1"))
		start := []byte("PTFOO/" + filestore.FixedWidthGroupID("GR1") + "/00000000")
		end := []byte("PTFOO/" + filestore.FixedWidthGroupID("GR9") + "/ffffffff")
		seen := map[string]bool{}
		for i := 0; i < draws; i++ {
			k := randomKeyInRange(start, end)
			require.True(t, bytes.HasPrefix(k, []byte("PTFOO/GR")), "key %q lost the group prefix", k)
			require.GreaterOrEqual(t, len(k), groupFieldLen)
			seen[string(k[:groupFieldLen])] = true
		}
		assert.Greater(t, len(seen), 1, "group field never varied")
	})

	// Degenerate bounds must not panic and fall back to ~start.
	t.Run("degenerate_bounds", func(t *testing.T) {
		assert.NotPanics(t, func() { randomKeyInRange([]byte("PTx/"), []byte("PTx/")) })
		assert.NotPanics(t, func() { randomKeyInRange([]byte("PTx/abc"), []byte("PTx/")) })
		// end is a prefix of start → return start unchanged.
		assert.Equal(t, "PTx/abc", string(randomKeyInRange([]byte("PTx/abc"), []byte("PTx/ab"))))
	})
}

// writeKeys writes n CAS v7 keys under the given group and flushes. Range-scoped
// sampling is layout-independent, but the sampled keys must still parse as
// PebbleKeys, so we write real keys with random digests.
func writeKeys(t *testing.T, dir, groupID string, n int) pebble.IPebbleDB {
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)

	fs := filestore.New()
	md := &sgpb.FileMetadata{
		LastAccessUsec:  1_000_000, // far in the past; passes minEvictionAge=0
		StorageMetadata: &sgpb.StorageMetadata{},
	}
	val, err := md.MarshalVT()
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		rn, _ := testdigest.RandomCASResourceBuf(t, 1)
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
		keyBytes, err := pk.Bytes(filestore.Version7)
		require.NoError(t, err)
		require.NoError(t, db.Set(keyBytes, val, &pebblev1.WriteOptions{}))
	}
	require.NoError(t, db.Flush())
	return db
}

type fakeStore struct{ rds []*rfpb.RangeDescriptor }

// All test ranges are in partition FOO, so the partition filter is a no-op here.
func (f *fakeStore) RandomOpenRangeDescriptor(partitionID string) *rfpb.RangeDescriptor {
	if len(f.rds) == 0 {
		return nil
	}
	return f.rds[rand.Intn(len(f.rds))]
}

func (f *fakeStore) HostedRangeIDs(partitionID string) map[uint64]struct{} {
	ids := make(map[uint64]struct{}, len(f.rds))
	for _, rd := range f.rds {
		ids[rd.GetRangeId()] = struct{}{}
	}
	return ids
}

func newRangeTestPU(t *testing.T, db pebble.IPebbleDB, store IStore, samplesPerRange int) *partitionUsage {
	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})
	return &partitionUsage{
		part:     disk.Partition{ID: "FOO", MaxSizeBytes: 1 << 20},
		store:    store,
		dbGetter: leaser,
		clock:    clockwork.NewRealClock(),
		// GlobalSizeBytes (1<<40) stays above the sleep threshold
		// (0.2 * MaxSizeBytes) so the sampler never sleeps and blocks the test.
		nodes:   map[string]*nodePartitionUsage{"n1": {sizeBytes: 1 << 40}},
		cursors: make(map[uint64][]byte),
		samples: make(chan *approxlru.Sample[*evictionKey], 128),

		samplesPerRange:          samplesPerRange,
		samplerIterRefreshPeriod: time.Hour, // keys are written before sampling; no refresh needed
		samplerSleepDuration:     time.Hour,
		minEvictionAge:           0,
	}
}

func rangeOf(ranges []*rfpb.RangeDescriptor, key []byte) uint64 {
	for _, rd := range ranges {
		if bytes.Compare(key, rd.GetStart()) >= 0 && bytes.Compare(key, rd.GetEnd()) < 0 {
			return rd.GetRangeId()
		}
	}
	return 0
}

// End-to-end: uniform range selection should give each range an ~equal share of
// samples, regardless of how many keys each range holds. (In production ranges
// are size-balanced by the splitter, so equal-per-range ~= uniform per key.)
func TestRangeScopedSamplingIsUniformOverRanges(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	// One group, keys spread across the hash space; three ranges partition it.
	// Deliberately unequal key counts per range (first-hash-char buckets are
	// 0-5, 6-a, b-f) to show the share depends on range selection, not size.
	db := writeKeys(t, dir, "GR1", 6000)
	gp := "PTFOO/" + filestore.FixedWidthGroupID("GR1") + "/"
	ranges := []*rfpb.RangeDescriptor{
		{RangeId: 1, Start: []byte(gp + "0"), End: []byte(gp + "6")},
		{RangeId: 2, Start: []byte(gp + "6"), End: []byte(gp + "b")},
		{RangeId: 3, Start: []byte(gp + "b"), End: []byte(gp + "g")},
	}
	// samplesPerRange small (< every range's key count) so each visit yields a
	// fixed number of samples and the per-range share reflects range selection.
	pu := newRangeTestPU(t, db, &fakeStore{rds: ranges}, 2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pu.generateSamplesForEviction(ctx)
		close(done)
	}()

	const want = 30000
	counts := map[uint64]int{}
	for i := 0; i < want; i++ {
		select {
		case s := <-pu.samples:
			rid := rangeOf(ranges, s.Key.bytes)
			require.NotZerof(t, rid, "sampled key %q fell outside all ranges", s.Key.bytes)
			counts[rid]++
		case <-time.After(30 * time.Second):
			cancel()
			<-done
			t.Fatalf("timed out after %d/%d samples", i, want)
		}
	}
	cancel()
	<-done

	for _, rd := range ranges {
		got := float64(counts[rd.GetRangeId()]) / float64(want)
		assert.InDeltaf(t, 1.0/3.0, got, 0.04, "range %d share (got=%.3f)", rd.GetRangeId(), got)
	}
}
