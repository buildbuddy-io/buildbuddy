package posting_test

import (
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	pl := posting.NewList()
	pl.Add(3)
	pl.Add(3)
	pl.Add(3)
	pl.Add(1)
	pl.Add(2)
	pl.Add(2)

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
}

func TestOr(t *testing.T) {
	pl := posting.NewList()
	pl.Add(1)
	pl.Add(2)

	pl2 := posting.NewList()
	pl2.Add(3)
	pl2.Add(4)

	pl.Or(pl2)
	assert.Equal(t, []uint64{1, 2, 3, 4}, pl.ToArray())
}

func TestAnd(t *testing.T) {
	pl := posting.NewList()
	pl.Add(1)
	pl.Add(2)
	pl.Add(3)

	pl2 := posting.NewList()
	pl2.Add(3)
	pl2.Add(4)
	pl2.Add(5)

	pl.And(pl2)
	assert.Equal(t, []uint64{3}, pl.ToArray())
}

func TestAndNot(t *testing.T) {
	pl := posting.NewList()
	pl.Add(1)
	pl.Add(2)
	pl.Add(3)

	pl2 := posting.NewList()
	pl2.Add(3)
	pl2.Add(4)
	pl2.Add(5)

	pl.AndNot(pl2)
	assert.Equal(t, []uint64{1, 2}, pl.ToArray())
}

func TestRemove(t *testing.T) {
	pl := posting.NewList()
	pl.Add(1)
	pl.Add(2)
	pl.Add(3)
	pl.Add(4)
	pl.Add(5)
	pl.Remove(3)

	assert.Equal(t, []uint64{1, 2, 4, 5}, pl.ToArray())
}

func TestAddMany(t *testing.T) {
	pl := posting.NewList(3, 4, 5)
	pl.Add(1)
	pl.Add(2)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
}

func TestBuilderList(t *testing.T) {
	pl := posting.NewBuilderList()
	pl.Add(1)
	pl.Add(2)
	pl.Add(3)

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
}

func TestBuilderListPanicsOnNonIncreasingAdd(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3)
	assert.Panics(t, func() { pl.Add(3) }, "duplicate trailing id should panic")
	assert.Panics(t, func() { pl.Add(2) }, "out-of-order id should panic")
}

func TestBuilderListFrequencyDecodesRLE(t *testing.T) {
	pl := posting.NewBuilderList()
	pl.AddWithFrequency(1, 1)
	pl.AddWithFrequency(2, 3)
	pl.AddWithFrequency(3, 3) // same run as id 2
	pl.AddWithFrequency(4, 1)

	assert.Equal(t, uint32(1), pl.Frequency(1))
	assert.Equal(t, uint32(3), pl.Frequency(2))
	assert.Equal(t, uint32(3), pl.Frequency(3))
	assert.Equal(t, uint32(1), pl.Frequency(4)) // in the not-yet-flushed final run
	assert.Equal(t, uint32(0), pl.Frequency(99))
}

func TestBuilderListRemovePreservesFrequencies(t *testing.T) {
	pl := posting.NewBuilderList()
	pl.AddWithFrequency(1, 2)
	pl.AddWithFrequency(2, 1)
	pl.AddWithFrequency(3, 5)
	pl.AddWithFrequency(4, 1)

	pl.Remove(2)

	assert.Equal(t, []uint64{1, 3, 4}, pl.ToArray())
	assert.Equal(t, uint32(2), pl.Frequency(1))
	assert.Equal(t, uint32(5), pl.Frequency(3))
	assert.Equal(t, uint32(1), pl.Frequency(4))
	assert.Equal(t, uint32(0), pl.Frequency(2))

	// Round-trips correctly after the decode-and-rebuild.
	buf, err := pl.Marshal()
	require.NoError(t, err)
	pl2, err := posting.Unmarshal(buf)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 3, 4}, pl2.ToArray())
	assert.Equal(t, uint32(5), pl2.Frequency(3))
}

func TestMarshal(t *testing.T) {
	pl := posting.NewList(1, 2, 3, 4, 5)
	buf, err := pl.Marshal()
	assert.NoError(t, err)

	pl2, err := posting.Unmarshal(buf)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl2.ToArray())
}

func TestBuilderListMarshal(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3, 4, 5, 4294967296, 4294967297)
	buf, err := pl.Marshal()
	assert.NoError(t, err)

	pl2, err := posting.Unmarshal(buf)
	assert.NoError(t, err)

	pl3, err := posting.UnmarshalReadOnly(buf)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 4294967296, 4294967297}, pl.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 4294967296, 4294967297}, pl2.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 4294967296, 4294967297}, pl3.ToArray())
}

func TestBuilderListMarshalAlternatingFrequencies(t *testing.T) {
	// Each doc is its own RLE run since adjacent TFs all differ. Also
	// exercises a multi-byte uvarint value (129 needs 2 bytes).
	pl := posting.NewBuilderList()
	pl.AddWithFrequency(1, 2)
	pl.AddWithFrequency(2, 1)
	pl.AddWithFrequency(3, 129)
	buf, err := pl.Marshal()
	require.NoError(t, err)

	pl2, err := posting.Unmarshal(buf)
	require.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3}, pl2.ToArray())
	assert.Equal(t, uint32(2), pl2.Frequency(1))
	assert.Equal(t, uint32(1), pl2.Frequency(2))
	assert.Equal(t, uint32(129), pl2.Frequency(3))

	pl3, err := posting.UnmarshalReadOnly(buf)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), pl3.Frequency(1))
	assert.Equal(t, uint32(1), pl3.Frequency(2))
	assert.Equal(t, uint32(129), pl3.Frequency(3))
}

func TestBuilderListMarshalLongRunWithOutlier(t *testing.T) {
	// 15 docs with TF=1 followed by one doc with TF=4 — RLE compresses this
	// to two (runlen, value) pairs: (15, 1) and (1, 4) = 4 varint bytes.
	pl := posting.NewBuilderList()
	for i := uint64(1); i <= 15; i++ {
		pl.AddWithFrequency(i, 1)
	}
	pl.AddWithFrequency(16, 4)
	buf, err := pl.Marshal()
	require.NoError(t, err)

	pl2, err := posting.Unmarshal(buf)
	require.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, pl2.ToArray())
	assert.Equal(t, uint32(1), pl2.Frequency(1))
	assert.Equal(t, uint32(4), pl2.Frequency(16))

	pl3, err := posting.UnmarshalReadOnly(buf)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), pl3.Frequency(1))
	assert.Equal(t, uint32(4), pl3.Frequency(16))
}

func TestMergeListOrAddsFrequencies(t *testing.T) {
	a := posting.NewBuilderList()
	a.AddWithFrequency(1, 2)
	a.AddWithFrequency(2, 3)
	b := posting.NewBuilderList()
	b.AddWithFrequency(2, 4)
	b.AddWithFrequency(3, 5)

	pl := posting.NewMergeList()
	pl.Or(a)
	pl.Or(b)

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
	assert.Equal(t, uint32(2), pl.Frequency(1))
	assert.Equal(t, uint32(7), pl.Frequency(2))
	assert.Equal(t, uint32(5), pl.Frequency(3))
}

func TestMergeListRemovePreservesFrequencies(t *testing.T) {
	src := posting.NewBuilderList()
	src.AddWithFrequency(1, 2)
	src.AddWithFrequency(2, 3)
	src.AddWithFrequency(3, 4)
	pl := posting.NewMergeList()
	pl.Or(src)

	pl.Remove(2)

	assert.Equal(t, []uint64{1, 3}, pl.ToArray())
	assert.Equal(t, uint32(2), pl.Frequency(1))
	assert.Equal(t, uint32(4), pl.Frequency(3))
	assert.Equal(t, uint32(0), pl.Frequency(2))
}

// TestMergeListUnmarshalThenRemove exercises the delete-compaction path:
// unmarshal a counted list, then Remove a doc, preserving survivors' TFs.
func TestMergeListUnmarshalThenRemove(t *testing.T) {
	src := posting.NewBuilderList()
	src.AddWithFrequency(1, 2)
	src.AddWithFrequency(2, 3)
	src.AddWithFrequency(3, 4)
	buf, err := src.Marshal()
	require.NoError(t, err)

	pl, err := posting.Unmarshal(buf)
	require.NoError(t, err)
	pl.Remove(2)

	assert.Equal(t, []uint64{1, 3}, pl.ToArray())
	assert.Equal(t, uint32(2), pl.Frequency(1))
	assert.Equal(t, uint32(4), pl.Frequency(3))
	assert.Equal(t, uint32(0), pl.Frequency(2))
}

func TestCountedReadOnlyListFrequencySparseIDs(t *testing.T) {
	// Use sparse, non-contiguous IDs to exercise Rank rather than a tight
	// iteration that would happen to match a sequential scan.
	pl := posting.NewBuilderList()
	pl.AddWithFrequency(10, 5)
	pl.AddWithFrequency(1000, 7)
	pl.AddWithFrequency(1<<32, 9)
	pl.AddWithFrequency(1<<40, 11)
	buf, err := pl.Marshal()
	require.NoError(t, err)

	roList, err := posting.UnmarshalReadOnly(buf)
	require.NoError(t, err)

	assert.Equal(t, uint32(5), roList.Frequency(10))
	assert.Equal(t, uint32(7), roList.Frequency(1000))
	assert.Equal(t, uint32(9), roList.Frequency(1<<32))
	assert.Equal(t, uint32(11), roList.Frequency(1<<40))
	// Missing IDs return 0.
	assert.Equal(t, uint32(0), roList.Frequency(0))
	assert.Equal(t, uint32(0), roList.Frequency(11))
	assert.Equal(t, uint32(0), roList.Frequency(1<<40+1))
}

func TestMergeListOr(t *testing.T) {
	pl := posting.NewMergeList()
	pl.Or(posting.NewBuilderList(1, 3))
	pl.Or(posting.NewBuilderList(2, 3))

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
}

func TestMergeListRemove(t *testing.T) {
	pl := posting.NewMergeList()
	pl.Or(posting.NewBuilderList(1, 2, 3))
	pl.Remove(2)
	assert.Equal(t, []uint64{1, 3}, pl.ToArray())

	// Collapse down to a single element, then merge back up again — exercises
	// the first-only representation transition in both directions.
	pl.Remove(1)
	assert.Equal(t, []uint64{3}, pl.ToArray())

	pl.Or(posting.NewBuilderList(5))
	assert.Equal(t, []uint64{3, 5}, pl.ToArray())
}

func TestMergeListRemoveAllAllOnes(t *testing.T) {
	// All-TF=1 list takes the bulk roaring-AndNot path (no stored frequencies).
	pl := posting.NewMergeList()
	pl.Or(posting.NewBuilderList(1, 2, 3, 4, 5))

	pl.RemoveAll(posting.NewList(2, 4, 99)) // 99 is absent

	assert.Equal(t, []uint64{1, 3, 5}, pl.ToArray())
	assert.Equal(t, uint32(1), pl.Frequency(1))
	assert.Equal(t, uint32(1), pl.Frequency(5))
	assert.Equal(t, uint32(0), pl.Frequency(2))
}

func TestMergeListRemoveAllPreservesFrequencies(t *testing.T) {
	// Counted list takes the single-pass survivor-rebuild path.
	src := posting.NewBuilderList()
	src.AddWithFrequency(1, 2)
	src.AddWithFrequency(2, 1)
	src.AddWithFrequency(3, 7)
	src.AddWithFrequency(4, 1)
	src.AddWithFrequency(5, 4)
	pl := posting.NewMergeList()
	pl.Or(src)

	// Remove a TF=1 doc (2) and a TF>1 doc (3) in one bulk call.
	pl.RemoveAll(posting.NewList(2, 3))

	assert.Equal(t, []uint64{1, 4, 5}, pl.ToArray())
	assert.Equal(t, uint32(2), pl.Frequency(1))
	assert.Equal(t, uint32(1), pl.Frequency(4))
	assert.Equal(t, uint32(4), pl.Frequency(5))
	assert.Equal(t, uint32(0), pl.Frequency(2))
	assert.Equal(t, uint32(0), pl.Frequency(3))

	// Survivors' frequencies round-trip through the serialized form.
	buf, err := pl.Marshal()
	require.NoError(t, err)
	pl2, err := posting.Unmarshal(buf)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 4, 5}, pl2.ToArray())
	assert.Equal(t, uint32(4), pl2.Frequency(5))
}

func TestMergeListRemoveAllEdgeCases(t *testing.T) {
	pl := posting.NewMergeList()
	pl.Or(posting.NewBuilderList(1, 2, 3))

	pl.RemoveAll(posting.NewList()) // empty deletes: no-op
	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())

	pl.RemoveAll(posting.NewList(1, 2, 3)) // remove everything
	assert.Empty(t, pl.ToArray())
	assert.Equal(t, uint64(0), pl.GetCardinality())
}

func TestBuilderListClearThenReuse(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3)
	pl.Clear()
	assert.Empty(t, pl.ToArray())

	pl.Add(4)
	pl.Add(5)
	assert.Equal(t, []uint64{4, 5}, pl.ToArray())
}

func TestBuilderListMarshalIntoTooSmall(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3)
	size := int(pl.GetSerializedSizeInBytes())

	err := pl.MarshalInto(make([]byte, 0, size-1))

	assert.Error(t, err)
}

func TestBuilderListMarshalInto(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3)
	want, err := pl.Marshal()
	require.NoError(t, err)
	buf := make([]byte, 0, pl.GetSerializedSizeInBytes())

	err = pl.MarshalInto(buf)

	require.NoError(t, err)
	assert.Equal(t, want, buf[:len(want)])
}

func TestConcat2(t *testing.T) {
	pl := posting.NewList(1, 2, 3, 4, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300)
	buf, err := pl.Marshal()
	assert.NoError(t, err)

	pl2, err := posting.Unmarshal(buf)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300}, pl.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300}, pl2.ToArray())
}

func TestClear(t *testing.T) {
	pl := posting.NewList(1, 2, 3, 4, 5)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
	pl.Clear()
	assert.Equal(t, []uint64{}, pl.ToArray())
}

func TestFieldMap(t *testing.T) {
	fm := posting.NewFieldMap()
	fm.OrField("test", posting.NewList(1, 2, 3, 4, 5))
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, fm["test"].ToArray())
}

func TestFieldMapOr(t *testing.T) {
	fm := posting.NewFieldMap()
	fm.OrField("test", posting.NewList(1, 2))
	fm.OrField("test2", posting.NewList(3, 4))

	fm2 := posting.NewFieldMap()
	fm2.OrField("test", posting.NewList(3, 4))
	fm2.OrField("test2", posting.NewList(5, 6))
	fm2.OrField("test3", posting.NewList(7, 8))
	fm.Or(fm2)

	assert.Equal(t, []uint64{1, 2, 3, 4}, fm["test"].ToArray())
	assert.Equal(t, []uint64{3, 4, 5, 6}, fm["test2"].ToArray())
	assert.Equal(t, []uint64{7, 8}, fm["test3"].ToArray())
}

func TestFieldMapAnd(t *testing.T) {
	fm := posting.NewFieldMap()
	fm.OrField("test", posting.NewList(1, 2))

	fm2 := posting.NewFieldMap()
	fm2.OrField("test2", posting.NewList(2, 4))
	fm.And(fm2)

	assert.Equal(t, []uint64{2}, fm["test"].ToArray())
	assert.Equal(t, []uint64{2}, fm["test2"].ToArray())
}

func BenchmarkListSerializationPosting(b *testing.B) {
	ids := make([]uint64, 1_000_000)
	for i := 0; i < len(ids); i++ {
		if i == 0 {
			ids[i] = 1
		} else {
			ids[i] = ids[i-1] + uint64(rand.Intn(5))
		}
	}

	b.ReportAllocs()
	b.StopTimer()
	for b.Loop() {
		b.StartTimer()
		pl := posting.NewList(ids...)
		_, err := pl.Marshal()
		b.StopTimer()
		require.NoError(b, err)
	}
}

func BenchmarkListDeserializationPosting(b *testing.B) {
	ids := make([]uint64, 1_000_000)
	for i := 0; i < len(ids); i++ {
		if i == 0 {
			ids[i] = 1
		} else {
			ids[i] = ids[i-1] + uint64(rand.Intn(5))
		}
	}

	pl := posting.NewList(ids...)
	buf, err := pl.Marshal()
	require.NoError(b, err)

	// Ensure this operation will succeed, to avoid checking err in the loop
	_, err = posting.Unmarshal(buf)
	require.NoError(b, err)

	b.ReportAllocs()

	b.Run("UnmarshalReadOnly", func(b *testing.B) {
		for b.Loop() {
			posting.UnmarshalReadOnly(buf)
		}
	})

	b.Run("Unmarshal", func(b *testing.B) {
		for b.Loop() {
			posting.Unmarshal(buf)
		}
	})
}

func BenchmarkListQuery(b *testing.B) {
	// Mimic a codesearch query: unmarshal a bunch of posting lists, or them together, then
	// remove one big list of deleted ids.
	c := 0
	delPls := posting.NewList()
	for range 100_000 {
		delPls.Add(uint64(c))
		c += rand.Intn(500_000)
	}

	bufs := make([][]byte, 0)
	c = 0
	for range 10_000 {
		newBuf, err := posting.NewList(uint64(c)).Marshal()
		require.NoError(b, err)
		bufs = append(bufs, newBuf)
		c += rand.Intn(500_000)
	}

	b.ReportAllocs()
	b.Run("MergeOptimized", func(b *testing.B) {
		for b.Loop() {
			pl := posting.NewList()
			for _, b := range bufs {
				newPl, _ := posting.UnmarshalReadOnly(b)
				pl.Or(newPl)
			}
			pl.AndNot(delPls)
		}
	})

	b.Run("MergeOld", func(b *testing.B) {
		for b.Loop() {
			pl := posting.NewList()
			for _, b := range bufs {
				newPl, _ := posting.Unmarshal(b)
				pl.Or(newPl)
			}
			delA := delPls.ToArray()
			for _, id := range delA {
				pl.Remove(id)
			}
		}
	})
}

// BenchmarkMergeListDeleteCompaction models the per-posting-list work in
// CompactDeletes: unmarshal a counted list and drop ~10% of its docs. RemoveAll
// is one O(cardinality) pass; the old per-ID loop does a binary search plus an
// O(n) slice shift per deleted ID, which dominates for large lists. The
// per-iteration Unmarshal (which CompactDeletes also does) is excluded from the
// timer so the two arms compare only the removal strategy.
func BenchmarkMergeListDeleteCompaction(b *testing.B) {
	const n = 100_000
	base := posting.NewBuilderList()
	delIDs := make([]uint64, 0, n/10)
	for i := uint64(0); i < n; i++ {
		freq := uint32(1)
		if i%50 == 0 { // sparse TF>1 outliers, so the list is counted
			freq = 3
		}
		base.AddWithFrequency(i, freq)
		if i%10 == 0 {
			delIDs = append(delIDs, i)
		}
	}
	buf, err := base.Marshal()
	require.NoError(b, err)
	dels := posting.NewList(delIDs...)

	b.Run("RemoveAll", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			pl, err := posting.Unmarshal(buf)
			require.NoError(b, err)
			b.StartTimer()
			pl.RemoveAll(dels)
		}
	})

	b.Run("PerIDRemoveLoop", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			pl, err := posting.Unmarshal(buf)
			require.NoError(b, err)
			b.StartTimer()
			for _, id := range delIDs {
				pl.Remove(id)
			}
		}
	})
}
