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
	pl.Add(2)
	pl.Add(3)
	pl.Add(1)

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
}

func TestBuilderListOutOfOrderAdd(t *testing.T) {
	pl := posting.NewBuilderList()
	pl.Add(10)
	pl.Add(12)
	pl.Add(11)
	pl.Add(9)
	pl.Add(10)

	assert.Equal(t, []uint64{9, 10, 11, 12}, pl.ToArray())
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

func TestBuilderListMarshalDenseCounts(t *testing.T) {
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

func TestBuilderListMarshalBitsetCounts(t *testing.T) {
	pl := posting.NewBuilderList()
	for i := uint64(1); i <= 16; i++ {
		pl.AddWithFrequency(i, 1)
	}
	pl.SetLastFrequency(16, 4)
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

func TestBuilderListOrAddsFrequencies(t *testing.T) {
	pl := posting.NewBuilderList()
	pl.AddWithFrequency(1, 2)
	pl.AddWithFrequency(2, 3)
	other := posting.NewBuilderList()
	other.AddWithFrequency(2, 4)
	other.AddWithFrequency(3, 5)

	pl.Or(other)

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
	assert.Equal(t, uint32(2), pl.Frequency(1))
	assert.Equal(t, uint32(7), pl.Frequency(2))
	assert.Equal(t, uint32(5), pl.Frequency(3))
}

func TestBuilderListOr(t *testing.T) {
	pl := posting.NewBuilderList(1, 3)
	pl.Or(posting.NewBuilderList(2, 3))

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
}

func TestBuilderListAnd(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3, 4)
	pl.And(posting.NewList(2, 4, 6))

	assert.Equal(t, []uint64{2, 4}, pl.ToArray())
}

func TestBuilderListAndNot(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3, 4)
	pl.AndNot(posting.NewList(2, 4, 6))

	assert.Equal(t, []uint64{1, 3}, pl.ToArray())
}

func TestBuilderListRemove(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3)
	pl.Remove(2)
	assert.Equal(t, []uint64{1, 3}, pl.ToArray())

	pl.Remove(1)
	assert.Equal(t, []uint64{3}, pl.ToArray())

	pl.Add(5)
	assert.Equal(t, []uint64{3, 5}, pl.ToArray())
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

func TestBuilderListSetFromRoaringCollapsesToOne(t *testing.T) {
	pl := posting.NewBuilderList(1, 2, 3)
	pl.And(posting.NewList(2, 4))
	assert.Equal(t, []uint64{2}, pl.ToArray())

	pl.Add(5)
	assert.Equal(t, []uint64{2, 5}, pl.ToArray())
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
