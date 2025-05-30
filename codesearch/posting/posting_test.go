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

func TestMarshal(t *testing.T) {
	pl := posting.NewList(1, 2, 3, 4, 5)
	buf, err := posting.Marshal(pl)
	assert.NoError(t, err)

	pl2, err := posting.Unmarshal(buf)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl2.ToArray())
}

func TestConcat2(t *testing.T) {
	pl := posting.NewList(1, 2, 3, 4, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300)
	buf, err := posting.Marshal(pl)
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
		_, err := posting.Marshal(pl)
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
	buf, err := posting.Marshal(pl)
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
		newBuf, err := posting.Marshal(posting.NewList(uint64(c)))
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

	b.Run("Merge", func(b *testing.B) {
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
