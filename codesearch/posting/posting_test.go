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

func TestConcat(t *testing.T) {
	pl := posting.NewList(1, 2, 3, 4, 5)
	buf, err := posting.Marshal(pl)
	assert.NoError(t, err)

	pl2 := posting.NewList(4294967296, 4294967297, 4294967298, 4294967299, 4294967300)
	buf2, err := posting.Marshal(pl2)
	assert.NoError(t, err)

	pl3, err := posting.Unmarshal(append(buf, buf2...))
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
	assert.Equal(t, []uint64{4294967296, 4294967297, 4294967298, 4294967299, 4294967300}, pl2.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 4294967296, 4294967297, 4294967298, 4294967299, 4294967300}, pl3.ToArray())
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

func TestMerge(t *testing.T) {
	pl1 := posting.NewList(1, 2, 3, 4, 5)
	buf1, err := posting.Marshal(pl1)
	assert.NoError(t, err)

	pl2 := posting.NewList(6, 7, 8)
	buf2, err := posting.Marshal(pl2)
	assert.NoError(t, err)

	combined := make([]byte, len(buf1)+len(buf2))
	copy(combined, buf1)
	copy(combined[len(buf1):], buf2)

	pl3, err := posting.Unmarshal(combined)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8}, pl3.ToArray())
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
	for n := 0; n < b.N; n++ {
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

	b.ReportAllocs()
	b.StopTimer()
	for n := 0; n < b.N; n++ {
		b.StartTimer()
		pl, err := posting.Unmarshal(buf)
		_ = pl.ToArray()
		b.StopTimer()
		require.NoError(b, err)
	}
}
