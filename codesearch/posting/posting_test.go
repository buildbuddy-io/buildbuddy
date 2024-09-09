package posting_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/stretchr/testify/assert"
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
