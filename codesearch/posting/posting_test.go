package posting_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/stretchr/testify/assert"
)

func docidArray(pl posting.List) []uint64 {
	arr := pl.ToArray()
	docids := make([]uint64, len(arr))
	for i, p := range arr {
		docids[i] = p.Docid()
	}
	return docids
}

func TestAdd(t *testing.T) {
	pl := posting.NewList()
	pl.Add(posting.New(3))
	pl.Add(posting.New(3))
	pl.Add(posting.New(3))
	pl.Add(posting.New(1))
	pl.Add(posting.New(2))
	pl.Add(posting.New(2))

	assert.Equal(t, []uint64{1, 2, 3}, docidArray(pl))
}

func TestOr(t *testing.T) {
	pl := posting.NewList()
	pl.Add(posting.New(1))
	pl.Add(posting.New(2))

	pl2 := posting.NewList()
	pl2.Add(posting.New(3))
	pl2.Add(posting.New(4))

	pl.Or(pl2)
	assert.Equal(t, []uint64{1, 2, 3, 4}, docidArray(pl))
}

func TestAnd(t *testing.T) {
	pl := posting.NewList()
	pl.Add(posting.New(1))
	pl.Add(posting.New(2))
	pl.Add(posting.New(3))

	pl2 := posting.NewList()
	pl2.Add(posting.New(3))
	pl2.Add(posting.New(4))
	pl2.Add(posting.New(5))

	pl.And(pl2)
	assert.Equal(t, []uint64{3}, docidArray(pl))
}

func TestRemove(t *testing.T) {
	pl := posting.NewList()
	pl.Add(posting.New(1))
	pl.Add(posting.New(2))
	pl.Add(posting.New(3))
	pl.Add(posting.New(4))
	pl.Add(posting.New(5))
	pl.Remove(3)

	assert.Equal(t, []uint64{1, 2, 4, 5}, docidArray(pl))
}

func TestAddMany(t *testing.T) {
	pl := posting.NewList(posting.New(3), posting.New(4), posting.New(5))
	pl.Add(posting.New(1))
	pl.Add(posting.New(2))
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, docidArray(pl))
}

func TestMarshal(t *testing.T) {
	pl := posting.NewList(posting.New(1), posting.New(2), posting.New(3), posting.New(4), posting.New(5))
	buf, err := pl.Marshal()
	assert.NoError(t, err)

	pl2 := posting.NewList()
	_, err = pl2.Unmarshal(buf)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, docidArray(pl))
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, docidArray(pl2))
}

func TestClear(t *testing.T) {
	pl := posting.NewList(posting.New(1), posting.New(2), posting.New(3), posting.New(4), posting.New(5))
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, docidArray(pl))
	pl.Clear()
	assert.Equal(t, []uint64{}, docidArray(pl))
}

func TestFieldMap(t *testing.T) {
	fm := posting.NewFieldMap()
	pl := posting.NewList(posting.New(1), posting.New(2), posting.New(3), posting.New(4), posting.New(5))
	fm.OrField("test", pl)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, docidArray(fm["test"]))
}

func TestFieldMapOr(t *testing.T) {
	fm := posting.NewFieldMap()
	fm.OrField("test", posting.NewList(posting.New(1), posting.New(2)))
	fm.OrField("test2", posting.NewList(posting.New(3), posting.New(4)))

	fm2 := posting.NewFieldMap()
	fm2.OrField("test", posting.NewList(posting.New(3), posting.New(4)))
	fm2.OrField("test2", posting.NewList(posting.New(5), posting.New(6)))
	fm2.OrField("test3", posting.NewList(posting.New(7), posting.New(8)))
	fm.Or(fm2)

	assert.Equal(t, []uint64{1, 2, 3, 4}, docidArray(fm["test"]))
	assert.Equal(t, []uint64{3, 4, 5, 6}, docidArray(fm["test2"]))
	assert.Equal(t, []uint64{7, 8}, docidArray(fm["test3"]))
}

func TestFieldMapAnd(t *testing.T) {
	fm := posting.NewFieldMap()
	fm.OrField("test", posting.NewList(posting.New(1), posting.New(2)))

	fm2 := posting.NewFieldMap()
	fm2.OrField("test2", posting.NewList(posting.New(2), posting.New(4)))
	fm.And(fm2)

	assert.Equal(t, []uint64{2}, docidArray(fm["test"]))
	assert.Equal(t, []uint64{2}, docidArray(fm["test2"]))
}
