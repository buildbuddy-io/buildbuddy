package postinglist_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/postinglist"
	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	pl := postinglist.New()
	pl.Add(3)
	pl.Add(3)
	pl.Add(3)
	pl.Add(1)
	pl.Add(2)
	pl.Add(2)

	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
}

func TestOr(t *testing.T) {
	pl := postinglist.New()
	pl.Add(1)
	pl.Add(2)

	pl2 := postinglist.New()
	pl2.Add(3)
	pl2.Add(4)

	pl.Or(pl2)
	assert.Equal(t, []uint64{1, 2, 3, 4}, pl.ToArray())
}

func TestAnd(t *testing.T) {
	pl := postinglist.New()
	pl.Add(1)
	pl.Add(2)
	pl.Add(3)

	pl2 := postinglist.New()
	pl2.Add(3)
	pl2.Add(4)
	pl2.Add(5)

	pl.And(pl2)
	assert.Equal(t, []uint64{3}, pl.ToArray())
}

func TestRemove(t *testing.T) {
	pl := postinglist.New()
	pl.Add(1)
	pl.Add(2)
	pl.Add(3)
	pl.Add(4)
	pl.Add(5)
	pl.Remove(3)

	assert.Equal(t, []uint64{1, 2, 4, 5}, pl.ToArray())
}

func TestAddMany(t *testing.T) {
	pl := postinglist.New(3, 4, 5)
	pl.Add(1)
	pl.Add(2)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
}

func TestMarshal(t *testing.T) {
	pl := postinglist.New(1, 2, 3, 4, 5)
	buf, err := pl.Marshal()
	assert.NoError(t, err)

	pl2 := postinglist.New()
	_, err = pl2.Unmarshal(buf)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl2.ToArray())
}

func TestClear(t *testing.T) {
	pl := postinglist.New(1, 2, 3, 4, 5)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, pl.ToArray())
	pl.Clear()
	assert.Equal(t, []uint64{}, pl.ToArray())
}
