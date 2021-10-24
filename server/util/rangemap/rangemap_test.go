package rangemap_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/stretchr/testify/require"
)

func TestAddOrdering(t *testing.T) {
	r := rangemap.New()

	addRange := func(left, right string, id int) {
		err := r.Add([]byte(left), []byte(right), id)
		require.Nil(t, err)
	}

	addRange("c", "d", 2)
	addRange("a", "b", 1)
	addRange("g", "h", 4)
	addRange("m", "n", 7)
	addRange("k", "l", 6)
	addRange("e", "f", 3)
	addRange("o", "pp", 8)
	addRange("i", "j", 5)

	ranges := r.Ranges()

	require.Equal(t, 8, len(ranges))
	require.Equal(t, ranges[0].Val, 1)
	require.Equal(t, ranges[1].Val, 2)
	require.Equal(t, ranges[2].Val, 3)
	require.Equal(t, ranges[3].Val, 4)
	require.Equal(t, ranges[4].Val, 5)
	require.Equal(t, ranges[5].Val, 6)
	require.Equal(t, ranges[6].Val, 7)
	require.Equal(t, ranges[7].Val, 8)
}

func TestAddOverlapError(t *testing.T) {
	r := rangemap.New()

	var err error
	err = r.Add([]byte("a"), []byte("c"), 1)
	require.Nil(t, err)

	err = r.Add([]byte("b"), []byte("f"), 2)
	require.Equal(t, rangemap.RangeOverlapError, err)
	r.Clear()

	err = r.Add([]byte("a"), []byte("z"), 1)
	require.Nil(t, err)

	err = r.Add([]byte("bbbb"), []byte("cccc"), 2)
	require.Equal(t, rangemap.RangeOverlapError, err)
}

func TestGetSingleRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		err := r.Add([]byte(left), []byte(right), id)
		require.Nil(t, err)
	}

	// Single range
	addRange("a", "z", 1)
	require.Equal(t, 1, r.Get([]byte("a")))
}

func TestGetMultiRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		err := r.Add([]byte(left), []byte(right), id)
		require.Nil(t, err)
	}

	// Lots of small ranges
	addRange("a", "b", 1)
	addRange("b", "c", 2)
	addRange("c", "d", 3)
	addRange("d", "e", 4)
	addRange("e", "f", 5)
	addRange("f", "g", 6)
	addRange("g", "h", 7)
	addRange("h", "iiii", 8)

	require.Equal(t, 1, r.Get([]byte("a")))
	require.Equal(t, 2, r.Get([]byte("b")))
	require.Equal(t, 3, r.Get([]byte("c")))
	require.Equal(t, 4, r.Get([]byte("d")))
	require.Equal(t, 5, r.Get([]byte("e")))
	require.Equal(t, 6, r.Get([]byte("f")))
	require.Equal(t, 7, r.Get([]byte("g")))
	require.Equal(t, 8, r.Get([]byte("h")))
	require.Equal(t, 8, r.Get([]byte("i")))
	require.Equal(t, nil, r.Get([]byte("z")))
}

func TestGetSparseRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		err := r.Add([]byte(left), []byte(right), id)
		require.Nil(t, err)
	}

	// Sparse ranges
	addRange("a", "e", 1)
	// f-m is missing
	addRange("m", "z", 2)

	require.Equal(t, 1, r.Get([]byte("d")))
	require.Equal(t, nil, r.Get([]byte("g")))
	require.Equal(t, 2, r.Get([]byte("m")))
}

func TestGetNarrowRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		err := r.Add([]byte(left), []byte(right), id)
		require.Nil(t, err)
	}

	addRange("aaaa", "aaac", 1)
	addRange("aaad", "ffff", 2)

	require.Equal(t, 1, r.Get([]byte("aaab")))
	require.Equal(t, nil, r.Get([]byte("aaac")))
	require.Equal(t, 2, r.Get([]byte("aaae")))
	require.Equal(t, 2, r.Get([]byte("bbbb")))
}

func TestClear(t *testing.T) {
	r := rangemap.New()

	var err error
	err = r.Add([]byte("a"), []byte("c"), 1)
	require.Nil(t, err)
	require.Equal(t, 1, len(r.Ranges()))

	r.Clear()
	require.Equal(t, 0, len(r.Ranges()))
}
