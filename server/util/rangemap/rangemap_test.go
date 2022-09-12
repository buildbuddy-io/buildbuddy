package rangemap_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/stretchr/testify/require"
)

func TestAddOrdering(t *testing.T) {
	r := rangemap.New()

	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
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
	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.NoError(t, err)

	_, err = r.Add([]byte("b"), []byte("f"), 2)
	require.Equal(t, rangemap.RangeOverlapError, err)
	r.Clear()

	_, err = r.Add([]byte("a"), []byte("z"), 1)
	require.NoError(t, err)

	_, err = r.Add([]byte("bbbb"), []byte("cccc"), 2)
	require.Equal(t, rangemap.RangeOverlapError, err)
}

func TestAddDuplicateError(t *testing.T) {
	r := rangemap.New()

	var err error
	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.NoError(t, err)

	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.Equal(t, rangemap.RangeOverlapError, err)
}

func TestLookupSingleRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	// Single range
	addRange("a", "z", 1)
	require.Equal(t, 1, r.Lookup([]byte("a")))
}

func TestLookupMultiRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
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

	require.Equal(t, 1, r.Lookup([]byte("a")))
	require.Equal(t, 2, r.Lookup([]byte("b")))
	require.Equal(t, 3, r.Lookup([]byte("c")))
	require.Equal(t, 4, r.Lookup([]byte("d")))
	require.Equal(t, 5, r.Lookup([]byte("e")))
	require.Equal(t, 6, r.Lookup([]byte("f")))
	require.Equal(t, 7, r.Lookup([]byte("g")))
	require.Equal(t, 8, r.Lookup([]byte("h")))
	require.Equal(t, 8, r.Lookup([]byte("i")))
	require.Equal(t, nil, r.Lookup([]byte("z")))
}

func TestLookupSparseRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	// Sparse ranges
	addRange("a", "e", 1)
	// f-m is missing
	addRange("m", "z", 2)
	// weird prefix
	addRange(string([]byte{0}), "a", 3)

	require.Equal(t, 1, r.Lookup([]byte("d")))
	require.Equal(t, nil, r.Lookup([]byte("g")))
	require.Equal(t, 2, r.Lookup([]byte("m")))
	require.Equal(t, 3, r.Lookup([]byte{'\x03', 'm'}))
}

func TestLookupNarrowRange(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("aaaa", "aaac", 1)
	addRange("aaad", "ffff", 2)

	require.Equal(t, 1, r.Lookup([]byte("aaab")))
	require.Equal(t, nil, r.Lookup([]byte("aaac")))
	require.Equal(t, 2, r.Lookup([]byte("aaae")))
	require.Equal(t, 2, r.Lookup([]byte("bbbb")))
}

func TestGet(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("aaaa", "aaac", 1)
	addRange("aaad", "ffff", 2)

	r1 := r.Get([]byte("aaaa"), []byte("aaac"))
	r2 := r.Get([]byte("aaad"), []byte("ffff"))
	require.Equal(t, 1, r1.Val)
	require.Equal(t, 2, r2.Val)
	require.Nil(t, r.Get([]byte("ffff"), []byte("z")))
}

func TestGetOverlapping(t *testing.T) {
	rm := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := rm.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("b", "e", 1)
	addRange("e", "i", 2)
	addRange("m", "q", 3)

	overlappingRangeIDs := func(left, right string) []int {
		var ids []int
		for _, r := range rm.GetOverlapping([]byte(left), []byte(right)) {
			ids = append(ids, r.Val.(int))
		}
		return ids
	}

	require.Equal(t, []int{1, 2}, overlappingRangeIDs("d", "m"))
	require.Equal(t, []int{1, 2, 3}, overlappingRangeIDs("d", "n"))
	require.Equal(t, []int{1, 2, 3}, overlappingRangeIDs("d", "q"))
	require.Equal(t, []int{1, 2, 3}, overlappingRangeIDs("b", "q"))
	require.Empty(t, overlappingRangeIDs("a", "b"))
	require.Empty(t, overlappingRangeIDs("q", "z"))
}

func TestRemove(t *testing.T) {
	r := rangemap.New()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	// Sparse ranges
	addRange("a", "z", 1)

	require.Equal(t, 1, r.Lookup([]byte("d")))

	err := r.Remove([]byte("a"), []byte("z"))
	require.NoError(t, err)

	err = r.Remove([]byte("a"), []byte("z"))
	require.NotNil(t, err)

	require.Equal(t, nil, r.Lookup([]byte("d")))
}

func TestClear(t *testing.T) {
	r := rangemap.New()

	var err error
	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(r.Ranges()))

	r.Clear()
	require.Equal(t, 0, len(r.Ranges()))
}

func TestGetOverlappingNPE(t *testing.T) {
	r := rangemap.New()

	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("e", "z", 1)
	overlap := r.GetOverlapping([]byte("d"), []byte("m"))
	require.Equal(t, 1, overlap[0].Val)
}

func TestGetOverlappingMulti(t *testing.T) {
	r := rangemap.New()

	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("a", "c", 1)
	addRange("c", "e", 2)
	addRange("e", "g", 3)
	addRange("l", "n", 4)
	addRange("n", "p", 5)

	overlap := r.GetOverlapping([]byte("d"), []byte("m"))
	require.Equal(t, 2, overlap[0].Val)
	require.Equal(t, 3, overlap[1].Val)
	require.Equal(t, 4, overlap[2].Val)

	overlap2 := r.GetOverlapping([]byte("g"), []byte("z"))
	require.Equal(t, 4, overlap2[0].Val)
	require.Equal(t, 5, overlap2[1].Val)
}
