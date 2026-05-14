package rangemap_test

import (
	"fmt"
	"math"
	"testing"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/stretchr/testify/require"
)

func TestAddOrdering(t *testing.T) {
	r := rangemap.New[int]()

	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("c", "d", 2)
	addRange("a", "b", 1)
	addRange("g", "h", 7)
	addRange("m", "n", 4)
	addRange("k", "l", 3)
	addRange("e", "f", 6)
	addRange("o", "pp", 8)
	addRange("i", "j", 5)

	ranges := r.Ranges()

	require.Equal(t, 8, len(ranges))

	require.Equal(t, ranges[0].Start, []byte("a"))
	require.Equal(t, ranges[1].Start, []byte("c"))
	require.Equal(t, ranges[2].Start, []byte("e"))
	require.Equal(t, ranges[3].Start, []byte("g"))
	require.Equal(t, ranges[4].Start, []byte("i"))
	require.Equal(t, ranges[5].Start, []byte("k"))
	require.Equal(t, ranges[6].Start, []byte("m"))
	require.Equal(t, ranges[7].Start, []byte("o"))
}

func TestAddOverlapError(t *testing.T) {
	r := rangemap.New[int]()

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
	r := rangemap.New[int]()

	var err error
	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.NoError(t, err)

	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.Equal(t, rangemap.RangeOverlapError, err)
}

func TestLookupSingleRange(t *testing.T) {
	r := rangemap.New[int]()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	// Single range
	addRange("a", "z", 1)
	res, found := r.Lookup([]byte("a"))
	require.True(t, found)
	require.Equal(t, 1, res)
}

func TestLookupMultiRange(t *testing.T) {
	r := rangemap.New[int]()
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

	tests := []struct {
		key   string
		found bool
		val   int
	}{
		{"b", true, 2},
		{"c", true, 3},
		{"d", true, 4},
		{"e", true, 5},
		{"f", true, 6},
		{"g", true, 7},
		{"h", true, 8},
		{"i", true, 8},
		{"z", false, 0},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Lookup(%q)", tc.key), func(t *testing.T) {
			actual, found := r.Lookup([]byte(tc.key))
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.val, actual)
		})
	}
}

func TestLookupSparseRange(t *testing.T) {
	r := rangemap.New[int]()
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

	tests := []struct {
		key   []byte
		found bool
		val   int
	}{
		{[]byte("d"), true, 1},
		{[]byte("g"), false, 0},
		{[]byte("m"), true, 2},
		{[]byte{'\x03', 'm'}, true, 3},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("Lookup(%q)", tc.key), func(t *testing.T) {
			actual, found := r.Lookup(tc.key)
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.val, actual)
		})
	}
}

func TestLookupNarrowRange(t *testing.T) {
	r := rangemap.New[int]()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("aaaa", "aaac", 1)
	addRange("aaad", "ffff", 2)

	tests := []struct {
		key   string
		found bool
		val   int
	}{
		{"aaab", true, 1},
		{"aaac", false, 0},
		{"aaae", true, 2},
		{"bbbb", true, 2},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Lookup(%q)", tc.key), func(t *testing.T) {
			actual, found := r.Lookup([]byte(tc.key))
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.val, actual)
		})
	}
}

func TestGet(t *testing.T) {
	r := rangemap.New[int]()
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
	require.Nil(t, r.Get([]byte("aaad"), []byte("aaae")))
	require.Nil(t, r.Get([]byte("aaae"), []byte("ffff")))
}

func TestGetOverlapping(t *testing.T) {
	rm := rangemap.New[int]()
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
			ids = append(ids, r.Val)
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
	r := rangemap.New[int]()
	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	// Sparse ranges
	addRange("a", "z", 1)

	res, found := r.Lookup([]byte("d"))
	require.True(t, found)
	require.Equal(t, 1, res)

	err := r.Remove([]byte("a"), []byte("z"))
	require.NoError(t, err)

	err = r.Remove([]byte("a"), []byte("z"))
	require.Error(t, err)

	res, found = r.Lookup([]byte("e"))
	require.False(t, found)
	require.Equal(t, 0, res)
}

func TestClear(t *testing.T) {
	r := rangemap.New[int]()

	var err error
	_, err = r.Add([]byte("a"), []byte("c"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(r.Ranges()))

	r.Clear()
	require.Equal(t, 0, len(r.Ranges()))
}

func TestGetOverlappingNPE(t *testing.T) {
	r := rangemap.New[int]()

	addRange := func(left, right string, id int) {
		_, err := r.Add([]byte(left), []byte(right), id)
		require.NoError(t, err)
	}

	addRange("e", "z", 1)
	overlap := r.GetOverlapping([]byte("d"), []byte("m"))
	require.Equal(t, 1, overlap[0].Val)
}

func TestGetOverlappingMulti(t *testing.T) {
	r := rangemap.New[int]()

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

func TestString(t *testing.T) {
	r := rangemap.Range{
		Start: []byte("a"),
		End:   []byte{math.MaxUint8},
	}
	require.True(t, utf8.ValidString(r.String()))
}
