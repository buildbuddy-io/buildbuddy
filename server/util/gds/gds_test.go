package gds_test

import (
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/gds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func evictAll(q *gds.Queue[string]) []string {
	var keys []string
	for {
		k, ok := q.Evict()
		if !ok {
			return keys
		}
		keys = append(keys, k)
	}
}

func TestEvictsLowestValueFirst(t *testing.T) {
	q := gds.New[string]()
	q.Add("big", 10_000)
	q.Add("small", 50)
	q.Add("medium", 1_000)

	evicted := evictAll(q)

	assert.Equal(t, []string{"small", "medium", "big"}, evicted)
}

func TestIdleEntryAgesOut(t *testing.T) {
	// A 10,000 entry outlives about ten 1,000 entries that arrive after it,
	// then gets evicted ahead of newer 1,000 entries.
	q := gds.New[string]()
	q.Add("big", 10_000)
	var evicted []string
	for i := range 12 {
		q.Add(string(rune('a'+i)), 1_000)
		k, ok := q.Evict()
		require.True(t, ok)
		evicted = append(evicted, k)
	}

	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "big", "j", "k"}, evicted)
}

func TestReAddRefreshesPriority(t *testing.T) {
	q := gds.New[string]()
	q.Add("big", 10_000)
	for i := range 9 {
		q.Add(string(rune('a'+i)), 1_000)
		_, ok := q.Evict()
		require.True(t, ok)
	}
	q.Add("big", 10_000)
	q.Add("small", 1_000)

	k, ok := q.Evict()

	require.True(t, ok)
	assert.Equal(t, "small", k)
}

func TestRemove(t *testing.T) {
	q := gds.New[string]()
	q.Add("a", 1)
	q.Add("b", 2)

	removed := q.Remove("a")
	removedAgain := q.Remove("a")
	evicted := evictAll(q)

	assert.True(t, removed)
	assert.False(t, removedAgain)
	assert.Equal(t, []string{"b"}, evicted)
}

func TestEqualValuesEvictInLRUOrder(t *testing.T) {
	q := gds.New[string]()
	q.Add("a", 7)
	q.Add("b", 7)
	q.Add("c", 7)
	first, ok := q.Evict()
	require.True(t, ok)
	q.Add("d", 7)
	q.Add("b", 7)

	evicted := append([]string{first}, evictAll(q)...)

	assert.Equal(t, []string{"a", "c", "d", "b"}, evicted)
}

func TestAll(t *testing.T) {
	q := gds.New[string]()
	q.Add("a", 1)
	q.Add("b", 2)
	q.Add("c", 3)
	_, ok := q.Evict()
	require.True(t, ok)

	keys := slices.Collect(q.All())

	assert.ElementsMatch(t, []string{"b", "c"}, keys)
}
