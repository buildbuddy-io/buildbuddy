package gds_test

import (
	"math"
	"math/rand/v2"
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

func TestNegativeAndNaNValuesCountAsZero(t *testing.T) {
	q := gds.New[string]()
	q.Add("old", 10)
	q.Add("negative", -100)
	q.Add("nan", math.NaN())
	_, ok := q.Evict()
	require.True(t, ok)
	_, ok = q.Evict()
	require.True(t, ok)
	q.Add("new", 10)

	evicted := evictAll(q)

	assert.Equal(t, []string{"old", "new"}, evicted)
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

func TestRemoveDuringAll(t *testing.T) {
	q := gds.New[string]()
	q.Add("a", 1)
	q.Add("b", 2)
	q.Add("c", 3)

	for k := range q.All() {
		q.Remove(k)
	}
	remaining := q.Len()

	assert.Equal(t, 0, remaining)
}

// TestMatchesReference checks that the queue evicts the same entries in the
// same order as a simple implementation, which scans every entry for the lowest
// priority and breaks ties by the order they were added. It also checks that
// both agree on which entries are present. Random adds, removes and evictions
// cover cases the other tests don't, such as re-adding an entry with a lower
// priority and removing an entry from deep in the heap.
func TestMatchesReference(t *testing.T) {
	type refEntry struct {
		priority float64
		added    int
	}
	rng := rand.New(rand.NewPCG(1, 2))
	q := gds.New[int]()
	ref := map[int]refEntry{}
	inflation := 0.0
	for i := range 20_000 {
		key := rng.IntN(200)
		switch op := rng.IntN(10); {
		case op < 6:
			value := float64(rng.IntN(1_000))
			q.Add(key, value)
			ref[key] = refEntry{priority: inflation + value, added: i}
		case op < 8:
			_, want := ref[key]
			removed := q.Remove(key)
			require.Equal(t, want, removed)
			delete(ref, key)
		default:
			wantKey, wantOK := 0, false
			for k, e := range ref {
				w := ref[wantKey]
				if !wantOK || e.priority < w.priority || (e.priority == w.priority && e.added < w.added) {
					wantKey, wantOK = k, true
				}
			}
			k, ok := q.Evict()
			require.Equal(t, wantOK, ok)
			if ok {
				require.Equal(t, wantKey, k)
				inflation = ref[k].priority
				delete(ref, k)
			}
		}
		n := q.Len()
		require.Equal(t, len(ref), n)
	}
}
