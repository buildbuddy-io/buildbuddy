package lru_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/stretchr/testify/require"
)

type eviction struct {
	value  int
	reason lru.EvictionReason
}

func TestAdd(t *testing.T) {
	evictions := []eviction{}

	l, err := lru.NewLRU[int](&lru.Config[int]{
		MaxSize: 10,
		OnEvict: func(value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{value, reason})
		},
		SizeFn: func(value int) int64 { return int64(value) },
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.True(t, l.Add("c", 3))
	require.Equal(t, []eviction{{5, lru.SizeEviction}}, evictions)

	// Now overwrite "c" so that its new size exceeds the max cache size;
	// should *remove* the existing "c" entry since it'll be overwritten, and
	// then *evict* the "b" entry to make room for the new "c".
	require.True(t, l.Add("c", 10))
	require.Equal(
		t,
		[]eviction{
			{5, lru.SizeEviction},
			{3, lru.ConflictEviction},
			{4, lru.SizeEviction},
		},
		evictions)
}

func TestAdd_UpdateInPlace(t *testing.T) {
	evictions := []eviction{}

	l, err := lru.NewLRU[int](&lru.Config[int]{
		MaxSize: 10,
		OnEvict: func(value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{value, reason})
		},
		SizeFn:        func(value int) int64 { return int64(value) },
		UpdateInPlace: true,
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.True(t, l.Add("c", 3))
	require.Equal(t, []eviction{{5, lru.SizeEviction}}, evictions)

	require.True(t, l.Add("c", 6))
	require.Equal(
		t, []eviction{{5, lru.SizeEviction}}, evictions,
		"c entry should be updated in place without causing any evictions")

	require.True(t, l.Add("c", 7))
	require.Equal(
		t, []eviction{{5, lru.SizeEviction}, {4, lru.SizeEviction}}, evictions,
		"c entry should be updated in place but evict b to make room")
}

func TestPushBack(t *testing.T) {
	evictions := []eviction{}

	l, err := lru.NewLRU[int](&lru.Config[int]{
		MaxSize: 10,
		OnEvict: func(value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{value, reason})
		},
		SizeFn: func(value int) int64 { return int64(value) },
	})
	require.NoError(t, err)

	require.True(t, l.PushBack("a", 5))
	require.True(t, l.PushBack("b", 4))
	require.False(t, l.PushBack("c", 3), "c should not be added since there isn't enough capacity")
	require.Empty(t, evictions)

	// Now attempt to overwrite "b" with a size that would exceed the cache
	// capacity; this should fail.
	require.False(t, l.PushBack("b", 10))
	require.Empty(t, evictions)
}

func TestRemoveOldest(t *testing.T) {
	evictions := []eviction{}

	l, err := lru.NewLRU[int](&lru.Config[int]{
		MaxSize: 10,
		OnEvict: func(value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{value, reason})
		},
		SizeFn: func(value int) int64 { return int64(value) },
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.Empty(t, evictions)

	oldest, ok := l.RemoveOldest()
	require.True(t, ok)
	require.Equal(t, 5, oldest)
	require.Equal(t, []eviction{{5, lru.SizeEviction}}, evictions)

	oldest, ok = l.RemoveOldest()
	require.True(t, ok)
	require.Equal(t, 4, oldest)
	require.Equal(t, []eviction{{5, lru.SizeEviction}, {4, lru.SizeEviction}}, evictions)
}
