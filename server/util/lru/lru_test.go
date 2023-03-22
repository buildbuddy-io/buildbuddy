package lru_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	evictions := make([]int, 0)

	l, err := lru.NewLRU(&lru.Config{
		MaxSize: 10,
		OnEvict: func(value interface{}) { evictions = append(evictions, value.(int)) },
		SizeFn:  func(value interface{}) int64 { return int64(value.(int)) },
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.True(t, l.Add("c", 3))
	require.Equal(t, []int{5}, evictions)

	// Now overwrite "c" so that its new size exceeds the max cache size;
	// should *remove* the existing "c" entry since it'll be overwritten, and
	// then *evict* the "a" entry.
	require.True(t, l.Add("c", 10))
	require.Equal(t, []int{5, 3, 4}, evictions)
}

func TestAdd_UpdateInPlace(t *testing.T) {
	evictions := make([]int, 0)

	l, err := lru.NewLRU(&lru.Config{
		MaxSize:       10,
		OnEvict:       func(value interface{}) { evictions = append(evictions, value.(int)) },
		SizeFn:        func(value interface{}) int64 { return int64(value.(int)) },
		UpdateInPlace: true,
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.True(t, l.Add("c", 3))
	require.Equal(t, []int{5}, evictions)

	require.True(t, l.Add("c", 6))
	require.Equal(
		t, []int{5}, evictions,
		"c entry should be updated in place without causing any evictions")

	require.True(t, l.Add("c", 7))
	require.Equal(
		t, []int{5, 4}, evictions,
		"c entry should be updated in place but evict b to make room")
}

func TestPushBack(t *testing.T) {
	evictions := make([]int, 0)

	l, err := lru.NewLRU(&lru.Config{
		MaxSize: 10,
		OnEvict: func(value interface{}) { evictions = append(evictions, value.(int)) },
		SizeFn:  func(value interface{}) int64 { return int64(value.(int)) },
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
