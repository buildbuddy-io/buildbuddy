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
		OnEvict: func(value interface{}, removed bool) {
			evictions = append(evictions, value.(int))
			require.False(t, removed, "should be evicted, not removed")
		},
		SizeFn: func(value interface{}) int64 { return int64(value.(int)) },
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.True(t, l.Add("c", 3))
	require.Equal(t, 1, len(evictions))
	require.Equal(t, 5, evictions[0])
}

func TestPushBack(t *testing.T) {
	evictions := make([]int, 0)

	l, err := lru.NewLRU(&lru.Config{
		MaxSize: 10,
		OnEvict: func(value interface{}, removed bool) {
			evictions = append(evictions, value.(int))
			require.False(t, removed, "should be evicted, not removed")
		},
		SizeFn: func(value interface{}) int64 { return int64(value.(int)) },
	})
	require.NoError(t, err)

	require.True(t, l.PushBack("a", 5))
	require.True(t, l.PushBack("b", 4))
	require.False(t, l.PushBack("c", 3))
	require.Equal(t, 1, len(evictions))
	require.Equal(t, 3, evictions[0])
}

func TestRemoveOldest(t *testing.T) {
	evictions := make([]int, 0)

	l, err := lru.NewLRU(&lru.Config{
		MaxSize: 10,
		OnEvict: func(value interface{}, removed bool) {
			evictions = append(evictions, value.(int))
			require.False(t, removed, "should be evicted, not removed")
		},
		SizeFn: func(value interface{}) int64 { return int64(value.(int)) },
	})
	require.NoError(t, err)

	require.True(t, l.Add("a", 5))
	require.True(t, l.Add("b", 4))
	require.Empty(t, evictions)

	oldest, ok := l.RemoveOldest()
	require.True(t, ok)
	require.Equal(t, 5, oldest.(int))
	require.Equal(t, []int{5}, evictions)

	oldest, ok = l.RemoveOldest()
	require.True(t, ok)
	require.Equal(t, 4, oldest.(int))
	require.Equal(t, []int{5, 4}, evictions)
}
