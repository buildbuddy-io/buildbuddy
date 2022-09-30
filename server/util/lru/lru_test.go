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
	require.Equal(t, 1, len(evictions))
	require.Equal(t, 5, evictions[0])
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
	require.False(t, l.PushBack("c", 3))
	require.Equal(t, 1, len(evictions))
	require.Equal(t, 3, evictions[0])
}
