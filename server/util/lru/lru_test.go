package lru_test

import (
	"testing"
	"time"

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
	require.Nil(t, err)

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
	require.Nil(t, err)

	now := time.Now().UnixNano()
	require.True(t, l.PushBack("a", 5, now, now))
	require.True(t, l.PushBack("b", 4, now, now))
	require.False(t, l.PushBack("c", 3, now, now))
	require.Equal(t, 1, len(evictions))
	require.Equal(t, 3, evictions[0])
}

func TestGet(t *testing.T) {
	evictions := make([]int, 0)
	l, err := lru.NewLRU(&lru.Config{
		MaxSize: 10,
		OnEvict: func(value interface{}) { evictions = append(evictions, value.(int)) },
		SizeFn:  func(value interface{}) int64 { return int64(value.(int)) },
	})
	require.Nil(t, err)

	longTimeAgoNanos := int64(5)
	now := time.Now().UnixNano()
	l.PushBack("a", 5, longTimeAgoNanos+2, now)
	l.PushBack("b", 4, longTimeAgoNanos, now)

	entry := l.Get("b")
	require.NotNil(t, entry)
	require.Equal(t, longTimeAgoNanos, entry.LastAccessedNanos)
	require.Equal(t, now, entry.LastModifiedNanos)
	require.Equal(t, 4, entry.Value.(int))

	// Verify that lastAccessTime was updated from the last Get
	entry = l.Get("b")
	require.NotNil(t, entry)
	require.Greater(t, entry.LastAccessedNanos, longTimeAgoNanos)
	require.Equal(t, now, entry.LastModifiedNanos)
	require.Equal(t, 4, entry.Value.(int))
}
