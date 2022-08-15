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

	// Verify that updates work
	v := l.Get("c")
	lastModified1 := v.LastModifiedNanos
	require.NotNil(t, v)
	require.Equal(t, 3, v.Value.(int))

	require.True(t, l.Add("c", 7))
	v = l.Get("c")
	lastModified2 := v.LastModifiedNanos
	require.NotNil(t, v)
	require.Equal(t, 7, v.Value.(int))
	require.Greater(t, lastModified2, lastModified1)
}

func TestPushBack(t *testing.T) {
	evictions := make([]int, 0)

	l, err := lru.NewLRU(&lru.Config{
		MaxSize: 10,
		OnEvict: func(value interface{}) { evictions = append(evictions, value.(int)) },
		SizeFn:  func(value interface{}) int64 { return int64(value.(int)) },
	})
	require.Nil(t, err)

	timestamp1 := int64(100)
	require.True(t, l.PushBack("a", 5, timestamp1, timestamp1))
	require.True(t, l.PushBack("b", 4, timestamp1, timestamp1))
	require.False(t, l.PushBack("c", 3, timestamp1, timestamp1))
	require.Equal(t, 1, len(evictions))
	require.Equal(t, 3, evictions[0])

	// Verify that updates work
	timestamp2 := int64(200)
	timestamp3 := int64(300)
	require.True(t, l.PushBack("b", 6, timestamp2, timestamp3))
	v := l.Get("b")
	require.NotNil(t, v)
	require.Equal(t, 6, v.Value.(int))
	require.Equal(t, timestamp2, v.LastAccessedNanos)
	require.Equal(t, timestamp3, v.LastModifiedNanos)
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
