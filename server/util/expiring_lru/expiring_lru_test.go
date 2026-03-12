package expiring_lru_test

import (
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/expiring_lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

type eviction struct {
	key    string
	value  int
	reason lru.EvictionReason
}

func newCache(t *testing.T, clock clockwork.Clock, ttl time.Duration, maxSize int64, onEvict func(string, int, lru.EvictionReason)) *expiring_lru.LRU[int] {
	t.Helper()
	c, err := expiring_lru.NewLRU(&expiring_lru.Config[int]{
		Clock: clock,
		TTL:   ttl,
		LRUConfig: lru.Config[int]{
			MaxSize: maxSize,
			SizeFn:  func(v int) int64 { return int64(v) },
			OnEvict: onEvict,
		},
	})
	require.NoError(t, err)
	return c
}

func TestGetExpiresEntry(t *testing.T) {
	clock := clockwork.NewFakeClock()
	evictions := []eviction{}
	c := newCache(t, clock, 5*time.Second, 10, func(key string, value int, reason lru.EvictionReason) {
		evictions = append(evictions, eviction{key: key, value: value, reason: reason})
	})

	require.True(t, c.Add("a", 1))
	clock.Advance(5*time.Second + time.Nanosecond)

	_, ok := c.Get("a")
	require.False(t, ok)
	require.Equal(t, 0, c.Len())
	require.Equal(t, []eviction{{key: "a", value: 1, reason: lru.ManualEviction}}, evictions)
}

func TestContainsExpiresEntry(t *testing.T) {
	clock := clockwork.NewFakeClock()
	c := newCache(t, clock, 5*time.Second, 10, nil)

	require.True(t, c.Add("a", 1))
	require.True(t, c.Contains("a"))

	clock.Advance(5*time.Second + time.Nanosecond)

	require.False(t, c.Contains("a"))
	require.Equal(t, 0, c.Len())
}

func TestAddResetsExpiration(t *testing.T) {
	clock := clockwork.NewFakeClock()
	c := newCache(t, clock, 5*time.Second, 10, nil)

	require.True(t, c.Add("a", 1))
	clock.Advance(3 * time.Second)
	require.True(t, c.Add("a", 2))
	clock.Advance(3 * time.Second)

	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

func TestPushBackAppliesExpiration(t *testing.T) {
	clock := clockwork.NewFakeClock()
	c := newCache(t, clock, 5*time.Second, 10, nil)

	require.True(t, c.PushBack("a", 1))
	clock.Advance(5*time.Second + time.Nanosecond)

	_, ok := c.Get("a")
	require.False(t, ok)
}

func TestOnEvictReceivesUnwrappedValue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	evictions := []eviction{}
	c := newCache(t, clock, time.Hour, 2, func(key string, value int, reason lru.EvictionReason) {
		evictions = append(evictions, eviction{key: key, value: value, reason: reason})
	})

	require.True(t, c.Add("a", 1))
	require.True(t, c.Add("b", 1))
	require.True(t, c.Add("c", 1))

	require.Equal(t, []eviction{{key: "a", value: 1, reason: lru.SizeEviction}}, evictions)
}

func TestConcurrentAccess(t *testing.T) {
	clock := clockwork.NewFakeClock()
	c := newCache(t, clock, time.Hour, 100, nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				key := string(rune('a' + (j % 8)))
				c.Add(key, id+j)
				c.Contains(key)
				c.Get(key)
				if j%5 == 0 {
					c.Remove(key)
				}
			}
		}(i)
	}
	wg.Wait()
}
