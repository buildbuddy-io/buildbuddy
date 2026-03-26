package lru_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

type eviction struct {
	key    string
	value  int
	reason lru.EvictionReason
}

func testConfigs[V any](t *testing.T, fn func(t *testing.T, config *lru.Config[V])) {
	t.Helper()
	clock := clockwork.NewFakeClock()
	configs := map[string]*lru.Config[V]{
		"LRU":                   {},
		"ThreadSafeLRU":         {ThreadSafe: true},
		"ExpiringLRU":           {TTL: time.Hour, Clock: clock},
		"ExpiringThreadSafeLRU": {TTL: time.Hour, Clock: clock, ThreadSafe: true},
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			fn(t, config)
		})
	}
}

func TestAdd(t *testing.T) {
	testConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		evictions := []eviction{}

		cfg := *config
		cfg.MaxSize = 10
		cfg.OnEvict = func(key string, value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{key, value, reason})
		}
		cfg.SizeFn = func(value int) int64 { return int64(value) }
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		require.True(t, l.Add("a", 5))
		require.True(t, l.Add("b", 4))
		require.True(t, l.Add("c", 3))
		require.Equal(t, []eviction{{"a", 5, lru.SizeEviction}}, evictions)

		// Now overwrite "c" so that its new size exceeds the max cache size;
		// should *remove* the existing "c" entry since it'll be overwritten, and
		// then *evict* the "b" entry to make room for the new "c".
		require.True(t, l.Add("c", 10))
		require.Equal(
			t,
			[]eviction{
				{"a", 5, lru.SizeEviction},
				{"c", 3, lru.ConflictEviction},
				{"b", 4, lru.SizeEviction},
			},
			evictions)
	})
}

func TestAdd_UpdateInPlace(t *testing.T) {
	testConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		evictions := []eviction{}

		cfg := *config
		cfg.MaxSize = 10
		cfg.OnEvict = func(key string, value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{key, value, reason})
		}
		cfg.SizeFn = func(value int) int64 { return int64(value) }
		cfg.UpdateInPlace = true
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		require.True(t, l.Add("a", 5))
		require.True(t, l.Add("b", 4))
		require.True(t, l.Add("c", 3))
		require.Equal(t, []eviction{{"a", 5, lru.SizeEviction}}, evictions)

		require.True(t, l.Add("c", 6))
		require.Equal(
			t, []eviction{{"a", 5, lru.SizeEviction}}, evictions,
			"c entry should be updated in place without causing any evictions")

		require.True(t, l.Add("c", 7))
		require.Equal(
			t, []eviction{{"a", 5, lru.SizeEviction}, {"b", 4, lru.SizeEviction}}, evictions,
			"c entry should be updated in place but evict b to make room")
	})
}

func TestPushBack(t *testing.T) {
	testConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		evictions := []eviction{}

		cfg := *config
		cfg.MaxSize = 10
		cfg.OnEvict = func(key string, value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{key, value, reason})
		}
		cfg.SizeFn = func(value int) int64 { return int64(value) }
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		require.True(t, l.PushBack("a", 5))
		require.True(t, l.PushBack("b", 4))
		require.False(t, l.PushBack("c", 3), "c should not be added since there isn't enough capacity")
		require.Empty(t, evictions)

		// Now attempt to overwrite "b" with a size that would exceed the cache
		// capacity; this should fail.
		require.False(t, l.PushBack("b", 10))
		require.Empty(t, evictions)
	})
}

func TestRemoveOldest(t *testing.T) {
	testConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		evictions := []eviction{}

		cfg := *config
		cfg.MaxSize = 10
		cfg.OnEvict = func(key string, value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{key, value, reason})
		}
		cfg.SizeFn = func(value int) int64 { return int64(value) }
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		require.True(t, l.Add("a", 5))
		require.True(t, l.Add("b", 4))
		require.Empty(t, evictions)

		oldest, ok := l.RemoveOldest()
		require.True(t, ok)
		require.Equal(t, 5, oldest)
		require.Equal(t, []eviction{{"a", 5, lru.SizeEviction}}, evictions)

		oldest, ok = l.RemoveOldest()
		require.True(t, ok)
		require.Equal(t, 4, oldest)
		require.Equal(t, []eviction{{"a", 5, lru.SizeEviction}, {"b", 4, lru.SizeEviction}}, evictions)
	})
}

func testExpiringConfigs[V any](t *testing.T, fn func(t *testing.T, config *lru.Config[V])) {
	t.Helper()
	configs := map[string]*lru.Config[V]{
		"ExpiringLRU": {
			MaxSize: 10,
			SizeFn:  func(value V) int64 { return int64(1) },
			TTL:     5 * time.Second,
		},
		"ExpiringThreadSafeLRU": {
			MaxSize:    10,
			SizeFn:     func(value V) int64 { return int64(1) },
			TTL:        5 * time.Second,
			ThreadSafe: true,
		},
	}
	for name, config := range configs {
		config.Clock = clockwork.NewFakeClock()
		t.Run(name, func(t *testing.T) {
			fn(t, config)
		})
	}
}

func TestExpiringLRU_GetAfterExpiry(t *testing.T) {
	testExpiringConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		l, err := lru.New[int](config)
		require.NoError(t, err)

		l.Add("a", 1)
		config.Clock.(*clockwork.FakeClock).Advance(6 * time.Second)
		v, ok := l.Get("a")
		require.False(t, ok)
		require.Zero(t, v)
	})
}

func TestExpiringLRU_ContainsAfterExpiry(t *testing.T) {
	testExpiringConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		l, err := lru.New[int](config)
		require.NoError(t, err)

		l.Add("a", 1)
		require.True(t, l.Contains("a"))
		config.Clock.(*clockwork.FakeClock).Advance(6 * time.Second)
		require.False(t, l.Contains("a"))
	})
}

func TestExpiringLRU_TTLEvictionCallback(t *testing.T) {
	testExpiringConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		evictions := []eviction{}
		cfg := *config
		cfg.OnEvict = func(key string, value int, reason lru.EvictionReason) {
			evictions = append(evictions, eviction{key, value, reason})
		}
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		l.Add("a", 1)
		config.Clock.(*clockwork.FakeClock).Advance(6 * time.Second)
		l.Get("a")
		require.Equal(t, []eviction{{"a", 1, lru.TTLEviction}}, evictions)
	})
}

func TestExpiringLRU_AddResetsTTL(t *testing.T) {
	testExpiringConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		cfg := *config
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		l.Add("a", 1)
		config.Clock.(*clockwork.FakeClock).Advance(4 * time.Second)
		l.Add("a", 2)
		config.Clock.(*clockwork.FakeClock).Advance(4 * time.Second)
		v, ok := l.Get("a")
		require.True(t, ok, "item should not have expired since it was re-added")
		require.Equal(t, 2, v)
	})
}

func TestKeys(t *testing.T) {
	testConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		cfg := *config
		cfg.MaxSize = 100
		cfg.SizeFn = func(value int) int64 { return int64(value) }
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		require.Empty(t, l.Keys())

		l.Add("a", 1)
		l.Add("b", 1)
		l.Add("c", 1)
		require.Equal(t, []string{"c", "b", "a"}, l.Keys())

		// Accessing "a" moves it to the front.
		l.Get("a")
		require.Equal(t, []string{"a", "c", "b"}, l.Keys())

		l.Remove("c")
		require.Equal(t, []string{"a", "b"}, l.Keys())
	})
}

func TestExpiringLRU_KeysExcludesExpired(t *testing.T) {
	testExpiringConfigs(t, func(t *testing.T, config *lru.Config[int]) {
		cfg := *config
		l, err := lru.New[int](&cfg)
		require.NoError(t, err)

		l.Add("a", 1)
		config.Clock.(*clockwork.FakeClock).Advance(3 * time.Second)
		l.Add("b", 2)

		// "a" and "b" are both alive.
		require.Equal(t, []string{"b", "a"}, l.Keys())

		// Advance past "a"'s TTL but not "b"'s.
		config.Clock.(*clockwork.FakeClock).Advance(3 * time.Second)

		// "a" is expired; Keys() should not include it.
		require.Equal(t, []string{"b"}, l.Keys())
	})
}

func TestThreadSafeLRU_ConcurrentAccess(t *testing.T) {
	l, err := lru.New[int](&lru.Config[int]{
		MaxSize:    10,
		SizeFn:     func(value int) int64 { return int64(value) },
		ThreadSafe: true,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%8)
			l.Add(key, 1)
			l.Get(key)
			l.Contains(key)
			if i%3 == 0 {
				l.Remove(key)
			}
		}(i)
	}
	wg.Wait()

	require.LessOrEqual(t, l.Len(), 10)
}
