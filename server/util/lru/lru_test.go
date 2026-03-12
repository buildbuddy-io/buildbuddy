package lru_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/stretchr/testify/require"
)

type eviction struct {
	key    string
	value  int
	reason lru.EvictionReason
}

func testConfigs[V any](t *testing.T, fn func(t *testing.T, config *lru.Config[V])) {
	t.Helper()
	configs := map[string]*lru.Config[V]{
		"LRU": {},
		"ThreadSafeLRU": {
			ThreadSafe: true,
		},
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
