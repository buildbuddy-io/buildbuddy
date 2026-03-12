package expiring_lru

import (
	"errors"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/jonboulle/clockwork"
)

type entry[V any] struct {
	value        V
	expiresAfter time.Time
}

// Config specifies how the expiring LRU cache is to be constructed.
//
// Expiration is lazy: expired entries are removed when read via Get or
// Contains. Until then, they still contribute to Len and Size.
type Config[V any] struct {
	TTL time.Duration

	// Clock defaults to a real clock if unset.
	Clock clockwork.Clock

	LRUConfig lru.Config[V]
}

// LRU is a wrapper around an lru.LRU that adds thread-safety and lazy
// time-based expiration on reads.
//
// TODO(iain): use this to replace hand-rolled expiring LRUs in these places:
// - iprules.go
// - authdb.go
// - oci.go
// - podman.go
// - distributed.go
// - claims.go (will require updating expiration logic a bit)
type LRU[V any] struct {
	mu    sync.Mutex
	clock clockwork.Clock
	ttl   time.Duration
	lru   *lru.LRU[*entry[V]]
}

func NewLRU[V any](config *Config[V]) (*LRU[V], error) {
	if config == nil {
		return nil, errors.New("config is required")
	}
	if config.TTL <= 0 {
		return nil, errors.New("must provide a positive TTL")
	}
	if config.LRUConfig.SizeFn == nil {
		return nil, errors.New("SizeFn is required")
	}

	clock := config.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	inner, err := lru.NewLRU(&lru.Config[*entry[V]]{
		MaxSize: config.LRUConfig.MaxSize,
		OnEvict: func(key string, value *entry[V], reason lru.EvictionReason) {
			if config.LRUConfig.OnEvict != nil {
				config.LRUConfig.OnEvict(key, value.value, reason)
			}
		},
		SizeFn: func(value *entry[V]) int64 {
			return config.LRUConfig.SizeFn(value.value)
		},
		UpdateInPlace: config.LRUConfig.UpdateInPlace,
	})
	if err != nil {
		return nil, err
	}

	return &LRU[V]{
		clock: clock,
		ttl:   config.TTL,
		lru:   inner,
	}, nil
}

func (c *LRU[V]) Add(key string, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Add(key, c.newEntry(value))
}

func (c *LRU[V]) PushBack(key string, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.PushBack(key, c.newEntry(value))
}

func (c *LRU[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(key)
}

func (c *LRU[V]) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.get(key)
	return ok
}

func (c *LRU[V]) Remove(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Remove(key)
}

func (c *LRU[V]) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Size()
}

func (c *LRU[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

func (c *LRU[V]) RemoveOldest() (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.lru.RemoveOldest()
	if !ok {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *LRU[V]) get(key string) (V, bool) {
	e, ok := c.lru.Get(key)
	if !ok {
		var zero V
		return zero, false
	}
	if c.clock.Now().After(e.expiresAfter) {
		c.lru.Remove(key)
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *LRU[V]) newEntry(value V) *entry[V] {
	return &entry[V]{
		value:        value,
		expiresAfter: c.clock.Now().Add(c.ttl),
	}
}
