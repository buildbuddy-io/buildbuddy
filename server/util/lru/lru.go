package lru

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
)

// LRU implements a Least Recently Used cache.
type LRU[V any] interface {
	// Inserts a value into the LRU. A boolean is returned that indicates
	// if the value was successfully added.
	Add(key string, value V) bool

	// Inserts a value into the back of the LRU. A boolean is returned that
	// indicates if the value was successfully added.
	PushBack(key string, value V) bool

	// Gets a value from the LRU, returns a boolean indicating if the value
	// was present.
	Get(key string) (V, bool)

	// Returns a boolean indicating if the value is present in the LRU.
	Contains(key string) bool

	// Removes a value from the LRU, releasing resources associated with
	// that value. Returns a boolean indicating if the value was successfully
	// removed.
	Remove(key string) bool

	// Returns the total "size" of the LRU.
	Size() int64

	// Returns the number of items in the LRU.
	Len() int

	// Remove()s the oldest value in the LRU. (See Remove() above).
	RemoveOldest() (V, bool)

	// Returns all keys in the LRU, ordered from most recently used to least.
	Keys() []string
}

// EvictionReason describes the reason for an entry being evicted from LRU.
type EvictionReason string

const (
	// SizeEviction means this was the least recently used entry which had to be
	// evicted to make room for newer entries.
	SizeEviction EvictionReason = "size"

	// TTLEviction means this entry expired and was removed due to age.
	TTLEviction EvictionReason = "ttl"

	// ManualEviction means this entry was evicted via a call to Remove.
	ManualEviction EvictionReason = "manual"

	// ConflictEviction means this entry was evicted because it was overwritten
	// by another element with a matching key.
	ConflictEviction EvictionReason = "conflict"
)

// EvictedCallback is a function invoked whenever an LRU entry is evicted or
// removed.
//
// It is passed the value which was removed or evicted, as well the reason for
// eviction.
type EvictedCallback[V any] func(key string, value V, reason EvictionReason)
type SizeFn[V any] func(value V) int64

// Config specifies how the LRU cache is to be constructed.
// MaxSize & SizeFn are required.
type Config[V any] struct {
	// Optional clock implementation used by implementations that need one. If
	// not provided a default (real) clock will be used.
	Clock clockwork.Clock

	// A maximum TTL to store objects in the LRU. Time-based eviction happens
	// lazily on read, so items are not evicted immediately if they expire,
	// but they will not be returned by LRU read operations.
	TTL time.Duration

	// Function to calculate size of cache entries.
	SizeFn SizeFn[V]

	// Optional callback for cache eviction events.
	//
	// The eviction callback may be invoked from within LRU methods, so if used
	// with ThreadSafe, it must not call cache methods to avoid deadlock.
	OnEvict EvictedCallback[V]

	// Maximum amount of data to store in the cache.
	// The size of each entry is determined by SizeFn.
	MaxSize int64

	// Whether adding an item with a key that already exists should update the
	// existing entry's size and value, instead of evicting the old entry and
	// invoking the OnEvict callback.
	UpdateInPlace bool

	// Whether to protect individual LRU method calls with a mutex. This makes
	// the returned LRU safe for concurrent use, but it does not make sequences
	// of multiple method calls atomic; callers using compound operations should
	// add their own synchronization.
	//
	// The wrapped LRU may invoke OnEvict synchronously during mutation, so
	// eviction callbacks can run while the mutex is held. Those callbacks must
	// not call back into the same LRU.
	ThreadSafe bool
}

// lru implements a non-thread safe fixed size LRU cache
type lru[V any] struct {
	sizeFn        SizeFn[V]
	evictList     *list.List
	items         map[string]*list.Element
	onEvict       EvictedCallback[V]
	maxSize       int64
	currentSize   int64
	updateInPlace bool
}

// An LRU wrapper that lazily expires entries based on age.
type expiringLRU[V any] struct {
	ttl   time.Duration
	clock clockwork.Clock
	inner *lru[*expiringEntry[V]]
}

// A thread-safe wrapper around an LRU.
type threadSafeLRU[V any] struct {
	mu    sync.Mutex
	inner LRU[V]
}

// Entry is used to hold a value in the evictList
type Entry[V any] struct {
	key   string
	value V
}

type expiringEntry[V any] struct {
	value     V
	createdAt time.Time
}

// New constructs an LRU based on the specified config.
func New[V any](config *Config[V]) (LRU[V], error) {
	if config.MaxSize <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if config.SizeFn == nil {
		return nil, status.InvalidArgumentError("SizeFn is required")
	}
	var c LRU[V]
	if config.TTL > 0 {
		clock := config.Clock
		if clock == nil {
			clock = clockwork.NewRealClock()
		}
		c = &expiringLRU[V]{
			ttl:   config.TTL,
			clock: clock,
			inner: &lru[*expiringEntry[V]]{
				currentSize: 0,
				maxSize:     config.MaxSize,
				evictList:   list.New(),
				items:       make(map[string]*list.Element),
				onEvict: func(key string, value *expiringEntry[V], reason EvictionReason) {
					if config.OnEvict != nil {
						config.OnEvict(key, value.value, reason)
					}
				},
				sizeFn: func(value *expiringEntry[V]) int64 {
					return config.SizeFn(value.value)
				},
				updateInPlace: config.UpdateInPlace,
			},
		}
	} else {
		c = &lru[V]{
			currentSize:   0,
			maxSize:       config.MaxSize,
			evictList:     list.New(),
			items:         make(map[string]*list.Element),
			onEvict:       config.OnEvict,
			sizeFn:        config.SizeFn,
			updateInPlace: config.UpdateInPlace,
		}
	}
	if config.ThreadSafe {
		return &threadSafeLRU[V]{
			inner: c,
		}, nil
	}
	return c, nil
}

func (c *lru[V]) Add(key string, value V) bool {
	if ent, ok := c.items[key]; ok {
		if c.updateInPlace {
			// Replace the existing item, moving it to the front.
			oldSize := c.sizeFn(ent.Value.(*Entry[V]).value)
			newSize := c.sizeFn(value)
			c.currentSize += (newSize - oldSize)
			c.evictList.MoveToFront(ent)
			ent.Value.(*Entry[V]).value = value
		} else {
			// Remove the existing item and re-insert
			c.removeElement(ent, ConflictEviction)
			c.addItem(key, value, c.sizeFn(value), true /*=front*/)
		}
	} else {
		// Add new item
		c.addItem(key, value, c.sizeFn(value), true /*=front*/)
	}

	for c.currentSize > c.maxSize {
		c.removeOldest()
	}
	return true
}

// PushBack adds a value to the back of the cache, but only if there is
// sufficient capacity.
//
// This is useful for populating the cache initially, by iterating over existing
// items in MRU to LRU order and repeatedly calling PushBack on each item.
//
// Returns true if the key was added.
func (c *lru[V]) PushBack(key string, value V) bool {
	size := c.sizeFn(value)
	if ent, ok := c.items[key]; ok {
		// Update or replace the existing item if there is capacity.
		sizeDelta := size - c.sizeFn(ent.Value.(*Entry[V]).value)
		if c.currentSize+sizeDelta > c.maxSize {
			return false
		}
		if c.updateInPlace {
			ent.Value.(*Entry[V]).value = value
			c.currentSize += sizeDelta
			return true
		}
		// Remove the existing item.
		c.removeElement(ent, ConflictEviction)
	}

	if c.currentSize+size > c.maxSize {
		return false
	}

	// Add new item
	c.addItem(key, value, size, false /*=front*/)
	return true
}

func (c *lru[V]) Get(key string) (V, bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*Entry[V]).value, true
	}
	var v V
	return v, false
}

func (c *lru[V]) Contains(key string) bool {
	_, ok := c.Get(key)
	return ok
}

func (c *lru[V]) Remove(key string) (present bool) {
	return c.removeWithReason(key, ManualEviction)
}

func (c *lru[V]) removeWithReason(key string, reason EvictionReason) bool {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent, reason)
		return true
	}
	return false
}

func (c *lru[V]) RemoveOldest() (V, bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent, SizeEviction)
		return ent.Value.(*Entry[V]).value, true
	}
	var v V
	return v, false
}

func (c *lru[V]) Len() int {
	return c.evictList.Len()
}

func (c *lru[V]) Size() int64 {
	return c.currentSize
}

func (c *lru[V]) Keys() []string {
	keys := make([]string, 0, c.evictList.Len())
	for e := c.evictList.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*Entry[V]).key)
	}
	return keys
}

func (c *lru[V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent, SizeEviction)
	}
}

func (c *lru[V]) addItem(key string, value V, size int64, front bool) {
	// Add new item
	kv := &Entry[V]{key, value}
	var element *list.Element
	if front {
		element = c.evictList.PushFront(kv)
	} else {
		element = c.evictList.PushBack(kv)
	}
	c.items[key] = element
	c.currentSize += size
}

func (c *lru[V]) removeElement(e *list.Element, reason EvictionReason) {
	c.evictList.Remove(e)
	kv := e.Value.(*Entry[V])
	delete(c.items, kv.key)
	c.currentSize -= c.sizeFn(kv.value)
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value, reason)
	}
}

func (c *expiringLRU[V]) Add(key string, value V) bool {
	return c.inner.Add(key, c.wrapValue(value))
}

func (c *expiringLRU[V]) PushBack(key string, value V) bool {
	return c.inner.PushBack(key, c.wrapValue(value))
}

func (c *expiringLRU[V]) Get(key string) (V, bool) {
	entry, ok := c.inner.Get(key)
	if !ok {
		var zero V
		return zero, false
	}
	if c.clock.Now().Sub(entry.createdAt) >= c.ttl {
		c.inner.removeWithReason(key, TTLEviction)
		var zero V
		return zero, false
	}
	return entry.value, true
}

func (c *expiringLRU[V]) Contains(key string) bool {
	_, ok := c.Get(key)
	return ok
}

func (c *expiringLRU[V]) Remove(key string) bool {
	return c.inner.Remove(key)
}

func (c *expiringLRU[V]) RemoveOldest() (V, bool) {
	entry, ok := c.inner.RemoveOldest()
	if !ok {
		var zero V
		return zero, false
	}
	return entry.value, true
}

func (c *expiringLRU[V]) Len() int {
	return c.inner.Len()
}

func (c *expiringLRU[V]) Size() int64 {
	return c.inner.Size()
}

func (c *expiringLRU[V]) Keys() []string {
	now := c.clock.Now()
	var keys []string
	for e := c.inner.evictList.Front(); e != nil; e = e.Next() {
		ent := e.Value.(*Entry[*expiringEntry[V]])
		// Don't return expired keys.
		if now.Sub(ent.value.createdAt) < c.ttl {
			keys = append(keys, ent.key)
		}
	}
	return keys
}

func (c *expiringLRU[V]) wrapValue(value V) *expiringEntry[V] {
	return &expiringEntry[V]{
		value:     value,
		createdAt: c.clock.Now(),
	}
}

func (c *threadSafeLRU[V]) Add(key string, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Add(key, value)
}

func (c *threadSafeLRU[V]) PushBack(key string, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.PushBack(key, value)
}

func (c *threadSafeLRU[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Get(key)
}

func (c *threadSafeLRU[V]) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Contains(key)
}

func (c *threadSafeLRU[V]) Remove(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Remove(key)
}

func (c *threadSafeLRU[V]) RemoveOldest() (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.RemoveOldest()
}

func (c *threadSafeLRU[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Len()
}

func (c *threadSafeLRU[V]) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Size()
}

func (c *threadSafeLRU[V]) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Keys()
}
