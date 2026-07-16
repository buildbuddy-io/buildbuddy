package lru

import (
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

	// SetMaxSize updates the maximum size of the LRU. If the new max is smaller
	// than the current size, the least recently used entries are evicted (with
	// SizeEviction) until the current size fits. New value must be positive.
	SetMaxSize(maxSize int64) error

	// Purge removes all entries from the LRU, invoking the eviction callback for
	// each (with ManualEviction) and releasing the backing storage. The LRU
	// remains usable afterwards.
	Purge()

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

// noIndex is the sentinel "no such entry" index used for prev/next/head/tail.
const noIndex int32 = -1

// node is one entry, stored by value in the lru's backing slice. The intrusive
// doubly-linked recency list is expressed with int32 indices (prev/next) rather
// than pointers, so the whole cache is a single slice + map for the GC to
// trace.
type node[V any] struct {
	key        string
	value      V
	prev, next int32
	size       int64
	expiresAt  int64 // unix nanos; 0 when TTL is disabled
}

// lru is a fixed-size LRU cache. It is not safe for concurrent use unless
// wrapped in a threadSafeLRU (see Config.ThreadSafe).
type lru[V any] struct {
	nodes         []node[V]        // backing storage; indices are stable until freed
	items         map[string]int32 // key -> node index
	free          []int32          // recycled node slots
	head, tail    int32            // most / least recently used indices
	currentSize   int64
	maxSize       int64
	sizeFn        SizeFn[V]
	onEvict       EvictedCallback[V]
	updateInPlace bool
	ttl           time.Duration
	clock         clockwork.Clock
}

// A thread-safe wrapper around an LRU.
type threadSafeLRU[V any] struct {
	mu    sync.Mutex
	inner LRU[V]
}

// New constructs an LRU based on the specified config.
func New[V any](config *Config[V]) (LRU[V], error) {
	if config.MaxSize <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if config.SizeFn == nil {
		return nil, status.InvalidArgumentError("SizeFn is required")
	}
	clock := config.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	var c LRU[V] = &lru[V]{
		items:         make(map[string]int32),
		head:          noIndex,
		tail:          noIndex,
		maxSize:       config.MaxSize,
		sizeFn:        config.SizeFn,
		onEvict:       config.OnEvict,
		updateInPlace: config.UpdateInPlace,
		ttl:           config.TTL,
		clock:         clock,
	}
	if config.ThreadSafe {
		c = &threadSafeLRU[V]{inner: c}
	}
	return c, nil
}

// alloc returns a node slot, reusing a freed one or growing the backing slice.
// Callers must not hold a *node across this call: the slice may reallocate.
func (c *lru[V]) alloc() int32 {
	if n := len(c.free); n > 0 {
		idx := c.free[n-1]
		c.free = c.free[:n-1]
		return idx
	}
	idx := int32(len(c.nodes))
	c.nodes = append(c.nodes, node[V]{})
	return idx
}

func (c *lru[V]) unlink(idx int32) {
	n := &c.nodes[idx]
	if n.prev != noIndex {
		c.nodes[n.prev].next = n.next
	} else {
		c.head = n.next
	}
	if n.next != noIndex {
		c.nodes[n.next].prev = n.prev
	} else {
		c.tail = n.prev
	}
}

// pushFront inserts node idx at the front of the lru (most recently used).
// node must not be in the list already.
func (c *lru[V]) pushFront(idx int32) {
	n := &c.nodes[idx]
	n.prev = noIndex
	n.next = c.head
	if c.head != noIndex {
		c.nodes[c.head].prev = idx
	} else {
		c.tail = idx
	}
	c.head = idx
}

// pushBack inserts node idx at the back of the lru (least recently used).
// node must not be in the list already.
func (c *lru[V]) pushBack(idx int32) {
	n := &c.nodes[idx]
	n.next = noIndex
	n.prev = c.tail
	if c.tail != noIndex {
		c.nodes[c.tail].next = idx
	} else {
		c.head = idx
	}
	c.tail = idx
}

func (c *lru[V]) moveToFront(idx int32) {
	if c.head == idx {
		return
	}
	c.unlink(idx)
	c.pushFront(idx)
}

// insert adds a brand-new entry (the caller must ensure key is absent).
func (c *lru[V]) insert(key string, value V, size int64, front bool) {
	idx := c.alloc()
	n := &c.nodes[idx]
	n.key = key
	n.value = value
	n.size = size
	if c.ttl > 0 {
		n.expiresAt = c.clock.Now().Add(c.ttl).UnixNano()
	}
	if front {
		c.pushFront(idx)
	} else {
		c.pushBack(idx)
	}
	c.items[key] = idx
	c.currentSize += size
}

func (c *lru[V]) removeIndex(idx int32, reason EvictionReason) {
	c.unlink(idx)
	n := &c.nodes[idx]
	delete(c.items, n.key)
	c.currentSize -= n.size
	if c.onEvict != nil {
		c.onEvict(n.key, n.value, reason)
	}
	*n = node[V]{} // release key/value references before recycling the slot.
	c.free = append(c.free, idx)
}

func (c *lru[V]) expired(idx int32) bool {
	return c.ttl > 0 && c.clock.Now().UnixNano() >= c.nodes[idx].expiresAt
}

func (c *lru[V]) Add(key string, value V) bool {
	size := c.sizeFn(value)
	if idx, ok := c.items[key]; ok {
		if c.updateInPlace {
			n := &c.nodes[idx]
			c.currentSize += size - n.size
			n.value = value
			n.size = size
			if c.ttl > 0 {
				n.expiresAt = c.clock.Now().Add(c.ttl).UnixNano()
			}
			c.moveToFront(idx)
		} else {
			// Remove the existing entry (with ConflictEviction) and re-insert.
			c.removeIndex(idx, ConflictEviction)
			c.insert(key, value, size, true /*=front*/)
		}
	} else {
		c.insert(key, value, size, true /*=front*/)
	}
	for c.currentSize > c.maxSize && c.tail != noIndex {
		c.removeIndex(c.tail, SizeEviction)
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
	if idx, ok := c.items[key]; ok {
		sizeDelta := size - c.nodes[idx].size
		if c.currentSize+sizeDelta > c.maxSize {
			return false
		}
		if c.updateInPlace {
			n := &c.nodes[idx]
			n.value = value
			n.size = size
			if c.ttl > 0 {
				n.expiresAt = c.clock.Now().Add(c.ttl).UnixNano()
			}
			c.currentSize += sizeDelta
			return true
		}
		c.removeIndex(idx, ConflictEviction)
	}
	if c.currentSize+size > c.maxSize {
		return false
	}
	c.insert(key, value, size, false /*=front*/)
	return true
}

func (c *lru[V]) Get(key string) (V, bool) {
	if idx, ok := c.items[key]; ok {
		if c.expired(idx) {
			c.removeIndex(idx, TTLEviction)
			var zero V
			return zero, false
		}
		c.moveToFront(idx)
		return c.nodes[idx].value, true
	}
	var zero V
	return zero, false
}

func (c *lru[V]) Contains(key string) bool {
	_, ok := c.Get(key)
	return ok
}

func (c *lru[V]) Remove(key string) bool {
	if idx, ok := c.items[key]; ok {
		c.removeIndex(idx, ManualEviction)
		return true
	}
	return false
}

func (c *lru[V]) RemoveOldest() (V, bool) {
	if c.tail == noIndex {
		var zero V
		return zero, false
	}
	value := c.nodes[c.tail].value
	c.removeIndex(c.tail, SizeEviction)
	return value, true
}

func (c *lru[V]) Len() int {
	return len(c.items)
}

func (c *lru[V]) Size() int64 {
	return c.currentSize
}

func (c *lru[V]) Keys() []string {
	keys := make([]string, 0, len(c.items))
	var now int64
	if c.ttl > 0 {
		now = c.clock.Now().UnixNano()
	}
	for idx := c.head; idx != noIndex; idx = c.nodes[idx].next {
		if c.ttl > 0 && now >= c.nodes[idx].expiresAt {
			// Don't return expired keys.
			continue
		}
		keys = append(keys, c.nodes[idx].key)
	}
	return keys
}

func (c *lru[V]) SetMaxSize(maxSize int64) error {
	if maxSize <= 0 {
		return errors.New("must provide a positive size")
	}
	c.maxSize = maxSize
	// Evict least-recently-used entries until the current size fits. The tail
	// check guards against accessing an empty cache.
	for c.currentSize > c.maxSize && c.tail != noIndex {
		c.removeIndex(c.tail, SizeEviction)
	}
	return nil
}

func (c *lru[V]) Purge() {
	if c.onEvict != nil {
		for idx := c.head; idx != noIndex; idx = c.nodes[idx].next {
			n := &c.nodes[idx]
			c.onEvict(n.key, n.value, ManualEviction)
		}
	}
	// Release the backing storage entirely (reclaims memory); the next insert
	// re-grows it.
	c.nodes = nil
	c.items = make(map[string]int32)
	c.free = nil
	c.head = noIndex
	c.tail = noIndex
	c.currentSize = 0
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

func (c *threadSafeLRU[V]) SetMaxSize(maxSize int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.SetMaxSize(maxSize)
}

func (c *threadSafeLRU[V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inner.Purge()
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
