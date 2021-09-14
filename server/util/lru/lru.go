package lru

import (
	"container/list"
	"errors"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// EvictedCallback is used to get a callback when a cache Entry is evicted
type EvictedCallback func(value interface{})
type SizeFn func(value interface{}) int64

// Config specifies how the LRU cache is to be constructed.
// MaxSize & SizeFn are required.
type Config struct {
	// Function to calculate size of cache entries.
	SizeFn SizeFn
	// Optional callback for cache eviction events.
	OnEvict EvictedCallback
	// Maximum amount of data to store in the cache.
	// The size of each entry is determined by SizeFn.
	MaxSize int64
}

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	sizeFn      SizeFn
	evictList   *list.List
	items       map[uint64]*list.Element
	onEvict     EvictedCallback
	maxSize     int64
	currentSize int64
}

// Entry is used to hold a value in the evictList
type Entry struct {
	key   uint64
	value interface{}
}

// NewLRU constructs an LRU based on the specified config.
func NewLRU(config *Config) (interfaces.LRU, error) {
	if config.MaxSize <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if config.SizeFn == nil {
		return nil, status.InvalidArgumentError("SizeFn is required")
	}
	c := &LRU{
		currentSize: 0,
		maxSize:     config.MaxSize,
		evictList:   list.New(),
		items:       make(map[uint64]*list.Element),
		onEvict:     config.OnEvict,
		sizeFn:      config.SizeFn,
	}
	return c, nil
}

func (c *LRU) keyHash(key interface{}) uint64 {
	if key == nil {
		return 0
	}

	switch k := key.(type) {
	case uint64:
		return k
	case string:
		return hash.MemHashString(k)
	case []byte:
		return hash.MemHash(k)
	case byte:
		return uint64(k)
	case int:
		return uint64(k)
	case int32:
		return uint64(k)
	case uint32:
		return uint64(k)
	case int64:
		return uint64(k)
	default:
		log.Errorf("LRU-Unhashable key: %+v", key)
		return 0
	}
}

func (c *LRU) Metrics() string {
	return ""
}

// Purge is used to completely clear the cache.
func (c *LRU) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(v.Value.(*Entry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *LRU) Add(key, value interface{}) bool {
	intKey := c.keyHash(key)
	// Check for existing item
	if ent, ok := c.items[intKey]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*Entry).value = value
		return true
	}

	// Add new item
	c.addElement(intKey, value)

	for c.currentSize > c.maxSize {
		c.removeOldest()
	}
	return true
}

// Get looks up a key's value from the cache.
func (c *LRU) Get(key interface{}) (value interface{}, ok bool) {
	intKey := c.keyHash(key)
	if ent, ok := c.items[intKey]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*Entry) == nil {
			return nil, false
		}
		return ent.Value.(*Entry).value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *LRU) Contains(key interface{}) (ok bool) {
	intKey := c.keyHash(key)
	if ent, ok := c.items[intKey]; ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*Entry) == nil {
			return false
		}
		return true
	}
	return false
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *LRU) Peek(key interface{}) (value interface{}, ok bool) {
	intKey := c.keyHash(key)
	var ent *list.Element
	if ent, ok = c.items[intKey]; ok {
		return ent.Value.(*Entry).value, true
	}
	return nil, ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU) Remove(key interface{}) (present bool) {
	intKey := c.keyHash(key)
	if ent, ok := c.items[intKey]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRU) RemoveOldest() (value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*Entry)
		return kv.value, true
	}
	return nil, false
}

// Len returns the number of items in the cache.
func (c *LRU) Len() int {
	return c.evictList.Len()
}

func (c *LRU) Size() int64 {
	return c.currentSize
}

func (c *LRU) MaxSize() int64 {
	return c.maxSize
}

// removeOldest removes the oldest item from the cache.
func (c *LRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// addElement adds a new item to the cache. It does not perform any
// size checks.
func (c *LRU) addElement(intKey uint64, value interface{}) {
	// Add new item
	kv := &Entry{intKey, value}
	Entry := c.evictList.PushFront(kv)
	c.items[intKey] = Entry
	c.currentSize += c.sizeFn(value)
}

// removeElement is used to remove a given list element from the cache
func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*Entry)
	delete(c.items, kv.key)
	c.currentSize -= c.sizeFn(kv.value)
	if c.onEvict != nil {
		c.onEvict(kv.value)
	}
}
