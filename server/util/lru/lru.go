package lru

import (
	"container/list"
	"errors"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	xxhash "github.com/cespare/xxhash/v2"
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
	items       map[uint64][]*list.Element
	onEvict     EvictedCallback
	maxSize     int64
	currentSize int64
}

// Entry is used to hold a value in the evictList
type Entry struct {
	key         uint64
	conflictKey uint64
	value       interface{}
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
		items:       make(map[uint64][]*list.Element),
		onEvict:     config.OnEvict,
		sizeFn:      config.SizeFn,
	}
	return c, nil
}

// keyHash returns a primary key, a conflict key, and a bool indicating if keys
// were succesfully generated or not.
func (c *LRU) keyHash(key interface{}) (uint64, uint64, bool) {
	if key == nil {
		log.Errorf("LRU nil key: %+v", key)
		return 0, 0, false
	}

	switch k := key.(type) {
	case string:
		return hash.MemHashString(k), xxhash.Sum64String(k), true
	case []byte:
		return hash.MemHash(k), xxhash.Sum64(k), true
	default:
		log.Errorf("LRU unhashable key: %+v", key)
		return 0, 0, false
	}
}

func (c *LRU) Metrics() string {
	return ""
}

// Purge is used to completely clear the cache.
func (c *LRU) Purge() {
	for k, vals := range c.items {
		for _, v := range vals {
			if c.onEvict != nil {
				c.onEvict(v.Value.(*Entry).value)
			}
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache. Returns true if the key was added.
func (c *LRU) Add(key, value interface{}) bool {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}
	// Check for existing item
	if ent, ok := c.lookupItem(pk, ck); ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*Entry).value = value
		return true
	}

	// Add new item
	c.addItem(pk, ck, value, true /*=front*/)

	for c.currentSize > c.maxSize {
		c.removeOldest()
	}
	return true
}

// PushBack adds a value to the back of the cache. Returns true if the key was added.
func (c *LRU) PushBack(key, value interface{}) bool {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}
	// Check for existing item
	if ent, ok := c.lookupItem(pk, ck); ok {
		ent.Value.(*Entry).value = value
		return true
	}

	// Add new item
	c.addItem(pk, ck, value, false /*=front*/)

	for c.currentSize > c.maxSize {
		c.removeOldest()
		return false
	}
	return true
}

// Get looks up a key's value from the cache.
func (c *LRU) Get(key interface{}) (interface{}, bool) {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return nil, false
	}
	if ent, ok := c.lookupItem(pk, ck); ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*Entry) == nil {
			return nil, false
		}
		return ent.Value.(*Entry).value, true
	}
	return nil, false
}

// Contains checks if a key is in the cache.
func (c *LRU) Contains(key interface{}) bool {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}
	if ent, ok := c.lookupItem(pk, ck); ok {
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
func (c *LRU) Peek(key interface{}) (interface{}, bool) {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return nil, false
	}
	var ent *list.Element
	if ent, ok = c.lookupItem(pk, ck); ok {
		return ent.Value.(*Entry).value, true
	}
	return nil, ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU) Remove(key interface{}) (present bool) {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}
	if ent, ok := c.lookupItem(pk, ck); ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRU) RemoveOldest() (interface{}, bool) {
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

func (c *LRU) lookupItem(key, conflictKey uint64) (*list.Element, bool) {
	entries, ok := c.items[key]
	if !ok {
		return nil, false
	}
	for _, ent := range entries {
		if ent.Value.(*Entry).conflictKey == conflictKey {
			return ent, true
		}
	}
	return nil, false
}

// addElement adds a new item to the cache. It does not perform any
// size checks.
func (c *LRU) addItem(key, conflictKey uint64, value interface{}, front bool) {
	// Add new item
	kv := &Entry{key, conflictKey, value}
	var element *list.Element
	if front {
		element = c.evictList.PushFront(kv)
	} else {
		element = c.evictList.PushBack(kv)
	}
	c.items[key] = append(c.items[key], element)
	c.currentSize += c.sizeFn(value)
}

func (c *LRU) removeItem(key, conflictKey uint64) {
	entries, ok := c.items[key]
	if !ok {
		return
	}

	deleteIndex := -1
	for i, ent := range entries {
		if ent.Value.(*Entry).conflictKey == conflictKey {
			deleteIndex = i
			break
		}
	}
	if deleteIndex != -1 {
		if len(entries) == 1 {
			delete(c.items, key)
			return
		}
		entries[deleteIndex] = entries[len(entries)-1]
		c.items[key] = entries[:len(entries)-1]
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*Entry)
	c.removeItem(kv.key, kv.conflictKey)
	c.currentSize -= c.sizeFn(kv.value)
	if c.onEvict != nil {
		c.onEvict(kv.value)
	}
}
