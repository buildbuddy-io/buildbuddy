package lru

import (
	"container/list"
	"errors"
)

// EvictCallback is used to get a callback when a cache Entry is evicted
type EvictedCallback func(key interface{}, value interface{})
type AddedCallback func(key interface{}, value interface{})
type SizeFn func(key interface{}, value interface{}) int64

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	currentSize int64
	maxSize     int64
	evictList   *list.List
	items       map[interface{}]*list.Element
	onEvict     EvictedCallback
	onAdd       AddedCallback
	sizeFn      SizeFn
}

// Entry is used to hold a value in the evictList
type Entry struct {
	key   interface{}
	value interface{}
}

// NewLRU constructs an LRU of the given size
func NewLRU(maxSize int64, onEvict EvictedCallback, onAdd AddedCallback, sizeFn SizeFn) (*LRU, error) {
	if maxSize <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	c := &LRU{
		currentSize: 0,
		maxSize:     maxSize,
		evictList:   list.New(),
		items:       make(map[interface{}]*list.Element),
		onEvict:     onEvict,
		onAdd:       onAdd,
		sizeFn:      sizeFn,
	}
	return c, nil
}

// Purge is used to completely clear the cache.
func (c *LRU) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value.(*Entry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *LRU) Add(key, value interface{}) (evicted bool) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*Entry).value = value
		return false
	}

	// Add new item
	c.addElement(key, value)

	evict := c.currentSize > c.maxSize
	for c.currentSize > c.maxSize {
		c.removeOldest()
	}
	return evict
}

// Get looks up a key's value from the cache.
func (c *LRU) Get(key interface{}) (value interface{}, ok bool) {
	if ent, ok := c.items[key]; ok {
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
	if ent, ok := c.items[key]; ok {
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
	var ent *list.Element
	if ent, ok = c.items[key]; ok {
		return ent.Value.(*Entry).value, true
	}
	return nil, ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU) Remove(key interface{}) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRU) RemoveOldest() (key, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*Entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// GetOldest returns the oldest Entry
func (c *LRU) GetOldest() (key, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*Entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *LRU) Keys() []interface{} {
	keys := make([]interface{}, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*Entry).key
		i++
	}
	return keys
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
func (c *LRU) addElement(key, value interface{}) {
	// Add new item
	kv := &Entry{key, value}
	Entry := c.evictList.PushFront(kv)
	c.items[key] = Entry
	c.currentSize += c.sizeFn(key, value)
	if c.onAdd != nil {
		c.onAdd(kv.key, kv.value)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*Entry)
	delete(c.items, kv.key)
	c.currentSize -= c.sizeFn(kv.key, kv.value)
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
