package lru

import (
	"container/list"
	"errors"

	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	xxhash "github.com/cespare/xxhash/v2"
)

// EvictionReason describes the reason for an entry being evicted from LRU.
type EvictionReason string

const (
	// SizeEviction means this was the least recently used entry which had to be
	// evicted to make room for newer entries.
	SizeEviction EvictionReason = "size"

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
type EvictedCallback[V any] func(value V, reason EvictionReason)
type SizeFn[V any] func(value V) int64

// Config specifies how the LRU cache is to be constructed.
// MaxSize & SizeFn are required.
type Config[V any] struct {
	// Function to calculate size of cache entries.
	SizeFn SizeFn[V]
	// Optional callback for cache eviction events.
	OnEvict EvictedCallback[V]
	// Maximum amount of data to store in the cache.
	// The size of each entry is determined by SizeFn.
	MaxSize int64
	// Whether adding an item with a key that already exists should update the
	// existing entry's size and value, instead of evicting the old entry and
	// invoking the OnEvict callback.
	UpdateInPlace bool
}

// LRU implements a non-thread safe fixed size LRU cache
type LRU[V any] struct {
	sizeFn        SizeFn[V]
	evictList     *list.List
	items         map[uint64][]*list.Element
	onEvict       EvictedCallback[V]
	maxSize       int64
	currentSize   int64
	updateInPlace bool
}

// Entry is used to hold a value in the evictList
type Entry[V any] struct {
	key         uint64
	conflictKey uint64
	value       V
}

// NewLRU constructs an LRU based on the specified config.
func NewLRU[V any](config *Config[V]) (*LRU[V], error) {
	if config.MaxSize <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if config.SizeFn == nil {
		return nil, status.InvalidArgumentError("SizeFn is required")
	}
	c := &LRU[V]{
		currentSize:   0,
		maxSize:       config.MaxSize,
		evictList:     list.New(),
		items:         make(map[uint64][]*list.Element),
		onEvict:       config.OnEvict,
		sizeFn:        config.SizeFn,
		updateInPlace: config.UpdateInPlace,
	}
	return c, nil
}

// keyHash returns a primary key, a conflict key, and a bool indicating if keys
// were succesfully generated or not.
func (c *LRU[V]) keyHash(k string) (uint64, uint64, bool) {
	return hash.MemHashString(k), xxhash.Sum64String(k), true
}

func (c *LRU[V]) Metrics() string {
	return ""
}

// Purge is used to completely clear the cache.
func (c *LRU[V]) Purge() {
	for k, vals := range c.items {
		for _, v := range vals {
			if c.onEvict != nil {
				c.onEvict(v.Value.(*Entry[V]).value, SizeEviction)
			}
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Add adds a value to the cache. Returns true if the key was added.
func (c *LRU[V]) Add(key string, value V) bool {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}

	if ent, ok := c.lookupItem(pk, ck); ok {
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
			c.addItem(pk, ck, value, c.sizeFn(value), true /*=front*/)
		}
	} else {
		// Add new item
		c.addItem(pk, ck, value, c.sizeFn(value), true /*=front*/)
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
func (c *LRU[V]) PushBack(key string, value V) bool {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}

	size := c.sizeFn(value)
	if ent, ok := c.lookupItem(pk, ck); ok {
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
	c.addItem(pk, ck, value, size, false /*=front*/)
	return true
}

// Get looks up a key's value from the cache.
func (c *LRU[V]) Get(key string) (V, bool) {
	var v V
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return v, false
	}
	if ent, ok := c.lookupItem(pk, ck); ok {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*Entry[V]) == nil {
			return v, false
		}
		return ent.Value.(*Entry[V]).value, true
	}
	return v, false
}

// Contains checks if a key is in the cache.
func (c *LRU[V]) Contains(key string) bool {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}
	if ent, ok := c.lookupItem(pk, ck); ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*Entry[V]) != nil
	}
	return false
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *LRU[V]) Peek(key string) (V, bool) {
	var v V
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return v, false
	}
	var ent *list.Element
	if ent, ok = c.lookupItem(pk, ck); ok {
		return ent.Value.(*Entry[V]).value, true
	}
	return v, ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU[V]) Remove(key string) (present bool) {
	pk, ck, ok := c.keyHash(key)
	if !ok {
		return false
	}
	if ent, ok := c.lookupItem(pk, ck); ok {
		c.removeElement(ent, ManualEviction)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
// The OnEvict callback treats this as a SizeEviction.
func (c *LRU[V]) RemoveOldest() (V, bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent, SizeEviction)
		kv := ent.Value.(*Entry[V])
		return kv.value, true
	}
	var v V
	return v, false
}

// Len returns the number of items in the cache.
func (c *LRU[V]) Len() int {
	return c.evictList.Len()
}

func (c *LRU[V]) Size() int64 {
	return c.currentSize
}

func (c *LRU[V]) MaxSize() int64 {
	return c.maxSize
}

// removeOldest removes the oldest item from the cache.
func (c *LRU[V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent, SizeEviction)
	}
}

func (c *LRU[V]) lookupItem(key, conflictKey uint64) (*list.Element, bool) {
	entries, ok := c.items[key]
	if !ok {
		return nil, false
	}
	for _, ent := range entries {
		if ent.Value.(*Entry[V]).conflictKey == conflictKey {
			return ent, true
		}
	}
	return nil, false
}

// addElement adds a new item to the cache. It does not perform any
// size checks.
func (c *LRU[V]) addItem(key, conflictKey uint64, value V, size int64, front bool) {
	// Add new item
	kv := &Entry[V]{key, conflictKey, value}
	var element *list.Element
	if front {
		element = c.evictList.PushFront(kv)
	} else {
		element = c.evictList.PushBack(kv)
	}
	c.items[key] = append(c.items[key], element)
	c.currentSize += size
}

func (c *LRU[V]) removeItem(key, conflictKey uint64) {
	entries, ok := c.items[key]
	if !ok {
		return
	}

	deleteIndex := -1
	for i, ent := range entries {
		if ent.Value.(*Entry[V]).conflictKey == conflictKey {
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
func (c *LRU[V]) removeElement(e *list.Element, reason EvictionReason) {
	c.evictList.Remove(e)
	kv := e.Value.(*Entry[V])
	c.removeItem(kv.key, kv.conflictKey)
	c.currentSize -= c.sizeFn(kv.value)
	if c.onEvict != nil {
		c.onEvict(kv.value, reason)
	}
}
