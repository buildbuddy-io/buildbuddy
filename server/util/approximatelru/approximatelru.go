package approximatelru

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

// SizeFn returns the size of the stored item. The sum total of all item sizes
// is compared against the LRU's max size to decide when items need to be
// evicted.
type SizeFn func(value interface{}) int64

// RandomSampleFn is used by the ApproximatedLRU only, and must return
// random, already stored, key-value pairs to be considered for eviction.
type RandomSampleFn func() (interface{}, interface{})

// Config specifies how the LRU cache is to be constructed.
// MaxSize & SizeFn are required.
type Config struct {
	// Function to calculate size of cache entries.
	SizeFn SizeFn
	// Optional callback for cache eviction events.
	OnEvict EvictedCallback
	// Optional callback for random sampling.
	RandomSample RandomSampleFn

	// Maximum amount of data to store in the cache.
	// The size of each entry is determined by SizeFn.
	MaxSize int64
}

// ApproximateLRU implements a non-thread safe fixed size LRU cache.
// No values are actually stored, and only hashed keys are stored, so that very
// little memory is used. Rather than storing a linked-list and moving items to
// the front when they are used -- the ApproximateLRU keeps a map of hashed item
// keys for fast existence checks, and will randomly sample values and pick
// the oldest ones for deletion when the total cache size is larger than the
// max. For more details, see:
//  https://github.com/redis/redis/blob/unstable/src/evict.c#L118 and
//  http://antirez.com/news/109
type ApproximateLRU struct {
	sizeFn       SizeFn
	onEvict      EvictedCallback
	randomSample RandomSampleFn

	evictionPool *list.List
	items        map[uint64][]ApproximateEntry
	maxSize      int64
	currentSize  int64
}

type ApproximateEntry struct {
	key         uint64
	conflictKey uint64
	lastUsed    int64
}

type EvictionSample {
	entry *ApproximateEntry
	value interface{}
}

// NewLRU constructs an LRU based on the specified config.
func NewApproximateLRU(config *Config) (interfaces.LRU, error) {
	if config.MaxSize <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if config.SizeFn == nil {
		return nil, status.InvalidArgumentError("SizeFn is required")
	}
	if config.RandomSample == nil {
		return nil, status.InvalidArgumentError("RandomSample is required")
	}
	c := &ApproximateLRU{
		sizeFn:       config.SizeFn,
		onEvict:      config.OnEvict,
		randomSample: config.RandomSample,

		currentSize:  0,
		maxSize:      config.MaxSize,
		items:        make(map[uint64]ApproximateEntry, 0),
		evictionPool: list.New(),
	}
	return c, nil
}

// Add adds a value to the cache. Returns true if the key was added.
func (c *ApproximateLRU) Add(key, value interface{}) bool {
	pk, ck, ok := keyHash(key)
	if !ok {
		return false
	}
	// Check for existing item
	if v, ok := c.lookupEntry(pk, ck); ok {
		v.lastUsed = time.Now().UnixNano()
		return true
	}

	// Add new item
	c.addItem(pk, ck, value)

	for c.currentSize > c.maxSize {
		c.removeOldest()
	}
	return true
}

// Contains checks if a key is in the cache.
func (c *ApproximateLRU) Contains(key interface{}) bool {
	pk, ck, ok := keyHash(key)
	if !ok {
		return false
	}
	v, ok := c.lookupEntry(pk, ck)
	if ok {
		v.lastUsed = time.Now().UnixNano()
	}
	return ok
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *ApproximateLRU) Remove(key interface{}) (present bool) {
	pk, ck, ok := keyHash(key)
	if !ok {
		return false
	}
	if _, ok := c.lookupEntry(pk, ck); ok {
		c.removeItem(pk, ck)
		return true
	}
	return false
}

func (c *ApproximateLRU) evictionPoolPopulate() {
	for i := 0; i < 5; i++ {
		key, val := c.randomSample()
		pk, ck, ok := keyHash(key)
		if !ok {
			continue
		}
		// Ensure that this item exists in the cache.
		approxEntry, ok := c.lookupEntry(pk, ck)
		if !ok {
			continue
		}
		if approxEntry.lastUsed < c.evictionPool[0].entry.lastUsed {
			kv := &EvictionSample{
				approxEntry: approxEntry,
				value:       val,
			}
			c.evictionPool = append(c.evictionPool, kv)
		}
	}

}
// RemoveOldest removes the oldest item from the cache.
func (c *ApproximateLRU) RemoveOldest() (interface{}, bool) {

	

}

func (c *LRU) Size() int64 {
	return c.currentSize
}

func (c *ApproximateLRU) addItem(key, conflictKey uint64, value interface{}) {
	kv := &ApproximateEntry{
		key:         key,
		conflictKey: conflictKey,
		lastUsed:    time.Now().UnixNano(),
	}
	c.items[key] = append(c.items[key], kv)
	c.currentSize += c.sizeFn(value)
}

func (c *ApproximateLRU) removeItem(key, conflictKey uint64) {
	entries, ok := c.items[key]
	if !ok {
		return
	}
	if len(entries) == 0 {
		c.items[key] = nil
		return
	}

	deleteIndex := -1
	for i, ent := range entries {
		if ent.conflictKey == conflictKey {
			deleteIndex = i
			break
		}
	}
	if deleteIndex != -1 {
		entries[deleteIndex] = entries[len(entries)-1]
		c.items[key] = entries[:len(entries)-1]
	}
}

func (c *ApproximateLRU) lookupEntry(key, conflictKey uint64) (*ApproximateEntry, bool) {
	entries, ok := c.items[key]
	if !ok {
		return nil, false
	}
	for v, ent := range entries {
		if ent.conflictKey == conflictKey {
			return v, true
		}
	}
	return nil, false
}
