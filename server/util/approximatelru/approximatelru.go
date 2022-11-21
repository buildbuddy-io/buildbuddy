// Package approximatelru implements an approximate LRU map.
package approximatelru

import (
	"errors"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	xxhash "github.com/cespare/xxhash/v2"
)

const (
	// The number of items to keep around in the eviction pool between
	// evictions.
	EvictionPoolSize = 16

	// Number of random samples to look at each time the eviction pool is
	// repopulated.
	Samples = 5
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
// MaxSize & SizeFn & RandomSample are required.
type Config struct {
	// Function to calculate size of cache entries.
	SizeFn SizeFn
	// Optional callback for cache eviction events.
	OnEvict EvictedCallback
	// Required callback for random sampling.
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
//
//	https://github.com/redis/redis/blob/unstable/src/evict.c#L118 and
//	http://antirez.com/news/109
type ApproximateLRU struct {
	sizeFn       SizeFn
	onEvict      EvictedCallback
	randomSample RandomSampleFn

	evictionPool []EvictionPoolEntry
	items        map[uint64][]ALRUEntry
	maxSize      int64
	currentSize  int64
}

type ALRUEntry struct {
	key         uint64
	conflictKey uint64
	lastUsed    int64
	size        int64
}

type EvictionPoolEntry struct {
	alruEntry *ALRUEntry
	value     interface{}
}

// New constructs an LRU based on the specified config.
func New(config *Config) (*ApproximateLRU, error) {
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
		items:        make(map[uint64][]ALRUEntry, 0),
		evictionPool: make([]EvictionPoolEntry, 0, EvictionPoolSize),
	}
	return c, nil
}

// keyHash returns a primary key, a conflict key, and a bool indicating if keys
// were succesfully generated or not.
func keyHash(key interface{}) (uint64, uint64, bool) {
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
		if !c.RemoveOldest() {
			log.Warningf("Error evicting items; cache may be oversize.")
			break
		}
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
// key was contained. Note that onEvict is NOT CALLED for items
// that are Removed().
func (c *ApproximateLRU) Remove(key interface{}) (present bool) {
	pk, ck, ok := keyHash(key)
	if !ok {
		return false
	}
	if _, ok := c.lookupEntry(pk, ck); ok {
		return c.removeItem(pk, ck)
	}
	return false
}

func (c *ApproximateLRU) evictionPoolContains(pk, ck uint64) bool {
	for _, evictionSample := range c.evictionPool {
		if evictionSample.alruEntry.key == pk && evictionSample.alruEntry.conflictKey == ck {
			return true
		}
	}
	return false
}

func (c *ApproximateLRU) populateEvictionPool() bool {
	for i := 0; i < Samples; i++ {
		key, val := c.randomSample()
		if key == nil {
			log.Errorf("Sampled key was nil: this should not happen")
			return false
		}
		pk, ck, ok := keyHash(key)
		if !ok {
			log.Errorf("keyhash for sampled value was nil")
			return false
		}
		// Ensure that this item exists in the cache.
		alruEntry, ok := c.lookupEntry(pk, ck)
		if !ok {
			log.Warningf("sampled value was not even in the LRU")
			continue
		}
		// Ensure that this item does not already exist in the eviction
		// pool.
		if c.evictionPoolContains(pk, ck) {
			continue
		}
		c.evictionPool = append(c.evictionPool, EvictionPoolEntry{
			alruEntry: alruEntry,
			value:     val,
		})
	}
	sort.Slice(c.evictionPool, func(i, j int) bool {
		return c.evictionPool[i].alruEntry.lastUsed < c.evictionPool[j].alruEntry.lastUsed
	})
	if len(c.evictionPool) > EvictionPoolSize {
		c.evictionPool = c.evictionPool[:EvictionPoolSize]
	}
	return true
}

func (c *ApproximateLRU) deleteFromEvictionPool() bool {
	for i, evictionSample := range c.evictionPool {
		if _, ok := c.lookupEntry(evictionSample.alruEntry.key, evictionSample.alruEntry.conflictKey); !ok {
			log.Warningf("%d %d was not stored by this LRU, not evicting.", evictionSample.alruEntry.key, evictionSample.alruEntry.conflictKey)
			continue
		}
		removed := c.removeItem(evictionSample.alruEntry.key, evictionSample.alruEntry.conflictKey)
		if c.onEvict != nil {
			c.onEvict(evictionSample.value)
		}
		c.evictionPool = append(c.evictionPool[:i], c.evictionPool[i+1:]...)
		return removed
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *ApproximateLRU) RemoveOldest() bool {
	if !c.populateEvictionPool() {
		return false
	}
	if !c.deleteFromEvictionPool() {
		return false
	}
	return true
}

func (c *ApproximateLRU) Size() int64 {
	return c.currentSize
}

func (c *ApproximateLRU) addItem(key, conflictKey uint64, value interface{}) {
	itemSize := c.sizeFn(value)
	kv := ALRUEntry{
		key:         key,
		conflictKey: conflictKey,
		lastUsed:    time.Now().UnixNano(),
		size:        itemSize,
	}
	c.items[key] = append(c.items[key], kv)
	c.currentSize += itemSize
}

func (c *ApproximateLRU) removeItem(key, conflictKey uint64) bool {
	entries, ok := c.items[key]
	if !ok {
		return false
	}
	if len(entries) == 0 {
		delete(c.items, key)
		return false
	}

	deleteIndex := -1
	for i, ent := range entries {
		if ent.conflictKey == conflictKey {
			deleteIndex = i
			break
		}
	}

	if deleteIndex != -1 {
		c.currentSize -= entries[deleteIndex].size

		if len(entries) == 1 {
			delete(c.items, key)
		} else {
			entries[deleteIndex] = entries[len(entries)-1]
			c.items[key] = entries[:len(entries)-1]
		}
	}
	return true
}

func (c *ApproximateLRU) lookupEntry(key, conflictKey uint64) (*ALRUEntry, bool) {
	entries, ok := c.items[key]
	if !ok {
		return nil, false
	}
	foundIndex := -1
	for i, ent := range entries {
		if ent.conflictKey == conflictKey {
			foundIndex = i
			break
		}
	}
	if foundIndex == -1 {
		return nil, false
	}
	return &c.items[key][foundIndex], true
}
