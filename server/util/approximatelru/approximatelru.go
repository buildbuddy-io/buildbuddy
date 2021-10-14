package approximatelru

import (
	"container/list"
	"errors"
	"time"

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
	size        int64
}

type EvictionSample struct {
	approxEntry *ApproximateEntry
	value interface{}
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
		items:        make(map[uint64][]ApproximateEntry, 0),
		evictionPool: list.New(),
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
		log.Printf("Add: %q was already in the cache.", key)
		return true
	}

	// Add new item
	c.addItem(pk, ck, value)

	log.Printf("Add: Added %q to the cache.", key)
	for c.currentSize > c.maxSize {
		log.Printf("c.currentSize: %d > c.maxSize: %d", c.currentSize, c.maxSize)
		c.RemoveOldest()
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
	log.Printf("Contains: %q val: %+v.", key, v)
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
		c.removeItem(pk, ck)
		return true
	}
	return false
}

func (c *ApproximateLRU) evictionPoolPopulate() {
	for i := 0; i < 5; i++ {
		key, val := c.randomSample()
		if key == nil {
			log.Errorf("Sampled key was nil")
			continue
		}
		pk, ck, ok := keyHash(key)
		if !ok {
			log.Errorf("keyhash for sampled value was nil")
			continue
		}
		// Ensure that this item exists in the cache.
		approxEntry, ok := c.lookupEntry(pk, ck)
		if !ok {
			log.Errorf("sampled value was not even in the LRU")
			continue
		}

		// evictionPool is a list ordered from newest to oldest
		// [t=-1,     t=-4,       t=-5,     t=-5,        t=-6]
		
		sample := EvictionSample{
			approxEntry: approxEntry,
			value: val,
		}

		if c.evictionPool.Len() == 0 {
			c.evictionPool.PushFront(sample)
			continue
		}

		insertionPoint := c.evictionPool.Front()
		for e := c.evictionPool.Front(); e != nil; e = e.Next() {
			if existingSample, ok := e.Value.(EvictionSample); ok {
				if existingSample.approxEntry.lastUsed < sample.approxEntry.lastUsed {
					insertionPoint = e
					break
				}
			}
		}
		c.evictionPool.InsertBefore(sample, insertionPoint)
	}
	log.Printf("c.evictionPool len is now: %d", c.evictionPool.Len())
	for c.evictionPool.Len() > 15 {
		log.Printf("eviction pool was > 15; removing front element %+v", c.evictionPool.Front())
		c.evictionPool.Remove(c.evictionPool.Front())
	}
	c.printEvictionPool()
}

func (c *ApproximateLRU) printEvictionPool() {
	log.Printf("printEvictionPool: %+v", c.evictionPool)
	i := 0
	for e := c.evictionPool.Front(); e != nil; e = e.Next() {
		evictionSample, _ := e.Value.(EvictionSample);
		log.Printf("printEvictionPool evictionPool[%2d] = %d (%+v)", i, evictionSample.approxEntry.lastUsed, evictionSample.approxEntry)
		i += 1
	}
}

func (c *ApproximateLRU) printItems() {
	log.Printf("printItems:")
	for k, v := range c.items {
		log.Printf("  %d = %+v", k, v)
	}
}

func (c *ApproximateLRU) deleteFromEvictionPool() bool {
	c.printItems()
	for e := c.evictionPool.Back(); e != nil; e = e.Prev() {
		if evictSample, ok := e.Value.(EvictionSample); ok {
			log.Printf("deleteFromEvictionPool: considering evictSample: %+v", evictSample.approxEntry)
			_, ok := c.lookupEntry(evictSample.approxEntry.key, evictSample.approxEntry.conflictKey)
			c.evictionPool.Remove(e)
			log.Printf("deleteFromEvictionPool: lookupEntry: %t", ok)
			if ok {
				// This value exists in the LRU, and is the
				// oldest element, according to our LRU sampling
				// algo. Delete it.
				c.removeItem(evictSample.approxEntry.key, evictSample.approxEntry.conflictKey)
				if c.onEvict != nil {
					c.onEvict(evictSample.value)
				}				
				return true
			}
		}
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *ApproximateLRU) RemoveOldest() bool {
	for i := 0; i < 5; i++ {
		c.evictionPoolPopulate()
		if c.deleteFromEvictionPool() {
			break
		}
		log.Warning("Nothing was deleted from the eviction pool")
	}
	return true
}

func (c *ApproximateLRU) Size() int64 {
	return c.currentSize
}

func (c *ApproximateLRU) addItem(key, conflictKey uint64, value interface{}) {
	itemSize := c.sizeFn(value)
	kv := ApproximateEntry{
		key:         key,
		conflictKey: conflictKey,
		lastUsed:    time.Now().UnixNano(),
		size:        itemSize,
	}
	c.items[key] = append(c.items[key], kv)
	c.currentSize += itemSize
}

func (c *ApproximateLRU) removeItem(key, conflictKey uint64) {
	log.Printf("removeItem deleting (%d, %d)", key, conflictKey)
	entries, ok := c.items[key]
	if !ok {
		log.Errorf("removeItem %d: not in items", key)
		return
	}
	if len(entries) == 0 {
		log.Printf("removeItem %d: deleted all entries", key)
		delete(c.items, key)
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
		c.currentSize -= entries[deleteIndex].size

		if len(entries) == 1 {
			delete(c.items, key)
		} else {
			entries[deleteIndex] = entries[len(entries)-1]
			c.items[key] = entries[:len(entries)-1]
		}
	}		
}

func (c *ApproximateLRU) lookupEntry(key, conflictKey uint64) (*ApproximateEntry, bool) {
	entries, ok := c.items[key]
	if !ok {
		return nil, false
	}
	for _, ent := range entries {
		if ent.conflictKey == conflictKey {
			return &ent, true
		}
	}
	return nil, false
}
