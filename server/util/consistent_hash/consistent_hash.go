package consistent_hash

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// HashFunction is the hash function used to map values to a location on the
// consistent hash ring.
type HashFunction func(string) int

var (
	// SHA256 is a consistent hash function that uses the first 8 bytes from the
	// SHA256 checksum of the key, interpreted as a big-endian uint64.
	SHA256 HashFunction = func(key string) int {
		sum := sha256.Sum256([]byte(key))
		return int(binary.BigEndian.Uint64(sum[:]))
	}

	// CRC32 is a consistent hash function based on the CRC32 checksum of the
	// key. We are currently migrating away from this, since it can result in
	// uneven load distribution.
	CRC32 HashFunction = func(key string) int {
		return int(crc32.ChecksumIEEE([]byte(key)))
	}
)

// The maximum number of items that can be passed to Set(). This is before they
// are multiplied by vnodes. It bounds the size of the fixed-size bitset used
// for deduplication in GetAllReplicas, which lives on the stack, so it should
// stay small (a few KB at most).
const maxSize = 4096

// Compile-time check that maxSize is a multiple of 64, so the bitset below
// covers every index exactly. Negating a non-zero unsigned constant is a
// compile error ("constant overflows uint").
const _ = -uint(maxSize % 64)

// itemIndex is the type used to index into the items slice. It is narrower
// than int to keep keyIndexToItemIndex compact.
type itemIndex uint16

// Compile-time check that every index below maxSize fits in itemIndex; this
// fails with a constant overflow error if maxSize is raised too far.
const _ = itemIndex(maxSize - 1)

// replicaBitset tracks which items have been seen while walking the ring.
type replicaBitset [maxSize / 64]uint64

func (b *replicaBitset) has(i itemIndex) bool {
	return b[i/64]&(1<<(i%64)) != 0
}

func (b *replicaBitset) add(i itemIndex) {
	b[i/64] |= 1 << (i % 64)
}

type ConsistentHash struct {
	keys                []int
	items               []string
	keyIndexToItemIndex []itemIndex
	numVnodes           int
	hashKey             HashFunction
	mu                  sync.RWMutex
}

// NewConsistentHash returns a new consistent hash ring.
//
// The given hash function is used to map vnodes and keys to positions on the
// ring. Ideally, the hash function produces random-looking outputs for distinct
// inputs.
//
// vnodes decides how many copies of each server replica to place on the ring.
// See https://en.wikipedia.org/wiki/Consistent_hashing#Variance_reduction
func NewConsistentHash(hashFunction HashFunction, vnodes int) *ConsistentHash {
	return &ConsistentHash{
		numVnodes:           vnodes,
		hashKey:             hashFunction,
		keys:                make([]int, 0),
		keyIndexToItemIndex: make([]itemIndex, 0),
	}
}

func (c *ConsistentHash) GetItems() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.items
}

func (c *ConsistentHash) Set(items ...string) error {
	if len(items) > maxSize {
		return status.InvalidArgumentErrorf("Too many items in consistent hash, max allowed: %v", maxSize)
	}
	sort.Strings(items)
	c.set(items, items)
	return nil
}

// SetFromMap builds the hash ring using the map keys as ring keys (for
// hashing / vnode placement) and the map values as the strings returned
// by Get, GetAllReplicas, and GetItems.
func (c *ConsistentHash) SetFromMap(m map[string]string) error {
	if len(m) > maxSize {
		return status.InvalidArgumentErrorf("Too many items in consistent hash, max allowed: %v", maxSize)
	}
	// Sort both keys and values by key
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]string, len(keys))
	for i, k := range keys {
		values[i] = m[k]
	}
	c.set(keys, values)
	return nil
}

func (c *ConsistentHash) set(keys, values []string) {
	// numVnodes and hashKey are immutable after construction, so the ring can
	// be built without holding the lock. The lock is only taken to swap in the
	// fully-built result.
	hashedKeys := make([]int, 0, len(keys)*c.numVnodes)
	ring := make(map[int]itemIndex, len(keys)*c.numVnodes)

	for idx, key := range keys {
		for i := 0; i < c.numVnodes; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			hashedKeys = append(hashedKeys, h)
			ring[h] = itemIndex(idx)
		}
	}
	sort.Ints(hashedKeys)
	// Precompute the mapping from key to item. This doesn't depened on the
	// keys that are passed to Get or GetAllReplicas.
	keyIndexToItemIndex := make([]itemIndex, len(hashedKeys))
	for i, key := range hashedKeys {
		keyIndexToItemIndex[i] = ring[key]
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = hashedKeys
	c.items = values
	c.keyIndexToItemIndex = keyIndexToItemIndex
}

// Get returns the single "item" responsible for the specified key.
func (c *ConsistentHash) Get(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.keys) == 0 {
		return ""
	}
	idx := c.firstKey(key)
	r := c.items[c.keyIndexToItemIndex[idx]]
	return r
}

func (c *ConsistentHash) firstKey(key string) int {
	h := c.hashKey(key)
	startKeyIdx, _ := slices.BinarySearch(c.keys, h)
	if startKeyIdx == len(c.keys) {
		return 0
	}
	return startKeyIdx
}

func (c *ConsistentHash) lookupVnodes(startKeyIdx int, fn func(vnodeIndex itemIndex) bool) {
	done := false
	for offset := 1; offset < len(c.keys) && !done; offset += 1 {
		keyIdx := (startKeyIdx + offset)
		if keyIdx >= len(c.keyIndexToItemIndex) {
			// This is 20% faster than always modding by
			// len(c.keyIndexToItemIndex) every iteration.
			startKeyIdx -= len(c.keyIndexToItemIndex)
			keyIdx -= len(c.keyIndexToItemIndex)
		}
		done = fn(c.keyIndexToItemIndex[keyIdx])
	}
}

func (c *ConsistentHash) GetAllReplicas(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.keys) == 0 {
		return nil
	}
	startKeyIdx := c.firstKey(key)
	originalIndex := c.keyIndexToItemIndex[startKeyIdx]

	replicas := make([]string, 0, len(c.items))
	replicas = append(replicas, c.items[originalIndex])
	var replicaSet replicaBitset // This doesn't allocate since it's on the stack.
	replicaSet.add(originalIndex)

	c.lookupVnodes(startKeyIdx, func(vnodeIndex itemIndex) bool {
		// If we already visited this vnode's corresponding replica, skip.
		if replicaSet.has(vnodeIndex) {
			return false
		}
		replicaSet.add(vnodeIndex)
		replicas = append(replicas, c.items[vnodeIndex])
		return len(replicas) == len(c.items)
	})

	return replicas
}
