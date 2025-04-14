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
// are multiplied by vnodes. Using 256 allows us to use uint8 values in a few
// places and an array of size 256 instead of a slice for deduplication.
// Increasing this would require changing all fields with uint8 values, and
// rethinking the deduplication strategy.
const maxSize = 256

type ConsistentHash struct {
	keys                []int
	items               []string
	keyIndexToItemIndex []uint8
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
		items:               make([]string, 0),
		keyIndexToItemIndex: make([]uint8, 0),
	}
}

func (c *ConsistentHash) GetItems() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items
}

func (c *ConsistentHash) Set(items ...string) error {
	if len(items) > maxSize {
		return status.InvalidArgumentError("Too many items in consistent hash, max allowed: 256")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = make([]int, 0, len(items)*c.numVnodes)
	ring := make(map[int]uint8, len(items)*c.numVnodes)

	c.items = items
	sort.Strings(c.items)

	for itemIndex, key := range items {
		for i := 0; i < c.numVnodes; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			c.keys = append(c.keys, h)
			ring[h] = uint8(itemIndex)
		}
	}
	sort.Ints(c.keys)
	// Precompute the mapping from key to item. This doesn't depened on the
	// keys that are passed to Get or GetAllReplicas.
	c.keyIndexToItemIndex = make([]uint8, len(c.keys))
	for i, key := range c.keys {
		c.keyIndexToItemIndex[i] = ring[key]
	}
	return nil
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

func (c *ConsistentHash) lookupVnodes(startKeyIdx int, fn func(vnodeIndex uint8) bool) {
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
	var replicaSet [maxSize]bool // This doesn't allocate since it's on the stack.
	replicaSet[originalIndex] = true

	c.lookupVnodes(startKeyIdx, func(vnodeIndex uint8) bool {
		// If we already visited this vnode's corresponding replica, skip.
		if replicaSet[vnodeIndex] {
			return false
		}
		replicaSet[vnodeIndex] = true
		replicas = append(replicas, c.items[vnodeIndex])
		return len(replicas) == len(c.items)
	})

	return replicas
}
