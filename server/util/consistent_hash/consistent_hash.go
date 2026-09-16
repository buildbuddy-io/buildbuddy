package consistent_hash

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
	"sync/atomic"

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
	numVnodes int
	hashKey   HashFunction
	statePtr  atomic.Pointer[state]
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
	c := &ConsistentHash{
		numVnodes: vnodes,
		hashKey:   hashFunction,
	}
	c.statePtr.Store(&state{keys: make([]int, 0), keyIndexToItemIndex: make([]uint8, 0)})
	return c
}

func (c *ConsistentHash) GetItems() []string {
	return c.statePtr.Load().items
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
	ring := make(map[int]uint8, len(keys)*c.numVnodes)

	for itemIndex, key := range keys {
		for i := 0; i < c.numVnodes; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			hashedKeys = append(hashedKeys, h)
			ring[h] = uint8(itemIndex)
		}
	}
	sort.Ints(hashedKeys)
	// Precompute the mapping from key to item. This doesn't depened on the
	// keys that are passed to Get or GetAllReplicas.
	keyIndexToItemIndex := make([]uint8, len(hashedKeys))
	for i, key := range hashedKeys {
		keyIndexToItemIndex[i] = ring[key]
	}

	c.statePtr.Store(&state{
		keys:                hashedKeys,
		items:               values,
		keyIndexToItemIndex: keyIndexToItemIndex,
	})
}

type state struct {
	keys                []int
	items               []string
	keyIndexToItemIndex []uint8
}

// Get returns the single "item" responsible for the specified key.
func (c *ConsistentHash) Get(key string) string {
	return c.statePtr.Load().get(key, c.hashKey)
}

func (c *state) get(key string, hashKey func(string) int) string {
	if len(c.keys) == 0 {
		return ""
	}
	idx := c.firstKey(key, hashKey)
	r := c.items[c.keyIndexToItemIndex[idx]]
	return r
}

func (c *state) firstKey(key string, hashKey func(string) int) int {
	h := hashKey(key)
	startKeyIdx, _ := slices.BinarySearch(c.keys, h)
	if startKeyIdx == len(c.keys) {
		return 0
	}
	return startKeyIdx
}

func (c *state) lookupVnodes(startKeyIdx int, fn func(vnodeIndex uint8) bool) {
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
	return c.statePtr.Load().getAllReplicas(key, c.hashKey)
}

func (c *state) getAllReplicas(key string, hashKey func(string) int) []string {
	startKeyIdx := c.firstKey(key, hashKey)
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
