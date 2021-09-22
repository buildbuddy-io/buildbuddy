package consistent_hash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const defaultNumReplicas = 100

type ConsistentHash struct {
	ring        map[int]uint8
	keys        []int
	items       []string
	numReplicas int
	mu          sync.RWMutex
	replicaMu   sync.RWMutex
	replicaSets map[uint32][]string
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		numReplicas: defaultNumReplicas,
		keys:        make([]int, 0),
		ring:        make(map[int]uint8, 0),
		items:       make([]string, 0),
		replicaSets: make(map[uint32][]string, 0),
	}
}

func (c *ConsistentHash) hashKey(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}

func (c *ConsistentHash) GetItems() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items
}

func (c *ConsistentHash) Set(items ...string) error {
	if len(items) > 256 {
		return status.InvalidArgumentError("Too many items in consistent hash, max allowed: 256")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = make([]int, 0)
	c.ring = make(map[int]uint8, 0)
	c.items = items
	sort.Strings(c.items)

	for itemIndex, key := range items {
		for i := 0; i < c.numReplicas; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			c.keys = append(c.keys, h)
			c.ring[h] = uint8(itemIndex)
		}
	}
	sort.Ints(c.keys)
	return nil
}

// Get returns the single "item" responsible for the specified key.
func (c *ConsistentHash) Get(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.keys) == 0 {
		return ""
	}
	h := c.hashKey(key)
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= h
	})
	if idx == len(c.keys) {
		idx = 0
	}
	return c.items[c.ring[c.keys[idx]]]
}

func (c *ConsistentHash) lookupReplicas(idx int, fn func(replicaIndex uint8)) {
	for offset := 1; offset < len(c.keys); offset += 1 {
		newIdx := (idx + offset) % len(c.keys)
		fn(c.ring[c.keys[newIdx]])
	}
}

func (c *ConsistentHash) GetAllReplicas(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.keys) == 0 {
		return nil
	}
	h := c.hashKey(key)
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= h
	})
	if idx == len(c.keys) {
		idx = 0
	}

	// first, attempt to lookup the replicaset in our local cache, by
	// computing a hash of the replicas in the set, and checking if this set
	// has been returned before. If it has -- return that set.
	originalIndex := c.ring[c.keys[idx]]

	replicasHash := crc32.NewIEEE()
	replicasHash.Write([]byte{originalIndex})
	c.lookupReplicas(idx, func(replicaIndex uint8) {
		replicasHash.Write([]byte{replicaIndex})
	})

	replicasKey := replicasHash.Sum32()

	c.replicaMu.RLock()
	replicas, ok := c.replicaSets[replicasKey]
	c.replicaMu.RUnlock()
	if ok {
		return replicas
	}

	// if no replicaset was found under the hash key, we'll go ahead and
	// allocate a new replica set and cache it, then return it.
	seen := make(map[uint8]struct{}, len(c.keys))
	seen[originalIndex] = struct{}{}

	replicas = make([]string, 0, len(c.keys))
	replicas = append(replicas, c.items[originalIndex])

	c.lookupReplicas(idx, func(replicaIndex uint8) {
		if _, ok := seen[replicaIndex]; !ok {
			replicas = append(replicas, c.items[replicaIndex])
			seen[replicaIndex] = struct{}{}
		}
	})

	c.replicaMu.Lock()
	if _, ok := c.replicaSets[replicasKey]; !ok {
		c.replicaSets[replicasKey] = replicas
	}
	c.replicaMu.Unlock()

	return replicas
}

// GetNReplicas returns the N "items" responsible for the specified key, in
// order. It does this by walking the consistent hash ring, in order,
// until N unique replicas have been found.
func (c *ConsistentHash) GetNReplicas(key string, n int) []string {
	replicas := c.GetAllReplicas(key)
	if len(replicas) < n {
		log.Warningf("Warning: client requested %d replicas but only %d were available.", n, len(replicas))
	}
	return replicas[:n]
}
