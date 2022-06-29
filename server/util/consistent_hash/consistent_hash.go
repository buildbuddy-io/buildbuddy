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
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		numReplicas: defaultNumReplicas,
		keys:        make([]int, 0),
		ring:        make(map[int]uint8, 0),
		items:       make([]string, 0),
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
	r := c.items[c.ring[c.keys[idx]]]
	return r
}

func (c *ConsistentHash) lookupReplicas(idx int, fn func(replicaIndex uint8) bool) {
	done := false
	for offset := 1; offset < len(c.keys) && !done; offset += 1 {
		newIdx := (idx + offset) % len(c.keys)
		done = fn(c.ring[c.keys[newIdx]])
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
	originalIndex := c.ring[c.keys[idx]]

	replicas := make([]string, 0, len(c.items))
	replicas = append(replicas, c.items[originalIndex])

	c.lookupReplicas(idx, func(replicaIndex uint8) bool {
		replica := c.items[replicaIndex]
		for _, r := range replicas {
			if r == replica {
				return false
			}
		}
		replicas = append(replicas, replica)
		return len(replicas) == len(c.items)
	})

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
