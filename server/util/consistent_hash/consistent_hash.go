package consistent_hash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const defaultNumReplicas = 100

type ConsistentHash struct {
	numReplicas int
	keys        []int
	ring        map[int]string
	items       []string
	mu          sync.RWMutex
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		numReplicas: defaultNumReplicas,
		keys:        make([]int, 0),
		ring:        make(map[int]string, 0),
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

func (c *ConsistentHash) Set(items ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = make([]int, 0)
	c.ring = make(map[int]string, 0)
	c.items = items
	for _, key := range items {
		for i := 0; i < c.numReplicas; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			c.keys = append(c.keys, h)
			c.ring[h] = key
		}
	}
	sort.Strings(c.items)
	sort.Ints(c.keys)
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
	return c.ring[c.keys[idx]]
}

// GetNReplicas returns the N "items" responsible for the specified key, in
// order. It does this by walking the consistent hash ring, in order,
// until N unique replicas have been found.
func (c *ConsistentHash) GetNReplicas(key string, n int) []string {
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
	replicas := make([]string, 0, n)
	seen := make(map[string]struct{}, 0)

	original := c.ring[c.keys[idx]]
	replicas = append(replicas, original)
	seen[original] = struct{}{}

	for offset := 1; offset < len(c.keys); offset += 1 {
		newIdx := (idx + offset) % len(c.keys)
		v := c.ring[c.keys[newIdx]]
		if _, ok := seen[v]; !ok {
			replicas = append(replicas, v)
			seen[v] = struct{}{}
		}
		if len(replicas) == n {
			break
		}
	}
	if len(replicas) < n {
		log.Warningf("Warning: client requested %d replicas but only %d were available.", n, len(replicas))
	}
	return replicas
}
