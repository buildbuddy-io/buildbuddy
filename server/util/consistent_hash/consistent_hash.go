package consistent_hash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

const defaultNumReplicas = 100

type ConsistentHash struct {
	numReplicas int
	keys        []int
	ring        map[int]string
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		numReplicas: defaultNumReplicas,
		keys:        make([]int, 0),
		ring:        make(map[int]string, 0),
	}
}

func (c *ConsistentHash) hashKey(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}

func (c *ConsistentHash) Add(items ...string) {
	for _, key := range items {
		for i := 0; i < c.numReplicas; i++ {
			h := c.hashKey(strconv.Itoa(i) + key)
			c.keys = append(c.keys, h)
			c.ring[h] = key
		}
	}
	sort.Ints(c.keys)
}

func (c *ConsistentHash) Get(key string) string {
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
