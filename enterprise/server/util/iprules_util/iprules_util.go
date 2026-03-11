package iprules_util

import (
	"flag"
	"net"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
)

var (
	cacheTTL  = flag.Duration("auth.ip_rules.cache_ttl", 5*time.Minute, "Duration of time IP rules will be cached in memory.")
	cacheSize = flag.Int("auth.ip_rules.cache_size", 100_000, "The number of IP rules to cache in memory.")
)

type cacheEntry struct {
	allowed      []*net.IPNet
	expiresAfter time.Time
}

type Cache interface {
	Add(groupID string, allowed []*net.IPNet)
	Get(groupID string) ([]*net.IPNet, bool)
}

type memIpRuleCache struct {
	mu  sync.Mutex
	lru interfaces.LRU[*cacheEntry]
}

func (c *memIpRuleCache) Get(groupID string) (allowed []*net.IPNet, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.lru.Get(groupID)
	if !ok {
		return nil, ok
	}
	if time.Now().After(entry.expiresAfter) {
		c.lru.Remove(groupID)
		return nil, false
	}
	return entry.allowed, true
}

func (c *memIpRuleCache) Add(groupID string, allowed []*net.IPNet) {
	c.mu.Lock()
	c.lru.Add(groupID, &cacheEntry{allowed: allowed, expiresAfter: time.Now().Add(*cacheTTL)})
	c.mu.Unlock()
}

type noopIpRuleCache struct {
}

func (c *noopIpRuleCache) Add(groupID string, allowed []*net.IPNet) {
}

func (c *noopIpRuleCache) Get(groupID string) ([]*net.IPNet, bool) {
	return nil, false
}

func NewCache() (Cache, error) {
	if *cacheTTL == 0 {
		return &noopIpRuleCache{}, nil
	}
	config := &lru.Config[*cacheEntry]{
		MaxSize: int64(*cacheSize),
		SizeFn:  func(v *cacheEntry) int64 { return int64(len(v.allowed)) },
	}
	l, err := lru.NewLRU[*cacheEntry](config)
	if err != nil {
		return nil, err
	}
	return &memIpRuleCache{
		lru: l,
	}, nil
}
