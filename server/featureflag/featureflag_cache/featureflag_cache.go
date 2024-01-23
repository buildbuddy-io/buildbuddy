package featureflag_cache

import (
	"flag"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"sync"
	"time"
)

var (
	CacheTTL = flag.Duration("featureflag.cache_ttl", 5*time.Minute, "Duration of time feature flags will be cached in memory. This is the max time a stale flag could be used.")
)

const (
	// The number of feature flags that we will cache in memory.
	flagCacheSize = 1000
)

type FlagCacheEntry struct {
	Enabled            bool
	ConfiguredGroupIds map[string]struct{}
	ExpiresAfter       time.Time
}

type FeatureFlagCache struct {
	mu        sync.Mutex
	flagCache interfaces.LRU[*FlagCacheEntry]
	ttl       time.Duration
}

func NewCache(ttl time.Duration) (*FeatureFlagCache, error) {
	flagConfig := &lru.Config[*FlagCacheEntry]{
		MaxSize: flagCacheSize,
		SizeFn:  func(v *FlagCacheEntry) int64 { return 1 },
	}
	flagCache, err := lru.NewLRU[*FlagCacheEntry](flagConfig)
	if err != nil {
		return nil, err
	}
	return &FeatureFlagCache{
		flagCache: flagCache,
		ttl:       ttl,
	}, nil
}

func (c *FeatureFlagCache) Get(flagName string) (e *FlagCacheEntry, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.flagCache.Get(flagName)
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.ExpiresAfter) {
		c.flagCache.Remove(flagName)
		return nil, false
	}
	return entry, true
}

func (c *FeatureFlagCache) Add(flagName string, f *FlagCacheEntry) {
	f.ExpiresAfter = time.Now().Add(c.ttl)
	c.mu.Lock()
	c.flagCache.Add(flagName, f)
	c.mu.Unlock()
}

func (c *FeatureFlagCache) UpdateExperimentAssignments(flagName string, experimentAssignments []string) (ok bool) {
	f, ok := c.Get(flagName)
	if !ok {
		return false
	}

	m := make(map[string]struct{})
	for _, e := range experimentAssignments {
		m[e] = struct{}{}
	}

	f.ConfiguredGroupIds = m
	c.Add(flagName, f)

	return true
}

func (c *FeatureFlagCache) UpdateFlag(flagName string, enabled bool) (ok bool) {
	f, ok := c.Get(flagName)
	if !ok {
		return false
	}

	f.Enabled = enabled
	c.Add(flagName, f)

	return true
}
