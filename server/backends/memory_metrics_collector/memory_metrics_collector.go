package memory_metrics_collector

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

const (
	// Maximum number of entries before keys are evicted from the metrics
	// collector.
	//
	// This value was chosen so that the LRU consumes around 100MB when maxed out
	// with keys of length ~50, metric names of length ~20, and ~10 metrics per
	// map.
	maxNumEntries = 50_000
)

type MemoryMetricsCollector struct {
	l  *lru.Cache
	mu sync.Mutex
}

func NewMemoryMetricsCollector() (*MemoryMetricsCollector, error) {
	l, err := lru.New(maxNumEntries)
	if err != nil {
		return nil, err
	}
	return &MemoryMetricsCollector{
		l: l,
	}, nil
}

func (m *MemoryMetricsCollector) IncrementCounts(ctx context.Context, key string, counts map[string]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(counts) == 0 {
		return nil
	}

	var existingMap map[string]int64
	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]int64); ok {
			existingMap = existingVal
		}
	}
	if existingMap == nil {
		existingMap = make(map[string]int64, len(counts))
	}
	for field, n := range counts {
		existingMap[field] += n
	}
	m.l.Add(key, existingMap)
	return nil

}

func (m *MemoryMetricsCollector) IncrementCountsWithExpiry(ctx context.Context, key string, counts map[string]int64, expiry time.Duration) error {
	return m.IncrementCounts(ctx, key, counts)
}

func (m *MemoryMetricsCollector) IncrementCount(ctx context.Context, key, field string, n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]int64); ok {
			existingVal[field] += n
			m.l.Add(key, existingVal)
			return nil
		}
	}
	m.l.Add(key, map[string]int64{field: n})
	return nil

}

func (m *MemoryMetricsCollector) IncrementCountWithExpiry(ctx context.Context, key, field string, n int64, expiry time.Duration) error {
	return m.IncrementCount(ctx, key, field, n)
}

func (m *MemoryMetricsCollector) SetAdd(ctx context.Context, key string, members ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(members) == 0 {
		return nil
	}

	var existingMap map[string]struct{}
	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]struct{}); ok {
			existingMap = existingVal
		}
	}
	if existingMap == nil {
		existingMap = make(map[string]struct{}, len(members))
	}
	for _, member := range members {
		existingMap[member] = struct{}{}
	}
	m.l.Add(key, existingMap)
	return nil

}

func (m *MemoryMetricsCollector) SetAddWithExpiry(ctx context.Context, key string, expiry time.Duration, members ...string) error {
	return m.SetAdd(ctx, key, members...)
}

func (m *MemoryMetricsCollector) ReadCounts(ctx context.Context, key string) (map[string]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]int64); ok {
			return existingVal, nil
		}
	}

	return make(map[string]int64), nil
}

func (m *MemoryMetricsCollector) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.l.Remove(key)
	return nil
}

func (m *MemoryMetricsCollector) Flush(ctx context.Context) error {
	return nil
}
