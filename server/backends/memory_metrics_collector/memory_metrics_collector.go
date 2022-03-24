package memory_metrics_collector

import (
	"context"
	"sync"

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
