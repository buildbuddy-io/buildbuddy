package memory_metrics_collector

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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

func (m *MemoryMetricsCollector) SetGetMembers(ctx context.Context, key string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var existingMap map[string]struct{}
	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]struct{}); ok {
			existingMap = existingVal
		}
	}
	if existingMap == nil {
		return nil, nil
	}
	// Convert string set to string slice
	members := make([]string, 0, len(existingMap))
	for k := range existingMap {
		members = append(members, k)
	}
	return members, nil
}

func (m *MemoryMetricsCollector) Set(ctx context.Context, key string, value string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.l.Add(key, value)
	return nil
}

func (m *MemoryMetricsCollector) get(ctx context.Context, key string) (string, error) {
	iface, ok := m.l.Get(key)
	if !ok {
		return "", status.NotFoundError("not found")
	}
	str, ok := iface.(string)
	if !ok {
		return "", status.InternalErrorf("cannot get non-string type %T stored at key %q", iface, key)
	}
	return str, nil
}

func (m *MemoryMetricsCollector) GetAll(ctx context.Context, keys ...string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vals := make([]string, 0, len(keys))
	for _, key := range keys {
		val, err := m.get(ctx, key)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

func (m *MemoryMetricsCollector) getList(key string) ([]string, error) {
	value, ok := m.l.Get(key)
	if !ok {
		return nil, nil
	}
	list, ok := value.([]string)
	if !ok {
		return nil, status.FailedPreconditionErrorf("list push failed: key %q holds existing value of type %T", key, value)
	}
	return list, nil
}

func (m *MemoryMetricsCollector) ListAppend(ctx context.Context, key string, values ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	list, err := m.getList(key)
	if err != nil {
		return err
	}
	m.l.Add(key, append(list, values...))
	return nil
}

func (m *MemoryMetricsCollector) ListRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	list, err := m.getList(key)
	if err != nil {
		return nil, err
	}

	lower := int(start)
	upper := int(stop)
	// Translate negative indexes => end-relative
	if lower < 0 {
		lower = len(list) + lower
	}
	if upper < 0 {
		upper = len(list) + upper
	}
	// Make upperbound inclusive, to match redis
	upper += 1
	// Handle bounds / edge cases
	if lower < 0 {
		lower = 0
	}
	if upper < 0 {
		upper = 0
	}
	if lower > len(list) {
		lower = len(list)
	}
	if upper > len(list) {
		upper = len(list)
	}
	if lower > upper {
		return nil, nil
	}
	return list[lower:upper], nil
}

func (m *MemoryMetricsCollector) ReadCounts(ctx context.Context, key string) (map[string]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]int64); ok {
			counts := make(map[string]int64, len(existingVal))
			for k, v := range existingVal {
				counts[k] = v
			}
			return counts, nil
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

func (m *MemoryMetricsCollector) Expire(ctx context.Context, key string, duration time.Duration) error {
	// Not implemented for now.
	return nil
}

func (m *MemoryMetricsCollector) Flush(ctx context.Context) error {
	return nil
}
