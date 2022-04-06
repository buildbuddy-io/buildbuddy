package memory_metrics_collector

import (
	"context"
	"fmt"
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

	var existingMap map[string]interface{}
	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]interface{}); ok {
			existingMap = existingVal
		}
	}
	if existingMap == nil {
		existingMap = make(map[string]interface{}, len(counts))
	}
	for field, n := range counts {
		existingVal, ok := existingMap[field]
		if !ok {
			existingMap[field] = n
			continue
		}
		existingInt64Val, ok := existingVal.(int64)
		if !ok {
			return status.InvalidArgumentErrorf("cannot increment hash value of type %T", existingVal)
		}
		existingMap[field] = existingInt64Val + n
	}
	m.l.Add(key, existingMap)
	return nil

}

func (m *MemoryMetricsCollector) HashSet(ctx context.Context, key, field string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var existingMap map[string]interface{}
	if existingValIface, ok := m.l.Get(key); ok {
		if existingVal, ok := existingValIface.(map[string]interface{}); ok {
			existingMap = existingVal
		}
	}
	if existingMap == nil {
		existingMap = make(map[string]interface{}, 1)
	}
	existingMap[field] = value
	return nil
}

func (m *MemoryMetricsCollector) HashGetAll(ctx context.Context, key string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	iface, ok := m.l.Get(key)
	if !ok {
		return make(map[string]string, 0), nil
	}
	h, ok := iface.(map[string]interface{})
	if !ok {
		return nil, status.InvalidArgumentErrorf("unexpected value of type %T stored at hash key %q", h, iface)
	}
	strs := make(map[string]string, len(h))
	for k, v := range h {
		strs[k] = stringify(v)
	}
	return strs, nil
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

func (m *MemoryMetricsCollector) SetMembers(ctx context.Context, key string) ([]string, error) {
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

func (m *MemoryMetricsCollector) Set(ctx context.Context, key string, value interface{}, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.l.Add(key, value)
	return nil
}

func (m *MemoryMetricsCollector) Get(ctx context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	iface, ok := m.l.Get(key)
	if !ok {
		return "", status.NotFoundError("not found")
	}
	return stringify(iface), nil
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

func stringify(value interface{}) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprintf("%v", value)
	}
}
