package memory_counter

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

const (
	// (8 + 36) bytes * 1,000,000 =~ 44 MB
	// That's 1 int64 + invocation_id string.
	maxNumEntries = 1000000
)

type MemoryCounter struct {
	l  *lru.Cache
	mu sync.Mutex
}

func NewMemoryCounter() (*MemoryCounter, error) {
	l, err := lru.New(maxNumEntries)
	if err != nil {
		return nil, err
	}
	return &MemoryCounter{
		l: l,
	}, nil
}

func (m *MemoryCounter) Increment(counterName string, n int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingValIface, ok := m.l.Get(counterName); ok {
		if existingVal, ok := existingValIface.(int64); ok {
			newVal := existingVal + n
			m.l.Add(counterName, newVal)
			return newVal, nil
		}
	}

	m.l.Add(counterName, n)
	return n, nil

}

func (m *MemoryCounter) Read(counterName string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingValIface, ok := m.l.Get(counterName); ok {
		if existingVal, ok := existingValIface.(int64); ok {
			return existingVal, nil
		}
	}

	return 0, nil
}
