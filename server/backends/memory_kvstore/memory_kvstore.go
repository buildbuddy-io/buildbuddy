package memory_kvstore

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	lru "github.com/hashicorp/golang-lru"
)

const (
	// This is primarily used for event log entries, which may be up to 2 megabytes in length
	// 2 MB * 5000 = ~10 GB max size, though entries will rarely be the full 2 MB.
	maxNumEntries = 5000
)

type MemoryKeyValStore struct {
	l  *lru.Cache
	mu sync.Mutex
}

func NewMemoryKeyValStore() (*MemoryKeyValStore, error) {
	l, err := lru.New(maxNumEntries)
	if err != nil {
		return nil, err
	}
	return &MemoryKeyValStore{
		l: l,
	}, nil
}

func (m *MemoryKeyValStore) Set(ctx context.Context, key string, val []byte) error {
	if val != nil {
		m.l.Add(key, val)
	} else {
		m.l.Remove(key)
	}
	return nil
}

func (m *MemoryKeyValStore) Get(ctx context.Context, key string) ([]byte, error) {
	if existingValIface, ok := m.l.Get(key); ok {
		if val, ok := existingValIface.([]byte); ok {
			return val, nil
		}
		return nil, status.InternalErrorf("Value in the KeyValStore for key %s was not a byte slice.", key)
	}
	return nil, status.NotFoundErrorf("No message found in the KeyValStore for key %s.", key)
}
