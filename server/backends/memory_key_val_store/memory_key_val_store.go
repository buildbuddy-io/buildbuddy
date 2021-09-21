package memory_key_val_store

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

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

func (m *MemoryKeyValStore) SetByKey(ctx context.Context, key string, val []byte) error {
	if val != nil {
		m.l.Add(key, val)
	} else {
		m.l.Remove(key)
	}
	return nil
}

func (m *MemoryKeyValStore) GetByKey(ctx context.Context, key string) ([]byte, error) {
	if existingValIface, ok := m.l.Get(key); ok {
		if val, ok := existingValIface.([]byte); ok {
			return val, nil
		}
		return nil, status.InternalErrorf("Value in the KeyValStore for key %s was not a byte slice.", key)
	}
	return nil, status.NotFoundErrorf("No message found in the KeyValStore for key %s.", key)
}

func (m *MemoryKeyValStore) SetProtoByKey(ctx context.Context, key string, msg proto.Message) error {
	if msg == nil {
		return m.SetByKey(ctx, key, nil)
	}
	marshaled, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	m.SetByKey(ctx, key, marshaled)
	return nil
}

func (m *MemoryKeyValStore) GetProtoByKey(ctx context.Context, key string, msg proto.Message) error {
	marshaled, err := m.GetByKey(ctx, key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(marshaled, msg)
}
