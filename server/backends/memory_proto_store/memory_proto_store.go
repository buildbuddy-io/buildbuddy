package memory_proto_store

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

type MemoryProtoStore struct {
	l  *lru.Cache
	mu sync.Mutex
}

func NewMemoryProtoStore() (*MemoryProtoStore, error) {
	l, err := lru.New(maxNumEntries)
	if err != nil {
		return nil, err
	}
	return &MemoryProtoStore{
		l: l,
	}, nil
}

func (m *MemoryProtoStore) SetMessageByKey(ctx context.Context, key string, msg proto.Message) error {
	marshaled, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	m.l.Add(key, marshaled)
	return nil
}

func (m *MemoryProtoStore) GetMessageByKey(ctx context.Context, key string, msg proto.Message) error {
	if existingValIface, ok := m.l.Get(key); ok {
		if marshaled, ok := existingValIface.([]byte); ok {
			return proto.Unmarshal(marshaled, msg)
		}
		return status.InternalErrorf("Value in the proto store for key %s was not a byte slice.", key)
	}
	return status.NotFoundErrorf("No message found in the proto store for key %s.", key)
}
