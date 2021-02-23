package memory_cache

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type MemoryCache struct {
	l      *lru.LRU
	lock   *sync.RWMutex
	prefix string
}

func sizeFn(key interface{}, value interface{}) int64 {
	size := int64(0)
	if k, ok := key.(string); ok {
		size += int64(len(k))
	}
	if v, ok := value.([]byte); ok {
		size += int64(len(v))
	}
	return size
}

func NewMemoryCache(maxSizeBytes int64) (*MemoryCache, error) {
	l, err := lru.NewLRU(maxSizeBytes, nil /*=evictFn*/, nil /*=addFn*/, sizeFn)
	if err != nil {
		return nil, err
	}
	return &MemoryCache{
		l:    l,
		lock: &sync.RWMutex{},
	}, nil
}

func (m *MemoryCache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	return userPrefix + m.prefix + hash, nil
}

func (m *MemoryCache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(m.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &MemoryCache{
		l:      m.l,
		lock:   m.lock,
		prefix: newPrefix,
	}
}

func (m *MemoryCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := m.key(ctx, d)
	if err != nil {
		return false, err
	}
	m.lock.Lock()
	contains := m.l.Contains(k)
	m.lock.Unlock()
	return contains, nil
}

func (m *MemoryCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	foundMap := make(map[*repb.Digest]bool, len(digests))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		ok, err := m.Contains(ctx, d)
		if err != nil {
			return nil, err
		}
		foundMap[d] = ok
	}
	return foundMap, nil
}

func (m *MemoryCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := m.key(ctx, d)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	v, ok := m.l.Get(k)
	m.lock.Unlock()
	if !ok {
		return nil, status.NotFoundErrorf("Key %s not found", d)
	}
	value, ok := v.([]byte)
	if !ok {
		return nil, status.InternalErrorf("LRU type assertion failed for %s", d)
	}
	return value, nil
}

func (m *MemoryCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		data, err := m.Get(ctx, d)
		if err != nil {
			return nil, err
		}
		foundMap[d] = data
	}
	return foundMap, nil
}

func (m *MemoryCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, err := m.key(ctx, d)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Add(k, data)
	m.lock.Unlock()
	return nil
}

func (m *MemoryCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := m.Set(ctx, d, data); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryCache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := m.key(ctx, d)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Remove(k)
	m.lock.Unlock()
	return nil
}

// Low level interface used for seeking and stream-writing.
func (m *MemoryCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	// Locking and key prefixing are handled in Get.
	buf, err := m.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(buf)
	r.Seek(offset, 0)
	length := int64(len(buf))
	if length > 0 {
		return io.LimitReader(r, length), nil
	}
	return r, nil
}

type closeFn func(b *bytes.Buffer) error
type setOnClose struct {
	*bytes.Buffer
	c closeFn
}

func (d *setOnClose) Close() error {
	return d.c(d.Buffer)
}

func (m *MemoryCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	var buffer bytes.Buffer
	return &setOnClose{
		Buffer: &buffer,
		c: func(b *bytes.Buffer) error {
			// Locking and key prefixing are handled in Set.
			return m.Set(ctx, d, b.Bytes())
		},
	}, nil
}

func (m *MemoryCache) Start() error {
	return nil
}

func (m *MemoryCache) Stop() error {
	return nil
}
