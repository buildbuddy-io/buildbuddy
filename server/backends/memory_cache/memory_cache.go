package memory_cache

import (
	"bytes"
	"context"
	"flag"
	"io"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var cacheInMemory = flag.Bool("cache.in_memory", false, "Whether or not to use the in_memory cache.")

type MemoryCache struct {
	l                  interfaces.LRU
	lock               *sync.RWMutex
	cacheType          interfaces.CacheTypeDeprecated
	remoteInstanceName string
}

func sizeFn(value interface{}) int64 {
	size := int64(0)
	if v, ok := value.([]byte); ok {
		size += int64(len(v))
	}
	return size
}

func Register(env environment.Env) error {
	if !*cacheInMemory {
		return nil
	}
	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with memory_cache.")
	}
	maxSizeBytes := cache_config.MaxSizeBytes()
	if maxSizeBytes == 0 {
		return status.FailedPreconditionError("Cache size must be greater than 0 if in_memory cache is enabled!")
	}
	c, err := NewMemoryCache(maxSizeBytes)
	if err != nil {
		return status.InternalErrorf("Error configuring in-memory cache: %s", err)
	}
	env.SetCache(c)
	return nil
}

func NewMemoryCache(maxSizeBytes int64) (*MemoryCache, error) {
	l, err := lru.NewLRU(&lru.Config{MaxSize: maxSizeBytes, SizeFn: sizeFn})
	if err != nil {
		return nil, err
	}
	return &MemoryCache{
		l:                  l,
		lock:               &sync.RWMutex{},
		cacheType:          interfaces.CASCacheType,
		remoteInstanceName: "",
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

	var key string
	if m.cacheType == interfaces.ActionCacheType {
		key = filepath.Join(userPrefix, m.cacheType.Prefix(), m.remoteInstanceName, hash)
	} else {
		key = filepath.Join(userPrefix, m.cacheType.Prefix(), hash)
	}
	return key, nil
}

func (m *MemoryCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
	return &MemoryCache{
		l:                  m.l,
		lock:               m.lock,
		cacheType:          cacheType,
		remoteInstanceName: remoteInstanceName,
	}, nil
}

func (m *MemoryCache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := m.key(ctx, d)
	if err != nil {
		return false, err
	}
	m.lock.Lock()
	contains := m.l.Contains(k)
	m.lock.Unlock()

	return contains, nil
}

// TODO(buildbuddy-internal#1485) - Add last access and modify time
func (m *MemoryCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	k, err := m.key(ctx, d)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	v, contains := m.l.Get(k)
	m.lock.Unlock()

	if !contains {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}

	vb, ok := v.([]byte)
	if !ok {
		return nil, status.InternalErrorf("not a []byte")
	}
	return &interfaces.CacheMetadata{SizeBytes: int64(len(vb))}, nil
}

func (m *MemoryCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	var missing []*repb.Digest
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		ok, err := m.ContainsDeprecated(ctx, d)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, d)
		}
	}
	return missing, nil
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
		if status.IsNotFoundError(err) {
			continue
		}
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
	removed := m.l.Remove(k)
	m.lock.Unlock()
	if !removed {
		return status.NotFoundErrorf("digest %s/%d not found in memory cache", d.GetHash(), d.GetSizeBytes())
	}
	return nil
}

// Low level interface used for seeking and stream-writing.
func (m *MemoryCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	// Locking and key prefixing are handled in Get.
	buf, err := m.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(buf)
	r.Seek(offset, 0)
	length := int64(len(buf))
	if limit != 0 && limit < length {
		length = limit
	}
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	return io.NopCloser(r), nil
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
