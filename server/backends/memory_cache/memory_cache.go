package memory_cache

import (
	"bytes"
	"context"
	"flag"
	"io"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
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
	cacheType          resource.CacheType
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
		cacheType:          resource.CacheType_CAS,
		remoteInstanceName: "",
	}, nil
}

func (m *MemoryCache) key(ctx context.Context, r *resource.ResourceName) (string, error) {
	hash, err := digest.Validate(r.GetDigest())
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}

	var key string
	if r.GetCacheType() == resource.CacheType_AC {
		key = filepath.Join(userPrefix, digest.CacheTypeToPrefix(r.GetCacheType()), r.GetInstanceName(), hash)
	} else {
		key = filepath.Join(userPrefix, digest.CacheTypeToPrefix(r.GetCacheType()), hash)
	}
	return key, nil
}

func (m *MemoryCache) WithIsolation(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	return &MemoryCache{
		l:                  m.l,
		lock:               m.lock,
		cacheType:          cacheType,
		remoteInstanceName: remoteInstanceName,
	}, nil
}

func (m *MemoryCache) Contains(ctx context.Context, r *resource.ResourceName) (bool, error) {
	k, err := m.key(ctx, r)
	if err != nil {
		return false, err
	}
	m.lock.Lock()
	contains := m.l.Contains(k)
	m.lock.Unlock()

	return contains, nil
}

// TODO(buildbuddy-internal#1485) - Add last access and modify time
func (m *MemoryCache) Metadata(ctx context.Context, r *resource.ResourceName) (*interfaces.CacheMetadata, error) {
	d := r.GetDigest()
	k, err := m.key(ctx, r)
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

func (m *MemoryCache) FindMissing(ctx context.Context, resources []*resource.ResourceName) ([]*repb.Digest, error) {
	var missing []*repb.Digest
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, r := range resources {
		ok, err := m.Contains(ctx, r)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, r.GetDigest())
		}
	}
	return missing, nil
}

func (m *MemoryCache) FindMissingDeprecated(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	rns := digest.ResourceNames(m.cacheType, m.remoteInstanceName, digests)
	return m.FindMissing(ctx, rns)
}

func (m *MemoryCache) Get(ctx context.Context, r *resource.ResourceName) ([]byte, error) {
	k, err := m.key(ctx, r)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	v, ok := m.l.Get(k)
	m.lock.Unlock()
	if !ok {
		return nil, status.NotFoundErrorf("Key %s not found", r.GetDigest())
	}
	value, ok := v.([]byte)
	if !ok {
		return nil, status.InternalErrorf("LRU type assertion failed for %s", r.GetDigest())
	}
	return value, nil
}

func (m *MemoryCache) GetMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(resources))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, r := range resources {
		data, err := m.Get(ctx, r)
		if status.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		foundMap[r.GetDigest()] = data
	}
	return foundMap, nil
}

func (m *MemoryCache) GetMultiDeprecated(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	rns := digest.ResourceNames(m.cacheType, m.remoteInstanceName, digests)
	return m.GetMulti(ctx, rns)
}

func (m *MemoryCache) Set(ctx context.Context, r *resource.ResourceName, data []byte) error {
	k, err := m.key(ctx, r)
	if err != nil {
		return err
	}
	m.lock.Lock()
	m.l.Add(k, data)
	m.lock.Unlock()
	return nil
}

func (m *MemoryCache) SetMulti(ctx context.Context, kvs map[*resource.ResourceName][]byte) error {
	for r, data := range kvs {
		if err := m.Set(ctx, r, data); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryCache) SetMultiDeprecated(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	rnMap := digest.ResourceNameMap(m.cacheType, m.remoteInstanceName, kvs)
	return m.SetMulti(ctx, rnMap)
}

func (m *MemoryCache) Delete(ctx context.Context, r *resource.ResourceName) error {
	k, err := m.key(ctx, r)
	if err != nil {
		return err
	}
	m.lock.Lock()
	removed := m.l.Remove(k)
	m.lock.Unlock()
	if !removed {
		d := r.GetDigest()
		return status.NotFoundErrorf("digest %s/%d not found in memory cache", d.GetHash(), d.GetSizeBytes())
	}
	return nil
}

func (m *MemoryCache) DeleteDeprecated(ctx context.Context, d *repb.Digest) error {
	rn := &resource.ResourceName{
		Digest:       d,
		InstanceName: m.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    m.cacheType,
	}
	return m.Delete(ctx, rn)
}

// Low level interface used for seeking and stream-writing.
func (m *MemoryCache) Reader(ctx context.Context, rn *resource.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	// Locking and key prefixing are handled in Get.
	buf, err := m.Get(ctx, rn)
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

func (m *MemoryCache) ReaderDeprecated(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	rn := &resource.ResourceName{
		Digest:       d,
		InstanceName: m.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    m.cacheType,
	}
	return m.Reader(ctx, rn, offset, limit)
}

func (m *MemoryCache) Writer(ctx context.Context, r *resource.ResourceName) (interfaces.CommittedWriteCloser, error) {
	var buffer bytes.Buffer
	wc := ioutil.NewCustomCommitWriteCloser(&buffer)
	wc.CommitFn = func(int64) error {
		// Locking and key prefixing are handled in SetDeprecated.
		return m.Set(ctx, r, buffer.Bytes())
	}
	return wc, nil
}

func (m *MemoryCache) WriterDeprecated(ctx context.Context, d *repb.Digest) (interfaces.CommittedWriteCloser, error) {
	rn := &resource.ResourceName{
		Digest:       d,
		InstanceName: m.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    m.cacheType,
	}
	return m.Writer(ctx, rn)
}

func (m *MemoryCache) Start() error {
	return nil
}

func (m *MemoryCache) Stop() error {
	return nil
}
