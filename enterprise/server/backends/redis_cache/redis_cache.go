package redis_cache

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	defaultCutoffSizeBytes = 10000000
	ttl                    = 3 * 24 * time.Hour
)

var (
	cacheLabels = cache_metrics.MakeCacheLabels(cache_metrics.MemoryCacheTier, "redis")
)

// Cache is a cache that uses digests as keys instead of strings.
// Adding a WithPrefix method allows us to separate AC content from CAS
// content.
type Cache struct {
	rdb             *redis.Client
	prefix          string
	cutoffSizeBytes int64
}

func NewCache(redisClient *redis.Client, maxValueSizeBytes int64) *Cache {
	c := &Cache{
		prefix:          "",
		rdb:             redisClient,
		cutoffSizeBytes: maxValueSizeBytes,
	}
	if c.cutoffSizeBytes == 0 {
		c.cutoffSizeBytes = defaultCutoffSizeBytes
	}
	return c
}

func (c *Cache) eligibleForCache(d *repb.Digest) bool {
	return d.GetSizeBytes() < c.cutoffSizeBytes
}

func (c *Cache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	return userPrefix + c.prefix + hash, nil
}

func (c *Cache) rdbGet(ctx context.Context, key string) ([]byte, error) {
	bytes, err := c.rdb.Get(ctx, key).Bytes()
	if err == nil {
		return bytes, nil
	}
	if err == redis.Nil {
		return nil, status.NotFoundErrorf("Key %q not found in cache", key)
	}
	return nil, err
}

func (c *Cache) rdbMultiExists(ctx context.Context, keys ...string) (map[string]bool, error) {
	result := make(map[string]bool, len(keys))
	pipe := c.rdb.Pipeline()
	m := map[string]*redis.BoolCmd{}
	for _, k := range keys {
		m[k] = pipe.Expire(ctx, k, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	for k, v := range m {
		found, err := v.Result()
		exists := err == nil && found
		result[k] = exists
	}
	return result, nil
}

func (c *Cache) rdbMultiSet(ctx context.Context, setMap map[string][]byte) error {
	pipe := c.rdb.Pipeline()
	for k, v := range setMap {
		pipe.Set(ctx, k, v, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Cache) rdbSet(ctx context.Context, key string, data []byte) error {
	err := c.rdb.Set(ctx, key, data, ttl).Err()
	return err
}

func (c *Cache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	newPrefix := filepath.Join(remoteInstanceName, cacheType.Prefix())
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	return &Cache{
		prefix:          newPrefix,
		rdb:             c.rdb,
		cutoffSizeBytes: c.cutoffSizeBytes,
	}, nil
}

func (c *Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	key, err := c.key(ctx, d)
	if err != nil {
		return false, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	found, err := c.rdb.Expire(ctx, key, ttl).Result()
	timer.ObserveContains(err)
	return found, err
}

func update(old, new map[string]bool) {
	for k, v := range new {
		old[k] = v
	}
}

func (c *Cache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	if len(digests) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(digests))
	digestsByKey := make(map[string]*repb.Digest, len(digests))
	for _, d := range digests {
		k, err := c.key(ctx, d)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
		digestsByKey[k] = d
	}

	mcMap, err := c.rdbMultiExists(ctx, keys...)
	if err != nil {
		return nil, err
	}

	// Assemble results.
	var missing []*repb.Digest
	for _, k := range keys {
		d := digestsByKey[k]
		found, ok := mcMap[k]
		if !ok || !found {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

func (c *Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	if !c.eligibleForCache(d) {
		return nil, status.ResourceExhaustedErrorf("Get: Digest %v too big for redis", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}

	timer := cache_metrics.NewCacheTimer(cacheLabels)
	b, err := c.rdbGet(ctx, k)
	timer.ObserveGet(len(b), err)
	return b, err
}

func (c *Cache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	if len(digests) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(digests))
	digestsByKey := make(map[string]*repb.Digest, len(digests))
	for _, d := range digests {
		k, err := c.key(ctx, d)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
		digestsByKey[k] = d
	}

	rMap, err := c.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	// Assemble results.
	response := make(map[*repb.Digest][]byte, len(keys))
	for i, k := range keys {
		d := digestsByKey[k]
		item, ok := (rMap[i]).(string)
		if ok {
			response[d] = stringToBytes(item)
		}
	}
	return response, nil
}

// Efficiently convert string to byte slice: https://github.com/go-redis/redis/pull/1106
// Copied from: https://github.com/go-redis/redis/blob/b965d69fc9defa439a46d8178b60fc1d44f8fe29/internal/util/unsafe.go#L15
func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

func (c *Cache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	if !c.eligibleForCache(d) {
		return status.ResourceExhaustedErrorf("Set: Digest %v too big for redis", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}

	timer := cache_metrics.NewCacheTimer(cacheLabels)
	err = c.rdbSet(ctx, k, data)
	timer.ObserveSet(len(data), err)
	return err
}

func (c *Cache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	if len(kvs) == 0 {
		return nil
	}
	setMap := make(map[string][]byte, len(kvs))
	for d, v := range kvs {
		k, err := c.key(ctx, d)
		if err != nil {
			return err
		}
		setMap[k] = v
	}
	return c.rdbMultiSet(ctx, setMap)
}

func (c *Cache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	err = c.rdb.Del(ctx, k).Err()
	timer.ObserveDelete(err)
	return err
}

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	if !c.eligibleForCache(d) {
		return nil, status.ResourceExhaustedErrorf("Reader: Digest %v too big for redis", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	buf, err := c.rdbGet(ctx, k)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(buf)
	r.Seek(offset, 0)
	length := d.GetSizeBytes()
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	return io.NopCloser(timer.NewInstrumentedReader(r, length)), nil
}

type closeFn func(b *bytes.Buffer) error
type setOnClose struct {
	*bytes.Buffer
	timer *cache_metrics.CacheTimer
	c     closeFn
}

func (d *setOnClose) Close() error {
	err := d.c(d.Buffer)
	d.timer.ObserveWrite(int64(d.Buffer.Len()), err)
	return err
}

func (c *Cache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	if !c.eligibleForCache(d) {
		return nil, status.ResourceExhaustedErrorf("Writer: Digest %v too big for redis", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	return &setOnClose{
		Buffer: &buffer,
		timer:  cache_metrics.NewCacheTimer(cacheLabels),
		c: func(b *bytes.Buffer) error {
			// Locking and key prefixing are handled in Set.
			return c.rdbSet(ctx, k, b.Bytes())
		},
	}, nil
}

func (c *Cache) Start() error {
	return nil
}

func (c *Cache) Stop() error {
	return nil
}
