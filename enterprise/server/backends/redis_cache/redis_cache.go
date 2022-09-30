package redis_cache

import (
	"bytes"
	"context"
	"flag"
	"io"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var maxValueSizeBytes = flag.Int64("cache.redis.max_value_size_bytes", 10000000, "The maximum value size to cache in redis (in bytes).")

const (
	ttl = 3 * 24 * time.Hour
)

var (
	cacheLabels = cache_metrics.MakeCacheLabels(cache_metrics.MemoryCacheTier, "redis")
)

// Cache is a cache that uses digests as keys instead of strings.
// Adding a WithPrefix method allows us to separate AC content from CAS
// content.
type Cache struct {
	rdb             redis.UniversalClient
	prefix          string
	cutoffSizeBytes int64
}

func Register(env environment.Env) error {
	opts := redis_client.CacheRedisClientOpts()
	if opts == nil {
		return nil
	}
	if _, ok := env.GetCache().(*composable_cache.ComposableCache); ok {
		// Cache has already been composed, don't do it again.
		log.Warning("Multiple cache compositions configured, ignoring redis_cache configuration.")
		return nil
	}
	if env.GetCache() == nil {
		return status.FailedPreconditionErrorf("Redis requires a base cache but one was not configured: please also enable a base cache")
	}
	rc, err := redisutil.NewClientWithOpts(opts, env.GetHealthChecker(), "cache_redis")
	if err != nil {
		return status.InternalErrorf("Error configuring cache Redis client: %s", err)
	}

	r := NewCache(rc)
	env.SetCache(composable_cache.NewComposableCache(r, env.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	return nil
}

func NewCache(redisClient redis.UniversalClient) *Cache {
	return &Cache{
		prefix:          "",
		rdb:             redisClient,
		cutoffSizeBytes: *maxValueSizeBytes,
	}
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

func (c *Cache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
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

func (c *Cache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	key, err := c.key(ctx, d)
	if err != nil {
		return false, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	found, err := c.rdb.Expire(ctx, key, ttl).Result()
	timer.ObserveContains(err)
	return found, err
}

// TODO(buildbuddy-internal#1485) - Add last access and modify time
// Note: Can't use rdb.ObjectIdleTime to calculate last access time, because rdb.StrLen resets the idle time to 0,
// so this API will always incorrectly set the last access time to the current time
func (c *Cache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	key, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	found, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if found == 0 {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	blobLen, err := c.rdb.StrLen(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	return &interfaces.CacheMetadata{
		SizeBytes: blobLen,
	}, nil
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
func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
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
	if limit != 0 && limit < length {
		length = limit
	}
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
