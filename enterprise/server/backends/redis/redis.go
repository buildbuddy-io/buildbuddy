package redis

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	redis "github.com/go-redis/redis/v8"
)

const (
	mcCutoffSizeBytes = 512000000 - 1        // 512 MB
	ttl               = 259200 * time.Second // 3 days
)

var (
	cacheLabels = cache_metrics.MakeCacheLabels(cache_metrics.MemoryCacheTier, "redis")
)

func eligibleForCache(d *repb.Digest) bool {
	return d.GetSizeBytes() < mcCutoffSizeBytes
}

// Cache is a cache that uses digests as keys instead of strings.
// Adding a WithPrefix method allows us to separate AC content from CAS
// content.
type Cache struct {
	prefix string
	rdb    *redis.Client
}

func NewCache(redisServer string, hc interfaces.HealthChecker) *Cache {
	c := &Cache{
		prefix: "",
		rdb: redis.NewClient(&redis.Options{
			Addr:     redisServer,
			Password: "", // no password set
			DB:       0,  // use default DB
		}),
	}
	hc.AddHealthCheck("redis_cache", c)
	return c
}

func (c *Cache) Check(ctx context.Context) error {
	_, err := c.rdb.Ping(ctx).Result()
	return err
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
	val, err := c.rdb.Get(ctx, key).Result()
	if err == nil {
		return []byte(val), nil
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

func (c *Cache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	return &Cache{
		prefix: newPrefix,
		rdb:    c.rdb,
	}
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

func (c *Cache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
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
	response := make(map[*repb.Digest]bool, len(keys))
	for _, k := range keys {
		d := digestsByKey[k]
		found, ok := mcMap[k]
		response[d] = ok && found
	}
	return response, nil
}

func (c *Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	if !eligibleForCache(d) {
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
		item, ok := (rMap[i]).([]byte)
		if ok {
			response[d] = item
		}
	}
	return response, nil
}

func (c *Cache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	if !eligibleForCache(d) {
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

func offsetLimReader(data []byte, offset, length int64) io.Reader {
	r := bytes.NewReader(data)
	r.Seek(offset, 0)
	if length > 0 {
		return io.LimitReader(r, length)
	}
	return r
}

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	if !eligibleForCache(d) {
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
		return io.LimitReader(r, length), nil
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	return timer.NewInstrumentedReader(r, length), nil
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
	if !eligibleForCache(d) {
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

func (c *Cache) IncrementCount(ctx context.Context, counterName string, n int64) (int64, error) {
	return c.rdb.IncrBy(ctx, counterName, n).Result()
}

func (c *Cache) ReadCount(ctx context.Context, counterName string) (int64, error) {
	return c.rdb.IncrBy(ctx, counterName, 0).Result()
}
