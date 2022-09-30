package memcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"golang.org/x/sync/errgroup"
)

var memcacheTargets = flagutil.New("cache.memcache_targets", []string{}, "Deprecated. Use Redis Target instead.")

const (
	mcCutoffSizeBytes = 134217728 - 1 // 128 MB
	ttl               = 259200        // 3 days in seconds
)

func eligibleForMc(d *repb.Digest) bool {
	return d.GetSizeBytes() < mcCutoffSizeBytes
}

// Cache is a cache that uses digests as keys instead of strings.
// Adding a WithPrefix method allows us to separate AC content from CAS
// content.
type Cache struct {
	mc     *memcache.Client
	prefix string
}

func Register(env environment.Env) error {
	if len(*memcacheTargets) == 0 {
		return nil
	}
	if _, ok := env.GetCache().(*composable_cache.ComposableCache); ok {
		// Cache has already been composed, don't do it again.
		log.Warning("Multiple cache compositions configured, ignoring memcache configuration.")
		return nil
	}
	if env.GetCache() == nil {
		return status.FailedPreconditionErrorf("Memcache requires a base cache but one was not configured: please also enable a base cache")
	}
	log.Infof("Enabling memcache layer with targets: %s", *memcacheTargets)
	mc := NewCache(*memcacheTargets...)
	env.SetCache(composable_cache.NewComposableCache(mc, env.GetCache(), composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough))
	return nil
}

func NewCache(mcServers ...string) *Cache {
	return &Cache{
		prefix: "",
		mc:     memcache.New(mcServers...),
	}
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

func (c *Cache) mcGet(key string) ([]byte, error) {
	item, err := c.mc.Get(key)
	if err == nil {
		return item.Value, nil
	}
	if err == memcache.ErrCacheMiss {
		return nil, status.NotFoundErrorf("Key %q not found in cache", key)
	}
	return nil, err
}

func makeItem(key string, data []byte) *memcache.Item {
	return &memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: ttl,
	}
}

func (c *Cache) mcSet(key string, data []byte) error {
	return c.mc.Set(makeItem(key, data))
}

func (c *Cache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
	newPrefix := filepath.Join(remoteInstanceName, cacheType.Prefix())
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	return &Cache{
		prefix: newPrefix,
		mc:     c.mc,
	}, nil
}

func (c *Cache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	key, err := c.key(ctx, d)
	if err != nil {
		return false, err
	}

	err = c.mc.Touch(key, ttl)
	if err == nil {
		return true, nil
	}
	if err == memcache.ErrCacheMiss {
		return false, nil
	}
	return false, err
}

// TODO(buildbuddy-internal#1485) - Add last access and modify time
func (c *Cache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	key, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}

	data, err := c.mcGet(key)
	if err != nil {
		return nil, err
	}
	if err == memcache.ErrCacheMiss {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	return &interfaces.CacheMetadata{SizeBytes: int64(len(data))}, nil
}

func update(old, new map[string]bool) {
	for k, v := range new {
		old[k] = v
	}
}

func (c *Cache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	lock := sync.RWMutex{} // protects(missing)
	var missing []*repb.Digest
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := c.ContainsDeprecated(ctx, d)
				if err != nil {
					return err
				}
				if !exists {
					lock.Lock()
					defer lock.Unlock()
					missing = append(missing, d)
				}
				return nil
			})
		}
		fetchFn(d)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return missing, nil
}

func (c *Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	if !eligibleForMc(d) {
		return nil, status.ResourceExhaustedErrorf("Get: Digest %v too big for memcache", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}

	return c.mcGet(k)
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

	mcMap, err := c.mc.GetMulti(keys)
	if err != nil {
		return nil, err
	}

	// Assemble results.
	response := make(map[*repb.Digest][]byte, len(keys))
	for _, k := range keys {
		d := digestsByKey[k]
		item, ok := mcMap[k]
		if ok {
			response[d] = item.Value
		}
	}
	return response, nil
}

func (c *Cache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	if !eligibleForMc(d) {
		return status.ResourceExhaustedErrorf("Set: Digest %v too big for memcache", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}

	return c.mcSet(k, data)
}

func (c *Cache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	eg, ctx := errgroup.WithContext(ctx)

	for d, data := range kvs {
		setFn := func(d *repb.Digest, data []byte) {
			eg.Go(func() error {
				return c.Set(ctx, d, data)
			})
		}
		setFn(d, data)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *Cache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	err = c.mc.Delete(k)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return status.NotFoundErrorf("digest %s/%d not found in memcache: %s", d.GetHash(), d.GetSizeBytes(), err.Error())
	}
	return err
}

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	if !eligibleForMc(d) {
		return nil, status.ResourceExhaustedErrorf("Reader: Digest %v too big for memcache", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	buf, err := c.mcGet(k)
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

func (c *Cache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	if !eligibleForMc(d) {
		return nil, status.ResourceExhaustedErrorf("Writer: Digest %v too big for memcache", d)
	}
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	return &setOnClose{
		Buffer: &buffer,
		c: func(b *bytes.Buffer) error {
			// Locking and key prefixing are handled in Set.
			return c.mcSet(k, b.Bytes())
		},
	}, nil
}

func (c *Cache) Start() error {
	return nil
}

func (c *Cache) Stop() error {
	return nil
}
