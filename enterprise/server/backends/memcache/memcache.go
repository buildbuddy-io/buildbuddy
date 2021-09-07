package memcache

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"golang.org/x/sync/errgroup"
)

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

func (c *Cache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	newPrefix := filepath.Join(remoteInstanceName, cacheType.Prefix())
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	return &Cache{
		prefix: newPrefix,
		mc:     c.mc,
	}, nil
}

func (c *Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
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

func update(old, new map[string]bool) {
	for k, v := range new {
		old[k] = v
	}
}

func (c *Cache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest]bool, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := c.Contains(ctx, d)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[d] = exists
				return nil
			})
		}
		fetchFn(d)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return foundMap, nil
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
	return c.mc.Delete(k)
}

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
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
