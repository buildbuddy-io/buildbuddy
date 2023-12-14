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
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var memcacheTargets = flag.Slice("cache.memcache_targets", []string{}, "Deprecated. Use Redis Target instead.")

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
	mc *memcache.Client
}

func Register(env *real_environment.RealEnv) error {
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
		mc: memcache.New(mcServers...),
	}
}

func (c *Cache) key(ctx context.Context, r *rspb.ResourceName) (string, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	isolationPrefix := filepath.Join(r.GetInstanceName(), digest.CacheTypeToPrefix(r.GetCacheType()))
	if len(isolationPrefix) > 0 && isolationPrefix[len(isolationPrefix)-1] != '/' {
		isolationPrefix += "/"
	}
	return userPrefix + isolationPrefix + rn.GetDigest().GetHash(), nil
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

func (c *Cache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	key, err := c.key(ctx, r)
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
func (c *Cache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	key, err := c.key(ctx, r)
	if err != nil {
		return nil, err
	}

	data, err := c.mcGet(key)
	if err != nil {
		return nil, err
	}
	if err == memcache.ErrCacheMiss {
		d := r.GetDigest()
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}

	// TODO - Add digest size support for AC
	digestSizeBytes := int64(-1)
	if r.GetCacheType() == rspb.CacheType_CAS {
		digestSizeBytes = int64(len(data))
	}

	return &interfaces.CacheMetadata{
		StoredSizeBytes: int64(len(data)),
		DigestSizeBytes: digestSizeBytes,
	}, nil
}

func (c *Cache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	lock := sync.RWMutex{} // protects(missing)
	var missing []*repb.Digest
	eg, ctx := errgroup.WithContext(ctx)

	for _, r := range resources {
		fetchFn := func(r *rspb.ResourceName) {
			eg.Go(func() error {
				exists, err := c.Contains(ctx, r)
				if err != nil {
					return err
				}
				if !exists {
					lock.Lock()
					defer lock.Unlock()
					missing = append(missing, r.GetDigest())
				}
				return nil
			})
		}
		fetchFn(r)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return missing, nil
}

func (c *Cache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	d := r.GetDigest()
	if !eligibleForMc(d) {
		return nil, status.ResourceExhaustedErrorf("Get: Digest %v too big for memcache", d)
	}
	k, err := c.key(ctx, r)
	if err != nil {
		return nil, err
	}

	return c.mcGet(k)
}

func (c *Cache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	keys := make([]string, 0, len(resources))
	digestsByKey := make(map[string]*repb.Digest, len(resources))
	for _, r := range resources {
		k, err := c.key(ctx, r)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
		digestsByKey[k] = r.GetDigest()
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

func (c *Cache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	if !eligibleForMc(r.GetDigest()) {
		return status.ResourceExhaustedErrorf("Set: Digest %v too big for memcache", r.GetDigest())
	}
	k, err := c.key(ctx, r)
	if err != nil {
		return err
	}

	return c.mcSet(k, data)
}

func (c *Cache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	eg, ctx := errgroup.WithContext(ctx)

	for r, data := range kvs {
		setFn := func(r *rspb.ResourceName, data []byte) {
			eg.Go(func() error {
				return c.Set(ctx, r, data)
			})
		}
		setFn(r, data)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *Cache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	k, err := c.key(ctx, r)
	if err != nil {
		return err
	}
	err = c.mc.Delete(k)
	if errors.Is(err, memcache.ErrCacheMiss) {
		d := r.GetDigest()
		return status.NotFoundErrorf("digest %s/%d not found in memcache: %s", d.GetHash(), d.GetSizeBytes(), err.Error())
	}
	return err

}

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, rn *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	if !eligibleForMc(rn.GetDigest()) {
		return nil, status.ResourceExhaustedErrorf("Reader: Digest %v too big for memcache", rn.GetDigest())
	}
	k, err := c.key(ctx, rn)
	if err != nil {
		return nil, err
	}
	buf, err := c.mcGet(k)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(buf)
	r.Seek(uncompressedOffset, 0)
	length := rn.GetDigest().GetSizeBytes()
	if limit != 0 && limit < length {
		length = limit
	}
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	return io.NopCloser(r), nil
}

func (c *Cache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	if !eligibleForMc(r.GetDigest()) {
		return nil, status.ResourceExhaustedErrorf("Writer: Digest %v too big for memcache", r.GetDigest())
	}
	k, err := c.key(ctx, r)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	wc := ioutil.NewCustomCommitWriteCloser(&buffer)
	wc.CommitFn = func(int64) error {
		// Locking and key prefixing are handled in Set.
		return c.mcSet(k, buffer.Bytes())
	}
	return wc, nil

}

func (c *Cache) Start() error {
	return nil
}

func (c *Cache) Stop() error {
	return nil
}

func (c *Cache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY
}

func (c *Cache) SupportsEncryption(ctx context.Context) bool {
	return false
}
