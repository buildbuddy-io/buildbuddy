package distributed

import (
	"context"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/heartbeat"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type Cache struct {
	local     interfaces.Cache
	myAddr    string
	groupName string
	prefix    string

	cacheProxy       *cacheproxy.CacheProxy
	consistentHash   *consistent_hash.ConsistentHash
	heartbeatChannel *heartbeat.HeartbeatChannel
}

func NewDistributedCache(env environment.Env, c interfaces.Cache, myAddr, groupName string) (*Cache, error) {
	chash := consistent_hash.NewConsistentHash()
	return &Cache{
		local:            c,
		cacheProxy:       cacheproxy.NewCacheProxy(env, c, myAddr),
		myAddr:           myAddr,
		groupName:        groupName,
		consistentHash:   chash,
		heartbeatChannel: heartbeat.NewHeartbeatChannel(env.GetPubSub(), myAddr, groupName, chash.Set),
	}, nil
}

func (c *Cache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	return &Cache{
		local:            c.local.WithPrefix(prefix),
		cacheProxy:       c.cacheProxy,
		myAddr:           c.myAddr,
		groupName:        c.groupName,
		consistentHash:   c.consistentHash,
		heartbeatChannel: c.heartbeatChannel,
		prefix:           newPrefix,
	}
}

func (c *Cache) peer(d *repb.Digest) string {
	peer := c.consistentHash.Get(d.GetHash())
	if peer == "" {
		// if no peer is found, write the file locally.
		peer = c.myAddr
	}
	return peer
}

func (c *Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	peer := c.peer(d)
	if peer == c.myAddr {
		return c.local.Contains(ctx, d)
	}
	return c.cacheProxy.RemoteContains(ctx, peer, c.prefix, d)
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
	peer := c.peer(d)
	if peer == c.myAddr {
		return c.local.Get(ctx, d)
	}
	r, err := c.cacheProxy.RemoteReader(ctx, peer, c.prefix, d, 0)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

func (c *Cache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				data, err := c.Get(ctx, d)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[d] = data
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

func (c *Cache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	peer := c.peer(d)
	if peer == c.myAddr {
		return c.local.Set(ctx, d, data)
	}
	wc, err := c.cacheProxy.RemoteWriter(ctx, peer, c.prefix, d)
	if err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Close()
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
	return status.UnimplementedError("Not yet implemented.")
}

func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	peer := c.peer(d)
	if peer == c.myAddr {
		return c.local.Reader(ctx, d, offset)
	}
	return c.cacheProxy.RemoteReader(ctx, peer, c.prefix, d, offset)
}

func (c *Cache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	peer := c.peer(d)
	if peer == c.myAddr {
		return c.local.Writer(ctx, d)
	}
	return c.cacheProxy.RemoteWriter(ctx, peer, c.prefix, d)
}
