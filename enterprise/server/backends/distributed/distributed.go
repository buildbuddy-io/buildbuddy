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
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type Cache struct {
	local  interfaces.Cache
	config CacheConfig

	prefix           string
	cacheProxy       *cacheproxy.CacheProxy
	consistentHash   *consistent_hash.ConsistentHash
	heartbeatChannel *heartbeat.HeartbeatChannel
}

type CacheConfig struct {
	ListenAddr         string
	GroupName          string
	ReplicationFactor  int
	DisableLocalLookup bool

	PubSub interfaces.PubSub
	Nodes  []string
}

// NewDistributedCache creates a new cache by wrapping the provided cache "c",
// in a HTTP API and announcing its presence over redis to other distributed
// cache nodes. Together, these distributed caches each maintain a consistent
// hash ring which identifies the owner (and replicas) of any stored keys.
//  - myAddr is the interface to listen on in "host:port" format.
//  - groupName is a string namespace which the distributed cache nodes must
// match to peer.
//  - replicationFactor is an int specifying how many copies of each key will
// be stored across unique caches.
func NewDistributedCache(env environment.Env, c interfaces.Cache, config CacheConfig, hc interfaces.HealthChecker) (*Cache, error) {
	chash := consistent_hash.NewConsistentHash()
	dc := &Cache{
		local:          c,
		config:         config,
		cacheProxy:     cacheproxy.NewCacheProxy(env, c, config.ListenAddr),
		consistentHash: chash,
	}
	if len(config.Nodes) > 0 {
		// Nodes are hardcoded. Set them once and be done with it.
		chash.Set(config.Nodes...)
	} else {
		// No nodes were hardcoded, use redis for discovery.
		heartbeatConfig := &heartbeat.Config{
			MyPublicAddr:     config.ListenAddr,
			GroupName:        config.GroupName,
			UpdateFn:         chash.Set,
			EnablePeerExpiry: false,
		}
		dc.heartbeatChannel = heartbeat.NewHeartbeatChannel(config.PubSub, heartbeatConfig)
	}
	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		return dc.Shutdown(ctx)
	})
	return dc, nil
}

func (c *Cache) StartListening() {
	go func() {
		log.Printf("Distributed cache listening on %q", c.config.ListenAddr)
		if c.heartbeatChannel != nil {
			c.heartbeatChannel.StartAdvertising()
		}
		c.cacheProxy.Server().ListenAndServe()
	}()
}

func (c *Cache) Shutdown(ctx context.Context) error {
	log.Printf("Distributed cache shutting down %q", c.config.ListenAddr)
	if c.heartbeatChannel != nil {
		c.heartbeatChannel.StopAdvertising()
	}
	return c.cacheProxy.Server().Shutdown(ctx)
}

func (c *Cache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	clone := *c
	clone.prefix = newPrefix
	clone.local = c.local.WithPrefix(prefix)
	return &clone
}

// peers returns the ordered slice of replicationFactor peers
// responsible for this key. They should be tried in order.
func (c *Cache) peers(d *repb.Digest) []string {
	return c.consistentHash.GetNReplicas(d.GetHash(), c.config.ReplicationFactor)
}

func (c *Cache) remoteContains(ctx context.Context, peer, prefix string, d *repb.Digest) (bool, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Contains(ctx, d)
	}
	return c.cacheProxy.RemoteContains(ctx, peer, prefix, d)
}
func (c *Cache) remoteContainsMulti(ctx context.Context, peer, prefix string, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.ContainsMulti(ctx, digests)
	}
	return c.cacheProxy.RemoteContainsMulti(ctx, peer, prefix, digests)
}
func (c *Cache) remoteGetMulti(ctx context.Context, peer, prefix string, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.GetMulti(ctx, digests)
	}
	return c.cacheProxy.RemoteGetMulti(ctx, peer, prefix, digests)
}
func (c *Cache) remoteReader(ctx context.Context, peer, prefix string, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Reader(ctx, d, offset)
	}
	return c.cacheProxy.RemoteReader(ctx, peer, prefix, d, offset)
}
func (c *Cache) remoteWriter(ctx context.Context, peer, prefix string, d *repb.Digest) (io.WriteCloser, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Writer(ctx, d)
	}
	return c.cacheProxy.RemoteWriter(ctx, peer, prefix, d)
}

func (c *Cache) backfillReplica(ctx context.Context, d *repb.Digest, source, dest string) error {
	log.Printf("Backfilling %q from %q => %q", d.GetHash(), source, dest)
	r, err := c.cacheProxy.RemoteReader(ctx, source, c.prefix, d, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.cacheProxy.RemoteWriter(ctx, dest, c.prefix, d)
	if err != nil {
		return err
	}
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Close()
}

// The first contains result with a nil err will be returned. If all potential
// peers for the digest are exhausted, then return a NotFoundError.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	peers := c.peers(d)
	for i, peer := range peers {
		b, err := c.remoteContains(ctx, peer, c.prefix, d)
		if err == nil {
			if i != 0 {
				c.backfillReplica(ctx, d, peer, peers[i-1])
			}
			return b, err
		}
		continue
	}
	log.Printf("Exhausted all peers attempting to check (contains) %q: peers: %s", d.GetHash(), peers)
	return false, nil
}

func (c *Cache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest]bool, len(digests))
	peerMap := make(map[*repb.Digest][]string, len(digests))
	for _, d := range digests {
		peerMap[d] = c.peers(d)
	}

	for {
		// Each iteration through this outer loop sends a "batch" of requests in
		// parallel, until all digests have been returned from a request without
		// error or we have exhausted our set of peers.
		eg, gCtx := errgroup.WithContext(ctx)
		peerRequests := make(map[string][]*repb.Digest, 0)
		for _, d := range digests {
			// If a previous request has already found this digest, skip it.
			if _, ok := foundMap[d]; ok {
				continue
			}
			// If no peers remain, return an error.
			peers := peerMap[d]
			if len(peers) == 0 {
				return nil, status.NotFoundErrorf("Exhausted all peers attempting to check (batch contains) %q", d.GetHash())
			}
			peer := peers[0]
			peers = peers[1:]
			peerRequests[peer] = append(peerRequests[peer], d)
		}
		for peer, digests := range peerRequests {
			peer := peer
			digests := digests
			eg.Go(func() error {
				foundOnPeer, err := c.cacheProxy.RemoteContainsMulti(gCtx, peer, c.prefix, digests)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				for d, exists := range foundOnPeer {
					foundMap[d] = exists
				}
				return nil
			})
		}
		err := eg.Wait()
		if err == nil {
			return foundMap, nil
		}
		log.Warningf("Error checking contains batch; will retry: %s", err)
	}

	return foundMap, nil
}

// The first reader with a non-empty value will be returned. If all potential
// peers for the digest are exhausted, then return a NotFoundError.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) distributedReader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	peers := c.peers(d)
	for i, peer := range peers {
		r, err := c.remoteReader(ctx, peer, c.prefix, d, offset)
		if err == nil {
			if i != 0 {
				if err := c.backfillReplica(ctx, d, peer, peers[i-1]); err != nil {
					log.Printf("Error backfilling %q => %q: %s", peer, peers[i-1], err)
				}
			}
			return r, err
		}
		continue
	}
	return nil, status.NotFoundErrorf("Exhausted all peers attempting to read %q, peers: %s", d.GetHash(), peers)
}

func (c *Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	r, err := c.distributedReader(ctx, d, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (c *Cache) GetMultiOld(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
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

func (c *Cache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	gotMap := make(map[*repb.Digest][]byte, len(digests))
	peerMap := make(map[*repb.Digest][]string, len(digests))
	for _, d := range digests {
		peerMap[d] = c.peers(d)
	}

	for {
		// Each iteration through this outer loop sends a "batch" of requests in
		// parallel, until all digests have been returned from a request without
		// error or we have exhausted our set of peers.
		eg, gCtx := errgroup.WithContext(ctx)
		peerRequests := make(map[string][]*repb.Digest, 0)
		for _, d := range digests {
			// If a previous request has already found this digest, skip it.
			if _, ok := gotMap[d]; ok {
				continue
			}
			// If no peers remain, return an error.
			peers := peerMap[d]
			if len(peers) == 0 {
				return nil, status.NotFoundErrorf("Exhausted all peers attempting to check (batch contains) %q", d.GetHash())
			}
			peer := peers[0]
			peers = peers[1:]
			peerRequests[peer] = append(peerRequests[peer], d)
		}
		for peer, digests := range peerRequests {
			peer := peer
			digests := digests
			eg.Go(func() error {
				got, err := c.cacheProxy.RemoteGetMulti(gCtx, peer, c.prefix, digests)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				for d, buf := range got {
					gotMap[d] = buf
				}
				return nil
			})
		}
		err := eg.Wait()
		if err == nil {
			return gotMap, nil
		}
		log.Warningf("Error reading batch; will retry: %s", err)
	}
	return gotMap, nil
}

type multiWriteCloser struct {
	ctx           context.Context
	peerClosers   map[string]io.WriteCloser
	totalNumPeers int
	mu            *sync.Mutex
	d             *repb.Digest
}

func (mc *multiWriteCloser) failCloserWithError(peer string, err error) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.peerClosers, peer)
	writersRemaining := len(mc.peerClosers)
	var allPeers []string
	for peer, _ := range mc.peerClosers {
		allPeers = append(allPeers, peer)
	}
	if writersRemaining < mc.totalNumPeers/2 {
		return status.UnavailableErrorf("Exhausted all peers attempting to write %q, peers: %s", mc.d.GetHash(), allPeers)
	}
	return nil

}
func (mc *multiWriteCloser) Write(data []byte) (int, error) {
	eg, _ := errgroup.WithContext(mc.ctx)
	for peer, wc := range mc.peerClosers {
		peer := peer
		wc := wc
		eg.Go(func() error {
			n, err := wc.Write(data)
			if err != nil {
				return mc.failCloserWithError(peer, err)
			}
			if n != len(data) {
				return mc.failCloserWithError(peer, io.ErrShortWrite)
			}
			return nil
		})
	}

	return len(data), eg.Wait()
}

func (mc *multiWriteCloser) Close() error {
	eg, _ := errgroup.WithContext(mc.ctx)
	for peer, wc := range mc.peerClosers {
		wc := wc
		eg.Go(func() error {
			if err := wc.Close(); err != nil {
				return mc.failCloserWithError(peer, err)
			}
			return nil
		})
	}
	return eg.Wait()
}

// Attempt to write digest to N peers (where N == replicationFactor).
// Return an unavailable error if less than a quarum of peers can be
// written to.
//
// This is like setting WRITE_CONSISTENCY = QUORUM.
func (c *Cache) multiWriter(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	peers := c.peers(d)
	mwc := &multiWriteCloser{
		ctx:           ctx,
		d:             d,
		peerClosers:   make(map[string]io.WriteCloser, 0),
		totalNumPeers: len(peers),
		mu:            &sync.Mutex{},
	}

	for _, peer := range peers {
		rwc, err := c.remoteWriter(ctx, peer, c.prefix, d)
		if err == nil {
			mwc.peerClosers[peer] = rwc
		}
	}
	return mwc, nil
}

func (c *Cache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	wc, err := c.multiWriter(ctx, d)
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

func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	return c.distributedReader(ctx, d, offset)
}

func (c *Cache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	return c.multiWriter(ctx, d)
}
