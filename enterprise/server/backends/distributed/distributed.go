package distributed

import (
	"context"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

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

type CacheConfig struct {
	ListenAddr         string
	GroupName          string
	ReplicationFactor  int
	DisableLocalLookup bool

	PubSub      interfaces.PubSub
	Nodes       []string
	ClusterSize int
}

type Cache struct {
	local  interfaces.Cache
	config CacheConfig

	prefix           string
	cacheProxy       *cacheproxy.CacheProxy
	consistentHash   *consistent_hash.ConsistentHash
	heartbeatChannel *heartbeat.Channel

	heartbeatMu     *sync.Mutex
	doneHeartbeat   chan bool
	lastContactedBy map[string]time.Time
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

		heartbeatMu:     &sync.Mutex{},
		doneHeartbeat:   make(chan bool, 0),
		lastContactedBy: make(map[string]time.Time, 0),
	}
	dc.cacheProxy.SetHeartbeatCallbackFunc(func(peer string) {
		dc.heartbeatMu.Lock()
		dc.lastContactedBy[peer] = time.Now()
		dc.heartbeatMu.Unlock()
	})
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
	if dc.config.ClusterSize > 0 {
		hc.AddHealthCheck("distributed_cache", dc)
	}
	return dc, nil
}

func (c *Cache) quorum() int {
	half := float64(c.config.ReplicationFactor) / 2.0
	quorum := math.Ceil(half)
	if half == quorum {
		quorum += 1
	}
	return int(quorum)
}

func (c *Cache) Check(ctx context.Context) error {
	// First check that the number of nodes in our chash
	// matches the cluster size. If not, we can return early.
	nodesAvailable := len(c.consistentHash.GetItems())
	if nodesAvailable < c.config.ClusterSize {
		return status.UnavailableErrorf("%d nodes available but cluster size is %d.", nodesAvailable, c.config.ClusterSize)
	}

	// Next check that we're participating in the network:
	// basically, that enough configured peers have *ever* contacted us.
	// TODO(tylerw): Should we have some recency threshold here?
	c.heartbeatMu.Lock()
	nodesInNetwork := len(c.lastContactedBy)
	c.heartbeatMu.Unlock()

	if nodesInNetwork < c.config.ClusterSize {
		return status.UnavailableErrorf("%d nodes in network but cluster size is %d.", nodesInNetwork, c.config.ClusterSize)
	}

	return nil
}

func (c *Cache) heartbeatPeers() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.doneHeartbeat:
			break
		case <-ticker.C:
			for _, peer := range c.consistentHash.GetItems() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := c.cacheProxy.SendHeartbeat(ctx, peer); err != nil {
					log.Debugf("%q: unable to reach peer: %q", c.config.ListenAddr, peer)
				}
				cancel()
			}
		}
	}
}

func (c *Cache) StartListening() {
	go c.heartbeatPeers()
	go func() {
		log.Printf("Distributed cache listening on %q", c.config.ListenAddr)
		if c.heartbeatChannel != nil {
			c.heartbeatChannel.StartAdvertising()
		}
		if err := c.cacheProxy.StartListening(); err != nil {
			log.Warningf("Unable to start cacheproxy: %s", err)
		}
	}()
}

func (c *Cache) Shutdown(ctx context.Context) error {
	log.Printf("Distributed cache shutting down %q", c.config.ListenAddr)
	if c.heartbeatChannel != nil {
		c.heartbeatChannel.StopAdvertising()
	}
	c.doneHeartbeat <- true
	return c.cacheProxy.Shutdown(ctx)
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

// peers returns the ordered slice of replicationFactor peers responsible for
// this key. They should be tried in order.
func (c *Cache) peers(d *repb.Digest) []string {
	return c.consistentHash.GetNReplicas(d.GetHash(), c.config.ReplicationFactor)
}

// readPeers returns a slice of replicationFactor peers responsible for this
// key. If this peer is a member of the set, it is returned first. Other
// peers are returned in random order.
func (c *Cache) readPeers(d *repb.Digest) []string {
	ordered := c.peers(d)
	reordered := make([]string, 0, len(ordered))

	for i := len(ordered) - 1; i >= 0; i-- {
		p := ordered[i]
		if p == c.config.ListenAddr {
			reordered = append(reordered, p)
			ordered = append(ordered[:i], ordered[i+1:]...)
			break
		}
	}
	rand.Shuffle(len(ordered), func(i, j int) {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	})
	reordered = append(reordered, ordered...)
	return reordered
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
	if exists, err := c.remoteContains(ctx, dest, c.prefix, d); err == nil && exists {
		return nil
	}
	log.Debugf("Backfilling (%s) from source %q to dest %q.", d.GetHash(), source, dest)
	r, err := c.remoteReader(ctx, source, c.prefix, d, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.remoteWriter(ctx, dest, c.prefix, d)
	if err != nil {
		return err
	}
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Close()
}

// lets say we're doing a write to 3 nodes.
// we write to 2 of them, but our 3rd node fails.
// we return success, because we've written to a quorum.
// now we get a Contains call, our first request goes
// to the 3rd node, which is now back up. We report
// the digest as missing, and we've told a lie.

// The first contains result that finds the digest will be returned. If all
// potential peers for the digest are exhausted, then return false.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	peers := c.readPeers(d)
	for i, peer := range peers {
		b, err := c.remoteContains(ctx, peer, c.prefix, d)
		if err == nil && b {
			log.Debugf("Distributed(%s) Contains(%q) found on peer %q", c.config.ListenAddr, d, peer)
			if i != 0 {
				if err := c.backfillReplica(ctx, d, peer, peers[i-1]); err != nil {
					log.Debugf("Error backfilling %q => %q: %s", peer, peers[i-1], err)
				}
			}
			return b, err
		}
		log.Debugf("Distributed(%s) Contains(%q) not found on peer %q (err: %+v)", c.config.ListenAddr, d, peer, err)
		continue
	}
	return false, nil
}

type backfillOrder struct {
	source string
	dest   string
	d      *repb.Digest
}

type peerSet struct {
	peers []string
	index int
}

func (c *Cache) backfillPeers(ctx context.Context, backfills []*backfillOrder) error {
	if len(backfills) == 0 {
		return nil
	}
	eg, gCtx := errgroup.WithContext(ctx)
	for _, bf := range backfills {
		bf := bf
		eg.Go(func() error {
			if err := c.backfillReplica(gCtx, bf.d, bf.source, bf.dest); err != nil {
				log.Debugf("Error backfilling %q => %q: %s", bf.source, bf.dest, err)
			}
			return nil
		})
	}
	return eg.Wait()
}

func (c *Cache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	mu := sync.RWMutex{} // protects(foundMap)
	hashDigests := make(map[string][]*repb.Digest, 0)
	foundMap := make(map[string]bool, len(digests))
	peerMap := make(map[string]*peerSet, len(digests))
	for _, d := range digests {
		hash := d.GetHash()
		hashDigests[hash] = append(hashDigests[hash], d)
		if _, ok := peerMap[hash]; !ok {
			peerMap[hash] = &peerSet{
				peers: c.readPeers(d),
				index: 0,
			}
		}
	}

	for {
		// Each iteration through this outer loop sends a "batch" of requests in
		// parallel, until all digests have been found or we have exhausted all
		// peers.
		peerRequests := make(map[string][]*repb.Digest, 0)
		for h, perHashDigests := range hashDigests {
			// If a previous request has already found this digest, skip it.
			if _, ok := foundMap[h]; ok {
				continue
			}
			// If no peers remain, skip this digest, we can't do anything more.
			ps := peerMap[h]
			if len(ps.peers) == ps.index {
				log.Debugf("Exhausted all peers for %q. Peerset: %s", h, ps.peers)
				continue
			}
			peer := ps.peers[ps.index]
			ps.index += 1
			peerRequests[peer] = append(peerRequests[peer], perHashDigests[0])
		}
		if len(peerRequests) == 0 {
			stillMissing := make([]string, 0)
			for h, _ := range hashDigests {
				if _, ok := foundMap[h]; !ok {
					stillMissing = append(stillMissing, h)
				}
			}
			log.Debugf("ContainsMulti: digests not found: %+v", stillMissing)
			// If we aren't able to plan any more batch requests, that means
			// we're out of peers and should exit, returning what we have.
			break
		}
		eg, gCtx := errgroup.WithContext(ctx)
		for peer, digests := range peerRequests {
			peer := peer
			digests := digests
			eg.Go(func() error {
				peerRsp, err := c.remoteContainsMulti(gCtx, peer, c.prefix, digests)
				if err != nil {
					return nil
				}
				mu.Lock()
				for d, exists := range peerRsp {
					if exists {
						foundMap[d.GetHash()] = exists
					}
				}
				mu.Unlock()
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			if err != context.Canceled {
				// Don't log context cancelled errors, they are common and expected when
				// clients cancel a request.
				log.Debugf("Error checking contains batch; will retry: %s", err)
			}
			continue
		}
		if len(foundMap) == len(hashDigests) {
			// If we've found everything, we can exit now.
			break
		}
	}

	// For every digest we found, if we did not find it
	// on the first peer in our list, we want to backfill it.
	backfills := make([]*backfillOrder, 0)
	for h, exists := range foundMap {
		if exists {
			ps := peerMap[h]
			if ps.index > 1 {
				bo := backfillOrder{
					source: ps.peers[ps.index-1],
					dest:   ps.peers[ps.index-2],
					d:      hashDigests[h][0],
				}
				log.Printf("backfill order %d is: %+v", len(backfills), bo)
				backfills = append(backfills, &bo)
			}
		}
	}
	if err := c.backfillPeers(ctx, backfills); err != nil {
		log.Debugf("Error backfilling peers: %s", err)
	}

	rsp := make(map[*repb.Digest]bool, len(digests))
	for _, d := range digests {
		rsp[d] = foundMap[d.GetHash()]
	}
	return rsp, nil
}

// The first reader with a non-empty value will be returned. If all potential
// peers for the digest are exhausted, then return a NotFoundError.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) distributedReader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	peers := c.readPeers(d)
	for i, peer := range peers {
		r, err := c.remoteReader(ctx, peer, c.prefix, d, offset)
		if err == nil {
			log.Debugf("Distributed(%s) Reader(%q) found on peer %s", c.config.ListenAddr, d, peer)
			if i != 0 {
				if err := c.backfillReplica(ctx, d, peer, peers[i-1]); err != nil {
					log.Debugf("Error backfilling %q => %q: %s", peer, peers[i-1], err)
				}
			}
			return r, err
		}
		log.Debugf("Distributed(%s) Reader(%q) not found on peer %s", c.config.ListenAddr, d, peer)
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

func (c *Cache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	mu := sync.RWMutex{} // protects(gotMap)
	hashDigests := make(map[string][]*repb.Digest, 0)
	gotMap := make(map[string][]byte, len(digests))
	peerMap := make(map[string]*peerSet, len(digests))
	for _, d := range digests {
		hash := d.GetHash()
		hashDigests[hash] = append(hashDigests[hash], d)
		if _, ok := peerMap[hash]; !ok {
			peerMap[hash] = &peerSet{
				peers: c.readPeers(d),
				index: 0,
			}
		}
	}

	for {
		// Each iteration through this outer loop sends a "batch" of requests in
		// parallel, until all digests have been found or we have exhausted all
		// peers.
		peerRequests := make(map[string][]*repb.Digest, 0)
		for h, perHashDigests := range hashDigests {
			// If a previous request has already found this digest, skip it.
			if _, ok := gotMap[h]; ok {
				continue
			}
			// If no peers remain, skip this digest, we can't do anything more.
			ps := peerMap[h]
			if len(ps.peers) == ps.index {
				log.Debugf("Exhausted all peers for %q. Peerset: %s", h, ps.peers)
				continue
			}
			peer := ps.peers[ps.index]
			ps.index += 1
			peerRequests[peer] = append(peerRequests[peer], perHashDigests[0])
		}
		if len(peerRequests) == 0 {
			stillMissing := make([]string, 0)
			for h, _ := range hashDigests {
				if _, ok := gotMap[h]; !ok {
					stillMissing = append(stillMissing, h)
				}
			}
			// If we aren't able to plan any more batch requests, that means
			// we're out of peers and should exit, returning what we have.
			break
		}
		eg, gCtx := errgroup.WithContext(ctx)
		for peer, digests := range peerRequests {
			peer := peer
			digests := digests
			eg.Go(func() error {
				peerRsp, err := c.remoteGetMulti(gCtx, peer, c.prefix, digests)
				if err != nil {
					log.Debugf("GetMulti: peer %q returned err: %s", peer, err)
					return nil
				}
				mu.Lock()
				for d, data := range peerRsp {
					gotMap[d.GetHash()] = data
				}
				mu.Unlock()
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			if err != context.Canceled {
				// Don't log context cancelled errors, they are common and expected when
				// clients cancel a request.
				log.Debugf("Error checking contains batch; will retry: %s", err)
			}
			continue
		}
		if len(gotMap) == len(hashDigests) {
			// If we've found everything, we can exit now.
			break
		}
	}

	// For every digest we found, if we did not find it
	// on the first peer in our list, we want to backfill it.
	backfills := make([]*backfillOrder, 0)
	for h, buf := range gotMap {
		if len(buf) > 0 {
			ps := peerMap[h]
			if ps.index > 1 {
				bo := backfillOrder{
					source: ps.peers[ps.index-1],
					dest:   ps.peers[ps.index-2],
					d:      hashDigests[h][0],
				}
				log.Printf("backfill order %d is: %+v", len(backfills), bo)
				backfills = append(backfills, &bo)
			}
		}
	}
	if err := c.backfillPeers(ctx, backfills); err != nil {
		log.Debugf("Error backfilling peers: %s", err)
	}

	rsp := make(map[*repb.Digest][]byte, len(digests))
	for _, d := range digests {
		rsp[d] = gotMap[d.GetHash()]
	}
	return rsp, nil
}

type multiWriteCloser struct {
	ctx           context.Context
	peerClosers   map[string]io.WriteCloser
	totalNumPeers int
	mu            *sync.Mutex
	d             *repb.Digest
	listenAddr    string
}

func (mc *multiWriteCloser) failCloserWithError(peer string, err error) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.peerClosers, peer)
	log.Debugf("Peer %q failed mid-write of %q with error: %s. Removing from active-write set.", peer, mc.d.GetHash(), err)
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
		peer := peer
		eg.Go(func() error {
			if err := wc.Close(); err != nil {
				return mc.failCloserWithError(peer, err)
			}
			log.Printf("Succesfully wrote %s to %q", mc.d, peer)
			return nil
		})
	}
	err := eg.Wait()
	if err == nil {
		peers := make([]string, len(mc.peerClosers))
		for peer, _ := range mc.peerClosers {
			peers = append(peers, peer)
		}
		log.Debugf("Distributed(%s) Writer(%q) successfully wrote to peers %s", mc.listenAddr, mc.d, peers)
	}
	return err
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
		listenAddr:    c.config.ListenAddr,
	}

	for _, peer := range peers {
		rwc, err := c.remoteWriter(ctx, peer, c.prefix, d)
		if err != nil {
			log.Debugf("Error opening remote writer for %q to peer %q: %s", d.GetHash(), peer, err)
			continue
		}
		mwc.peerClosers[peer] = rwc
	}
	if len(mwc.peerClosers) > len(peers)/2 {
		return mwc, nil
	}

	openPeers := make([]string, len(mwc.peerClosers))
	for peer, _ := range mwc.peerClosers {
		openPeers = append(openPeers, peer)
	}
	log.Debugf("Could not open enough remoteWriters to satisfy quorum for digest %s. All peers: %s, opened: %s", d.GetHash(), peers, openPeers)
	return nil, status.UnavailableErrorf("Exhausted all peers attempting to write %q, peers: %s", d.GetHash(), peers)

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
