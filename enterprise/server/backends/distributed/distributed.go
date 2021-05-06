package distributed

import (
	"context"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/heartbeat"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/peerset"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Each hinted handoff is a digest (~64 bytes), prefix, and peer
	// (40 bytes). So keeping around 10000 of these means an extra 1MB
	// per peer.
	maxHintedHandoffsPerPeer = 10000
)

type CacheConfig struct {
	PubSub             interfaces.PubSub
	ListenAddr         string
	GroupName          string
	Nodes              []string
	ReplicationFactor  int
	ClusterSize        int
	RPCHeartbeatInterval time.Duration
	DisableLocalLookup bool
}

type hintedHandoffOrder struct {
	prefix string
	d      *repb.Digest
}

type Cache struct {
	local            interfaces.Cache
	doneHeartbeat    chan bool
	lastContactedBy  map[string]time.Time
	hintedHandoffsByPeer map[string]chan *hintedHandoffOrder
	cacheProxy       *cacheproxy.CacheProxy
	consistentHash   *consistent_hash.ConsistentHash
	heartbeatChannel *heartbeat.Channel

	heartbeatMu      *sync.Mutex
	shutDownChan    chan bool
	prefix           string
	config           CacheConfig

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
	if config.RPCHeartbeatInterval == 0 {
		config.RPCHeartbeatInterval = 1 * time.Second
	}
	dc := &Cache{
		local:          c,
		config:         config,
		cacheProxy:     cacheproxy.NewCacheProxy(env, c, config.ListenAddr),
		consistentHash: chash,

		heartbeatMu:     &sync.Mutex{},
		shutDownChan:    make(chan bool, 0),
		lastContactedBy: make(map[string]time.Time, 0),

		hintedHandoffsByPeer: make(map[string]chan *hintedHandoffOrder, 0),
	}
	dc.cacheProxy.SetHeartbeatCallbackFunc(dc.recvHeartbeatCallback)
	dc.cacheProxy.SetHintedHandoffCallbackFunc(dc.recvHintedHandoffCallback)

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

func (c *Cache) recvHeartbeatCallback(peer string) {
	c.heartbeatMu.Lock()
	c.lastContactedBy[peer] = time.Now()
	c.heartbeatMu.Unlock()

	go c.handleHintedHandoffs(peer)
}

func (c *Cache) recvHintedHandoffCallback(peer, prefix string, d *repb.Digest) {
	if _, ok := c.hintedHandoffsByPeer[peer]; !ok {
		// If this is the first hinted handoff for this peer we've
		// received, then initialize the channel.
		c.hintedHandoffsByPeer[peer] = make(chan *hintedHandoffOrder, maxHintedHandoffsPerPeer)
	}
	order := &hintedHandoffOrder{
		prefix: prefix,
		d:      d,
	}
	select {
	case c.hintedHandoffsByPeer[peer] <- order:
		log.Printf("Wrote order %+v to %q's hinted handoff channel", order, peer)
		// write was sucessful
	default:
		log.Warningf("Buffer full: unable to store hinted handoff for %q", peer)
	}
}

func (c *Cache) handleHintedHandoffs(peer string) {
	for {
		select {
		case handoffOrder := <-c.hintedHandoffsByPeer[peer]:
			err := c.backfill(context.Background(), handoffOrder.d, c.config.ListenAddr, handoffOrder.prefix, peer)
			if err != nil {
				log.Printf("%q: unable to complete hinted handoff to peer: %q: %s", c.config.ListenAddr, peer, err)
			}
		default:
			// read was unsuccessful -- no more handoffOrders to process.
			return
		}
	}
}

func (c *Cache) heartbeatPeers() {
	ticker := time.NewTicker(c.config.RPCHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.shutDownChan:
			break
		case <-ticker.C:
			for _, peer := range c.consistentHash.GetItems() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := c.cacheProxy.SendHeartbeat(ctx, peer); err != nil {
					//log.Debugf("%q: unable to reach peer: %q", c.config.ListenAddr, peer)
				}
				cancel()
			}
		}
	}
}

func (c *Cache) StartListening() {
	if c.shutDownChan == nil {
		c.shutDownChan = make(chan bool, 0)
	}
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
	if c.shutDownChan != nil {
		close(c.shutDownChan)
		c.shutDownChan = nil
	}
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
func (c *Cache) peers(d *repb.Digest) *peerset.PeerSet {
	allPeers := c.consistentHash.GetAllReplicas(d.GetHash())
	return peerset.New(allPeers[:c.config.ReplicationFactor], allPeers[c.config.ReplicationFactor:])
}

// readPeers returns a slice of peers responsible for this key. If this peer is
// a member of the set, it is returned first. Other
// peers are returned in random order.
func (c *Cache) readPeers(d *repb.Digest) *peerset.PeerSet {
	allPeers := c.consistentHash.GetAllReplicas(d.GetHash())
	return peerset.NewRead(c.config.ListenAddr, allPeers[:c.config.ReplicationFactor], allPeers[c.config.ReplicationFactor:])
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
func (c *Cache) remoteWriter(ctx context.Context, peer, handoffPeer, prefix string, d *repb.Digest) (io.WriteCloser, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Writer(ctx, d)
	}
	return c.cacheProxy.RemoteWriter(ctx, peer, handoffPeer, prefix, d)
}

func (c *Cache) backfill(ctx context.Context, d *repb.Digest, source, prefix, dest string) error {
	if exists, err := c.remoteContains(ctx, dest, prefix, d); err == nil && exists {
		return nil
	}
	log.Debugf("Backfilling (%s) from source %q to dest %q.", d.GetHash(), source, dest)
	r, err := c.remoteReader(ctx, source, prefix, d, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.remoteWriter(ctx, dest, "", prefix, d)
	if err != nil {
		return err
	}
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Close()
}

func (c *Cache) backfillReplica(ctx context.Context, d *repb.Digest, source, dest string) error {
	return c.backfill(ctx, d, source, c.prefix, dest)
}

type backfillOrder struct {
	d      *repb.Digest
	source string
	dest   string
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

func (c *Cache) getBackfillOrders(d *repb.Digest, ps *peerset.PeerSet) []*backfillOrder {
	source, targets := ps.GetBackfillTargets()
	if len(targets) == 0 {
		return nil
	}

	orders := make([]*backfillOrder, 0, len(targets))
	for _, target := range targets {
		orders = append(orders, &backfillOrder{
			source: source,
			dest:   target,
			d:      d,
		})
	}
	return orders
}

// The first contains result that finds the digest will be returned. If all
// potential peers for the digest are exhausted, then return false.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	ps := c.readPeers(d)
	defer func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(d, ps)); err != nil {
			log.Debugf("Error backfilling peers: %s", err)
		}
	}()

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		exists, err := c.remoteContains(ctx, peer, c.prefix, d)
		if err == nil {
			if exists {
				log.Debugf("Distributed(%s) Contains(%q) found on peer %q", c.config.ListenAddr, d, peer)
				return exists, err
			}
			log.Debugf("Distributed(%s) Contains(%q) not found on peer %q (err: %+v)", c.config.ListenAddr, d, peer, err)
			continue
		}

		// Got an error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)
	}
	return false, nil
}

func (c *Cache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	mu := sync.RWMutex{} // protects(foundMap)
	hashDigests := make(map[string][]*repb.Digest, 0)
	foundMap := make(map[string]bool, len(digests))
	peerMap := make(map[string]*peerset.PeerSet, len(digests))
	for _, d := range digests {
		hash := d.GetHash()
		hashDigests[hash] = append(hashDigests[hash], d)
		if _, ok := peerMap[hash]; !ok {
			peerMap[hash] = c.readPeers(d)
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

			ps := peerMap[h]
			peer := ps.GetNextPeer()
			// If no peers remain, skip this digest, we can't do anything more.
			if peer == "" {
				log.Debugf("Exhausted all peers for %q. Peerset: %+v", h, ps)
				continue
			}
			peerRequests[peer] = append(peerRequests[peer], perHashDigests[0])
		}
		if len(peerRequests) == 0 {
			stillMissing := make([]string, 0)
			for h := range hashDigests {
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
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					for _, d := range digests {
						peerMap[d.GetHash()].MarkPeerAsFailed(peer)
					}
					return nil
				}
				for d, exists := range peerRsp {
					if exists {
						foundMap[d.GetHash()] = exists
					}
				}
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
			d := hashDigests[h][0]
			ps := peerMap[h]
			backfills = append(backfills, c.getBackfillOrders(d, ps)...)
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
	ps := c.readPeers(d)
	defer func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(d, ps)); err != nil {
			log.Debugf("Error backfilling peers: %s", err)
		}
	}()

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		r, err := c.remoteReader(ctx, peer, c.prefix, d, offset)
		if err == nil {
			log.Debugf("Distributed(%s) Reader(%q) found on peer %s", c.config.ListenAddr, d, peer)
			return r, err
		}
		if status.IsNotFoundError(err) {
			log.Debugf("Distributed(%s) Reader(%q) not found on peer %s", c.config.ListenAddr, d, peer)
			continue
		}

		// Some other error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)

	}
	return nil, status.NotFoundErrorf("Exhausted all peers attempting to read %q, peerset: %v", d.GetHash(), ps)
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
	peerMap := make(map[string]*peerset.PeerSet, len(digests))
	for _, d := range digests {
		hash := d.GetHash()
		hashDigests[hash] = append(hashDigests[hash], d)
		if _, ok := peerMap[hash]; !ok {
			peerMap[hash] = c.readPeers(d)
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

			ps := peerMap[h]
			peer := ps.GetNextPeer()
			// If no peers remain, skip this digest, we can't do anything more.
			if peer == "" {
				log.Debugf("Exhausted all peers for %q. Peerset: %+v", h, ps)
				continue
			}
			peerRequests[peer] = append(peerRequests[peer], perHashDigests[0])
		}
		if len(peerRequests) == 0 {
			stillMissing := make([]string, 0)
			for h := range hashDigests {
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
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					log.Debugf("GetMulti: peer %q returned err: %s", peer, err)
					for _, d := range digests {
						peerMap[d.GetHash()].MarkPeerAsFailed(peer)
					}
					return nil
				}
				for d, data := range peerRsp {
					gotMap[d.GetHash()] = data
				}
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
			d := hashDigests[h][0]
			backfills = append(backfills, c.getBackfillOrders(d, ps)...)
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
	mu            *sync.Mutex
	d             *repb.Digest
	listenAddr    string
	totalNumPeers int
}

func (mc *multiWriteCloser) Write(data []byte) (int, error) {
	eg, _ := errgroup.WithContext(mc.ctx)
	for _, wc := range mc.peerClosers {
		wc := wc
		eg.Go(func() error {
			n, err := wc.Write(data)
			if err != nil {
				return err
			}
			if n != len(data) {
				return io.ErrShortWrite
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
				return err
			}
			log.Printf("Succesfully wrote %s to %q", mc.d, peer)
			return nil
		})
	}
	err := eg.Wait()
	if err == nil {
		peers := make([]string, len(mc.peerClosers))
		for peer := range mc.peerClosers {
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
	ps := c.peers(d)
	mwc := &multiWriteCloser{
		ctx:         ctx,
		d:           d,
		peerClosers: make(map[string]io.WriteCloser, 0),
		mu:          &sync.Mutex{},
		listenAddr:  c.config.ListenAddr,
	}
	log.Printf("multiWriter start")
	for peer, hintedHandoff := ps.GetNextPeerAndHandoff(); peer != ""; peer, hintedHandoff = ps.GetNextPeerAndHandoff() {
		rwc, err := c.remoteWriter(ctx, peer, hintedHandoff, c.prefix, d)
		if err != nil {
			ps.MarkPeerAsFailed(peer)
			log.Debugf("Error opening remote writer for %q to peer %q: %s", d.GetHash(), peer, err)
			continue
		}
		mwc.peerClosers[peer] = rwc
		if hintedHandoff != "" {
			log.Printf("Writing to fallback peer: %s, hh to %s, ps: %+v", peer, hintedHandoff, ps)
		}
		if len(mwc.peerClosers) == c.config.ReplicationFactor {
			break
		}
	}
	if len(mwc.peerClosers) < c.config.ReplicationFactor {
		openPeers := make([]string, len(mwc.peerClosers))
		for peer := range mwc.peerClosers {
			openPeers = append(openPeers, peer)
		}
		allPeers := append(ps.PreferredPeers, ps.FallbackPeers...)
		log.Debugf("Could not open enough remoteWriters for digest %s. All peers: %s, opened: %s", d.GetHash(), allPeers, openPeers)
		return nil, status.UnavailableErrorf("Not enough peers (%d) available to satisfy replication factor (%d).", len(mwc.peerClosers), c.config.ReplicationFactor)
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
