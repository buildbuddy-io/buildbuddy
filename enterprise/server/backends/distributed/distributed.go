package distributed

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/heartbeat"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/peerset"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	listenAddr        = flag.String("cache.distributed_cache.listen_addr", "", "The address to listen for local BuildBuddy distributed cache traffic on.")
	redisTarget       = flagutil.New("cache.distributed_cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **", flagutil.SecretTag)
	groupName         = flag.String("cache.distributed_cache.group_name", "", "A unique name for this distributed cache group. ** Enterprise only **")
	nodes             = flagutil.New("cache.distributed_cache.nodes", []string{}, "The hardcoded list of peer distributed cache nodes. If this is set, redis_target will be ignored. ** Enterprise only **")
	replicationFactor = flag.Int("cache.distributed_cache.replication_factor", 0, "How many total servers the data should be replicated to. Must be >= 1. ** Enterprise only **")
	clusterSize       = flag.Int("cache.distributed_cache.cluster_size", 0, "The total number of nodes in this cluster. Required for health checking. ** Enterprise only **")
	enableLocalWrites = flag.Bool("cache.distributed_cache.enable_local_writes", false, "If enabled, shortcuts distributed writes that belong to the local shard to local cache instead of making an RPC.")
)

const (
	// Each hinted handoff is a digest (~64 bytes), prefix, and peer
	// (40 bytes). So keeping around 100000 of these means an extra 10MB
	// per peer.
	maxHintedHandoffsPerPeer = 100000
)

type CacheConfig struct {
	PubSub               interfaces.PubSub
	ListenAddr           string
	GroupName            string
	Nodes                []string
	ReplicationFactor    int
	ClusterSize          int
	RPCHeartbeatInterval time.Duration
	DisableLocalLookup   bool
	EnableLocalWrites    bool
}

type hintedHandoffOrder struct {
	ctx       context.Context
	d         *repb.Digest
	isolation *dcpb.Isolation
}

func (o *hintedHandoffOrder) String() string {
	return fmt.Sprintf("{digest:%q isolation:{%s}}", o.d.GetHash(), o.isolation)
}

type peerInfo struct {
	lastContact time.Time
	zone        string
}

type Cache struct {
	local                interfaces.Cache
	log                  log.Logger
	peerMetadata         map[string]*peerInfo
	hintedHandoffsMu     *sync.RWMutex
	hintedHandoffsByPeer map[string]chan *hintedHandoffOrder
	cacheProxy           *cacheproxy.CacheProxy
	consistentHash       *consistent_hash.ConsistentHash
	heartbeatChannel     *heartbeat.Channel
	heartbeatMu          *sync.Mutex
	shutdownMu           *sync.RWMutex
	shutDownChan         chan struct{}
	finishedShutdown     bool
	isolation            *dcpb.Isolation
	config               CacheConfig
	zone                 string
}

func Register(env environment.Env) error {
	if *listenAddr == "" {
		return nil
	}
	if env.GetCache() == nil {
		return status.FailedPreconditionErrorf("Distributed Cache requires a base cache but one was not configured: please also enable a base cache")
	}
	dcConfig := CacheConfig{
		ListenAddr:        *listenAddr,
		GroupName:         *groupName,
		ReplicationFactor: *replicationFactor,
		Nodes:             *nodes,
		ClusterSize:       *clusterSize,
		EnableLocalWrites: *enableLocalWrites,
	}
	log.Infof("Enabling distributed cache with config: %+v", dcConfig)
	if len(dcConfig.Nodes) == 0 {
		dcConfig.PubSub = pubsub.NewPubSub(redisutil.NewSimpleClient(*redisTarget, env.GetHealthChecker(), "distributed_cache_redis"))
	}
	dc, err := NewDistributedCache(env, env.GetCache(), dcConfig, env.GetHealthChecker())
	if err != nil {
		log.Fatalf("Error enabling distributed cache: %s", err.Error())
	}
	dc.StartListening()
	env.SetCache(dc)
	return nil
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
		log:            log.NamedSubLogger(fmt.Sprintf("Coordinator(%s)", config.ListenAddr)),
		config:         config,
		cacheProxy:     cacheproxy.NewCacheProxy(env, c, config.ListenAddr),
		consistentHash: chash,
		isolation:      &dcpb.Isolation{},

		heartbeatMu:      &sync.Mutex{},
		shutdownMu:       &sync.RWMutex{},
		shutDownChan:     nil,
		finishedShutdown: true,
		peerMetadata:     make(map[string]*peerInfo, 0),

		hintedHandoffsMu:     &sync.RWMutex{},
		hintedHandoffsByPeer: make(map[string]chan *hintedHandoffOrder, 0),
	}
	zone, err := resources.GetZone()
	if err != nil {
		log.Warningf("Error detecting zone: %s", err)
	} else {
		dc.zone = zone
	}
	dc.cacheProxy.SetHeartbeatCallbackFunc(dc.recvHeartbeatCallback)
	dc.cacheProxy.SetHintedHandoffCallbackFunc(dc.recvHintedHandoffCallback)
	if len(config.Nodes) > 0 {
		// Nodes are hardcoded. Set them once and be done with it.
		chash.Set(config.Nodes...)
	} else {
		// No nodes were hardcoded, use redis for discovery.
		heartbeatConfig := &heartbeat.Config{
			MyPublicAddr: config.ListenAddr,
			GroupName:    config.GroupName,
			UpdateFn: func(peers ...string) {
				if err := chash.Set(peers...); err != nil {
					log.Errorf("Error setting peers in consistent hash: %s", err)
				}
			},
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
	nodesInNetwork := len(c.peerMetadata)
	c.heartbeatMu.Unlock()

	if nodesInNetwork < c.config.ClusterSize {
		return status.UnavailableErrorf("%d nodes in network but cluster size is %d.", nodesInNetwork, c.config.ClusterSize)
	}

	return nil
}

func (c *Cache) recvHeartbeatCallback(ctx context.Context, peer string) {
	pi := &peerInfo{
		lastContact: time.Now(),
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		zoneVals := md.Get(resources.ZoneHeader)
		if len(zoneVals) == 1 {
			pi.zone = zoneVals[0]
		}
	}
	c.heartbeatMu.Lock()
	c.peerMetadata[peer] = pi
	c.heartbeatMu.Unlock()
}

func (c *Cache) recvHintedHandoffCallback(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) {
	c.hintedHandoffsMu.Lock()
	defer c.hintedHandoffsMu.Unlock()
	if _, ok := c.hintedHandoffsByPeer[peer]; !ok {
		// If this is the first hinted handoff for this peer we've
		// received, then initialize the channel.
		c.hintedHandoffsByPeer[peer] = make(chan *hintedHandoffOrder, maxHintedHandoffsPerPeer)
	}
	order := &hintedHandoffOrder{
		ctx:       ctx,
		isolation: isolation,
		d:         d,
	}
	select {
	case c.hintedHandoffsByPeer[peer] <- order:
		log.Debugf("Wrote order %+v to %q's hinted handoff channel", order, peer)
		// write was sucessful
	default:
		log.Warningf("Buffer full: unable to store hinted handoff for %q", peer)
	}
}

func (c *Cache) handleHintedHandoffs(peer string) {
	c.hintedHandoffsMu.RLock()
	defer c.hintedHandoffsMu.RUnlock()
	for {
		select {
		case handoffOrder := <-c.hintedHandoffsByPeer[peer]:
			ctx, cancel := background.ExtendContextForFinalization(handoffOrder.ctx, 10*time.Second)
			err := c.sendFile(ctx, handoffOrder.d, handoffOrder.isolation, peer)
			if err != nil {
				c.log.Warningf("unable to complete hinted handoff to peer: %q: %s (order %s)", peer, err, handoffOrder)
				return
			}
			c.log.Debugf("completed hinted handoff to peer: %q", peer)
			cancel()
		default:
			// read was unsuccessful -- no more handoffOrders to process.
			return
		}
	}
}

func (c *Cache) heartbeatPeers(shutDownChan chan struct{}) {
	ticker := time.NewTicker(c.config.RPCHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-shutDownChan:
			return
		case <-ticker.C:
			for _, peer := range c.consistentHash.GetItems() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := c.cacheProxy.SendHeartbeat(ctx, peer); err == nil {
					// Trigger handoffs if we were able to ping this peer.
					go c.handleHintedHandoffs(peer)
				}
				cancel()
			}
		}
	}
}

func (c *Cache) StartListening() {
	c.shutdownMu.Lock()
	defer c.shutdownMu.Unlock()

	if c.finishedShutdown == false {
		return
	}
	c.shutDownChan = make(chan struct{}, 0)
	go c.heartbeatPeers(c.shutDownChan)
	go func() {
		log.Infof("Distributed cache listening on %q", c.config.ListenAddr)
		if c.heartbeatChannel != nil {
			c.heartbeatChannel.StartAdvertising()
		}
		if err := c.cacheProxy.StartListening(); err != nil {
			log.Warningf("Unable to start cacheproxy: %s", err)
		}
	}()
	c.finishedShutdown = false
}

func (c *Cache) Shutdown(ctx context.Context) error {
	log.Infof("Distributed cache shutting down %q", c.config.ListenAddr)
	c.shutdownMu.Lock()
	defer c.shutdownMu.Unlock()
	if c.finishedShutdown {
		log.Printf("Already finished shutdown, returning early.")
		return nil
	}

	if c.heartbeatChannel != nil {
		c.heartbeatChannel.StopAdvertising()
	}
	close(c.shutDownChan)
	c.finishedShutdown = true
	return c.cacheProxy.Shutdown(ctx)
}

func toProtoCacheType(cacheType interfaces.CacheTypeDeprecated) (dcpb.Isolation_CacheType, error) {
	switch cacheType {
	case interfaces.CASCacheType:
		return dcpb.Isolation_CAS_CACHE, nil
	case interfaces.ActionCacheType:
		return dcpb.Isolation_ACTION_CACHE, nil
	default:
		return dcpb.Isolation_UNKNOWN_TYPE, status.InvalidArgumentErrorf("Unknown cache type %v", cacheType)
	}
}

func (c *Cache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
	newLocal, err := c.local.WithIsolation(ctx, cacheType, remoteInstanceName)
	if err != nil {
		return nil, err
	}

	newPrefix := filepath.Join(remoteInstanceName, cacheType.Prefix())
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	clone := *c
	protoCacheType, err := toProtoCacheType(cacheType)
	if err != nil {
		return nil, err
	}
	clone.isolation = &dcpb.Isolation{
		CacheType:          protoCacheType,
		RemoteInstanceName: remoteInstanceName,
	}
	clone.local = newLocal
	return &clone, nil
}

func (c *Cache) peerZone(peer string) (string, bool) {
	c.heartbeatMu.Lock()
	pi, ok := c.peerMetadata[peer]
	c.heartbeatMu.Unlock()
	if ok && pi.zone != "" {
		return pi.zone, true
	}
	return "", false
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
	primaryPeers := allPeers[:c.config.ReplicationFactor]
	secondaryPeers := allPeers[c.config.ReplicationFactor:]

	sortVal := func(peer string) int {
		if peer == c.config.ListenAddr {
			return 0
		} else if zone, ok := c.peerZone(peer); ok && zone == c.zone {
			return 1
		} else {
			return 2
		}
	}
	sort.Slice(primaryPeers, func(i, j int) bool {
		return sortVal(primaryPeers[i]) < sortVal(primaryPeers[j])
	})
	return peerset.New(primaryPeers, secondaryPeers)
}

func (c *Cache) remoteContains(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) (bool, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		// No prefix necessary -- it's already set on the local cache.
		return c.local.ContainsDeprecated(ctx, d)
	}
	return c.cacheProxy.RemoteContains(ctx, peer, isolation, d)
}

func (c *Cache) remoteMetadata(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		// No prefix necessary -- it's already set on the local cache.
		return c.local.Metadata(ctx, d)
	}
	return c.cacheProxy.RemoteMetadata(ctx, peer, isolation, d)
}

func (c *Cache) remoteFindMissing(ctx context.Context, peer string, isolation *dcpb.Isolation, digests []*repb.Digest) ([]*repb.Digest, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		// No prefix necessary -- it's already set on the local cache.
		return c.local.FindMissing(ctx, digests)
	}
	return c.cacheProxy.RemoteFindMissing(ctx, peer, isolation, digests)
}

func (c *Cache) remoteGetMulti(ctx context.Context, peer string, isolation *dcpb.Isolation, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		// No prefix necessary -- it's already set on the local cache.
		return c.local.GetMulti(ctx, digests)
	}
	return c.cacheProxy.RemoteGetMulti(ctx, peer, isolation, digests)
}
func (c *Cache) remoteReader(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		// No prefix necessary -- it's already set on the local cache.
		return c.local.Reader(ctx, d, offset, limit)
	}
	return c.cacheProxy.RemoteReader(ctx, peer, isolation, d, offset, limit)
}
func (c *Cache) remoteWriter(ctx context.Context, peer, handoffPeer string, isolation *dcpb.Isolation, d *repb.Digest) (io.WriteCloser, error) {
	if c.config.EnableLocalWrites && peer == c.config.ListenAddr {
		// No prefix necessary -- it's already set on the local cache.
		return c.local.Writer(ctx, d)
	}
	return c.cacheProxy.RemoteWriter(ctx, peer, handoffPeer, isolation, d)
}
func (c *Cache) remoteDelete(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) error {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		// No prefix (remote instance name + cache type) necessary -- it's already set on the local cache.
		return c.local.Delete(ctx, d)
	}
	return c.cacheProxy.RemoteDelete(ctx, peer, isolation, d)
}
func (c *Cache) sendFile(ctx context.Context, d *repb.Digest, isolation *dcpb.Isolation, dest string) error {
	if exists, err := c.cacheProxy.RemoteContains(ctx, dest, isolation, d); err == nil && exists {
		return nil
	}

	cacheType, err := cacheproxy.ProtoCacheTypeToCacheType(isolation.GetCacheType())
	if err != nil {
		return err
	}
	localCache, err := c.local.WithIsolation(ctx, cacheType, isolation.GetRemoteInstanceName())
	if err != nil {
		return err
	}
	r, err := localCache.Reader(ctx, d, 0, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.cacheProxy.RemoteWriter(ctx, dest, "", isolation, d)
	if err != nil {
		return err
	}
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Close()
}

func (c *Cache) copyFile(ctx context.Context, d *repb.Digest, source string, isolation *dcpb.Isolation, dest string) error {
	if exists, err := c.remoteContains(ctx, dest, isolation, d); err == nil && exists {
		return nil
	}
	r, err := c.remoteReader(ctx, source, isolation, d, 0, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.remoteWriter(ctx, dest, "", isolation, d)
	if err != nil {
		return err
	}
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Close()
}

type backfillOrder struct {
	d      *repb.Digest
	source string
	dest   string
}

func dedupeBackfills(backfills []*backfillOrder) []*backfillOrder {
	deduped := make([]*backfillOrder, 0, len(backfills))
	seen := make(map[string]struct{}, len(backfills))
	for _, bf := range backfills {
		if _, ok := seen[bf.d.GetHash()]; ok {
			continue
		}
		seen[bf.d.GetHash()] = struct{}{}
		deduped = append(deduped, bf)
	}
	return deduped
}

func (c *Cache) backfillPeers(ctx context.Context, backfills []*backfillOrder) error {
	if len(backfills) == 0 {
		return nil
	}
	backfills = dedupeBackfills(backfills)
	eg, gCtx := errgroup.WithContext(ctx)
	for _, bf := range backfills {
		bf := bf
		eg.Go(func() error {
			return c.copyFile(gCtx, bf.d, bf.source, c.isolation, bf.dest)
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
func (c *Cache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	ps := c.readPeers(d)
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(d, ps)); err != nil {
			c.log.Debugf("Error backfilling peers: %s", err)
		}
	}

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		exists, err := c.remoteContains(ctx, peer, c.isolation, d)
		if err == nil {
			if exists {
				c.log.Debugf("Contains(%q) found on peer %q", d, peer)
				backfill()
				return exists, err
			}
			c.log.Debugf("Contains(%q) not found on peer %q (err: %+v)", d, peer, err)
			continue
		}

		// Got an error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)
	}
	return false, nil
}

func (c *Cache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	ps := c.readPeers(d)
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(d, ps)); err != nil {
			c.log.Debugf("Error backfilling peers: %s", err)
		}
	}

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		md, err := c.remoteMetadata(ctx, peer, c.isolation, d)
		if err == nil {
			c.log.Debugf("Metadata(%q) found on peer %q", d, peer)
			backfill()
			return md, nil
		}
		if status.IsNotFoundError(err) {
			c.log.Debugf("Metadata(%q) not found on peer %s", cacheproxy.IsolationToString(c.isolation)+d.GetHash(), peer)
			continue
		}

		// Got an error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)
	}

	return nil, status.NotFoundErrorf("Exhausted all peers attempting to query metadata %q, peerset: %v", d.GetHash(), ps)
}

func (c *Cache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	mu := sync.RWMutex{} // protects(foundMap)
	hashDigests := make(map[string][]*repb.Digest, 0)
	foundMap := make(map[string]struct{}, len(digests))
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
				c.log.Debugf("Exhausted all peers for %q. Peerset: %+v", h, ps)
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
			c.log.Debugf("FindMissing: digests not found: %+v", stillMissing)
			// If we aren't able to plan any more batch requests, that means
			// we're out of peers and should exit, returning what we have.
			break
		}
		eg, gCtx := errgroup.WithContext(ctx)
		for peer, digests := range peerRequests {
			peer := peer
			digests := digests
			eg.Go(func() error {
				peerRsp, err := c.remoteFindMissing(gCtx, peer, c.isolation, digests)
				peerMissingHashes := make(map[string]struct{})
				for _, d := range peerRsp {
					peerMissingHashes[d.GetHash()] = struct{}{}
				}
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					for _, d := range digests {
						peerMap[d.GetHash()].MarkPeerAsFailed(peer)
					}
					return nil
				}
				for _, d := range digests {
					if _, ok := peerMissingHashes[d.GetHash()]; !ok {
						foundMap[d.GetHash()] = struct{}{}
					}
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			if err != context.Canceled {
				// Don't log context cancelled errors, they are common and expected when
				// clients cancel a request.
				c.log.Debugf("Error checking contains batch; will retry: %s", err)
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
	for h := range foundMap {
		d := hashDigests[h][0]
		ps := peerMap[h]
		backfills = append(backfills, c.getBackfillOrders(d, ps)...)
	}
	if err := c.backfillPeers(ctx, backfills); err != nil {
		c.log.Debugf("Error backfilling peers: %s", err)
	}

	var missing []*repb.Digest
	for _, d := range digests {
		if _, ok := foundMap[d.GetHash()]; !ok {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

// The first reader with a non-empty value will be returned. If all potential
// peers for the digest are exhausted, then return a NotFoundError.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) distributedReader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	ps := c.readPeers(d)
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(d, ps)); err != nil {
			c.log.Debugf("Error backfilling peers: %s", err)
		}
	}

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		r, err := c.remoteReader(ctx, peer, c.isolation, d, offset, limit)
		if err == nil {
			c.log.Debugf("Reader(%q) found on peer %s", cacheproxy.IsolationToString(c.isolation)+d.GetHash(), peer)
			backfill()
			return r, err
		}
		if status.IsNotFoundError(err) {
			c.log.Debugf("Reader(%q) not found on peer %s", cacheproxy.IsolationToString(c.isolation)+d.GetHash(), peer)
			continue
		}

		// Some other error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)

	}
	return nil, status.NotFoundErrorf("Exhausted all peers attempting to read %q, peerset: %v", d.GetHash(), ps)
}

func (c *Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	r, err := c.distributedReader(ctx, d, 0, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
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
				c.log.Debugf("Exhausted all peers for %q. Peerset: %+v", h, ps)
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
				peerRsp, err := c.remoteGetMulti(gCtx, peer, c.isolation, digests)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					c.log.Debugf("GetMulti: peer %q returned err: %s", peer, err)
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
				c.log.Debugf("Error checking contains batch; will retry: %s", err)
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
		c.log.Debugf("Error backfilling peers: %s", err)
	}

	rsp := make(map[*repb.Digest][]byte, len(digests))
	for _, d := range digests {
		if buf, ok := gotMap[d.GetHash()]; ok {
			rsp[d] = buf
		}
	}
	return rsp, nil
}

type multiWriteCloser struct {
	ctx           context.Context
	log           log.Logger
	isolation     *dcpb.Isolation
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
			mc.log.Debugf("Successfully wrote %s to %q", cacheproxy.IsolationToString(mc.isolation)+mc.d.GetHash(), peer)
			return nil
		})
	}
	err := eg.Wait()
	if err == nil {
		peers := make([]string, len(mc.peerClosers))
		for peer := range mc.peerClosers {
			peers = append(peers, peer)
		}
		mc.log.Debugf("Writer(%q) successfully wrote to peers %s", cacheproxy.IsolationToString(mc.isolation)+mc.d.GetHash(), peers)
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
		log:         c.log,
		d:           d,
		peerClosers: make(map[string]io.WriteCloser, 0),
		mu:          &sync.Mutex{},
		listenAddr:  c.config.ListenAddr,
		isolation:   c.isolation,
	}
	for peer, hintedHandoff := ps.GetNextPeerAndHandoff(); peer != ""; peer, hintedHandoff = ps.GetNextPeerAndHandoff() {
		rwc, err := c.remoteWriter(ctx, peer, hintedHandoff, c.isolation, d)
		if err != nil {
			ps.MarkPeerAsFailed(peer)
			log.Infof("Error opening remote writer for %q to peer %q: %s", d.GetHash(), peer, err)
			continue
		}
		mwc.peerClosers[peer] = rwc
	}
	if len(mwc.peerClosers) < c.config.ReplicationFactor {
		openPeers := make([]string, len(mwc.peerClosers))
		for peer := range mwc.peerClosers {
			openPeers = append(openPeers, peer)
		}
		allPeers := append(ps.PreferredPeers, ps.FallbackPeers...)
		log.Infof("Could not open enough remoteWriters for digest %s. All peers: %s, opened: %s (peerset: %+v)", d.GetHash(), allPeers, openPeers, ps)
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
	ps := c.readPeers(d)
	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		err := c.remoteDelete(ctx, peer, c.isolation, d)
		if err != nil {
			if status.IsNotFoundError(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func (c *Cache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	return c.distributedReader(ctx, d, offset, limit)
}

func (c *Cache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	return c.multiWriter(ctx, d)
}
