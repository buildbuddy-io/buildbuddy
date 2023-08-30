package distributed

import (
	"context"
	"flag"
	"fmt"
	"io"
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
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	listenAddr                   = flag.String("cache.distributed_cache.listen_addr", "", "The address to listen for local BuildBuddy distributed cache traffic on.")
	redisTarget                  = flagutil.New("cache.distributed_cache.redis_target", "", "A redis target for improved Caching/RBE performance. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **", flagutil.SecretTag)
	groupName                    = flag.String("cache.distributed_cache.group_name", "", "A unique name for this distributed cache group. ** Enterprise only **")
	nodes                        = flagutil.New("cache.distributed_cache.nodes", []string{}, "The hardcoded list of peer distributed cache nodes. If this is set, redis_target will be ignored. ** Enterprise only **")
	replicationFactor            = flag.Int("cache.distributed_cache.replication_factor", 0, "How many total servers the data should be replicated to. Must be >= 1. ** Enterprise only **")
	clusterSize                  = flag.Int("cache.distributed_cache.cluster_size", 0, "The total number of nodes in this cluster. Required for health checking. ** Enterprise only **")
	enableLocalWrites            = flag.Bool("cache.distributed_cache.enable_local_writes", false, "If enabled, shortcuts distributed writes that belong to the local shard to local cache instead of making an RPC.")
	enableLocalCompressionLookup = flag.Bool("cache.distributed_cache.enable_local_compression_lookup", true, "If enabled, checks the local cache for compression support. If not set, distributed compression defaults to off.")
	extraNodes                   = flagutil.New("cache.distributed_cache.extra_nodes", []string{}, "The hardcoded list of extra nodes to add data too. Useful for migrations. ** Enterprise only **")
)

const (
	// Each hinted handoff is a digest (~64 bytes), prefix, and peer
	// (40 bytes). So keeping around 100000 of these means an extra 10MB
	// per peer.
	maxHintedHandoffsPerPeer = 100000
)

type CacheConfig struct {
	PubSub                       interfaces.PubSub
	ListenAddr                   string
	GroupName                    string
	Nodes                        []string
	ExtraNodes                   []string
	ReplicationFactor            int
	ClusterSize                  int
	RPCHeartbeatInterval         time.Duration
	DisableLocalLookup           bool
	EnableLocalWrites            bool
	EnableLocalCompressionLookup bool
}

type hintedHandoffOrder struct {
	ctx context.Context
	r   *rspb.ResourceName
}

func (o *hintedHandoffOrder) String() string {
	hash := o.r.GetDigest().GetHash()
	isolation := &dcpb.Isolation{
		CacheType:          o.r.GetCacheType(),
		RemoteInstanceName: o.r.GetInstanceName(),
	}
	return fmt.Sprintf("{digest:%q isolation:{%s}}", hash, isolation)
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
	extraConsistentHash  *consistent_hash.ConsistentHash
	heartbeatChannel     *heartbeat.Channel
	heartbeatMu          *sync.Mutex
	shutdownMu           *sync.RWMutex
	shutDownChan         chan struct{}
	finishedShutdown     bool
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
		ListenAddr:                   *listenAddr,
		GroupName:                    *groupName,
		ReplicationFactor:            *replicationFactor,
		Nodes:                        *nodes,
		ExtraNodes:                   *extraNodes,
		ClusterSize:                  *clusterSize,
		EnableLocalWrites:            *enableLocalWrites,
		EnableLocalCompressionLookup: *enableLocalCompressionLookup,
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
//   - myAddr is the interface to listen on in "host:port" format.
//   - groupName is a string namespace which the distributed cache nodes must
//
// match to peer.
//   - replicationFactor is an int specifying how many copies of each key will
//
// be stored across unique caches.
func NewDistributedCache(env environment.Env, c interfaces.Cache, config CacheConfig, hc interfaces.HealthChecker) (*Cache, error) {
	// Check Preconditions: if extraNodes are enabled, node list must have been manually specified.
	if len(config.ExtraNodes) > 0 && len(config.Nodes) == 0 {
		return nil, status.FailedPreconditionError("extra nodes may only be specified when all nodes are hardcoded.")
	}
	chash := consistent_hash.NewConsistentHash()
	extraCHash := consistent_hash.NewConsistentHash()
	if config.RPCHeartbeatInterval == 0 {
		config.RPCHeartbeatInterval = 1 * time.Second
	}
	dc := &Cache{
		local:               c,
		log:                 log.NamedSubLogger(fmt.Sprintf("Coordinator(%s)", config.ListenAddr)),
		config:              config,
		cacheProxy:          cacheproxy.NewCacheProxy(env, c, config.ListenAddr),
		consistentHash:      chash,
		extraConsistentHash: extraCHash,

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

		if len(config.ExtraNodes) > 0 {
			extendedNodeList := make([]string, len(config.Nodes))
			copy(extendedNodeList, config.Nodes)
			extendedNodeList = append(extendedNodeList, config.ExtraNodes...)
			extraCHash.Set(extendedNodeList...)
		}
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
	// If the distributed layer was configured with a hardcoded node list,
	// then it's not necessary to wait for any heartbeats and this cache
	// will report healthy immediately.
	if len(c.config.Nodes) > 0 {
		return nil
	}

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

func (c *Cache) recvHintedHandoffCallback(ctx context.Context, peer string, r *rspb.ResourceName) {
	c.hintedHandoffsMu.Lock()
	defer c.hintedHandoffsMu.Unlock()
	if _, ok := c.hintedHandoffsByPeer[peer]; !ok {
		// If this is the first hinted handoff for this peer we've
		// received, then initialize the channel.
		c.hintedHandoffsByPeer[peer] = make(chan *hintedHandoffOrder, maxHintedHandoffsPerPeer)
	}
	order := &hintedHandoffOrder{
		ctx: ctx,
		r:   r,
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
			err := c.sendFile(ctx, handoffOrder.r, peer)
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

	if !c.finishedShutdown {
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
func (c *Cache) writePeers(d *repb.Digest) *peerset.PeerSet {
	allPeers := c.consistentHash.GetAllReplicas(d.GetHash())
	if len(c.config.ExtraNodes) > 0 {
		allPeers = c.extraConsistentHash.GetAllReplicas(d.GetHash())
	}
	return peerset.New(allPeers[:c.config.ReplicationFactor], allPeers[c.config.ReplicationFactor:])
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0)
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		out = append(out, s)
		seen[s] = struct{}{}
	}
	return out
}

// readPeers returns a slice of peers responsible for this key. If this peer is
// a member of the set, it is returned first. Other
// peers are returned in random order.
func (c *Cache) readPeers(d *repb.Digest) *peerset.PeerSet {
	peers := c.consistentHash.GetAllReplicas(d.GetHash())
	primaryPeers := peers[:c.config.ReplicationFactor]
	secondaryPeers := peers[c.config.ReplicationFactor:]

	if len(c.config.ExtraNodes) > 0 {
		extendedPeerList := c.extraConsistentHash.GetAllReplicas(d.GetHash())
		allPrimaryPeers := extendedPeerList[:c.config.ReplicationFactor]
		allSecondaryPeers := extendedPeerList[c.config.ReplicationFactor:]

		// If extraNodes is set, we want to additionally attempt reads
		// on the nodes where the data ~would~ be if the extra nodes
		// were included in the full peer set.
		//
		// These extra reads allow us to move data to the new nodes
		// and read it immediately, while falling back to the old data
		// location if it's not found and backfilling to the new
		// nodes.
		primaryPeers = dedupe(append(allPrimaryPeers, primaryPeers...))
		secondaryPeers = dedupe(append(allSecondaryPeers, secondaryPeers...))
	}

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

func (c *Cache) remoteContains(ctx context.Context, peer string, r *rspb.ResourceName) (bool, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Contains(ctx, r)
	}
	return c.cacheProxy.RemoteContains(ctx, peer, r)
}

func (c *Cache) remoteMetadata(ctx context.Context, peer string, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Metadata(ctx, r)
	}
	return c.cacheProxy.RemoteMetadata(ctx, peer, r)
}

func (c *Cache) remoteFindMissing(ctx context.Context, peer string, isolation *dcpb.Isolation, rns []*rspb.ResourceName) ([]*repb.Digest, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.FindMissing(ctx, rns)
	}
	return c.cacheProxy.RemoteFindMissing(ctx, peer, isolation, rns)
}

func (c *Cache) remoteGetMulti(ctx context.Context, peer string, isolation *dcpb.Isolation, rns []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.GetMulti(ctx, rns)
	}
	return c.cacheProxy.RemoteGetMulti(ctx, peer, isolation, rns)
}
func (c *Cache) remoteReader(ctx context.Context, peer string, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Reader(ctx, r, offset, limit)
	}
	return c.cacheProxy.RemoteReader(ctx, peer, r, offset, limit)
}
func (c *Cache) remoteWriter(ctx context.Context, peer, handoffPeer string, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	if c.config.EnableLocalWrites && peer == c.config.ListenAddr {
		return c.local.Writer(ctx, r)
	}
	return c.cacheProxy.RemoteWriter(ctx, peer, handoffPeer, r)
}
func (c *Cache) remoteDelete(ctx context.Context, peer string, r *rspb.ResourceName) error {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Delete(ctx, r)
	}
	return c.cacheProxy.RemoteDelete(ctx, peer, r)
}

func (c *Cache) sendFile(ctx context.Context, rn *rspb.ResourceName, dest string) error {
	if exists, err := c.cacheProxy.RemoteContains(ctx, dest, rn); err == nil && exists {
		return nil
	}

	r, err := c.local.Reader(ctx, rn, 0, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.cacheProxy.RemoteWriter(ctx, dest, "", rn)
	if err != nil {
		return err
	}
	defer rwc.Close()
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Commit()
}

func (c *Cache) copyFile(ctx context.Context, rn *rspb.ResourceName, source string, dest string) error {
	if exists, err := c.remoteContains(ctx, dest, rn); err == nil && exists {
		return nil
	}
	r, err := c.remoteReader(ctx, source, rn, 0, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.remoteWriter(ctx, dest, "", rn)
	if err != nil {
		return err
	}
	defer rwc.Close()
	if _, err := io.Copy(rwc, r); err != nil {
		return err
	}
	return rwc.Commit()
}

type backfillOrder struct {
	r      *rspb.ResourceName
	source string
	dest   string
}

func dedupeBackfills(backfills []*backfillOrder) []*backfillOrder {
	deduped := make([]*backfillOrder, 0, len(backfills))
	seen := make(map[string]struct{}, len(backfills))
	for _, bf := range backfills {
		d := bf.r.GetDigest()
		if _, ok := seen[d.GetHash()]; ok {
			continue
		}
		seen[d.GetHash()] = struct{}{}
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
			return c.copyFile(gCtx, bf.r, bf.source, bf.dest)
		})
	}
	return eg.Wait()
}

func (c *Cache) getBackfillOrders(r *rspb.ResourceName, ps *peerset.PeerSet) []*backfillOrder {
	source, targets := ps.GetBackfillTargets()
	if len(targets) == 0 {
		return nil
	}

	orders := make([]*backfillOrder, 0, len(targets))
	for _, target := range targets {
		orders = append(orders, &backfillOrder{
			source: source,
			dest:   target,
			r:      r,
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
func (c *Cache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	ps := c.readPeers(r.GetDigest())
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(r, ps)); err != nil {
			c.log.Debugf("Error backfilling peers: %s", err)
		}
	}

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		exists, err := c.remoteContains(ctx, peer, r)
		if err == nil {
			if exists {
				c.log.Debugf("Contains(%q) found on peer %q", r.GetDigest(), peer)
				backfill()
				return exists, err
			}
			c.log.Debugf("Contains(%q) not found on peer %q (err: %+v)", r.GetDigest(), peer, err)
			continue
		}

		// Got an error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)
	}
	return false, nil
}

func (c *Cache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	d := r.GetDigest()
	ps := c.readPeers(d)

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		md, err := c.remoteMetadata(ctx, peer, r)
		if err == nil {
			c.log.Debugf("Metadata(%q) found on peer %q", d, peer)
			return md, nil
		}
		if status.IsNotFoundError(err) {
			c.log.Debugf("Metadata(%q) not found on peer %s", cacheproxy.ResourceIsolationString(r), peer)
			continue
		}
		c.log.Debugf("Metadata(%q) lookup failed on peer %s: (err: %v)", cacheproxy.ResourceIsolationString(r), peer, err)

		// Got an error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)
	}

	c.log.Debugf("Exhausted all peers attempting to query metadata %q. Peerset: %+v", d.GetHash(), ps)
	return nil, status.NotFoundErrorf("Exhausted all peers attempting to query metadata %q.", d.GetHash())
}

func (c *Cache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	isolation := getIsolation(resources)
	if isolation == nil {
		return nil, nil
	}

	mu := sync.RWMutex{} // protects(foundMap)
	hashResources := make(map[string][]*rspb.ResourceName, 0)
	foundMap := make(map[string]struct{}, len(resources))
	peerMap := make(map[string]*peerset.PeerSet, len(resources))
	for _, r := range resources {
		hash := r.GetDigest().GetHash()
		hashResources[hash] = append(hashResources[hash], r)
		if _, ok := peerMap[hash]; !ok {
			peerMap[hash] = c.readPeers(r.GetDigest())
		}
	}

	for {
		// Each iteration through this outer loop sends a "batch" of requests in
		// parallel, until all digests have been found or we have exhausted all
		// peers.
		peerRequests := make(map[string][]*rspb.ResourceName, 0)
		for h, perHashResources := range hashResources {
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
			peerRequests[peer] = append(peerRequests[peer], perHashResources[0])
		}
		if len(peerRequests) == 0 {
			stillMissing := make([]string, 0)
			for h := range hashResources {
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
		for peer, resources := range peerRequests {
			peer := peer
			resources := resources
			eg.Go(func() error {
				peerRsp, err := c.remoteFindMissing(gCtx, peer, isolation, resources)
				peerMissingHashes := make(map[string]struct{})
				for _, d := range peerRsp {
					peerMissingHashes[d.GetHash()] = struct{}{}
				}
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					for _, r := range resources {
						hash := r.GetDigest().GetHash()
						peerMap[hash].MarkPeerAsFailed(peer)
					}
					return nil
				}
				for _, r := range resources {
					hash := r.GetDigest().GetHash()
					if _, ok := peerMissingHashes[hash]; !ok {
						foundMap[hash] = struct{}{}
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
		if len(foundMap) == len(hashResources) {
			// If we've found everything, we can exit now.
			break
		}
	}

	// For every digest we found, if we did not find it
	// on the first peer in our list, we want to backfill it.
	backfills := make([]*backfillOrder, 0)
	for h := range foundMap {
		r := hashResources[h][0]
		ps := peerMap[h]
		backfills = append(backfills, c.getBackfillOrders(r, ps)...)
	}
	if err := c.backfillPeers(ctx, backfills); err != nil {
		c.log.Debugf("Error backfilling peers: %s", err)
	}

	var missing []*repb.Digest
	for _, r := range resources {
		d := r.GetDigest()
		if _, ok := foundMap[d.GetHash()]; !ok {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

// Returns the isolation from the first resource name, assuming that all resources have the same isolation
func getIsolation(resources []*rspb.ResourceName) *dcpb.Isolation {
	if len(resources) == 0 {
		return nil
	}
	return &dcpb.Isolation{
		CacheType:          resources[0].GetCacheType(),
		RemoteInstanceName: resources[0].GetInstanceName(),
	}
}

// The first reader with a non-empty value will be returned. If all potential
// peers for the digest are exhausted, then return a NotFoundError.
//
// This is like setting READ_CONSISTENCY = ONE.
//
// Values found on a non-primary replica will be backfilled to the primary.
func (c *Cache) distributedReader(ctx context.Context, rn *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	ps := c.readPeers(rn.GetDigest())
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(rn, ps)); err != nil {
			c.log.Debugf("Error backfilling peers: %s", err)
		}
	}

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		r, err := c.remoteReader(ctx, peer, rn, offset, limit)
		if err == nil {
			c.log.Debugf("Reader(%q) found on peer %s", cacheproxy.ResourceIsolationString(rn), peer)
			backfill()
			return r, err
		}
		if status.IsNotFoundError(err) {
			c.log.Debugf("Reader(%q) not found on peer %s", cacheproxy.ResourceIsolationString(rn), peer)
			continue
		}

		// Some other error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)

	}
	c.log.Debugf("Exhausted all peers attempting to query metadata %q. Peerset: %+v", rn.GetDigest().GetHash(), ps)
	return nil, status.NotFoundErrorf("Exhausted all peers attempting to read %q.", rn.GetDigest().GetHash())
}

func (c *Cache) Get(ctx context.Context, rn *rspb.ResourceName) ([]byte, error) {
	r, err := c.distributedReader(ctx, rn, 0, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (c *Cache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	isolation := getIsolation(resources)
	if isolation == nil {
		return nil, nil
	}

	mu := sync.RWMutex{} // protects(gotMap)
	hashResources := make(map[string][]*rspb.ResourceName, 0)
	gotMap := make(map[string][]byte, len(resources))
	peerMap := make(map[string]*peerset.PeerSet, len(resources))
	for _, r := range resources {
		hash := r.GetDigest().GetHash()
		hashResources[hash] = append(hashResources[hash], r)
		if _, ok := peerMap[hash]; !ok {
			peerMap[hash] = c.readPeers(r.GetDigest())
		}
	}

	for {
		// Each iteration through this outer loop sends a "batch" of requests in
		// parallel, until all digests have been found or we have exhausted all
		// peers.
		peerRequests := make(map[string][]*rspb.ResourceName, 0)
		for h, perHashResources := range hashResources {
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
			peerRequests[peer] = append(peerRequests[peer], perHashResources[0])
		}
		if len(peerRequests) == 0 {
			stillMissing := make([]string, 0)
			for h := range hashResources {
				if _, ok := gotMap[h]; !ok {
					stillMissing = append(stillMissing, h)
				}
			}
			// If we aren't able to plan any more batch requests, that means
			// we're out of peers and should exit, returning what we have.
			break
		}
		eg, gCtx := errgroup.WithContext(ctx)
		for peer, resources := range peerRequests {
			peer := peer
			resources := resources
			eg.Go(func() error {
				peerRsp, err := c.remoteGetMulti(gCtx, peer, isolation, resources)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					c.log.Debugf("GetMulti: peer %q returned err: %s", peer, err)
					for _, r := range resources {
						hash := r.GetDigest().GetHash()
						peerMap[hash].MarkPeerAsFailed(peer)
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
		if len(gotMap) == len(hashResources) {
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
			r := hashResources[h][0]
			backfills = append(backfills, c.getBackfillOrders(r, ps)...)
		}
	}
	if err := c.backfillPeers(ctx, backfills); err != nil {
		c.log.Debugf("Error backfilling peers: %s", err)
	}

	rsp := make(map[*repb.Digest][]byte, len(resources))
	for _, r := range resources {
		d := r.GetDigest()
		if buf, ok := gotMap[d.GetHash()]; ok {
			rsp[d] = buf
		}
	}
	return rsp, nil
}

type multiWriteCloser struct {
	ctx           context.Context
	log           log.Logger
	peerClosers   map[string]interfaces.CommittedWriteCloser
	mu            *sync.Mutex
	r             *rspb.ResourceName
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

func (mc *multiWriteCloser) Commit() error {
	eg, _ := errgroup.WithContext(mc.ctx)
	for peer, wc := range mc.peerClosers {
		wc := wc
		peer := peer
		eg.Go(func() error {
			if err := wc.Commit(); err != nil {
				return err
			}
			mc.log.Debugf("Successfully wrote %s to %q", cacheproxy.ResourceIsolationString(mc.r), peer)
			return nil
		})
	}
	err := eg.Wait()
	if err == nil {
		peers := make([]string, len(mc.peerClosers))
		for peer := range mc.peerClosers {
			peers = append(peers, peer)
		}
		mc.log.Debugf("Writer(%q) successfully wrote to peers %s", cacheproxy.ResourceIsolationString(mc.r), peers)
	}
	return err
}

func (mc *multiWriteCloser) Close() error {
	eg, _ := errgroup.WithContext(mc.ctx)
	for peer, wc := range mc.peerClosers {
		wc := wc
		peer := peer
		eg.Go(func() error {
			if err := wc.Close(); err != nil {
				log.Errorf("Error closing peer %q writer: %s", peer, err)
			}
			return nil
		})
	}
	err := eg.Wait()
	return err
}

// Attempt to write digest to N peers (where N == replicationFactor).
// Return an unavailable error if less than a quarum of peers can be
// written to.
//
// This is like setting WRITE_CONSISTENCY = QUORUM.
func (c *Cache) multiWriter(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	ps := c.writePeers(r.GetDigest())
	mwc := &multiWriteCloser{
		ctx:         ctx,
		log:         c.log,
		peerClosers: make(map[string]interfaces.CommittedWriteCloser, 0),
		mu:          &sync.Mutex{},
		listenAddr:  c.config.ListenAddr,
		r:           r,
	}
	for peer, hintedHandoff := ps.GetNextPeerAndHandoff(); peer != ""; peer, hintedHandoff = ps.GetNextPeerAndHandoff() {
		rwc, err := c.remoteWriter(ctx, peer, hintedHandoff, r)
		if err != nil {
			ps.MarkPeerAsFailed(peer)
			log.Infof("Error opening remote writer for %q to peer %q: %s", r.GetDigest().GetHash(), peer, err)
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
		log.Infof("Could not open enough remoteWriters for digest %s. All peers: %s, opened: %s (peerset: %+v)", r.Digest.GetHash(), allPeers, openPeers, ps)
		return nil, status.UnavailableErrorf("Not enough peers (%d) available to satisfy replication factor (%d).", len(mwc.peerClosers), c.config.ReplicationFactor)
	}
	return mwc, nil
}

func (c *Cache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	wc, err := c.multiWriter(ctx, r)
	if err != nil {
		return err
	}
	defer wc.Close()
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Commit()
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
	ps := c.readPeers(r.GetDigest())
	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		err := c.remoteDelete(ctx, peer, r)
		if err != nil {
			if status.IsNotFoundError(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func (c *Cache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	return c.distributedReader(ctx, r, uncompressedOffset, limit)
}

func (c *Cache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	mwc, err := c.multiWriter(ctx, r)
	if err != nil {
		return nil, err
	}
	return mwc, nil
}

// SupportsCompressor Distributed compression should only be enabled if all peers support compression
//
// To safely roll out compression to distributed caches:
//  1. Enable compression on the underlying cache type. As this is getting rolled out, this distributed::SupportsCompressor
//     function should continue to return false, and compression will be disabled
//  2. Once compression is rolled out to all underlying caches, set EnableLocalCompressionLookup=true. All the
//     distributed underlying caches should have compression enabled, so it is safe to only check the local cache
//     for compresion support
func (c *Cache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	if c.config.EnableLocalCompressionLookup {
		return c.local.SupportsCompressor(compressor)
	}
	return false
}

func (c *Cache) SupportsEncryption(ctx context.Context) bool {
	return c.local.SupportsEncryption(ctx)
}
