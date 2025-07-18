package distributed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/distributed_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/heartbeat"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/peerset"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	listenAddr                   = flag.String("cache.distributed_cache.listen_addr", "", "The address to listen for local BuildBuddy distributed cache traffic on.")
	redisTarget                  = flag.String("cache.distributed_cache.redis_target", "", "Redis target for used for discovering distributed cache replicas. Target can be provided as either a redis connection URI or a host:port pair. URI schemas supported: redis[s]://[[USER][:PASSWORD]@][HOST][:PORT][/DATABASE] or unix://[[USER][:PASSWORD]@]SOCKET_PATH[?db=DATABASE] ** Enterprise only **", flag.Secret)
	groupName                    = flag.String("cache.distributed_cache.group_name", "", "A unique name for this distributed cache group. ** Enterprise only **")
	nodes                        = flag.Slice("cache.distributed_cache.nodes", []string{}, "The hardcoded list of peer distributed cache nodes. If this is set, redis_target will be ignored. ** Enterprise only **")
	consistentHashFunction       = flag.String("cache.distributed_cache.consistent_hash_function", "CRC32", "A consistent hash function to use when hashing data. CRC32 or SHA256")
	consistentHashVNodes         = flag.Int("cache.distributed_cache.consistent_hash_vnodes", 100, "The number of copies (virtual nodes) of each peer on the consistent hash ring")
	replicationFactor            = flag.Int("cache.distributed_cache.replication_factor", 1, "How many total servers the data should be replicated to. Must be >= 1. ** Enterprise only **")
	clusterSize                  = flag.Int("cache.distributed_cache.cluster_size", 0, "The total number of nodes in this cluster. Required for health checking. ** Enterprise only **")
	enableLocalWrites            = flag.Bool("cache.distributed_cache.enable_local_writes", false, "If enabled, shortcuts distributed writes that belong to the local shard to local cache instead of making an RPC.")
	readThroughLocalCache        = flag.Bool("cache.distributed_cache.read_through_local_cache", false, "If enabled, all data read will be written to the local cache node if not already present")
	enableBackfill               = flag.Bool("cache.distributed_cache.enable_backfill", true, "If enabled, digests written to avoid unavailable nodes will be backfilled when those nodes return")
	enableLocalCompressionLookup = flag.Bool("cache.distributed_cache.enable_local_compression_lookup", true, "If enabled, checks the local cache for compression support. If not set, distributed compression defaults to off.")
	newNodes                     = flag.Slice("cache.distributed_cache.new_nodes", []string{}, "The new nodeset to add data too. Useful for migrations. ** Enterprise only **")
	newConsistentHashFunction    = flag.String("cache.distributed_cache.new_consistent_hash_function", "CRC32", "A consistent hash function to use when hashing data. CRC32 or SHA256")
	newConsistentHashVNodes      = flag.Int("cache.distributed_cache.new_consistent_hash_vnodes", 100, "The number of copies of each peer on the new consistent hash ring")
	newNodesReadOnly             = flag.Bool("cache.distributed_cache.new_nodes_read_only", false, "If true, only attempt to read from the newNodes set; do not write to them yet")

	lookasideCacheSizeBytes  = flag.Int64("cache.distributed_cache.lookaside_cache_size_bytes", 0, "If > 0 ; lookaside cache will be enabled")
	lookasideCacheTTL        = flag.Duration("cache.distributed_cache.lookaside_cache_ttl", 1*time.Minute, "The maximum TTL of items served from the lookaside cache. When this flag is set to a duration >0, items will only be served from the lookaside cache if they were added less than this long ago. If it is set to a duration <=0, no TTL check will occur before serving items from the lookaside cache. This value should be << atime_update_threshold when used in the authoritative cache.")
	maxLookasideEntryBytes   = flag.Int64("cache.distributed_cache.max_lookaside_entry_bytes", 10_000, "The biggest allowed entry size in the lookaside cache.")
	maxHintedHandoffsPerPeer = flag.Int64("cache.distributed_cache.max_hinted_handoffs_per_peer", 100_000, "The maximum number of hinted handoffs to keep in memory. Each hinted handoff is a digest (~64 bytes), prefix, and peer (40 bytes). So keeping around 100000 of these means an extra 10MB per peer.")
)

type CacheConfig struct {
	PubSub                       interfaces.PubSub
	ListenAddr                   string
	GroupName                    string
	Nodes                        []string
	NewNodes                     []string
	ReplicationFactor            int
	ClusterSize                  int
	RPCHeartbeatInterval         time.Duration
	LookasideCacheSizeBytes      int64
	DisableLocalLookup           bool
	EnableLocalWrites            bool
	EnableLocalCompressionLookup bool
	ReadThroughLocalCache        bool
}

type hintedHandoffOrder struct {
	ctx context.Context
	r   *rspb.ResourceName
}

type lookasideCacheEntry struct {
	createdAtMillis int64
	data            []byte
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
	authenticator        interfaces.Authenticator
	local                interfaces.Cache
	log                  log.Logger
	lookasideMu          *sync.Mutex
	lookaside            interfaces.LRU[lookasideCacheEntry]
	peerMetadata         map[string]*peerInfo
	hintedHandoffsMu     *sync.RWMutex
	hintedHandoffsByPeer map[string]chan *hintedHandoffOrder
	distributedProxy     *distributed_client.Proxy
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

func Register(env *real_environment.RealEnv) error {
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
		NewNodes:                     *newNodes,
		ClusterSize:                  *clusterSize,
		EnableLocalWrites:            *enableLocalWrites,
		EnableLocalCompressionLookup: *enableLocalCompressionLookup,
		LookasideCacheSizeBytes:      *lookasideCacheSizeBytes,
		ReadThroughLocalCache:        *readThroughLocalCache,
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

func parseConsistentHash(c string) (consistent_hash.HashFunction, error) {
	switch c {
	case "CRC32":
		return consistent_hash.CRC32, nil
	case "SHA256":
		return consistent_hash.SHA256, nil
	default:
		return nil, status.InvalidArgumentErrorf("Unknown hash function: %s", c)
	}
}

// Converts an LRU eviction reason into a metrics.LookasideCacheEvictionReason.
func convertEvictionReason(r lru.EvictionReason) string {
	switch r {
	case lru.SizeEviction:
		return "size"
	case lru.ManualEviction:
		return "age"
	default:
		return string(r)
	}
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
	// Check Preconditions: if newNodes are enabled, node list must have been manually specified.
	if len(config.NewNodes) > 0 && len(config.Nodes) == 0 {
		return nil, status.FailedPreconditionError("new nodes may only be specified when all nodes are hardcoded.")
	}
	hashFn, err := parseConsistentHash(*consistentHashFunction)
	if err != nil {
		return nil, err
	}
	chash := consistent_hash.NewConsistentHash(hashFn, *consistentHashVNodes)

	newHashFn, err := parseConsistentHash(*newConsistentHashFunction)
	if err != nil {
		return nil, err
	}
	extraCHash := consistent_hash.NewConsistentHash(newHashFn, *newConsistentHashVNodes)
	if config.RPCHeartbeatInterval == 0 {
		config.RPCHeartbeatInterval = 1 * time.Second
	}
	dc := &Cache{
		authenticator:       env.GetAuthenticator(),
		local:               c,
		lookasideMu:         &sync.Mutex{},
		log:                 log.NamedSubLogger(fmt.Sprintf("Coordinator(%s)", config.ListenAddr)),
		config:              config,
		distributedProxy:    distributed_client.New(env, c, config.ListenAddr),
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

	if config.LookasideCacheSizeBytes > 0 {
		l, err := lru.NewLRU[lookasideCacheEntry](&lru.Config[lookasideCacheEntry]{
			MaxSize: config.LookasideCacheSizeBytes,
			OnEvict: func(key string, v lookasideCacheEntry, reason lru.EvictionReason) {
				age := time.Since(time.UnixMilli(v.createdAtMillis))
				metrics.LookasideCacheEvictionAgeMsec.WithLabelValues(
					convertEvictionReason(reason),
				).Observe(float64(age.Milliseconds()))
			},
			SizeFn: func(v lookasideCacheEntry) int64 {
				// []byte size + 8 bytes for the int64 timestamp.
				return int64(len(v.data) + 8)
			},
		})
		if err != nil {
			return nil, err
		}
		dc.lookaside = l

		lookasideCacheTTLString := "INF"
		if *lookasideCacheTTL > 0 {
			lookasideCacheTTLString = lookasideCacheTTL.String()
		}
		log.Printf("Initialized lookaside cache (Size %d, ttl=%s)", config.LookasideCacheSizeBytes, lookasideCacheTTLString)
	}

	if zone := resources.GetZone(); zone != "" {
		dc.zone = zone
	}
	dc.distributedProxy.SetHeartbeatCallbackFunc(dc.recvHeartbeatCallback)
	dc.distributedProxy.SetHintedHandoffCallbackFunc(dc.recvHintedHandoffCallback)
	if len(config.Nodes) > 0 {
		// Nodes are hardcoded. Set them once and be done with it.
		chash.Set(config.Nodes...)

		if len(config.NewNodes) > 0 {
			extraCHash.Set(config.NewNodes...)
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
		if len(c.config.Nodes) < c.config.ReplicationFactor {
			return status.UnavailableErrorf("Not enough nodes configured %d to meet replication factor %d.", len(c.config.Nodes), c.config.ReplicationFactor)
		}
		return nil
	}

	// First check that the number of nodes in our chash
	// matches the cluster size. If not, we can return early.
	nodesAvailable := len(c.consistentHash.GetItems())
	if nodesAvailable < c.config.ClusterSize {
		return status.UnavailableErrorf("%d nodes available but cluster size is %d.", nodesAvailable, c.config.ClusterSize)
	}
	if nodesAvailable < c.config.ReplicationFactor {
		return status.UnavailableErrorf("Not enough nodes available %d to meet replication factor %d.", nodesAvailable, c.config.ReplicationFactor)
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

type teeReadCloser struct {
	rc          io.ReadCloser
	cwc         interfaces.CommittedWriteCloser
	lastReadErr error
	failedWrite bool
}

func (t *teeReadCloser) Read(p []byte) (int, error) {
	var read int
	read, t.lastReadErr = t.rc.Read(p)
	if read > 0 {
		written, err := t.cwc.Write(p[:read])
		if err != nil {
			t.failedWrite = true
			return written, err
		}
		if written < read {
			t.failedWrite = true
			return written, io.ErrShortWrite
		}
	}
	return read, t.lastReadErr
}

func (t *teeReadCloser) Close() error {
	err := t.rc.Close()
	if err == nil && t.lastReadErr != nil && t.lastReadErr != io.EOF {
		log.Warningf("teeReadCloser Close succeeded but Read failed with: %s", t.lastReadErr)
	}
	if err == nil && t.lastReadErr == io.EOF && !t.failedWrite {
		if err := t.cwc.Commit(); err != nil {
			log.Infof("Error committing write to local cache: %s", err)
		}
		if err := t.cwc.Close(); err != nil {
			log.Infof("Error closing local cache writer: %s", err)
		}
	}
	return err
}

func isTreeCacheResource(r *rspb.ResourceName) bool {
	return r.GetCacheType() == rspb.CacheType_AC && strings.HasPrefix(r.GetInstanceName(), content_addressable_storage_server.TreeCacheRemoteInstanceName)
}

// lookasideKey returns the resource's key in the lookaside cache and true,
// or "" and false if the resource shouldn't be stored in the lookaside cache.
func (c *Cache) lookasideKey(ctx context.Context, r *rspb.ResourceName) (key string, ok bool) {
	// Don't store contents for encrypted users/groups in the lookaside cache
	// to avoid cache inconsistencies between the group's cache and the
	// lookaside cache.
	// TODO(go/b/5175): treat non-default-partition contents this way too.
	if authutil.EncryptionEnabled(ctx, c.authenticator) {
		return "", false
	}

	if isTreeCacheResource(r) {
		// These are OK to put in the lookaside cache because even
		// though they are technically AC entries, they are based on CAS
		// content that does not change.
		if rn, err := digest.ACResourceNameFromProto(r); err == nil {
			return rn.ActionCacheString(), true
		}
	} else if r.GetCacheType() == rspb.CacheType_CAS {
		if rn, err := digest.CASResourceNameFromProto(r); err == nil {
			return rn.DownloadString(), true
		}
	}
	return "", false
}

func (c *Cache) addLookasideEntry(ctx context.Context, r *rspb.ResourceName, data []byte) {
	if !c.lookasideCacheEnabled() {
		return
	}
	if r.GetDigest().GetSizeBytes() > *maxLookasideEntryBytes {
		return
	}
	if len(data) == 0 {
		c.log.Infof("Attempted to set zero-length lookaside entry. Key %q", r)
		return
	}
	k, ok := c.lookasideKey(ctx, r)
	if !ok {
		c.log.Debugf("Not setting lookaside entry for resource: %s", r)
		return
	}
	c.setLookasideEntry(k, data)
}

func (c *Cache) setLookasideEntry(lookasideKey string, data []byte) {
	entry := lookasideCacheEntry{
		createdAtMillis: time.Now().UnixMilli(),
		data:            data,
	}

	c.lookasideMu.Lock()
	if !c.lookaside.Contains(lookasideKey) {
		c.lookaside.Add(lookasideKey, entry)
	}
	c.lookasideMu.Unlock()
	c.log.Debugf("Set %q in lookaside cache", lookasideKey)
}

var lookasideCacheLookupCount map[bool]prometheus.Counter

func init() {
	// Calling LookasideCacheLookupCount.With is a large portion of the time
	// spent on the lookaside cache, so just do it once.
	lookasideCacheLookupCount = make(map[bool]prometheus.Counter, 2)
	lookasideCacheLookupCount[true] = metrics.LookasideCacheLookupCount.With(prometheus.Labels{
		metrics.LookasideCacheLookupStatus: metrics.HitStatusLabel,
	})
	lookasideCacheLookupCount[false] = metrics.LookasideCacheLookupCount.With(prometheus.Labels{
		metrics.LookasideCacheLookupStatus: metrics.MissStatusLabel,
	})
}

// getLookasideEntry returns the resource and if it was found in the lookaside
// cache.
func (c *Cache) getLookasideEntry(ctx context.Context, r *rspb.ResourceName) ([]byte, bool) {
	if !c.lookasideCacheEnabled() {
		return nil, false
	}
	k, ok := c.lookasideKey(ctx, r)
	if !ok {
		c.log.Debugf("Not getting lookaside entry for resource: %s", r)
		return nil, false
	}

	c.lookasideMu.Lock()
	found := false
	entry, ok := c.lookaside.Get(k)
	if ok {
		if *lookasideCacheTTL > 0 && time.Since(time.UnixMilli(entry.createdAtMillis)) > *lookasideCacheTTL {
			// Remove the item from the LRU if it's expired.
			c.lookaside.Remove(k)
		} else {
			found = true
		}
	}
	c.lookasideMu.Unlock()

	lookasideCacheLookupCount[found].Inc()
	if found {
		c.log.Debugf("Got %q from lookaside cache", k)
		return entry.data, true
	}
	return nil, false
}

func (c *Cache) lookasideWriter(r *rspb.ResourceName, lookasideKey string) (interfaces.CommittedWriteCloser, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, r.GetDigest().GetSizeBytes()))
	wc := ioutil.NewCustomCommitWriteCloser(buffer)
	wc.CommitFn = func(int64) error {
		c.setLookasideEntry(lookasideKey, buffer.Bytes())
		return nil
	}
	return wc, nil
}

func (c *Cache) lookasideCacheEnabled() bool {
	return c.config.LookasideCacheSizeBytes > 0
}

func (c *Cache) localReadthroughEnabled() bool {
	return c.config.ReadThroughLocalCache
}

func combineCommittedWriteClosers(a, b interfaces.CommittedWriteCloser) interfaces.CommittedWriteCloser {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	c := io.MultiWriter(a, b)
	cwc := ioutil.NewCustomCommitWriteCloser(c)
	cwc.CommitFn = func(n int64) error {
		if err := a.Commit(); err != nil {
			return err
		}
		return b.Commit()
	}
	cwc.CloseFn = func() error {
		var firstErr error
		if err := a.Close(); err != nil {
			firstErr = err
		}
		if err := b.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return cwc
}

func (c *Cache) recvHeartbeatCallback(ctx context.Context, peer string) {
	pi := &peerInfo{
		lastContact: time.Now(),
	}
	if zoneVals := metadata.ValueFromIncomingContext(ctx, resources.ZoneHeader); len(zoneVals) == 1 {
		pi.zone = zoneVals[0]
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
		c.hintedHandoffsByPeer[peer] = make(chan *hintedHandoffOrder, *maxHintedHandoffsPerPeer)
	}
	order := &hintedHandoffOrder{
		ctx: ctx,
		r:   r,
	}
	select {
	case c.hintedHandoffsByPeer[peer] <- order:
		c.log.CtxDebugf(ctx, "Wrote order %+v to %q's hinted handoff channel", order, peer)
		// write was sucessful
	default:
		c.log.CtxWarningf(ctx, "Buffer full: unable to store hinted handoff for %q", peer)
	}
}

func (c *Cache) handleHintedHandoffs(peer string) {
	c.hintedHandoffsMu.RLock()
	handoffs := c.hintedHandoffsByPeer[peer]
	c.hintedHandoffsMu.RUnlock()
	for {
		select {
		case handoffOrder := <-handoffs:
			err := c.sendFile(handoffOrder.ctx, handoffOrder.r, peer)
			if err != nil {
				c.log.CtxWarningf(handoffOrder.ctx, "unable to complete hinted handoff to peer: %q: %s (order %s)", peer, err, handoffOrder)
				return
			}
			c.log.CtxDebugf(handoffOrder.ctx, "completed hinted handoff to peer: %q", peer)
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
				if err := c.distributedProxy.SendHeartbeat(ctx, peer); err == nil {
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
	c.shutDownChan = make(chan struct{})
	go c.heartbeatPeers(c.shutDownChan)
	go func() {
		log.Infof("Distributed cache listening on %q", c.config.ListenAddr)
		if c.heartbeatChannel != nil {
			c.heartbeatChannel.StartAdvertising()
		}
		if err := c.distributedProxy.StartListening(); err != nil {
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
	return c.distributedProxy.Shutdown(ctx)
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
	if len(c.config.NewNodes) > 0 && !*newNodesReadOnly {
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
	var primaryPeers, secondaryPeers []string
	// To prevent a panic if replication is misconfigured to be higher than peer count.
	if len(peers) >= c.config.ReplicationFactor {
		primaryPeers = peers[:c.config.ReplicationFactor]
		secondaryPeers = peers[c.config.ReplicationFactor:]
	}

	if len(c.config.NewNodes) > 0 {
		extendedPeerList := c.extraConsistentHash.GetAllReplicas(d.GetHash())
		// To prevent a panic if replication is misconfigured to be higher than extended peer count.
		if len(extendedPeerList) >= c.config.ReplicationFactor {
			newPrimaryPeers := extendedPeerList[:c.config.ReplicationFactor]
			newSecondaryPeers := extendedPeerList[c.config.ReplicationFactor:]

			// If newNodes is set, we want to first attempt reads on
			// the nodes where the data ~would~ be if the new nodes
			// were the primary peer set, but also read from the old
			// set of peers.
			//
			// These extra reads allow us to move data to the new
			// nodes and read it immediately, while falling back to
			// the old data location if it's not found.
			primaryPeers = dedupe(append(newPrimaryPeers, primaryPeers...))
			secondaryPeers = dedupe(append(newSecondaryPeers, secondaryPeers...))
		}
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
	return c.distributedProxy.RemoteContains(ctx, peer, r)
}

func (c *Cache) remoteMetadata(ctx context.Context, peer string, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Metadata(ctx, r)
	}
	return c.distributedProxy.RemoteMetadata(ctx, peer, r)
}

func (c *Cache) remoteFindMissing(ctx context.Context, peer string, isolation *dcpb.Isolation, rns []*rspb.ResourceName) ([]*repb.Digest, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.FindMissing(ctx, rns)
	}

	stillMissing := make([]*rspb.ResourceName, 0, len(rns))
	for _, r := range rns {
		if _, found := c.getLookasideEntry(ctx, r); found {
			continue
		} else {
			stillMissing = append(stillMissing, r)
		}
	}
	if len(stillMissing) == 0 {
		return nil, nil
	}
	return c.distributedProxy.RemoteFindMissing(ctx, peer, isolation, stillMissing)
}

func (c *Cache) remoteGetMulti(ctx context.Context, peer string, isolation *dcpb.Isolation, rns []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.GetMulti(ctx, rns)
	}
	results := make(map[*repb.Digest][]byte)
	stillMissing := make([]*rspb.ResourceName, 0, len(rns))

	for _, r := range rns {
		if buf, found := c.getLookasideEntry(ctx, r); found {
			results[r.GetDigest()] = buf
		} else {
			stillMissing = append(stillMissing, r)
		}
	}
	if len(stillMissing) == 0 {
		return results, nil
	}

	remoteResults, err := c.distributedProxy.RemoteGetMulti(ctx, peer, isolation, stillMissing)
	if err != nil {
		return nil, err
	}

	for _, r := range stillMissing {
		buf, ok := remoteResults[r.GetDigest()]
		if ok {
			c.addLookasideEntry(ctx, r, buf)
		}
	}
	if len(results) == 0 {
		// If there weren't any local results, we can just return the remote
		// ones, instead instead of copying into an empty map.
		return remoteResults, nil
	}
	for k, v := range remoteResults {
		results[k] = v
	}
	return results, nil
}

func (c *Cache) remoteReader(ctx context.Context, peer string, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Reader(ctx, r, offset, limit)
	}
	cacheable := offset == 0 && limit == 0
	var lookasideWriter, localWriter interfaces.CommittedWriteCloser

	// Check if the blob is in the lookaside cache, and if it's eligible,
	// configure the lookaside writer so the blob can be cached as it is
	// read.
	if cacheable {
		if buf, found := c.getLookasideEntry(ctx, r); found {
			return io.NopCloser(bytes.NewReader(buf)), nil
		}
		if c.lookasideCacheEnabled() && r.GetDigest().GetSizeBytes() <= *maxLookasideEntryBytes {
			if k, ok := c.lookasideKey(ctx, r); ok {
				if lwc, err := c.lookasideWriter(r, k); err == nil {
					lookasideWriter = lwc
				}
			}
		}
	}

	var readCloser io.ReadCloser

	// Check if the blob is in the local read-through cache, if that feature
	// is enabled. If not, configure localWriter so the blob can be written
	// to the local cache as it is read.
	if c.localReadthroughEnabled() {
		if rc, err := c.local.Reader(ctx, r, offset, limit); err == nil {
			c.log.CtxDebugf(ctx, "Reader(%q) found locally", distributed_client.ResourceIsolationString(r))
			readCloser = rc
		} else if r.GetCacheType() == rspb.CacheType_CAS || isTreeCacheResource(r) {
			// AC entries are can be updated, so we don't want to hold on to an
			// old version.
			if local, err := c.local.Writer(ctx, r); err == nil {
				localWriter = local
			}
		}
	}

	// The blob was not found in the lookaside cache or local read-through
	// cache, so look for it on another node in the distributed hash set.
	if readCloser == nil {
		// An error here indicates the digest is not readable, so just
		// return it to the caller.
		rc, err := c.distributedProxy.RemoteReader(ctx, peer, r, offset, limit)
		if err != nil {
			return nil, err
		}
		readCloser = rc
		c.log.CtxDebugf(ctx, "Reader(%q) found on peer %s", distributed_client.ResourceIsolationString(r), peer)
	}

	// If the object is cacheable and lookasideWriter or localWriter are
	// configured, return a teeReadCloser that will write the reads to the
	// configured writers.
	if cacheable && (lookasideWriter != nil || localWriter != nil) {
		cwc := combineCommittedWriteClosers(lookasideWriter, localWriter)
		return &teeReadCloser{rc: readCloser, cwc: cwc}, nil
	}

	return readCloser, nil
}

func (c *Cache) remoteWriter(ctx context.Context, peer, handoffPeer string, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	if c.config.EnableLocalWrites && peer == c.config.ListenAddr {
		return c.local.Writer(ctx, r)
	}
	return c.distributedProxy.RemoteWriter(ctx, peer, handoffPeer, r)
}

func (c *Cache) remoteDelete(ctx context.Context, peer string, r *rspb.ResourceName) error {
	if !c.config.DisableLocalLookup && peer == c.config.ListenAddr {
		return c.local.Delete(ctx, r)
	}
	return c.distributedProxy.RemoteDelete(ctx, peer, r)
}

func (c *Cache) sendFile(ctx context.Context, rn *rspb.ResourceName, dest string) error {
	ctx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
	defer cancel()
	if exists, err := c.distributedProxy.RemoteContains(ctx, dest, rn); err == nil && exists {
		return nil
	}

	r, err := c.local.Reader(ctx, rn, 0, 0)
	if err != nil {
		return err
	}
	defer r.Close()
	rwc, err := c.distributedProxy.RemoteWriter(ctx, dest, "", rn)
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

func (c *Cache) backfillPeers(ctx context.Context, backfills []*backfillOrder) (err error) {
	if len(backfills) == 0 {
		return nil
	}
	start := time.Now()
	defer func() {
		c.log.CtxDebugf(ctx, "backfill took %s err %v", time.Since(start), err)
	}()
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
	if !*enableBackfill {
		return nil
	}
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
	if _, found := c.getLookasideEntry(ctx, r); found {
		return true, nil
	}
	ps := c.readPeers(r.GetDigest())
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(r, ps)); err != nil {
			c.log.CtxDebugf(ctx, "Error backfilling peers: %s", err)
		}
	}

	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		exists, err := c.remoteContains(ctx, peer, r)
		if err == nil {
			if exists {
				c.log.CtxDebugf(ctx, "Contains(%q) found on peer %q", r.GetDigest(), peer)
				backfill()
				return exists, err
			}
			c.log.CtxDebugf(ctx, "Contains(%q) not found on peer %q (err: %+v)", r.GetDigest(), peer, err)
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
			c.log.CtxDebugf(ctx, "Metadata(%q) found on peer %q", d, peer)
			return md, nil
		}
		if status.IsNotFoundError(err) {
			c.log.CtxDebugf(ctx, "Metadata(%q) not found on peer %s", distributed_client.ResourceIsolationString(r), peer)
			continue
		}
		c.log.CtxDebugf(ctx, "Metadata(%q) lookup failed on peer %s: (err: %v)", distributed_client.ResourceIsolationString(r), peer, err)

		// Got an error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)
	}

	c.log.CtxDebugf(ctx, "Exhausted all peers attempting to query metadata %q. Peerset: %+v", d.GetHash(), ps)
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

	hitMetric := metrics.DistributedCachePeerLookups.WithLabelValues(
		"FindMissing",
		metrics.HitStatusLabel,
	)
	lookups := 0
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
				c.log.CtxDebugf(ctx, "Exhausted all peers for %q. Peerset: %+v", h, ps)
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
			c.log.CtxDebugf(ctx, "FindMissing: digests not found: %+v", stillMissing)
			// If we aren't able to plan any more batch requests, that means
			// we're out of peers and should exit, returning what we have.
			break
		}
		lookups++
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
						hitMetric.Observe(float64(lookups))
					}
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			if err != context.Canceled {
				// Don't log context cancelled errors, they are common and expected when
				// clients cancel a request.
				c.log.CtxDebugf(ctx, "Error checking contains batch; will retry: %s", err)
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
		c.log.CtxDebugf(ctx, "Error backfilling peers: %s", err)
	}

	var missing []*repb.Digest
	for _, r := range resources {
		d := r.GetDigest()
		if _, ok := foundMap[d.GetHash()]; !ok {
			missing = append(missing, d)
		}
	}

	// For every resource we didn't find, record an observation indicating how
	// many lookups we did.
	if len(missing) > 0 {
		missMetric := metrics.DistributedCachePeerLookups.WithLabelValues(
			"FindMissing",
			metrics.MissStatusLabel,
		)
		for range len(missing) {
			missMetric.Observe(float64(lookups))
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
func (c *Cache) distributedReader(ctx context.Context, rn *rspb.ResourceName, offset, limit int64, metricsLabel string) (io.ReadCloser, error) {
	ps := c.readPeers(rn.GetDigest())
	backfill := func() {
		if err := c.backfillPeers(ctx, c.getBackfillOrders(rn, ps)); err != nil {
			c.log.CtxDebugf(ctx, "Error backfilling peers: %s", err)
		}
	}

	lookups := 0
	for peer := ps.GetNextPeer(); peer != ""; peer = ps.GetNextPeer() {
		lookups++
		r, err := c.remoteReader(ctx, peer, rn, offset, limit)
		if err == nil {
			backfill()
			metrics.DistributedCachePeerLookups.WithLabelValues(
				metricsLabel,
				metrics.HitStatusLabel,
			).Observe(float64(lookups))
			return r, err
		}
		if status.IsNotFoundError(err) {
			c.log.CtxDebugf(ctx, "Reader(%q) not found on peer %s", distributed_client.ResourceIsolationString(rn), peer)
			continue
		}
		c.log.CtxDebugf(ctx, "Reader(%q) error on peer %s: %s", distributed_client.ResourceIsolationString(rn), peer, err)

		// Some other error -- mark this peer as failed and try the next one.
		ps.MarkPeerAsFailed(peer)

	}
	metrics.DistributedCachePeerLookups.WithLabelValues(
		metricsLabel,
		metrics.MissStatusLabel,
	).Observe(float64(lookups))
	c.log.CtxDebugf(ctx, "Exhausted all peers attempting to read %q. Peerset: %+v", rn.GetDigest().GetHash(), ps)
	return nil, status.NotFoundErrorf("Exhausted all peers attempting to read %q.", rn.GetDigest().GetHash())
}

// Below, in Get(), this value is the max initial allocatable buffer size.
// Set it somewhat conservatively so that we're not DOSed by someone crafting
// remote_instance_names that match this just to use memory.
const maxInitialByteBufferSize = (1024 * 1024 * 4)

func (c *Cache) Get(ctx context.Context, rn *rspb.ResourceName) ([]byte, error) {
	r, err := c.distributedReader(ctx, rn, 0, 0, "Get" /*=metricsLabel*/)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf *bytes.Buffer
	if rn.GetCacheType() == rspb.CacheType_CAS {
		// If this is a CAS object, size the buffer to fit exactly.
		buf = bytes.NewBuffer(make([]byte, 0, int(rn.GetDigest().GetSizeBytes())))
	} else if strings.HasPrefix(rn.GetInstanceName(), content_addressable_storage_server.TreeCacheRemoteInstanceName) {
		// If this is a TreeCache entry that we wrote; pull the size
		// from the remote instance name.
		parts := strings.Split(rn.GetInstanceName(), "/")
		if s, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
			buf = bytes.NewBuffer(make([]byte, 0, min(s, maxInitialByteBufferSize)))
		} else {
			buf = new(bytes.Buffer)
		}
	} else {
		buf = new(bytes.Buffer)
	}
	_, err = io.Copy(buf, r)
	return buf.Bytes(), err
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
				c.log.CtxDebugf(ctx, "Exhausted all peers for %q. Peerset: %+v", h, ps)
				continue
			}
			peerRequests[peer] = append(peerRequests[peer], perHashResources[0])
		}
		if len(peerRequests) == 0 {
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
					c.log.CtxDebugf(ctx, "GetMulti: peer %q returned err: %s", peer, err)
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
				c.log.CtxDebugf(ctx, "Error checking contains batch; will retry: %s", err)
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
		c.log.CtxDebugf(ctx, "Error backfilling peers: %s", err)
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
			mc.log.CtxDebugf(mc.ctx, "Successfully wrote %s to %q", distributed_client.ResourceIsolationString(mc.r), peer)
			return nil
		})
	}
	err := eg.Wait()
	if err == nil {
		peers := make([]string, len(mc.peerClosers))
		for peer := range mc.peerClosers {
			peers = append(peers, peer)
		}
		mc.log.CtxDebugf(mc.ctx, "Writer(%q) successfully wrote to peers %s", distributed_client.ResourceIsolationString(mc.r), peers)
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
				mc.log.CtxErrorf(mc.ctx, "Error closing peer %q writer: %s", peer, err)
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
	// Don't bother doing any work if the context is already done.
	if err := ctx.Err(); err != nil {
		c.log.CtxDebugf(ctx, "multiWriter called when context was already done: %s", err)
		return nil, err
	}

	ps := c.writePeers(r.GetDigest())
	mwc := &multiWriteCloser{
		ctx:         ctx,
		log:         c.log,
		peerClosers: make(map[string]interfaces.CommittedWriteCloser, c.config.ReplicationFactor),
		mu:          &sync.Mutex{},
		listenAddr:  c.config.ListenAddr,
		r:           r,
	}
	for peer, hintedHandoff := ps.GetNextPeerAndHandoff(); peer != ""; peer, hintedHandoff = ps.GetNextPeerAndHandoff() {
		start := time.Now()
		rwc, err := c.remoteWriter(ctx, peer, hintedHandoff, r)
		if err != nil {
			ps.MarkPeerAsFailed(peer)
			c.log.CtxDebugf(ctx, "Error opening remote writer for %q to peer %q after %s: %s", r.GetDigest().GetHash(), peer, time.Since(start), err)
			// If the context is done, there's no point trying more peers.
			if err := ctx.Err(); err != nil {
				c.log.CtxDebugf(ctx, "Not trying more peers for %q since the context is done: %s", r.GetDigest().GetHash(), err)
				break
			}
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
		c.log.CtxDebugf(ctx, "Could not open enough remoteWriters for digest %s. All peers: %s, opened: %s (peerset: %+v)", r.Digest.GetHash(), allPeers, openPeers, ps)
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
	return c.distributedReader(ctx, r, uncompressedOffset, limit, "Reader" /*=metricsLabel*/)
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
