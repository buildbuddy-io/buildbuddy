package store

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/leasekeeper"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagetracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/hashicorp/serf/serf"
	"github.com/jonboulle/clockwork"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	dbConfig "github.com/lni/dragonboat/v4/config"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	zombieNodeScanInterval = flag.Duration("cache.raft.zombie_node_scan_interval", 10*time.Second, "Check if one replica is a zombie every this often. 0 to disable.")
	zombieMinDuration      = flag.Duration("cache.raft.zombie_min_duration", 1*time.Minute, "The minimum duration a replica must remain in a zombie state to be considered a zombie.")
	replicaScanInterval    = flag.Duration("cache.raft.replica_scan_interval", 1*time.Minute, "The interval we wait to check if the replicas need to be queued for replication")
	clientSessionTTL       = flag.Duration("cache.raft.client_session_ttl", 24*time.Hour, "The duration we keep the sessions stored.")
	maxRangeSizeBytes      = flag.Int64("cache.raft.max_range_size_bytes", 1e8, "If set to a value greater than 0, ranges will be split until smaller than this size")
	enableDriver           = flag.Bool("cache.raft.enable_driver", true, "If true, enable placement driver")
)

const (
	deleteSessionsRateLimit = 1
)

type Store struct {
	env        environment.Env
	rootDir    string
	grpcAddr   string
	partitions []disk.Partition

	nodeHost      *dragonboat.NodeHost
	gossipManager interfaces.GossipService
	sender        *sender.Sender
	registry      registry.NodeRegistry
	grpcServer    *grpc.Server
	apiClient     *client.APIClient
	liveness      *nodeliveness.Liveness
	session       *client.Session
	log           log.Logger

	db     pebble.IPebbleDB
	leaser pebble.Leaser

	configuredClusters int
	rangeMu            sync.RWMutex
	openRanges         map[uint64]*rfpb.RangeDescriptor

	leaseKeeper *leasekeeper.LeaseKeeper
	replicas    sync.Map // map of uint64 rangeID -> *replica.Replica

	eventsMu       sync.Mutex
	events         chan events.Event
	eventListeners []chan events.Event
	splitRequests  chan *rfpb.RangeDescriptor

	usages *usagetracker.Tracker

	metaRangeMu   sync.Mutex
	metaRangeData []byte

	eg       *errgroup.Group
	egCancel context.CancelFunc

	updateTagsWorker *updateTagsWorker
	txnCoordinator   *txn.Coordinator

	driverQueue         *driver.Queue
	deleteSessionWorker *deleteSessionWorker

	clock clockwork.Clock
}

// registryHolder implements NodeRegistryFactory. When nodeHost is created, it
// will call this method to create the registry and use it until nodehost close.
type registryHolder struct {
	raftAddr string
	grpcAddr string
	g        interfaces.GossipService
	r        registry.NodeRegistry
}

func (rc *registryHolder) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	r := registry.NewDynamicNodeRegistry(rc.g, streamConnections, v)
	rc.r = r
	r.AddNode(nhid, rc.raftAddr, rc.grpcAddr)
	return r, nil
}

func New(env environment.Env, rootDir, raftAddress, grpcAddr string, partitions []disk.Partition) (*Store, error) {
	rangeCache := rangecache.New()
	raftListener := listener.NewRaftListener()
	gossipManager := env.GetGossipService()
	regHolder := &registryHolder{raftAddress, grpcAddr, gossipManager, nil}
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(rootDir, "wal"),
		NodeHostDir:    filepath.Join(rootDir, "nodehost"),
		RTTMillisecond: constants.RTTMillisecond,
		RaftAddress:    raftAddress,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: regHolder,
		},
		RaftEventListener:   raftListener,
		SystemEventListener: raftListener,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	registry := regHolder.r
	apiClient := client.NewAPIClient(env, nodeHost.ID(), registry)
	sender := sender.New(rangeCache, apiClient)
	db, err := pebble.Open(rootDir, "raft_store", &pebble.Options{})
	if err != nil {
		return nil, err
	}
	leaser := pebble.NewDBLeaser(db)
	clock := clockwork.NewRealClock()
	return NewWithArgs(env, rootDir, nodeHost, gossipManager, sender, registry, raftListener, apiClient, grpcAddr, partitions, db, leaser, clock)
}

func NewWithArgs(env environment.Env, rootDir string, nodeHost *dragonboat.NodeHost, gossipManager interfaces.GossipService, sender *sender.Sender, registry registry.NodeRegistry, listener *listener.RaftListener, apiClient *client.APIClient, grpcAddress string, partitions []disk.Partition, db pebble.IPebbleDB, leaser pebble.Leaser, clock clockwork.Clock) (*Store, error) {
	nodeLiveness := nodeliveness.New(nodeHost.ID(), sender)

	nhLog := log.NamedSubLogger(nodeHost.ID())
	eventsChan := make(chan events.Event, 100)

	session := client.NewSessionWithClock(clock)
	lkSession := client.NewSessionWithClock(clock)

	s := &Store{
		env:           env,
		rootDir:       rootDir,
		grpcAddr:      grpcAddress,
		nodeHost:      nodeHost,
		partitions:    partitions,
		gossipManager: gossipManager,
		sender:        sender,
		registry:      registry,
		apiClient:     apiClient,
		liveness:      nodeLiveness,
		session:       session,
		log:           nhLog,

		rangeMu:    sync.RWMutex{},
		openRanges: make(map[uint64]*rfpb.RangeDescriptor),

		leaseKeeper: leasekeeper.New(nodeHost, nhLog, nodeLiveness, listener, eventsChan, lkSession),
		replicas:    sync.Map{},

		eventsMu:       sync.Mutex{},
		events:         eventsChan,
		eventListeners: make([]chan events.Event, 0),
		splitRequests:  make(chan *rfpb.RangeDescriptor, 100),

		metaRangeMu:   sync.Mutex{},
		metaRangeData: make([]byte, 0),

		db:     db,
		leaser: leaser,
		clock:  clock,
	}

	updateTagsWorker := &updateTagsWorker{
		store:          s,
		tasks:          make(chan *updateTagsTask, 2000),
		lastExecutedAt: time.Now(),
	}

	s.updateTagsWorker = updateTagsWorker

	txnCoordinator := txn.NewCoordinator(s, apiClient, clock)
	s.txnCoordinator = txnCoordinator

	usages, err := usagetracker.New(s, gossipManager, s.NodeDescriptor(), partitions, s.AddEventListener())

	if *enableDriver {
		s.driverQueue = driver.NewQueue(s, gossipManager)
	}
	s.deleteSessionWorker = newDeleteSessionsWorker(clock, s)

	if err != nil {
		return nil, err
	}
	s.usages = usages

	grpcOptions := grpc_server.CommonGRPCServerOptions(s.env)
	s.grpcServer = grpc.NewServer(grpcOptions...)
	reflection.Register(s.grpcServer)
	grpc_prometheus.Register(s.grpcServer)
	rfspb.RegisterApiServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return nil, err
	}
	go func() {
		log.Debugf("Store started to serve on listener %s", s.grpcAddr)
		s.grpcServer.Serve(lis)
	}()

	// Start the leaseKeeper before we rejoin configured clusters, otherwise,
	// StartOnDiskReplica can be blocked when the the buffered leaderChangeListener channel is full.
	s.leaseKeeper.Start()

	s.updateTagsWorker.Start()

	// rejoin configured clusters
	nodeHostInfo := nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})

	logSize := len(nodeHostInfo.LogInfo)
	for i, logInfo := range nodeHostInfo.LogInfo {
		if nodeHost.HasNodeInfo(logInfo.ShardID, logInfo.ReplicaID) {
			s.log.Infof("Had info for cluster: %d, node: %d. (%d/%d)", logInfo.ShardID, logInfo.ReplicaID, i+1, logSize)
			r := raftConfig.GetRaftConfig(logInfo.ShardID, logInfo.ReplicaID)
			if err := nodeHost.StartOnDiskReplica(nil, false /*=join*/, s.ReplicaFactoryFn, r); err != nil {
				return nil, err
			}
			s.configuredClusters++
			s.log.Infof("Recreated cluster: %d, node: %d.", logInfo.ShardID, logInfo.ReplicaID)
		}
	}

	gossipManager.AddListener(s)
	statusz.AddSection("raft_store", "Store", s)

	// Whenever we bring up a brand new store with no ranges, we need to inform
	// other stores about its existence using store_usage tag, in order to make
	// it a potentia replication target.
	updateTagsWorker.Enqueue()

	return s, nil
}

func (s *Store) getMetaRangeBuf() []byte {
	s.metaRangeMu.Lock()
	defer s.metaRangeMu.Unlock()
	if len(s.metaRangeData) == 0 {
		return nil
	}
	buf := make([]byte, len(s.metaRangeData))
	copy(buf, s.metaRangeData)
	return buf
}

func (s *Store) setMetaRangeBuf(buf []byte) {
	s.metaRangeMu.Lock()
	defer s.metaRangeMu.Unlock()
	new := &rfpb.RangeDescriptor{}
	if err := proto.Unmarshal(buf, new); err != nil {
		s.log.Errorf("Error unmarshaling new metarange data: %s", err)
		return
	}
	if len(s.metaRangeData) > 0 {
		// Compare existing to new -- only update if generation is greater.
		existing := &rfpb.RangeDescriptor{}
		if err := proto.Unmarshal(s.metaRangeData, existing); err != nil {
			s.log.Errorf("Error unmarshaling existing metarange data: %s", err)
			return
		}
		if new.GetGeneration() <= existing.GetGeneration() {
			return
		}
	}
	// Update the value
	s.metaRangeData = buf
	s.sender.UpdateRange(new)
}

func (s *Store) queryForMetarange(ctx context.Context) {
	start := time.Now()
	stream, err := s.gossipManager.Query(constants.MetaRangeTag, nil, nil)
	if err != nil {
		s.log.Errorf("Error querying for metarange: %s", err)
	}
	for {
		select {
		case p := <-stream.ResponseCh():
			s.setMetaRangeBuf(p.Payload)
			stream.Close()
			s.log.Infof("Discovered metarange in %s", time.Since(start))
			return
		case <-ctx.Done():
			stream.Close()
			return
		}
	}
}

func (s *Store) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += s.liveness.String() + "\n"

	su := s.Usage()
	buf += fmt.Sprintf("%36s | Replicas: %4d | Leases: %4d | QPS (R): %5d | (W): %5d | Size: %d MB\n",
		su.GetNode().GetNhid(),
		su.GetReplicaCount(),
		s.leaseKeeper.LeaseCount(),
		su.GetReadQps(),
		su.GetRaftProposeQps(),
		su.GetTotalBytesUsed()/1e6,
	)

	replicas := make([]*replica.Replica, 0)
	s.replicas.Range(func(key, value any) bool {
		if r, ok := value.(*replica.Replica); ok {
			replicas = append(replicas, r)
		}
		return true
	})
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].ShardID() < replicas[j].ShardID()
	})
	for _, r := range replicas {
		replicaName := fmt.Sprintf("  Shard: %5d   Replica: %5d", r.ShardID(), r.ReplicaID())
		ru, err := r.Usage()
		if err != nil {
			buf += fmt.Sprintf("%s error: %s\n", replicaName, err)
			continue
		}
		isLeader := 0
		if rd := s.lookupRange(r.ShardID()); rd != nil {
			if s.leaseKeeper.HaveLease(rd.GetRangeId()) {
				isLeader = 1
			}
		}
		buf += fmt.Sprintf("%36s |                | Leader: %4d | QPS (R): %5d | (W): %5d\n",
			replicaName,
			isLeader,
			ru.GetReadQps(),
			ru.GetRaftProposeQps(),
		)
	}
	buf += s.usages.Statusz(ctx)
	buf += "</pre>"
	return buf
}

func (s *Store) handleEvents(ctx context.Context) error {
	for {
		select {
		case e := <-s.events:
			s.eventsMu.Lock()
			for _, ch := range s.eventListeners {
				select {
				case ch <- e:
					continue
				default:
					s.log.Warningf("Dropped event: %s", e)
				}
			}
			s.eventsMu.Unlock()
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Store) AddEventListener() <-chan events.Event {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()

	ch := make(chan events.Event, 100)
	s.eventListeners = append(s.eventListeners, ch)
	return ch
}

// Start starts a new grpc server which exposes an API that can be used to manage
// ranges on this node.
func (s *Store) Start() error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	s.egCancel = cancelFunc

	eg, gctx := errgroup.WithContext(ctx)
	s.eg = eg
	eg.Go(func() error {
		return s.handleEvents(gctx)
	})
	eg.Go(func() error {
		s.acquireNodeLiveness(gctx)
		return nil
	})
	eg.Go(func() error {
		s.queryForMetarange(gctx)
		return nil
	})
	eg.Go(func() error {
		s.cleanupZombieNodes(gctx)
		return nil
	})
	eg.Go(func() error {
		s.checkIfReplicasNeedSplitting(gctx)
		return nil
	})
	eg.Go(func() error {
		s.updateStoreUsageTag(gctx)
		return nil
	})
	eg.Go(func() error {
		s.processSplitRequests(gctx)
		return nil
	})
	eg.Go(func() error {
		s.txnCoordinator.Start(gctx)
		return nil
	})
	eg.Go(func() error {
		s.scanReplicas(gctx)
		return nil
	})
	eg.Go(func() error {
		if s.driverQueue != nil {
			s.driverQueue.Start(gctx)
		}
		return nil
	})
	eg.Go(func() error {
		s.deleteSessionWorker.Start(gctx)
		return nil
	})

	return nil
}

func (s *Store) Stop(ctx context.Context) error {
	s.dropLeadershipForShutdown()
	now := time.Now()
	defer func() {
		s.log.Infof("Store shutdown finished in %s", time.Since(now))
	}()

	if s.egCancel != nil {
		s.egCancel()
		s.leaseKeeper.Stop()
		s.liveness.Release()
		s.eg.Wait()
	}
	s.updateTagsWorker.Stop()

	s.log.Info("Store: waitgroups finished")
	s.nodeHost.Close()
	s.log.Info("Store: nodeHost closed")

	if err := s.db.Flush(); err != nil {
		return err
	}
	log.Info("Store: db flushed")

	// Wait for all active requests to be finished.
	s.leaser.Close()
	log.Info("Store: leaser closed")
	return grpc_server.GRPCShutdown(ctx, s.grpcServer)
}

func (s *Store) lookupRange(shardID uint64) *rfpb.RangeDescriptor {
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()

	for _, rangeDescriptor := range s.openRanges {
		if len(rangeDescriptor.GetReplicas()) == 0 {
			continue
		}
		if shardID == rangeDescriptor.GetReplicas()[0].GetShardId() {
			return rangeDescriptor
		}
	}
	return nil
}

func (s *Store) dropLeadershipForShutdown() {
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})
	if nodeHostInfo == nil {
		return
	}
	eg := errgroup.Group{}
	for _, clusterInfo := range nodeHostInfo.ShardInfoList {
		clusterInfo := clusterInfo
		if clusterInfo.LeaderID != clusterInfo.ReplicaID || clusterInfo.Term == 0 {
			// skip if not the leader
			continue
		}

		// Pick the first node in the map that isn't us. Map ordering is
		// random; which is a good thing, it means we're randomly picking
		// another node in the cluster and requesting they take the lead.
		for replicaID := range clusterInfo.Nodes {
			if replicaID == clusterInfo.ReplicaID {
				continue
			}
			eg.Go(func() error {
				log.Debugf("request to transfer leadership of shard %d to replica %d from replica %d", clusterInfo.ShardID, replicaID, clusterInfo.ReplicaID)
				if err := s.nodeHost.RequestLeaderTransfer(clusterInfo.ShardID, replicaID); err != nil {
					s.log.Warningf("Error transferring leadership: %s", err)
				}
				return nil
			})
			break
		}
	}
	eg.Wait()
}

func (s *Store) GetRange(shardID uint64) *rfpb.RangeDescriptor {
	return s.lookupRange(shardID)
}

func (s *Store) sendRangeEvent(eventType events.EventType, rd *rfpb.RangeDescriptor) {
	ev := events.RangeEvent{
		Type:            eventType,
		RangeDescriptor: rd,
	}

	select {
	case s.events <- ev:
		break
	default:
		s.log.Warningf("Dropping range event: %+v", ev)
	}
}

// We need to implement the Add/RemoveRange interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Adding range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetStart(), rd.GetEnd(), rd.GetGeneration())
	_, loaded := s.replicas.LoadOrStore(rd.GetRangeId(), r)
	if loaded {
		s.log.Warningf("AddRange stomped on another range. Did you forget to call RemoveRange?")
	}

	s.rangeMu.Lock()
	s.openRanges[rd.GetRangeId()] = rd
	s.rangeMu.Unlock()

	metrics.RaftRanges.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
	}).Inc()

	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("range %d has no replicas (yet?)", rd.GetRangeId())
		return
	}

	if rd.GetStart() == nil && rd.GetEnd() == nil {
		s.log.Debugf("range %d has no bounds (yet?)", rd.GetRangeId())
		return
	}

	s.sendRangeEvent(events.EventRangeAdded, rd)

	if rangelease.ContainsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			s.log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		go s.gossipManager.SendUserEvent(constants.MetaRangeTag, buf /*coalesce=*/, false)
	}

	s.leaseKeeper.AddRange(rd, r)
	s.updateTagsWorker.Enqueue()
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Removing range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetStart(), rd.GetEnd(), rd.GetGeneration())
	s.replicas.Delete(rd.GetRangeId())
	s.usages.RemoveRange(rd.GetRangeId())

	s.rangeMu.Lock()
	delete(s.openRanges, rd.GetRangeId())
	s.rangeMu.Unlock()

	metrics.RaftRanges.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
	}).Dec()

	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("range descriptor had no replicas yet")
		return
	}

	s.sendRangeEvent(events.EventRangeRemoved, rd)
	s.leaseKeeper.RemoveRange(rd, r)
	s.updateTagsWorker.Enqueue()
}

func (s *Store) Sample(ctx context.Context, rangeID uint64, partition string, n int) ([]*approxlru.Sample[*replica.LRUSample], error) {
	r, _, err := s.replicaForRange(rangeID)
	if err != nil {
		return nil, err
	}
	return r.Sample(ctx, partition, n)
}

func (s *Store) replicaForRange(rangeID uint64) (*replica.Replica, *rfpb.RangeDescriptor, error) {
	s.rangeMu.RLock()
	rd, rangeOK := s.openRanges[rangeID]
	s.rangeMu.RUnlock()
	if !rangeOK {
		return nil, nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, rangeID)
	}

	if len(rd.GetReplicas()) == 0 {
		return nil, nil, status.OutOfRangeErrorf("%s: range had no replicas %d", constants.RangeNotFoundMsg, rangeID)
	}

	r, err := s.GetReplica(rangeID)
	if err != nil {
		return nil, nil, err
	}
	return r, rd, nil
}

// validatedRange verifies that the header is valid and the client is using
// an up-to-date range descriptor. In most cases, it's also necessary to verify
// that a local replica has a range lease for the given range ID which can be
// done by using the LeasedRange function.
func (s *Store) validatedRange(header *rfpb.Header) (*replica.Replica, *rfpb.RangeDescriptor, error) {
	if header == nil {
		return nil, nil, status.FailedPreconditionError("Header must be set (was nil)")
	}

	r, rd, err := s.replicaForRange(header.GetRangeId())
	if err != nil {
		return nil, nil, err
	}

	// Ensure the header generation matches what we have locally -- if not,
	// force client to go back and re-pull the rangeDescriptor from the meta
	// range.
	if rd.GetGeneration() != header.GetGeneration() {
		return nil, nil, status.OutOfRangeErrorf("%s: id %d generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetRangeId(), rd.GetGeneration(), header.GetGeneration())
	}

	return r, rd, nil
}

func (s *Store) HaveLease(rangeID uint64) bool {
	if r, err := s.GetReplica(rangeID); err == nil {
		return s.leaseKeeper.HaveLease(r.ShardID())
	}
	s.log.Warningf("HaveLease check for unheld range: %d", rangeID)
	return false
}

// LeasedRange verifies that the header is valid and the client is using
// an up-to-date range descriptor. It also checks that a local replica owns
// the range lease for the requested range.
func (s *Store) LeasedRange(header *rfpb.Header) (*replica.Replica, error) {
	r, _, err := s.validatedRange(header)
	if err != nil {
		return nil, err
	}

	// Stale reads don't need a lease, so return early.
	if header.GetConsistencyMode() == rfpb.Header_STALE {
		return r, nil
	}

	if s.HaveLease(header.GetRangeId()) {
		return r, nil
	}
	return nil, status.OutOfRangeErrorf("%s: no lease found for range: %d", constants.RangeLeaseInvalidMsg, header.GetRangeId())
}

func (s *Store) ReplicaFactoryFn(shardID, replicaID uint64) dbsm.IOnDiskStateMachine {
	r := replica.New(s.leaser, shardID, replicaID, s, s.events)
	return r
}

func (s *Store) Sender() *sender.Sender {
	return s.sender
}

func (s *Store) APIClient() *client.APIClient {
	return s.apiClient
}

func (s *Store) ConfiguredClusters() int {
	return s.configuredClusters
}

func (s *Store) NodeHost() *dragonboat.NodeHost {
	return s.nodeHost
}

func (s *Store) NHID() string {
	return s.nodeHost.ID()
}

func (s *Store) NodeDescriptor() *rfpb.NodeDescriptor {
	return &rfpb.NodeDescriptor{
		Nhid:        s.nodeHost.ID(),
		RaftAddress: s.nodeHost.RaftAddress(),
		GrpcAddress: s.grpcAddr,
	}
}

func (s *Store) GetReplica(rangeID uint64) (*replica.Replica, error) {
	// This code will be called by all replicas in a range when
	// doing a split, so we do not check for range leases here.
	rIface, ok := s.replicas.Load(rangeID)
	if !ok {
		return nil, status.OutOfRangeErrorf("%s: replica for range %d not found", constants.RangeNotFoundMsg, rangeID)
	}
	r, ok := rIface.(*replica.Replica)
	if !ok {
		alert.UnexpectedEvent("unexpected_replicas_map_type_error")
		return nil, status.FailedPreconditionError("Replica type-mismatch; this should not happen")
	}
	return r, nil
}

func (s *Store) IsLeader(shardID uint64) bool {
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})
	if nodeHostInfo == nil {
		return false
	}
	for _, clusterInfo := range nodeHostInfo.ShardInfoList {
		if clusterInfo.ShardID == shardID {
			return clusterInfo.LeaderID == clusterInfo.ReplicaID && clusterInfo.Term > 0
		}
	}
	return false
}

func (s *Store) TransferLeadership(ctx context.Context, req *rfpb.TransferLeadershipRequest) (*rfpb.TransferLeadershipResponse, error) {
	if err := s.nodeHost.RequestLeaderTransfer(req.GetShardId(), req.GetTargetReplicaId()); err != nil {
		return nil, err
	}
	return &rfpb.TransferLeadershipResponse{}, nil
}

// SnapshotCluster snapshots the cluster *on this node*. This is a local operation and does not
// create a snapshot on other nodes that are members of this cluster.
func (s *Store) SnapshotCluster(ctx context.Context, shardID uint64) error {
	defer canary.Start("SnapshotCluster", 10*time.Second)()
	if _, ok := ctx.Deadline(); !ok {
		c, cancel := context.WithTimeout(ctx, client.DefaultContextTimeout)
		defer cancel()
		ctx = c
	}
	opts := dragonboat.SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         0,
	}

	// Wait a little longer for the replica to accept the snapshot request
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := s.nodeHost.SyncRequestSnapshot(ctx, shardID, opts)
			if err == nil {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (s *Store) ListReplicas(ctx context.Context, req *rfpb.ListReplicasRequest) (*rfpb.ListReplicasResponse, error) {
	rsp := &rfpb.ListReplicasResponse{
		Node: s.NodeDescriptor(),
	}
	nhInfo := s.nodeHost.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
	for _, shardInfo := range nhInfo.ShardInfoList {
		rsp.Replicas = append(rsp.Replicas, &rfpb.ReplicaDescriptor{
			ShardId:   shardInfo.ShardID,
			ReplicaId: shardInfo.ReplicaID,
		})
	}
	return rsp, nil
}

func (s *Store) getLeasedReplicas() []*replica.Replica {
	s.rangeMu.RLock()
	openRanges := make([]*rfpb.RangeDescriptor, 0, len(s.openRanges))
	for _, rd := range s.openRanges {
		openRanges = append(openRanges, rd)
	}
	s.rangeMu.RUnlock()

	res := make([]*replica.Replica, 0, len(openRanges))
	for _, rd := range openRanges {
		header := &rfpb.Header{
			RangeId:    rd.GetRangeId(),
			Generation: rd.GetGeneration(),
		}
		r, err := s.LeasedRange(header)
		if err != nil {
			continue
		}
		res = append(res, r)
	}
	return res
}

func (s *Store) StartShard(ctx context.Context, req *rfpb.StartShardRequest) (*rfpb.StartShardResponse, error) {
	s.log.Infof("Starting new raft node c%dn%d", req.GetShardId(), req.GetReplicaId())
	rc := raftConfig.GetRaftConfig(req.GetShardId(), req.GetReplicaId())
	err := s.nodeHost.StartOnDiskReplica(req.GetInitialMember(), req.GetJoin(), s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrShardAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return nil, err
	}

	if req.GetLastAppliedIndex() > 0 {
		if err := s.waitForReplicaToCatchUp(ctx, req.GetShardId(), req.GetLastAppliedIndex()); err != nil {
			return nil, err
		}
	}

	rsp := &rfpb.StartShardResponse{}
	if req.GetBatch() == nil || len(req.GetInitialMember()) == 0 {
		return rsp, nil
	}

	// If we are the last member in the cluster, we'll do the syncPropose.
	replicaIDs := make([]uint64, 0, len(req.GetInitialMember()))
	for replicaID := range req.GetInitialMember() {
		replicaIDs = append(replicaIDs, replicaID)
	}
	sort.Slice(replicaIDs, func(i, j int) bool { return replicaIDs[i] < replicaIDs[j] })
	if req.GetReplicaId() == replicaIDs[len(replicaIDs)-1] {
		batchResponse, err := s.session.SyncProposeLocal(ctx, s.nodeHost, req.GetShardId(), req.GetBatch())
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

// RemoveData tries to remove all data associated with the specified node (shard, replica). It waits for the node (shard, replica) to be fully offloaded or the context is cancelled. This method should only be used after the node is deleted from its Raft cluster.
func (s *Store) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	err := client.RunNodehostFn(ctx, func(ctx context.Context) error {
		err := s.nodeHost.SyncRemoveData(ctx, req.GetShardId(), req.GetReplicaId())
		if err == dragonboat.ErrShardNotStopped {
			err = dragonboat.ErrTimeout
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &rfpb.RemoveDataResponse{}, nil
}

// SyncPropose makes a synchronous proposal (writes) on the Raft shard.
func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	var shardID uint64
	header := req.GetHeader()

	// Proxied SyncPropose requests don't need a lease, so don't bother
	// checking for one. If the referenced shard is not present on this
	// node, the request will fail in client.SyncProposeLocal().
	if header.GetRangeId() == 0 && header.GetReplica() != nil {
		shardID = header.GetReplica().GetShardId()
	} else {
		r, err := s.LeasedRange(req.GetHeader())
		if err != nil {
			return nil, err
		}
		shardID = r.ShardID()
	}

	batchResponse, err := s.session.SyncProposeLocal(ctx, s.nodeHost, shardID, req.GetBatch())
	if err != nil {
		if err == dragonboat.ErrShardNotFound {
			return nil, status.OutOfRangeErrorf("%s: cluster %d not found", constants.RangeLeaseInvalidMsg, shardID)
		}
		return nil, err
	}
	return &rfpb.SyncProposeResponse{
		Batch: batchResponse,
	}, nil
}

// SyncRead performs a synchronous linearizable read on the specified Raft shard.
func (s *Store) SyncRead(ctx context.Context, req *rfpb.SyncReadRequest) (*rfpb.SyncReadResponse, error) {
	batch := req.GetBatch()
	batch.Header = req.GetHeader()

	if batch.Header != nil {
		if _, err := s.LeasedRange(batch.Header); err != nil {
			return nil, err
		}
	} else {
		s.log.Warningf("SyncRead without header: %+v", req)
	}

	shardID := req.GetHeader().GetReplica().GetShardId()
	batchResponse, err := client.SyncReadLocal(ctx, s.nodeHost, shardID, batch)
	if err != nil {
		if err == dragonboat.ErrShardNotFound {
			return nil, status.OutOfRangeErrorf("%s: cluster not found for %+v", constants.RangeLeaseInvalidMsg, req.GetHeader())
		}
		return nil, err
	}

	return &rfpb.SyncReadResponse{
		Batch: batchResponse,
	}, nil
}

func (s *Store) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		if query.Name == constants.MetaRangeTag {
			if buf := s.getMetaRangeBuf(); len(buf) > 0 {
				if err := query.Respond(buf); err != nil {
					s.log.Debugf("Error responding to metarange query: %s", err)
				}
			}
		}
	case serf.EventUser:
		userEvent, _ := event.(serf.UserEvent)
		if userEvent.Name == constants.MetaRangeTag {
			s.setMetaRangeBuf(userEvent.Payload)
		}
	default:
		break
	}
}

func (s *Store) acquireNodeLiveness(ctx context.Context) {
	start := time.Now()
	defer func() {
		s.log.Infof("Acquired node liveness in %s", time.Since(start))
	}()

	ticker := s.clock.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			s.metaRangeMu.Lock()
			haveMetaRange := len(s.metaRangeData) > 0
			s.metaRangeMu.Unlock()

			if !haveMetaRange {
				continue
			}
			if s.liveness.Valid() {
				return
			}
			err := s.liveness.Lease()
			if err == nil {
				return
			}
			s.log.Errorf("Error leasing node liveness record: %s", err)
		}
	}
}

type membershipStatus int

const (
	membershipStatusUnknown membershipStatus = iota
	membershipStatusMember
	membershipStatusNotMember
)

// checkMembershipStatus checks the status of the membership given the shard info.
// Returns membershipStatusMember when the replica's config change is current and
// it's one of the voters, non-voters, or witnesses;
// Returns membershipStatusUnknown when the replica's config change is behind or
// there is error to get membership info.
func (s *Store) checkMembershipStatus(ctx context.Context, shardInfo dragonboat.ShardInfo) membershipStatus {
	// Get the config change index for this shard.
	membership, err := s.getMembership(ctx, shardInfo.ShardID)
	if err != nil {
		s.log.Errorf("checkMembershipStatus failed to get membership for shard %d: %s", shardInfo.ShardID, err)
		return membershipStatusUnknown
	}

	if shardInfo.ConfigChangeIndex > 0 && shardInfo.ConfigChangeIndex <= membership.ConfigChangeID {
		return membershipStatusUnknown
	}

	for replicaID := range membership.Nodes {
		if replicaID == shardInfo.ReplicaID {
			return membershipStatusMember
		}
	}
	for replicaID := range membership.NonVotings {
		if replicaID == shardInfo.ReplicaID {
			return membershipStatusMember
		}
	}
	for replicaID := range membership.Witnesses {
		if replicaID == shardInfo.ReplicaID {
			return membershipStatusMember
		}
	}
	return membershipStatusNotMember
}

// isZombieNode checks whether a node is a zombie node.
func (s *Store) isZombieNode(ctx context.Context, shardInfo dragonboat.ShardInfo) bool {
	membershipStatus := s.checkMembershipStatus(ctx, shardInfo)
	if membershipStatus == membershipStatusNotMember {
		return true
	}

	rd := s.lookupRange(shardInfo.ShardID)
	if rd == nil {
		return true
	}

	// The replica info in the local range descriptor could be out of date. For
	// example, when another node in the raft cluster proposed to remove this node
	// when this node is dead. When this node restarted, the range descriptor is
	// behind, but it cannot get updates from other nodes b/c it was removed from the
	// cluster.
	updatedRD, err := s.Sender().LookupRangeDescriptor(ctx, rd.GetStart(), true /*skip Cache */)
	if err != nil {
		s.log.Errorf("failed to look up range descriptor: %s", err)
		return false
	}
	if updatedRD.GetGeneration() >= rd.GetGeneration() {
		rd = updatedRD
	}
	for _, r := range rd.GetReplicas() {
		if r.GetShardId() == shardInfo.ShardID && r.GetReplicaId() == shardInfo.ReplicaID {
			return false
		}
	}
	return true
}

func (s *Store) cleanupZombieNodes(ctx context.Context) {
	if *zombieNodeScanInterval == 0 {
		return
	}
	timer := s.clock.NewTicker(*zombieNodeScanInterval)
	defer timer.Stop()

	nInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
	idx := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.Chan():
			if idx == len(nInfo.ShardInfoList) {
				idx = 0
				nInfo = s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
				continue
			}
			sInfo := nInfo.ShardInfoList[idx]
			idx += 1

			if s.isZombieNode(ctx, sInfo) {
				s.log.Debugf("Found a potential Zombie: %+v", sInfo)
				potentialZombie := sInfo
				deleteTimer := s.clock.AfterFunc(*zombieMinDuration, func() {
					if !s.isZombieNode(ctx, potentialZombie) {
						return
					}
					s.log.Debugf("Removing zombie node: %+v...", potentialZombie)
					if err := s.nodeHost.StopReplica(potentialZombie.ShardID, potentialZombie.ReplicaID); err != nil {
						s.log.Errorf("Error stopping zombie replica: %s", err)
					} else {
						if _, err := s.RemoveData(ctx, &rfpb.RemoveDataRequest{
							ShardId:   potentialZombie.ShardID,
							ReplicaId: potentialZombie.ReplicaID,
						}); err != nil {
							s.log.Errorf("Error removing zombie replica data: %s", err)
						} else {
							s.log.Infof("Successfully removed zombie node: %+v", potentialZombie)
						}
					}
				})
				defer deleteTimer.Stop()
			}
		}
	}
}

func (s *Store) checkIfReplicasNeedSplitting(ctx context.Context) {
	if *maxRangeSizeBytes == 0 {
		return
	}
	eventsCh := s.AddEventListener()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-eventsCh:
			switch e.EventType() {
			case events.EventRangeUsageUpdated:
				rangeUsageEvent := e.(events.RangeUsageEvent)
				if !s.leaseKeeper.HaveLease(rangeUsageEvent.RangeDescriptor.GetRangeId()) {
					continue
				}
				if rangeUsageEvent.ReplicaUsage.GetEstimatedDiskBytesUsed() < *maxRangeSizeBytes {
					continue
				}
				rd := rangeUsageEvent.RangeDescriptor.CloneVT()
				s.log.Infof("Requesting split for range: %+v", rd)
				select {
				case s.splitRequests <- rd:
					break
				default:
					s.log.Debugf("Split queue full. Dropping message.")
				}
			default:
				break
			}
		}
	}
}

func (s *Store) updateStoreUsageTag(ctx context.Context) {
	eventsCh := s.AddEventListener()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-eventsCh:
			switch e.EventType() {
			case events.EventRangeUsageUpdated:
				rangeUsageEvent := e.(events.RangeUsageEvent)
				if !s.leaseKeeper.HaveLease(rangeUsageEvent.RangeDescriptor.GetRangeId()) {
					continue
				}
				s.updateTagsWorker.Enqueue()
			default:
				break
			}
		}
	}
}

func makeHeader(rangeDescriptor *rfpb.RangeDescriptor) *rfpb.Header {
	return &rfpb.Header{
		Replica:         rangeDescriptor.GetReplicas()[0],
		RangeId:         rangeDescriptor.GetRangeId(),
		Generation:      rangeDescriptor.GetGeneration(),
		ConsistencyMode: rfpb.Header_LINEARIZABLE,
	}
}

func (s *Store) processSplitRequests(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rd := <-s.splitRequests:
			splitReq := &rfpb.SplitRangeRequest{
				Header: makeHeader(rd),
				Range:  rd,
			}
			if rsp, err := s.SplitRange(ctx, splitReq); err != nil {
				s.log.Errorf("Error splitting range: %s", err)
			} else {
				s.log.Infof("Successfully split range: %+v", rsp)
			}
		}
	}
}

func (s *Store) Usage() *rfpb.StoreUsage {
	su := &rfpb.StoreUsage{
		Node: s.NodeDescriptor(),
	}

	s.rangeMu.RLock()
	su.ReplicaCount = int64(len(s.openRanges))
	s.rangeMu.RUnlock()

	su.LeaseCount = s.leaseKeeper.LeaseCount()
	s.replicas.Range(func(key, value any) bool {
		r, _ := value.(*replica.Replica)
		ru, err := r.Usage()
		if err != nil {
			return true // keep going
		}
		su.ReadQps += ru.GetReadQps()
		su.RaftProposeQps += ru.GetRaftProposeQps()
		return true
	})

	db, err := s.leaser.DB()
	if err != nil {
		return nil
	}
	defer db.Close()
	diskEstimateBytes, err := db.EstimateDiskUsage(keys.MinByte, keys.MaxByte)
	if err != nil {
		return nil
	}
	su.TotalBytesUsed = int64(diskEstimateBytes)

	capacity := int64(0)
	for _, p := range s.partitions {
		capacity += p.MaxSizeBytes
	}
	su.TotalBytesFree = capacity - su.TotalBytesUsed
	return su
}

type updateTagsTask struct {
	// createdAt is the time at which this task was created
	createdAt time.Time
}

type updateTagsWorker struct {
	mu sync.Mutex // protects(lastExecutedAt, tasks)
	// lastExecutedAt is the time at which udpateTagsWorker finished execution.
	lastExecutedAt time.Time
	tasks          chan *updateTagsTask

	quitChan chan struct{}
	eg       errgroup.Group

	store *Store
}

func (w *updateTagsWorker) Enqueue() {
	task := &updateTagsTask{
		createdAt: time.Now(),
	}

	select {
	case w.tasks <- task:
		break
	default:
		alert.UnexpectedEvent(
			"update_tags_channel_buffer_full",
			"Failed to update tags: update tags task buffer is full")
	}
}

func (w *updateTagsWorker) Start() {
	w.quitChan = make(chan struct{})
	w.eg.Go(func() error {
		w.processUpdateTags()
		return nil
	})
}

func (w *updateTagsWorker) processUpdateTags() error {
	eg := &errgroup.Group{}
	eg.Go(func() error {
		for {
			select {
			case <-w.quitChan:
				return nil
			case task := <-w.tasks:
				w.handleTask(task)
			}
		}
	})
	eg.Wait()

	for len(w.tasks) > 0 {
		<-w.tasks
	}
	return nil

}

func (w *updateTagsWorker) Stop() {
	close(w.quitChan)

	if err := w.eg.Wait(); err != nil {
		log.Error(err.Error())
	}
}

func (w *updateTagsWorker) handleTask(task *updateTagsTask) {
	w.mu.Lock()
	if task.createdAt.Before(w.lastExecutedAt) {
		w.mu.Unlock()
		return
	}

	w.lastExecutedAt = time.Now()
	w.mu.Unlock()
	w.updateTags()
}

func (w *updateTagsWorker) updateTags() error {
	storeTags := make(map[string]string, 0)

	if zone := resources.GetZone(); zone != "" {
		storeTags[constants.ZoneTag] = zone
	} else {
		storeTags[constants.ZoneTag] = "local"
	}

	su := w.store.Usage()
	buf, err := proto.Marshal(su)
	if err != nil {
		return err
	}
	storeTags[constants.StoreUsageTag] = base64.StdEncoding.EncodeToString(buf)
	err = w.store.gossipManager.SetTags(storeTags)
	return err
}

type deleteSessionWorker struct {
	rateLimiter       *rate.Limiter
	lastExecutionTime sync.Map // map of uint64 rangeID -> the timestamp we last delete sessions
	session           *client.Session
	clock             clockwork.Clock
	store             *Store
	deleteChan        chan *replica.Replica
}

func newDeleteSessionsWorker(clock clockwork.Clock, store *Store) *deleteSessionWorker {
	return &deleteSessionWorker{
		rateLimiter:       rate.NewLimiter(rate.Limit(deleteSessionsRateLimit), 1),
		store:             store,
		lastExecutionTime: sync.Map{},
		session:           client.NewSessionWithClock(clock),
		clock:             clock,
		deleteChan:        make(chan *replica.Replica, 10000),
	}
}

func (w *deleteSessionWorker) Enqueue(repl *replica.Replica) {
	select {
	case w.deleteChan <- repl:
		break
	default:
		log.Warning("deleteSessionWorker queue full")
	}
}

func (w *deleteSessionWorker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for len(w.deleteChan) > 0 {
				<-w.deleteChan
			}
			return
		case repl := <-w.deleteChan:
			w.deleteSessions(ctx, repl)
		}
	}
}

func (w *deleteSessionWorker) deleteSessions(ctx context.Context, repl *replica.Replica) error {
	rd := repl.RangeDescriptor()
	lastExecutionTimeI, found := w.lastExecutionTime.Load(rd.GetRangeId())
	if found {
		lastExecutionTime, ok := lastExecutionTimeI.(time.Time)
		if ok {
			if w.clock.Since(lastExecutionTime) <= client.SessionLifetime() {
				// There are probably no client sessions to delete.
				return nil
			}
		}

		return status.InternalErrorf("unable to delete sessions for rangeID=%d: unable to parse lastExecutionTime", rd.GetRangeId())
	}

	if !w.store.HaveLease(rd.GetRangeId()) {
		log.Infof("skip because the store doesn't have lease for range ID:= %d", rd.GetRangeId())
		// The replica doesn't have the lease right now. skip.
		return nil
	}

	if err := w.rateLimiter.Wait(ctx); err != nil {
		return err
	}
	now := w.clock.Now()
	request, err := rbuilder.NewBatchBuilder().Add(
		&rfpb.DeleteSessionsRequest{
			CreatedAtUsec: now.Add(-*clientSessionTTL).UnixMicro(),
		}).ToProto()
	if err != nil {
		return status.InternalErrorf("unable to delete sessions for rangeID=%d: unable to build request: %s", rd.GetRangeId(), err)
	}
	rsp, err := w.session.SyncProposeLocal(ctx, w.store.NodeHost(), repl.ShardID(), request)
	if err != nil {
		return status.InternalErrorf("unable to delete sessions for rangeID=%d: SyncProposeLocal fails: %s", rd.GetRangeId(), err)
	}
	w.lastExecutionTime.Store(rd.GetRangeId(), now)
	_, err = rbuilder.NewBatchResponseFromProto(rsp).DeleteSessionsResponse(0)
	return status.InternalErrorf("unable to delete sessions for rangeID=%d: DeleteSessions fails: %s", rd.GetRangeId(), err)
}

func (s *Store) SplitRange(ctx context.Context, req *rfpb.SplitRangeRequest) (*rfpb.SplitRangeResponse, error) {
	startTime := time.Now()
	leftRange := req.GetRange()
	if leftRange == nil {
		return nil, status.FailedPreconditionErrorf("no range provided to split: %+v", req)
	}
	if len(leftRange.GetReplicas()) == 0 {
		return nil, status.FailedPreconditionErrorf("no replicas in range: %+v", leftRange)
	}

	// Validate the header to ensure we don't start new raft nodes if the
	// split is gonna fail later when the transaction is run on an out-of
	// -date range.
	_, _, err := s.validatedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}

	// Copy left range, because it's a pointer and will change when we
	// propose the split.
	leftRange = leftRange.CloneVT()
	shardID := leftRange.GetReplicas()[0].GetShardId()

	// Reserve new IDs for this cluster.
	newShardID, newRangeID, err := s.reserveClusterAndRangeID(ctx)
	if err != nil {
		return nil, status.InternalErrorf("could not reserve IDs for new cluster: %s", err)
	}

	// Find Split Point.
	fsp := rbuilder.NewBatchBuilder().Add(&rfpb.FindSplitPointRequest{}).SetHeader(req.GetHeader())
	fspRsp, err := client.SyncReadLocalBatch(ctx, s.nodeHost, shardID, fsp)
	if err != nil {
		return nil, status.InternalErrorf("find split point err: %s", err)
	}
	splitPointResponse, err := fspRsp.FindSplitPointResponse(0)
	if err != nil {
		return nil, err
	}

	stubRightRange := proto.Clone(leftRange).(*rfpb.RangeDescriptor)
	stubRightRange.Start = nil
	stubRightRange.End = nil
	stubRightRange.RangeId = newRangeID
	stubRightRange.Generation += 1
	for i, r := range stubRightRange.GetReplicas() {
		r.ReplicaId = uint64(i + 1)
		r.ShardId = newShardID
	}
	stubRightRangeBuf, err := proto.Marshal(stubRightRange)
	if err != nil {
		return nil, err
	}
	stubBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: stubRightRangeBuf,
		},
	})
	// Bringup new peers.
	servers := make(map[string]string, len(leftRange.GetReplicas()))
	for _, r := range leftRange.GetReplicas() {
		if r.GetNhid() == "" {
			return nil, status.InternalErrorf("empty nhid in ReplicaDescriptor %+v", r)
		}
		grpcAddr, _, err := s.registry.ResolveGRPC(r.GetShardId(), r.GetReplicaId())
		if err != nil {
			return nil, err
		}
		servers[r.GetNhid()] = grpcAddr
	}
	bootstrapInfo := bringup.MakeBootstrapInfo(newShardID, 1, servers)
	if err := bringup.StartShard(ctx, s.apiClient, bootstrapInfo, stubBatch); err != nil {
		return nil, err
	}

	// Assemble new range descriptor.
	newRightRange := proto.Clone(leftRange).(*rfpb.RangeDescriptor)
	newRightRange.Start = splitPointResponse.GetSplitKey()
	newRightRange.RangeId = newRangeID
	newRightRange.Generation += 1
	newRightRange.Replicas = bootstrapInfo.Replicas
	newRightRangeBuf, err := proto.Marshal(newRightRange)
	if err != nil {
		return nil, err
	}

	updatedLeftRange := proto.Clone(leftRange).(*rfpb.RangeDescriptor)
	updatedLeftRange.End = splitPointResponse.GetSplitKey()
	updatedLeftRange.Generation += 1

	leftBatch := rbuilder.NewBatchBuilder()
	if err := addLocalRangeEdits(leftRange, updatedLeftRange, leftBatch); err != nil {
		return nil, err
	}
	leftBatch.AddPostCommitHook(&rfpb.SnapshotClusterHook{})
	rightBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: newRightRangeBuf,
		},
	})
	rightBatch.AddPostCommitHook(&rfpb.SnapshotClusterHook{})
	metaBatch := rbuilder.NewBatchBuilder()
	if err := addMetaRangeEdits(leftRange, updatedLeftRange, newRightRange, metaBatch); err != nil {
		return nil, err
	}
	mrd := s.sender.GetMetaRangeDescriptor()
	txn := rbuilder.NewTxn().AddStatement(leftRange.GetReplicas()[0], leftBatch)
	txn = txn.AddStatement(newRightRange.GetReplicas()[0], rightBatch)
	txn = txn.AddStatement(mrd.GetReplicas()[0], metaBatch)
	if err := s.txnCoordinator.RunTxn(ctx, txn); err != nil {
		return nil, err
	}

	// Increment RaftSplits counter.
	metrics.RaftSplits.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
	}).Inc()

	// Observe split duration.
	metrics.RaftSplitDurationUs.With(prometheus.Labels{
		metrics.RaftRangeIDLabel: strconv.Itoa(int(leftRange.GetRangeId())),
	}).Observe(float64(time.Since(startTime).Microseconds()))

	return &rfpb.SplitRangeResponse{
		Left:  updatedLeftRange,
		Right: newRightRange,
	}, nil
}

func (s *Store) getLastAppliedIndex(header *rfpb.Header) (uint64, error) {
	r, _, err := s.validatedRange(header)
	if err != nil {
		return 0, err
	}
	return r.LastAppliedIndex()
}

func (s *Store) waitForReplicaToCatchUp(ctx context.Context, shardID uint64, desiredLastAppliedIndex uint64) error {
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue with for loop
		}
		if rd := s.lookupRange(shardID); rd != nil {
			if r, err := s.GetReplica(rd.GetRangeId()); err == nil {
				if lastApplied, err := r.LastAppliedIndex(); err == nil {
					if lastApplied >= desiredLastAppliedIndex {
						s.log.Infof("Cluster %d took %s to catch up", shardID, time.Since(start))
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (s *Store) getMembership(ctx context.Context, shardID uint64) (*dragonboat.Membership, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		// Get the config change index for this cluster.
		membership, err = s.nodeHost.SyncGetShardMembership(ctx, shardID)
		if err != nil {
			return err
		}
		// Trick client.RunNodehostFn into running this again if we got a nil
		// membership back
		if membership == nil {
			return status.OutOfRangeErrorf("cluster not ready")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if membership == nil {
		return nil, status.InternalErrorf("nil cluster membership for cluster: %d", shardID)
	}
	return membership, nil
}

func (s *Store) getConfigChangeID(ctx context.Context, shardID uint64) (uint64, error) {
	membership, err := s.getMembership(ctx, shardID)
	if err != nil {
		return 0, err
	}
	return membership.ConfigChangeID, nil
}

// AddReplica adds a new node to the specified cluster if pre-reqs are met.
// Pre-reqs are:
//   - The request must be valid and contain all information
//   - This node must be a member of the cluster that is being added to
//   - The provided range descriptor must be up to date
func (s *Store) AddReplica(ctx context.Context, req *rfpb.AddReplicaRequest) (*rfpb.AddReplicaResponse, error) {
	// Check the request looks valid.
	if len(req.GetRange().GetReplicas()) == 0 {
		return nil, status.FailedPreconditionErrorf("No replicas in range: %+v", req.GetRange())
	}
	node := req.GetNode()
	if node.GetNhid() == "" || node.GetRaftAddress() == "" || node.GetGrpcAddress() == "" {
		return nil, status.FailedPreconditionErrorf("Incomplete node descriptor: %+v", node)
	}

	// Check this is a range we have and the range descriptor provided is up to date
	s.rangeMu.RLock()
	rd, rangeOK := s.openRanges[req.GetRange().GetRangeId()]
	s.rangeMu.RUnlock()

	if !rangeOK {
		return nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, req.GetRange().GetRangeId())
	}
	if rd.GetGeneration() != req.GetRange().GetGeneration() {
		return nil, status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetGeneration(), req.GetRange().GetGeneration())
	}

	shardID := req.GetRange().GetReplicas()[0].GetShardId()

	// Reserve a new node ID for the node about to be added.
	replicaIDs, err := s.reserveReplicaIDs(ctx, 1)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to reserveReplicaIDs: %s", err)
	}
	newReplicaID := replicaIDs[0]

	// Get the config change index for this cluster.
	configChangeID, err := s.getConfigChangeID(ctx, shardID)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get config change ID: %s", err)
	}
	lastAppliedIndex, err := s.getLastAppliedIndex(&rfpb.Header{
		RangeId:    rd.GetRangeId(),
		Generation: rd.GetGeneration(),
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get last applied index: %s", err)
	}

	// Gossip the address of the node that is about to be added.
	s.registry.Add(shardID, newReplicaID, node.GetNhid())
	s.registry.AddNode(node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node to the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddReplica(ctx, shardID, newReplicaID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to call nodeHost.SyncRequestAddReplica: %s", err)
	}

	// Start the cluster on the newly added node.
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get the client for the node: %s", err)
	}
	_, err = c.StartShard(ctx, &rfpb.StartShardRequest{
		ShardId:          shardID,
		ReplicaId:        newReplicaID,
		Join:             true,
		LastAppliedIndex: lastAppliedIndex,
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to start shard: %s", err)
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	rd, err = s.addReplicaToRangeDescriptor(ctx, shardID, newReplicaID, node.GetNhid(), rd)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to add replica to range descriptor: %s", err)
	}
	metrics.RaftMoves.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
		metrics.RaftMoveLabel:       "add",
	}).Inc()

	return &rfpb.AddReplicaResponse{
		Range: rd,
	}, nil
}

// RemoveReplica removes a new node from the specified cluster if pre-reqs are
// met. Pre-reqs are:
//   - The request must be valid and contain all information
//   - This node must be a member of the cluster that is being removed from
//   - The provided range descriptor must be up to date
func (s *Store) RemoveReplica(ctx context.Context, req *rfpb.RemoveReplicaRequest) (*rfpb.RemoveReplicaResponse, error) {
	// Check this is a range we have and the range descriptor provided is up to date
	s.rangeMu.RLock()
	rd, rangeOK := s.openRanges[req.GetRange().GetRangeId()]
	s.rangeMu.RUnlock()

	if !rangeOK {
		return nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, req.GetRange().GetRangeId())
	}
	if rd.GetGeneration() != req.GetRange().GetGeneration() {
		return nil, status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetGeneration(), req.GetRange().GetGeneration())
	}

	var replicaDesc *rfpb.ReplicaDescriptor
	for _, replica := range req.GetRange().GetReplicas() {
		if replica.GetReplicaId() == req.GetReplicaId() {
			replicaDesc = replica
			break
		}
	}
	if replicaDesc == nil {
		return nil, status.FailedPreconditionErrorf("No node with id %d found in range: %+v", req.GetReplicaId(), req.GetRange())
	}

	// First, update the range descriptor information to reflect the
	// the node being removed.
	rd, err := s.removeReplicaFromRangeDescriptor(ctx, replicaDesc.GetShardId(), replicaDesc.GetReplicaId(), req.GetRange())
	if err != nil {
		return nil, err
	}

	configChangeID, err := s.getConfigChangeID(ctx, replicaDesc.GetShardId())
	if err != nil {
		return nil, err
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		err := s.nodeHost.SyncRequestDeleteReplica(ctx, replicaDesc.GetShardId(), replicaDesc.GetReplicaId(), configChangeID)
		if err == dragonboat.ErrShardClosed {
			return nil
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	metrics.RaftMoves.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
		metrics.RaftMoveLabel:       "remove",
	}).Inc()

	rsp := &rfpb.RemoveReplicaResponse{
		Range: rd,
	}

	// Remove the data from the now stopped node. This is best-effort only,
	// because we can remove the replica when the node is dead; and in this case,
	// we won't be able to connect to the node.
	c, err := s.apiClient.GetForReplica(ctx, replicaDesc)
	if err != nil {
		s.log.Warningf("RemoveReplica unable to remove data, err getting api client: %s", err)
		return rsp, nil
	}
	_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ShardId:   replicaDesc.GetShardId(),
		ReplicaId: replicaDesc.GetReplicaId(),
	})
	if err != nil {
		s.log.Warningf("RemoveReplica unable to remove data err: %s", err)
		return rsp, nil
	}

	s.log.Infof("Removed shard: s%dr%d", replicaDesc.GetShardId(), replicaDesc.GetReplicaId())
	return rsp, nil
}

func (s *Store) reserveReplicaIDs(ctx context.Context, n int) ([]uint64, error) {
	newVal, err := s.sender.Increment(ctx, constants.LastReplicaIDKey, uint64(n))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, newVal-uint64(i))
	}
	return ids, nil
}

func (s *Store) reserveClusterAndRangeID(ctx context.Context) (uint64, uint64, error) {
	metaRangeBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   constants.LastShardIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).ToProto()
	if err != nil {
		return 0, 0, err
	}
	metaRangeRsp, err := s.sender.SyncPropose(ctx, constants.MetaRangePrefix, metaRangeBatch)
	if err != nil {
		return 0, 0, err
	}
	clusterIncrRsp, err := rbuilder.NewBatchResponseFromProto(metaRangeRsp).IncrementResponse(0)
	if err != nil {
		return 0, 0, err
	}
	rangeIDIncrRsp, err := rbuilder.NewBatchResponseFromProto(metaRangeRsp).IncrementResponse(1)
	if err != nil {
		return 0, 0, err
	}
	return clusterIncrRsp.GetValue(), rangeIDIncrRsp.GetValue(), nil
}

func casRangeEdit(key []byte, old, new *rfpb.RangeDescriptor) (*rfpb.CASRequest, error) {
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return nil, err
	}
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return nil, err
	}
	return &rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   key,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}, nil
}

func addLocalRangeEdits(oldRange, newRange *rfpb.RangeDescriptor, b *rbuilder.BatchBuilder) error {
	cas, err := casRangeEdit(constants.LocalRangeKey, oldRange, newRange)
	if err != nil {
		return err
	}
	b.Add(cas)
	return nil
}

func addMetaRangeEdits(oldLeftRange, newLeftRange, newRightRange *rfpb.RangeDescriptor, b *rbuilder.BatchBuilder) error {
	newLeftRangeBuf, err := proto.Marshal(newLeftRange)
	if err != nil {
		return err
	}
	oldLeftRangeBuf, err := proto.Marshal(oldLeftRange)
	if err != nil {
		return err
	}
	newRightRangeBuf, err := proto.Marshal(newRightRange)
	if err != nil {
		return err
	}

	// Send a single request that:
	//  - CAS sets the newLeftRange value to newNewStartBuf
	//  - inserts the new newRightRangeBuf
	//
	// if the CAS fails, check the existing value
	//  if it's generation is past ours, ignore the error, we're out of date
	//  if the existing value already matches what we were trying to set, we're done.
	//  else return an error
	b.Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newRightRange.GetEnd()),
			Value: newRightRangeBuf,
		},
		ExpectedValue: oldLeftRangeBuf,
	}).Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newLeftRange.GetEnd()),
			Value: newLeftRangeBuf,
		},
	})
	return nil
}

func (s *Store) updateMetarange(ctx context.Context, oldStart, start, end *rfpb.RangeDescriptor) error {
	b := rbuilder.NewBatchBuilder()
	if err := addMetaRangeEdits(oldStart, start, end, b); err != nil {
		return err
	}
	batchProto, err := b.ToProto()
	if err != nil {
		return err
	}
	rsp, err := s.Sender().SyncPropose(ctx, keys.RangeMetaKey(end.GetEnd()), batchProto)
	if err != nil {
		return err
	}
	batchRsp := rbuilder.NewBatchResponseFromProto(rsp)
	_, err = batchRsp.CASResponse(0)
	if err != nil {
		return err
	}
	return batchRsp.AnyError()
}

func (s *Store) updateRangeDescriptor(ctx context.Context, shardID uint64, old, new *rfpb.RangeDescriptor) error {
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return err
	}
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return err
	}

	localBatch := rbuilder.NewBatchBuilder()
	if err := addLocalRangeEdits(old, new, localBatch); err != nil {
		return err
	}
	metaRangeDescriptorKey := keys.RangeMetaKey(new.GetEnd())
	metaRangeCasReq := &rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaRangeDescriptorKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}

	metaRangeBatch := rbuilder.NewBatchBuilder()
	metaRangeBatch.Add(metaRangeCasReq)

	mrd := s.sender.GetMetaRangeDescriptor()
	newReplica := new.GetReplicas()[0]
	metaReplica := mrd.GetReplicas()[0]

	txn := rbuilder.NewTxn()
	if newReplica.GetShardId() == metaReplica.GetShardId() {
		localBatch.Add(metaRangeCasReq)
		txn.AddStatement(newReplica, localBatch)
	} else {
		metaRangeBatch := rbuilder.NewBatchBuilder()
		metaRangeBatch.Add(metaRangeCasReq)
		txn.AddStatement(newReplica, localBatch)
		txn = txn.AddStatement(metaReplica, metaRangeBatch)
	}
	return s.txnCoordinator.RunTxn(ctx, txn)
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, shardID, replicaID uint64, nhid string, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		ShardId:   shardID,
		ReplicaId: replicaID,
		Nhid:      proto.String(nhid),
	})
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	newDescriptor.LastAddedReplicaId = proto.Uint64(replicaID)
	newDescriptor.LastReplicaAddedAtUsec = proto.Int64(time.Now().UnixMicro())
	if err := s.updateRangeDescriptor(ctx, shardID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (s *Store) removeReplicaFromRangeDescriptor(ctx context.Context, shardID, replicaID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Replicas {
		if replica.GetReplicaId() == replicaID {
			newDescriptor.Replicas = append(newDescriptor.Replicas[:i], newDescriptor.Replicas[i+1:]...)
			break
		}
	}
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if newDescriptor.GetLastAddedReplicaId() == replicaID {
		newDescriptor.LastAddedReplicaId = nil
		newDescriptor.LastReplicaAddedAtUsec = nil
	}
	if err := s.updateRangeDescriptor(ctx, shardID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (store *Store) scanReplicas(ctx context.Context) {
	scanDelay := store.clock.NewTicker(*replicaScanInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-scanDelay.Chan():
		}
		replicas := store.getLeasedReplicas()
		for _, repl := range replicas {
			if store.driverQueue != nil {
				store.driverQueue.MaybeAdd(repl)
			}
			store.deleteSessionWorker.Enqueue(repl)
		}
	}
}

func bytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func (s *Store) GetReplicaStates(ctx context.Context, rd *rfpb.RangeDescriptor) map[uint64]constants.ReplicaState {
	localReplica, err := s.GetReplica(rd.GetRangeId())
	if err != nil {
		s.log.Errorf("GetReplicaStates failed to get replica(range_id=%d): %s", rd.GetRangeId(), err)
		return nil
	}
	if !s.IsLeader(localReplica.ShardID()) {
		// we are not the leader. we don't know whether the replica is behind
		// or not.
		return nil
	}
	curIndex, err := localReplica.LastAppliedIndex()
	if err != nil {
		s.log.Errorf("ReplicaIsBehind failed to get last applied index(range_id=%d): %s", rd.GetRangeId(), err)
		return nil
	}

	res := make(map[uint64]constants.ReplicaState, 0)
	for _, r := range rd.GetReplicas() {
		if r.GetReplicaId() == localReplica.ReplicaID() {
			res[r.GetReplicaId()] = constants.ReplicaStateCurrent
		} else {
			res[r.GetReplicaId()] = constants.ReplicaStateUnknown
		}
	}
	rd = localReplica.RangeDescriptor()
	header := makeHeader(rd)
	// To read a local key for a replica, we don't need to check whether the
	// replica has lease or not.
	header.ConsistencyMode = rfpb.Header_STALE
	for _, r := range rd.GetReplicas() {
		if r.GetReplicaId() == localReplica.ReplicaID() {
			res[r.GetReplicaId()] = constants.ReplicaStateCurrent
			continue
		}
		client, err := s.apiClient.GetForReplica(ctx, r)
		if err != nil {
			s.log.Errorf("GetReplicaStates failed to get client for replica %+v: %s", r, err)
			continue
		}
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: constants.LastAppliedIndexKey,
		}).ToProto()
		if err != nil {
			s.log.Errorf("GetReplicaStates failed to construct direct read request for replica %+v: %s", r, err)
			continue
		}
		syncResp, err := client.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: header,
			Batch:  readReq,
		})
		if err != nil {
			s.log.Errorf("GetReplicaStates failed to read last index for replica %+v: %s", r, err)
			continue
		}
		batchResp := rbuilder.NewBatchResponseFromProto(syncResp.GetBatch())
		readResponse, err := batchResp.DirectReadResponse(0)
		if err != nil {
			s.log.Errorf("GetReplicaStates failed to parse direct read response for replica %+v: %s", r, err)
			continue
		}
		index := bytesToUint64(readResponse.GetKv().GetValue())
		if index >= curIndex {
			res[r.GetReplicaId()] = constants.ReplicaStateCurrent
		} else {
			res[r.GetReplicaId()] = constants.ReplicaStateBehind
		}
	}

	return res
}
