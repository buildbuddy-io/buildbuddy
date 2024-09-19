package store

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
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
	enableDriver           = flag.Bool("cache.raft.enable_driver", true, "If true, enable placement driver")
	enableTxnCleanup       = flag.Bool("cache.raft.enable_txn_cleanup", true, "If true, clean up stuck transactions periodically")
)

const (
	deleteSessionsRateLimit = 1
	// The number of go routines we use to wait for the replicas to be ready
	readyNumGoRoutines           = 100
	checkReplicaCaughtUpInterval = 1 * time.Second
	maxWaitTimeForReplicaRange   = 30 * time.Second
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

	replicaInitStatusWaiter *replicaStatusWaiter
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
	return NewWithArgs(env, rootDir, nodeHost, gossipManager, sender, registry, raftListener, apiClient, grpcAddr, partitions, db, leaser)
}

func NewWithArgs(env environment.Env, rootDir string, nodeHost *dragonboat.NodeHost, gossipManager interfaces.GossipService, sender *sender.Sender, registry registry.NodeRegistry, listener *listener.RaftListener, apiClient *client.APIClient, grpcAddress string, partitions []disk.Partition, db pebble.IPebbleDB, leaser pebble.Leaser) (*Store, error) {
	nodeLiveness := nodeliveness.New(env.GetServerContext(), nodeHost.ID(), sender)

	nhLog := log.NamedSubLogger(nodeHost.ID())
	eventsChan := make(chan events.Event, 100)

	clock := env.GetClock()
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

		metaRangeMu:   sync.Mutex{},
		metaRangeData: make([]byte, 0),

		db:     db,
		leaser: leaser,
		clock:  clock,
	}

	s.replicaInitStatusWaiter = newReplicaStatusWaiter(env.GetServerContext(), s)

	updateTagsWorker := &updateTagsWorker{
		store:          s,
		tasks:          make(chan *updateTagsTask, 2000),
		lastExecutedAt: time.Now(),
	}

	s.updateTagsWorker = updateTagsWorker

	txnCoordinator := txn.NewCoordinator(s, apiClient, clock)
	s.txnCoordinator = txnCoordinator

	usages, err := usagetracker.New(s.rootDir, s.sender, s.leaser, gossipManager, s.NodeDescriptor(), partitions, clock)

	if *enableDriver {
		s.driverQueue = driver.NewQueue(s, gossipManager, nhLog, clock)
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

	nodeHostInfo := nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	previouslyStartedReplicas := make([]*rfpb.ReplicaDescriptor, 0, len(nodeHostInfo.LogInfo))
	for _, logInfo := range nodeHostInfo.LogInfo {
		if !nodeHost.HasNodeInfo(logInfo.ShardID, logInfo.ReplicaID) {
			// Skip nodes not on this machine.
			continue
		}
		if logInfo.ShardID == constants.MetaRangeID {
			s.log.Infof("Starting metarange replica: %+v", logInfo)
			s.replicaInitStatusWaiter.MarkStarted(logInfo.ShardID)
			rc := raftConfig.GetRaftConfig(logInfo.ShardID, logInfo.ReplicaID)
			if err := nodeHost.StartOnDiskReplica(nil, false /*=join*/, s.ReplicaFactoryFn, rc); err != nil {
				return nil, status.InternalErrorf("failed to start c%dn%d: %s", logInfo.ShardID, logInfo.ReplicaID, err)
			}
			s.configuredClusters++
		} else {
			replicaDescriptor := &rfpb.ReplicaDescriptor{RangeId: logInfo.ShardID, ReplicaId: logInfo.ReplicaID}
			previouslyStartedReplicas = append(previouslyStartedReplicas, replicaDescriptor)
		}
	}

	ctx := context.Background()

	// Scan the metarange and start any clusters we own that have not been
	// removed. If previouslyStartedReplicas is an empty list, then
	// LookupActiveReplicas will return nil, nil, and the following loop
	// will be a no-op.
	activeReplicas, err := s.sender.LookupActiveReplicas(ctx, previouslyStartedReplicas)
	if err != nil {
		return nil, err
	}
	for _, r := range activeReplicas {
		s.replicaInitStatusWaiter.MarkStarted(r.GetRangeId())
		rc := raftConfig.GetRaftConfig(r.GetRangeId(), r.GetReplicaId())
		if err := nodeHost.StartOnDiskReplica(nil, false /*=join*/, s.ReplicaFactoryFn, rc); err != nil {
			return nil, status.InternalErrorf("failed to start c%dn%d: %s", r.GetRangeId(), r.GetReplicaId(), err)
		}
		s.configuredClusters++
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
	s.log.Infof("update meta range to generation %d", new.GetGeneration())
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
		s.leaseKeeper.LeaseCount(ctx),
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
		return replicas[i].RangeID() < replicas[j].RangeID()
	})
	for _, r := range replicas {
		replicaName := fmt.Sprintf("  Shard: %5d   Replica: %5d", r.RangeID(), r.ReplicaID())
		ru, err := r.Usage()
		if err != nil {
			buf += fmt.Sprintf("%s error: %s\n", replicaName, err)
			continue
		}
		isLeader := 0
		if rd := s.lookupRange(r.RangeID()); rd != nil {
			if s.leaseKeeper.HaveLease(ctx, rd.GetRangeId()) {
				isLeader = 1
			}
		}

		buf += fmt.Sprintf("%36s |                | Leader: %4d | QPS (R): %5d | (W): %5d | %s\n",
			replicaName,
			isLeader,
			ru.GetReadQps(),
			ru.GetRaftProposeQps(),
			s.replicaInitStatusWaiter.InitStatus(r.RangeID()),
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
		if *enableTxnCleanup {
			s.txnCoordinator.Start(gctx)
		}
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
	s.log.Info("Store: started to shut down")
	s.dropLeadershipForShutdown()
	now := time.Now()
	defer func() {
		s.log.Infof("Store: shutdown finished in %s", time.Since(now))
	}()

	s.usages.Stop()
	if s.egCancel != nil {
		s.egCancel()
		s.leaseKeeper.Stop()
		s.liveness.Stop()
		s.eg.Wait()
	}
	s.updateTagsWorker.Stop()
	s.replicaInitStatusWaiter.Stop()

	s.log.Info("Store: waitgroups finished")
	s.nodeHost.Close()
	s.log.Info("Store: nodeHost closed")

	if err := s.db.Flush(); err != nil {
		return err
	}
	s.log.Info("Store: db flushed")

	// Wait for all active requests to be finished.
	s.leaser.Close()
	s.log.Info("Store: leaser closed")

	if err := s.db.Close(); err != nil {
		return err
	}
	return grpc_server.GRPCShutdown(ctx, s.grpcServer)
}

func (s *Store) lookupRange(rangeID uint64) *rfpb.RangeDescriptor {
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()

	return s.openRanges[rangeID]
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
		for replicaID := range clusterInfo.Replicas {
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

func (s *Store) GetRange(rangeID uint64) *rfpb.RangeDescriptor {
	return s.lookupRange(rangeID)
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

	s.replicaInitStatusWaiter.WaitReplicaToCatchupInBackground(r)

	s.sendRangeEvent(events.EventRangeAdded, rd)

	if rangelease.ContainsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			s.log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		go s.setMetaRangeBuf(buf)
		go s.gossipManager.SendUserEvent(constants.MetaRangeTag, buf /*coalesce=*/, false)
	}

	s.leaseKeeper.AddRange(rd, r)
	s.updateTagsWorker.Enqueue()
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Removing range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetStart(), rd.GetEnd(), rd.GetGeneration())
	s.replicas.Delete(rd.GetRangeId())

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

func (s *Store) HaveLease(ctx context.Context, rangeID uint64) bool {
	if r, err := s.GetReplica(rangeID); err == nil {
		return s.leaseKeeper.HaveLease(ctx, r.RangeID())
	}
	s.log.Warningf("HaveLease check for unheld range: %d", rangeID)
	return false
}

// LeasedRange verifies that the header is valid and the client is using
// an up-to-date range descriptor. It also checks that a local replica owns
// the range lease for the requested range.
func (s *Store) LeasedRange(ctx context.Context, header *rfpb.Header) (*replica.Replica, error) {
	r, _, err := s.validatedRange(header)
	if err != nil {
		return nil, err
	}

	// Stale reads don't need a lease, so return early.
	if header.GetConsistencyMode() == rfpb.Header_STALE {
		return r, nil
	}

	if s.HaveLease(ctx, header.GetRangeId()) {
		return r, nil
	}
	return nil, status.OutOfRangeErrorf("%s: no lease found for range: %d", constants.RangeLeaseInvalidMsg, header.GetRangeId())
}

func (s *Store) ReplicaFactoryFn(rangeID, replicaID uint64) dbsm.IOnDiskStateMachine {
	r := replica.New(s.leaser, rangeID, replicaID, s, s.events)
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

func (s *Store) GetLeaderID(rangeID uint64) (uint64, bool) {
	leaderID, _, valid, err := s.nodeHost.GetLeaderID(rangeID)
	if err != nil {
		s.log.Debugf("failed to get leader id for range %d: %s", rangeID, err)
		valid = false
	}
	return leaderID, valid
}

func (s *Store) IsLeader(rangeID uint64) bool {
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})
	if nodeHostInfo == nil {
		return false
	}
	for _, clusterInfo := range nodeHostInfo.ShardInfoList {
		if clusterInfo.ShardID == rangeID {
			return clusterInfo.LeaderID == clusterInfo.ReplicaID && clusterInfo.Term > 0
		}
	}
	return false
}

func (s *Store) TransferLeadership(ctx context.Context, req *rfpb.TransferLeadershipRequest) (*rfpb.TransferLeadershipResponse, error) {
	if err := s.nodeHost.RequestLeaderTransfer(req.GetRangeId(), req.GetTargetReplicaId()); err != nil {
		return nil, err
	}
	return &rfpb.TransferLeadershipResponse{}, nil
}

// SnapshotCluster snapshots the cluster *on this node*. This is a local operation and does not
// create a snapshot on other nodes that are members of this cluster.
func (s *Store) SnapshotCluster(ctx context.Context, rangeID uint64) error {
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
			_, err := s.nodeHost.SyncRequestSnapshot(ctx, rangeID, opts)
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
			RangeId:   shardInfo.ShardID,
			ReplicaId: shardInfo.ReplicaID,
		})
	}
	return rsp, nil
}

func (s *Store) getLeasedReplicas(ctx context.Context) []*replica.Replica {
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
		r, err := s.LeasedRange(ctx, header)
		if err != nil {
			continue
		}
		res = append(res, r)
	}
	return res
}

func (s *Store) StartShard(ctx context.Context, req *rfpb.StartShardRequest) (*rfpb.StartShardResponse, error) {
	s.log.Infof("Starting new raft node c%dn%d", req.GetRangeId(), req.GetReplicaId())
	rc := raftConfig.GetRaftConfig(req.GetRangeId(), req.GetReplicaId())
	rc.IsNonVoting = req.GetIsNonVoting()
	err := s.nodeHost.StartOnDiskReplica(req.GetInitialMember(), req.GetJoin(), s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrShardAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return nil, err
	}

	if req.GetLastAppliedIndex() > 0 {
		if err := s.waitForReplicaToCatchUp(ctx, req.GetRangeId(), req.GetLastAppliedIndex()); err != nil {
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
		batchResponse, err := s.session.SyncProposeLocal(ctx, s.nodeHost, req.GetRangeId(), req.GetBatch())
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

func (s *Store) syncRequestDeleteReplica(ctx context.Context, rangeID, replicaID uint64) error {
	configChangeID, err := s.getConfigChangeID(ctx, rangeID)
	if err != nil {
		return status.InternalErrorf("failed to get configChangeID for range %d: %s", rangeID, err)
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		err := s.nodeHost.SyncRequestDeleteReplica(ctx, rangeID, replicaID, configChangeID)
		if err == dragonboat.ErrShardClosed {
			return nil
		}
		return err
	})
	return err
}

// syncRequestStopAndDeleteReplica attempts to delete a replica but stops it if
// the delete fails because this is the last node in the cluster.
func (s *Store) syncRequestStopAndDeleteReplica(ctx context.Context, rangeID, replicaID uint64) error {
	err := s.syncRequestDeleteReplica(ctx, rangeID, replicaID)
	if err == dragonboat.ErrRejected {
		log.Warningf("request to delete replica c%dn%d was rejected, attempting to stop...: %s", rangeID, replicaID, err)
		err := client.RunNodehostFn(ctx, func(ctx context.Context) error {
			err := s.nodeHost.StopReplica(rangeID, replicaID)
			if err == dragonboat.ErrShardClosed {
				return nil
			}
			return err
		})
		if err != nil {
			return status.InternalErrorf("failed to stop replica c%dn%d: %s", rangeID, replicaID, err)
		} else {
			log.Infof("succesfully stopped replica c%dn%d", rangeID, replicaID)
		}
	} else if err != nil {
		return err
	} else {
		log.Infof("succesfully deleted replica c%dn%d", rangeID, replicaID)
	}
	return nil
}

// RemoveData tries to remove all data associated with the specified node (shard, replica). It waits for the node (shard, replica) to be fully offloaded or the context is cancelled. This method should only be used after the node is deleted from its Raft cluster.
func (s *Store) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	err := client.RunNodehostFn(ctx, func(ctx context.Context) error {
		err := s.nodeHost.SyncRemoveData(ctx, req.GetRangeId(), req.GetReplicaId())
		// If the shard is not stopped, we want to retry SyncRemoveData call.
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
	var rangeID uint64
	header := req.GetHeader()

	// Proxied SyncPropose requests don't need a lease, so don't bother
	// checking for one. If the referenced shard is not present on this
	// node, the request will fail in client.SyncProposeLocal().
	if header.GetRangeId() == 0 && header.GetReplica() != nil {
		rangeID = header.GetReplica().GetRangeId()
	} else {
		r, err := s.LeasedRange(ctx, req.GetHeader())
		if err != nil {
			return nil, err
		}
		rangeID = r.RangeID()
	}

	batchResponse, err := s.session.SyncProposeLocal(ctx, s.nodeHost, rangeID, req.GetBatch())
	if err != nil {
		if err == dragonboat.ErrShardNotFound {
			return nil, status.OutOfRangeErrorf("%s: range %d not found", constants.RangeLeaseInvalidMsg, rangeID)
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
		if _, err := s.LeasedRange(ctx, batch.Header); err != nil {
			return nil, err
		}
	} else {
		s.log.Warningf("SyncRead without header: %+v", req)
	}

	rangeID := req.GetHeader().GetReplica().GetRangeId()
	batchResponse, err := client.SyncReadLocal(ctx, s.nodeHost, rangeID, batch)
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
			err := s.liveness.Lease(ctx)
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
	membershipStatusShardNotFound
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
		if errors.Is(err, dragonboat.ErrShardNotFound) {
			return membershipStatusShardNotFound
		}
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
	if membershipStatus == membershipStatusShardNotFound {
		// The shard was already deleted.
		return false
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

	if rd.GetStart() == nil {
		s.log.Debugf("range descriptor for c%dn%d doesn't have start", shardInfo.ShardID, shardInfo.ReplicaID)
		// This could happen in the middle of a split. We mark it as a
		// potential zombie. After *zombieMinDuration, if the range still
		// doesn't have bound, we assume the split failed and will clean
		// up the zombie.
		// Note: due to https://github.com/lni/dragonboat/issues/364, deletion
		// of the last replica of the shard will fail.
		return true
	}
	updatedRD, err := s.Sender().LookupRangeDescriptor(ctx, rd.GetStart(), true /*skip Cache */)
	if err != nil {
		s.log.Errorf("failed to look up range descriptor for c%dn%d: %s", shardInfo.ShardID, shardInfo.ReplicaID, err)
		return false
	}
	if updatedRD.GetGeneration() >= rd.GetGeneration() {
		rd = updatedRD
	}
	for _, r := range rd.GetReplicas() {
		if r.GetRangeId() == shardInfo.ShardID && r.GetReplicaId() == shardInfo.ReplicaID {
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
					err := s.syncRequestStopAndDeleteReplica(ctx, potentialZombie.ShardID, potentialZombie.ReplicaID)
					if err != nil {
						s.log.Warningf("Error stopping and deleting zombie replica c%dn%d: %s", potentialZombie.ShardID, potentialZombie.ReplicaID, err)
						return
					}
					if _, err := s.RemoveData(ctx, &rfpb.RemoveDataRequest{
						RangeId:   potentialZombie.ShardID,
						ReplicaId: potentialZombie.ReplicaID,
					}); err != nil {
						s.log.Errorf("Error removing zombie replica data: %s", err)
					} else {
						s.log.Infof("Successfully removed zombie node: %+v", potentialZombie)
					}
				})
				defer deleteTimer.Stop()
			}
		}
	}
}

func (s *Store) checkIfReplicasNeedSplitting(ctx context.Context) {
	if raftConfig.MaxRangeSizeBytes() == 0 {
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
				rangeID := rangeUsageEvent.RangeDescriptor.GetRangeId()
				if rangeID == constants.MetaRangeID {
					continue
				}
				if !s.leaseKeeper.HaveLease(ctx, rangeID) {
					continue
				}
				estimatedDiskBytes := rangeUsageEvent.ReplicaUsage.GetEstimatedDiskBytesUsed()
				metrics.RaftBytes.With(prometheus.Labels{
					metrics.RaftRangeIDLabel: strconv.Itoa(int(rangeID)),
				}).Set(float64(estimatedDiskBytes))
				if estimatedDiskBytes < raftConfig.MaxRangeSizeBytes() {
					continue
				}
				repl, err := s.GetReplica(rangeID)
				if err != nil {
					s.log.Errorf("failed to get replica with rangeID=%d: %s", rangeID, err)
					continue
				}
				s.driverQueue.MaybeAdd(ctx, repl)
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
				if !s.leaseKeeper.HaveLease(ctx, rangeUsageEvent.RangeDescriptor.GetRangeId()) {
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

func (s *Store) Usage() *rfpb.StoreUsage {
	su := &rfpb.StoreUsage{
		Node: s.NodeDescriptor(),
	}

	s.rangeMu.RLock()
	su.ReplicaCount = int64(len(s.openRanges))
	s.rangeMu.RUnlock()

	su.LeaseCount = s.leaseKeeper.LeaseCount(context.TODO())
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

type replicaInitStatus int

const (
	replicaInitStatusStarted replicaInitStatus = iota
	replicaInitStatusInProgress
	replicaInitStatusCompleted
)

type replicaInitState struct {
	startedAt time.Time
	status    replicaInitStatus
}

type replicaStatusWaiter struct {
	ctx      context.Context
	eg       *errgroup.Group
	egCancel context.CancelFunc

	initStates sync.Map     // map of uint64 rangeID -> replica init state
	mu         sync.RWMutex // protects allReady
	allReady   bool

	store *Store
}

func newReplicaStatusWaiter(ctx context.Context, store *Store) *replicaStatusWaiter {
	ctx, cancelFunc := context.WithCancel(ctx)
	eg, gCtx := errgroup.WithContext(ctx)
	eg.SetLimit(readyNumGoRoutines)
	return &replicaStatusWaiter{
		ctx:      gCtx,
		eg:       eg,
		egCancel: cancelFunc,
		store:    store,
	}
}

func (w *replicaStatusWaiter) MarkStarted(rangeID uint64) error {
	_, ok := w.initStates.Load(rangeID)
	if ok {
		return status.InternalErrorf("rangeID %d has been configured", rangeID)
	}

	state := replicaInitState{
		status:    replicaInitStatusStarted,
		startedAt: time.Now(),
	}
	w.initStates.Store(rangeID, state)
	return nil
}

func (w *replicaStatusWaiter) InitStatus(rangeID uint64) string {
	stateI, loaded := w.initStates.Load(rangeID)
	if !loaded {
		return "unknown"
	}
	state := stateI.(replicaInitState)
	switch state.status {
	case replicaInitStatusStarted:
		return "started"
	case replicaInitStatusInProgress:
		return "in-progress"
	case replicaInitStatusCompleted:
		return "completed"
	}
	return "unknown"
}

func (w *replicaStatusWaiter) WaitReplicaToCatchupInBackground(repl *replica.Replica) error {
	w.mu.RLock()
	allReady := w.allReady
	w.mu.RUnlock()

	if allReady {
		return nil
	}

	stateI, loaded := w.initStates.Load(repl.RangeID())
	if !loaded {
		// This is not a replica that we started from disk. do nothing.
		return nil
	}

	state, ok := stateI.(replicaInitState)
	if !ok {
		alert.UnexpectedEvent("unexpected_replica_init_state_map_type_error")
	}

	if state.status != replicaInitStatusStarted {
		// If the replica is in progress, a goroutine has been started to wait
		// for the index to catch up do nothing. If it is completed, do nothing
		return nil
	}

	w.eg.Go(func() error {
		state.status = replicaInitStatusInProgress
		w.initStates.Store(repl.RangeID(), state)
		w.wait(repl)
		return nil
	})
	return nil
}

func (w *replicaStatusWaiter) wait(repl *replica.Replica) {
	ticker := time.NewTicker(checkReplicaCaughtUpInterval)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			isCurrent, err := w.checkIfReplicaCaughtUp(repl)
			if !isCurrent || err != nil {
				if err != nil {
					log.Debugf("c%dn%d failed to check replica init status: %s", repl.RangeID(), repl.ReplicaID(), err)
				}
				continue
			}
		}
		log.Infof("c%dn%d becomes current", repl.RangeID(), repl.ReplicaID())

		w.initStates.Store(repl.RangeID(), replicaInitState{status: replicaInitStatusCompleted})
		return
	}
}

func (w *replicaStatusWaiter) checkIfReplicaCaughtUp(repl *replica.Replica) (bool, error) {
	rd := repl.RangeDescriptor()
	if w.store.IsLeader(repl.RangeID()) {
		// We are the leader, so we have caught up
		return true, nil
	}

	// If there is a leader, we just need to see if we have caught up with the leader.
	leaderID, valid := w.store.GetLeaderID(repl.RangeID())
	if valid {
		leaderIdx := -1
		for i, r := range rd.GetReplicas() {
			if r.GetReplicaId() == leaderID {
				leaderIdx = i
			}
		}
		if leaderIdx != -1 {
			leaderLastAppliedIdx, err := w.store.GetRemoteLastAppliedIndex(w.ctx, rd, leaderIdx)
			if err != nil {
				return false, err
			}
			curIndex, err := repl.LastAppliedIndex()
			if err != nil {
				return false, err
			}
			return curIndex >= leaderLastAppliedIdx, nil
		}
	}

	// Currently, there is no leader, so we need to compare ourselves with other
	// replicas.
	lastAppliedIndices := w.store.GetRemoteLastAppliedIndices(w.ctx, rd, repl)
	curIndex, err := repl.LastAppliedIndex()
	if err != nil {
		return false, err
	}

	for _, r := range rd.GetReplicas() {
		if r.GetReplicaId() == repl.ReplicaID() {
			continue
		}
		idx, ok := lastAppliedIndices[r.GetReplicaId()]
		if !ok {
			return false, status.UnavailableErrorf("miss last_applied_index for c%dn%d", r.GetRangeId(), r.GetReplicaId())
		}
		if curIndex < idx {
			return false, nil
		}
	}
	return true, nil
}

func (w *replicaStatusWaiter) Done() bool {
	w.mu.RLock()
	allReady := w.allReady
	w.mu.RUnlock()

	if allReady {
		return true
	}

	allReady = true
	w.initStates.Range(func(key, value any) bool {
		if state, ok := value.(replicaInitState); ok {
			if state.status == replicaInitStatusCompleted {
				return true
			}

			if time.Since(state.startedAt) > maxWaitTimeForReplicaRange {
				// The time to wait for AddRange call with a bound from the replica exceeded the threshold. Let's consider the replica to be ready, since it's likely a zombie.
				return true
			}

			allReady = false
			log.Infof("rangeID %d is not ready", key.(uint64))
			return false
		}
		return true
	})

	if allReady {
		w.mu.Lock()
		w.allReady = true
		w.mu.Unlock()
		return true
	}
	return false
}

func (w *replicaStatusWaiter) Stop() {
	if w.egCancel != nil {
		w.egCancel()
		w.eg.Wait()
	}
	if err := w.eg.Wait(); err != nil {
		log.Errorf("failed to stop replicaStatusWaiter: %s", err.Error())
	}
	log.Infof("replicaStatusWaiter stopped")
}

func (s *Store) ReplicasInitDone() bool {
	return s.replicaInitStatusWaiter.Done()
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

	if !w.store.HaveLease(ctx, rd.GetRangeId()) {
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
	rsp, err := w.session.SyncProposeLocal(ctx, w.store.NodeHost(), repl.RangeID(), request)
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
	rangeID := leftRange.GetRangeId()

	// Reserve new IDs for this cluster.
	newRangeID, err := s.reserveClusterAndRangeID(ctx)
	if err != nil {
		return nil, status.InternalErrorf("could not reserve IDs for new cluster: %s", err)
	}

	// Find Split Point.
	fsp := rbuilder.NewBatchBuilder().Add(&rfpb.FindSplitPointRequest{}).SetHeader(req.GetHeader())
	fspRsp, err := client.SyncReadLocalBatch(ctx, s.nodeHost, rangeID, fsp)
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
		r.RangeId = newRangeID
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
		grpcAddr, _, err := s.registry.ResolveGRPC(r.GetRangeId(), r.GetReplicaId())
		if err != nil {
			return nil, err
		}
		servers[r.GetNhid()] = grpcAddr
	}
	bootstrapInfo := bringup.MakeBootstrapInfo(newRangeID, 1, servers)
	log.Debugf("StartShard called with bootstrapInfo: %+v", bootstrapInfo)
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
	txn := rbuilder.NewTxn().AddStatement(leftRange, leftBatch)
	txn = txn.AddStatement(newRightRange, rightBatch)
	txn = txn.AddStatement(mrd, metaBatch)
	if err := s.txnCoordinator.RunTxn(ctx, txn); err != nil {
		return nil, err
	}

	// Observe split duration.
	metrics.RaftSplitDurationUs.With(prometheus.Labels{
		metrics.RaftRangeIDLabel: strconv.Itoa(int(leftRange.GetRangeId())),
	}).Observe(float64(time.Since(startTime).Microseconds()))

	return &rfpb.SplitRangeResponse{
		Left:  updatedLeftRange,
		Right: newRightRange,
	}, nil
}

// getLocalLastAppliedIndex returns the last applied index of the replica of a
// given range on the store
func (s *Store) getLocalLastAppliedIndex(header *rfpb.Header) (uint64, error) {
	r, _, err := s.validatedRange(header)
	if err != nil {
		return 0, err
	}
	return r.LastAppliedIndex()
}

func (s *Store) GetRemoteLastAppliedIndex(ctx context.Context, rd *rfpb.RangeDescriptor, replicaIdx int) (uint64, error) {
	r := rd.GetReplicas()[replicaIdx]
	client, err := s.apiClient.GetForReplica(ctx, r)
	if err != nil {
		return 0, status.UnavailableErrorf("failed to get client: %s", err)
	}
	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: constants.LastAppliedIndexKey,
	}).ToProto()
	if err != nil {
		return 0, status.InternalErrorf("failed to construct direct read request: %s", err)
	}
	// To read a local key for a replica, we don't need to check whether the
	// replica has lease or not.
	header := header.New(rd, replicaIdx, rfpb.Header_STALE)
	syncResp, err := client.SyncRead(ctx, &rfpb.SyncReadRequest{
		Header: header,
		Batch:  readReq,
	})
	if err != nil {
		return 0, status.InternalErrorf("failed to read last index: %s", err)
	}
	batchResp := rbuilder.NewBatchResponseFromProto(syncResp.GetBatch())
	readResponse, err := batchResp.DirectReadResponse(0)
	if err != nil {
		return 0, status.InternalErrorf("failed to parse direct read response: %s", err)
	}
	index := bytesToUint64(readResponse.GetKv().GetValue())
	return index, nil
}

// GetRemoteLastAppliedIndices queries remote stores to get a map from replica
// ID to last applied indices.
func (s *Store) GetRemoteLastAppliedIndices(ctx context.Context, rd *rfpb.RangeDescriptor, localReplica *replica.Replica) map[uint64]uint64 {
	res := make(map[uint64]uint64)
	for i, r := range rd.GetReplicas() {
		if r.GetReplicaId() == localReplica.ReplicaID() {
			continue
		}
		index, err := s.GetRemoteLastAppliedIndex(ctx, rd, i)
		if err != nil {
			s.log.Errorf("failed to get remote last applied index for replica %+v: %s", r, err)
			continue
		}
		res[r.GetReplicaId()] = index
	}
	return res
}

func (s *Store) waitForReplicaToCatchUp(ctx context.Context, rangeID uint64, desiredLastAppliedIndex uint64) error {
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue with for loop
		}
		if r, err := s.GetReplica(rangeID); err == nil {
			if lastApplied, err := r.LastAppliedIndex(); err == nil {
				if lastApplied >= desiredLastAppliedIndex {
					s.log.Infof("Range %d took %s to catch up", rangeID, time.Since(start))
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (s *Store) getMembership(ctx context.Context, rangeID uint64) (*dragonboat.Membership, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		// Get the config change index for this cluster.
		membership, err = s.nodeHost.SyncGetShardMembership(ctx, rangeID)
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
		return nil, status.InternalErrorf("nil cluster membership for range: %d", rangeID)
	}
	return membership, nil
}

func (s *Store) getConfigChangeID(ctx context.Context, rangeID uint64) (uint64, error) {
	membership, err := s.getMembership(ctx, rangeID)
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

	rangeID := req.GetRange().GetRangeId()

	// Reserve a new node ID for the node about to be added.
	replicaIDs, err := s.reserveReplicaIDs(ctx, 1)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to reserveReplicaIDs: %s", err)
	}
	newReplicaID := replicaIDs[0]

	// Get the config change index for adding a non-voter.
	configChangeID, err := s.getConfigChangeID(ctx, rangeID)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get config change ID for adding a non-voter: %s", err)
	}
	lastAppliedIndex, err := s.getLocalLastAppliedIndex(&rfpb.Header{
		RangeId:    rd.GetRangeId(),
		Generation: rd.GetGeneration(),
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get last applied index: %s", err)
	}

	// Gossip the address of the node that is about to be added.
	s.registry.Add(rangeID, newReplicaID, node.GetNhid())
	s.registry.AddNode(node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node as a non-voter to the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddNonVoting(ctx, rangeID, newReplicaID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to call nodeHost.SyncRequestAddNonVoting: %s", err)
	}

	// Start the cluster on the newly added node.
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get the client for the node: %s", err)
	}
	_, err = c.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:          rangeID,
		ReplicaId:        newReplicaID,
		Join:             true,
		LastAppliedIndex: lastAppliedIndex,
		IsNonVoting:      true,
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to start shard: %s", err)
	}

	// StartShard ensures the node is up-to-date. We can promote non-voter to voter.
	// Propose the config change (this adds the node as a non-voter to the raft cluster).

	// Get the config change index for promoting a non-voter to a voter
	configChangeID, err = s.getConfigChangeID(ctx, rangeID)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to get config change ID for adding a non-voter: %s", err)
	}
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddReplica(ctx, rangeID, newReplicaID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to promote non-voter to voter: %s", err)
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	rd, err = s.addReplicaToRangeDescriptor(ctx, rangeID, newReplicaID, node.GetNhid(), rd)
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
	rd, err := s.removeReplicaFromRangeDescriptor(ctx, replicaDesc.GetRangeId(), replicaDesc.GetReplicaId(), req.GetRange())
	if err != nil {
		return nil, err
	}

	if err = s.syncRequestDeleteReplica(ctx, replicaDesc.GetRangeId(), replicaDesc.GetReplicaId()); err != nil {
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
		RangeId:   replicaDesc.GetRangeId(),
		ReplicaId: replicaDesc.GetReplicaId(),
	})
	if err != nil {
		s.log.Warningf("RemoveReplica unable to remove data err: %s", err)
		return rsp, nil
	}

	s.log.Infof("Removed shard: c%dn%d", replicaDesc.GetRangeId(), replicaDesc.GetReplicaId())
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

func (s *Store) reserveClusterAndRangeID(ctx context.Context) (uint64, error) {
	metaRangeBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).ToProto()
	if err != nil {
		return 0, err
	}
	metaRangeRsp, err := s.sender.SyncPropose(ctx, constants.MetaRangePrefix, metaRangeBatch)
	if err != nil {
		return 0, err
	}
	rangeIDIncrRsp, err := rbuilder.NewBatchResponseFromProto(metaRangeRsp).IncrementResponse(0)
	if err != nil {
		return 0, err
	}
	return rangeIDIncrRsp.GetValue(), nil
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

func (s *Store) updateRangeDescriptor(ctx context.Context, rangeID uint64, old, new *rfpb.RangeDescriptor) error {
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
	if newReplica.GetRangeId() == metaReplica.GetRangeId() {
		localBatch.Add(metaRangeCasReq)
		txn.AddStatement(mrd, localBatch)
	} else {
		metaRangeBatch := rbuilder.NewBatchBuilder()
		metaRangeBatch.Add(metaRangeCasReq)
		txn.AddStatement(new, localBatch)
		txn = txn.AddStatement(mrd, metaRangeBatch)
	}
	err = s.txnCoordinator.RunTxn(ctx, txn)
	if err != nil {
		return status.InternalErrorf("failed to update range descriptor for rangeID=%d, err: %s", rangeID, err)
	}
	return nil
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, rangeID, replicaID uint64, nhid string, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		RangeId:   rangeID,
		ReplicaId: replicaID,
		Nhid:      proto.String(nhid),
	})
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	newDescriptor.LastAddedReplicaId = proto.Uint64(replicaID)
	newDescriptor.LastReplicaAddedAtUsec = proto.Int64(time.Now().UnixMicro())
	if err := s.updateRangeDescriptor(ctx, rangeID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (s *Store) removeReplicaFromRangeDescriptor(ctx context.Context, rangeID, replicaID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
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
	if err := s.updateRangeDescriptor(ctx, rangeID, oldDescriptor, newDescriptor); err != nil {
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
		replicas := store.getLeasedReplicas(ctx)
		for _, repl := range replicas {
			if store.driverQueue != nil {
				store.driverQueue.MaybeAdd(ctx, repl)
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
	if !s.IsLeader(localReplica.RangeID()) {
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
	indicesByReplicaID := s.GetRemoteLastAppliedIndices(ctx, rd, localReplica)
	for replicaID, index := range indicesByReplicaID {
		if index >= curIndex {
			res[replicaID] = constants.ReplicaStateCurrent
		} else {
			res[replicaID] = constants.ReplicaStateBehind
		}
	}
	return res
}

func (s *Store) TestingWaitForGC() {
	s.usages.TestingWaitForGC()
}

func (s *Store) TestingFlush() {
	s.db.Flush()
}
