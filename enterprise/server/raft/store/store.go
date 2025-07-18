package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/elastic/gosigar"
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
	enableRegistryPreload  = flag.Bool("cache.raft.enable_registry_preload", false, "If true, preload the registry on start-up")
)

const (
	deleteSessionsRateLimit      = 1
	removeZombieRateLimit        = 1
	numReplicaStarter            = 50
	checkReplicaCaughtUpInterval = 1 * time.Second
	maxWaitTimeForReplicaRange   = 30 * time.Second
	metricsRefreshPeriod         = 30 * time.Second

	// listenerID for replicaStatusWaiter
	listenerID = "replicaStatusWaiter"
)

type LogDBConfigType int

const (
	SmallMemLogDBConfigType LogDBConfigType = iota
	LargeMemLogDBConfigType
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
	// This session is used by most of the SyncPropose traffic
	session *client.Session
	// The following sessions are created so that we can seperate background
	// traffic such as eviction, startShard, splitRange from the main write
	// traffic.
	// session for transactions; used by split, add and remove replica.
	txnSession *client.Session
	// session for eviction
	evictionSession *client.Session
	// session for StartShard
	shardStarterSession *client.Session

	log log.Logger

	db     pebble.IPebbleDB
	leaser pebble.Leaser

	configuredClusters atomic.Int32

	rangeMu    sync.RWMutex
	openRanges map[uint64]*rfpb.RangeDescriptor
	rangeMap   *rangemap.RangeMap[*rfpb.RangeDescriptor]

	leaseKeeper *leasekeeper.LeaseKeeper
	replicas    sync.Map // map of uint64 rangeID -> *replica.Replica

	eventsMu       sync.Mutex
	events         chan events.Event
	eventListeners map[string]chan events.Event

	usages *usagetracker.Tracker

	metaRangeMu   sync.Mutex
	metaRangeData []byte

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel context.CancelFunc

	updateTagsWorker *updateTagsWorker
	txnCoordinator   *txn.Coordinator

	driverQueue         *driver.Queue
	deleteSessionWorker *deleteSessionWorker
	replicaJanitor      *replicaJanitor

	clock clockwork.Clock

	replicaInitStatusWaiter *replicaStatusWaiter

	oldMetrics       pebble.Metrics
	metricsCollector *pebble.MetricsCollector

	mu      sync.Mutex // protects stopped
	stopped bool

	maxSingleOpTimeout time.Duration
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
	nhLog := log.NamedSubLogger(nhid)
	r := registry.NewDynamicNodeRegistry(rc.g, streamConnections, v, nhLog)
	rc.r = r
	r.AddNode(nhid, rc.raftAddr, rc.grpcAddr)
	return r, nil
}

func getLogDbConfig(t LogDBConfigType) dbConfig.LogDBConfig {
	switch t {
	case SmallMemLogDBConfigType:
		return dbConfig.GetSmallMemLogDBConfig()
	case LargeMemLogDBConfigType:
		return dbConfig.GetLargeMemLogDBConfig()
	default:
		alert.UnexpectedEvent("unknown-raft-log-db-config-type", "unknown type: %d", t)
	}
	return dbConfig.GetDefaultLogDBConfig()
}

func New(env environment.Env, rootDir, raftAddr, grpcAddr, grpcListeningAddr string, partitions []disk.Partition, logDBConfigType LogDBConfigType) (*Store, error) {
	rangeCache := rangecache.New()
	raftListener := listener.NewRaftListener()
	gossipManager := env.GetGossipService()
	regHolder := &registryHolder{raftAddr, grpcAddr, gossipManager, nil}
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(rootDir, "wal"),
		NodeHostDir:    filepath.Join(rootDir, "nodehost"),
		RTTMillisecond: constants.RTTMillisecond,
		RaftAddress:    raftAddr,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: regHolder,
			LogDB:               getLogDbConfig(logDBConfigType),
		},
		RaftEventListener:   raftListener,
		SystemEventListener: raftListener,
		EnableMetrics:       true,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	registry := regHolder.r
	apiClient := client.NewAPIClient(env, nodeHost.ID(), registry)
	sender := sender.New(rangeCache, apiClient)
	mc := &pebble.MetricsCollector{}
	db, err := pebble.Open(rootDir, "raft_store", &pebble.Options{
		EventListener: &pebble.EventListener{
			WriteStallBegin: mc.WriteStallBegin,
			WriteStallEnd:   mc.WriteStallEnd,
			DiskSlow:        mc.DiskSlow,
		},
	})
	if err != nil {
		return nil, err
	}
	leaser := pebble.NewDBLeaser(db)
	return NewWithArgs(env, rootDir, nodeHost, gossipManager, sender, registry, raftListener, apiClient, grpcAddr, grpcListeningAddr, partitions, db, leaser, mc)
}

func NewWithArgs(env environment.Env, rootDir string, nodeHost *dragonboat.NodeHost, gossipManager interfaces.GossipService, sender *sender.Sender, registry registry.NodeRegistry, listener *listener.RaftListener, apiClient *client.APIClient, grpcAddress, grpcListeningAddr string, partitions []disk.Partition, db pebble.IPebbleDB, leaser pebble.Leaser, mc *pebble.MetricsCollector) (*Store, error) {
	nodeLiveness := nodeliveness.New(env.GetServerContext(), nodeHost.ID(), sender)

	nhLog := log.NamedSubLogger(nodeHost.ID())
	eventsChan := make(chan events.Event, 1000)

	clock := env.GetClock()
	session := client.NewSessionWithClock(clock)
	txnSession := client.NewSessionWithClock(clock)
	evictionSession := client.NewSessionWithClock(clock)
	shardStarterSession := client.NewSessionWithClock(clock)
	lkSession := client.NewSessionWithClock(clock)

	s := &Store{
		env:                 env,
		rootDir:             rootDir,
		grpcAddr:            grpcAddress,
		nodeHost:            nodeHost,
		partitions:          partitions,
		gossipManager:       gossipManager,
		sender:              sender,
		registry:            registry,
		apiClient:           apiClient,
		liveness:            nodeLiveness,
		session:             session,
		txnSession:          txnSession,
		evictionSession:     evictionSession,
		shardStarterSession: shardStarterSession,
		log:                 nhLog,

		rangeMu:    sync.RWMutex{},
		openRanges: make(map[uint64]*rfpb.RangeDescriptor),
		rangeMap:   rangemap.New[*rfpb.RangeDescriptor](),

		leaseKeeper: leasekeeper.New(nodeHost, nhLog, nodeLiveness, listener, eventsChan, lkSession),
		replicas:    sync.Map{},

		eventsMu:       sync.Mutex{},
		events:         eventsChan,
		eventListeners: make(map[string]chan events.Event),

		metaRangeMu:   sync.Mutex{},
		metaRangeData: make([]byte, 0),

		db:                 db,
		leaser:             leaser,
		clock:              clock,
		metricsCollector:   mc,
		maxSingleOpTimeout: raftConfig.SingleRaftOpTimeout(),
	}

	s.replicaInitStatusWaiter = newReplicaStatusWaiter(listener, nhLog)

	updateTagsWorker := &updateTagsWorker{
		store:          s,
		tasks:          make(chan *updateTagsTask, 2000),
		lastExecutedAt: time.Now(),
	}

	s.updateTagsWorker = updateTagsWorker

	txnCoordinator := txn.NewCoordinator(s, apiClient, clock)
	s.txnCoordinator = txnCoordinator

	usages, err := usagetracker.New(s.sender, s.leaser, gossipManager, s.NodeDescriptor(), partitions, clock)

	if *enableDriver {
		s.driverQueue = driver.NewQueue(s, gossipManager, nhLog, apiClient, clock)
	}
	s.deleteSessionWorker = newDeleteSessionsWorker(clock, s, *clientSessionTTL)
	s.replicaJanitor = newReplicaJanitor(clock, s, *zombieNodeScanInterval, *zombieMinDuration)

	if err != nil {
		return nil, err
	}
	s.usages = usages

	grpcOptions := grpc_server.CommonGRPCServerOptions(s.env)
	s.grpcServer = grpc.NewServer(grpcOptions...)
	reflection.Register(s.grpcServer)
	grpc_server.Metrics().InitializeMetrics(s.grpcServer)
	rfspb.RegisterApiServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", grpcListeningAddr)
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

	ctx, cancelFunc := context.WithCancel(context.Background())
	s.egCancel = cancelFunc

	eg, gctx := errgroup.WithContext(ctx)
	s.eg = eg
	s.egCtx = gctx
	eg.Go(func() error {
		s.queryForMetarange(gctx)
		return nil
	})
	eg.Go(func() error {
		s.replicaInitStatusWaiter.Start(gctx)
		return nil
	})
	nodeHostInfo := nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})

	egStarter := &errgroup.Group{}
	egStarter.SetLimit(numReplicaStarter)
	numReplicas := len(nodeHostInfo.LogInfo)
	for i, logInfo := range nodeHostInfo.LogInfo {
		i, logInfo := i, logInfo
		if !nodeHost.HasNodeInfo(logInfo.ShardID, logInfo.ReplicaID) {
			// Skip nodes not on this machine.
			continue
		}
		egStarter.Go(func() error {
			start := time.Now()
			s.log.Infof("Replica c%dn%d is active. (%d/%d)", logInfo.ShardID, logInfo.ReplicaID, i+1, numReplicas)
			rc := raftConfig.GetRaftConfig(logInfo.ShardID, logInfo.ReplicaID)
			s.replicaInitStatusWaiter.MarkStarted(logInfo.ShardID, logInfo.ReplicaID)
			if err := nodeHost.StartOnDiskReplica(nil, false /*=join*/, s.ReplicaFactoryFn, rc); err != nil {
				if err == dragonboat.ErrReplicaRemoved {
					s.log.Debugf("Replica c%dn%d was removed in the past, skipping.", logInfo.ShardID, logInfo.ReplicaID)
					return nil
				}
				return status.InternalErrorf("failed to start c%dn%d: %s", logInfo.ShardID, logInfo.ReplicaID, err)
			}
			s.configuredClusters.Add(1)
			s.log.Infof("Recreated c%dn%d in %s. (%d/%d)", logInfo.ShardID, logInfo.ReplicaID, time.Since(start), i+1, numReplicas)
			return nil
		})
	}

	err = egStarter.Wait()

	ctx = context.Background()
	if *enableRegistryPreload {
		s.log.Debug("Start to preloadRegistry")
		err := s.preloadRegistry(ctx)
		if err != nil {
			s.log.Debugf("preloadRegistry failed: %s", err)
		}
		s.log.Debug("preloadRegistry finished")
	}
	if err != nil {
		return nil, err
	}

	gossipManager.AddListener(s)
	statusz.AddSection("raft_store", "Store", s)

	// Whenever we bring up a brand new store with no ranges, we need to inform
	// other stores about its existence using store_usage tag, in order to make
	// it a potentia replication target.
	updateTagsWorker.Enqueue()

	return s, nil
}

func (s *Store) preloadRegistry(ctx context.Context) error {
	localMember := s.gossipManager.LocalMember()
	members := s.gossipManager.Members()

	eg := &errgroup.Group{}
	for _, m := range members {
		if m.Status != serf.StatusAlive {
			continue
		}
		if m.Name == localMember.Name {
			continue
		}
		addr, ok := m.Tags[constants.GRPCAddressTag]
		if !ok {
			s.log.Warningf("not pre-load registry from member %s, because grpc address tag doesn't exist", m.Addr.String())
			continue
		}
		eg.Go(func() error {
			c, err := s.apiClient.Get(ctx, addr)
			if err != nil {
				s.log.Errorf("preloadRegistry: unable to get client for addr (%q): %s", addr, err)
				return nil
			}
			rsp, err := c.GetRegistry(ctx, &rfpb.GetRegistryRequest{})
			if err != nil {
				s.log.Errorf("preloadRegistry: unable to get registry from addr (%q): %s", addr, err)
				return nil
			}
			for _, conn := range rsp.GetConnections() {
				if conn.GetNhid() == "" {
					s.log.Warningf("preloadRegistry ignoring malformed connection: %+v", conn)
					continue
				}
				s.registry.AddNode(conn.GetNhid(), conn.GetRaftAddress(), conn.GetGrpcAddress())
			}
			return nil
		})
	}

	return eg.Wait()
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
	if len(buf) == 0 {
		return
	}
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
func (s *Store) GetRangeDebugInfo(ctx context.Context, req *rfpb.GetRangeDebugInfoRequest) (*rfpb.GetRangeDebugInfoResponse, error) {
	leaderID, term, valid, _ := s.nodeHost.GetLeaderID(req.GetRangeId())
	rsp := &rfpb.GetRangeDebugInfoResponse{
		Nhid:            s.NHID(),
		RangeDescriptor: s.lookupRange(req.GetRangeId()),
		HasLease:        s.leaseKeeper.HaveLease(ctx, req.GetRangeId()),
		Leader: &rfpb.RaftLeaderInfo{
			LeaderId: leaderID,
			Term:     term,
			Valid:    valid,
		},
	}
	// Fetch the range descriptor from meta range to make sure it's the most-up-to-date.
	ranges, err := s.sender.LookupRangeDescriptorsByIDs(ctx, []uint64{req.GetRangeId()})
	if err != nil {
		s.log.Errorf("GetRangeDebugInfo failed to look up range descriptor from meta range: %s", err)
	} else if len(ranges) > 0 {
		rsp.RangeDescriptorInMetaRange = ranges[0]
	}
	membership, err := s.GetMembership(ctx, req.GetRangeId())
	if err != nil {
		return rsp, err
	}

	membershipRsp := &rfpb.RaftMembership{}
	for replicaID, addr := range membership.Nodes {
		membershipRsp.Voters = append(membershipRsp.Voters, &rfpb.ReplicaDescriptor{
			RangeId:   req.GetRangeId(),
			ReplicaId: replicaID,
			Nhid:      proto.String(addr),
		})
	}
	for replicaID, addr := range membership.NonVotings {
		membershipRsp.NonVoters = append(membershipRsp.NonVoters, &rfpb.ReplicaDescriptor{
			RangeId:   req.GetRangeId(),
			ReplicaId: replicaID,
			Nhid:      proto.String(addr),
		})
	}
	for replicaID, addr := range membership.Witnesses {
		membershipRsp.Witnesses = append(membershipRsp.Witnesses, &rfpb.ReplicaDescriptor{
			RangeId:   req.GetRangeId(),
			ReplicaId: replicaID,
			Nhid:      proto.String(addr),
		})
	}
	for replicaID := range membership.Removed {
		membershipRsp.Removed = append(membershipRsp.Removed, replicaID)
	}
	rsp.Membership = membershipRsp
	return rsp, nil
}

func (s *Store) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += s.liveness.String() + "\n"

	su := s.Usage()
	buf += fmt.Sprintf("%36s | Replicas: %4d | Leases: %7d | QPS (R): %5d | (W): %5d | Size: %d MB\n",
		su.GetNode().GetNhid(),
		su.GetReplicaCount(),
		su.GetLeaseCount(),
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
		haveLease := 0
		isLeader := 0
		if s.isLeader(r.RangeID(), r.ReplicaID()) {
			isLeader = 1
		}
		numStaging := 0
		numRemoving := 0
		if rd := s.lookupRange(r.RangeID()); rd != nil {
			if s.leaseKeeper.HaveLease(ctx, rd.GetRangeId()) {
				haveLease = 1
			}
			numStaging = len(rd.GetStaging())
			numRemoving = len(rd.GetRemoved())
		}

		buf += fmt.Sprintf("%36s | Leader: %6d | HaveLease: %4d | QPS (R): %5d | (W): %5d | # Staging: %4d | # Removing: %4d | %s\n",
			replicaName,
			isLeader,
			haveLease,
			ru.GetReadQps(),
			ru.GetRaftProposeQps(),
			numStaging,
			numRemoving,
			s.replicaInitStatusWaiter.InitStatus(r.RangeID(), r.ReplicaID()),
		)
	}
	buf += s.usages.Statusz(ctx)
	buf += "</pre>"
	return buf
}

func (s *Store) handleEvents(ctx context.Context) {
	for {
		select {
		case e := <-s.events:
			s.eventsMu.Lock()
			for id, ch := range s.eventListeners {
				select {
				case ch <- e:
					continue
				default:
					metrics.RaftStoreEventListenerDropped.With(prometheus.Labels{
						metrics.RaftListenerID: id,
						metrics.RaftEventType:  e.EventType().String(),
					}).Inc()
					s.log.Warningf("EventListener(%s) dropped event: %s", id, e)
				}
			}
			s.eventsMu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) AddEventListener(id string) <-chan events.Event {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()

	ch := make(chan events.Event, 1000)
	s.eventListeners[id] = ch
	return ch
}

// Start starts a new grpc server which exposes an API that can be used to manage
// ranges on this node.
func (s *Store) Start() error {
	s.usages.Start()
	if s.driverQueue != nil {
		s.driverQueue.Start()
	}
	s.eg.Go(func() error {
		s.handleEvents(s.egCtx)
		return nil
	})
	s.eg.Go(func() error {
		s.acquireNodeLiveness(s.egCtx)
		return nil
	})
	s.eg.Go(func() error {
		s.replicaJanitor.Start(s.egCtx)
		return nil
	})

	if maxRangeSizeBytes := raftConfig.MaxRangeSizeBytes(); maxRangeSizeBytes != 0 {
		s.eg.Go(func() error {
			s.checkIfReplicasNeedSplitting(s.egCtx, maxRangeSizeBytes)
			return nil
		})
	}
	s.eg.Go(func() error {
		s.updateStoreUsageTag(s.egCtx)
		return nil
	})
	s.eg.Go(func() error {
		s.refreshMetrics(s.egCtx)
		return nil
	})
	if *enableTxnCleanup {
		s.eg.Go(func() error {
			s.txnCoordinator.Start(s.egCtx)
			return nil
		})
	}
	if scanInterval := *replicaScanInterval; scanInterval != 0 {
		s.eg.Go(func() error {
			s.scanReplicas(s.egCtx, scanInterval)
			return nil
		})
	}
	s.eg.Go(func() error {
		s.deleteSessionWorker.Start(s.egCtx)
		return nil
	})
	return nil
}

func (s *Store) Stop(ctx context.Context) error {
	s.log.Info("Store: started to shut down")
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	if s.driverQueue != nil {
		s.driverQueue.Stop()
	}
	s.dropLeadershipForShutdown(ctx)
	s.log.Info("Store: dropped leadership for shutdown")
	now := time.Now()
	defer func() {
		s.log.Infof("Store: shutdown finished in %s", time.Since(now))
	}()

	s.usages.Stop()
	if s.egCancel != nil {
		s.egCancel()
		// Liveness should be shutdown before leasekeeper; Otherwise,
		// liveness.ensureValidLease can keep sending requests to the store; and
		// the leasekeeper can keep queuing the lease instructions; and thus
		// leaseAgent.runloop unable to finish in time.
		s.liveness.Stop()
		s.leaseKeeper.Stop()
		s.eg.Wait()
		s.log.Info("Store: waitgroups finished")
	}
	s.updateTagsWorker.Stop()

	s.nodeHost.Close()
	s.log.Info("Store: nodehost closed")

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

// findTargetReplicaIDForLeadershipTransfer finds a target replica ID to tranfer
// leader to. Returns 0 if no such replica is found.
func (s *Store) findTargetReplicaIDForLeadershipTransfer(ctx context.Context, rd *rfpb.RangeDescriptor, fromReplicaID uint64) uint64 {
	// Pick the first node in the map that isn't us. Map ordering is
	// random; which is a good thing, it means we're randomly picking
	// another node in the cluster and requesting they take the lead.
	targetReplicaID := uint64(0)
	backupReplicaID := uint64(0)
	for _, repl := range rd.GetReplicas() {
		if repl.GetReplicaId() == fromReplicaID {
			continue
		}
		if connReady, err := s.apiClient.HaveReadyConnections(ctx, repl); err != nil || !connReady {
			// During rollout, after a machine restarted, it takes some time
			// for the connections to that machine to be re-established. If
			// we move leader to such machines, it can cause
			// sender.SyncPropose to retry until the connections are
			// re-established. To prevent such scenerios, we don't want to
			// move leaders to such machines if possible.
			backupReplicaID = repl.GetReplicaId()
			continue
		}
		targetReplicaID = repl.GetReplicaId()
		break
	}

	if targetReplicaID == 0 {
		log.Debugf("cannot find a replica with ready connections to transfer leadership to for shard %d, choose a random replica: %d", rd.GetRangeId(), backupReplicaID)
		targetReplicaID = backupReplicaID
	}
	return targetReplicaID
}

func (s *Store) dropLeadershipForShutdown(ctx context.Context) {
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

		rd := s.GetRange(clusterInfo.ShardID)
		targetReplicaID := s.findTargetReplicaIDForLeadershipTransfer(ctx, rd, clusterInfo.ReplicaID)

		if targetReplicaID != 0 {
			eg.Go(func() error {
				log.Debugf("request to transfer leadership of shard %d to replica %d from replica %d", clusterInfo.ShardID, targetReplicaID, clusterInfo.ReplicaID)
				if err := s.nodeHost.RequestLeaderTransfer(clusterInfo.ShardID, targetReplicaID); err != nil {
					s.log.Warningf("Error transferring leadership: %s", err)
				}
				return nil
			})
		}
	}
	eg.Wait()
}

func (s *Store) GetRange(rangeID uint64) *rfpb.RangeDescriptor {
	return s.lookupRange(rangeID)
}

func (s *Store) UpdateRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Update range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetStart(), rd.GetEnd(), rd.GetGeneration())
	_, loaded := s.replicas.LoadOrStore(rd.GetRangeId(), r)

	var err error
	added := false
	s.rangeMu.Lock()
	s.openRanges[rd.GetRangeId()] = rd

	if rd.GetStart() != nil && rd.GetEnd() != nil {
		checkFn := func(v *rfpb.RangeDescriptor) bool {
			return v.GetGeneration() < rd.GetGeneration()
		}
		added, err = s.rangeMap.AddAndRemoveOverlapping(rd.GetStart(), rd.GetEnd(), rd, checkFn)
		if !added {
			s.log.Debugf("UpdateRange: Ignoring rangeDescriptor %+v, because current has same or later generation", rd)
		}
	}
	s.rangeMu.Unlock()

	if err != nil {
		s.log.Warningf("failed to add range %d: [%q, %q) gen %d failed: %s", rd.GetRangeId(), rd.GetStart(), rd.GetEnd(), rd.GetGeneration(), err)
		return
	}

	if !loaded {
		metrics.RaftRanges.With(prometheus.Labels{
			metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
		}).Inc()
	}

	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("range %d has no replicas (yet?)", rd.GetRangeId())
		return
	}

	if rd.GetStart() == nil || rd.GetEnd() == nil {
		s.log.Debugf("range %d has no bounds (yet?)", rd.GetRangeId())
		return
	}

	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()

	if stopped {
		return
	}

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
	if rd.GetStart() != nil && rd.GetEnd() != nil {
		s.rangeMap.Remove(rd.GetStart(), rd.GetEnd())
	}
	s.rangeMu.Unlock()

	metrics.RaftRanges.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
	}).Dec()

	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("range descriptor had no replicas yet")
		return
	}

	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()

	if stopped {
		return
	}

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

// validatedRangeAgainstMetaRange fetches the range descriptor from meta range and
// verifies that the generation is up-to-date. It's stricter than validatedRange.
func (s *Store) validatedRangeAgainstMetaRange(ctx context.Context, rd *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	if rd.GetStart() == nil {
		return nil, status.InvalidArgumentErrorf("start is not specified in range descriptor")
	}
	// Fetch the range descriptor from meta range to make sure it's the most-up-to-date.
	remoteRD, err := s.Sender().LookupRangeDescriptor(ctx, rd.GetStart(), true /*skip Cache */)
	if err != nil {
		return nil, status.InternalErrorf("failed to look up range descriptor")
	}

	if remoteRD.GetRangeId() != rd.GetRangeId() {
		return nil, status.OutOfRangeErrorf("%s: found range_id: %d with range [%q, %q), expected: %d [%q, %q)", constants.RangeNotFoundMsg, remoteRD.GetRangeId(), remoteRD.GetStart(), remoteRD.GetEnd(), rd.GetRangeId(), rd.GetStart(), rd.GetEnd())
	}

	if remoteRD.GetGeneration() != rd.GetGeneration() {
		return nil, status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, remoteRD.GetGeneration(), rd.GetGeneration())
	}
	return remoteRD, nil

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
	return int(s.configuredClusters.Load())
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

// HasReplicaAndIsLeader checks whether the store has the replica loaded and if
// so, whether is replica is the leader or not
func (s *Store) HasReplicaAndIsLeader(rangeID uint64) bool {
	repl, err := s.GetReplica(rangeID)
	if err != nil {
		s.log.Debugf("failed to get replica for range %d: %s", rangeID, err)
		return false
	}
	return s.isLeader(rangeID, repl.ReplicaID())
}

// isLeader checks whether a replica is a leader in raft.
func (s *Store) isLeader(rangeID uint64, replicaID uint64) bool {
	leaderID, term, valid, err := s.nodeHost.GetLeaderID(rangeID)
	if err != nil {
		s.log.Debugf("failed to get leader id for range %d: %s", rangeID, err)
		return false
	}
	if !valid {
		return false
	}
	return leaderID == replicaID && term > 0
}

// rangeDescriptorHasReplica checks whether replica ID exists in staging,
// replicas, or removed list.
func rangeDescriptorHasReplica(rd *rfpb.RangeDescriptor, replicaID uint64) bool {
	replicas := append(rd.GetReplicas(), rd.GetStaging()...)
	replicas = append(replicas, rd.GetRemoved()...)
	// The range is found in meta range.
	for _, repl := range replicas {
		if repl.GetReplicaId() == replicaID {
			return true
		}
	}
	return false
}

func (s *Store) GetRegistry(ctx context.Context, req *rfpb.GetRegistryRequest) (*rfpb.GetRegistryResponse, error) {
	connections := s.registry.ListNodes()
	return &rfpb.GetRegistryResponse{
		Connections: connections,
	}, nil
}

func (s *Store) TransferLeadership(ctx context.Context, req *rfpb.TransferLeadershipRequest) (*rfpb.TransferLeadershipResponse, error) {
	log.CtxDebugf(ctx, "request to transfer leadership of range %d to replica %d", req.GetRangeId(), req.GetTargetReplicaId())
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

// ListReplicasForTest lists replicas that are opened in the machine
func (s *Store) ListOpenReplicasForTest() []*rfpb.ReplicaDescriptor {
	replicas := []*rfpb.ReplicaDescriptor{}
	nhInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})
	for _, shardInfo := range nhInfo.ShardInfoList {
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			RangeId:   shardInfo.ShardID,
			ReplicaId: shardInfo.ReplicaID,
		})
	}
	return replicas
}

// ListAllReplicasInHistoryForTest lists all replicas that have been opened on
// the machine; even if they are removed from membership.
func (s *Store) ListAllReplicasInHistoryForTest() []*rfpb.ReplicaDescriptor {
	replicas := []*rfpb.ReplicaDescriptor{}
	nhInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	for _, logInfo := range nhInfo.LogInfo {
		if s.nodeHost.HasNodeInfo(logInfo.ShardID, logInfo.ReplicaID) {
			replicas = append(replicas, &rfpb.ReplicaDescriptor{
				RangeId:   logInfo.ShardID,
				ReplicaId: logInfo.ReplicaID,
			})
		}
	}
	return replicas
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
	startShardErr := s.nodeHost.StartOnDiskReplica(req.GetInitialMember(), req.GetJoin(), s.ReplicaFactoryFn, rc)
	if startShardErr != nil {
		if startShardErr != dragonboat.ErrShardAlreadyExist {
			return nil, status.WrapErrorf(startShardErr, "failed to start node c%dn%d on dragonboat", req.GetRangeId(), req.GetReplicaId())
		}
		nu, nuErr := s.nodeHost.GetNodeUser(req.GetRangeId())
		if nuErr != nil {
			return nil, status.InternalErrorf("failed to get node user: %s", startShardErr)
		}
		if nu.ReplicaID() != req.GetReplicaId() {
			return nil, status.InternalErrorf("cannot start c%dn%d because c%dn%d already exists", nu.ShardID(), nu.ReplicaID(), req.GetRangeId(), req.GetReplicaId())
		}

		// If the shard already exists, we want to wait for the replica to catch up
		// if last_applied_index is set in the request
		startShardErr = status.AlreadyExistsError(startShardErr.Error())
	}

	if req.GetLastAppliedIndex() > 0 {
		if err := s.waitForReplicaToCatchUp(ctx, req.GetRangeId(), req.GetLastAppliedIndex()); err != nil {
			return nil, status.WrapErrorf(err, "failed to wait for c%dn%d to catch up", req.GetRangeId(), req.GetReplicaId())
		}
	}

	rsp := &rfpb.StartShardResponse{}
	return rsp, startShardErr
}

func (s *Store) syncRequestDeleteReplica(ctx context.Context, rangeID, replicaID uint64) error {
	configChangeID, err := s.getConfigChangeID(ctx, rangeID)
	if err != nil {
		return status.InternalErrorf("failed to get configChangeID for range %d: %s", rangeID, err)
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, s.maxSingleOpTimeout, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestDeleteReplica(ctx, rangeID, replicaID, configChangeID)
	})
	return err
}

func (s *Store) stopReplica(ctx context.Context, rangeID, replicaID uint64) error {
	if rangeID == 0 || replicaID == 0 {
		return status.InvalidArgumentErrorf("rangeID or replicaID is not set")
	}
	err := client.RunNodehostFn(ctx, s.maxSingleOpTimeout, func(ctx context.Context) error {
		err := s.nodeHost.StopReplica(rangeID, replicaID)
		if err == dragonboat.ErrShardClosed {
			return nil
		}
		return err
	})
	if err != nil && err != dragonboat.ErrShardNotFound {
		// Ignore ShardNotFound error. The shard can be unloaded or deleted by
		// syncRequestDeleteReplica.
		return status.InternalErrorf("failed to stop replica c%dn%d: %s", rangeID, replicaID, err)
	}

	s.log.Infof("succesfully stopped replica c%dn%d", rangeID, replicaID)
	return nil
}

// removeReplica attempts to delete a replica from a Raft cluster.
func (s *Store) removeReplica(ctx context.Context, rd *rfpb.RangeDescriptor, req *rfpb.RemoveReplicaRequest) error {
	replicaID := req.GetReplicaId()
	runFn := func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
		_, err := c.RemoveReplica(ctx, req)
		return err
	}
	_, err := s.sender.TryReplicas(ctx, rd, runFn, func(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
		return nil
	})

	rangeID := req.GetRangeId()
	if rangeID == 0 {
		rangeID = rd.GetRangeId()
	}

	if err != nil {
		return status.WrapErrorf(err, "failed to remove replica c%dn%d", rangeID, replicaID)
	}

	s.log.Infof("succesfully removed replica c%dn%d", rangeID, replicaID)
	return nil
}

func (s *Store) syncRemoveData(ctx context.Context, rangeID, replicaID uint64) error {
	// stop the replica just in case it was not stopped.
	if err := s.stopReplica(ctx, rangeID, replicaID); err != nil {
		return status.WrapErrorf(err, "failed to stop replica before removing data of c%dn%d", rangeID, replicaID)
	}

	err := client.RunNodehostFn(ctx, s.maxSingleOpTimeout, func(ctx context.Context) error {
		err := s.nodeHost.SyncRemoveData(ctx, rangeID, replicaID)
		// If the shard is not stopped, we want to retry SyncRemoveData call.
		if err == dragonboat.ErrShardNotStopped {
			log.Warning("shard not stopped when remove data")
			err = dragonboat.ErrTimeout
		}
		return err
	})
	if err != nil {
		return status.InternalErrorf("failed to remove data of c%dn%d from raft: %s", rangeID, replicaID, err)
	}
	s.log.Infof("successfully removed data c%dn%d", rangeID, replicaID)
	return nil
}

// RemoveData tries to remove all data associated with the specified node (shard,
// replica). It waits for the node (shard, replica) to be fully offloaded or the
// context is cancelled. This method should only be used after the node is
// deleted from its Raft cluster; or if it is the last node of this cluster then
// the method can be used once the node is stopped.
//
// Note: after RemoveData is successfully executed, the node info won't be shown
// up in the result of dragonboat.GetNodeHostInfo even when skipLogInfo is false.
func (s *Store) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	if req.GetReplicaId() == 0 {
		return nil, status.InvalidArgumentErrorf("replica_id is not specified")
	}
	if req.GetRangeId() != 0 {
		if err := s.syncRemoveData(ctx, req.GetRangeId(), req.GetReplicaId()); err != nil {
			return nil, err
		}
		return &rfpb.RemoveDataResponse{}, nil
	}
	rd := req.GetRange()
	if rd == nil {
		return nil, status.InvalidArgumentErrorf("need to specify either range_id or range")
	}
	remoteRD, err := s.Sender().LookupRangeDescriptor(ctx, rd.GetStart(), true /*skip Cache */)
	if err != nil {
		return nil, status.InternalErrorf("failed to look up range descriptor: %s", err)
	}

	if rd.GetRangeId() != remoteRD.GetRangeId() {
		err := status.InternalErrorf("range descriptor from req doesn't match metarange. requested range_id=%d, but remote range_id=%d", rd.GetRangeId(), remoteRD.GetRangeId())
		return nil, err
	}

	markedForRemoval := false
	for _, repl := range remoteRD.GetRemoved() {
		if repl.GetReplicaId() == req.GetReplicaId() {
			markedForRemoval = true
			break
		}
	}

	shouldDeleteRange := rd.GetStart() != nil && rd.GetEnd() != nil && markedForRemoval

	if shouldDeleteRange {
		// This should not happen because we don't allow a range to be split while there are removals in progress.
		if !bytes.Equal(remoteRD.GetStart(), rd.GetStart()) || !bytes.Equal(remoteRD.GetEnd(), rd.GetEnd()) {
			err := status.InternalErrorf("range descriptor's range changed from [%q, %q) (gen: %d) to [%q, %q) (gen: %d) while there are replicas in process of removal", rd.GetStart(), rd.GetEnd(), rd.GetGeneration(), remoteRD.GetStart(), remoteRD.GetEnd(), remoteRD.GetGeneration())
			return nil, err
		}
	}
	if err := s.syncRemoveData(ctx, rd.GetRangeId(), req.GetReplicaId()); err != nil {
		return nil, err
	}

	if shouldDeleteRange {
		db, err := s.leaser.DB()
		if err != nil {
			return nil, err
		}
		defer db.Close()
		if err = db.DeleteRange(remoteRD.GetStart(), remoteRD.GetEnd(), pebble.NoSync); err != nil {
			return nil, status.InternalErrorf("failed to delete data of c%dn%d from pebble: %s", req.GetRange().GetRangeId(), req.GetReplicaId(), err)
		}

		// Remove the local range of the replica.
		localStart, localEnd := keys.Range(replica.LocalKeyPrefix(rd.GetRangeId(), req.GetReplicaId()))
		if err := db.DeleteRange(localStart, localEnd, pebble.NoSync); err != nil {
			return nil, status.InternalErrorf("failed to delete data of local range of c%dn%d from pebble: %s", req.GetRange().GetRangeId(), req.GetReplicaId(), err)
		}
	}

	// update range descriptor: remove the replica descriptor from the removed list.
	if markedForRemoval {
		rd, err = s.removeReplicaFromRangeDescriptor(ctx, remoteRD.GetRangeId(), req.GetReplicaId(), remoteRD)
		if err != nil {
			return nil, status.InternalErrorf("failed to remove replica c%dn%d from range descriptor: %s", remoteRD.GetRangeId(), req.GetReplicaId(), err)
		}
	}
	return &rfpb.RemoveDataResponse{
		Range: rd,
	}, nil
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

	session := s.session
	if len(req.GetBatch().GetTransactionId()) > 0 {
		session = s.txnSession
	} else {
		// use eviction session for delete requests
		unions := req.GetBatch().GetUnion()
		if len(unions) > 0 && unions[0].GetDelete() != nil {
			session = s.evictionSession
		}
	}
	batchResponse, err := session.SyncProposeLocal(ctx, s.nodeHost, rangeID, req.GetBatch())
	if err != nil {
		if err == dragonboat.ErrShardNotFound {
			return nil, status.OutOfRangeErrorf("%s: range %d not found", constants.RangeNotFoundMsg, rangeID)
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
	batchResponse, err := client.SyncReadLocal(ctx, s.nodeHost, rangeID, batch, s.maxSingleOpTimeout)
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
			if s.liveness.Valid(ctx) {
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
	membership, err := s.GetMembership(ctx, shardInfo.ShardID)
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

type replicaMembership struct {
	replicaID   uint64
	isNonVoting bool
}

func (s *Store) checkReplicaMembership(ctx context.Context, rangeID uint64, nhid string) (*replicaMembership, error) {
	membership, err := s.GetMembership(ctx, rangeID)
	if err != nil {
		return nil, err
	}
	for replicaID, addr := range membership.NonVotings {
		if addr == nhid {
			return &replicaMembership{
				replicaID:   replicaID,
				isNonVoting: true,
			}, nil
		}
	}
	for replicaID, addr := range membership.Nodes {
		if addr == nhid {
			return &replicaMembership{
				replicaID:   replicaID,
				isNonVoting: false,
			}, nil
		}
	}
	return nil, nil
}

func (s *Store) newRangeDescriptorFromRaftMembership(ctx context.Context, rangeID uint64) (*rfpb.RangeDescriptor, error) {
	membership, err := s.GetMembership(ctx, rangeID)
	if err != nil {
		return nil, err
	}
	res := &rfpb.RangeDescriptor{RangeId: rangeID}
	for replicaID, addr := range membership.Nodes {
		res.Replicas = append(res.Replicas, &rfpb.ReplicaDescriptor{
			RangeId:   rangeID,
			ReplicaId: replicaID,
			Nhid:      proto.String(addr),
		})
	}
	return res, nil
}

type zombieCleanupAction int

const (
	zombieCleanupNoAction zombieCleanupAction = iota
	zombieCleanupRemoveReplica
	zombieCleanupRemoveData
	zombieCleanupWait
)

type zombieCleanupTask struct {
	rangeID   uint64
	replicaID uint64
	shardInfo dragonboat.ShardInfo
	rd        *rfpb.RangeDescriptor
	opened    bool

	action          zombieCleanupAction
	attempts        int
	nextAttemptTime time.Time
}

type replicaJanitor struct {
	scanInterval      time.Duration
	zombieMinDuration time.Duration

	lastDetectedAt map[uint64]time.Time // from rangeID to last_detected_at

	rateLimiter *rate.Limiter
	clock       clockwork.Clock
	tasks       chan zombieCleanupTask

	mu              sync.Mutex // protects rangeIDsInQueue
	rangeIDsInQueue map[uint64]bool

	store    *Store
	ctx      context.Context
	cancelFn context.CancelFunc
	eg       errgroup.Group
}

func newReplicaJanitor(clock clockwork.Clock, store *Store, scanInterval time.Duration, zombieMinDuration time.Duration) *replicaJanitor {
	return &replicaJanitor{
		scanInterval:      scanInterval,
		zombieMinDuration: zombieMinDuration,
		rateLimiter:       rate.NewLimiter(rate.Limit(removeZombieRateLimit), 1),
		clock:             clock,
		tasks:             make(chan zombieCleanupTask, 500),
		rangeIDsInQueue:   make(map[uint64]bool),
		lastDetectedAt:    make(map[uint64]time.Time),
		store:             store,
	}
}

func (rj *replicaJanitor) nextAttemptTime(attemptNumber int) time.Time {
	backoff := float64(1*time.Second) * math.Pow(2, float64(attemptNumber))
	return rj.clock.Now().Add(time.Duration(backoff))
}

func setZombieAction(ss *zombieCleanupTask, rangeMap map[uint64]*rfpb.RangeDescriptor) *zombieCleanupTask {
	rd, foundRange := rangeMap[ss.rangeID]
	if foundRange {
		ss.rd = rd

		foundReplica := rangeDescriptorHasReplica(rd, ss.replicaID)
		if foundReplica {
			// We have the info for this replica in range descriptor. Don't
			// touch it, leave it to the driver.
			ss.action = zombieCleanupNoAction
			return ss
		}
	}
	if !ss.opened {
		// This shard is in LogInfo, but not started and it's not found in
		// meta range; so we should remove the data of this shard so that
		// it won't be re-created during start-up
		ss.action = zombieCleanupRemoveData
		return ss
	}

	// The replica is a zombie, and it should be removed.
	ss.action = zombieCleanupRemoveReplica
	return ss
}

func (j *replicaJanitor) Start(ctx context.Context) {
	if j.scanInterval == 0 {
		return
	}
	eg := &errgroup.Group{}
	eg.Go(func() error {
		j.scan(ctx)
		return nil
	})
	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case task := <-j.tasks:
				action, err := j.removeZombie(ctx, task)
				metrics.RaftZombieCleanup.With(prometheus.Labels{
					metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
				}).Inc()
				if err != nil {
					j.store.log.Errorf("failed to remove zombie c%dn%d: %s", task.rangeID, task.replicaID, err)
					task.attempts++
					task.nextAttemptTime = j.nextAttemptTime(task.attempts)
				} else {
					task.attempts = 0
					task.nextAttemptTime = time.Time{}
				}
				if action != zombieCleanupNoAction {
					if action != zombieCleanupWait {
						task.action = action
					}
					j.tasks <- task
				} else {
					j.store.log.Debugf("removed zombie c%dn%d", task.rangeID, task.replicaID)
					j.mu.Lock()
					delete(j.rangeIDsInQueue, task.rangeID)
					j.mu.Unlock()
				}
			}
		}
	})

	eg.Wait()

	for len(j.tasks) > 0 {
		<-j.tasks
	}
}

func (j *replicaJanitor) scan(ctx context.Context) {
	if j.scanInterval == 0 {
		return
	}
	timer := j.clock.NewTicker(j.scanInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.Chan():
			j.scanForZombies(ctx)
		}
	}
}

func (j *replicaJanitor) scanForZombies(ctx context.Context) {
	nInfo := j.store.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	rangeIDs := make([]uint64, 0, len(nInfo.ShardInfoList))
	shardStateMap := make(map[uint64]zombieCleanupTask, len(nInfo.LogInfo))

	// Add all the replicas on the machine to shardStateMap, including
	// replicas that are not currently open.
	for _, logInfo := range nInfo.LogInfo {
		if j.store.nodeHost.HasNodeInfo(logInfo.ShardID, logInfo.ReplicaID) {
			rangeIDs = append(rangeIDs, logInfo.ShardID)
			shardStateMap[logInfo.ShardID] = zombieCleanupTask{
				rangeID:   logInfo.ShardID,
				replicaID: logInfo.ReplicaID,
			}
		}
	}

	// Go through the list of replicas that are currently open on raft,
	// and mark them in shardStateMap
	for _, sInfo := range nInfo.ShardInfoList {
		ss, ok := shardStateMap[sInfo.ShardID]
		if ok {
			ss.shardInfo = sInfo
			ss.opened = true
			shardStateMap[sInfo.ShardID] = ss
		}
	}

	// Fetch the range descritpors from meta range
	ranges, err := j.store.sender.LookupRangeDescriptorsByIDs(ctx, rangeIDs)
	if err != nil {
		j.store.log.Warningf("failed to scan zombie nodes: %s", err)
		return
	}
	rangeMap := make(map[uint64]*rfpb.RangeDescriptor, len(ranges))
	for _, rd := range ranges {
		rangeMap[rd.GetRangeId()] = rd
	}

	for rangeID, ss := range shardStateMap {
		newSS := setZombieAction(&ss, rangeMap)
		if newSS.action == zombieCleanupNoAction {
			continue
		}
		detectedAt, ok := j.lastDetectedAt[rangeID]
		if !ok {
			j.lastDetectedAt[rangeID] = j.clock.Now()
			continue
		}
		if j.clock.Since(detectedAt) < j.zombieMinDuration {
			continue
		}
		// this is a zombie.
		j.mu.Lock()
		inQueue := j.rangeIDsInQueue[rangeID]
		j.mu.Unlock()
		if !inQueue {
			j.tasks <- *newSS
		}
		j.mu.Lock()
		j.rangeIDsInQueue[rangeID] = true
		j.mu.Unlock()
	}
	metrics.RaftZombieCleanupTasks.Set(float64(len(j.tasks)))
}

func (j *replicaJanitor) removeZombie(ctx context.Context, task zombieCleanupTask) (zombieCleanupAction, error) {
	if task.action == zombieCleanupNoAction {
		return zombieCleanupNoAction, nil
	}

	if err := j.rateLimiter.Wait(ctx); err != nil {
		return task.action, err
	}

	if !task.nextAttemptTime.IsZero() && j.clock.Now().Before(task.nextAttemptTime) {
		return zombieCleanupWait, nil
	}

	log.Debugf("removing zombie c%dn%d, action=%d", task.rangeID, task.replicaID, task.action)
	removeDataReq := &rfpb.RemoveDataRequest{
		ReplicaId: task.replicaID,
	}
	if task.action == zombieCleanupRemoveData {
		if task.rd == nil {
			removeDataReq.RangeId = task.rangeID
		} else {
			removeDataReq.Range = task.rd
		}
	} else if task.action == zombieCleanupRemoveReplica {
		// fetched the up-to-date range from meta range
		var rd *rfpb.RangeDescriptor
		var err error
		removeReplicaReq := &rfpb.RemoveReplicaRequest{
			ReplicaId: task.replicaID,
		}

		if task.rd == nil {
			// When SplitRange failed in the middle, it can cause the shards to be
			// created without any range descriptors created.
			rd, err = j.store.newRangeDescriptorFromRaftMembership(ctx, task.rangeID)
			if err != nil {
				if err == dragonboat.ErrShardNotReady || err == dragonboat.ErrShardNotFound {
					// This can happen when SplitRange failed to initialize all initial members.
					// move on to stop replica
					log.Warningf("getMembership for c%dn%d failed: %s", task.rangeID, task.replicaID, err)
				} else {
					return zombieCleanupRemoveReplica, status.InternalErrorf("failed to create range descriptor from meta range:%s", err)
				}
			}
			// Set RangeId instead of Range in RemoveDataRequest and RemoveReplicaRequest to skip range descriptor check because there is
			// no range descriptor in meta range.
			removeDataReq.RangeId = task.rangeID
			removeReplicaReq.RangeId = task.rangeID
		} else {
			rd, err = j.store.validatedRangeAgainstMetaRange(ctx, task.rd)
			if err != nil {
				return zombieCleanupRemoveReplica, status.InternalErrorf("failed to fetch up-to-date range descriptor from meta range:%s", err)
			}

			if j.store.isLeader(task.rangeID, task.replicaID) && !rangeDescriptorHasReplica(rd, task.replicaID) {
				// The replica is not in the range descriptor; and if it is the
				// leader, we won't be able to call the sender to remove it from
				// raft membership. This is because, none of the replicas in the
				// range descriptor can get lease.
				if targetReplicaID := j.store.findTargetReplicaIDForLeadershipTransfer(ctx, rd, task.replicaID); targetReplicaID != 0 {
					if err := j.store.nodeHost.RequestLeaderTransfer(task.rangeID, targetReplicaID); err != nil {
						return zombieCleanupRemoveReplica, status.InternalErrorf("failed to transfer leader for range %d from %d to %d: %s", task.rangeID, task.replicaID, targetReplicaID, err)
					}
				}
			}
			removeReplicaReq.Range = rd
			removeDataReq.Range = rd
		}

		// Skip removeReplica if we don't have rd. This is possible when we were
		// not able to create rd from the membership because the shard is not
		// ready. In that case, we stopped the replica directly.
		if rd != nil {
			err = j.store.removeReplica(ctx, rd, removeReplicaReq)
			if err != nil {
				if strings.Contains(err.Error(), dragonboat.ErrRejected.Error()) {
					// move on to stop replica
					log.Warningf("request to delete replica c%dn%d was rejected, attempting to stop: %s", task.rangeID, task.replicaID, err)
				} else {
					return zombieCleanupRemoveReplica, err
				}
			}
		}
	}

	_, err := j.store.RemoveData(ctx, removeDataReq)
	if err != nil {
		return zombieCleanupRemoveData, status.InternalErrorf("failed to remove data: %s", err)
	}
	return zombieCleanupNoAction, nil
}

func (s *Store) checkIfReplicasNeedSplitting(ctx context.Context, maxRangeSizeBytes int64) {
	eventsCh := s.AddEventListener("splitRanges")
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-eventsCh:
			switch e.EventType() {
			case events.EventRangeUsageUpdated:
				rangeUsageEvent := e.(events.RangeUsageEvent)
				rangeID := rangeUsageEvent.RangeDescriptor.GetRangeId()
				estimatedDiskBytes := rangeUsageEvent.ReplicaUsage.GetEstimatedDiskBytesUsed()
				metrics.RaftBytes.With(prometheus.Labels{
					metrics.RaftRangeIDLabel: strconv.Itoa(int(rangeID)),
				}).Set(float64(estimatedDiskBytes))
				if rangeID == constants.MetaRangeID {
					continue
				}
				if !s.leaseKeeper.HaveLease(ctx, rangeID) {
					continue
				}
				if estimatedDiskBytes < maxRangeSizeBytes {
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
	eventsCh := s.AddEventListener("updateTags")
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
			case events.EventRangeLeaseAcquired, events.EventRangeLeaseDropped:
				s.updateTagsWorker.Enqueue()
			default:
				break
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

	fsu, err := s.getFileSystemUsage()
	if err != nil {
		log.Warningf("cannot get file system usage: %s", err)
	}
	su.TotalBytesUsed = int64(fsu.Used)
	su.TotalBytesFree = int64(fsu.Free)

	replicaInitDone := s.ReplicasInitDone()
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	su.IsReady = replicaInitDone && !stopped
	return su
}

type replicaInitStatus int

const (
	replicaInitStatusStarted replicaInitStatus = iota
	replicaInitStatusCompleted
)

func replicaKey(rangeID, replicaID uint64) string {
	return fmt.Sprintf("%d-%d", rangeID, replicaID)
}

type replicaStatusWaiter struct {
	initStates sync.Map     // map of replicaKey(rangeID-replicaID) -> replica init status
	mu         sync.RWMutex // protects allReady
	allReady   bool

	log       log.Logger
	listener  *listener.RaftListener
	nodeReady <-chan raftio.NodeInfo
	quitChan  chan struct{}
}

func newReplicaStatusWaiter(listener *listener.RaftListener, log log.Logger) *replicaStatusWaiter {
	return &replicaStatusWaiter{
		log:       log,
		listener:  listener,
		nodeReady: listener.AddNodeReadyListener(listenerID),
	}
}

func (w *replicaStatusWaiter) MarkStarted(rangeID, replicaID uint64) error {
	key := replicaKey(rangeID, replicaID)
	_, ok := w.initStates.Load(key)
	if ok {
		return status.InternalErrorf("c%dn%d has been configured", rangeID, replicaID)
	}

	w.initStates.Store(key, replicaInitStatusStarted)
	return nil
}

func (w *replicaStatusWaiter) InitStatus(rangeID, replicaID uint64) string {
	key := replicaKey(rangeID, replicaID)
	statusI, loaded := w.initStates.Load(key)
	if !loaded {
		return "unknown"
	}
	status := statusI.(replicaInitStatus)
	switch status {
	case replicaInitStatusStarted:
		return "started"
	case replicaInitStatusCompleted:
		return "completed"
	}
	return "unknown"
}

func (w *replicaStatusWaiter) Start(ctx context.Context) {
	for {
		select {
		case info, ok := <-w.nodeReady:
			if !ok {
				// channel was closed and drained
				continue
			}
			key := replicaKey(info.ShardID, info.ReplicaID)
			_, ok = w.initStates.Load(key)
			if ok {
				w.initStates.Store(key, replicaInitStatusCompleted)
			}
		case <-ctx.Done():
			w.listener.RemoveNodeReadyListener(listenerID)
			return
		}
	}
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
		if status, ok := value.(replicaInitStatus); ok {
			if status == replicaInitStatusCompleted {
				return true
			}

			allReady = false
			log.Infof("replica %s is not ready", key.(string))
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

	w.store.log.Info("updateTagsWorker shutdown started")
	now := time.Now()
	defer func() {
		w.store.log.Infof("updateTagsWorker shutdown finished in %s", time.Since(now))
	}()
	if err := w.eg.Wait(); err != nil {
		w.store.log.Error(err.Error())
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

	storeTags[constants.GRPCAddressTag] = w.store.grpcAddr

	su := w.store.Usage()
	buf, err := proto.Marshal(su)
	if err != nil {
		return err
	}
	storeTags[constants.StoreUsageTag] = base64.StdEncoding.EncodeToString(buf)
	err = w.store.gossipManager.SetTags(storeTags)
	return err
}

type handleTaskFunc func(context.Context, *replica.Replica) error

type replicaWorker struct {
	name     string
	tasks    chan *replica.Replica
	handleFn handleTaskFunc
}

func (w *replicaWorker) Enqueue(repl *replica.Replica) {
	select {
	case w.tasks <- repl:
		break
	default:
		log.Warningf("%s queue full", w.name)
	}
}

func (w *replicaWorker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for len(w.tasks) > 0 {
				<-w.tasks
			}
			return
		case repl := <-w.tasks:
			if err := w.handleFn(ctx, repl); err != nil {
				log.Warningf("%s failed to handle task: %s", w.name, err)
			}
		}
	}
}

type deleteSessionWorker struct {
	rateLimiter       *rate.Limiter
	lastExecutionTime sync.Map // map of uint64 rangeID -> the timestamp we last delete sessions
	session           *client.Session
	clock             clockwork.Clock
	store             *Store
	clientSessionTTL  time.Duration

	*replicaWorker
}

func newDeleteSessionsWorker(clock clockwork.Clock, store *Store, clientSessionTTL time.Duration) *deleteSessionWorker {
	res := &deleteSessionWorker{
		rateLimiter:       rate.NewLimiter(rate.Limit(deleteSessionsRateLimit), 1),
		store:             store,
		lastExecutionTime: sync.Map{},
		session:           client.NewSessionWithClock(clock),
		clock:             clock,
		clientSessionTTL:  clientSessionTTL,
	}
	res.replicaWorker = &replicaWorker{
		name:     "deleteSessionWorker",
		tasks:    make(chan *replica.Replica, 10000),
		handleFn: res.deleteSessions,
	}
	return res
}

func (w *deleteSessionWorker) deleteSessions(ctx context.Context, repl *replica.Replica) error {
	rd := w.store.lookupRange(repl.RangeID())
	lastExecutionTimeI, found := w.lastExecutionTime.Load(rd.GetRangeId())
	if found {
		lastExecutionTime, ok := lastExecutionTimeI.(time.Time)
		if ok {
			if w.clock.Since(lastExecutionTime) <= client.SessionLifetime() {
				// There are probably no client sessions to delete.
				return nil
			}
		} else {
			return status.InternalErrorf("unable to delete sessions for rangeID=%d: unable to parse lastExecutionTime", rd.GetRangeId())
		}
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
			CreatedAtUsec: now.Add(-w.clientSessionTTL).UnixMicro(),
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
	if err != nil {
		return status.InternalErrorf("unable to delete sessions for rangeID=%d: deleteSessions fails: %s", rd.GetRangeId(), err)
	}
	return nil
}

func (s *Store) SplitRange(ctx context.Context, req *rfpb.SplitRangeRequest) (*rfpb.SplitRangeResponse, error) {
	startTime := time.Now()

	if req.GetRange() == nil {
		return nil, status.FailedPreconditionErrorf("no range provided to split: %+v", req)
	}
	if len(req.GetRange().GetReplicas()) == 0 {
		return nil, status.FailedPreconditionErrorf("no replicas in range: %+v", req.GetRange())
	}

	// Validate the header to ensure we don't start new raft nodes if the
	// split is gonna fail later when the transaction is run on an out-of
	// -date range.
	remoteRD, err := s.validatedRangeAgainstMetaRange(ctx, req.GetRange())
	if err != nil {
		return nil, err
	}

	leftRange := remoteRD

	// Copy left range, because it's a pointer and will change when we
	// propose the split.
	leftRange = leftRange.CloneVT()
	rangeID := leftRange.GetRangeId()

	// Reserve new IDs for this cluster.
	newRangeID, err := s.reserveRangeID(ctx)
	if err != nil {
		return nil, status.InternalErrorf("could not reserve RangeID for new range %d: %s", rangeID, err)
	}

	// Find Split Point.
	fsp := rbuilder.NewBatchBuilder().Add(&rfpb.FindSplitPointRequest{}).SetHeader(req.GetHeader())
	fspRsp, err := client.SyncReadLocalBatch(ctx, s.nodeHost, rangeID, fsp, s.maxSingleOpTimeout)
	if err != nil {
		return nil, status.InternalErrorf("find split point err: %s", err)
	}
	splitPointResponse, err := fspRsp.FindSplitPointResponse(0)
	if err != nil {
		return nil, err
	}

	replicaIDs, err := s.reserveReplicaIDs(ctx, newRangeID, len(leftRange.GetReplicas()))
	if err != nil {
		return nil, status.InternalErrorf("could not reserve replica IDs for new range %d: %s", rangeID, err)
	}

	initialMembers := make(map[uint64]string, len(leftRange.GetReplicas()))
	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(leftRange.GetReplicas()))

	for i, r := range leftRange.GetReplicas() {
		if r.GetNhid() == "" {
			return nil, status.InternalErrorf("empty nhid in ReplicaDescriptor %+v", r)
		}
		replicaID := replicaIDs[i]
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			RangeId:   newRangeID,
			ReplicaId: replicaID,
			Nhid:      proto.String(r.GetNhid()),
		})
		initialMembers[replicaID] = r.GetNhid()
	}

	// Assemble new range descriptor.
	newRightRange := proto.Clone(leftRange).(*rfpb.RangeDescriptor)
	newRightRange.Start = splitPointResponse.GetSplitKey()
	newRightRange.RangeId = newRangeID
	newRightRange.Generation += 1
	newRightRange.Replicas = replicas
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
	// lock the left range when we prepare the transaction. Before we finalize
	// the transaction, we don't allow keys in the old left range to be written.
	leftBatch.SetLockMappedRange(true)

	rightBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: newRightRangeBuf,
		},
	})
	metaBatch := rbuilder.NewBatchBuilder()
	if err := addMetaRangeEdits(leftRange, updatedLeftRange, newRightRange, metaBatch); err != nil {
		return nil, err
	}
	mrd := s.sender.GetMetaRangeDescriptor()

	tb := rbuilder.NewTxn()
	leftStmt := tb.AddStatement()
	leftStmt = leftStmt.SetRangeDescriptor(leftRange).SetBatch(leftBatch)
	leftStmt.AddPostCommitHook(rfpb.TransactionHook_COMMIT, &rfpb.SnapshotClusterHook{})
	leftStmt.AddPostCommitHook(rfpb.TransactionHook_PREPARE, &rfpb.StartShardHook{
		RangeId:       newRangeID,
		InitialMember: initialMembers,
	})
	// Range Validation is required on the existing range to make sure that the
	// existing leader/range lease holder has the update-to-date range descriptor.
	// See go/raft-range-validation-in-txn.
	leftStmt.SetRangeValidationRequired(true)

	rightStmt := tb.AddStatement()
	rightStmt.SetRangeDescriptor(newRightRange).SetBatch(rightBatch)
	rightStmt.AddPostCommitHook(rfpb.TransactionHook_COMMIT, &rfpb.SnapshotClusterHook{})

	// No range validation b/c the range descriptor is a new one.
	// See go/raft-range-validation-in-txn
	rightStmt.SetRangeValidationRequired(false)

	metaStmt := tb.AddStatement()
	metaStmt.SetRangeDescriptor(mrd).SetBatch(metaBatch)
	// The meta range descriptor is from range cache and can be not-up-to date.
	// Validating meta range can make the txn difficult to succeed.
	// See go/raft-range-validation-in-txn.
	metaStmt.SetRangeValidationRequired(false)
	if err := s.txnCoordinator.RunTxn(ctx, tb); err != nil {
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
	header := header.New(rd, r, rfpb.Header_STALE)
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

func (s *Store) GetMembership(ctx context.Context, rangeID uint64) (*dragonboat.Membership, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, s.maxSingleOpTimeout, func(ctx context.Context) error {
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
	membership, err := s.GetMembership(ctx, rangeID)
	if err != nil {
		return 0, err
	}
	return membership.ConfigChangeID, nil
}

// CheckRangeOverlaps checks whether there is an open range overlapped with the
// [start, end) in the request.
func (s *Store) CheckRangeOverlaps(ctx context.Context, req *rfpb.CheckRangeOverlapsRequest) (*rfpb.CheckRangeOverlapsResponse, error) {
	if req.GetStart() == nil || req.GetEnd() == nil {
		return nil, status.FailedPreconditionError("req.Start or req.End cannot be nil")
	}
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()
	overlapping := s.rangeMap.GetOverlapping(req.GetStart(), req.GetEnd())
	rsp := &rfpb.CheckRangeOverlapsResponse{}
	for _, overlapped := range overlapping {
		rsp.Ranges = append(rsp.Ranges, overlapped.Val)
	}
	return rsp, nil
}

func (s *Store) validateAddReplicaRequest(ctx context.Context, req *rfpb.AddReplicaRequest) error {
	// Check the request looks valid.
	if len(req.GetRange().GetReplicas()) == 0 {
		return status.FailedPreconditionErrorf("No replicas in range: %+v", req.GetRange())
	}
	node := req.GetNode()
	if node.GetNhid() == "" || node.GetRaftAddress() == "" || node.GetGrpcAddress() == "" {
		return status.FailedPreconditionErrorf("Incomplete node descriptor: %+v", node)
	}

	// Check this is a range we have and the range descriptor provided is up to date
	s.rangeMu.RLock()
	_, rangeOK := s.openRanges[req.GetRange().GetRangeId()]
	s.rangeMu.RUnlock()

	if !rangeOK {
		return status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, req.GetRange().GetRangeId())
	}

	remoteRD, err := s.validatedRangeAgainstMetaRange(ctx, req.GetRange())
	if err != nil {
		return err
	}

	for _, repl := range remoteRD.GetReplicas() {
		if repl.GetNhid() == node.GetNhid() {
			return status.AlreadyExistsErrorf("range %d is already on the node %q", req.GetRange().GetRangeId(), node.GetNhid())
		}
	}

	for _, repl := range remoteRD.GetRemoved() {
		if repl.GetNhid() == node.GetNhid() {
			return status.FailedPreconditionErrorf("range %d is being removed from node %q", req.GetRange().GetRangeId(), node.GetNhid())
		}
	}
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return status.InternalErrorf("failed to get the client for the node %q: %s", node.GetNhid(), err)
	}

	// Check the target node doesn't have an overlapping range. An overlapping
	// range can be caused by a slow node that hasn't committed the split txn.
	// Since we don't support range merge operations, we don't need to worry
	// about the case where overlapping ranges occur between the check and when
	// the replica is added to the node.
	if remoteRD.GetStart() != nil && remoteRD.GetEnd() != nil {
		rsp, err := c.CheckRangeOverlaps(ctx, &rfpb.CheckRangeOverlapsRequest{
			Start: remoteRD.GetStart(),
			End:   remoteRD.GetEnd(),
		})
		if err != nil {
			return status.InternalErrorf("failed to check range overlap on node %q: %s", node.GetNhid(), err)
		}

		for _, overlap := range rsp.GetRanges() {
			// If the range was added to the node, but the range descriptor was
			// not successfully updated to meta range, don't consider it as an
			// overlap range.
			if overlap.GetRangeId() != remoteRD.GetRangeId() || !bytes.Equal(overlap.GetStart(), remoteRD.GetStart()) || !bytes.Equal(overlap.GetEnd(), remoteRD.GetEnd()) {
				return status.FailedPreconditionErrorf("node %q has overlapping ranges, e.g. range_id: %d: [%q, %q)", node.GetNhid(), overlap.GetRangeId(), overlap.GetStart(), overlap.GetEnd())
			}
		}
	}

	return nil
}

// addNonVoting adds the range to the specified node.
func (s *Store) addNonVoting(ctx context.Context, rangeID uint64, newReplicaID uint64, node *rfpb.NodeDescriptor) error {
	// Get the config change index for adding a non-voter.
	configChangeID, err := s.getConfigChangeID(ctx, rangeID)
	if err != nil {
		return status.InternalErrorf("failed to get config change ID: %s", err)
	}

	// Propose the config change (this adds the node as a non-voter to the raft cluster).
	err = client.RunNodehostFn(ctx, s.maxSingleOpTimeout, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddNonVoting(ctx, rangeID, newReplicaID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return status.InternalErrorf("nodeHost.SyncRequestAddNonVoting failed (configChangeID=%d): %s", configChangeID, err)
	}
	return nil
}

func (s *Store) promoteToVoter(ctx context.Context, rd *rfpb.RangeDescriptor, newReplicaID uint64, node *rfpb.NodeDescriptor) error {
	// Start the cluster on the newly added node.
	lastAppliedIndex, err := s.getLocalLastAppliedIndex(&rfpb.Header{
		RangeId:    rd.GetRangeId(),
		Generation: rd.GetGeneration(),
	})
	if err != nil {
		return status.InternalErrorf("failed to get last applied index: %s", err)
	}
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return status.InternalErrorf("failed to get the client for the node %q: %s", node.GetNhid(), err)
	}
	_, err = c.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:          rd.GetRangeId(),
		ReplicaId:        newReplicaID,
		Join:             true,
		LastAppliedIndex: lastAppliedIndex,
		IsNonVoting:      true,
	})
	if err != nil {
		if !status.IsAlreadyExistsError(err) {
			return status.InternalErrorf("failed to start shard c%dn%d: %s", rd.GetRangeId(), newReplicaID, err)
		}
	}
	// StartShard ensures the node is up-to-date. We can promote non-voter to voter.
	// Propose the config change (this adds the node as a non-voter to the raft cluster).
	// Get the config change index for promoting a non-voter to a voter
	configChangeID, err := s.getConfigChangeID(ctx, rd.GetRangeId())
	if err != nil {
		return status.InternalErrorf("failed to get config change ID: %s", err)
	}
	s.log.Infof("promote c%dn%d to voter, ccid=%d", rd.GetRangeId(), newReplicaID, configChangeID)
	err = client.RunNodehostFn(ctx, s.maxSingleOpTimeout, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddReplica(ctx, rd.GetRangeId(), newReplicaID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		if err == dragonboat.ErrRejected {
			// SyncRequestAddReplica can be retried by RunNodehostFn. In some
			// rare cases, when it returns a temp dragonboat error such as system
			// busy, the config change can actually be applied in the background.
			// However, the next attempt of SyncRequestAddReplica can fail
			// because the config change ID is equal to the last applied change
			// id.
			membership, membershipErr := s.GetMembership(ctx, rd.GetRangeId())
			if membershipErr != nil {
				return status.InternalErrorf("nodeHost.SyncRequestAddReplica failed (configChangeID=%d): %s, and getMembership failed: %s", configChangeID, err, membershipErr)
			}
			for replicaID := range membership.Nodes {
				if replicaID == newReplicaID {
					// the replica has been promoted to the voter
					return nil
				}
			}
		}
		return status.InternalErrorf("nodeHost.SyncRequestAddReplica failed (configChangeID=%d): %s", configChangeID, err)
	}
	return nil
}

type addReplicaState int

const (
	addReplicaStateAbsent addReplicaState = iota
	addReplicaStateNonVoter
	addReplicaStateVoter
)

// AddReplica adds a range to the node with the specified NHID.
// Pre-reqs are:
//   - The request must be valid and contain all information
//   - This node must be a member of the cluster that is being added to
//   - The provided range descriptor must be up to date
//
// Depending on whether and when the same request has run and failed in the past,
// there can be four potential states:
// 1. No replicas of the range is present on the node.
// 2. A replica has been added to the node as a non-voter.
// 3. A replica has been added to the node as a voter, but the range descriptor
// hasn't been updated.
// 4. The desired state: a replica has been added to the node as a voter, and the
// range descriptor has been updated.
//
// Therefore, AddReplica first queries raft to check which membership status the
// range is in with the specified node; and then move the state to the desired
// state.
func (s *Store) AddReplica(ctx context.Context, req *rfpb.AddReplicaRequest) (*rfpb.AddReplicaResponse, error) {
	if err := s.validateAddReplicaRequest(ctx, req); err != nil {
		return nil, err
	}

	rd := req.GetRange()
	rangeID := rd.GetRangeId()
	node := req.GetNode()

	// Check raft membership for the replica.
	replicaMembership, err := s.checkReplicaMembership(ctx, rangeID, node.GetNhid())
	if err != nil {
		return nil, err
	}

	state := addReplicaStateAbsent
	// Determine the replica ID and the state.
	// When there was previous attempts to add the range
	// to the specified node, we should use the existing replicaID; otherwise, the
	// request will be rejected by raft.
	var newReplicaID uint64
	if replicaMembership != nil {
		// There are previous attempts to add a replica of this range to the node.
		// Use the existing replica ID.
		newReplicaID = replicaMembership.replicaID

		if replicaMembership.isNonVoting {
			state = addReplicaStateNonVoter
		} else {
			state = addReplicaStateVoter
		}
	} else {
		// Reserve a new replica ID for the replica to be added on the node.
		replicaIDs, err := s.reserveReplicaIDs(ctx, rangeID, 1)
		if err != nil {
			return nil, status.InternalErrorf("AddReplica failed to add range(%d) to node %q: failed to reserve replica IDs: %s", rangeID, node.GetNhid(), err)
		}
		newReplicaID = replicaIDs[0]
		rd, err = s.addStagingReplicaToRangeDescriptor(ctx, rangeID, newReplicaID, node.GetNhid(), rd)
		if err != nil {
			return nil, status.WrapErrorf(err, "AddReplica failed to add staging replica c%dn%d on %q: %s", rangeID, newReplicaID, node.GetNhid(), err)
		}
	}

	// addReplicaStateAbsent -> addReplicaStateNonVoter
	if state == addReplicaStateAbsent {
		if err = s.addNonVoting(ctx, rangeID, newReplicaID, node); err != nil {
			return nil, status.InternalErrorf("AddReplica failed to add c%dn%d to node %q as a non-voter: %s", rangeID, newReplicaID, node.GetNhid(), err)
		}
		state = addReplicaStateNonVoter
	}

	if state == addReplicaStateNonVoter {
		if err = s.promoteToVoter(ctx, rd, newReplicaID, node); err != nil {
			return nil, status.InternalErrorf("promote c%dn%d to voter failed: %s", rd.GetRangeId(), newReplicaID, err)
		}
		state = addReplicaStateVoter
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	rd, err = s.addReplicaToRangeDescriptor(ctx, rangeID, newReplicaID, rd)
	if err != nil {
		return nil, status.InternalErrorf("AddReplica failed to add replica to range descriptor: %s", err)
	}

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
	rangeID := uint64(0)
	if r := req.GetRange(); r != nil {
		rangeID = r.GetRangeId()
	} else {
		rangeID = req.GetRangeId()
	}

	if rangeID == 0 {
		return nil, status.InvalidArgumentError("no range id is specified")
	}

	rsp := &rfpb.RemoveReplicaResponse{}

	if req.GetRange() != nil {
		if !s.HaveLease(ctx, rangeID) {
			return nil, status.OutOfRangeErrorf("%s: no lease found for range: %d", constants.RangeLeaseInvalidMsg, rangeID)
		}

		// Check this is a range we have.
		s.rangeMu.RLock()
		rd, rangeOK := s.openRanges[rangeID]
		s.rangeMu.RUnlock()

		if !rangeOK {
			return nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, req.GetRange().GetRangeId())
		}

		if rd.GetGeneration() != req.GetRange().GetGeneration() {
			return nil, status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetGeneration(), req.GetRange().GetGeneration())
		}

		var replicaDesc *rfpb.ReplicaDescriptor
		replicas := append(rd.GetReplicas(), rd.GetStaging()...)
		for _, replica := range replicas {
			if replica.GetReplicaId() == req.GetReplicaId() {
				if replica.GetNhid() == s.NHID() {
					return nil, status.FailedPreconditionErrorf("cannot remove leader c%dn%d", rangeID, req.GetReplicaId())
				}
				replicaDesc = replica
				break
			}
		}

		var err error
		if replicaDesc != nil {
			// First, update the range descriptor information to reflect the
			// the node being removed.
			rd, err = s.markReplicaForRemovalFromRangeDescriptor(ctx, replicaDesc.GetRangeId(), replicaDesc.GetReplicaId(), req.GetRange())
			if err != nil {
				return nil, err
			}

			rsp.Range = rd
		}
	}

	if err := s.syncRequestDeleteReplica(ctx, rangeID, req.GetReplicaId()); err != nil {
		return rsp, status.InternalErrorf("nodehost.SyncRequestDeleteReplica failed for c%dn%d: %s", rangeID, req.GetReplicaId(), err)
	}

	return rsp, nil
}

func (s *Store) reserveReplicaIDs(ctx context.Context, rangeID uint64, n int) ([]uint64, error) {
	key := keys.MakeKey(constants.LastReplicaIDKeyPrefix, []byte(fmt.Sprintf("%d", rangeID)))
	newVal, err := s.sender.Increment(ctx, key, uint64(n))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, newVal-uint64(i))
	}
	return ids, nil
}

func (s *Store) reserveRangeID(ctx context.Context) (uint64, error) {
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
func (s *Store) UpdateRangeDescriptor(ctx context.Context, rangeID uint64, old, new *rfpb.RangeDescriptor) error {
	s.log.Infof("start to update range descriptor for rangeID %d to gen %d", rangeID, new.GetGeneration())
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
		stmt := txn.AddStatement()
		stmt.SetRangeDescriptor(mrd).SetBatch(localBatch)
		// Range Validation is required on the existing range to make sure that the
		// existing leader/range lease holder has the update-to-date range descriptor.
		// Normally, we don't want range validation for meta range, but since we
		// are changing meta range discriptor, validation is required.
		// See go/raft-range-validation-in-txn.
		stmt.SetRangeValidationRequired(true)
	} else {
		metaRangeBatch := rbuilder.NewBatchBuilder()
		metaRangeBatch.Add(metaRangeCasReq)

		stmt := txn.AddStatement()
		stmt.SetRangeDescriptor(old).SetBatch(localBatch)
		// Range Validation is required on the existing range to make sure that the
		// existing leader/range lease holder has the update-to-date range descriptor.
		// See go/raft-range-validation-in-txn.
		stmt.SetRangeValidationRequired(true)

		stmt = txn.AddStatement()
		stmt.SetRangeDescriptor(mrd).SetBatch(metaRangeBatch)
		// No range validation for meta range. See
		// go/raft-range-validation-in-txn for more details.
		stmt.SetRangeValidationRequired(false)
	}
	err = s.txnCoordinator.RunTxn(ctx, txn)
	if err != nil {
		return status.InternalErrorf("failed to update range descriptor for rangeID=%d, err: %s", rangeID, err)
	}
	s.log.Infof("range descriptor for rangeID %d updated to gen %d", rangeID, new.GetGeneration())

	if stagingDiff := len(new.GetStaging()) - len(old.GetStaging()); stagingDiff != 0 {
		metrics.RaftIntermediateReplicaCount.With(prometheus.Labels{
			metrics.RaftRangeIDLabel: strconv.Itoa(int(rangeID)),
			metrics.RaftReplicaState: "staging",
		}).Add(float64(stagingDiff))
	}

	if removedDiff := len(new.GetRemoved()) - len(old.GetRemoved()); removedDiff != 0 {
		metrics.RaftIntermediateReplicaCount.With(prometheus.Labels{
			metrics.RaftRangeIDLabel: strconv.Itoa(int(rangeID)),
			metrics.RaftReplicaState: "removed",
		}).Add(float64(removedDiff))
	}

	return nil
}

func (s *Store) addStagingReplicaToRangeDescriptor(ctx context.Context, rangeID, replicaID uint64, nhid string, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Staging = append(newDescriptor.GetStaging(), &rfpb.ReplicaDescriptor{
		RangeId:   rangeID,
		ReplicaId: replicaID,
		Nhid:      proto.String(nhid),
	})
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if err := s.UpdateRangeDescriptor(ctx, rangeID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, rangeID, replicaID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)

	for i, replica := range newDescriptor.GetStaging() {
		if replica.GetReplicaId() == replicaID {
			newDescriptor.Replicas = append(newDescriptor.GetReplicas(), newDescriptor.Staging[i])
			newDescriptor.Staging = append(newDescriptor.Staging[:i], newDescriptor.Staging[i+1:]...)
			break
		}
	}
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	newDescriptor.LastAddedReplicaId = proto.Uint64(replicaID)
	newDescriptor.LastReplicaAddedAtUsec = proto.Int64(time.Now().UnixMicro())
	if err := s.UpdateRangeDescriptor(ctx, rangeID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}

	return newDescriptor, nil
}

func (s *Store) markReplicaForRemovalFromRangeDescriptor(ctx context.Context, rangeID, replicaID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Replicas {
		if replica.GetReplicaId() == replicaID {
			newDescriptor.Removed = append(newDescriptor.Removed, newDescriptor.Replicas[i])
			newDescriptor.Replicas = append(newDescriptor.Replicas[:i], newDescriptor.Replicas[i+1:]...)
			break
		}
	}
	for i, replica := range newDescriptor.Staging {
		if replica.GetReplicaId() == replicaID {
			newDescriptor.Removed = append(newDescriptor.Removed, newDescriptor.Staging[i])
			newDescriptor.Staging = append(newDescriptor.Staging[:i], newDescriptor.Staging[i+1:]...)
		}

	}
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if newDescriptor.GetLastAddedReplicaId() == replicaID {
		newDescriptor.LastAddedReplicaId = nil
		newDescriptor.LastReplicaAddedAtUsec = nil
	}
	if err := s.UpdateRangeDescriptor(ctx, rangeID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (s *Store) removeReplicaFromRangeDescriptor(ctx context.Context, rangeID, replicaID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	s.log.Infof("removing c%dn%d from range descriptor", rangeID, replicaID)
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Removed {
		if replica.GetReplicaId() == replicaID {
			newDescriptor.Removed = append(newDescriptor.Removed[:i], newDescriptor.Removed[i+1:]...)
			break
		}
	}
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if err := s.UpdateRangeDescriptor(ctx, rangeID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (store *Store) scanReplicas(ctx context.Context, scanInterval time.Duration) {
	scanDelay := store.clock.NewTicker(scanInterval)
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
	if !s.isLeader(localReplica.RangeID(), localReplica.ReplicaID()) {
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

	rd = s.lookupRange(localReplica.RangeID())
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

func (s *Store) TestingWaitForGC(ctx context.Context) error {
	return s.usages.TestingWaitForGC(ctx)
}

func (s *Store) TestingFlush() {
	s.db.Flush()
}

func (s *Store) refreshMetrics(ctx context.Context) {
	ticker := s.clock.NewTicker(metricsRefreshPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			if err := s.updatePebbleMetrics(); err != nil {
				log.Warningf("[%s] could not update pebble metrics: %s", constants.CacheName, err)
			}

			if fsu, err := s.getFileSystemUsage(); err != nil {
				log.Warningf("[%s] could not update file system usage: %s", constants.CacheName, err)
			} else {
				metrics.DiskCacheFilesystemTotalBytes.With(prometheus.Labels{metrics.CacheNameLabel: constants.CacheName}).Set(float64(fsu.Total))
				metrics.DiskCacheFilesystemAvailBytes.With(prometheus.Labels{metrics.CacheNameLabel: constants.CacheName}).Set(float64(fsu.Avail))
			}
			metrics.RaftStoreEventsChanSize.Set(float64(len(s.events)))
		}
	}
}

func (s *Store) getFileSystemUsage() (gosigar.FileSystemUsage, error) {
	fsu := gosigar.FileSystemUsage{}
	err := fsu.Get(s.rootDir)
	return fsu, err
}

func (s *Store) updatePebbleMetrics() error {
	db, err := s.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	m := db.Metrics()
	om := s.oldMetrics

	s.metricsCollector.UpdateMetrics(m, om, constants.CacheName)
	s.oldMetrics = *m

	return nil
}
