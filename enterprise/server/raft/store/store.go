package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/leasekeeper"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagetracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v4"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	readBufSizeBytes = 1000000 // 1MB
)

type Store struct {
	env        environment.Env
	rootDir    string
	grpcAddr   string
	partitions []disk.Partition

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	registry      registry.NodeRegistry
	listener      *listener.RaftListener
	grpcServer    *grpc.Server
	apiClient     *client.APIClient
	liveness      *nodeliveness.Liveness
	log           log.Logger

	db     pebble.IPebbleDB
	leaser pebble.Leaser

	rangeMu    sync.RWMutex
	openRanges map[uint64]*rfpb.RangeDescriptor

	leaseKeeper *leasekeeper.LeaseKeeper
	replicas    sync.Map // map of uint64 rangeID -> *replica.Replica

	eventsMu       sync.Mutex
	events         chan events.Event
	eventListeners []chan events.Event

	usages *usagetracker.Tracker

	metaRangeMu   sync.Mutex
	metaRangeData []byte

	fileStorer filestore.Store

	eg       *errgroup.Group
	egCancel context.CancelFunc
}

func New(env environment.Env, rootDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, registry registry.NodeRegistry, listener *listener.RaftListener, apiClient *client.APIClient, grpcAddress string, partitions []disk.Partition) (*Store, error) {
	nodeLiveness := nodeliveness.New(nodeHost.ID(), sender)

	nhLog := log.NamedSubLogger(nodeHost.ID())
	eventsChan := make(chan events.Event, 100)
	s := &Store{
		env:           env,
		rootDir:       rootDir,
		grpcAddr:      grpcAddress,
		nodeHost:      nodeHost,
		partitions:    partitions,
		gossipManager: gossipManager,
		sender:        sender,
		registry:      registry,
		listener:      listener,
		apiClient:     apiClient,
		liveness:      nodeLiveness,
		log:           nhLog,

		rangeMu:    sync.RWMutex{},
		openRanges: make(map[uint64]*rfpb.RangeDescriptor),

		leaseKeeper: leasekeeper.New(nodeHost, nhLog, nodeLiveness, listener, eventsChan),
		replicas:    sync.Map{},

		eventsMu:       sync.Mutex{},
		events:         eventsChan,
		eventListeners: make([]chan events.Event, 0),

		metaRangeMu:   sync.Mutex{},
		metaRangeData: make([]byte, 0),
		fileStorer:    filestore.New(),
	}

	db, err := pebble.Open(rootDir, "raft_store", &pebble.Options{})
	if err != nil {
		return nil, err
	}
	s.db = db
	s.leaser = pebble.NewDBLeaser(db)

	usages, err := usagetracker.New(s, gossipManager, s.NodeDescriptor(), partitions, s.AddEventListener())
	if err != nil {
		return nil, err
	}
	s.usages = usages

	gossipManager.AddListener(s)
	statusz.AddSection("raft_store", "Store", s)

	go s.queryForMetarange()
	go s.updateTags()

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

func (s *Store) queryForMetarange() {
	start := time.Now()
	stream, err := s.gossipManager.Query(constants.MetaRangeTag, nil, nil)
	if err != nil {
		s.log.Errorf("Error querying for metarange: %s", err)
	}
	for p := range stream.ResponseCh() {
		s.setMetaRangeBuf(p.Payload)
		stream.Close()
		s.log.Infof("Discovered metarange in %s", time.Since(start))
		return
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
		return replicas[i].ShardID < replicas[j].ShardID
	})
	for _, r := range replicas {
		replicaName := fmt.Sprintf("  Shard: %5d   Replica: %5d", r.ShardID, r.ReplicaID)
		ru, err := r.Usage()
		if err != nil {
			buf += fmt.Sprintf("%s error: %s\n", replicaName, err)
			continue
		}
		isLeader := 0
		if rd := s.lookupRange(r.ShardID); rd != nil {
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

	ch := make(chan events.Event, 10)
	s.eventListeners = append(s.eventListeners, ch)
	return ch
}

func (s *Store) Start() error {
	// A grpcServer is run which is responsible for presenting a meta API
	// to manage raft nodes on each host, as well as an API to shuffle data
	// around between nodes, outside of raft.
	grpcOptions := grpc_server.CommonGRPCServerOptions(s.env)
	s.grpcServer = grpc.NewServer(grpcOptions...)
	reflection.Register(s.grpcServer)
	grpc_prometheus.Register(s.grpcServer)
	rfspb.RegisterApiServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return err
	}
	go func() {
		s.grpcServer.Serve(lis)
	}()

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
	s.leaseKeeper.Start()

	return nil
}

func (s *Store) Stop(ctx context.Context) error {
	now := time.Now()
	defer func() {
		s.log.Infof("Store shutdown finished in %s", time.Since(now))
	}()

	s.egCancel()
	s.leaseKeeper.Stop()
	s.liveness.Release()
	s.eg.Wait()

	s.log.Info("Store: waitgroups finished")
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
		if !clusterInfo.IsLeader {
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

	s.leaseKeeper.AddRange(rd)
	// Start goroutines for these so that Adding ranges is quick.
	go s.updateTags()
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
	s.leaseKeeper.RemoveRange(rd)
	go s.updateTags()
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
		return s.leaseKeeper.HaveLease(r.ShardID)
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

func (s *Store) isLeader(shardID uint64) bool {
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

// Snapshots the cluster *on this node*. This is a local operation and does not
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

func (s *Store) AddPeer(ctx context.Context, sourceShardID, newShardID uint64) error {
	defer canary.Start("AddPeer", 10*time.Second)()
	rd := s.lookupRange(sourceShardID)
	if rd == nil {
		return status.FailedPreconditionErrorf("cluster %d not found on this node", sourceShardID)
	}
	sourceReplica, err := s.GetReplica(rd.GetRangeId())
	if err != nil || sourceReplica == nil {
		return status.FailedPreconditionErrorf("range %d not found on this node", rd.GetRangeId())
	}
	initialMembers := make(map[uint64]string)
	for _, replica := range rd.GetReplicas() {
		nhid, _, err := s.registry.ResolveNHID(replica.GetShardId(), replica.GetReplicaId())
		if err != nil {
			return status.InternalErrorf("could not resolve node host ID: %s", err)
		}
		initialMembers[replica.GetReplicaId()] = nhid
	}

	_, err = s.StartShard(ctx, &rfpb.StartShardRequest{
		ShardId:       newShardID,
		ReplicaId:     sourceReplica.ReplicaID,
		InitialMember: initialMembers,
		Join:          false,
	})
	if status.IsAlreadyExistsError(err) {
		return nil
	}
	return err
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
		batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, req.GetShardId(), req.GetBatch())
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

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

func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	if _, err := s.LeasedRange(req.GetHeader()); err != nil {
		return nil, err
	}
	shardID := req.GetHeader().GetReplica().GetShardId()

	batch := req.GetBatch()
	batch.Header = req.GetHeader()

	batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, shardID, batch)
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

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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

func (s *Store) Usage() *rfpb.StoreUsage {
	su := &rfpb.StoreUsage{
		Node: s.NodeDescriptor(),
	}

	s.rangeMu.Lock()
	su.ReplicaCount = int64(len(s.openRanges))
	s.rangeMu.Unlock()

	su.LeaseCount = s.leaseKeeper.LeaseCount()
	s.replicas.Range(func(key, value any) bool {
		r, _ := value.(*replica.Replica)
		ru, err := r.Usage()
		if err != nil {
			return true // keep going
		}
		su.ReadQps += ru.GetReadQps()
		su.RaftProposeQps += ru.GetRaftProposeQps()
		su.TotalBytesUsed += ru.GetEstimatedDiskBytesUsed()
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
	return su
}

func (s *Store) updateTags() error {
	storeTags := make(map[string]string, 0)

	zone, err := resources.GetZone()
	if err == nil {
		storeTags[constants.ZoneTag] = zone
	} else {
		storeTags[constants.ZoneTag] = "local"
	}

	su := s.Usage()
	buf, err := proto.Marshal(su)
	if err != nil {
		return err
	}
	storeTags[constants.StoreUsageTag] = base64.StdEncoding.EncodeToString(buf)
	err = s.gossipManager.SetTags(storeTags)
	return err
}

func (s *Store) NodeDescriptor() *rfpb.NodeDescriptor {
	return &rfpb.NodeDescriptor{
		Nhid:        s.nodeHost.ID(),
		RaftAddress: s.nodeHost.RaftAddress(),
		GrpcAddress: s.grpcAddr,
	}
}

func (s *Store) NodeHost() *dragonboat.NodeHost {
	return s.nodeHost
}

func (s *Store) GetMembership(ctx context.Context, shardID uint64) ([]*rfpb.ReplicaDescriptor, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
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

	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(membership.Nodes))
	for replicaID := range membership.Nodes {
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			ShardId:   shardID,
			ReplicaId: replicaID,
		})
	}
	return replicas, nil
}

func (s *Store) SplitRange(ctx context.Context, req *rfpb.SplitRangeRequest) (*rfpb.SplitRangeResponse, error) {
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
	leftRange = proto.Clone(leftRange).(*rfpb.RangeDescriptor)
	shardID := leftRange.GetReplicas()[0].GetShardId()

	// Reserve new IDs for this cluster.
	newShardID, newRangeID, err := s.reserveClusterAndRangeID(ctx)
	if err != nil {
		return nil, status.InternalErrorf("could not reserve IDs for new cluster: %s", err)
	}

	// Find Split Point.
	fsp := rbuilder.NewBatchBuilder().Add(&rfpb.FindSplitPointRequest{})
	fspRsp, err := client.SyncProposeLocalBatch(ctx, s.nodeHost, shardID, fsp)
	if err != nil {
		return nil, status.InternalErrorf("find split point err: %s", err)
	}
	splitPointResponse, err := fspRsp.FindSplitPointResponse(0)
	if err != nil {
		return nil, err
	}

	// Bringup new peers.
	servers := make(map[string]string)
	for _, r := range leftRange.GetReplicas() {
		nhid, _, err := s.registry.ResolveNHID(r.GetShardId(), r.GetReplicaId())
		if err != nil {
			return nil, err
		}
		grpcAddr, _, err := s.registry.ResolveGRPC(r.GetShardId(), r.GetReplicaId())
		if err != nil {
			return nil, err
		}
		servers[nhid] = grpcAddr
	}
	bootstrapInfo := bringup.MakeBootstrapInfo(newShardID, 1, servers)
	if err := bringup.StartShard(ctx, s.apiClient, bootstrapInfo, rbuilder.NewBatchBuilder()); err != nil {
		return nil, err
	}

	// Assemble new range descriptor.
	newRightRange := proto.Clone(leftRange).(*rfpb.RangeDescriptor)
	newRightRange.Start = splitPointResponse.GetSplitKey()
	newRightRange.RangeId = newRangeID
	newRightRange.Generation += 1
	for i, r := range newRightRange.GetReplicas() {
		r.ReplicaId = uint64(i + 1)
		r.ShardId = newShardID
	}
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

	txn := rbuilder.NewTxn().AddStatement(shardID, leftBatch)
	txn = txn.AddStatement(newShardID, rightBatch)
	txn = txn.AddStatement(constants.MetaRangeID, metaBatch)
	if err := client.RunTxn(ctx, s.nodeHost, txn); err != nil {
		return nil, err
	}
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
			break
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

func (s *Store) getConfigChangeID(ctx context.Context, shardID uint64) (uint64, error) {
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
		return 0, err
	}
	if membership == nil {
		return 0, status.InternalErrorf("null cluster membership for cluster: %d", shardID)
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
		return nil, err
	}
	newReplicaID := replicaIDs[0]

	// Get the config change index for this cluster.
	configChangeID, err := s.getConfigChangeID(ctx, shardID)
	if err != nil {
		return nil, err
	}
	lastAppliedIndex, err := s.getLastAppliedIndex(&rfpb.Header{
		RangeId:    rd.GetRangeId(),
		Generation: rd.GetGeneration(),
	})
	if err != nil {
		return nil, err
	}

	// Gossip the address of the node that is about to be added.
	s.registry.Add(shardID, newReplicaID, node.GetNhid())
	s.registry.AddNode(node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node to the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddReplica(ctx, shardID, newReplicaID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return nil, err
	}

	// Start the cluster on the newly added node.
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return nil, err
	}
	_, err = c.StartShard(ctx, &rfpb.StartShardRequest{
		ShardId:          shardID,
		ReplicaId:        newReplicaID,
		Join:             true,
		LastAppliedIndex: lastAppliedIndex,
	})
	if err != nil {
		return nil, err
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	rd, err = s.addReplicaToRangeDescriptor(ctx, shardID, newReplicaID, rd)
	if err != nil {
		return nil, err
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

	var shardID, replicaID uint64
	for _, replica := range req.GetRange().GetReplicas() {
		if replica.GetReplicaId() == req.GetReplicaId() {
			shardID = replica.GetShardId()
			replicaID = replica.GetReplicaId()
			break
		}
	}
	if shardID == 0 && replicaID == 0 {
		return nil, status.FailedPreconditionErrorf("No node with id %d found in range: %+v", req.GetReplicaId(), req.GetRange())
	}

	configChangeID, err := s.getConfigChangeID(ctx, shardID)
	if err != nil {
		return nil, err
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestDeleteReplica(ctx, shardID, replicaID, configChangeID)
	})
	if err != nil {
		return nil, err
	}

	grpcAddr, _, err := s.registry.ResolveGRPC(shardID, replicaID)
	if err != nil {
		s.log.Errorf("error resolving grpc addr for c%dn%d: %s", shardID, replicaID, err)
		return nil, err
	}
	// Remove the data from the now stopped node.
	c, err := s.apiClient.Get(ctx, grpcAddr)
	if err != nil {
		s.log.Errorf("err getting api client: %s", err)
		return nil, err
	}
	_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ShardId:   shardID,
		ReplicaId: replicaID,
	})
	if err != nil {
		s.log.Errorf("remove data err: %s", err)
		return nil, err
	}

	// Finally, update the range descriptor information to reflect the
	// new membership of this range without the removed node.
	rd, err = s.removeReplicaFromRangeDescriptor(ctx, shardID, replicaID, req.GetRange())
	if err != nil {
		return nil, err
	}

	metrics.RaftMoves.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
		metrics.RaftMoveLabel:       "remove",
	}).Inc()

	return &rfpb.RemoveReplicaResponse{
		Range: rd,
	}, nil
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

	txn := rbuilder.NewTxn().AddStatement(shardID, localBatch)
	txn = txn.AddStatement(constants.MetaRangeID, metaRangeBatch)
	return client.RunTxn(ctx, s.nodeHost, txn)
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, shardID, replicaID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		ShardId:   shardID,
		ReplicaId: replicaID,
	})
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
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
	if err := s.updateRangeDescriptor(ctx, shardID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}
