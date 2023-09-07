package store

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/docker/go-units"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	readBufSizeBytes = 1000000 // 1MB

	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to receive ranges moved from other nodes.
	maximumDiskCapacity = .95

	// evictionCutoffThreshold is the point above which the cache will be
	// considered to be full and eviction will kick in.
	evictionCutoffThreshold = .90

	// How often stores wil check whether to gossip usage data if it is
	// sufficiently different from the last broadcast.
	storePartitionUsageCheckInterval = 15 * time.Second

	// How often stores can go without broadcasting usage information.
	// Usage data will be gossiped after this time if no updated were triggered
	// based on data changes.
	storePartitionUsageMaxAge = 15 * time.Minute

	// How old store partition usage data can be before we consider it invalid.
	storePartitionStalenessLimit = storePartitionUsageMaxAge * 2
)

var (
	partitionUsageDeltaGossipThreshold = flag.Int("cache.raft.partition_usage_delta_bytes_threshold", 100e6, "Gossip partition usage information if it has changed by more than this amount since the last gossip.")
	samplesPerEviction                 = flag.Int("cache.raft.samples_per_eviction", 20, "How many records to sample on each eviction")
	samplePoolSize                     = flag.Int("cache.raft.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")
	enableSplittingReplicas            = flag.Bool("cache.raft.enable_splitting_replicas", true, "If set, allow splitting oversize replicas")
)

type nodePartitionUsage struct {
	sizeBytes  int64
	lastUpdate time.Time
}

type partitionUsage struct {
	id    string
	store *Store

	mu  sync.Mutex
	lru *approxlru.LRU[*ReplicaSample]
	// Global view of usage, keyed by Node Host ID.
	nodes map[string]*nodePartitionUsage
	// Usage information for local replicas, keyed by Range ID.
	replicas map[uint64]*rfpb.PartitionMetadata
}

func (pu *partitionUsage) LocalSizeBytes() int64 {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	sizeBytes := int64(0)
	for _, r := range pu.replicas {
		sizeBytes += r.GetSizeBytes()
	}
	return sizeBytes
}

func (pu *partitionUsage) GlobalSizeBytes() int64 {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	sizeBytes := int64(0)
	for _, nu := range pu.nodes {
		sizeBytes += nu.sizeBytes
	}
	return sizeBytes
}

func (pu *partitionUsage) RemoteUpdate(nhid string, update *rfpb.PartitionMetadata) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	n, ok := pu.nodes[nhid]
	if !ok {
		n = &nodePartitionUsage{}
		pu.nodes[nhid] = n
	}
	n.lastUpdate = time.Now()
	n.sizeBytes = update.GetSizeBytes()

	// Prune stale data.
	for id, n := range pu.nodes {
		if time.Since(n.lastUpdate) > storePartitionStalenessLimit {
			delete(pu.nodes, id)
		}
	}
}

func (pu *partitionUsage) evict(ctx context.Context, sample *approxlru.Sample[*ReplicaSample]) (skip bool, err error) {
	deleteReq := rbuilder.NewBatchBuilder().Add(&rfpb.FileDeleteRequest{
		FileRecord: sample.Key.fileRecord,
	})
	rsp, err := client.SyncProposeLocalBatch(ctx, pu.store.nodeHost, sample.Key.header.GetReplica().GetClusterId(), deleteReq)
	if err != nil {
		return false, status.InternalErrorf("could not propose eviction: %s", err)
	}
	if err := rsp.AnyError(); err != nil {
		if status.IsNotFoundError(err) || status.IsOutOfRangeError(err) {
			log.Infof("Skipping eviction for %q: %s", sample.Key, err)
			return true, nil
		}
		return false, status.InternalErrorf("eviction request failed: %s", rsp.AnyError())
	}

	ageMillis := float64(time.Since(sample.Timestamp).Milliseconds())
	metrics.RaftEvictionAgeMsec.With(prometheus.Labels{metrics.PartitionID: pu.id}).Observe(ageMillis)

	globalSizeBytes := pu.GlobalSizeBytes()

	pu.mu.Lock()
	defer pu.mu.Unlock()
	// Update local replica information to reflect the eviction. Don't need
	// to wait for a proactive update from the replica.
	u, ok := pu.replicas[sample.Key.header.GetRangeId()]
	if ok {
		u.SizeBytes -= sample.SizeBytes
		u.TotalCount--
	} else {
		log.Warningf("eviction succeeded but range %d wasn't found", sample.Key.header.GetRangeId())
	}

	// Assume eviction on all stores is happening at a similar rate as on the
	// current store and update the usage information speculatively since we
	// don't know when we'll receive the next usage update from remote stores.
	// When we do receive updates from other stores they will overwrite our
	// speculative numbers.
	for _, npu := range pu.nodes {
		npu.sizeBytes -= int64(float64(sample.SizeBytes) * float64(globalSizeBytes) / float64(npu.sizeBytes))
		if npu.sizeBytes < 0 {
			npu.sizeBytes = 0
		}
	}

	return false, nil
}

func (pu *partitionUsage) sample(ctx context.Context, n int) ([]*approxlru.Sample[*ReplicaSample], error) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	totalCount := int64(0)
	sizeBytes := int64(0)
	for _, u := range pu.replicas {
		totalCount += u.GetTotalCount()
		sizeBytes += u.GetSizeBytes()
	}

	if totalCount == 0 {
		return nil, status.FailedPreconditionError("cannot sample empty partition")
	}

	var samples []*approxlru.Sample[*ReplicaSample]
	for len(samples) < n {
		rn := rand.Int63n(totalCount)
		count := int64(0)
		for rangeID, u := range pu.replicas {
			count += u.GetTotalCount()
			if rn < count {
				ps, err := pu.store.Sample(ctx, rangeID, pu.id, 1)
				if err != nil {
					return nil, status.InternalErrorf("could not sample partition %q: %s", pu.id, err)
				}
				samples = append(samples, ps...)
				break
			}
		}
	}
	return samples, nil
}

func (pu *partitionUsage) refresh(ctx context.Context, key *ReplicaSample) (skip bool, timestamp time.Time, err error) {
	rsp, err := pu.store.Metadata(ctx, &rfpb.MetadataRequest{Header: key.header, FileRecord: key.fileRecord})
	if err != nil {
		if status.IsNotFoundError(err) || status.IsOutOfRangeError(err) {
			log.Infof("Skipping refresh for %q: %s", key, err)
			return true, time.Time{}, nil
		}
		return false, time.Time{}, err
	}
	atime := time.UnixMicro(rsp.GetMetadata().GetLastAccessUsec())
	return false, atime, nil
}

type usageTracker struct {
	store         *Store
	gossipManager *gossip.GossipManager
	partitions    []disk.Partition

	quitChan      chan struct{}
	mu            sync.Mutex
	byRange       map[uint64]*rfpb.ReplicaUsage
	byPartition   map[string]*partitionUsage
	lastBroadcast map[string]*rfpb.PartitionMetadata
}

func newUsageTracker(store *Store, gossipManager *gossip.GossipManager, partitions []disk.Partition) (*usageTracker, error) {
	ut := &usageTracker{
		store:         store,
		gossipManager: gossipManager,
		partitions:    partitions,
		quitChan:      make(chan struct{}),
		byRange:       make(map[uint64]*rfpb.ReplicaUsage),
		byPartition:   make(map[string]*partitionUsage),
		lastBroadcast: make(map[string]*rfpb.PartitionMetadata),
	}

	for _, p := range partitions {
		u := &partitionUsage{
			id:       p.ID,
			store:    store,
			nodes:    make(map[string]*nodePartitionUsage),
			replicas: make(map[uint64]*rfpb.PartitionMetadata),
		}
		ut.byPartition[p.ID] = u
		maxSizeBytes := int64(evictionCutoffThreshold * float64(p.MaxSizeBytes))
		l, err := approxlru.New(&approxlru.Opts[*ReplicaSample]{
			SamplePoolSize:     *samplePoolSize,
			SamplesPerEviction: *samplesPerEviction,
			MaxSizeBytes:       maxSizeBytes,
			OnEvict: func(ctx context.Context, sample *approxlru.Sample[*ReplicaSample]) (skip bool, err error) {
				return u.evict(ctx, sample)
			},
			OnSample: func(ctx context.Context, n int) ([]*approxlru.Sample[*ReplicaSample], error) {
				return u.sample(ctx, n)
			},
			OnRefresh: func(ctx context.Context, key *ReplicaSample) (skip bool, timestamp time.Time, err error) {
				return u.refresh(ctx, key)
			},
		})
		if err != nil {
			return nil, err
		}
		l.Start()
		u.lru = l
	}

	go ut.broadcastLoop()
	gossipManager.AddListener(ut)
	return ut, nil
}

func (ut *usageTracker) Stop() {
	close(ut.quitChan)
	for _, p := range ut.byPartition {
		p.lru.Stop()
	}
}

func (ut *usageTracker) Statusz(ctx context.Context) string {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	buf := "Partitions:\n"
	for _, p := range ut.partitions {
		buf += fmt.Sprintf("\t%s\n", p.ID)
		u, ok := ut.byPartition[p.ID]
		if !ok {
			buf += "\t\tno data\n"
			continue
		}

		globalSizeBytes := u.GlobalSizeBytes()
		percentFull := (float64(globalSizeBytes) / float64(p.MaxSizeBytes)) * 100

		buf += fmt.Sprintf("\t\tCapacity: %s / %s (%2.2f%% full)\n", units.BytesSize(float64(globalSizeBytes)), units.BytesSize(float64(p.MaxSizeBytes)), percentFull)
		buf += "\t\tLocal Ranges:\n"

		u.mu.Lock()
		// Show ranges in a consistent order so that they don't jump around when
		// refreshing the statusz page.
		var rids []uint64
		for rid := range u.replicas {
			rids = append(rids, rid)
		}
		sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })

		for _, rid := range rids {
			pu, ok := u.replicas[rid]
			if !ok {
				continue
			}
			buf += fmt.Sprintf("\t\t\t%d: %s, %d records\n", rid, units.BytesSize(float64(pu.GetSizeBytes())), pu.GetTotalCount())
		}

		// Show nodes in a consistent order so that they don't jump around when
		// refreshing the statusz page.
		var nhids []string
		for nhid := range u.nodes {
			nhids = append(nhids, nhid)
		}
		sort.Strings(nhids)
		buf += "\t\tGlobal Usage:\n"
		for _, nhid := range nhids {
			nu, ok := u.nodes[nhid]
			if !ok {
				continue
			}
			buf += fmt.Sprintf("\t\t\t%s: %s\n", nhid, units.BytesSize(float64(nu.sizeBytes)))
		}
		u.mu.Unlock()
	}
	return buf
}

func (ut *usageTracker) OnEvent(updateType serf.EventType, event serf.Event) {
	if updateType != serf.EventUser {
		return
	}
	userEvent, ok := event.(serf.UserEvent)
	if !ok {
		return
	}
	if userEvent.Name != constants.NodePartitionUsageEvent {
		return
	}

	nu := &rfpb.NodePartitionUsage{}
	if err := proto.Unmarshal(userEvent.Payload, nu); err != nil {
		return
	}

	ut.RemoteUpdate(nu)
}

// RemoteUpdate processes a usage update broadcast by Raft stores.
// Note that this also includes data broadcast by the local store.
func (ut *usageTracker) RemoteUpdate(usage *rfpb.NodePartitionUsage) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	nhid := usage.GetNode().GetNhid()
	for _, pu := range usage.GetPartitionUsage() {
		lpu, ok := ut.byPartition[pu.GetPartitionId()]
		if !ok {
			log.Warningf("unknown partition %q", pu.GetPartitionId())
			continue
		}
		lpu.RemoteUpdate(nhid, pu)
	}

	// Propagate the updated usage to the LRU.
	for _, u := range ut.byPartition {
		u.lru.UpdateGlobalSizeBytes(u.GlobalSizeBytes())
	}
}

// LocalUpdate processes a usage update from a local replica.
func (ut *usageTracker) LocalUpdate(rangeID uint64, usage *rfpb.ReplicaUsage) {
	haveLease := ut.store.haveLease(rangeID)

	ut.mu.Lock()
	defer ut.mu.Unlock()

	ut.byRange[rangeID] = usage

	// Partition usage is only tracked for leased ranges.
	if !haveLease {
		ut.removeRangePartitions(rangeID)
		return
	}

	for _, u := range usage.GetPartitions() {
		ud, ok := ut.byPartition[u.GetPartitionId()]
		if !ok {
			log.Warningf("unknown partition %q", u.GetPartitionId())
			continue
		}
		ud.replicas[rangeID] = u
	}

	// Propagate the updated usage to the LRU.
	for _, u := range ut.byPartition {
		u.lru.UpdateLocalSizeBytes(u.LocalSizeBytes())
	}
}

func (ut *usageTracker) removeRangePartitions(rangeID uint64) {
	for _, u := range ut.byPartition {
		delete(u.replicas, rangeID)
	}
}

func (ut *usageTracker) RemoveRange(rangeID uint64) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	delete(ut.byRange, rangeID)

	ut.removeRangePartitions(rangeID)
}

func (ut *usageTracker) RangeUsages() []*rfpb.ReplicaUsage {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	var us []*rfpb.ReplicaUsage
	for _, u := range ut.byRange {
		us = append(us, u)
	}
	return us
}

func (ut *usageTracker) computeUsage() *rfpb.NodePartitionUsage {
	usages := ut.store.RefreshReplicaUsages()
	for _, u := range usages {
		ut.LocalUpdate(u.GetRangeId(), u)
	}

	ut.mu.Lock()
	defer ut.mu.Unlock()
	nu := &rfpb.NodePartitionUsage{
		Node: ut.store.NodeDescriptor(),
	}

	for _, p := range ut.partitions {
		up := &rfpb.PartitionMetadata{
			PartitionId: p.ID,
		}
		if u, ok := ut.byPartition[p.ID]; ok {
			// Sum up total partition usage. Other nodes don't need to know
			// about individual ranges.
			for _, ru := range u.replicas {
				up.SizeBytes += ru.GetSizeBytes()
				up.TotalCount += ru.GetTotalCount()
			}
		}
		nu.PartitionUsage = append(nu.PartitionUsage, up)
	}
	return nu
}

func (ut *usageTracker) broadcastLoop() {
	idleTimer := time.NewTimer(storePartitionUsageMaxAge)

	for {
		select {
		case <-ut.quitChan:
			return
		case <-time.After(storePartitionUsageCheckInterval):
			if !idleTimer.Stop() {
				<-idleTimer.C
			}
			idleTimer.Reset(storePartitionUsageMaxAge)
			if err := ut.broadcast(false /*=force*/); err != nil {
				log.Warningf("could not gossip node partition usage info: %s", err)
			}
		case <-idleTimer.C:
			if err := ut.broadcast(true /*=force*/); err != nil {
				log.Warningf("could not gossip node partition usage info: %s", err)
			}
		}
	}
}

func (ut *usageTracker) broadcast(force bool) error {
	usage := ut.computeUsage()

	// If not forced, check whether there's enough changes to force a broadcast.
	if !force {
		significantChange := false
		ut.mu.Lock()
		for _, u := range usage.GetPartitionUsage() {
			lb, ok := ut.lastBroadcast[u.GetPartitionId()]
			if !ok || math.Abs(float64(u.GetSizeBytes()-lb.GetSizeBytes())) > float64(*partitionUsageDeltaGossipThreshold) {
				significantChange = true
				break
			}
		}
		ut.mu.Unlock()
		if !significantChange {
			return nil
		}
	}

	buf, err := proto.Marshal(usage)
	if err != nil {
		return err
	}

	if err := ut.gossipManager.SendUserEvent(constants.NodePartitionUsageEvent, buf, false /*coalesce*/); err != nil {
		return err
	}

	ut.mu.Lock()
	defer ut.mu.Unlock()
	for _, u := range usage.GetPartitionUsage() {
		ut.lastBroadcast[u.GetPartitionId()] = u
	}

	return nil
}

type Store struct {
	rootDir    string
	grpcAddr   string
	partitions []disk.Partition

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	registry      registry.NodeRegistry
	grpcServer    *grpc.Server
	apiClient     *client.APIClient
	liveness      *nodeliveness.Liveness
	log           log.Logger

	db     pebble.IPebbleDB
	leaser pebble.Leaser

	rangeMu    sync.RWMutex
	openRanges map[uint64]*rfpb.RangeDescriptor

	leases   sync.Map // map of uint64 rangeID -> *rangelease.Lease
	replicas sync.Map // map of uint64 rangeID -> *replica.Replica
	usages   *usageTracker

	metaRangeData   string
	leaderUpdatedCB listener.LeaderCB

	fileStorer filestore.Store
	eg         *errgroup.Group
}

func New(rootDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, registry registry.NodeRegistry, apiClient *client.APIClient, grpcAddress string, partitions []disk.Partition) (*Store, error) {
	s := &Store{
		rootDir:       rootDir,
		grpcAddr:      grpcAddress,
		nodeHost:      nodeHost,
		partitions:    partitions,
		gossipManager: gossipManager,
		sender:        sender,
		registry:      registry,
		apiClient:     apiClient,
		liveness:      nodeliveness.New(nodeHost.ID(), sender),
		log:           log.NamedSubLogger(nodeHost.ID()),

		rangeMu:    sync.RWMutex{},
		openRanges: make(map[uint64]*rfpb.RangeDescriptor),

		leases:   sync.Map{},
		replicas: sync.Map{},

		metaRangeData: "",
		fileStorer:    filestore.New(),
		eg:            &errgroup.Group{},
	}

	db, err := pebble.Open(rootDir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	s.db = db
	s.leaser = pebble.NewDBLeaser(db)

	s.leaderUpdatedCB = listener.LeaderCB(s.onLeaderUpdated)
	usages, err := newUsageTracker(s, gossipManager, partitions)
	if err != nil {
		return nil, err
	}
	s.usages = usages

	gossipManager.AddListener(s)

	listener.DefaultListener().RegisterLeaderUpdatedCB(&s.leaderUpdatedCB)
	statusz.AddSection("raft_store", "Store", s)

	go s.updateTags()

	return s, nil
}

func (s *Store) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += fmt.Sprintf("NHID: %s\n", s.nodeHost.ID())
	buf += fmt.Sprintf("Liveness lease: %s\n", s.liveness)

	replicas := make([]*replica.Replica, 0)
	s.replicas.Range(func(key, value any) bool {
		if r, ok := value.(*replica.Replica); ok {
			replicas = append(replicas, r)
		}
		return true
	})
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].ClusterID < replicas[j].ClusterID
	})
	buf += "Replicas:\n"
	for _, r := range replicas {
		cluster := fmt.Sprintf("(c%03dn%03d)", r.ClusterID, r.NodeID)

		usage, err := r.Usage()
		if err != nil {
			buf += fmt.Sprintf("\t%s error: %s\n", cluster, err)
			continue
		}

		extra := ""
		if rd := s.lookupRange(r.ClusterID); rd != nil {
			if rlIface, ok := s.leases.Load(rd.GetRangeId()); ok && rlIface.(*rangelease.Lease).Valid() {
				extra += "Leaseholder"
			}
		}
		buf += fmt.Sprintf("\t%s Usage: %s %s\n", cluster, units.BytesSize(float64(usage.GetEstimatedDiskBytesUsed())), extra)
	}
	buf += s.usages.Statusz(ctx)
	buf += "</pre>"
	return buf
}

func (s *Store) onLeaderUpdated(info raftio.LeaderInfo) {
	if !s.isLeader(info.ClusterID) {
		return
	}
	rd := s.lookupRange(info.ClusterID)
	if rd == nil {
		return
	}
	go s.maybeAcquireRangeLease(rd)
}

func (s *Store) NotifyUsage(usage *rfpb.ReplicaUsage) {
	// TODO(tylerw): do something with usage here. Split?
}

func (s *Store) Start() error {
	// A grpcServer is run which is responsible for presenting a meta API
	// to manage raft nodes on each host, as well as an API to shuffle data
	// around between nodes, outside of raft.
	s.grpcServer = grpc.NewServer()
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

	return nil
}

func (s *Store) Stop(ctx context.Context) error {
	s.dropLeadershipForShutdown()
	if err := s.eg.Wait(); err != nil {
		return err
	}
	s.log.Info("Store: waitgroups finished")

	listener.DefaultListener().UnregisterLeaderUpdatedCB(&s.leaderUpdatedCB)
	return grpc_server.GRPCShutdown(ctx, s.grpcServer)
}

func (s *Store) lookupRange(clusterID uint64) *rfpb.RangeDescriptor {
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()

	for _, rangeDescriptor := range s.openRanges {
		if len(rangeDescriptor.GetReplicas()) == 0 {
			continue
		}
		if clusterID == rangeDescriptor.GetReplicas()[0].GetClusterId() {
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
	for _, clusterInfo := range nodeHostInfo.ClusterInfoList {
		clusterInfo := clusterInfo
		if !clusterInfo.IsLeader {
			continue
		}

		// Pick the first node in the map that isn't us. Map ordering is
		// random; which is a good thing, it means we're randomly picking
		// another node in the cluster and requesting they take the lead.
		for nodeID := range clusterInfo.Nodes {
			if nodeID == clusterInfo.NodeID {
				continue
			}
			eg.Go(func() error {
				if err := s.nodeHost.RequestLeaderTransfer(clusterInfo.ClusterID, nodeID); err != nil {
					log.Warningf("Error transferring leadership: %s", err)
				}
				return nil
			})
			break
		}
	}
	eg.Wait()
}

func (s *Store) maybeAcquireRangeLease(rd *rfpb.RangeDescriptor) {
	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("Not acquiring range %d lease: no replicas", rd.GetRangeId())
		return
	}

	clusterID := rd.GetReplicas()[0].GetClusterId()
	if !s.isLeader(clusterID) {
		return
	}

	rangeID := rd.GetRangeId()
	rlIface, _ := s.leases.LoadOrStore(rangeID, rangelease.New(s.nodeHost, clusterID, s.liveness, rd))
	rl, ok := rlIface.(*rangelease.Lease)
	if !ok {
		alert.UnexpectedEvent("unexpected_leases_map_type_error")
		return
	}

	for attempt := 0; attempt < 3; attempt++ {
		if !s.isLeader(clusterID) {
			break
		}
		if rl.Valid() {
			break
		}
		err := rl.Lease()
		if err == nil {
			break
		}
		s.log.Warningf("Error leasing range: %s: %s, will try again.", rl, err)
	}
	s.updateTags()
}

func (s *Store) releaseRangeLease(rangeID uint64) {
	rlIface, ok := s.leases.Load(rangeID)
	if !ok {
		return
	}
	rl, ok := rlIface.(*rangelease.Lease)
	if !ok {
		alert.UnexpectedEvent("unexpected_leases_map_type_error")
		return
	}
	s.leases.Delete(rangeID)
	if rl.Valid() {
		rl.Release()
		s.updateTags()
	}
}

func (s *Store) GetRange(clusterID uint64) *rfpb.RangeDescriptor {
	return s.lookupRange(clusterID)
}

func (s *Store) updateUsages(r *replica.Replica) error {
	usage, err := r.Usage()
	if err != nil {
		return err
	}
	s.usages.LocalUpdate(usage.GetRangeId(), usage)
	return nil
}

// We need to implement the Add/RemoveRange interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Adding range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetLeft(), rd.GetRight(), rd.GetGeneration())
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

	if rd.GetLeft() == nil && rd.GetRight() == nil {
		s.log.Debugf("range %d has no bounds (yet?)", rd.GetRangeId())
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
		go s.gossipManager.SetTags(map[string]string{constants.MetaRangeTag: string(buf)})
	}

	// Start goroutines for these so that Adding ranges is quick.
	go s.maybeAcquireRangeLease(rd)
	go s.updateTags()
	go s.updateUsages(r)
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Removing range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetLeft(), rd.GetRight(), rd.GetGeneration())
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

	// Start goroutines for these so that Removing ranges is quick.
	go s.releaseRangeLease(rd.GetRangeId())
	go s.updateTags()
}

type ReplicaSample struct {
	header     *rfpb.Header
	key        string
	fileRecord *rfpb.FileRecord
}

func (rs *ReplicaSample) ID() string {
	return rs.key
}

func (rs *ReplicaSample) String() string {
	return fmt.Sprintf("hdr: %+v, key: %s", rs.header, rs.key)
}

func (s *Store) Sample(ctx context.Context, rangeID uint64, partition string, n int) ([]*approxlru.Sample[*ReplicaSample], error) {
	r, rd, err := s.replicaForRange(rangeID)
	if err != nil {
		return nil, err
	}
	samples, err := r.Sample(ctx, partition, n)
	if err != nil {
		return nil, err
	}

	var rs []*approxlru.Sample[*ReplicaSample]
	for _, samp := range samples {
		pebbleKey, err := s.fileStorer.PebbleKey(samp.GetFileRecord())
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := pebbleKey.Bytes(filestore.Version2)
		if err != nil {
			return nil, err
		}
		sampleKey := &ReplicaSample{
			header: &rfpb.Header{
				Replica:    rd.GetReplicas()[0],
				RangeId:    rd.GetRangeId(),
				Generation: rd.GetGeneration(),
			},
			key:        string(fileMetadataKey),
			fileRecord: samp.GetFileRecord(),
		}
		rs = append(rs, &approxlru.Sample[*ReplicaSample]{
			Key:       sampleKey,
			SizeBytes: samp.GetStoredSizeBytes(),
			Timestamp: time.UnixMicro(samp.GetLastAccessUsec()),
		})
	}
	return rs, nil
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
		return nil, nil, status.FailedPreconditionError("Nil header not allowed")
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

func (s *Store) haveLease(rangeID uint64) bool {
	if rlIface, ok := s.leases.Load(rangeID); ok {
		if rl, ok := rlIface.(*rangelease.Lease); ok {
			if rl.Valid() {
				return true
			}
		} else {
			alert.UnexpectedEvent("unexpected_leases_map_type_error")
		}
	}
	return false
}

// LeasedRange verifies that the header is valid and the client is using
// an up-to-date range descriptor. It also checks that a local replica owns
// the range lease for the requested range.
func (s *Store) LeasedRange(header *rfpb.Header) (*replica.Replica, error) {
	r, rd, err := s.validatedRange(header)
	if err != nil {
		return nil, err
	}

	if s.haveLease(header.GetRangeId()) {
		return r, nil
	}

	go s.maybeAcquireRangeLease(rd)
	return nil, status.OutOfRangeErrorf("%s: no lease found for range: %d", constants.RangeLeaseInvalidMsg, header.GetRangeId())
}

func (s *Store) ReplicaFactoryFn(clusterID, nodeID uint64) dbsm.IOnDiskStateMachine {
	r := replica.New(s.leaser, clusterID, nodeID, s, s.partitions)
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

func (s *Store) isLeader(clusterID uint64) bool {
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})
	if nodeHostInfo == nil {
		return false
	}
	for _, clusterInfo := range nodeHostInfo.ClusterInfoList {
		if clusterInfo.ClusterID == clusterID {
			return clusterInfo.IsLeader
		}
	}
	return false
}

func (s *Store) TransferLeadership(ctx context.Context, req *rfpb.TransferLeadershipRequest) (*rfpb.TransferLeadershipResponse, error) {
	if err := s.nodeHost.RequestLeaderTransfer(req.GetClusterId(), req.GetTargetNodeId()); err != nil {
		return nil, err
	}
	return &rfpb.TransferLeadershipResponse{}, nil
}

func (s *Store) waitForReplicaOpen(ctx context.Context, clusterID uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if rd := s.lookupRange(clusterID); rd != nil {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Snapshots the cluster *on this node*. This is a local operation and does not
// create a snapshot on other nodes that are members of this cluster.
func (s *Store) SnapshotCluster(ctx context.Context, clusterID uint64) error {
	if _, ok := ctx.Deadline(); !ok {
		c, cancel := context.WithTimeout(ctx, client.DefaultContextTimeout)
		defer cancel()
		ctx = c
	}
	opts := dragonboat.SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         0,
	}

	// Wait for the cluster to be opened on this store
	if err := s.waitForReplicaOpen(ctx, clusterID); err != nil {
		return err
	}

	// Wait a little longer for the replica to accept the snapshot request
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := s.nodeHost.SyncRequestSnapshot(ctx, clusterID, opts)
			if err == nil {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (s *Store) AddPeer(ctx context.Context, sourceClusterID, newClusterID uint64) error {
	rd := s.lookupRange(sourceClusterID)
	if rd == nil {
		return status.FailedPreconditionErrorf("cluster %d not found on this node", sourceClusterID)
	}
	sourceReplica, err := s.GetReplica(rd.GetRangeId())
	if err != nil || sourceReplica == nil {
		return status.FailedPreconditionErrorf("range %d not found on this node", rd.GetRangeId())
	}
	initialMembers := make(map[uint64]string)
	for _, replica := range rd.GetReplicas() {
		nhid, _, err := s.registry.ResolveNHID(replica.GetClusterId(), replica.GetNodeId())
		if err != nil {
			return status.InternalErrorf("could not resolve node host ID: %s", err)
		}
		initialMembers[replica.GetNodeId()] = nhid
	}

	waitErr := make(chan error, 1)
	// Wait for the notification that the cluster node is ready on the local
	// nodehost.
	go func() {
		err := listener.DefaultListener().WaitForClusterReady(ctx, newClusterID)
		waitErr <- err
		close(waitErr)
	}()

	s.log.Infof("Starting new raft node c%dn%d", newClusterID, sourceReplica.NodeID)
	rc := raftConfig.GetRaftConfig(newClusterID, sourceReplica.NodeID)
	err = s.nodeHost.StartOnDiskCluster(initialMembers, false /*join*/, s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return err
	}

	err, ok := <-waitErr
	if ok && err != nil {
		s.log.Errorf("WaitForClusterReady err: %s", err)
		return err
	}

	return nil
}

func (s *Store) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	rc := raftConfig.GetRaftConfig(req.GetClusterId(), req.GetNodeId())

	waitErr := make(chan error, 1)
	// Wait for the notification that the cluster node is ready on the local
	// nodehost.
	go func() {
		err := listener.DefaultListener().WaitForClusterReady(ctx, req.GetClusterId())
		waitErr <- err
		close(waitErr)
	}()

	err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), req.GetJoin(), s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return nil, err
	}

	err, ok := <-waitErr
	if ok && err != nil {
		s.log.Errorf("WaitForClusterReady err: %s", err)
		return nil, err
	}

	if req.GetLastAppliedIndex() > 0 {
		if err := s.waitForReplicaToCatchUp(ctx, req.GetClusterId(), req.GetLastAppliedIndex()); err != nil {
			return nil, err
		}
	}

	rsp := &rfpb.StartClusterResponse{}
	if req.GetBatch() == nil || len(req.GetInitialMember()) == 0 {
		return rsp, nil
	}

	// If we are the last member in the cluster, we'll do the syncPropose.
	nodeIDs := make([]uint64, 0, len(req.GetInitialMember()))
	for nodeID := range req.GetInitialMember() {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	if req.GetNodeId() == nodeIDs[len(nodeIDs)-1] {
		batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, req.GetClusterId(), req.GetBatch())
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

func (s *Store) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	err := client.RunNodehostFn(ctx, func(ctx context.Context) error {
		err := s.nodeHost.SyncRemoveData(ctx, req.GetClusterId(), req.GetNodeId())
		if err == dragonboat.ErrClusterNotStopped {
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
	if _, _, err := s.validatedRange(req.GetHeader()); err != nil {
		return nil, err
	}
	clusterID := req.GetHeader().GetReplica().GetClusterId()

	batch := req.GetBatch()
	batch.Header = req.GetHeader()

	batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, batch)
	if err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return nil, status.OutOfRangeErrorf("%s: cluster %d not found", constants.RangeLeaseInvalidMsg, clusterID)
		}
		return nil, err
	}
	return &rfpb.SyncProposeResponse{
		Batch: batchResponse,
	}, nil
}

func (s *Store) SyncRead(ctx context.Context, req *rfpb.SyncReadRequest) (*rfpb.SyncReadResponse, error) {
	clusterID := req.GetHeader().GetReplica().GetClusterId()
	batchResponse, err := client.SyncReadLocal(ctx, s.nodeHost, clusterID, req.GetBatch())
	if err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return nil, status.OutOfRangeErrorf("%s: cluster %d not found", constants.RangeLeaseInvalidMsg, clusterID)
		}
		return nil, err
	}

	return &rfpb.SyncReadResponse{
		Batch: batchResponse,
	}, nil
}

func (s *Store) FindMissing(ctx context.Context, req *rfpb.FindMissingRequest) (*rfpb.FindMissingResponse, error) {
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}
	missing, err := r.FindMissing(ctx, req.GetHeader(), req.GetFileRecord())
	if err != nil {
		return nil, err
	}
	return &rfpb.FindMissingResponse{
		FileRecord: missing,
	}, nil
}

func (s *Store) GetMulti(ctx context.Context, req *rfpb.GetMultiRequest) (*rfpb.GetMultiResponse, error) {
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}
	data, err := r.GetMulti(ctx, req.GetHeader(), req.GetFileRecord())
	if err != nil {
		return nil, err
	}
	return &rfpb.GetMultiResponse{
		Data: data,
	}, nil
}

type streamWriter struct {
	stream rfspb.Api_ReadServer
}

func (w *streamWriter) Write(buf []byte) (int, error) {
	err := w.stream.Send(&rfpb.ReadResponse{
		Data: buf,
	})
	return len(buf), err
}

func (s *Store) Metadata(ctx context.Context, req *rfpb.MetadataRequest) (*rfpb.MetadataResponse, error) {
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}
	md, err := r.Metadata(ctx, req.GetHeader(), req.GetFileRecord())
	if err != nil {
		return nil, err
	}

	return &rfpb.MetadataResponse{Metadata: md}, nil
}

func (s *Store) Read(req *rfpb.ReadRequest, stream rfspb.Api_ReadServer) error {
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return err
	}

	readCloser, err := r.Reader(stream.Context(), req.GetHeader(), req.GetFileRecord(), req.GetOffset(), req.GetLimit())
	if err != nil {
		return err
	}
	defer readCloser.Close()

	bufSize := int64(readBufSizeBytes)
	d := req.GetFileRecord().GetDigest()
	if d.GetSizeBytes() > 0 && d.GetSizeBytes() < bufSize {
		bufSize = d.GetSizeBytes()
	}
	copyBuf := make([]byte, bufSize)
	_, err = io.CopyBuffer(&streamWriter{stream}, readCloser, copyBuf)
	return err
}

func (s *Store) Write(stream rfspb.Api_WriteServer) error {
	var bytesWritten int64
	var writeCloser interfaces.MetadataWriteCloser
	var clusterID uint64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if writeCloser == nil {
			r, err := s.LeasedRange(req.GetHeader())
			if err != nil {
				s.log.Errorf("Error while calling LeasedRange: %s", err)
				return err
			}
			clusterID = r.ClusterID
			writeCloser, err = r.Writer(stream.Context(), req.GetHeader(), req.GetFileRecord())
			if err != nil {
				return err
			}
			defer writeCloser.Close()
			// Send the client an empty write response as an indicator that we
			// have accepted the write.
			if err := stream.Send(&rfpb.WriteResponse{}); err != nil {
				return err
			}
		}
		n, err := writeCloser.Write(req.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.FinishWrite {
			now := time.Now()
			md := &rfpb.FileMetadata{
				FileRecord:      req.GetFileRecord(),
				StorageMetadata: writeCloser.Metadata(),
				StoredSizeBytes: bytesWritten,
				LastModifyUsec:  now.UnixMicro(),
				LastAccessUsec:  now.UnixMicro(),
			}
			pebbleKey, err := s.fileStorer.PebbleKey(req.GetFileRecord())
			if err != nil {
				return err
			}
			fileMetadataKey, err := pebbleKey.Bytes(filestore.Version2)
			if err != nil {
				return err
			}
			protoBytes, err := proto.Marshal(md)
			if err != nil {
				return err
			}
			writeReq := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
				Kv: &rfpb.KV{
					Key:   fileMetadataKey,
					Value: protoBytes,
				},
			})
			if err := client.SyncProposeLocalBatchNoRsp(stream.Context(), s.nodeHost, clusterID, writeReq); err != nil {
				return err
			}
			return stream.Send(&rfpb.WriteResponse{
				CommittedSize: bytesWritten,
			})
		}
	}
	return nil
}

func (s *Store) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			if metaRangeData, ok := member.Tags[constants.MetaRangeTag]; ok {
				// Whenever the metarange data changes, for any
				// reason, start a goroutine that ensures the
				// node liveness record is up to date.
				if s.metaRangeData != metaRangeData {
					s.metaRangeData = metaRangeData
					// Start this in a goroutine so that
					// other gossip callbacks are not
					// blocked.
					go s.renewNodeLiveness()
				}
			}
		}
	default:
		return
	}
}

func (s *Store) renewNodeLiveness() {
	retrier := retry.DefaultWithContext(context.Background())
	for retrier.Next() {
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

func (s *Store) Usage() *rfpb.StoreUsage {
	su := &rfpb.StoreUsage{
		Node: s.NodeDescriptor(),
	}

	s.rangeMu.Lock()
	su.ReplicaCount = int64(len(s.openRanges))
	s.rangeMu.Unlock()

	s.leases.Range(func(key, value any) bool {
		su.LeaseCount += 1
		return true
	})

	for _, ru := range s.usages.RangeUsages() {
		su.ReadQps += ru.GetReadQps()
		su.RaftProposeQps += ru.GetRaftProposeQps()
		su.TotalBytesUsed += ru.GetEstimatedDiskBytesUsed()
	}
	return su
}

func (s *Store) RefreshReplicaUsages() []*rfpb.ReplicaUsage {
	s.rangeMu.RLock()
	openRanges := make([]*rfpb.RangeDescriptor, 0, len(s.openRanges))
	for _, rd := range s.openRanges {
		openRanges = append(openRanges, rd)
	}
	s.rangeMu.RUnlock()

	var usages []*rfpb.ReplicaUsage
	for _, rd := range openRanges {
		r, err := s.GetReplica(rd.GetRangeId())
		if err != nil {
			log.Warningf("could not get replica %d to refresh usage: %s", rd.GetRangeId(), err)
			continue
		}
		u, err := r.Usage()
		if err != nil {
			log.Warningf("could not refresh usage for replica %d: %s", rd.GetRangeId(), err)
			continue
		}
		usages = append(usages, u)
	}
	return usages
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

func (s *Store) GetClusterMembership(ctx context.Context, clusterID uint64) ([]*rfpb.ReplicaDescriptor, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		membership, err = s.nodeHost.SyncGetClusterMembership(ctx, clusterID)
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
	for nodeID := range membership.Nodes {
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			ClusterId: clusterID,
			NodeId:    nodeID,
		})
	}
	return replicas, nil
}

func (s *Store) SplitCluster(ctx context.Context, req *rfpb.SplitClusterRequest) (*rfpb.SplitClusterResponse, error) {
	if !*enableSplittingReplicas {
		return nil, status.FailedPreconditionError("Splitting not enabled")
	}

	sourceRange := req.GetRange()
	if sourceRange == nil {
		return nil, status.FailedPreconditionErrorf("no range provided to split: %+v", req)
	}
	if len(sourceRange.GetReplicas()) == 0 {
		return nil, status.FailedPreconditionErrorf("no replicas in range: %+v", sourceRange)
	}

	// Copy source range, because it's a pointer and will change when we
	// propose the split.
	sourceRange = proto.Clone(sourceRange).(*rfpb.RangeDescriptor)
	clusterID := sourceRange.GetReplicas()[0].GetClusterId()

	newClusterID, newRangeID, err := s.reserveClusterAndRangeID(ctx)
	if err != nil {
		return nil, status.InternalErrorf("could not reserve IDs for new cluster: %s", err)
	}
	simpleSplitReq := rbuilder.NewBatchBuilder().Add(&rfpb.SimpleSplitRequest{
		SourceRangeId: sourceRange.GetRangeId(),
		NewClusterId:  newClusterID,
		NewRangeId:    newRangeID,
	})
	rsp, err := client.SyncProposeLocalBatch(ctx, s.nodeHost, clusterID, simpleSplitReq)
	if err != nil {
		return nil, status.InternalErrorf("simple split err: %s", err)
	}

	simpleSplitRsp, err := rsp.SimpleSplitResponse(0)
	if err != nil {
		return nil, err
	}
	if err := s.updateMetarange(ctx, sourceRange, simpleSplitRsp.GetNewLeft(), simpleSplitRsp.GetNewRight()); err != nil {
		log.Errorf("metarange update error: %s", err)
		return nil, err
	}

	if err := s.SnapshotCluster(ctx, clusterID); err != nil {
		return nil, err
	}
	return &rfpb.SplitClusterResponse{
		Left:  simpleSplitRsp.GetNewLeft(),
		Right: simpleSplitRsp.GetNewRight(),
	}, nil
}

func (s *Store) getLastAppliedIndex(header *rfpb.Header) (uint64, error) {
	r, _, err := s.validatedRange(header)
	if err != nil {
		return 0, err
	}
	return r.LastAppliedIndex()
}

func (s *Store) waitForReplicaToCatchUp(ctx context.Context, clusterID uint64, desiredLastAppliedIndex uint64) error {
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
		if rd := s.lookupRange(clusterID); rd != nil {
			if r, err := s.GetReplica(rd.GetRangeId()); err == nil {
				if lastApplied, err := r.LastAppliedIndex(); err == nil {
					if lastApplied >= desiredLastAppliedIndex {
						s.log.Infof("Cluster %d took %s to catch up", clusterID, time.Since(start))
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (s *Store) getConfigChangeID(ctx context.Context, clusterID uint64) (uint64, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		// Get the config change index for this cluster.
		membership, err = s.nodeHost.SyncGetClusterMembership(ctx, clusterID)
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
		return 0, status.InternalErrorf("null cluster membership for cluster: %d", clusterID)
	}
	return membership.ConfigChangeID, nil
}

// AddClusterNode adds a new node to the specified cluster if pre-reqs are met.
// Pre-reqs are:
//   - The request must be valid and contain all information
//   - This node must be a member of the cluster that is being added to
//   - The provided range descriptor must be up to date
func (s *Store) AddClusterNode(ctx context.Context, req *rfpb.AddClusterNodeRequest) (*rfpb.AddClusterNodeResponse, error) {
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

	clusterID := req.GetRange().GetReplicas()[0].GetClusterId()

	// Reserve a new node ID for the node about to be added.
	nodeIDs, err := s.reserveNodeIDs(ctx, 1)
	if err != nil {
		return nil, err
	}
	newNodeID := nodeIDs[0]

	// Get the config change index for this cluster.
	configChangeID, err := s.getConfigChangeID(ctx, clusterID)
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
	s.registry.Add(clusterID, newNodeID, node.GetNhid())
	s.registry.AddNode(node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node to the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddNode(ctx, clusterID, newNodeID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return nil, err
	}

	// Start the cluster on the newly added node.
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return nil, err
	}
	_, err = c.StartCluster(ctx, &rfpb.StartClusterRequest{
		ClusterId:        clusterID,
		NodeId:           newNodeID,
		Join:             true,
		LastAppliedIndex: lastAppliedIndex,
	})
	if err != nil {
		return nil, err
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	rd, err = s.addReplicaToRangeDescriptor(ctx, clusterID, newNodeID, rd)
	if err != nil {
		return nil, err
	}
	metrics.RaftMoves.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
		metrics.RaftMoveLabel:       "add",
	}).Inc()

	return &rfpb.AddClusterNodeResponse{
		Range: rd,
	}, nil
}

// RemoveClusterNode removes a new node from the specified cluster if pre-reqs are
// met. Pre-reqs are:
//   - The request must be valid and contain all information
//   - This node must be a member of the cluster that is being removed from
//   - The provided range descriptor must be up to date
func (s *Store) RemoveClusterNode(ctx context.Context, req *rfpb.RemoveClusterNodeRequest) (*rfpb.RemoveClusterNodeResponse, error) {
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

	var clusterID, nodeID uint64
	for _, replica := range req.GetRange().GetReplicas() {
		if replica.GetNodeId() == req.GetNodeId() {
			clusterID = replica.GetClusterId()
			nodeID = replica.GetNodeId()
			break
		}
	}
	if clusterID == 0 && nodeID == 0 {
		return nil, status.FailedPreconditionErrorf("No node with id %d found in range: %+v", req.GetNodeId(), req.GetRange())
	}

	configChangeID, err := s.getConfigChangeID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestDeleteNode(ctx, clusterID, nodeID, configChangeID)
	})
	if err != nil {
		return nil, err
	}

	grpcAddr, _, err := s.registry.ResolveGRPC(clusterID, nodeID)
	if err != nil {
		s.log.Errorf("error resolving grpc addr for c%dn%d: %s", clusterID, nodeID, err)
		return nil, err
	}
	// Remove the data from the now stopped node.
	c, err := s.apiClient.Get(ctx, grpcAddr)
	if err != nil {
		s.log.Errorf("err getting api client: %s", err)
		return nil, err
	}
	_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	if err != nil {
		s.log.Errorf("remove data err: %s", err)
		return nil, err
	}

	// Finally, update the range descriptor information to reflect the
	// new membership of this range without the removed node.
	rd, err = s.removeReplicaFromRangeDescriptor(ctx, clusterID, nodeID, req.GetRange())
	if err != nil {
		return nil, err
	}

	metrics.RaftMoves.With(prometheus.Labels{
		metrics.RaftNodeHostIDLabel: s.nodeHost.ID(),
		metrics.RaftMoveLabel:       "remove",
	}).Inc()

	return &rfpb.RemoveClusterNodeResponse{
		Range: rd,
	}, nil
}

func (s *Store) ListCluster(ctx context.Context, req *rfpb.ListClusterRequest) (*rfpb.ListClusterResponse, error) {
	s.rangeMu.RLock()
	openRanges := make([]*rfpb.RangeDescriptor, 0, len(s.openRanges))
	for _, rd := range s.openRanges {
		openRanges = append(openRanges, rd)
	}
	s.rangeMu.RUnlock()

	rsp := &rfpb.ListClusterResponse{
		Node: s.NodeDescriptor(),
	}
	for _, rd := range openRanges {
		if req.GetLeasedOnly() {
			header := &rfpb.Header{
				RangeId:    rd.GetRangeId(),
				Generation: rd.GetGeneration(),
			}
			if _, err := s.LeasedRange(header); err != nil {
				continue
			}
		}
		rr := &rfpb.RangeReplica{
			Range: rd,
		}
		if replica, err := s.GetReplica(rd.GetRangeId()); err == nil {
			usage, err := replica.Usage()
			if err == nil {
				rr.ReplicaUsage = usage
			}
		}
		rsp.RangeReplicas = append(rsp.RangeReplicas, rr)
	}
	return rsp, nil
}

func (s *Store) reserveNodeIDs(ctx context.Context, n int) ([]uint64, error) {
	newVal, err := s.sender.Increment(ctx, constants.LastNodeIDKey, uint64(n))
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
		Key:   constants.LastClusterIDKey,
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

func addLocalRangeEdits(oldLeft, newLeft *rfpb.RangeDescriptor, b *rbuilder.BatchBuilder) error {
	cas, err := casRangeEdit(constants.LocalRangeKey, oldLeft, newLeft)
	if err != nil {
		return err
	}
	b.Add(cas)
	return nil
}

func addMetaRangeEdits(oldLeft, newLeft, newRight *rfpb.RangeDescriptor, b *rbuilder.BatchBuilder) error {
	newLeftBuf, err := proto.Marshal(newLeft)
	if err != nil {
		return err
	}
	oldLeftBuf, err := proto.Marshal(oldLeft)
	if err != nil {
		return err
	}
	newRightBuf, err := proto.Marshal(newRight)
	if err != nil {
		return err
	}

	// Send a single request that:
	//  - CAS sets the newLeft value to newNewLeftBuf
	//  - inserts the new newRightBuf
	//
	// if the CAS fails, check the existing value
	//  if it's generation is past ours, ignore the error, we're out of date
	//  if the existing value already matches what we were trying to set, we're done.
	//  else return an error
	b.Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newRight.GetRight()),
			Value: newRightBuf,
		},
		ExpectedValue: oldLeftBuf,
	}).Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newLeft.GetRight()),
			Value: newLeftBuf,
		},
	})
	return nil
}

func (s *Store) updateMetarange(ctx context.Context, oldLeft, left, right *rfpb.RangeDescriptor) error {
	b := rbuilder.NewBatchBuilder()
	if err := addMetaRangeEdits(oldLeft, left, right, b); err != nil {
		return err
	}
	batchProto, err := b.ToProto()
	if err != nil {
		return err
	}
	rsp, err := s.Sender().SyncPropose(ctx, keys.RangeMetaKey(right.GetRight()), batchProto)
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

func (s *Store) updateRangeDescriptor(ctx context.Context, clusterID uint64, old, new *rfpb.RangeDescriptor) error {
	// TODO(tylerw): this should use 2PC.
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return err
	}
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return err
	}

	metaRangeBatch := rbuilder.NewBatchBuilder()
	localBatch := rbuilder.NewBatchBuilder()
	if err := addLocalRangeEdits(old, new, localBatch); err != nil {
		return err
	}
	metaRangeDescriptorKey := keys.RangeMetaKey(new.GetRight())
	metaRangeCasReq := &rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaRangeDescriptorKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}

	if clusterID == constants.InitialClusterID {
		localBatch.Add(metaRangeCasReq)
	} else {
		metaRangeBatch.Add(metaRangeCasReq)
	}

	localReq, err := localBatch.ToProto()
	if err != nil {
		return err
	}

	// Update the local range.
	localRsp, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, localReq)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(localRsp).CASResponse(0)
	if err != nil {
		return err
	}
	// If both changes (to local and metarange descriptors) applied to the
	// MetaRange, they were applied in the localReq, and there's nothing
	// remaining to do.
	if metaRangeBatch.Size() == 0 {
		return nil
	}

	// Update the metarange.
	metaReq, err := metaRangeBatch.ToProto()
	if err != nil {
		return err
	}
	metaRangeRsp, err := s.sender.SyncPropose(ctx, metaRangeDescriptorKey, metaReq)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(metaRangeRsp).CASResponse(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if err := s.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (s *Store) removeReplicaFromRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Replicas {
		if replica.GetNodeId() == nodeID {
			newDescriptor.Replicas = append(newDescriptor.Replicas[:i], newDescriptor.Replicas[i+1:]...)
			break
		}
	}
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if err := s.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}
