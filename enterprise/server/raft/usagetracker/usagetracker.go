package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/docker/go-units"
	"github.com/hashicorp/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

var (
	partitionUsageDeltaGossipThreshold = flag.Int("cache.raft.partition_usage_delta_bytes_threshold", 100e6, "Gossip partition usage information if it has changed by more than this amount since the last gossip.")
	samplesPerEviction                 = flag.Int("cache.raft.samples_per_eviction", 20, "How many records to sample on each eviction")
	samplePoolSize                     = flag.Int("cache.raft.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")
)

const (
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

type IStore interface {
	NodeDescriptor() *rfpb.NodeDescriptor
	RefreshReplicaUsages() []*rfpb.ReplicaUsage
	Sender() *sender.Sender
	Sample(ctx context.Context, rangeID uint64, partition string, n int) ([]*approxlru.Sample[*replica.LRUSample], error)
}

type Tracker struct {
	store         IStore
	gossipManager *gossip.GossipManager
	partitions    []disk.Partition

	quitChan      chan struct{}
	mu            sync.Mutex
	byRange       map[uint64]*rfpb.ReplicaUsage
	byPartition   map[string]*partitionUsage
	lastBroadcast map[string]*rfpb.PartitionMetadata
}

type nodePartitionUsage struct {
	sizeBytes  int64
	lastUpdate time.Time
}

type partitionUsage struct {
	id    string
	store IStore

	mu  sync.Mutex
	lru *approxlru.LRU[*replica.LRUSample]
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

func (pu *partitionUsage) evict(ctx context.Context, sample *approxlru.Sample[*replica.LRUSample]) error {
	deleteReq := rbuilder.NewBatchBuilder().Add(&rfpb.DeleteRequest{
		Key:        sample.Key.Bytes,
		MatchAtime: sample.Timestamp.UnixMicro(),
	})
	batchCmd, err := deleteReq.ToProto()
	if err != nil {
		return err
	}
	rsp, err := pu.store.Sender().SyncPropose(ctx, sample.Key.Bytes, batchCmd)
	if err != nil {
		return status.InternalErrorf("could not propose eviction: %s", err)
	}
	batchRsp := rbuilder.NewBatchResponseFromProto(rsp)
	if err := batchRsp.AnyError(); err != nil {
		if status.IsNotFoundError(err) || status.IsOutOfRangeError(err) {
			log.Infof("Skipping eviction for %q: %s", sample.Key, err)
			return nil
		}
		return status.InternalErrorf("eviction request failed: %s", err)
	}

	ageMillis := float64(time.Since(sample.Timestamp).Milliseconds())
	metrics.RaftEvictionAgeMsec.With(prometheus.Labels{metrics.PartitionID: pu.id}).Observe(ageMillis)

	globalSizeBytes := pu.GlobalSizeBytes()

	pu.mu.Lock()
	defer pu.mu.Unlock()
	// Update local replica information to reflect the eviction. Don't need
	// to wait for a proactive update from the replica.
	u, ok := pu.replicas[sample.Key.RangeID]
	if ok {
		u.SizeBytes -= sample.SizeBytes
		u.TotalCount--
	} else {
		log.Warningf("eviction succeeded but range %d wasn't found", sample.Key.RangeID)
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

	return nil
}

func (pu *partitionUsage) sample(ctx context.Context, n int) ([]*approxlru.Sample[*replica.LRUSample], error) {
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

	var samples []*approxlru.Sample[*replica.LRUSample]
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

func New(store IStore, gossipManager *gossip.GossipManager, partitions []disk.Partition) (*Tracker, error) {
	ut := &Tracker{
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
		l, err := approxlru.New(&approxlru.Opts[*replica.LRUSample]{
			SamplePoolSize:     *samplePoolSize,
			SamplesPerEviction: *samplesPerEviction,
			MaxSizeBytes:       maxSizeBytes,
			OnEvict: func(ctx context.Context, sample *approxlru.Sample[*replica.LRUSample]) error {
				return u.evict(ctx, sample)
			},
			OnSample: func(ctx context.Context, n int) ([]*approxlru.Sample[*replica.LRUSample], error) {
				return u.sample(ctx, n)
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

func (ut *Tracker) Stop() {
	close(ut.quitChan)
	for _, p := range ut.byPartition {
		p.lru.Stop()
	}
}

func (ut *Tracker) Statusz(ctx context.Context) string {
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

func (ut *Tracker) OnEvent(updateType serf.EventType, event serf.Event) {
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
func (ut *Tracker) RemoteUpdate(usage *rfpb.NodePartitionUsage) {
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
func (ut *Tracker) LocalUpdate(rangeID uint64, usage *rfpb.ReplicaUsage) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	ut.byRange[rangeID] = usage

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

func (ut *Tracker) removeRangePartitions(rangeID uint64) {
	for _, u := range ut.byPartition {
		delete(u.replicas, rangeID)
	}
}

func (ut *Tracker) RemoveRange(rangeID uint64) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	delete(ut.byRange, rangeID)

	ut.removeRangePartitions(rangeID)
}

func (ut *Tracker) ReplicaUsages() []*rfpb.ReplicaUsage {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	var us []*rfpb.ReplicaUsage
	for _, u := range ut.byRange {
		us = append(us, u)
	}
	return us
}

func (ut *Tracker) computeUsage() *rfpb.NodePartitionUsage {
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

func (ut *Tracker) broadcastLoop() {
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

func (ut *Tracker) broadcast(force bool) error {
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
