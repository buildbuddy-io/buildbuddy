package driver

import (
	"cmp"
	"container/heap"
	"context"
	"flag"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

var (
	minReplicasPerRange   = flag.Int("cache.raft.min_replicas_per_range", 3, "The minimum number of replicas each range should have")
	minMetaRangeReplicas  = flag.Int("cache.raft.min_meta_range_replicas", 5, "The minimum number of replicas each range for meta range")
	newReplicaGracePeriod = flag.Duration("cache.raft.new_replica_grace_period", 5*time.Minute, "The amount of time we allow for a new replica to catch up to the leader's before we start to consider it to be behind.")
)

const (
	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to be used for a rebalance or allocation target and we will
	// actively try to move replicas from this node.
	maximumDiskCapacity = .95
	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to be used as a rebalance target.
	maxDiskCapacityForRebalance = .925
)

type DriverAction int

const (
	_ DriverAction = iota
	DriverNoop
	DriverSplitRange
	DriverRemoveReplica
	DriverRemoveDeadReplica
	DriverAddReplica
	DriverReplaceDeadReplica
	DriverRebalanceReplica
	DriverRebalanceLease
)

const (
	// how long do we wait until we process the next item
	queueWaitDuration = 1 * time.Second
	// This is a ratio used in determining whether a store is above, around or
	// below the mean. If a store has range count that is greater than the mean
	// plus the product of the mean and this ratio, it is considered above the
	// mean; and if the range count is less than the mean minus the product of
	// the mean and this ratio, it is considered below the mean. Otherwise, it's
	// considered around the mean.
	replicaCountMeanRatioThreshold = .05
	// The minimum number of ranges by which a store must deviate from the mean
	// to be considered above or below the mean.
	minReplicaCountThreshold = 2

	// Similar to replica count mean ration Threshold; but for lease count
	// instead.
	leaseCountMeanRatioThreshold = .05
	// The minimum number of leases by which a store must deviate from the mean
	// to be considered above or below the mean.
	minLeaseCountThreshold = 2
)

func (a DriverAction) Priority() float64 {
	switch a {
	case DriverReplaceDeadReplica:
		return 600
	case DriverAddReplica:
		return 500
	case DriverRemoveDeadReplica:
		return 400
	case DriverRemoveReplica:
		return 300
	case DriverSplitRange:
		return 200
	case DriverRebalanceReplica, DriverRebalanceLease, DriverNoop:
		return 0
	default:
		alert.UnexpectedEvent("unknown-driver-action", "unknown driver action %s", a)
		return -1
	}
}

func (a DriverAction) String() string {
	switch a {
	case DriverNoop:
		return "no-op"
	case DriverRemoveReplica:
		return "remove-replica"
	case DriverRemoveDeadReplica:
		return "remove-dead-replica"
	case DriverAddReplica:
		return "add-replica"
	case DriverReplaceDeadReplica:
		return "replace-dead-replica"
	case DriverRebalanceReplica:
		return "consider-rebalance-replica"
	case DriverRebalanceLease:
		return "consider-rebalance-lease"
	case DriverSplitRange:
		return "split-range"
	default:
		return "unknown"
	}
}

type IReplica interface {
	RangeDescriptor() *rfpb.RangeDescriptor
	ReplicaID() uint64
	RangeID() uint64
	Usage() (*rfpb.ReplicaUsage, error)
}

type IStore interface {
	GetReplica(rangeID uint64) (*replica.Replica, error)
	HaveLease(ctx context.Context, rangeID uint64) bool
	AddReplica(ctx context.Context, req *rfpb.AddReplicaRequest) (*rfpb.AddReplicaResponse, error)
	RemoveReplica(ctx context.Context, req *rfpb.RemoveReplicaRequest) (*rfpb.RemoveReplicaResponse, error)
	GetReplicaStates(ctx context.Context, rd *rfpb.RangeDescriptor) map[uint64]constants.ReplicaState
	SplitRange(ctx context.Context, req *rfpb.SplitRangeRequest) (*rfpb.SplitRangeResponse, error)
	TransferLeadership(ctx context.Context, req *rfpb.TransferLeadershipRequest) (*rfpb.TransferLeadershipResponse, error)
	NHID() string
}

// computeQuorum computes a quorum, which a majority of members from a peer set.
// In raft, when a quorum of nodes is unavailable, the cluster becomes
// unavailable.
func computeQuorum(numNodes int) int {
	return (numNodes / 2) + 1
}

// computeAction computes the action needed and its priority.
func (rq *Queue) computeAction(rd *rfpb.RangeDescriptor, usage *rfpb.ReplicaUsage, localReplicaID uint64) (DriverAction, float64) {
	if rq.storeMap == nil {
		action := DriverNoop
		return action, action.Priority()
	}
	replicas := rd.GetReplicas()
	curReplicas := len(replicas)
	if curReplicas == 0 {
		action := DriverNoop
		return action, action.Priority()
	}
	rangeID := replicas[0].GetRangeId()
	minReplicas := *minReplicasPerRange
	if rangeID == constants.MetaRangeID {
		minReplicas = *minMetaRangeReplicas
	}

	desiredQuorum := computeQuorum(minReplicas)
	quorum := computeQuorum(curReplicas)

	if curReplicas < minReplicas {
		action := DriverAddReplica
		adjustedPriority := action.Priority() + float64(desiredQuorum-curReplicas)
		return action, adjustedPriority
	}
	replicasByStatus := rq.storeMap.DivideByStatus(replicas)
	numLiveReplicas := len(replicasByStatus.LiveReplicas) + len(replicasByStatus.SuspectReplicas)
	numDeadReplicas := len(replicasByStatus.DeadReplicas)

	if numLiveReplicas < quorum {
		// The cluster is unavailable since we don't have enough live nodes.
		// There is no point of doing anything right now.
		log.Debugf("noop because num live replicas = %d less than quorum =%d", numLiveReplicas, quorum)
		action := DriverNoop
		return action, action.Priority()
	}

	if curReplicas <= minReplicas && numDeadReplicas > 0 {
		action := DriverReplaceDeadReplica
		return action, action.Priority()
	}

	if numDeadReplicas > 0 {
		action := DriverRemoveDeadReplica
		return action, action.Priority()
	}

	if curReplicas > minReplicas {
		action := DriverRemoveReplica
		adjustedPriority := action.Priority() - float64(curReplicas%2)
		return action, adjustedPriority
	}

	if len(replicasByStatus.SuspectReplicas) == 0 && numDeadReplicas == 0 {
		// Do not split if there is a replica is dead or suspect.
		if maxRangeSizeBytes := config.MaxRangeSizeBytes(); maxRangeSizeBytes > 0 {
			if sizeUsed := usage.GetEstimatedDiskBytesUsed(); sizeUsed >= maxRangeSizeBytes {
				action := DriverSplitRange
				adjustedPriority := action.Priority() + float64(sizeUsed-maxRangeSizeBytes)/float64(sizeUsed)*100.0
				return action, adjustedPriority
			}
		}
	}

	if rd.GetRangeId() == constants.MetaRangeID {
		// Do not try to re-balance meta-range.
		//
		// When meta-range is moved onto a different node, range cache has to
		// update its range descriptor. Before the range descriptor get updated,
		// SyncPropose to all other ranges can fail temporarily because the range
		// descriptor is not current. Therefore, we should only move meta-range
		// when it's absolutely necessary.
		action := DriverNoop
		return action, action.Priority()
	}

	// For DriverConsiderRebalance check if there are rebalance opportunities.
	storesWithStats := rq.storeMap.GetStoresWithStats()
	op := rq.findRebalanceReplicaOp(rd, storesWithStats, localReplicaID)
	if op != nil {
		action := DriverRebalanceReplica
		return action, action.Priority()
	}
	op = rq.findRebalanceLeaseOp(rd, localReplicaID)
	if op != nil {
		action := DriverRebalanceLease
		return action, action.Priority()
	}

	action := DriverNoop
	return action, action.Priority()
}

type pqItem struct {
	rangeID   uint64
	replicaID uint64

	priority   float64
	insertTime time.Time

	index      int // The index of the item in the heap.
	processing bool
	requeue    bool
}

// An priorityQueue implements heap.Interface and holds pqItems.
type priorityQueue []*pqItem

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority ||
		(pq[i].priority == pq[j].priority && pq[i].insertTime.Before(pq[j].insertTime))
}
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItem)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *priorityQueue) update(item *pqItem, priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

// The Queue is responsible for up-replicate, down-replicate and reblance ranges
// across the stores.
type Queue struct {
	storeMap storemap.IStoreMap
	store    IStore

	maxSize int
	stop    chan struct{}

	mu        sync.Mutex //protects pq, pqItemMap
	pq        *priorityQueue
	pqItemMap map[uint64]*pqItem

	clock     clockwork.Clock
	log       log.Logger
	apiClient *client.APIClient

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel context.CancelFunc
}

func NewQueue(store IStore, gossipManager interfaces.GossipService, nhlog log.Logger, apiClient *client.APIClient, clock clockwork.Clock) *Queue {
	storeMap := storemap.New(gossipManager, clock)
	ctx, cancelFunc := context.WithCancel(context.Background())
	eg, gctx := errgroup.WithContext(ctx)
	return &Queue{
		storeMap:  storeMap,
		pq:        &priorityQueue{},
		pqItemMap: make(map[uint64]*pqItem),
		store:     store,
		maxSize:   100,
		clock:     clock,
		log:       nhlog,
		apiClient: apiClient,

		eg:       eg,
		egCtx:    gctx,
		egCancel: cancelFunc,
	}
}

func (rq *Queue) shouldQueue(ctx context.Context, repl IReplica) (bool, float64) {
	rd := repl.RangeDescriptor()

	if !rq.store.HaveLease(ctx, rd.GetRangeId()) {
		// The store doesn't have lease for this range. do not queue.
		rq.log.Debugf("should not queue because we don't have lease")
		return false, 0
	}

	usage, err := repl.Usage()
	if err != nil {
		rq.log.Errorf("failed to get Usage of replica c%dn%d", repl.RangeID(), repl.ReplicaID())
	}

	action, priority := rq.computeAction(rd, usage, repl.ReplicaID())
	if action == DriverNoop {
		rq.log.Debugf("should not queue because no-op")
		return false, 0
	}
	return true, priority
}

func (rq *Queue) pushLocked(item *pqItem) {
	heap.Push(rq.pq, item)
	rq.pqItemMap[item.rangeID] = item
}

func (rq *Queue) getItemWithMinPriority() *pqItem {
	old := *rq.pq
	n := len(old)
	return old[n-1]
}

func (rq *Queue) Len() int {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	return rq.pq.Len()
}

// MaybeAdd adds a replica to the queue if the store has the lease for the range,
// and there is work needs to be done.
// When the queue is full, it deletes the least important replica from the queue.
func (rq *Queue) MaybeAdd(ctx context.Context, replica IReplica) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	shouldQueue, priority := rq.shouldQueue(ctx, replica)
	if !shouldQueue {
		return
	}

	rd := replica.RangeDescriptor()
	item, ok := rq.pqItemMap[rd.GetRangeId()]
	if ok {
		// The item is processing. Mark to be requeued.
		if item.processing {
			item.requeue = true
			return
		}
		rq.pq.update(item, priority)
		rq.log.Infof("updated priority for replica rangeID=%d", item.rangeID)
		return
	}

	item = &pqItem{
		rangeID:    rd.GetRangeId(),
		replicaID:  replica.ReplicaID(),
		priority:   priority,
		insertTime: rq.clock.Now(),
	}
	rq.pushLocked(item)
	rq.log.Infof("queued replica rangeID=%d with priority %.2f", item.rangeID, priority)

	// If the priroityQueue if full, let's remove the item with the lowest priority.
	if pqLen := rq.pq.Len(); pqLen > rq.maxSize {
		rq.removeLocked(rq.getItemWithMinPriority())
	}
}

func (rq *Queue) removeLocked(item *pqItem) {
	if item.processing {
		item.requeue = false
		return
	}
	if item.index >= 0 {
		heap.Remove(rq.pq, item.index)
	}
	delete(rq.pqItemMap, item.rangeID)
}

func (rq *Queue) pop() IReplica {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	item := heap.Pop(rq.pq).(*pqItem)
	item.processing = true
	repl, err := rq.store.GetReplica(item.rangeID)
	if err != nil || repl.ReplicaID() != item.replicaID {
		rq.log.Errorf("unable to get replica for c%dn%d: %s", item.rangeID, item.replicaID, err)
		delete(rq.pqItemMap, item.rangeID)
		return nil
	}
	return repl
}

func (rq *Queue) Start() {
	rq.eg.Go(func() error {
		rq.processQueue()
		return nil
	})
}

func (rq *Queue) Stop() {
	rq.log.Infof("Driver shutdown started")
	now := time.Now()
	defer func() {
		rq.log.Infof("Driver shutdown finished in %s", time.Since(now))
	}()

	rq.egCancel()
	rq.eg.Wait()
}

func (rq *Queue) processQueue() {
	queueDelay := rq.clock.NewTicker(queueWaitDuration)
	defer queueDelay.Stop()
	for {
		select {
		case <-rq.egCtx.Done():
			return
		case <-queueDelay.Chan():
		}
		if rq.Len() == 0 {
			continue
		}
		repl := rq.pop()
		if repl == nil {
			continue
		}

		requeue, err := rq.processReplica(rq.egCtx, repl)
		if err != nil {
			// TODO: check if err can be retried.
			rq.log.Errorf("failed to process replica: %s", err)
		} else {
			rq.log.Debugf("successfully processed replica: %d", repl.RangeID())
		}
		rq.postProcess(rq.egCtx, repl, requeue)
	}

}

// findDeadReplica finds a dead replica to be removed.
func findDeadReplica(replicas []*rfpb.ReplicaDescriptor, replicaByStatus *storemap.ReplicasByStatus) *rfpb.ReplicaDescriptor {
	if len(replicaByStatus.DeadReplicas) == 0 {
		// nothing to be removed
		return nil
	}

	if len(replicas) == 1 {
		// only one replica remains, don't remove the replica
		return nil
	}

	return replicaByStatus.DeadReplicas[0]
}

func storeHasReplica(node *rfpb.NodeDescriptor, existing []*rfpb.ReplicaDescriptor) bool {
	for _, repl := range existing {
		if repl.GetNhid() == node.GetNhid() {
			return true
		}
	}
	return false
}

// findNodeForAllocation finds a target node for the range to up-replicate.
func (rq *Queue) findNodeForAllocation(rd *rfpb.RangeDescriptor, storesWithStats *storemap.StoresWithStats) *rfpb.NodeDescriptor {
	var candidates []*candidate
	for _, su := range storesWithStats.Usages {
		if storeHasReplica(su.GetNode(), rd.GetReplicas()) {
			rq.log.Debugf("skip node %+v because the replica is already on the node", su.GetNode())
			continue
		}
		if isDiskFull(su) {
			rq.log.Debugf("skip node %+v because the disk is full", su)
			continue
		}
		rq.log.Debugf("add node %+v to candidate list", su.GetNode())
		candidates = append(candidates, &candidate{
			nhid:                  su.GetNode().GetNhid(),
			usage:                 su,
			replicaCount:          su.GetReplicaCount(),
			replicaCountMeanLevel: replicaCountMeanLevel(storesWithStats, su),
		})
	}

	if len(candidates) == 0 {
		return nil
	}
	slices.SortFunc(candidates, func(a, b *candidate) int {
		// Best targets are up front.
		return -compareByScoreAndID(a, b)
	})
	return candidates[0].usage.GetNode()
}

type change struct {
	addOp                *rfpb.AddReplicaRequest
	removeOp             *rfpb.RemoveReplicaRequest
	splitOp              *rfpb.SplitRangeRequest
	transferLeadershipOp *rfpb.TransferLeadershipRequest
}

func (rq *Queue) splitRange(rd *rfpb.RangeDescriptor) *change {
	return &change{
		splitOp: &rfpb.SplitRangeRequest{
			Header: header.New(rd, 0, rfpb.Header_LINEARIZABLE),
			Range:  rd,
		},
	}
}

func (rq *Queue) addReplica(rd *rfpb.RangeDescriptor) *change {
	storesWithStats := rq.storeMap.GetStoresWithStats()
	target := rq.findNodeForAllocation(rd, storesWithStats)
	if target == nil {
		rq.log.Debugf("cannot find targets for range descriptor:%+v", rd)
		return nil
	}

	return &change{
		addOp: &rfpb.AddReplicaRequest{
			Range: rd,
			Node:  target,
		},
	}
}

func (rq *Queue) replaceDeadReplica(rd *rfpb.RangeDescriptor) *change {
	replicasByStatus := rq.storeMap.DivideByStatus(rd.GetReplicas())
	dead := findDeadReplica(rd.GetReplicas(), replicasByStatus)
	if dead == nil {
		// nothing to remove
		return nil
	}
	change := rq.addReplica(rd)
	if change == nil {
		rq.log.Debug("replaceDeadReplica cannot find node for allocation")
		return nil
	}

	change.removeOp = &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: dead.GetReplicaId(),
	}

	return change
}

func (rq *Queue) removeDeadReplica(rd *rfpb.RangeDescriptor) *change {
	replicasByStatus := rq.storeMap.DivideByStatus(rd.GetReplicas())
	dead := findDeadReplica(rd.GetReplicas(), replicasByStatus)
	if dead == nil {
		// nothing to remove
		return nil
	}
	return &change{
		removeOp: &rfpb.RemoveReplicaRequest{
			Range:     rd,
			ReplicaId: dead.GetReplicaId(),
		},
	}
}

type rebalanceChoice struct {
	existing   *candidate
	candidates []*candidate
}

type rebalanceOp struct {
	from *candidate
	to   *candidate
}

func compareOp(op1 *rebalanceOp, op2 *rebalanceOp) int {
	c1 := compareByScore(op1.to, op1.from)
	c2 := compareByScore(op2.to, op2.from)
	if c1 != c2 {
		return cmp.Compare(c1, c2)
	}
	return compareByScore(op1.to, op2.to)
}

func canConvergeByRebalanceReplica(choice *rebalanceChoice, allStores *storemap.StoresWithStats) bool {
	if len(choice.candidates) == 0 {
		return false
	}
	overfullThreshold := int64(math.Ceil(aboveMeanReplicaCountThreshold(allStores.ReplicaCount.Mean)))
	// The existing store is too far above the mean.
	if choice.existing.usage.ReplicaCount > overfullThreshold {
		return true
	}

	// The existing store is above the mean, but not too far; but there is at least one other store that is too far below the mean.
	if float64(choice.existing.usage.ReplicaCount) > allStores.ReplicaCount.Mean {
		underfullThreshold := int64(math.Floor(belowMeanReplicaCountThreshold(allStores.ReplicaCount.Mean)))
		for _, c := range choice.candidates {
			if c.usage.ReplicaCount < underfullThreshold {
				return true
			}
		}
	}
	return false
}

func canConvergeByRebalanceLease(choice *rebalanceChoice, allStores *storemap.StoresWithStats) bool {
	if len(choice.candidates) == 0 {
		return false
	}
	overfullThreshold := int64(math.Ceil(aboveMeanLeaseCountThreshold(allStores.LeaseCount.Mean)))
	// The existing store is too far above the mean.
	if choice.existing.usage.LeaseCount > overfullThreshold {
		return true
	}

	// The existing store is above the mean, but not too far; but there is at least one other store that is too far below the mean.
	if float64(choice.existing.usage.LeaseCount) > allStores.LeaseCount.Mean {
		underfullThreshold := int64(math.Floor(belowMeanReplicaCountThreshold(allStores.LeaseCount.Mean)))
		for _, c := range choice.candidates {
			if c.usage.LeaseCount < underfullThreshold {
				return true
			}
		}
	}
	return false
}

func findReplicaWithNHID(rd *rfpb.RangeDescriptor, nhid string) (uint64, error) {
	for _, replica := range rd.GetReplicas() {
		if replica.GetNhid() == nhid {
			return replica.GetReplicaId(), nil
		}
	}
	return 0, status.InternalErrorf("cannot find replica with NHID: %s", nhid)
}

func (rq *Queue) rebalanceReplica(rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	storesWithStats := rq.storeMap.GetStoresWithStats()
	op := rq.findRebalanceReplicaOp(rd, storesWithStats, localRepl.ReplicaID())
	if op == nil {
		return nil
	}
	rq.log.Debugf("found rebalance replica op for range %d: %s -> %s", rd.GetRangeId(), op.from.nhid, op.to.nhid)

	replicaID, err := findReplicaWithNHID(rd, op.from.usage.GetNode().GetNhid())
	if err != nil {
		rq.log.Errorf("failed to rebalance replica: %s", err)
		return nil
	}

	return &change{
		addOp: &rfpb.AddReplicaRequest{
			Range: rd,
			Node:  op.to.usage.GetNode(),
		},
		removeOp: &rfpb.RemoveReplicaRequest{
			Range:     rd,
			ReplicaId: replicaID,
		},
	}
}

func (rq *Queue) rebalanceLease(rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	op := rq.findRebalanceLeaseOp(rd, localRepl.ReplicaID())
	if op == nil {
		return nil
	}
	rq.log.Debugf("found rebalance lease op for range %d: %s -> %s", rd.GetRangeId(), op.from.nhid, op.to.nhid)
	replicaID, err := findReplicaWithNHID(rd, op.to.usage.GetNode().GetNhid())
	if err != nil {
		rq.log.Errorf("failed to rebalance lease: %s", err)
		return nil
	}
	return &change{
		transferLeadershipOp: &rfpb.TransferLeadershipRequest{
			RangeId:         rd.GetRangeId(),
			TargetReplicaId: replicaID,
		},
	}
}

func (rq *Queue) findRebalanceLeaseOp(rd *rfpb.RangeDescriptor, localReplicaID uint64) *rebalanceOp {
	var existing *candidate
	nhids := make([]string, 0, len(rd.GetReplicas()))
	existingNHID := ""
	for _, repl := range rd.GetReplicas() {
		nhids = append(nhids, repl.GetNhid())
		if repl.GetReplicaId() == localReplicaID {
			existingNHID = repl.GetNhid()
		}
	}
	storesWithStats := rq.storeMap.GetStoresWithStatsFromIDs(nhids)
	allStores := make(map[string]*candidate)
	for _, su := range storesWithStats.Usages {
		nhid := su.GetNode().GetNhid()
		store := &candidate{
			nhid:  nhid,
			usage: su,
		}
		allStores[nhid] = store
		if nhid == existingNHID {
			existing = store
		}
	}

	if existing == nil {
		rq.log.Warningf("failed to find existing store for c%dn%d", rd.GetRangeId(), localReplicaID)
		return nil
	}

	existing.leaseCount = existing.usage.LeaseCount
	existing.leaseCountMeanLevel = leaseCountMeanLevel(storesWithStats, existing.usage)
	choice := &rebalanceChoice{
		existing:   existing,
		candidates: make([]*candidate, 0, len(rd.GetReplicas())-1),
	}
	for _, repl := range rd.GetReplicas() {
		if repl.GetReplicaId() == localReplicaID {
			continue
		}
		store, ok := allStores[repl.GetNhid()]
		if !ok {
			// The store might not be available.
			continue
		}
		choice.candidates = append(choice.candidates, &candidate{
			nhid:                repl.GetNhid(),
			usage:               store.usage,
			leaseCount:          store.usage.LeaseCount,
			leaseCountMeanLevel: leaseCountMeanLevel(storesWithStats, store.usage),
		})
	}
	if !canConvergeByRebalanceLease(choice, storesWithStats) {
		return nil
	}

	best := slices.MaxFunc(choice.candidates, compareByScoreAndID)

	if compareByScore(best, existing) < 0 {
		return nil
	}
	return &rebalanceOp{
		from: existing,
		to:   best,
	}
}

func (rq *Queue) findRebalanceReplicaOp(rd *rfpb.RangeDescriptor, storesWithStats *storemap.StoresWithStats, localReplicaID uint64) *rebalanceOp {
	allStores := make(map[string]*candidate)

	existingStores := make(map[string]*candidate)
	needRebalance := false
	for _, su := range storesWithStats.Usages {
		nhid := su.GetNode().GetNhid()
		store := &candidate{
			nhid:     nhid,
			usage:    su,
			fullDisk: isDiskFull(su),
		}
		allStores[nhid] = store
	}

	nhids := make([]string, 0, len(rd.GetReplicas()))
	localNHID := ""
	for _, repl := range rd.GetReplicas() {
		if repl.GetReplicaId() == localReplicaID {
			localNHID = repl.GetNhid()
		}
		store, ok := allStores[repl.GetNhid()]
		if !ok {
			// The store might not be available rn.
			continue
		}
		if store.fullDisk {
			// We want to move the replica away from the store with full disk.
			needRebalance = true
		}
		existingStores[repl.GetNhid()] = store
		nhids = append(nhids, repl.GetNhid())
	}

	// Find valid targeting stores for rebalancing.
	var choices []*rebalanceChoice
	for _, existingNHID := range nhids {
		if existingNHID == localNHID {
			// This is to prevent us from removing the replica on this node. We
			// can only support this after we have the ability to transfer the
			// leadership away.
			continue
		}
		existing := existingStores[existingNHID]
		var targetCandidates []*candidate
		for nhid, store := range allStores {
			if _, ok := existingStores[nhid]; ok {
				// The store already contains the range.
				continue
			}
			if store.fullDisk {
				continue
			}
			targetCandidates = append(targetCandidates, store)
		}
		if len(targetCandidates) == 0 {
			continue
		}

		choices = append(choices, &rebalanceChoice{
			existing:   existing,
			candidates: targetCandidates,
		})
	}

	if !needRebalance {
		for _, choice := range choices {
			if canConvergeByRebalanceReplica(choice, storesWithStats) {
				needRebalance = true
				break
			}
		}
	}

	if !needRebalance {
		return nil
	}

	// Populating scores.
	potentialOps := make([]*rebalanceOp, 0, len(choices))
	for _, rebalanceChoice := range choices {
		existing := rebalanceChoice.existing
		existing.replicaCountMeanLevel = replicaCountMeanLevel(storesWithStats, existing.usage)
		existing.replicaCount = existing.usage.ReplicaCount

		cl := rebalanceChoice.candidates
		for _, c := range cl {
			c.fullDisk = isDiskFullForRebalance(c.usage)
			c.replicaCountMeanLevel = replicaCountMeanLevel(storesWithStats, c.usage)
			c.replicaCount = c.usage.ReplicaCount
		}
		best := slices.MaxFunc(cl, compareByScoreAndID)
		if compareByScore(best, existing) >= 0 {
			potentialOps = append(potentialOps, &rebalanceOp{
				from: existing,
				to:   best,
			})
		}
	}

	if len(potentialOps) == 0 {
		return nil
	}
	// Find the best rebalance move.
	return slices.MaxFunc(potentialOps, compareOp)
}

func (rq *Queue) findRemovableReplicas(rd *rfpb.RangeDescriptor, replicaStateMap map[uint64]constants.ReplicaState, brandNewReplicaID *uint64) []*rfpb.ReplicaDescriptor {
	numUpToDateReplicas := 0
	replicasBehind := make([]*rfpb.ReplicaDescriptor, 0)
	for _, r := range rd.GetReplicas() {
		rs, ok := replicaStateMap[r.GetReplicaId()]
		if !ok {
			continue
		}
		if rs == constants.ReplicaStateCurrent {
			numUpToDateReplicas++
		} else if rs == constants.ReplicaStateBehind {
			// Don't consider brand new replica ID as falling behind
			if brandNewReplicaID == nil || r.GetReplicaId() != *brandNewReplicaID {
				replicasBehind = append(replicasBehind, r)
			}
		}
	}

	quorum := computeQuorum(len(rd.GetReplicas()) - 1)
	if numUpToDateReplicas < quorum {
		// The number of up-to-date replicas is less than quorum. Don't remove
		rq.log.Debugf("there are %d up-to-date replicas and quorum is %d, don't remove", numUpToDateReplicas, quorum)
		return nil
	}

	if numUpToDateReplicas > quorum {
		// Any replica can be removed.
		rq.log.Debugf("there are %d up-to-date replicas and quorum is %d, any replicas can be removed", numUpToDateReplicas, quorum)
		return rd.GetReplicas()
	}

	// The number of up-to-date-replicas equals to the quorum. We only want to delete replicas that are behind.
	rq.log.Debugf("there are %d up-to-date replicas and quorum is %d, only remove behind replicas", numUpToDateReplicas, quorum)
	return replicasBehind
}

func (rq *Queue) findReplicaForRemoval(rd *rfpb.RangeDescriptor, replicaStateMap map[uint64]constants.ReplicaState, localReplicaID uint64) *rfpb.ReplicaDescriptor {
	lastReplIDAdded := rd.LastAddedReplicaId
	if lastReplIDAdded != nil && rd.LastReplicaAddedAtUsec != nil {
		if rq.clock.Since(time.UnixMicro(rd.GetLastReplicaAddedAtUsec())) > *newReplicaGracePeriod {
			lastReplIDAdded = nil
		}
	}

	removableReplicas := rq.findRemovableReplicas(rd, replicaStateMap, lastReplIDAdded)

	if len(removableReplicas) == 0 {
		// there is nothing to remove
		return nil
	}

	nhids := make([]string, 0, len(removableReplicas))
	for _, repl := range removableReplicas {
		nhids = append(nhids, repl.GetNhid())
	}

	storesWithStats := rq.storeMap.GetStoresWithStatsFromIDs(nhids)

	var candidates []*candidate
	for _, su := range storesWithStats.Usages {
		candidates = append(candidates, &candidate{
			usage:                 su,
			replicaCount:          su.GetReplicaCount(),
			replicaCountMeanLevel: replicaCountMeanLevel(storesWithStats, su),
			fullDisk:              isDiskFull(su),
		})
	}

	if len(candidates) == 0 {
		// cannot find candidates for removal
		return nil
	}

	slices.SortFunc(candidates, compareByScoreAndID)

	for _, c := range candidates {
		for _, repl := range rd.GetReplicas() {
			if repl.GetNhid() == c.usage.GetNode().GetNhid() {
				if repl.GetReplicaId() != localReplicaID {
					return repl
				}
				// For simplicity, we don't want to select the local replica
				// as the removal target. Let's go to the next worst candidate.
				// TODO: support lease transfer so we can remove local replica.
				break
			}
		}
	}
	return nil
}

func (rq *Queue) removeReplica(ctx context.Context, rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	replicaStateMap := rq.store.GetReplicaStates(ctx, rd)
	localReplicaID := localRepl.ReplicaID()
	removingReplica := rq.findReplicaForRemoval(rd, replicaStateMap, localReplicaID)
	if removingReplica == nil {
		return nil
	}

	return &change{
		removeOp: &rfpb.RemoveReplicaRequest{
			Range:     rd,
			ReplicaId: removingReplica.GetReplicaId(),
		},
	}
}

func (rq *Queue) applyChange(ctx context.Context, change *change) error {
	var rd *rfpb.RangeDescriptor
	if change.splitOp != nil {
		rsp, err := rq.store.SplitRange(ctx, change.splitOp)
		// Increment RaftSplits counter.
		metrics.RaftSplits.With(prometheus.Labels{
			metrics.RaftNodeHostIDLabel:      rq.store.NHID(),
			metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
		}).Inc()
		if err != nil {
			rq.log.Errorf("Error splitting range, request: %+v: %s", change.splitOp, err)
			return err
		} else {
			rq.log.Infof("Successfully split range: %+v", rsp)
		}
	}
	if change.addOp != nil {
		rsp, err := rq.store.AddReplica(ctx, change.addOp)
		metrics.RaftMoves.With(prometheus.Labels{
			metrics.RaftNodeHostIDLabel:      rq.store.NHID(),
			metrics.RaftMoveLabel:            "add",
			metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
		}).Inc()
		if err != nil {
			rq.log.Errorf("AddReplica %+v err: %s", change.addOp, err)
			return err
		}
		rq.log.Infof("AddReplicaRequest finished: %+v", change.addOp)
		rd = rsp.GetRange()
	}
	if change.removeOp != nil {
		if rd != nil {
			change.removeOp.Range = rd
		}
		rsp, err := rq.store.RemoveReplica(ctx, change.removeOp)
		metrics.RaftMoves.With(prometheus.Labels{
			metrics.RaftNodeHostIDLabel:      rq.store.NHID(),
			metrics.RaftMoveLabel:            "remove",
			metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
		}).Inc()

		if err != nil {
			rq.log.Errorf("RemoveReplica %+v err: %s", change.removeOp, err)
			return err
		}
		rq.log.Infof("RemoveReplicaRequest finished: %+v", change.removeOp)

		replicaDesc := &rfpb.ReplicaDescriptor{RangeId: change.removeOp.GetRange().GetRangeId(), ReplicaId: change.removeOp.GetReplicaId()}
		// Remove the data from the now stopped node. This is best-effort only,
		// because we can remove the replica when the node is dead; and in this case,
		// we won't be able to connect to the node.
		c, err := rq.apiClient.GetForReplica(ctx, replicaDesc)
		if err != nil {
			rq.log.Warningf("RemoveReplica unable to remove data on c%dn%d, err getting api client: %s", replicaDesc.GetRangeId(), replicaDesc.GetReplicaId(), err)
			return nil
		}
		_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
			ReplicaId: replicaDesc.GetReplicaId(),
			Range:     rsp.GetRange(),
		})
		if err != nil {
			rq.log.Warningf("RemoveReplica unable to remove data err: %s", err)
			return nil
		}

		rq.log.Infof("Removed shard: c%dn%d", replicaDesc.GetRangeId(), replicaDesc.GetReplicaId())
	}
	if change.transferLeadershipOp != nil {
		_, err := rq.store.TransferLeadership(ctx, change.transferLeadershipOp)
		if err != nil {
			rq.log.Errorf("TransferLeadership %+v err: %s", change.transferLeadershipOp, err)
			return err
		}
		rq.log.Infof("TransferLeadershipRequest finished: %+v", change.transferLeadershipOp)
	}
	return nil
}

func (rq *Queue) processReplica(ctx context.Context, repl IReplica) (bool, error) {
	rd := repl.RangeDescriptor()
	if !rq.store.HaveLease(ctx, rd.GetRangeId()) {
		// the store doesn't have the lease of this range.
		rq.log.Debugf("store doesn't have lease for c%dn%d, do not process", repl.RangeID(), repl.ReplicaID())
		return false, nil
	}
	rq.log.Debugf("start to process c%dn%d", repl.RangeID(), repl.ReplicaID())
	usage, err := repl.Usage()
	if err != nil {
		rq.log.Errorf("failed to get Usage of replica c%dn%d", repl.RangeID(), repl.ReplicaID())
	}
	action, _ := rq.computeAction(rd, usage, repl.ReplicaID())

	var change *change

	switch action {
	case DriverNoop:
	case DriverSplitRange:
		rq.log.Debugf("split range (range_id: %d)", repl.RangeID())
		change = rq.splitRange(rd)
	case DriverAddReplica:
		rq.log.Debugf("add replica (range_id: %d)", repl.RangeID())
		change = rq.addReplica(rd)
	case DriverReplaceDeadReplica:
		rq.log.Debugf("replace dead replica (range_id: %d)", repl.RangeID())
		change = rq.replaceDeadReplica(rd)
	case DriverRemoveReplica:
		rq.log.Debugf("remove replica (range_id: %d)", repl.RangeID())
		change = rq.removeReplica(ctx, rd, repl)
	case DriverRemoveDeadReplica:
		rq.log.Debugf("remove dead replica (range_id: %d)", repl.RangeID())
		change = rq.removeDeadReplica(rd)
	case DriverRebalanceReplica:
		rq.log.Debugf("consider rebalance replica: (range_id: %d)", repl.RangeID())
		change = rq.rebalanceReplica(rd, repl)
	case DriverRebalanceLease:
		rq.log.Debugf("consider rebalance lease: (range_id: %d)", repl.RangeID())
		change = rq.rebalanceLease(rd, repl)
	}

	if change == nil {
		rq.log.Debugf("nothing to do for replica: (range_id: %d)", repl.RangeID())
		return false, nil
	}

	err = rq.applyChange(ctx, change)
	if err != nil {
		rq.log.Warningf("Error apply change to range_id: %d: %s", repl.RangeID(), err)
	}

	if action == DriverNoop || action == DriverRebalanceReplica || action == DriverRebalanceLease {
		return false, err
	}
	return true, err
}

func (rq *Queue) postProcess(ctx context.Context, repl IReplica, requeue bool) {
	rd := repl.RangeDescriptor()
	rq.mu.Lock()
	item, ok := rq.pqItemMap[rd.GetRangeId()]
	if !ok {
		alert.UnexpectedEvent("unexpected_pq_item_not_found", "pqItem not found for range %d", rd.GetRangeId())
		rq.mu.Unlock()
		return
	}
	requeue = requeue || item.requeue
	delete(rq.pqItemMap, rd.GetRangeId())
	rq.mu.Unlock()
	if requeue {
		rq.MaybeAdd(ctx, repl)
	}
}

func isDiskFull(su *rfpb.StoreUsage) bool {
	return isDiskCapacityReached(su, maximumDiskCapacity)
}

func isDiskFullForRebalance(su *rfpb.StoreUsage) bool {
	return isDiskCapacityReached(su, maxDiskCapacityForRebalance)
}

func isDiskCapacityReached(su *rfpb.StoreUsage, capacity float64) bool {
	bytesFree := su.GetTotalBytesFree()
	bytesUsed := su.GetTotalBytesUsed()
	return float64(bytesFree+bytesUsed)*capacity <= float64(bytesUsed)
}

func aboveMeanReplicaCountThreshold(mean float64) float64 {
	return mean + math.Max(mean*replicaCountMeanRatioThreshold, minReplicaCountThreshold)
}

func belowMeanReplicaCountThreshold(mean float64) float64 {
	return mean - math.Max(mean*replicaCountMeanRatioThreshold, minReplicaCountThreshold)
}

func aboveMeanLeaseCountThreshold(mean float64) float64 {
	return mean + math.Max(mean*leaseCountMeanRatioThreshold, minLeaseCountThreshold)
}

func belowMeanLeaseCountThreshold(mean float64) float64 {
	return mean - math.Max(mean*leaseCountMeanRatioThreshold, minLeaseCountThreshold)
}

type meanLevel int

const (
	aboveMean  meanLevel = -1
	aroundMean meanLevel = 0
	belowMean  meanLevel = 1
)

type candidate struct {
	nhid                  string
	usage                 *rfpb.StoreUsage
	fullDisk              bool
	replicaCountMeanLevel meanLevel
	replicaCount          int64
	leaseCount            int64
	leaseCountMeanLevel   meanLevel
}

// compare returns
//   - a positive number if a is a better fit than b;
//   - 0 if a and b is equivalent
//   - a negative number if a is a worse fit than b.
//
// The result reflects how much better or worse candidate a is.
func compareByScore(a *candidate, b *candidate) int {
	if a.fullDisk != b.fullDisk {
		if a.fullDisk {
			return -20
		}
		if b.fullDisk {
			return 20
		}
	}

	// [10, 12] or [-12, -10]
	if a.replicaCountMeanLevel != b.replicaCountMeanLevel {
		score := int(10 + math.Abs(float64(a.replicaCountMeanLevel-b.replicaCountMeanLevel)))
		if a.replicaCountMeanLevel > b.replicaCountMeanLevel {
			return score
		}
		return -score
	}

	// (-10, 10)
	diff := math.Abs(float64(a.replicaCount - b.replicaCount))
	if a.replicaCount < b.replicaCount {
		return int(math.Ceil(diff / float64(b.replicaCount) * 10))
	} else if a.replicaCount > b.replicaCount {
		return -int(math.Ceil(diff / float64(a.replicaCount) * 10))
	}

	// [10, 12] or [-12, -10]
	if a.leaseCountMeanLevel != b.leaseCountMeanLevel {
		score := int(10 + math.Abs(float64(a.leaseCountMeanLevel-b.leaseCountMeanLevel)))
		if a.leaseCountMeanLevel > b.leaseCountMeanLevel {
			return score
		}
		return -score
	}

	// (-10, 10)
	leaseCountDiff := math.Abs(float64(a.leaseCount - b.leaseCount))
	if a.leaseCount < b.leaseCount {
		return int(math.Ceil(leaseCountDiff / float64(b.leaseCount) * 10))
	} else if a.leaseCount > b.leaseCount {
		return -int(math.Ceil(leaseCountDiff / float64(a.leaseCount) * 10))
	}
	return 0
}

func compareByScoreAndID(a *candidate, b *candidate) int {
	if res := compareByScore(a, b); res != 0 {
		return res
	}
	return cmp.Compare(a.nhid, b.nhid)
}

func replicaCountMeanLevel(storesWithStats *storemap.StoresWithStats, su *rfpb.StoreUsage) meanLevel {
	maxReplicaCount := aboveMeanReplicaCountThreshold(storesWithStats.ReplicaCount.Mean)
	minReplicaCount := belowMeanReplicaCountThreshold(storesWithStats.ReplicaCount.Mean)
	curReplicaCount := float64(su.GetReplicaCount())
	if curReplicaCount < minReplicaCount {
		return belowMean
	} else if curReplicaCount >= maxReplicaCount {
		return aboveMean
	}
	return aroundMean
}

func leaseCountMeanLevel(storesWithStats *storemap.StoresWithStats, su *rfpb.StoreUsage) meanLevel {
	maxLeaseCount := aboveMeanReplicaCountThreshold(storesWithStats.LeaseCount.Mean)
	minLeaseCount := belowMeanReplicaCountThreshold(storesWithStats.LeaseCount.Mean)
	curLeaseCount := float64(su.GetLeaseCount())
	if curLeaseCount < minLeaseCount {
		return belowMean
	} else if curLeaseCount >= maxLeaseCount {
		return aboveMean
	}
	return aroundMean
}
