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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	minReplicasPerRange   = flag.Int("cache.raft.min_replicas_per_range", 3, "The minimum number of replicas each range should have")
	newReplicaGracePeriod = flag.Duration("cache.raft.new_replica_grace_period", 5*time.Minute, "The amount of time we allow for a new replica to catch up to the leader's before we start to consider it to be behind.")
)

const (
	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to receive ranges moved from other nodes.
	maximumDiskCapacity = .95
)

type DriverAction int

const (
	_ DriverAction = iota
	DriverNoop
	DriverRemoveReplica
	DriverRemoveDeadReplica
	DriverAddReplica
	DriverReplaceDeadReplica
	DriverConsiderRebalance
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
	// to be considerred above or below the mean.
	minReplicaCountThreshold = 2
)

func (a DriverAction) Priority() float64 {
	switch a {
	case DriverReplaceDeadReplica:
		return 400
	case DriverAddReplica:
		return 300
	case DriverRemoveDeadReplica:
		return 200
	case DriverRemoveReplica:
		return 100
	case DriverConsiderRebalance, DriverNoop:
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
	case DriverConsiderRebalance:
		return "consider-rebalance"
	default:
		return "unknown"
	}
}

type IReplica interface {
	RangeDescriptor() *rfpb.RangeDescriptor
	ReplicaID() uint64
	ShardID() uint64
}

type IStore interface {
	GetReplica(rangeID uint64) (*replica.Replica, error)
	HaveLease(rangeID uint64) bool
	AddReplica(ctx context.Context, req *rfpb.AddReplicaRequest) (*rfpb.AddReplicaResponse, error)
	RemoveReplica(ctx context.Context, req *rfpb.RemoveReplicaRequest) (*rfpb.RemoveReplicaResponse, error)
	GetReplicaStates(ctx context.Context, rd *rfpb.RangeDescriptor) map[uint64]constants.ReplicaState
}

// computeQuorum computes a quorum, which a majority of members from a peer set.
// In raft, when a quorum of nodes is unavailable, the cluster becomes
// unavailable.
func computeQuorum(numNodes int) int {
	return (numNodes / 2) + 1
}

// computeAction computes the action needed and its priority.
func (rq *Queue) computeAction(replicas []*rfpb.ReplicaDescriptor) (DriverAction, float64) {
	if rq.storeMap == nil {
		action := DriverNoop
		return action, action.Priority()
	}
	curReplicas := len(replicas)
	desiredQuorum := computeQuorum(*minReplicasPerRange)
	quorum := computeQuorum(curReplicas)

	if curReplicas < *minReplicasPerRange {
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

	if curReplicas <= *minReplicasPerRange && numDeadReplicas > 0 {
		action := DriverReplaceDeadReplica
		return action, action.Priority()
	}

	if numDeadReplicas > 0 {
		action := DriverRemoveDeadReplica
		return action, action.Priority()
	}

	if curReplicas > *minReplicasPerRange {
		action := DriverRemoveReplica
		ajustedPriority := action.Priority() - float64(curReplicas%2)
		return action, ajustedPriority
	}

	action := DriverConsiderRebalance
	return action, action.Priority()
}

type pqItem struct {
	rangeID   uint64
	shardID   uint64
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

// The Queue is responsible for up-replicate, down-replicate and reblance ranges
// across the stores.
type Queue struct {
	pq       *priorityQueue
	storeMap *storemap.StoreMap
	store    IStore

	pqItemMap sync.Map // shardID -> *pqItem
	maxSize   int
	stop      chan struct{}

	mu      sync.Mutex //protects started and pq
	started bool
}

func NewQueue(store IStore, gossipManager interfaces.GossipService) *Queue {
	storeMap := storemap.New(gossipManager)
	return &Queue{
		storeMap: storeMap,
		pq:       &priorityQueue{},
		store:    store,
		maxSize:  100,
	}
}

func (rq *Queue) shouldQueue(repl IReplica) (bool, float64) {
	rd := repl.RangeDescriptor()

	if !rq.store.HaveLease(rd.GetRangeId()) {
		// The store doesn't have lease for this range. do not queue.
		log.Debugf("should not queue because we don't have lease")
		return false, 0
	}

	action, priority := rq.computeAction(rd.GetReplicas())
	log.Debugf("shouldQueue for replica:%d returns action:%s, with priority %.2f", repl.ShardID(), action, priority)
	if action == DriverNoop {
		log.Debugf("should not queue because no-op")
		return false, 0
	} else if action != DriverConsiderRebalance {
		return true, priority
	}

	// TODO: For DriverConsiderRebalance check if there are rebalance targets.
	return false, 0
}

func (rq *Queue) push(item *pqItem) {
	rq.mu.Lock()
	heap.Push(rq.pq, item)
	rq.mu.Unlock()

	rq.pqItemMap.Store(item.rangeID, item)
}

func (rq *Queue) update(item *pqItem, priority float64) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if item.processing {
		item.requeue = true
		return
	}
	item.priority = priority

	heap.Fix(rq.pq, item.index)
}

func (rq *Queue) getItemWithMinPriority() *pqItem {
	rq.mu.Lock()
	defer rq.mu.Unlock()
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
func (rq *Queue) MaybeAdd(replica IReplica) {
	shouldQueue, priority := rq.shouldQueue(replica)
	if !shouldQueue {
		return
	}

	rd := replica.RangeDescriptor()
	itemIface, ok := rq.pqItemMap.Load(rd.GetRangeId())
	if ok {
		item, ok := itemIface.(*pqItem)
		if !ok {
			alert.UnexpectedEvent("unexpected_pq_item_map_type_error_in_replicate_queue")
		}
		// update item.priority and item.requeue
		rq.update(item, priority)
		log.Infof("updated priority for replica rangeID=%d", item.rangeID)
		return
	}

	item := &pqItem{
		rangeID:    rd.GetRangeId(),
		shardID:    replica.ShardID(),
		replicaID:  replica.ReplicaID(),
		priority:   priority,
		insertTime: time.Now(),
	}
	rq.push(item)
	log.Infof("queued replica rangeID=%d", item.rangeID)

	// If the priroityQueue if full, let's remove the item with the lowest priority.
	if pqLen := rq.Len(); pqLen > rq.maxSize {
		rq.remove(rq.getItemWithMinPriority())
	}
}

func (rq *Queue) remove(item *pqItem) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if item.processing {
		item.requeue = false
		return
	}
	if item.index >= 0 {
		heap.Remove(rq.pq, item.index)
	}
	rq.pqItemMap.Delete(item.rangeID)
}

func (rq *Queue) Start(ctx context.Context) {
	go func() {
		queueDelay := time.NewTicker(queueWaitDuration)
		defer queueDelay.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-queueDelay.C:
			}
			if rq.Len() == 0 {
				continue
			}
			rq.mu.Lock()
			item := heap.Pop(rq.pq).(*pqItem)
			item.processing = true
			rq.mu.Unlock()
			repl, err := rq.store.GetReplica(item.rangeID)
			if err != nil || repl.ReplicaID() != item.replicaID {
				log.Errorf("unable to get replica for shard_id: %d, replica_id:%d: %s", item.replicaID, item.shardID, err)
				rq.pqItemMap.Delete(item.rangeID)
				continue
			}

			requeue, err := rq.processReplica(repl)
			if err != nil {
				// TODO: check if err can be retried.
				log.Errorf("failed to process replica: %s", err)
			} else {
				log.Debugf("successfully processed replica: %d", repl.ShardID())
			}
			rq.mu.Lock()
			item.requeue = requeue
			rq.mu.Unlock()
			rq.postProcess(repl)
		}
	}()
}

// findReplacingReplica finds a dead replica to be removed.
func findReplacingReplica(replicas []*rfpb.ReplicaDescriptor, replicaByStatus *storemap.ReplicasByStatus) *rfpb.ReplicaDescriptor {
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
func (rq *Queue) findNodeForAllocation(rd *rfpb.RangeDescriptor) *rfpb.NodeDescriptor {
	storesWithStats := rq.storeMap.GetStoresWithStats()
	var candidates []*candidate
	for _, su := range storesWithStats.Usages {
		if storeHasReplica(su.GetNode(), rd.GetReplicas()) {
			log.Debugf("skip node %+v because the replica is already on the node", su.GetNode())
			continue
		}
		if isDiskFull(su) {
			log.Debugf("skip node %+v because the disk is full", su)
			continue
		}
		log.Debugf("add node %+v to candidate list", su.GetNode())
		candidates = append(candidates, &candidate{
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
		return -int(compare(a, b))
	})
	return candidates[0].usage.GetNode()
}

type change struct {
	addOp    *rfpb.AddReplicaRequest
	removeOp *rfpb.RemoveReplicaRequest
}

func (rq *Queue) addReplica(rd *rfpb.RangeDescriptor) *change {
	target := rq.findNodeForAllocation(rd)
	if target == nil {
		log.Debugf("cannot find targets for range descriptor:%+v", rd)
		return nil
	}

	return &change{
		addOp: &rfpb.AddReplicaRequest{
			Range: rd,
			Node:  target,
		},
	}
}

func (rq *Queue) replaceDeadReplica(rd *rfpb.RangeDescriptor, replicasByStatus *storemap.ReplicasByStatus) *change {
	replacing := findReplacingReplica(rd.GetReplicas(), replicasByStatus)
	if replacing == nil {
		// nothing to remove
		return nil
	}
	change := rq.addReplica(rd)
	if change == nil {
		log.Debug("replaceDeadReplica cannot find node for allocation")
		return nil
	}

	change.removeOp = &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: replacing.GetReplicaId(),
	}

	return change
}

func (rq *Queue) findRemovableReplicas(rd *rfpb.RangeDescriptor, brandNewReplicaID *uint64) []*rfpb.ReplicaDescriptor {
	replicaStateMap :=
		rq.store.GetReplicaStates(context.TODO(), rd)

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
			if brandNewReplicaID != nil && r.GetReplicaId() != *brandNewReplicaID {
				replicasBehind = append(replicasBehind, r)
			}
		}
	}

	quorum := computeQuorum(len(rd.GetReplicas()) - 1)
	if numUpToDateReplicas < quorum {
		// The number of up-to-date replicas is less than quorum. Don't remove
		log.Debugf("there are %d up-to-date replicas and quorum is %d, don't remove", numUpToDateReplicas, quorum)
		return nil
	}

	if numUpToDateReplicas > quorum {
		// Any replica can be removed.
		log.Debugf("there are %d up-to-date replicas and quorum is %d, any replicas can be removed", numUpToDateReplicas, quorum)
		return rd.GetReplicas()
	}

	// The number of up-to-date-replicas equals to the quorum. We only want to delete replicas that are behind.
	log.Debugf("there are %d up-to-date replicas and quorum is %d, only remove behind replicas", numUpToDateReplicas, quorum)
	return replicasBehind
}

func (rq *Queue) findReplicaForRemoval(rd *rfpb.RangeDescriptor, localRepl IReplica) *rfpb.ReplicaDescriptor {
	lastReplIDAdded := rd.LastAddedReplicaId
	if lastReplIDAdded != nil && rd.LastReplicaAddedAtUsec != nil {
		if time.Since(time.UnixMicro(rd.GetLastReplicaAddedAtUsec())) > *newReplicaGracePeriod {
			lastReplIDAdded = nil
		}
	}

	removableReplicas := rq.findRemovableReplicas(rd, lastReplIDAdded)

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

	slices.SortFunc(candidates, func(a, b *candidate) int {
		// The worst targets are at the front
		return int(compare(a, b))
	})

	for _, c := range candidates {
		for _, repl := range rd.GetReplicas() {
			if repl.GetNhid() == c.usage.GetNode().GetNhid() {
				if repl.GetReplicaId() != localRepl.ReplicaID() {
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

func (rq *Queue) removeReplica(rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	removingReplica := rq.findReplicaForRemoval(rd, localRepl)
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
	if change.addOp != nil {
		rsp, err := rq.store.AddReplica(ctx, change.addOp)
		if err != nil {
			log.Errorf("AddReplica err: %s", err)
			return err
		}
		log.Infof("AddReplicaRequest finished: %+v", change.addOp)
		rd = rsp.GetRange()
	}
	if change.removeOp != nil {
		if rd != nil {
			change.removeOp.Range = rd
		}
		_, err := rq.store.RemoveReplica(ctx, change.removeOp)
		if err != nil {
			log.Errorf("RemoveReplica err: %s", err)
			return err
		}
		log.Infof("RemoveReplicaRequest finished: %+v", change.removeOp)
	}
	return nil

}

func (rq *Queue) processReplica(repl IReplica) (bool, error) {
	rd := repl.RangeDescriptor()
	if !rq.store.HaveLease(rd.GetRangeId()) {
		// the store doesn't have the lease of this range.
		log.Debugf("store doesn't have lease for shard_id: %d, replica_id:%d, do not process", repl.ReplicaID(), repl.ShardID())
		return false, nil
	}
	log.Debugf("start to process shard_id: %d, replica_id:%d", repl.ReplicaID(), repl.ShardID())
	action, _ := rq.computeAction(rd.GetReplicas())

	replicasByStatus := rq.storeMap.DivideByStatus(rd.GetReplicas())
	var change *change

	switch action {
	case DriverNoop:
	case DriverAddReplica:
		log.Debugf("add replica: %d", repl.ShardID())
		change = rq.addReplica(rd)
	case DriverReplaceDeadReplica:
		log.Debugf("replace dead replica: %d", repl.ShardID())
		change = rq.replaceDeadReplica(rd, replicasByStatus)
	case DriverRemoveReplica:
		log.Debugf("remove replica: %d", repl.ShardID())
		change = rq.removeReplica(rd, repl)
		// TODO
	case DriverRemoveDeadReplica:
		log.Debugf("remove dead replica: %d", repl.ShardID())
		// TODO
	case DriverConsiderRebalance:
		log.Debugf("remove consider rebalance: %d", repl.ShardID())
		// TODO
	}

	if change == nil {
		log.Debugf("nothing to do for replica: %d", repl.ShardID())
		return false, nil
	}

	err := rq.applyChange(context.TODO(), change)
	if err != nil {
		log.Warningf("Error apply change: %s", err)
	}

	if action == DriverNoop || action == DriverConsiderRebalance {
		return false, err
	}
	return true, err
}

func (rq *Queue) postProcess(repl IReplica) {
	rd := repl.RangeDescriptor()
	itemIface, ok := rq.pqItemMap.Load(rd.GetRangeId())
	if !ok {
		alert.UnexpectedEvent("unexpected_pq_item_not_found")
	}
	item, ok := itemIface.(*pqItem)
	if !ok {
		alert.UnexpectedEvent("unexpected_pq_item_map_type_error_in_replicate_queue")
	}
	rq.mu.Lock()
	requeue := item.requeue
	rq.mu.Unlock()

	if requeue {
		rq.MaybeAdd(repl)
	}
	rq.pqItemMap.Delete(rd.GetRangeId())
}

func isDiskFull(su *rfpb.StoreUsage) bool {
	bytesFree := su.GetTotalBytesFree()
	bytesUsed := su.GetTotalBytesUsed()
	return float64(bytesFree+bytesUsed)*maximumDiskCapacity <= float64(bytesUsed)
}

func aboveMeanReplicaCountThreshold(mean float64) float64 {
	return mean + math.Max(mean*replicaCountMeanRatioThreshold, minReplicaCountThreshold)
}

func belowMeanReplicaCountThreshold(mean float64) float64 {
	return mean + math.Max(mean*replicaCountMeanRatioThreshold, minReplicaCountThreshold)
}

type meanLevel int

const (
	aboveMean  meanLevel = -1
	aroundMean meanLevel = 0
	belowMean  meanLevel = 1
)

type candidate struct {
	usage                 *rfpb.StoreUsage
	fullDisk              bool
	replicaCountMeanLevel meanLevel
	replicaCount          int64
}

// compare returns
//   - a positive number if a is a better fit than b;
//   - 0 if a and b is equivalent
//   - a negative number if a is a worse fit than b.
func compare(a *candidate, b *candidate) float64 {
	if a.fullDisk != b.fullDisk {
		if a.fullDisk {
			return 20
		}
		if b.fullDisk {
			return -20
		}
	}
	if a.replicaCountMeanLevel != b.replicaCountMeanLevel {
		score := 10 + math.Abs(float64(a.replicaCountMeanLevel-b.replicaCountMeanLevel))
		if a.replicaCountMeanLevel > b.replicaCountMeanLevel {
			return score
		}
		return -10
	}

	diff := math.Abs(float64(a.replicaCount - b.replicaCount))
	if a.replicaCount < b.replicaCount {
		return diff / float64(b.replicaCount)
	} else if a.replicaCount > b.replicaCount {
		return diff / float64(a.replicaCount)
	}
	return float64(cmp.Compare(a.usage.GetNode().GetNhid(), b.usage.GetNode().GetNhid()))
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
