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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
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
	// The max number of retries per action. When a driver operation failed, we
	// will put the replica back on the queue to retry during post-process. An
	// alert will be fired once the max number of retries have reached, and we
	// won't put the replica back on to the queue during post-process.  However,
	// the store periodically scan the replicas and add to the queue if a driver
	// action is needed.
	maxRetry = 10
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

type RequeueType int

const (
	_ RequeueType = iota
	// Do not requeue.
	RequeueNoop
	// Current operation succeeded, but we want to requeue to see if we need to
	// perform other driver actions.
	RequeueCheckOtherActions
	// Current operation failed, but we want to retry.
	RequeueRetry
	// We need to wait to perform the current operation.
	RequeueWait
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

type IClient interface {
	HaveReadyConnections(ctx context.Context, rd *rfpb.ReplicaDescriptor) (bool, error)
	GetForReplica(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error)
}

// computeQuorum computes a quorum, which a majority of members from a peer set.
// In raft, when a quorum of nodes is unavailable, the cluster becomes
// unavailable.
func computeQuorum(numNodes int) int {
	return (numNodes / 2) + 1
}

type attemptRecord struct {
	action          DriverAction
	attempts        int
	nextAttemptTime time.Time
}

type driverTask struct {
	rangeID   uint64
	replicaID uint64

	processing bool
	requeue    bool

	attemptRecord attemptRecord

	item *priority_queue.Item[uint64]
}

type queueImpl interface {
	processReplica(ctx context.Context, repl IReplica, action DriverAction) RequeueType
	computeAction(ctx context.Context, repl IReplica) (DriverAction, float64)
	getReplica(rangeID uint64) (IReplica, error)
}

type baseQueue struct {
	impl queueImpl

	maxSize int
	stop    chan struct{}

	mu      sync.Mutex //protects pq, taskMap
	pq      *priority_queue.PriorityQueue[uint64]
	taskMap map[uint64]*driverTask

	clock clockwork.Clock

	log log.Logger

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel context.CancelFunc
}

func newBaseQueue(nhlog log.Logger, clock clockwork.Clock, impl queueImpl) *baseQueue {
	ctx, cancelFunc := context.WithCancel(context.Background())
	eg, gctx := errgroup.WithContext(ctx)
	return &baseQueue{
		clock:    clock,
		log:      nhlog,
		maxSize:  1000,
		pq:       &priority_queue.PriorityQueue[uint64]{},
		taskMap:  make(map[uint64]*driverTask),
		eg:       eg,
		egCtx:    gctx,
		egCancel: cancelFunc,
		impl:     impl,
	}
}

func (bq *baseQueue) pop() IReplica {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	item := heap.Pop(bq.pq).(*priority_queue.Item[uint64])
	rangeID := item.Value()
	task, ok := bq.taskMap[rangeID]
	if !ok {
		alert.UnexpectedEvent("unexpected_task_not_found", "task not found for range %d", rangeID)
		return nil
	}
	task.processing = true
	repl, err := bq.impl.getReplica(rangeID)
	if err != nil || repl.ReplicaID() != task.replicaID {
		log.Errorf("unable to get replica for c%dn%d: %s", task.rangeID, task.replicaID, err)
		delete(bq.taskMap, rangeID)
		return nil
	}
	return repl
}

func (bq *baseQueue) pushLocked(task *driverTask, priority float64) {
	item := priority_queue.NewItem(task.rangeID, priority)
	heap.Push(bq.pq, item)
	task.item = item
	bq.taskMap[task.rangeID] = task
}

func (bq *baseQueue) removeItemWithMinPriority() {
	item := bq.pq.RemoveItemWithMinPriority()
	if item == nil {
		return
	}
	rangeID := item.Value()
	task, ok := bq.taskMap[rangeID]
	if !ok {
		alert.UnexpectedEvent("unexpected_task_not_found", "task not found for range %d", rangeID)
		return
	}

	if task.processing {
		task.requeue = false
		return
	}
	delete(bq.taskMap, rangeID)
}

func (bq *baseQueue) Len() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.pq.Len()
}

func (bq *baseQueue) postProcess(ctx context.Context, repl IReplica, requeueType RequeueType) {
	rangeID := repl.RangeID()
	bq.mu.Lock()
	task, ok := bq.taskMap[rangeID]
	if !ok {
		alert.UnexpectedEvent("unexpected_task_not_found", "task not found for range %d", rangeID)
		bq.mu.Unlock()
		return
	}
	ar := attemptRecord{}
	if requeueType == RequeueRetry {
		ar = task.attemptRecord
		ar.attempts++
		ar.nextAttemptTime = bq.nextAttemptTime(ar.attempts)
	} else if requeueType == RequeueWait {
		ar = task.attemptRecord
	}
	delete(bq.taskMap, rangeID)
	bq.mu.Unlock()

	if ar.attempts >= maxRetry {
		alert.UnexpectedEvent("driver_action_retries_exceeded", "c%dn%d action: %s retries exceeded", rangeID, repl.ReplicaID(), ar.action)
		// do not add it to the queue
	} else if requeueType != RequeueNoop || task.requeue {
		bq.maybeAdd(ctx, repl, ar)
	}
}

func (bq *baseQueue) nextAttemptTime(attemptNumber int) time.Time {
	backoff := float64(1*time.Second) * math.Pow(2, float64(attemptNumber))
	return bq.clock.Now().Add(time.Duration(backoff))
}

func (bq *baseQueue) process(ctx context.Context, repl IReplica) RequeueType {
	action, _ := bq.impl.computeAction(ctx, repl)
	rangeID := repl.RangeID()
	bq.log.Debugf("start to process c%dn%d", rangeID, repl.ReplicaID())
	bq.mu.Lock()
	task, ok := bq.taskMap[rangeID]
	if !ok {
		alert.UnexpectedEvent("unexpected_task_not_found", "task not found for range %d", rangeID)
		bq.mu.Unlock()
		return RequeueNoop
	}
	ar := task.attemptRecord
	bq.mu.Unlock()
	if action == ar.action && !ar.nextAttemptTime.IsZero() {
		if bq.clock.Now().Before(ar.nextAttemptTime) {
			// Do nothing until nextAttemptTime becomes current
			return RequeueWait
		}
	}

	return bq.impl.processReplica(ctx, repl, action)
}

// The Queue is responsible for up-replicate, down-replicate and reblance ranges
// across the stores.
type Queue struct {
	*baseQueue

	storeMap storemap.IStoreMap
	store    IStore

	apiClient IClient
}

func NewQueue(store IStore, gossipManager interfaces.GossipService, nhlog log.Logger, apiClient IClient, clock clockwork.Clock) *Queue {
	storeMap := storemap.New(gossipManager, clock)
	q := &Queue{
		storeMap:  storeMap,
		store:     store,
		apiClient: apiClient,
	}
	q.baseQueue = newBaseQueue(nhlog, clock, q)
	return q
}

func (rq *Queue) getReplica(rangeID uint64) (IReplica, error) {
	return rq.store.GetReplica(rangeID)
}

// computeAction computes the action needed and its priority.
func (rq *Queue) computeAction(ctx context.Context, repl IReplica) (DriverAction, float64) {
	rd := repl.RangeDescriptor()
	action := DriverNoop
	if !rq.store.HaveLease(ctx, rd.GetRangeId()) {
		return action, action.Priority()
	}
	if rq.storeMap == nil {
		return action, action.Priority()
	}
	replicas := rd.GetReplicas()
	curReplicas := len(replicas)
	if curReplicas == 0 {
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
		action = DriverAddReplica
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
		action = DriverNoop
		return action, action.Priority()
	}

	if curReplicas <= minReplicas && numDeadReplicas > 0 {
		action = DriverReplaceDeadReplica
		return action, action.Priority()
	}

	if numDeadReplicas > 0 {
		action = DriverRemoveDeadReplica
		return action, action.Priority()
	}

	if curReplicas > minReplicas {
		action = DriverRemoveReplica
		adjustedPriority := action.Priority() - float64(curReplicas%2)
		return action, adjustedPriority
	}

	// Do not split if there is a replica is dead or suspect or a replica is
	// in the middle of a removal.
	allReady := rq.storeMap.AllAvailableStoresReady()
	isClusterHealthy := len(replicasByStatus.SuspectReplicas) == 0 && numDeadReplicas == 0 && allReady
	canSplit := isClusterHealthy && len(rd.GetRemoved()) == 0
	if canSplit {
		if maxRangeSizeBytes := config.MaxRangeSizeBytes(); maxRangeSizeBytes > 0 {
			usage, err := repl.Usage()
			if err != nil {
				rq.log.Errorf("failed to get Usage of replica c%dn%d", repl.RangeID(), repl.ReplicaID())
			} else {
				if sizeUsed := usage.GetEstimatedDiskBytesUsed(); sizeUsed >= maxRangeSizeBytes {
					action = DriverSplitRange
					adjustedPriority := action.Priority() + float64(sizeUsed-maxRangeSizeBytes)/float64(sizeUsed)*100.0
					return action, adjustedPriority
				}
			}
		}
	} else {
		rq.log.Debugf("cannot split range %d: num of suspect replicas: %d, num deadReplicas: %d, num replicas marked for removal: %d, allReady=%t", rd.GetRangeId(), len(replicasByStatus.SuspectReplicas), numDeadReplicas, len(rd.GetRemoved()), allReady)
	}

	if rd.GetRangeId() == constants.MetaRangeID {
		// Do not try to re-balance meta-range.
		//
		// When meta-range is moved onto a different node, range cache has to
		// update its range descriptor. Before the range descriptor get updated,
		// SyncPropose to all other ranges can fail temporarily because the range
		// descriptor is not current. Therefore, we should only move meta-range
		// when it's absolutely necessary.
		action = DriverNoop
		return action, action.Priority()
	}

	// Do not try to rebalance replica or leases if there is a dead or suspect,
	// because it can make the system more unstable.
	if isClusterHealthy {
		// For DriverConsiderRebalance check if there are rebalance opportunities.
		storesWithStats := rq.storeMap.GetStoresWithStats()
		op := rq.findRebalanceReplicaOp(rd, storesWithStats, repl.ReplicaID())
		if op != nil {
			log.Debugf("find rebalancing opportunities: from (nhid=%q, replicaCount=%d, isReady=%t) to (nhid=%q, replicaCount=%d, isReady=%t)", op.from.nhid, op.from.replicaCount, op.from.usage.GetIsReady(), op.to.nhid, op.to.replicaCount, op.to.usage.GetIsReady())
			action = DriverRebalanceReplica
			return action, action.Priority()
		}
		op = rq.findRebalanceLeaseOp(ctx, rd, repl.ReplicaID())
		if op != nil {
			action = DriverRebalanceLease
			return action, action.Priority()
		}
	} else {
		rq.log.Debugf("do not consider rebalance because range %d is not healthy. num of suspect replicas: %d, num deadReplicas: %d, allReady: %t", rd.GetRangeId(), len(replicasByStatus.SuspectReplicas), numDeadReplicas, allReady)
	}

	action = DriverNoop
	return action, action.Priority()
}

func (bq *baseQueue) maybeAdd(ctx context.Context, repl IReplica, ar attemptRecord) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	action, priority := bq.impl.computeAction(ctx, repl)
	if action == DriverNoop {
		return
	}

	rangeID := repl.RangeID()
	task, ok := bq.taskMap[rangeID]
	if ok {
		// The item is processing. Mark to be requeued.
		if task.processing {
			task.requeue = true
			return
		}
		if task.attemptRecord.action != action {
			// clear the attempt record if action is different
			task.attemptRecord = attemptRecord{
				action: action,
			}
		}
		bq.pq.Update(task.item, priority)
		return
	}
	task = &driverTask{
		rangeID:   rangeID,
		replicaID: repl.ReplicaID(),
	}

	if action == ar.action {
		task.attemptRecord = ar
	} else {
		task.attemptRecord = attemptRecord{
			action: action,
		}
	}

	bq.pushLocked(task, priority)

	// If the priroityQueue if full, let's remove the item with the lowest priority.
	if pqLen := bq.pq.Len(); pqLen > bq.maxSize {
		bq.removeItemWithMinPriority()
	}
}

func (rq *Queue) MaybeAdd(ctx context.Context, replica IReplica) {
	rq.maybeAdd(ctx, replica, attemptRecord{})
}

func (bq *baseQueue) Start() {
	bq.eg.Go(func() error {
		queueDelay := bq.clock.NewTicker(queueWaitDuration)
		defer queueDelay.Stop()
		for {
			select {
			case <-bq.egCtx.Done():
				return nil
			case <-queueDelay.Chan():
				bq.processQueue()
			}
		}
	})
}

func (bq *baseQueue) Stop() {
	bq.log.Infof("Driver shutdown started")
	now := time.Now()
	defer func() {
		bq.log.Infof("Driver shutdown finished in %s", time.Since(now))
	}()

	bq.egCancel()
	bq.eg.Wait()
}

func (bq *baseQueue) processQueue() {
	if bq.Len() == 0 {
		return
	}
	repl := bq.pop()
	if repl == nil {
		return
	}

	requeueType := bq.process(bq.egCtx, repl)
	bq.postProcess(bq.egCtx, repl, requeueType)
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
	existing := append(rd.GetReplicas(), rd.GetRemoved()...)
	for _, su := range storesWithStats.Usages {
		if storeHasReplica(su.GetNode(), existing) {
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

func (rq *Queue) rebalanceLease(ctx context.Context, rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	op := rq.findRebalanceLeaseOp(ctx, rd, localRepl.ReplicaID())
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

func (rq *Queue) findRebalanceLeaseOp(ctx context.Context, rd *rfpb.RangeDescriptor, localReplicaID uint64) *rebalanceOp {
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

		if hasReadyConnections, err := rq.apiClient.HaveReadyConnections(ctx, repl); err != nil || !hasReadyConnections {
			// Do not try to rebalance to replicas that cannot be connected to.
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

	// Add replicas that are in the middle of removal to existingStores to
	// prevent them from becoming target candidates.
	for _, repl := range rd.GetRemoved() {
		store, ok := allStores[repl.GetNhid()]
		if !ok {
			// The store might not be available rn.
			continue
		}
		existingStores[repl.GetNhid()] = store
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
		rd = rsp.GetRange()
		rq.log.Infof("AddReplicaRequest finished: op: %+v, rd: %+v", change.addOp, rd)
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
		rq.log.Infof("RemoveReplicaRequest finished: op: %+v, rd: %+v", change.removeOp, rsp.GetRange())

		var replToRemove *rfpb.ReplicaDescriptor
		for _, repl := range change.removeOp.GetRange().GetReplicas() {
			if repl.GetReplicaId() == change.removeOp.GetReplicaId() {
				replToRemove = repl
			}
		}

		// Remove the data from the now stopped node. This is best-effort only,
		// because we can remove the replica when the node is dead; and in this case,
		// we won't be able to connect to the node.
		c, err := rq.apiClient.GetForReplica(ctx, replToRemove)
		if err != nil {
			rq.log.Warningf("RemoveReplica unable to remove data on c%dn%d, err getting api client: %s", replToRemove.GetRangeId(), replToRemove.GetReplicaId(), err)
			return nil
		}
		removeDataRsp, err := c.RemoveData(ctx, &rfpb.RemoveDataRequest{
			ReplicaId: replToRemove.GetReplicaId(),
			Range:     rsp.GetRange(),
		})
		if err != nil {
			rq.log.Warningf("RemoveReplica unable to remove data err: %s", err)
			return nil
		}

		rq.log.Infof("Removed shard: c%dn%d, rd: %+v", replToRemove.GetRangeId(), replToRemove.GetReplicaId(), removeDataRsp.GetRange())
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

func (rq *Queue) processReplica(ctx context.Context, repl IReplica, action DriverAction) RequeueType {
	var change *change
	rd := repl.RangeDescriptor()
	rangeID := repl.RangeID()

	switch action {
	case DriverNoop:
	case DriverSplitRange:
		rq.log.Debugf("split range (range_id: %d)", rangeID)
		change = rq.splitRange(rd)
	case DriverAddReplica:
		rq.log.Debugf("add replica (range_id: %d)", rangeID)
		change = rq.addReplica(rd)
	case DriverReplaceDeadReplica:
		rq.log.Debugf("replace dead replica (range_id: %d)", rangeID)
		change = rq.replaceDeadReplica(rd)
	case DriverRemoveReplica:
		rq.log.Debugf("remove replica (range_id: %d)", rangeID)
		change = rq.removeReplica(ctx, rd, repl)
	case DriverRemoveDeadReplica:
		rq.log.Debugf("remove dead replica (range_id: %d)", rangeID)
		change = rq.removeDeadReplica(rd)
	case DriverRebalanceReplica:
		rq.log.Debugf("consider rebalance replica: (range_id: %d)", rangeID)
		change = rq.rebalanceReplica(rd, repl)
	case DriverRebalanceLease:
		rq.log.Debugf("consider rebalance lease: (range_id: %d)", rangeID)
		change = rq.rebalanceLease(ctx, rd, repl)
	}

	if change == nil {
		rq.log.Debugf("nothing to do for replica: (range_id: %d)", rangeID)
		return RequeueNoop
	}

	err := rq.applyChange(ctx, change)
	if err != nil {
		rq.log.Warningf("Error apply change to range_id: %d: %s", rangeID, err)
	}

	if action == DriverNoop || action == DriverRebalanceReplica || action == DriverRebalanceLease {
		return RequeueNoop
	}

	if err != nil {
		rq.log.Errorf("failed to process replica: %s", err)
		return RequeueRetry
	} else {
		return RequeueCheckOtherActions
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
