// Package driver implements a priority queue that drives Raft range management
// decisions. It periodically examines ranges and partitions, determines what
// action (if any) is needed, and executes the appropriate change.
//
// For ranges, the driver handles:
//   - Up-replication: adding replicas when a range has fewer than the configured minimum.
//   - Down-replication: removing replicas when a range exceeds the minimum.
//   - Dead replica replacement and removal.
//   - Range splitting when a range exceeds the target size.
//   - Replica rebalancing to distribute ranges evenly across stores.
//   - Lease rebalancing to distribute lease ownership evenly across stores.
//   - Finishing replica removal by cleaning up data on removed nodes.
//
// For partitions, the driver handles initialization of new partitions by
// creating the required Raft shards across available nodes.
//
// Actions are prioritized so that critical operations (e.g. replacing dead
// replicas) run before less urgent ones (e.g. rebalancing). Failed actions are
// retried with exponential backoff up to a maximum retry count.
package driver

import (
	"cmp"
	"container/heap"
	"context"
	"encoding/binary"
	"flag"
	"hash/fnv"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
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
	minReplicasPerRange        = flag.Int("cache.raft.min_replicas_per_range", 3, "The minimum number of replicas each range should have")
	minMetaRangeReplicas       = flag.Int("cache.raft.min_meta_range_replicas", 5, "The minimum number of replicas the meta range should have")
	missingLeaseCountThreshold = flag.Int("cache.raft.missing_lease_count_threshold", 5, "When the number of ranges without leases is greater than this number, don't rebalance leases")
	newReplicaGracePeriod      = flag.Duration("cache.raft.new_replica_grace_period", 5*time.Minute, "The amount of time we allow for a new replica to catch up to the leader's before we start to consider it to be behind.")

	rebalanceQPSBandRatio  = flag.Float64("cache.raft.rebalance_qps_band_ratio", 0.10, "A store is over/underfull when its QPS deviates from the mean by more than this fraction of the mean")
	rebalanceQPSFloor      = flag.Float64("cache.raft.rebalance_qps_floor", 100, "Minimum absolute band width in QPS; also the mean QPS below which load-based rebalancing is inactive for a dimension")
	minLeaseLoadFraction   = flag.Float64("cache.raft.min_lease_load_fraction", 0.005, "Don't move a lease whose read QPS is a smaller fraction of its store's read QPS than this")
	minReplicaLoadFraction = flag.Float64("cache.raft.min_replica_load_fraction", 0.02, "Don't move a replica whose propose QPS is a smaller fraction of its store's propose QPS than this")
	rangeRebalanceCooldown = flag.Duration("cache.raft.range_rebalance_cooldown", 5*time.Minute, "Don't rebalance a range within this duration of its last rebalance op")
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
	DriverFinishReplicaRemoval
	DriverRemoveReplica
	DriverRemoveDeadReplica
	DriverAddReplica
	DriverReplaceDeadReplica
	DriverRebalanceReplica
	DriverRebalanceLease
	// Partition-level actions
	DriverInitializePartition
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

func (r RequeueType) String() string {
	switch r {
	case RequeueNoop:
		return "requeue-noop"
	case RequeueCheckOtherActions:
		return "requeue-check-other-actions"
	case RequeueRetry:
		return "requeue-retry"
	case RequeueWait:
		return "requeue-wait"
	default:
		return "requeue-unknown"
	}
}

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
	case DriverInitializePartition:
		return 800
	case DriverReplaceDeadReplica:
		return 700
	case DriverAddReplica:
		return 600
	case DriverFinishReplicaRemoval:
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
	case DriverRemoveDeadReplica:
		return "remove-dead-replica"
	case DriverAddReplica:
		return "add-replica"
	case DriverRemoveReplica:
		return "remove-replica"
	case DriverReplaceDeadReplica:
		return "replace-dead-replica"
	case DriverFinishReplicaRemoval:
		return "finish-replica-removal"
	case DriverSplitRange:
		return "split-range"
	case DriverRebalanceReplica:
		return "consider-rebalance-replica"
	case DriverRebalanceLease:
		return "consider-rebalance-lease"
	case DriverNoop:
		return "no-op"
	case DriverInitializePartition:
		return "initialize-partition"
	default:
		return "unknown"
	}
}

type IReplica interface {
	ReplicaID() uint64
	RangeID() uint64
	Usage() (*rfpb.ReplicaUsage, error)
}

type IStore interface {
	GetReplica(rangeID uint64) (*replica.Replica, error)
	GetRange(rangeID uint64) *rfpb.RangeDescriptor
	HaveLease(ctx context.Context, rangeID uint64) bool
	AddReplica(ctx context.Context, req *rfpb.AddReplicaRequest) (*rfpb.AddReplicaResponse, error)
	RemoveReplica(ctx context.Context, req *rfpb.RemoveReplicaRequest) (*rfpb.RemoveReplicaResponse, error)
	GetReplicaStates(ctx context.Context, rd *rfpb.RangeDescriptor) map[uint64]constants.ReplicaState
	SplitRange(ctx context.Context, req *rfpb.SplitRangeRequest) (*rfpb.SplitRangeResponse, error)
	TransferLeadership(ctx context.Context, req *rfpb.TransferLeadershipRequest) (*rfpb.TransferLeadershipResponse, error)
	NHID() string
	ReserveRangeIDs(ctx context.Context, n int) ([]uint64, error)
	InitializeShardsForPartition(ctx context.Context, nodeGrpcAddrs map[string]string, partition disk.Partition) error
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

type TaskType int

const (
	_ TaskType = iota
	RangeTaskType
	PartitionTaskType
)

type taskKey struct {
	taskType    TaskType
	rangeID     uint64 // used for range tasks
	partitionID string // used for partition tasks
}

type rangeTask struct {
	repl IReplica
}

type partitionTask struct {
	config disk.Partition
	pd     *rfpb.PartitionDescriptor
}

type driverTask struct {
	key taskKey

	// Task-type-specific data
	rangeTask     *rangeTask
	partitionTask *partitionTask

	processing bool
	requeue    bool

	attemptRecord attemptRecord

	item *priority_queue.Item[taskKey]
}

type queueImpl interface {
	processTask(ctx context.Context, task *driverTask, action DriverAction) RequeueType
	computeAction(ctx context.Context, task *driverTask) (DriverAction, float64)
	getReplica(rangeID uint64) (IReplica, error)
	// clearLeaseBlockedHint drops any cached per-range lease-blocked hint when
	// a range task finishes, so hints for deleted ranges don't accumulate.
	clearLeaseBlockedHint(rangeID uint64)
}

type baseQueue struct {
	impl queueImpl

	maxSize int
	stop    chan struct{}

	mu      sync.Mutex //protects pq, taskMap
	pq      *priority_queue.PriorityQueue[taskKey]
	taskMap map[taskKey]*driverTask

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
		pq:       &priority_queue.PriorityQueue[taskKey]{},
		taskMap:  make(map[taskKey]*driverTask),
		eg:       eg,
		egCtx:    gctx,
		egCancel: cancelFunc,
		impl:     impl,
	}
}

func (bq *baseQueue) pop() *driverTask {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	item := heap.Pop(bq.pq).(*priority_queue.Item[taskKey])
	key := item.Value()
	task, ok := bq.taskMap[key]
	if !ok {
		alert.UnexpectedEvent("unexpected_task_not_found", "task not found for key %+v", key)
		return nil
	}
	task.processing = true
	return task
}

func (bq *baseQueue) pushLocked(task *driverTask, priority float64) {
	item := priority_queue.NewItem(task.key, priority)
	heap.Push(bq.pq, item)
	task.item = item
	bq.taskMap[task.key] = task
}

func (bq *baseQueue) removeItemWithMinPriority() {
	item := bq.pq.RemoveItemWithMinPriority()
	if item == nil {
		return
	}
	key := item.Value()
	task, ok := bq.taskMap[key]
	if !ok {
		alert.UnexpectedEvent("unexpected_task_not_found", "task not found for key %+v", key)
		return
	}

	if task.processing {
		task.requeue = false
		return
	}
	delete(bq.taskMap, key)
}

func (bq *baseQueue) Len() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.pq.Len()
}

func (bq *baseQueue) postProcess(ctx context.Context, task *driverTask, requeueType RequeueType) {
	ar := attemptRecord{}
	if requeueType == RequeueRetry {
		ar = task.attemptRecord
		ar.attempts++
		ar.nextAttemptTime = bq.nextAttemptTime(ar.attempts)
	} else if requeueType == RequeueWait {
		ar = task.attemptRecord
	}
	bq.mu.Lock()
	delete(bq.taskMap, task.key)
	bq.mu.Unlock()

	if task.key.taskType == RangeTaskType {
		// The lease-blocked hint only bridges compute→execute within one
		// process cycle; drop it now so hints for ranges deleted while blocked
		// don't accumulate. It is re-derived on the range's next evaluation.
		bq.impl.clearLeaseBlockedHint(task.key.rangeID)
	}

	if ar.attempts >= maxRetry {
		if task.key.taskType == RangeTaskType {
			alert.UnexpectedEvent("driver_action_retries_exceeded", "c%dn%d action: %s retries exceeded", task.key.rangeID, task.rangeTask.repl.ReplicaID(), ar.action)
		} else if task.key.taskType == PartitionTaskType {
			alert.UnexpectedEvent("driver_action_retries_exceeded", "partition %s action: %s retries exceeded", task.key.partitionID, ar.action)
		}
		// do not add it to the queue
	} else if requeueType != RequeueNoop || task.requeue {
		if task.key.taskType == RangeTaskType {
			bq.maybeAddRangeTask(ctx, task.rangeTask, ar)
		} else if task.key.taskType == PartitionTaskType {
			bq.maybeAddPartitionTask(ctx, task.partitionTask, ar)
		}
	}
}

func (bq *baseQueue) nextAttemptTime(attemptNumber int) time.Time {
	backoff := float64(1*time.Second) * math.Pow(2, float64(attemptNumber))
	return bq.clock.Now().Add(time.Duration(backoff))
}

func (bq *baseQueue) process(ctx context.Context, task *driverTask) RequeueType {
	action, _ := bq.impl.computeAction(ctx, task)

	if task.key.taskType == RangeTaskType {
		rangeID := task.key.rangeID
		if task.rangeTask == nil {
			bq.log.Errorf("task is nil for range %d", rangeID)
			return RequeueNoop
		}
		replicaID := task.rangeTask.repl.ReplicaID()
		bq.log.Debugf("start to process c%dn%d", rangeID, replicaID)
	} else if task.key.taskType == PartitionTaskType {
		bq.log.Debugf("start to process partition %s", task.key.partitionID)
		if task.partitionTask == nil {
			bq.log.Errorf("task is nil for partition %s", task.key.partitionID)
			return RequeueNoop
		}
	}

	ar := task.attemptRecord
	if action == ar.action && !ar.nextAttemptTime.IsZero() {
		if bq.clock.Now().Before(ar.nextAttemptTime) {
			// Do nothing until nextAttemptTime becomes current
			return RequeueWait
		}
	}

	return bq.impl.processTask(ctx, task, action)
}

// The Queue is responsible for up-replicate, down-replicate and reblance ranges
// across the stores.
type Queue struct {
	*baseQueue

	storeMap storemap.IStoreMap
	store    IStore
	sender   *sender.Sender

	apiClient IClient

	minReplicasPerRange  int
	minMetaRangeReplicas int

	efp interfaces.ExperimentFlagProvider

	// leaseBlockedHint caches, per range, the blocked result of the lease
	// evaluation computeActionForRangeTask just ran, so the execution path
	// (rebalanceReplica) doesn't repeat it (store stats + a connection check
	// per replica). It is a hint: process() recomputes the action — and thus
	// the hint — immediately before executing it. Guarded by its own mutex
	// because computeActionForRangeTask runs both under bq.mu (from
	// maybeAddTask) and without it (from process()) on the same range.
	// Entries are cleared in postProcess so deleted ranges don't accumulate.
	leaseBlockedMu   sync.Mutex
	leaseBlockedHint map[uint64]bool

	// Anti-thrash state for load-based rebalancing.
	loadMu sync.Mutex
	// lastRebalanceExpiry records, per range, when the cooldown from its
	// last successfully applied rebalance op ends. Expired entries are
	// purged on read and on record, so deleted ranges don't accumulate.
	lastRebalanceExpiry map[uint64]time.Time
	// pendingMoves are short-lived local adjustments to the gossiped store
	// QPS for moves this store just initiated, so that several hot ranges
	// evaluated within one gossip interval don't all pick the same cold
	// target.
	pendingMoves []pendingMoveAdjustment
}

func (rq *Queue) setLeaseBlockedHint(rangeID uint64, blocked bool) {
	rq.leaseBlockedMu.Lock()
	defer rq.leaseBlockedMu.Unlock()
	if rq.leaseBlockedHint == nil {
		rq.leaseBlockedHint = make(map[uint64]bool)
	}
	if blocked {
		rq.leaseBlockedHint[rangeID] = true
	} else {
		delete(rq.leaseBlockedHint, rangeID)
	}
}

func (rq *Queue) leaseBlocked(rangeID uint64) bool {
	rq.leaseBlockedMu.Lock()
	defer rq.leaseBlockedMu.Unlock()
	return rq.leaseBlockedHint[rangeID]
}

// clearLeaseBlockedHint drops a range's cached hint. Called from postProcess
// when a range task finishes so entries for ranges that were blocked and then
// deleted (merge/split/teardown) don't accumulate. The hint is re-derived on
// the range's next evaluation if it is still blocked.
func (rq *Queue) clearLeaseBlockedHint(rangeID uint64) {
	rq.leaseBlockedMu.Lock()
	defer rq.leaseBlockedMu.Unlock()
	delete(rq.leaseBlockedHint, rangeID)
}

// pendingMoveAdjustment is a signed, expiring correction to one store's
// gossiped QPS: positive on the target of a just-applied move, negative on
// the source.
type pendingMoveAdjustment struct {
	nhid       string
	readQPS    float64
	proposeQPS float64
	expiresAt  time.Time
}

// pendingMoveTTL is how long a local move adjustment shadows the gossiped
// QPS. It must outlive gossip lag: with EWMA smoothing, the moved load only
// shows up in other stores' gossiped view after a few time constants — a
// TTL of one gossip interval would expire while the target still looks far
// colder than it is, re-enabling the pile-on this ledger exists to prevent.
func pendingMoveTTL() time.Duration {
	if tau := storemap.QPSEWMATimeConstant(); tau > 0 {
		return 2 * tau
	}
	// No smoothing: a couple of gossip refreshes suffice.
	return 30 * time.Second
}

// rebalanceRecord describes an applied rebalance op for bookkeeping.
type rebalanceRecord struct {
	rangeID    uint64
	fromNHID   string
	toNHID     string
	readQPS    int64
	proposeQPS int64
	// skipCooldown is set for lease-unblock replica moves: their whole
	// purpose is the follow-up lease transfer on the next pass, which the
	// per-range cooldown would otherwise block.
	skipCooldown bool
}

// recordRebalance is called after a rebalance op was successfully applied:
// it starts the per-range cooldown and credits/debits the affected stores'
// QPS in the pending-move ledger. Only successful ops are recorded so a
// failed transfer can't lock a still-hot range out for a whole cooldown or
// leave phantom load in the ledger.
func (rq *Queue) recordRebalance(ctx context.Context, rec *rebalanceRecord) {
	if rq.baseQueue == nil || rq.clock == nil {
		return
	}
	cooldown := rq.loadParams(ctx).cooldown
	now := rq.clock.Now()
	rq.loadMu.Lock()
	defer rq.loadMu.Unlock()
	if !rec.skipCooldown && cooldown > 0 {
		if rq.lastRebalanceExpiry == nil {
			rq.lastRebalanceExpiry = make(map[uint64]time.Time)
		}
		for rangeID, expiry := range rq.lastRebalanceExpiry {
			if now.After(expiry) {
				delete(rq.lastRebalanceExpiry, rangeID)
			}
		}
		rq.lastRebalanceExpiry[rec.rangeID] = now.Add(cooldown)
	}
	expiresAt := now.Add(pendingMoveTTL())
	rq.pendingMoves = append(rq.pendingMoves,
		pendingMoveAdjustment{nhid: rec.toNHID, readQPS: float64(rec.readQPS), proposeQPS: float64(rec.proposeQPS), expiresAt: expiresAt},
		pendingMoveAdjustment{nhid: rec.fromNHID, readQPS: -float64(rec.readQPS), proposeQPS: -float64(rec.proposeQPS), expiresAt: expiresAt},
	)
}

// inCooldown reports whether the range's cooldown from its last applied
// rebalance op is still running.
func (rq *Queue) inCooldown(rangeID uint64) bool {
	if rq.baseQueue == nil || rq.clock == nil {
		return false
	}
	now := rq.clock.Now()
	rq.loadMu.Lock()
	defer rq.loadMu.Unlock()
	expiry, ok := rq.lastRebalanceExpiry[rangeID]
	if !ok {
		return false
	}
	if now.After(expiry) {
		delete(rq.lastRebalanceExpiry, rangeID)
		return false
	}
	return true
}

// pendingAdjustment sums the unexpired ledger entries for a store.
func (rq *Queue) pendingAdjustment(nhid string) (readQPS, proposeQPS float64) {
	if rq.baseQueue == nil || rq.clock == nil {
		return 0, 0
	}
	now := rq.clock.Now()
	rq.loadMu.Lock()
	defer rq.loadMu.Unlock()
	unexpired := rq.pendingMoves[:0]
	for _, adj := range rq.pendingMoves {
		if now.After(adj.expiresAt) {
			continue
		}
		unexpired = append(unexpired, adj)
		if adj.nhid == nhid {
			readQPS += adj.readQPS
			proposeQPS += adj.proposeQPS
		}
	}
	rq.pendingMoves = unexpired
	return readQPS, proposeQPS
}

// populateCandidateQPS fills the candidate's QPS fields from its (smoothed)
// store usage, corrected by the pending-move ledger.
func (rq *Queue) populateCandidateQPS(c *candidate, p loadParams, readQPSMean, proposeQPSMean float64) {
	adjRead, adjPropose := rq.pendingAdjustment(c.nhid)
	c.readQPS = max(c.usage.GetReadQps()+int64(adjRead), 0)
	c.readQPSMeanLevel = qpsMeanLevel(p, readQPSMean, c.readQPS)
	c.proposeQPS = max(c.usage.GetRaftProposeQps()+int64(adjPropose), 0)
	c.proposeQPSMeanLevel = qpsMeanLevel(p, proposeQPSMean, c.proposeQPS)
}

func NewQueue(store IStore, sender *sender.Sender, gossipManager interfaces.GossipService, nhlog log.Logger, apiClient IClient, clock clockwork.Clock, efp interfaces.ExperimentFlagProvider) *Queue {
	storeMap := storemap.New(gossipManager, clock, nhlog, *minReplicasPerRange, *minMetaRangeReplicas, *missingLeaseCountThreshold)
	q := &Queue{
		storeMap:             storeMap,
		store:                store,
		apiClient:            apiClient,
		sender:               sender,
		minReplicasPerRange:  *minReplicasPerRange,
		minMetaRangeReplicas: *minMetaRangeReplicas,
		efp:                  efp,
	}
	q.baseQueue = newBaseQueue(nhlog, clock, q)
	return q
}

func (rq *Queue) getReplica(rangeID uint64) (IReplica, error) {
	return rq.store.GetReplica(rangeID)
}

const driverEnabledFlag = "cache.raft.enable_driver"
const splitEnabledFlag = "cache.raft.enable_split"

// Experiment-flag names for load-based rebalancing. Decision-time parameters
// are read through the experiment flag provider on every evaluation (with the
// command-line flag as the default) so they can be tuned live during a load
// test without a redeploy.
const (
	loadBasedRebalanceEnabledFlag = "cache.raft.enable_load_based_rebalance"
	qpsBandRatioFlag              = "cache.raft.rebalance_qps_band_ratio"
	qpsFloorFlag                  = "cache.raft.rebalance_qps_floor"
	minLeaseLoadFractionFlag      = "cache.raft.min_lease_load_fraction"
	minReplicaLoadFractionFlag    = "cache.raft.min_replica_load_fraction"
	rangeRebalanceCooldownSecFlag = "cache.raft.range_rebalance_cooldown_seconds"
)

// loadParams holds the decision-time tunables for load-based rebalancing.
type loadParams struct {
	bandRatio              float64
	qpsFloor               float64
	minLeaseLoadFraction   float64
	minReplicaLoadFraction float64
	cooldown               time.Duration
}

func (rq *Queue) loadParams(ctx context.Context) loadParams {
	p := loadParams{
		bandRatio:              *rebalanceQPSBandRatio,
		qpsFloor:               *rebalanceQPSFloor,
		minLeaseLoadFraction:   *minLeaseLoadFraction,
		minReplicaLoadFraction: *minReplicaLoadFraction,
		cooldown:               *rangeRebalanceCooldown,
	}
	if rq.efp == nil {
		return p
	}
	p.bandRatio = rq.efp.Float64(ctx, qpsBandRatioFlag, p.bandRatio)
	p.qpsFloor = rq.efp.Float64(ctx, qpsFloorFlag, p.qpsFloor)
	p.minLeaseLoadFraction = rq.efp.Float64(ctx, minLeaseLoadFractionFlag, p.minLeaseLoadFraction)
	p.minReplicaLoadFraction = rq.efp.Float64(ctx, minReplicaLoadFractionFlag, p.minReplicaLoadFraction)
	p.cooldown = time.Duration(rq.efp.Int64(ctx, rangeRebalanceCooldownSecFlag, int64(p.cooldown.Seconds()))) * time.Second
	return p
}

// isLoadBasedRebalanceEnabled checks if load-based rebalancing is enabled.
// The command-line flag (declared in storemap, shared with the store's
// periodic usage gossip and the storemap's QPS smoothing) must be on; the
// experiment flag then acts as a live kill-switch. The experiment flag can't
// enable the feature by itself because the supporting signal plumbing is
// only active with the command-line flag on.
func (rq *Queue) isLoadBasedRebalanceEnabled(ctx context.Context) bool {
	if !*storemap.LoadBasedRebalance {
		return false
	}
	if rq.efp == nil {
		return true
	}
	return rq.efp.Boolean(ctx, loadBasedRebalanceEnabledFlag, true)
}

// isDriverEnabled checks if the driver is enabled via the experiment flag.
// By default, the driver is enabled (returns true).
func (rq *Queue) isDriverEnabled(ctx context.Context) bool {
	if rq.efp == nil {
		return true
	}
	return rq.efp.Boolean(ctx, driverEnabledFlag, true)
}

// isSplitEnabled checks if split is enabled via the experiment flag.
// By default, split is enabled (returns true).
func (rq *Queue) isSplitEnabled(ctx context.Context) bool {
	if rq.efp == nil {
		return true
	}
	return rq.efp.Boolean(ctx, splitEnabledFlag, true)
}

// computeActionForRangeTask computes the drive action needed for range task and its priority.
func (rq *Queue) computeActionForRangeTask(ctx context.Context, task *rangeTask) (DriverAction, float64) {
	// Handle range tasks
	repl := task.repl
	rangeID := repl.RangeID()
	rd := rq.store.GetRange(rangeID)
	action := DriverNoop
	if rd == nil || !rq.store.HaveLease(ctx, rd.GetRangeId()) {
		return action, action.Priority()
	}

	if rd.GetDeleted() {
		// If the range descriptor is marked as deleted, we don't want to do
		// any up/down-replicate and reblance actions, except for finish the
		// cleanup.
		return action, action.Priority()
	}

	needsRemoveData := false
	if len(rd.GetRemoved()) > 0 {
		// There is no point to call to finish replica removal if the node is
		// dead.
		byStatus := rq.storeMap.DivideByStatus(rd.GetRemoved())
		if len(byStatus.LiveReplicas)+len(byStatus.SuspectReplicas) > 0 {
			needsRemoveData = true
			action = DriverFinishReplicaRemoval
		}
	}

	if rq.storeMap == nil {
		return action, action.Priority()
	}

	replicas := rd.GetReplicas()
	curReplicas := len(replicas)
	if curReplicas == 0 {
		return action, action.Priority()
	}
	minReplicas := rq.minReplicasPerRange
	if rangeID == constants.MetaRangeID {
		minReplicas = rq.minMetaRangeReplicas
	}

	desiredQuorum := computeQuorum(minReplicas)
	quorum := computeQuorum(curReplicas)

	if curReplicas < minReplicas || len(rd.GetStaging()) > 0 {
		action = DriverAddReplica
		adjustedPriority := action.Priority() + float64(desiredQuorum-curReplicas)
		change := rq.addReplica(rd)
		if change == nil {
			// not able to find target node for allocation; if there is
			// in-progress replica removal, complete it so this can be a target
			// for allocation
			if needsRemoveData {
				return DriverFinishReplicaRemoval, adjustedPriority
			}
		}
		return action, adjustedPriority
	}
	replicasByStatus := rq.storeMap.DivideByStatus(replicas)
	numLiveReplicas := len(replicasByStatus.LiveReplicas) + len(replicasByStatus.SuspectReplicas)
	numDeadReplicas := len(replicasByStatus.DeadReplicas)

	if numLiveReplicas < quorum {
		// We don't have enough live nodes to do any cluster membership change;
		// However, RemoveData doesn't require cluster membership change
		if needsRemoveData {
			return DriverFinishReplicaRemoval, action.Priority()
		}
		log.Debugf("noop because num live replicas of range %d = %d less than quorum =%d", rd.GetRangeId(), numLiveReplicas, quorum)
		action = DriverNoop
		return action, action.Priority()
	}

	if curReplicas <= minReplicas && numDeadReplicas > 0 {
		action = DriverReplaceDeadReplica
		// not able to find target node for allocation; if there is
		// in-progress replica removal, complete it so this can be a target
		// for allocation
		if needsRemoveData {
			return DriverFinishReplicaRemoval, action.Priority()
		}
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

	if needsRemoveData {
		action = DriverFinishReplicaRemoval
		return action, action.Priority()
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

	// Do not split when there is a store that's unavailable and a replica is
	// in the middle of a removal.
	isClusterHealthy := rq.storeMap.AllStoresAvailableAndReady()
	if isClusterHealthy && rq.isSplitEnabled(ctx) {
		if targetRangeSizeBytes := config.TargetRangeSizeBytes(); targetRangeSizeBytes > 0 {
			usage, err := repl.Usage()
			if err != nil {
				rq.log.Errorf("failed to get Usage of replica c%dn%d", repl.RangeID(), repl.ReplicaID())
			} else {
				jitterFactor := config.RangeSizeJitterFactor()
				lowerBound := (1 - jitterFactor) * float64(targetRangeSizeBytes)
				jitter := (rand.Float64()*2 - 1) * jitterFactor * float64(targetRangeSizeBytes)
				threshold := float64(targetRangeSizeBytes) + jitter
				if sizeUsed := usage.GetEstimatedDiskBytesUsed(); float64(sizeUsed) >= threshold {
					action = DriverSplitRange
					adjustedPriority := action.Priority() + (float64(sizeUsed)-lowerBound)/float64(sizeUsed)*100.0
					return action, adjustedPriority
				}
			}
		}
	}

	loadBased := rq.isLoadBasedRebalanceEnabled(ctx)
	// Compute the load context once per pass. loadParams and the all-stores
	// snapshot (a lock + clone of every store's usage) are reused by the lease
	// and replica evaluations below instead of each recomputing them.
	var p loadParams
	var allStoresStats *storemap.StoresWithStats
	if loadBased {
		p = rq.loadParams(ctx)
		allStoresStats = rq.storeMap.GetStoresWithStats()
	}

	leaseBlocked := false
	if loadBased {
		// In load-based mode, try lease transfers before replica moves: a
		// leadership transfer is nearly free while a replica move costs a
		// snapshot, and shedding read load often removes the need to move
		// data at all. Lease rebalancing runs regardless of cluster health —
		// a lease transfer is a leadership change among reachable replica
		// stores and never involves the unavailable store — so it is outside
		// the health gate below.
		res := rq.findRebalanceLeaseOpLoaded(ctx, rd, repl, loadBased, p, allStoresStats)
		// Cache the hint for the execution path (rebalanceReplica) and keep a
		// local copy for the compute-phase replica evaluation below.
		rq.setLeaseBlockedHint(rd.GetRangeId(), res.blocked)
		leaseBlocked = res.blocked
		if res.op != nil {
			action = DriverRebalanceLease
			return action, action.Priority()
		}
	}

	// Do not move replicas when a store is unavailable: unlike a lease
	// transfer, a replica move copies data and can further destabilize an
	// already-degraded cluster.
	if isClusterHealthy {
		// For DriverConsiderRebalance check if there are rebalance opportunities.
		storesWithStats := allStoresStats
		if storesWithStats == nil {
			storesWithStats = rq.storeMap.GetStoresWithStats()
		}
		op := rq.findRebalanceReplicaOpLoaded(ctx, rd, storesWithStats, repl, leaseBlocked, loadBased, p)
		if op != nil {
			log.Debugf("find rebalancing opportunities: from (nhid=%q, replicaCount=%d, isReady=%t) to (nhid=%q, replicaCount=%d, isReady=%t)", op.from.nhid, op.from.replicaCount, op.from.usage.GetIsReady(), op.to.nhid, op.to.replicaCount, op.to.usage.GetIsReady())
			action = DriverRebalanceReplica
			return action, action.Priority()
		}
	}

	if !loadBased {
		// Count mode balances lease counts regardless of health too (matching
		// behavior before load-based rebalancing), but ranks it below replica
		// moves.
		if rq.findRebalanceLeaseOpLoaded(ctx, rd, repl, loadBased, p, allStoresStats).op != nil {
			action = DriverRebalanceLease
			return action, action.Priority()
		}
	}

	action = DriverNoop
	return action, action.Priority()
}

// computeActionForPartitionTask computes the action needed for a partition task and its priority.
func (rq *Queue) computeActionForPartitionTask(ctx context.Context, task *partitionTask) (DriverAction, float64) {
	if task.pd == nil || task.pd.GetState() == rfpb.PartitionDescriptor_INITIALIZING {
		a := DriverInitializePartition
		rq.log.Infof("action: %s for partition: %q", a, task.config.ID)
		return a, a.Priority()
	}
	return DriverNoop, DriverNoop.Priority()
}

// computeAction computes the action needed and its priority.
func (rq *Queue) computeAction(ctx context.Context, task *driverTask) (DriverAction, float64) {
	switch task.key.taskType {
	case RangeTaskType:
		return rq.computeActionForRangeTask(ctx, task.rangeTask)
	case PartitionTaskType:
		return rq.computeActionForPartitionTask(ctx, task.partitionTask)
	}
	return DriverNoop, DriverNoop.Priority()

}

func (bq *baseQueue) maybeAddTask(ctx context.Context, newTask *driverTask, ar attemptRecord) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	task, ok := bq.taskMap[newTask.key]
	if !ok {
		task = newTask
	}

	action, priority := bq.impl.computeAction(ctx, task)
	if action == DriverNoop {
		return
	}

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

func (bq *baseQueue) maybeAddRangeTask(ctx context.Context, rt *rangeTask, ar attemptRecord) {
	if rt.repl == nil {
		return
	}
	rangeID := rt.repl.RangeID()
	key := taskKey{taskType: RangeTaskType, rangeID: rangeID}
	newTask := &driverTask{
		key:       key,
		rangeTask: rt,
	}
	bq.maybeAddTask(ctx, newTask, ar)
}

func (bq *baseQueue) maybeAddPartitionTask(ctx context.Context, pt *partitionTask, ar attemptRecord) {
	key := taskKey{taskType: PartitionTaskType, partitionID: pt.config.ID}
	newTask := &driverTask{
		key:           key,
		partitionTask: pt,
	}
	bq.maybeAddTask(ctx, newTask, ar)
}

func (rq *Queue) MaybeAddPartitionTask(ctx context.Context, p disk.Partition, pd *rfpb.PartitionDescriptor) {
	if !rq.isDriverEnabled(ctx) {
		return
	}
	rq.log.Infof("maybe add partition task for %q", p.ID)
	rq.maybeAddPartitionTask(ctx, &partitionTask{config: p, pd: pd}, attemptRecord{})
}

func (rq *Queue) MaybeAddRangeTask(ctx context.Context, replica IReplica) {
	if !rq.isDriverEnabled(ctx) {
		return
	}
	rq.maybeAddRangeTask(ctx, &rangeTask{repl: replica}, attemptRecord{})
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
	task := bq.pop()
	if task == nil {
		return
	}
	requeueType := bq.process(bq.egCtx, task)
	bq.postProcess(bq.egCtx, task, requeueType)
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

func (rq *Queue) findNodesForAllocation(storesWithStats *storemap.StoresWithStats) []*rfpb.NodeDescriptor {
	var candidates []*candidate
	for _, su := range storesWithStats.Usages {
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
			// No rendezvousScore because we don't know the range IDs yet.
		})
	}

	quorum := computeQuorum(rq.minReplicasPerRange)
	if len(candidates) < quorum {
		log.Warning("we don't have enough nodes to bring up a new raft cluster")
		// We don't have enough nodes to bring up a new raft cluster.
		return nil
	}
	expectedReplicas := min(rq.minReplicasPerRange, len(candidates))
	sortFunc := func(a, b *candidate) int {
		// Best targets are up front.
		return -compareByScoreAndID(a, b)
	}
	slices.SortFunc(candidates, sortFunc)

	// Split stores by zone
	storesPerZone := make(map[string][]*candidate)
	for _, c := range candidates {
		zone := c.usage.GetNode().GetZone()
		storesPerZone[zone] = append(storesPerZone[zone], c)
	}
	res := make([]*rfpb.NodeDescriptor, 0, rq.minReplicasPerRange)
	for {
		// Take one store per zone that stil has stores. This is guaranteed to
		// finish because quorum <= expectedReplicas <= len(candidates).
		round := make([]*candidate, 0, len(storesPerZone))
		for zone, stores := range storesPerZone {
			round = append(round, stores[0])
			stores = stores[1:]
			if len(stores) == 0 {
				delete(storesPerZone, zone)
			} else {
				storesPerZone[zone] = stores
			}
		}
		if len(round) == 0 {
			alert.UnexpectedEvent("findNodesForAllocation_unexpected_return", "%d candidates, only got %d replicas. expectedReplicas=%d", len(candidates), len(res), expectedReplicas)
			return nil
		}
		// Pick the best stores in this round first.
		slices.SortFunc(round, sortFunc)
		for _, c := range round {
			res = append(res, c.usage.GetNode())
			if len(res) >= expectedReplicas {
				return res
			}
		}
	}
}

// findNodeForAllocation finds a target node for the range to up-replicate.
func (rq *Queue) findNodeForAllocation(rd *rfpb.RangeDescriptor, storesWithStats *storemap.StoresWithStats) *rfpb.NodeDescriptor {
	var candidates []*candidate
	existing := append(rd.GetReplicas(), rd.GetRemoved()...)
	rangeID := rd.GetRangeId()
	for _, su := range storesWithStats.Usages {
		if storeHasReplica(su.GetNode(), rd.GetStaging()) {
			// There is a staging replica, complete it.
			return su.GetNode()
		}
		if storeHasReplica(su.GetNode(), existing) {
			rq.log.Debugf("skip node %+v because the replica is already on the node", su.GetNode())
			continue
		}
		if isDiskFull(su) {
			rq.log.Debugf("skip node %+v because the disk is full", su)
			continue
		}
		rq.log.Debugf("add node %+v to candidate list", su.GetNode())
		nhid := su.GetNode().GetNhid()
		candidates = append(candidates, &candidate{
			nhid:                  nhid,
			usage:                 su,
			replicaCount:          su.GetReplicaCount(),
			replicaCountMeanLevel: replicaCountMeanLevel(storesWithStats, su),
			rendezvousScore:       rendezvousScore(rangeID, nhid),
		})
	}
	if len(candidates) == 0 {
		return nil
	}

	usagesByNhid := make(map[string]*rfpb.StoreUsage, len(storesWithStats.Usages))
	for _, su := range storesWithStats.Usages {
		usagesByNhid[su.GetNode().GetNhid()] = su
	}
	replicasByZone := make(map[string]int)
	for _, repl := range rd.GetReplicas() {
		if su, ok := usagesByNhid[repl.GetNhid()]; ok {
			replicasByZone[su.GetNode().GetZone()]++
		}
	}
	return slices.MaxFunc(candidates, compareByZoneAndScore(replicasByZone)).usage.GetNode()
}

type removeDataOp struct {
	replDesc *rfpb.ReplicaDescriptor
	rd       *rfpb.RangeDescriptor
}

type change struct {
	addOp                *rfpb.AddReplicaRequest
	removeOp             *rfpb.RemoveReplicaRequest
	removeDataOp         *removeDataOp
	splitOp              *rfpb.SplitRangeRequest
	transferLeadershipOp *rfpb.TransferLeadershipRequest
	// rebalanceRecord, when set, is applied via recordRebalance after the
	// change succeeds (cooldown + pending-move ledger).
	rebalanceRecord *rebalanceRecord
	// leaseUnblock marks a replica move whose purpose is to create a lease
	// landing spot; processRangeTask requeues after it so the follow-up lease
	// transfer happens on the next pass rather than waiting for a scan. Set
	// independently of rebalanceRecord so the requeue survives a Usage() error.
	leaseUnblock bool
}

func (rq *Queue) finishReplicaRemoval(rd *rfpb.RangeDescriptor) *change {
	if len(rd.GetRemoved()) == 0 {
		return nil
	}
	return &change{
		removeDataOp: &removeDataOp{
			replDesc: rd.GetRemoved()[0],
			rd:       rd,
		},
	}
}

func (rq *Queue) splitRange(rd *rfpb.RangeDescriptor) *change {
	return &change{
		splitOp: &rfpb.SplitRangeRequest{
			Header: header.New(rd, rd.GetReplicas()[0], rfpb.Header_LINEARIZABLE),
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

func (rq *Queue) initializePartition(ctx context.Context, p disk.Partition) error {
	rq.log.Infof("initialize partitions: %q", p.ID)
	storesWithStats := rq.storeMap.GetStoresWithStats()
	nodes := rq.findNodesForAllocation(storesWithStats)
	if nodes == nil {
		return status.InternalErrorf("cannot find nodes to initialize partition %q", p.ID)
	}
	nodeGrpcAddrs := make(map[string]string, len(nodes))
	for _, n := range nodes {
		nodeGrpcAddrs[n.GetNhid()] = n.GetGrpcAddress()
	}
	return rq.store.InitializeShardsForPartition(ctx, nodeGrpcAddrs, p)
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
	// leaseUnblock marks a replica move whose purpose is to create a lease
	// landing spot for a read-hot range (see findLeaseUnblockReplicaOp).
	leaseUnblock bool
}

func (rOp *rebalanceOp) String() string {
	return "from: " + rOp.from.nhid + " to: " + rOp.to.nhid
}

func canConvergeByRebalanceLease(choice *rebalanceChoice, mean float64) bool {
	if len(choice.candidates) == 0 {
		return false
	}
	overfullThreshold := int64(math.Ceil(aboveMeanLeaseCountThreshold(mean)))
	// The existing store is too far above the mean.
	if choice.existing.usage.LeaseCount > overfullThreshold {
		// There is a candidate store that's below mean
		for _, c := range choice.candidates {
			if float64(c.usage.LeaseCount) < mean {
				return true
			}
		}
		// No candidate store is below mean, don't transfer.
		return false
	}

	// The existing store is above the mean, but not too far; but there is at least one other store that is too far below the mean.
	if float64(choice.existing.usage.LeaseCount) > mean {
		underfullThreshold := int64(math.Floor(belowMeanLeaseCountThreshold(mean)))
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

func (rq *Queue) rebalanceReplica(ctx context.Context, rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	storesWithStats := rq.storeMap.GetStoresWithStats()
	// The blocked hint was cached by the lease evaluation that
	// computeActionForRangeTask just ran; don't repeat that evaluation here.
	leaseBlocked := rq.isLoadBasedRebalanceEnabled(ctx) && rq.leaseBlocked(rd.GetRangeId())
	op := rq.findRebalanceReplicaOp(ctx, rd, storesWithStats, localRepl, leaseBlocked)
	if op == nil {
		return nil
	}
	if rq.isLoadBasedRebalanceEnabled(ctx) {
		rq.log.Infof("rebalance replica op for range %d: %s -> %s (propose qps %d -> %d, read qps %d -> %d, lease blocked=%t)",
			rd.GetRangeId(), op.from.nhid, op.to.nhid, op.from.proposeQPS, op.to.proposeQPS, op.from.readQPS, op.to.readQPS, leaseBlocked)
	} else {
		rq.log.Debugf("found rebalance replica op for range %d: %s -> %s", rd.GetRangeId(), op.from.nhid, op.to.nhid)
	}

	replicaID, err := findReplicaWithNHID(rd, op.from.usage.GetNode().GetNhid())
	if err != nil {
		rq.log.Errorf("failed to rebalance replica: %s", err)
		return nil
	}

	c := &change{
		addOp: &rfpb.AddReplicaRequest{
			Range: rd,
			Node:  op.to.usage.GetNode(),
		},
		removeOp: &rfpb.RemoveReplicaRequest{
			Range:     rd,
			ReplicaId: replicaID,
		},
		leaseUnblock: op.leaseUnblock,
	}
	if rq.isLoadBasedRebalanceEnabled(ctx) {
		// Attach a record that processRangeTask applies after the change
		// succeeds (like CRDB's local store-load updates), so other ranges
		// evaluated before gossip catches up see the effect of this move.
		if ru, usageErr := localRepl.Usage(); usageErr == nil {
			c.rebalanceRecord = &rebalanceRecord{
				rangeID:    rd.GetRangeId(),
				fromNHID:   op.from.nhid,
				toNHID:     op.to.nhid,
				proposeQPS: ru.GetRaftProposeQps(),
				// A lease-unblock move exists to enable the follow-up lease
				// transfer on the next pass; the cooldown must not block it.
				skipCooldown: op.leaseUnblock,
			}
		}
	}
	return c
}

func (rq *Queue) rebalanceLease(ctx context.Context, rd *rfpb.RangeDescriptor, localRepl IReplica) *change {
	op := rq.findRebalanceLeaseOp(ctx, rd, localRepl).op
	if op == nil {
		return nil
	}
	if rq.isLoadBasedRebalanceEnabled(ctx) {
		rq.log.Infof("rebalance lease op for range %d: %s -> %s (read qps %d -> %d)",
			rd.GetRangeId(), op.from.nhid, op.to.nhid, op.from.readQPS, op.to.readQPS)
	} else {
		rq.log.Debugf("found rebalance lease op for range %d: %s -> %s", rd.GetRangeId(), op.from.nhid, op.to.nhid)
	}
	replicaID, err := findReplicaWithNHID(rd, op.to.usage.GetNode().GetNhid())
	if err != nil {
		rq.log.Errorf("failed to rebalance lease: %s", err)
		return nil
	}
	c := &change{
		transferLeadershipOp: &rfpb.TransferLeadershipRequest{
			RangeId:         rd.GetRangeId(),
			TargetReplicaId: replicaID,
		},
	}
	if rq.isLoadBasedRebalanceEnabled(ctx) {
		// A lease transfer moves the range's read load; the record is
		// applied by processRangeTask after the transfer succeeds so
		// decisions made before gossip catches up account for it.
		if ru, usageErr := localRepl.Usage(); usageErr == nil {
			c.rebalanceRecord = &rebalanceRecord{
				rangeID:  rd.GetRangeId(),
				fromNHID: op.from.nhid,
				toNHID:   op.to.nhid,
				readQPS:  ru.GetReadQps(),
			}
		}
	}
	return c
}

// leaseRebalanceResult is the outcome of looking for a lease-rebalance op.
type leaseRebalanceResult struct {
	op *rebalanceOp
	// blocked is true when the local store should shed read load for this
	// range, but no store holding one of the range's replicas is a viable
	// lease target. The replica controller uses this to move a follower to a
	// read-underfull store, creating a lease target for a later pass.
	blocked bool
}

// findRebalanceLeaseOp computes the load context and delegates to
// findRebalanceLeaseOpLoaded. computeActionForRangeTask calls the Loaded form
// directly with a context it shares with the replica evaluation.
func (rq *Queue) findRebalanceLeaseOp(ctx context.Context, rd *rfpb.RangeDescriptor, localRepl IReplica) leaseRebalanceResult {
	loadBased := rq.isLoadBasedRebalanceEnabled(ctx)
	var p loadParams
	var allStoresStats *storemap.StoresWithStats
	if loadBased {
		p = rq.loadParams(ctx)
		allStoresStats = rq.storeMap.GetStoresWithStats()
	}
	return rq.findRebalanceLeaseOpLoaded(ctx, rd, localRepl, loadBased, p, allStoresStats)
}

func (rq *Queue) findRebalanceLeaseOpLoaded(ctx context.Context, rd *rfpb.RangeDescriptor, localRepl IReplica, loadBased bool, p loadParams, allStoresStats *storemap.StoresWithStats) leaseRebalanceResult {
	globalMean, shouldRebalance := rq.storeMap.CheckLeaseRebalancePrecondition()
	if !shouldRebalance {
		return leaseRebalanceResult{}
	}
	localReplicaID := localRepl.ReplicaID()
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
		return leaseRebalanceResult{}
	}

	candidates := make([]*candidate, 0, len(rd.GetReplicas())-1)
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
		candidates = append(candidates, &candidate{
			nhid:  repl.GetNhid(),
			usage: store.usage,
		})
	}

	if loadBased {
		if qpsSignalPresent(p, allStoresStats.ReadQPS.Mean) {
			res := rq.findRebalanceLeaseOpQPS(rd, localRepl, existing, candidates, allStoresStats, p)
			if res.op != nil || res.blocked {
				return res
			}
			// No load-based lease op for this range; fall through to the
			// count-based check (with hot-target vetoes below) so lease
			// counts still converge for ranges the load controller leaves
			// alone (e.g. the many ranges under the worth-it floor).
		}
	}

	// A lease transfer isn't free: reads are served only by the leaseholder, so
	// every transfer costs a read blip (retries during the leadership handoff)
	// plus a cache-warmup tail. There is no urgent reason to move a lease, so —
	// unlike count replica moves, which stay reachable for disk-full/zone —
	// the count lease path fully respects the cooldown. With the load lease
	// path also bailing on cooldown, a range in cooldown takes no lease move at
	// all, so it can't take a second read blip right after a load-driven one.
	if loadBased && rq.inCooldown(rd.GetRangeId()) {
		return leaseRebalanceResult{}
	}

	// Count mode: balance lease counts across stores.
	existing.leaseCount = existing.usage.LeaseCount
	existing.leaseCountMeanLevel = leaseCountMeanLevel(globalMean, existing.usage)
	for _, c := range candidates {
		c.leaseCount = c.usage.LeaseCount
		c.leaseCountMeanLevel = leaseCountMeanLevel(globalMean, c.usage)
	}
	choice := &rebalanceChoice{
		existing:   existing,
		candidates: candidates,
	}
	if !canConvergeByRebalanceLease(choice, globalMean) {
		return leaseRebalanceResult{}
	}

	// The read load this range would carry onto a target: a lease transfer
	// moves the range's reads to the new leaseholder. On a Usage error it
	// stays 0, so the post-move check degrades to the old pre-move behavior.
	var rangeReadQPS int64
	if ru, err := localRepl.Usage(); err == nil {
		rangeReadQPS = ru.GetReadQps()
	}

	// Consider candidates best-first. In load-based mode the QPS veto may skip
	// the count-best candidate; fall through to the next-best rather than
	// abandoning the move, so a single hot store can't stall lease-count
	// convergence.
	ranked := slices.Clone(choice.candidates)
	slices.SortFunc(ranked, compareByScoreAndID)
	slices.Reverse(ranked) // best first
	for _, best := range ranked {
		if compareByScore(best, existing) < 0 {
			// This candidate does not improve on the source's lease count, and
			// ranked is score-descending, so neither will any lower-ranked one.
			break
		}
		if loadBased {
			// Count-based lease moves must not undo load balance: skip a
			// store that's hot on either QPS dimension and try the next-best.
			// Use effective (ledger-adjusted) QPS so a store with a large
			// pending inbound move isn't treated as cold and piled onto. The
			// read check is post-move (best.readQPS + the reads this lease
			// carries); the propose/write-hot check stays pre-move (a lease
			// move adds no apply load — it just avoids piling reads onto an
			// already write-hot store's CPU).
			rq.populateCandidateQPS(best, p, allStoresStats.ReadQPS.Mean, allStoresStats.RaftProposeQPS.Mean)
			if qpsSignalPresent(p, allStoresStats.ReadQPS.Mean) &&
				float64(best.readQPS)+float64(rangeReadQPS) > aboveMeanQPSThreshold(p, allStoresStats.ReadQPS.Mean) {
				continue
			}
			if qpsSignalPresent(p, allStoresStats.RaftProposeQPS.Mean) &&
				float64(best.proposeQPS) > aboveMeanQPSThreshold(p, allStoresStats.RaftProposeQPS.Mean) {
				continue
			}
		}
		return leaseRebalanceResult{op: &rebalanceOp{
			from: existing,
			to:   best,
		}}
	}
	return leaseRebalanceResult{}
}

// findRebalanceLeaseOpQPS looks for a lease transfer that improves the
// balance of read QPS across stores. A lease transfer moves the range's read
// load (reads are served by the leaseholder) but not its apply cost, so read
// QPS is the only dimension it balances; the write dimension appears solely
// as a veto on targets.
func (rq *Queue) findRebalanceLeaseOpQPS(rd *rfpb.RangeDescriptor, localRepl IReplica, existing *candidate, candidates []*candidate, allStores *storemap.StoresWithStats, p loadParams) leaseRebalanceResult {
	ru, err := localRepl.Usage()
	if err != nil {
		rq.log.Errorf("failed to get usage of replica c%dn%d: %s", rd.GetRangeId(), localRepl.ReplicaID(), err)
		return leaseRebalanceResult{}
	}
	if rq.inCooldown(rd.GetRangeId()) {
		return leaseRebalanceResult{}
	}
	rangeReadQPS := ru.GetReadQps()
	readQPSMean := allStores.ReadQPS.Mean
	proposeQPSMean := allStores.RaftProposeQPS.Mean
	rq.populateCandidateQPS(existing, p, readQPSMean, proposeQPSMean)
	for _, c := range candidates {
		rq.populateCandidateQPS(c, p, readQPSMean, proposeQPSMean)
	}

	// This range's lease carries too small a fraction of the store's read
	// load to meaningfully help; don't churn it.
	if float64(rangeReadQPS) < p.minLeaseLoadFraction*float64(existing.readQPS) {
		return leaseRebalanceResult{}
	}

	// Converge gate, same two-sided shape as the count path: an overfull
	// source may shed to any below-mean candidate; a source that's above the
	// mean but still in band may only shed to an underfull candidate.
	srcOverfull := float64(existing.readQPS) > aboveMeanQPSThreshold(p, readQPSMean)
	if float64(existing.readQPS) <= readQPSMean {
		return leaseRebalanceResult{}
	}

	viable := make([]*candidate, 0, len(candidates))
	for _, c := range candidates {
		// The converge condition is checked per candidate (not "does any
		// candidate qualify") so that a candidate the vetoes below remove
		// can't open the gate for a move between two in-band stores.
		if srcOverfull {
			if float64(c.readQPS) >= readQPSMean {
				continue
			}
		} else if float64(c.readQPS) >= belowMeanQPSThreshold(p, readQPSMean) {
			continue
		}
		// Improvement check: the source/target gap must exceed the load
		// being moved. This guarantees the reverse transfer can't qualify
		// on the next pass (that would need the new gap, 2*rangeReadQPS -
		// gap, to exceed rangeReadQPS — impossible when gap > rangeReadQPS),
		// so the lease can't ping-pong.
		if float64(existing.readQPS-c.readQPS) <= float64(rangeReadQPS) {
			continue
		}
		// Don't stack read load on a store that's overfull on the write
		// dimension or short on disk.
		if qpsDimensionHot(p, proposeQPSMean, float64(c.proposeQPS)) {
			continue
		}
		if isDiskFullForRebalance(c.usage) {
			continue
		}
		viable = append(viable, c)
	}
	if len(viable) == 0 {
		// An overfull store that can't shed this range's reads to any
		// replica store signals the replica controller to create a landing
		// spot. In-band stores don't get to force replica moves.
		return leaseRebalanceResult{blocked: srcOverfull}
	}
	best := slices.MaxFunc(viable, compareForLeaseRebalance)
	return leaseRebalanceResult{op: &rebalanceOp{
		from: existing,
		to:   best,
	}}
}

// findRebalanceReplicaOp computes the load context and delegates to
// findRebalanceReplicaOpLoaded. computeActionForRangeTask calls the Loaded
// form directly with a context it shares with the lease evaluation.
func (rq *Queue) findRebalanceReplicaOp(ctx context.Context, rd *rfpb.RangeDescriptor, storesWithStats *storemap.StoresWithStats, localRepl IReplica, leaseBlocked bool) *rebalanceOp {
	loadBased := rq.isLoadBasedRebalanceEnabled(ctx)
	var p loadParams
	if loadBased {
		p = rq.loadParams(ctx)
	}
	return rq.findRebalanceReplicaOpLoaded(ctx, rd, storesWithStats, localRepl, leaseBlocked, loadBased, p)
}

func (rq *Queue) findRebalanceReplicaOpLoaded(ctx context.Context, rd *rfpb.RangeDescriptor, storesWithStats *storemap.StoresWithStats, localRepl IReplica, leaseBlocked bool, loadBased bool, p loadParams) *rebalanceOp {
	if len(storesWithStats.Usages) == 0 {
		return nil
	}
	localReplicaID := localRepl.ReplicaID()
	targetsByID := make(map[string]*candidate, len(storesWithStats.Usages))
	var sources []*candidate
	rangeID := rd.GetRangeId()
	for _, su := range storesWithStats.Usages {
		nhid := su.GetNode().GetNhid()
		store := &candidate{
			nhid:                  nhid,
			usage:                 su,
			fullDisk:              isDiskFull(su),
			replicaCountMeanLevel: replicaCountMeanLevel(storesWithStats, su),
			replicaCount:          su.ReplicaCount,
			rendezvousScore:       rendezvousScore(rangeID, nhid),
		}
		targetsByID[nhid] = store
	}

	replicasByZone := make(map[string]int)
	var localStore *candidate
	for _, repl := range rd.GetReplicas() {
		store, ok := targetsByID[repl.GetNhid()]
		if !ok {
			// The store might not be available rn.
			continue
		}
		replicasByZone[store.usage.GetNode().GetZone()]++
		delete(targetsByID, repl.GetNhid())
		if repl.GetReplicaId() != localReplicaID {
			// This is to prevent us from removing the replica on this node. We
			// can only support this after we have the ability to transfer the
			// leadership away.
			sources = append(sources, store)
		} else {
			localStore = store
		}
	}
	// Remove replicas that are in the middle of removal from candidates.
	for _, repl := range rd.GetRemoved() {
		delete(targetsByID, repl.GetNhid())
	}
	var targets []*candidate
	for _, target := range targetsByID {
		if !target.fullDisk {
			target.fullDisk = isDiskFullForRebalance(target.usage)
			targets = append(targets, target)
		}
	}

	if len(sources) == 0 || len(targets) == 0 {
		return nil
	}

	readSignal, proposeSignal := false, false
	if loadBased {
		readSignal = qpsSignalPresent(p, storesWithStats.ReadQPS.Mean)
		proposeSignal = qpsSignalPresent(p, storesWithStats.RaftProposeQPS.Mean)
		if readSignal || proposeSignal {
			// Populate effective (ledger-adjusted) QPS whenever there's a
			// load signal, even in cooldown: the count-based fallback below
			// vetoes hot targets using these fields, so skipping population
			// would leave them zero and silently bypass the veto.
			if localStore != nil {
				rq.populateCandidateQPS(localStore, p, storesWithStats.ReadQPS.Mean, storesWithStats.RaftProposeQPS.Mean)
			}
			for _, c := range sources {
				rq.populateCandidateQPS(c, p, storesWithStats.ReadQPS.Mean, storesWithStats.RaftProposeQPS.Mean)
			}
			for _, c := range targets {
				rq.populateCandidateQPS(c, p, storesWithStats.ReadQPS.Mean, storesWithStats.RaftProposeQPS.Mean)
			}
			// The cooldown gates only the load-based move selection: the
			// count-based fallback below must stay reachable so urgent moves
			// (evacuating a full disk, zone repair) aren't suppressed for a
			// range that just had a load-based op.
			if !rq.inCooldown(rd.GetRangeId()) {
				if leaseBlocked && readSignal {
					if op := rq.findLeaseUnblockReplicaOp(rd, localRepl, localStore, storesWithStats, sources, targets, replicasByZone, p); op != nil {
						return op
					}
				}
				if proposeSignal {
					if op := rq.findRebalanceReplicaOpQPS(rd, localRepl, storesWithStats, sources, targets, replicasByZone, p); op != nil {
						return op
					}
				}
				// No qualifying load-based move; fall through to the
				// count-based check so replica counts (≈ bytes ≈ eviction
				// pressure) still converge for ranges the load controllers
				// leave alone.
			}
		}
	}

	sortFunc := compareByZoneAndScore(replicasByZone)
	// MinFunc picks the worst source (best to evict).
	bestSource := slices.MinFunc(sources, sortFunc)
	// Consider targets best-first. The QPS vetoes below may skip the
	// count-best target; fall through to the next-best rather than abandoning
	// the move, so a single hot store can't stall count convergence.
	tgts := slices.Clone(targets)
	slices.SortFunc(tgts, sortFunc)
	slices.Reverse(tgts) // best target first
	overfullThreshold := int64(math.Ceil(aboveMeanReplicaCountThreshold(storesWithStats.ReplicaCount.Mean)))
	underfullThreshold := int64(math.Floor(belowMeanReplicaCountThreshold(storesWithStats.ReplicaCount.Mean)))
	sourceOverfull := bestSource.usage.ReplicaCount >= overfullThreshold
	// The apply load this range would carry onto a target — every replica
	// applies the same entries, so the local replica's propose QPS ≈ the load
	// each follower bears. Used for the post-move QPS-neutral check below and
	// the hot-range cooldown deferral. On a Usage error it stays 0, so the
	// post-move check degrades to the old pre-move behavior (safe).
	var rangeProposeQPS int64
	if ru, err := localRepl.Usage(); err == nil {
		rangeProposeQPS = ru.GetRaftProposeQps()
	}
	// Hot-range deferral: a range carrying meaningful apply load that just had a
	// load-based move is the load controller's to manage. A count move of it
	// during its cooldown would undo that (snapshot-costly) work, so leave it
	// alone; cold ranges may still be count-moved during cooldown, and a
	// disk-full source is never deferred (evacuation wins). The rangeProposeQPS
	// > 0 guard matters: without it an idle range on an idle source hits the
	// 0 >= minFraction*0 edge and gets wrongly deferred.
	if loadBased && proposeSignal && !bestSource.fullDisk &&
		rq.inCooldown(rd.GetRangeId()) && rangeProposeQPS > 0 &&
		float64(rangeProposeQPS) >= p.minReplicaLoadFraction*float64(bestSource.proposeQPS) {
		return nil
	}
	for _, bestTarget := range tgts {
		sourceZoneCount := replicasByZone[bestSource.usage.GetNode().GetZone()]
		targetZoneCount := replicasByZone[bestTarget.usage.GetNode().GetZone()]
		// If the difference between zone counts is 1 or less, a move can't
		// help zone balance.
		moveAcrossZones := sourceZoneCount-targetZoneCount >= 2
		// Worth-it gate: move only if the source is disk-full, the move
		// improves zone balance, the source is count-overfull, or this target
		// is count-underfull.
		if !bestSource.fullDisk && !moveAcrossZones && !sourceOverfull &&
			bestTarget.usage.ReplicaCount >= underfullThreshold {
			continue
		}
		// Count-based moves must not undo load balance: skip a target that's
		// hot on either QPS dimension and try the next-best. The propose check
		// is post-move — it counts the apply load this move would *add*
		// (tgt.proposeQPS + rangeProposeQPS), so count never re-heats a store
		// the load controller just cooled. The read check stays pre-move: a
		// replica move adds no read load; it just avoids stacking apply load
		// onto an already read-hot store's CPU. A disk-full source is exempt —
		// evacuating it outranks load balance.
		if loadBased && !bestSource.fullDisk {
			if readSignal && float64(bestTarget.readQPS) > aboveMeanQPSThreshold(p, storesWithStats.ReadQPS.Mean) {
				continue
			}
			if proposeSignal && float64(bestTarget.proposeQPS)+float64(rangeProposeQPS) > aboveMeanQPSThreshold(p, storesWithStats.RaftProposeQPS.Mean) {
				continue
			}
		}
		return &rebalanceOp{
			from: bestSource,
			to:   bestTarget,
		}
	}
	return nil
}

// zonePairOK reports whether moving a replica of this range from src's zone
// to tgt's zone preserves zone spread: the target zone must not end up with
// more replicas of the range than the source zone started with.
func zonePairOK(replicasByZone map[string]int, src, tgt *candidate) bool {
	sZone := src.usage.GetNode().GetZone()
	tZone := tgt.usage.GetNode().GetZone()
	if sZone == tZone {
		return true
	}
	return replicasByZone[tZone]+1 <= replicasByZone[sZone]
}

// bytesHigh reports whether the store's disk usage is above the byte-mean
// band. This keeps bytes level across stores long before the disk-full
// cliffs: eviction is local to each store, so the fullest store would start
// evicting first and silently shorten the cache lifetime for everyone.
func bytesHigh(p loadParams, c *candidate, bytesMean float64) bool {
	return float64(c.usage.GetTotalBytesUsed()) > bytesMean*(1+p.bandRatio)
}

// findRebalanceReplicaOpQPS looks for a follower move that improves the
// balance of propose (apply) QPS across stores. Moving a follower moves the
// range's apply cost but no read cost, so propose QPS is the only dimension
// it balances; reads and bytes appear as vetoes on targets.
func (rq *Queue) findRebalanceReplicaOpQPS(rd *rfpb.RangeDescriptor, localRepl IReplica, storesWithStats *storemap.StoresWithStats, sources, targets []*candidate, replicasByZone map[string]int, p loadParams) *rebalanceOp {
	ru, err := localRepl.Usage()
	if err != nil {
		rq.log.Errorf("failed to get usage of replica c%dn%d: %s", rd.GetRangeId(), localRepl.ReplicaID(), err)
		return nil
	}
	// Every replica of a range applies the same entries, so the local
	// replica's propose QPS ≈ the apply cost carried by each follower.
	rangeProposeQPS := ru.GetRaftProposeQps()
	proposeMean := storesWithStats.RaftProposeQPS.Mean
	readMean := storesWithStats.ReadQPS.Mean
	bytesMean := storesWithStats.TotalBytesUsed.Mean

	sortFunc := compareByZoneThen(replicasByZone, compareForReplicaRebalance)
	srcs := slices.Clone(sources)
	slices.SortFunc(srcs, sortFunc) // worst (best to evict) first
	tgts := slices.Clone(targets)
	slices.SortFunc(tgts, sortFunc)
	slices.Reverse(tgts) // best target first

	for _, src := range srcs {
		// Worth-it floor: the range's apply load must be a meaningful
		// fraction of the source store's propose load.
		if float64(rangeProposeQPS) < p.minReplicaLoadFraction*float64(src.proposeQPS) {
			continue
		}
		// Source floor: only shed from a store above the mean (or disk-full),
		// mirroring the lease path's `existing.readQPS <= mean` gate.
		// Equalizing two below-mean stores never reduces the busiest store and
		// just burns a snapshot.
		if !src.fullDisk && float64(src.proposeQPS) <= proposeMean {
			continue
		}
		srcSheds := src.fullDisk || float64(src.proposeQPS) > aboveMeanQPSThreshold(p, proposeMean)
		for _, tgt := range tgts {
			if !zonePairOK(replicasByZone, src, tgt) {
				continue
			}
			// Converge gate, two-sided like the lease path: an overfull (or
			// disk-full) source may shed to any below-mean target; an in-band
			// source (above the mean but within the band) may only shed to an
			// underfull one.
			if srcSheds {
				if float64(tgt.proposeQPS) >= proposeMean {
					continue
				}
			} else if float64(tgt.proposeQPS) >= belowMeanQPSThreshold(p, proposeMean) {
				continue
			}
			// Improvement check: the gap must exceed the load being moved,
			// or the reverse move looks attractive on the next pass.
			if float64(src.proposeQPS-tgt.proposeQPS) <= float64(rangeProposeQPS) {
				continue
			}
			// Don't stack apply load on a read-hot or byte-heavy store.
			if qpsDimensionHot(p, readMean, float64(tgt.readQPS)) {
				continue
			}
			if bytesHigh(p, tgt, bytesMean) {
				continue
			}
			return &rebalanceOp{from: src, to: tgt}
		}
	}
	return nil
}

// findLeaseUnblockReplicaOp moves a follower of a read-hot range to a
// read-underfull store so that a later pass can transfer the lease there. A
// lease can only move to a store that already holds a replica; when all of a
// range's replicas sit on read-hot stores the lease controller is blocked,
// and this creates the landing spot. The improvement check uses the range's
// READ QPS with the local (leaseholder) store as the source: that is the
// load that will follow the lease, even though the replica being moved is a
// follower on another store.
func (rq *Queue) findLeaseUnblockReplicaOp(rd *rfpb.RangeDescriptor, localRepl IReplica, localStore *candidate, storesWithStats *storemap.StoresWithStats, sources, targets []*candidate, replicasByZone map[string]int, p loadParams) *rebalanceOp {
	if localStore == nil {
		return nil
	}
	ru, err := localRepl.Usage()
	if err != nil {
		rq.log.Errorf("failed to get usage of replica c%dn%d: %s", rd.GetRangeId(), localRepl.ReplicaID(), err)
		return nil
	}
	rangeReadQPS := ru.GetReadQps()
	readMean := storesWithStats.ReadQPS.Mean
	proposeMean := storesWithStats.RaftProposeQPS.Mean
	bytesMean := storesWithStats.TotalBytesUsed.Mean

	if float64(rangeReadQPS) < p.minLeaseLoadFraction*float64(localStore.readQPS) {
		return nil
	}
	if float64(localStore.readQPS) <= readMean {
		return nil
	}

	srcs := slices.Clone(sources)
	slices.SortFunc(srcs, compareByZoneThen(replicasByZone, compareForReplicaRebalance)) // worst first
	tgts := slices.Clone(targets)
	slices.SortFunc(tgts, compareByZoneThen(replicasByZone, compareForLeaseRebalance))
	slices.Reverse(tgts) // most read headroom first

	for _, src := range srcs {
		for _, tgt := range tgts {
			if !zonePairOK(replicasByZone, src, tgt) {
				continue
			}
			// Improvement check on the read dimension: once the lease
			// follows, the local store sheds rangeReadQPS to tgt.
			if float64(localStore.readQPS-tgt.readQPS) <= float64(rangeReadQPS) {
				continue
			}
			// The follow-up lease transfer only moves the lease to a store
			// below the read mean (findRebalanceLeaseOpQPS's srcOverfull
			// gate, which is the gate that set leaseBlocked here). Don't
			// create a landing spot that won't qualify for it.
			if float64(tgt.readQPS) >= readMean {
				continue
			}
			// The follower's apply cost moves with it; don't stack it on a
			// write-hot or byte-heavy store.
			if qpsDimensionHot(p, proposeMean, float64(tgt.proposeQPS)) {
				continue
			}
			if bytesHigh(p, tgt, bytesMean) {
				continue
			}
			return &rebalanceOp{from: src, to: tgt, leaseUnblock: true}
		}
	}
	return nil
}

func (rq *Queue) findRemovableReplicas(rd *rfpb.RangeDescriptor, replicaStateMap map[uint64]constants.ReplicaState, brandNewReplicaID *uint64) []*rfpb.ReplicaDescriptor {
	numUpToDateReplicas := 0
	replicasBehind := make([]*rfpb.ReplicaDescriptor, 0)
	replicasUnavailable := make([]*rfpb.ReplicaDescriptor, 0)
	for _, r := range rd.GetReplicas() {
		rs, ok := replicaStateMap[r.GetReplicaId()]
		if !ok {
			replicasUnavailable = append(replicasUnavailable, r)
			continue
		}
		switch rs {
		case constants.ReplicaStateCurrent:
			numUpToDateReplicas++
		case constants.ReplicaStateBehind:
			// Don't consider brand new replica ID as falling behind
			if brandNewReplicaID == nil || r.GetReplicaId() != *brandNewReplicaID {
				replicasBehind = append(replicasBehind, r)
			}
		case constants.ReplicaStateUnknown:
			replicasUnavailable = append(replicasUnavailable, r)
		}
	}

	quorum := computeQuorum(len(rd.GetReplicas()) - 1)
	if numUpToDateReplicas < quorum {
		// The number of up-to-date replicas is less than quorum. Don't remove
		rq.log.Debugf("there are %d up-to-date replicas for range %d and quorum is %d, don't remove", rd.GetRangeId(), numUpToDateReplicas, quorum)
		return nil
	}

	if numUpToDateReplicas > quorum {
		// Any replica can be removed.
		replicasByStatus := rq.storeMap.DivideByStatus(rd.GetReplicas())
		if suspects := replicasByStatus.SuspectReplicas; len(suspects) > 0 {
			rq.log.Debugf("there are %d suspects for range %d, removing suspects", rd.GetRangeId(), len(suspects))
			return suspects
		}
		if count := len(replicasUnavailable); count > 0 {
			rq.log.Debugf("there are %d replicas for range %d without states, removing replicas without states", rd.GetRangeId(), count)
			return replicasUnavailable
		}
		rq.log.Debugf("there are %d up-to-date replicas for range %d and quorum is %d, any replica can be removed", rd.GetRangeId(), numUpToDateReplicas, quorum)
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

	removableSet := make(set.Set[string], len(removableReplicas))
	for _, repl := range removableReplicas {
		removableSet.Add(repl.GetNhid())
	}
	// Find the local replica's nhid (if any) so we can skip it as a
	// candidate.
	replicaByNhid := make(map[string]*rfpb.ReplicaDescriptor, len(rd.GetReplicas()))
	replicaNhids := make([]string, 0, len(rd.GetReplicas()))
	var localNhid string
	for _, repl := range rd.GetReplicas() {
		replicaByNhid[repl.GetNhid()] = repl
		replicaNhids = append(replicaNhids, repl.GetNhid())
		if repl.GetReplicaId() == localReplicaID {
			localNhid = repl.GetNhid()
		}
	}
	// Get all replica stores to be able to get per-zone counts.
	stores := rq.storeMap.GetStoresWithStatsFromIDs(replicaNhids)

	replicasByZone := make(map[string]int)
	var candidates []*candidate
	for _, su := range stores.Usages {
		replicasByZone[su.GetNode().GetZone()]++
		nhid := su.GetNode().GetNhid()
		if !removableSet.Contains(nhid) || nhid == localNhid {
			continue
		}
		candidates = append(candidates, &candidate{
			nhid:                  nhid,
			usage:                 su,
			replicaCount:          su.GetReplicaCount(),
			replicaCountMeanLevel: replicaCountMeanLevel(stores, su),
			fullDisk:              isDiskFull(su),
			rendezvousScore:       rendezvousScore(rd.GetRangeId(), nhid),
		})
	}
	if len(candidates) == 0 {
		// cannot find candidates for removal
		return nil
	}

	worst := slices.MinFunc(candidates, compareByZoneAndScore(replicasByZone))
	return replicaByNhid[worst.nhid]
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
	}
	if op := change.removeDataOp; op != nil {
		c, err := rq.apiClient.GetForReplica(ctx, op.replDesc)
		if err != nil {
			err = status.WrapErrorf(err, "unable to remove data on c%dn%d due to failure to get api client", op.replDesc.GetRangeId(), op.replDesc.GetReplicaId())
			rq.log.Error(err.Error())
			return err
		}
		_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
			ReplicaId: op.replDesc.GetReplicaId(),
			Range:     op.rd,
		})
		if err != nil {
			rq.log.Errorf("unable to remove data on c%dn%d: %s", op.replDesc.GetRangeId(), op.replDesc.GetReplicaId(), err)
			return err
		}
		rq.log.Infof("RemoveData on c%dn%d succeeded", op.replDesc.GetRangeId(), op.replDesc.GetReplicaId())
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

func (rq *Queue) processRangeTask(ctx context.Context, task *driverTask, action DriverAction) (requeueType RequeueType) {
	var change *change
	rangeID := task.key.rangeID
	repl := task.rangeTask.repl
	rd := rq.store.GetRange(rangeID)
	if rd == nil {
		// We might be calling GetRange in the small window between RemoveRange and AddRange
		return RequeueCheckOtherActions
	}

	if !rq.isDriverEnabled(ctx) {
		rq.log.Debugf("driver is disabled via experiment flag, skipping action %s", action)
		return RequeueNoop
	}

	switch action {
	case DriverNoop:
	case DriverFinishReplicaRemoval:
		change = rq.finishReplicaRemoval(rd)
	case DriverSplitRange:
		change = rq.splitRange(rd)
	case DriverAddReplica:
		change = rq.addReplica(rd)
	case DriverReplaceDeadReplica:
		change = rq.replaceDeadReplica(rd)
	case DriverRemoveReplica:
		change = rq.removeReplica(ctx, rd, repl)
	case DriverRemoveDeadReplica:
		change = rq.removeDeadReplica(rd)
	case DriverRebalanceReplica:
		change = rq.rebalanceReplica(ctx, rd, repl)
	case DriverRebalanceLease:
		change = rq.rebalanceLease(ctx, rd, repl)
	case DriverInitializePartition:
		// This should not be called for range tasks
		alert.UnexpectedEvent("unexpected-action-for-range-task", "driver action %s for range_id: %q", action, rangeID)
	}

	rq.log.Debugf("driver action: %s, range_id: %d, hasChange=%t", action, rangeID, change != nil)

	defer func() {
		if change == nil {
			action = DriverNoop
		}
		metrics.RaftDriverActionCount.With(prometheus.Labels{
			metrics.RaftDriverAction:      action.String(),
			metrics.RaftDriverRequeueType: requeueType.String(),
			metrics.RaftRangeIDLabel:      strconv.Itoa(int(rangeID)),
		}).Inc()
	}()

	if change == nil {
		return RequeueNoop
	}

	err := rq.applyChange(ctx, change)
	if err != nil {
		rq.log.Warningf("Error apply change for action %s to range_id: %d: %s", action, rangeID, err)
	}
	if err == nil && change.rebalanceRecord != nil {
		// Start the cooldown and adjust the pending-move ledger only for
		// changes that actually applied; a failed transfer must not lock a
		// still-hot range out or leave phantom load in the ledger.
		rq.recordRebalance(ctx, change.rebalanceRecord)
	}

	if action == DriverNoop || action == DriverRebalanceReplica || action == DriverRebalanceLease {
		if action == DriverRebalanceReplica && err == nil && change.leaseUnblock {
			// Only a lease-unblock move requeues: it created a lease landing
			// spot for a read-hot range, so requeue for the follow-up lease
			// transfer instead of waiting for the next scan. A normal replica
			// move must not requeue — the count-based fallback is not
			// cooldown-gated, so an immediate re-evaluation could chain
			// back-to-back moves and defeat the range's cooldown.
			return RequeueCheckOtherActions
		}
		return RequeueNoop
	}

	if err != nil {
		rq.log.Errorf("failed to process replica for action %s (range_id: %d): %s", action, rangeID, err)
		return RequeueRetry
	} else {
		return RequeueCheckOtherActions
	}
}

func (rq *Queue) processPartitionTask(ctx context.Context, task *driverTask, action DriverAction) (requeueType RequeueType) {
	if !rq.isDriverEnabled(ctx) {
		rq.log.Debugf("driver is disabled via experiment flag, skipping action %s", action)
		return RequeueNoop
	}

	var err error
	switch action {
	case DriverInitializePartition:
		err = rq.initializePartition(ctx, task.partitionTask.config)
	case DriverAddReplica, DriverFinishReplicaRemoval, DriverNoop, DriverRebalanceLease, DriverRebalanceReplica, DriverRemoveDeadReplica, DriverRemoveReplica, DriverReplaceDeadReplica, DriverSplitRange:
		// This should not be called for range tasks
		alert.UnexpectedEvent("unexpected-action-for-partition-task", "driver action %s for partition %q", action, task.key.partitionID)
	}
	if err != nil {
		rq.log.Errorf("failed to process partition task action %s (ID: %s): %s", action, task.key.partitionID, err)
		return RequeueRetry
	}
	return RequeueNoop
}

func (rq *Queue) processTask(ctx context.Context, task *driverTask, action DriverAction) (requeueType RequeueType) {
	switch task.key.taskType {
	case RangeTaskType:
		return rq.processRangeTask(ctx, task, action)
	case PartitionTaskType:
		return rq.processPartitionTask(ctx, task, action)
	}
	return RequeueNoop
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

func aboveMeanQPSThreshold(p loadParams, mean float64) float64 {
	return mean + math.Max(mean*p.bandRatio, p.qpsFloor)
}

func belowMeanQPSThreshold(p loadParams, mean float64) float64 {
	return mean - math.Max(mean*p.bandRatio, p.qpsFloor)
}

// qpsSignalPresent reports whether a QPS dimension carries enough load to
// balance on. Below the floor, count-based rebalancing applies instead.
func qpsSignalPresent(p loadParams, mean float64) bool {
	return mean >= p.qpsFloor
}

// qpsDimensionHot reports whether a store carrying `qps` on a QPS dimension
// with cluster mean `mean` is above that dimension's band — but only when the
// dimension carries a real load signal (below the floor the band is just
// noise). It is the cross-dimension veto shared by the load-based lease and
// replica moves: don't stack a move onto a store already hot on the *other*
// dimension.
func qpsDimensionHot(p loadParams, mean, qps float64) bool {
	return qpsSignalPresent(p, mean) && qps > aboveMeanQPSThreshold(p, mean)
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
	// Smoothed QPS of the store and its position relative to the cluster
	// mean, per dimension. Only populated on load-based rebalance paths.
	readQPS             int64
	readQPSMeanLevel    meanLevel
	proposeQPS          int64
	proposeQPSMeanLevel meanLevel
	// Tie-breaker in compareByScore. Zero when no rangeID is available.
	rendezvousScore uint64
}

// rendezvousScore is a deterministic tie-breaker hash over (rangeID, nhid).
func rendezvousScore(rangeID uint64, nhid string) uint64 {
	// FNV-1a 64 is fast enough, doesn't add any deps, and 64 bits is enough for
	// breaking ties. The fixed-width rangeID prefix avoids ambiguity between
	// e.g. (12, "3-foo") and (123, "-foo"), and FNV's spec-defined seed makes
	// the score reproducible across processes.
	h := fnv.New64a()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], rangeID)
	h.Write(buf[:])
	h.Write([]byte(nhid))
	return h.Sum64()
}

func (c *candidate) String() string {
	return c.nhid
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

	// Rendezvous hash tie-breaker, -1, 0, or 1
	return cmp.Compare(a.rendezvousScore, b.rendezvousScore)
}

func compareByScoreAndID(a *candidate, b *candidate) int {
	if res := compareByScore(a, b); res != 0 {
		return res
	}
	return cmp.Compare(a.nhid, b.nhid)
}

// compareByZoneAndScore returns a comparator that ranks candidates as replica
// placement targets: candidates in less-represented zones are preferred, with
// compareByScoreAndID as the tiebreak. A positive result means a is a better
// target than b.
//
// Use with slices.MaxFunc to pick the best target (when adding a replica).
// Use with slices.MinFunc to pick the worst replica (for removing or
// rebalancing).
func compareByZoneAndScore(replicasByZone map[string]int) func(a, b *candidate) int {
	return compareByZoneThen(replicasByZone, compareByScoreAndID)
}

// compareByZoneThen returns a comparator that prefers candidates in
// less-represented zones and breaks ties with the given comparator.
func compareByZoneThen(replicasByZone map[string]int, then func(a, b *candidate) int) func(a, b *candidate) int {
	return func(a, b *candidate) int {
		za := replicasByZone[a.usage.GetNode().GetZone()]
		zb := replicasByZone[b.usage.GetNode().GetZone()]
		if za != zb {
			// Fewer replicas in a's zone → a is a better target.
			return cmp.Compare(zb, za)
		}
		return then(a, b)
	}
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

func leaseCountMeanLevel(mean float64, su *rfpb.StoreUsage) meanLevel {
	maxLeaseCount := aboveMeanLeaseCountThreshold(mean)
	minLeaseCount := belowMeanLeaseCountThreshold(mean)
	curLeaseCount := float64(su.GetLeaseCount())
	if curLeaseCount < minLeaseCount {
		return belowMean
	} else if curLeaseCount >= maxLeaseCount {
		return aboveMean
	}
	return aroundMean
}

func qpsMeanLevel(p loadParams, mean float64, qps int64) meanLevel {
	if float64(qps) < belowMeanQPSThreshold(p, mean) {
		return belowMean
	} else if float64(qps) >= aboveMeanQPSThreshold(p, mean) {
		return aboveMean
	}
	return aroundMean
}

// compareByQPSDimension compares two candidates on one QPS dimension with
// the same shape as the count comparisons in compareByScore: mean level
// first (±[10, 12]), then the relative QPS difference ((-10, 10)). Returns 0
// when the candidates are equivalent on this dimension.
func compareByQPSDimension(aLevel, bLevel meanLevel, aQPS, bQPS int64) int {
	if aLevel != bLevel {
		score := int(10 + math.Abs(float64(aLevel-bLevel)))
		if aLevel > bLevel {
			return score
		}
		return -score
	}
	diff := math.Abs(float64(aQPS - bQPS))
	if aQPS < bQPS {
		return int(math.Ceil(diff / float64(bQPS) * 10))
	} else if aQPS > bQPS {
		return -int(math.Ceil(diff / float64(aQPS) * 10))
	}
	return 0
}

// compareForLeaseRebalance ranks candidates as lease-transfer targets in
// load-based mode: read QPS first, then the count-based score as tie-break.
func compareForLeaseRebalance(a *candidate, b *candidate) int {
	if res := compareByQPSDimension(a.readQPSMeanLevel, b.readQPSMeanLevel, a.readQPS, b.readQPS); res != 0 {
		return res
	}
	return compareByScoreAndID(a, b)
}

// compareForReplicaRebalance ranks candidates as replica-move targets in
// load-based mode: disk fullness dominates (as in compareByScore), then
// propose QPS, then the count-based score as tie-break.
func compareForReplicaRebalance(a *candidate, b *candidate) int {
	if a.fullDisk != b.fullDisk {
		if a.fullDisk {
			return -20
		}
		return 20
	}
	if res := compareByQPSDimension(a.proposeQPSMeanLevel, b.proposeQPSMeanLevel, a.proposeQPS, b.proposeQPS); res != 0 {
		return res
	}
	return compareByScoreAndID(a, b)
}
