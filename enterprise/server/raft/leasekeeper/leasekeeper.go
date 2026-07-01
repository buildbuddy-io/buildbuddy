package leasekeeper

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/boundedstack"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type rangeID uint64

type leaseAction int

const (
	Drop leaseAction = iota
	Acquire
)

const (
	listenerID = "leaseKeeper"
)

func (a leaseAction) String() string {
	switch a {
	case Acquire:
		return "Acquire"
	case Drop:
		return "Drop"
	default:
		return "UnknownLeaseAction"
	}
}

type leaseInstruction struct {
	rangeID rangeID
	reason  string
	action  leaseAction
}

func (l leaseInstruction) String() string {
	return fmt.Sprintf("instruction for range %d (%s)", l.rangeID, l.reason)
}

type LeaseKeeper struct {
	nodeHost  *dragonboat.NodeHost
	zone      string
	log       log.Logger
	liveness  *nodeliveness.Liveness
	session   *client.Session
	listener  *listener.RaftListener
	broadcast chan<- events.Event

	mu      sync.Mutex
	leases  map[rangeID]*leaseAgent
	leaders set.Set[rangeID]
	open    set.Set[rangeID]

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel context.CancelFunc

	quitAll chan struct{}

	leaderUpdates       <-chan raftio.LeaderInfo
	nodeUnloaded        <-chan raftio.NodeInfo
	nodeLivenessUpdates <-chan *rfpb.NodeLivenessRecord
}

func New(nodeHost *dragonboat.NodeHost, zone string, log log.Logger, liveness *nodeliveness.Liveness, listener *listener.RaftListener, broadcast chan<- events.Event, session *client.Session) *LeaseKeeper {
	ctx, cancelFunc := context.WithCancel(context.Background())
	eg, gctx := errgroup.WithContext(ctx)
	return &LeaseKeeper{
		nodeHost:            nodeHost,
		zone:                zone,
		log:                 log,
		liveness:            liveness,
		session:             session,
		listener:            listener,
		broadcast:           broadcast,
		leases:              make(map[rangeID]*leaseAgent),
		leaders:             make(set.Set[rangeID]),
		open:                make(set.Set[rangeID]),
		leaderUpdates:       listener.AddLeaderChangeListener(listenerID),
		nodeUnloaded:        listener.AddNodeUnloadedListener(listenerID),
		nodeLivenessUpdates: liveness.AddListener(),

		eg:       eg,
		egCtx:    gctx,
		egCancel: cancelFunc,
	}
}

func (lk *LeaseKeeper) Start() {
	lk.eg.Go(func() error {
		lk.watchLeases()
		return nil
	})
}

func (lk *LeaseKeeper) Stop() {
	lk.log.Info("Leasekeeper shutdown started")
	now := time.Now()
	defer func() {
		lk.log.Infof("Leasekeeper shutdown finished in %s", time.Since(now))
	}()

	lk.egCancel()
	lk.eg.Wait()
}

// A leaseAgent keeps a single rangelease up to date based on the instructions
// received on the updates chan. Because leaseAgents are stored in a sync.Map
// and created via LoadOrStore(), a leaseAgent's goroutine is not started until
// the first instruction is enqueued.
type leaseAgent struct {
	replicaID uint64
	log       log.Logger
	l         *rangelease.Lease
	ctx       context.Context
	cancel    context.CancelFunc
	eg        *errgroup.Group
	broadcast chan<- events.Event

	// labels is the standard per-range metric labelset (range, nodehost,
	// partition, zone) for this lease agent's range. Computed once at
	// creation and immutable thereafter, so it can be read without locking.
	labels prometheus.Labels

	updates *boundedstack.BoundedStack[*leaseInstruction]
}

func (la *leaseAgent) isStopped() bool {
	select {
	case <-la.ctx.Done():
		return true
	default:
		return false
	}
}

// setLeaderGauge records whether this node is the raft leader for the agent's
// range, keyed by the full per-range labelset.
func (la *leaseAgent) setLeaderGauge(leader bool) {
	v := 0.0
	if leader {
		v = 1.0
	}
	metrics.RaftLeaders.With(la.labels).Set(v)
}

func (la *leaseAgent) sendRangeEvent(eventType events.EventType) {
	ev := events.RangeEvent{
		Type: eventType,
	}
	select {
	case la.broadcast <- ev:
		break
	default:
		metrics.RaftStoreEventBroadcastDropped.With(prometheus.Labels{
			metrics.RaftEventBroadcaster: "leasekeeper",
			metrics.RaftEventType:        ev.EventType().String(),
		}).Inc()
		la.log.Warningf("leaseAgent dropped range event: %+v", ev)
	}
}

func (la *leaseAgent) doSingleInstruction(ctx context.Context, instruction *leaseInstruction) {
	valid := la.l.Valid(ctx)
	start := time.Now()

	rangeID := la.l.GetRangeID()

	switch instruction.action {
	case Acquire:
		// If the lease is already valid, early-return here.
		if valid {
			return
		}
		err := la.l.Lease(ctx)
		dur := time.Since(start)
		leaseAction := "Acquire"
		metrics.RaftLeaseActionCount.With(prometheus.Labels{
			metrics.RaftRangeIDLabel:         strconv.Itoa(int(rangeID)),
			metrics.RaftLeaseActionLabel:     leaseAction,
			metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
		}).Inc()
		if err != nil {
			la.log.Errorf("Error acquiring rangelease (%s): %s %s after %s", la.l.Desc(ctx), err, instruction, dur)
			return
		}
		la.log.Debugf("Acquired lease [%s] %s after callback (%s)", la.l.Desc(ctx), dur, instruction)
		metrics.RaftLeases.With(la.labels).Inc()
		metrics.RaftLeaseActionDurationMsec.With(prometheus.Labels{
			metrics.RaftLeaseActionLabel: leaseAction,
		}).Observe(float64(dur.Milliseconds()))
		la.sendRangeEvent(events.EventRangeLeaseAcquired)
	case Drop:
		// If the lease is already invalid, early-return here.
		if !valid {
			return
		}
		leaseAction := "Drop"
		// This is a no-op if we don't have the lease.
		err := la.l.Release(ctx)
		dur := time.Since(start)
		metrics.RaftLeaseActionCount.With(prometheus.Labels{
			metrics.RaftRangeIDLabel:         strconv.Itoa(int(rangeID)),
			metrics.RaftLeaseActionLabel:     leaseAction,
			metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
		}).Inc()
		if err != nil {
			la.log.Errorf("Error dropping rangelease (%s): %s (%s)", la.l.Desc(ctx), err, instruction)
			return
		}
		la.log.Debugf("Dropped lease [%s] %s after callback (%s)", la.l.Desc(ctx), dur, instruction)
		metrics.RaftLeases.With(la.labels).Dec()
		metrics.RaftLeaseActionDurationMsec.With(prometheus.Labels{
			metrics.RaftLeaseActionLabel: leaseAction,
		}).Observe(float64(dur.Milliseconds()))
		la.sendRangeEvent(events.EventRangeLeaseDropped)
	}
}

func (la *leaseAgent) stop() {
	la.cancel()
	la.eg.Wait()

	la.l.Stop()
}

func (la *leaseAgent) runloop() {
	for {
		instruction, err := la.updates.Recv(la.ctx)
		if err != nil {
			// context cancelled
			return
		}
		la.doSingleInstruction(la.ctx, instruction)
	}
}

func (la *leaseAgent) queueInstruction(instruction *leaseInstruction) {
	if la.isStopped() {
		return
	}
	la.updates.Push(instruction)
}

func (lk *LeaseKeeper) newLeaseAgent(rd *rfpb.RangeDescriptor, r *replica.Replica) *leaseAgent {
	ctx, cancel := context.WithCancel(context.TODO())
	updates, err := boundedstack.New[*leaseInstruction](1)
	eg, gctx := errgroup.WithContext(ctx)
	if err != nil {
		alert.UnexpectedEvent("unexpected_boundedstack_error", err)
	}
	la := &leaseAgent{
		replicaID: r.ReplicaID(),
		log:       lk.log,
		l:         rangelease.New(lk.nodeHost, lk.session, lk.log, lk.liveness, rd.GetRangeId(), r),
		ctx:       gctx,
		cancel:    cancel,
		eg:        eg,
		broadcast: lk.broadcast,
		labels:    keys.RangeMetricLabels(rd, lk.nodeHost.ID(), lk.zone),
		updates:   updates,
	}
	la.eg.Go(func() error {
		la.runloop()
		return nil
	})
	return la
}

func (lk *LeaseKeeper) watchLeases() {
	for {
		select {
		case info, ok := <-lk.leaderUpdates:
			if !ok {
				// channel was closed and drained
				continue
			}
			leader := info.LeaderID == info.ReplicaID && info.Term > 0
			rangeID := rangeID(info.ShardID)

			lk.mu.Lock()
			open := lk.open.Contains(rangeID)
			if leader {
				lk.leaders.Add(rangeID)
			} else {
				lk.leaders.Remove(rangeID)
			}
			la := lk.leases[rangeID]
			lk.mu.Unlock()

			if la == nil {
				// We don't have a lease agent for this range yet, so we
				// can't build the full RaftLeaders labelset. The gauge is
				// emitted once the range is added (see setLeaderGauge in
				// AddRange) and on subsequent leader updates.
				lk.log.Debugf("Range %d has not been opened yet (ignoring leader update)", rangeID)
				continue
			}
			la.setLeaderGauge(leader)
			action := Drop
			if open && leader {
				action = Acquire
			}
			la.queueInstruction(&leaseInstruction{
				rangeID: rangeID,
				reason:  fmt.Sprintf("raft leader change = %t, open = %t", leader, open),
				action:  action,
			})
		case <-lk.egCtx.Done():
			lk.mu.Lock()
			leases := make([]*leaseAgent, 0, len(lk.leases))
			for _, la := range lk.leases {
				leases = append(leases, la)
			}
			lk.mu.Unlock()
			for _, la := range leases {
				la.stop()
			}
			lk.listener.RemoveLeaderChangeListener(listenerID)
			lk.listener.RemoveNodeUnloadedListener(listenerID)
			return
		case <-lk.nodeLivenessUpdates:
			lk.mu.Lock()
			leases := maps.Clone(lk.leases)
			lk.mu.Unlock()
			for rangeID, la := range leases {
				action := Drop
				lk.mu.Lock()
				if lk.open.Contains(rangeID) && lk.leaders.Contains(rangeID) {
					action = Acquire
				}
				lk.mu.Unlock()

				la.queueInstruction(&leaseInstruction{
					rangeID: rangeID,
					reason:  "node liveness update",
					action:  action,
				})
			}
		case nodeInfo, ok := <-lk.nodeUnloaded:
			if !ok {
				// channel was closed and drained
				continue
			}
			rangeID := rangeID(nodeInfo.ShardID)
			lk.mu.Lock()
			la := lk.leases[rangeID]
			delete(lk.leases, rangeID)
			lk.mu.Unlock()
			if la != nil {
				if la.replicaID == nodeInfo.ReplicaID {
					la.stop()
				}
			}
		}
	}
}

func (lk *LeaseKeeper) isStopped() bool {
	return lk.egCtx.Err() != nil
}

func (lk *LeaseKeeper) loadOrStoreNewLeaseAgent(rd *rfpb.RangeDescriptor, r *replica.Replica) *leaseAgent {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	rangeID := rangeID(rd.GetRangeId())
	la := lk.leases[rangeID]
	if la != nil {
		if la.replicaID == r.ReplicaID() {
			// the store lease agent is for the same replica id, we don't need to create a new lease agent.
			return la
		}
		lk.log.Warningf("stored leaseAgent is for c%dn%d, but the current replica is c%dn%d, overwriting", rangeID, la.replicaID, rangeID, r.ReplicaID())
	}
	la = lk.newLeaseAgent(rd, r)
	lk.leases[rangeID] = la
	lk.log.Infof("create new lease agent for c%dn%d", rangeID, la.replicaID)
	return la
}

func (lk *LeaseKeeper) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	if lk.isStopped() {
		return
	}

	// Don't track ranges that have not been setup yet.
	if len(rd.GetReplicas()) == 0 {
		return
	}
	la := lk.loadOrStoreNewLeaseAgent(rd, r)

	rangeID := rangeID(rd.GetRangeId())
	// When a range is added via AddRange(), the raft leader may already
	// have been chosen, meaning that `watchLeases` will not receive
	// additional callbacks that would trigger range lease acquisition. So
	// for newly added ranges, check if this node is the leader and trigger
	// lease acquisition here.
	lk.mu.Lock()
	lk.open.Add(rangeID)
	leader := lk.leaders.Contains(rangeID)
	lk.mu.Unlock()

	la.setLeaderGauge(leader)

	action := Drop
	if leader {
		action = Acquire
	}

	la.queueInstruction(&leaseInstruction{
		rangeID: rangeID,
		reason:  "Add range",
		action:  action,
	})
}

func (lk *LeaseKeeper) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	if lk.isStopped() {
		return
	}

	// Don't track ranges that have not been setup yet.
	if len(rd.GetReplicas()) == 0 {
		return
	}
	rangeID := rangeID(rd.GetRangeId())

	lk.mu.Lock()
	lk.open.Remove(rangeID)
	la := lk.leases[rangeID]
	lk.mu.Unlock()

	if la != nil {
		la.queueInstruction(&leaseInstruction{
			rangeID: rangeID,
			reason:  "remove range",
			action:  Drop,
		})
	}
}

func (lk *LeaseKeeper) LeaseCount(ctx context.Context) int64 {
	leaseCount := int64(0)
	lk.mu.Lock()
	leases := make([]*leaseAgent, 0, len(lk.leases))
	for _, la := range lk.leases {
		leases = append(leases, la)
	}
	lk.mu.Unlock()

	for _, la := range leases {
		if la.l.Valid(ctx) {
			leaseCount += 1
		}
	}
	return leaseCount
}

func (lk *LeaseKeeper) HaveLease(ctx context.Context, rid uint64) bool {
	rangeID := rangeID(rid)
	lk.mu.Lock()
	la := lk.leases[rangeID]
	lk.mu.Unlock()

	if la == nil {
		return false
	}
	valid := la.l.Valid(ctx)

	lk.mu.Lock()
	leader := lk.leaders.Contains(rangeID)
	open := lk.open.Contains(rangeID)
	lk.mu.Unlock()

	shouldHaveLease := leader && open
	if !lk.isStopped() {
		if shouldHaveLease && !valid {
			lk.log.CtxWarningf(ctx, "HaveLease range: %d valid: %t, should have lease: %t", rangeID, valid, shouldHaveLease)
			la.queueInstruction(&leaseInstruction{
				rangeID: rangeID,
				reason:  "should have range",
				action:  Acquire,
			})
		} else if !shouldHaveLease && valid {
			lk.log.CtxWarningf(ctx, "HaveLease range: %d valid: %t, should have lease: %t", rangeID, valid, shouldHaveLease)
			la.queueInstruction(&leaseInstruction{
				rangeID: rangeID,
				reason:  "should not have range",
				action:  Drop,
			})
		}
	}
	return valid && shouldHaveLease
}
