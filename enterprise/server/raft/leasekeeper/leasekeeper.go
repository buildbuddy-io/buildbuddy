package leasekeeper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type shardID uint64

type leaseAction int

const (
	Drop leaseAction = iota
	Acquire
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
	shard  shardID
	reason string
	action leaseAction
}

func (l leaseInstruction) String() string {
	return fmt.Sprintf("instruction for range %d (%s)", l.shard, l.reason)
}

type LeaseKeeper struct {
	nodeHost  *dragonboat.NodeHost
	log       log.Logger
	liveness  *nodeliveness.Liveness
	listener  *listener.RaftListener
	broadcast chan<- events.Event

	// map shardID -> leaseAgent
	leases sync.Map

	mu      sync.Mutex
	started bool
	leaders map[shardID]bool
	open    map[shardID]bool
	quitAll chan struct{}

	leaderUpdates       <-chan raftio.LeaderInfo
	nodeLivenessUpdates <-chan *rfpb.NodeLivenessRecord
}

func New(nodeHost *dragonboat.NodeHost, log log.Logger, liveness *nodeliveness.Liveness, listener *listener.RaftListener, broadcast chan<- events.Event) *LeaseKeeper {
	return &LeaseKeeper{
		nodeHost:            nodeHost,
		log:                 log,
		liveness:            liveness,
		listener:            listener,
		broadcast:           broadcast,
		mu:                  sync.Mutex{},
		leases:              sync.Map{},
		leaders:             make(map[shardID]bool),
		open:                make(map[shardID]bool),
		leaderUpdates:       listener.AddLeaderChangeListener(),
		nodeLivenessUpdates: liveness.AddListener(),
	}
}

func (lk *LeaseKeeper) Start() {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	lk.quitAll = make(chan struct{})

	go lk.watchLeases()
	lk.started = true
}

func (lk *LeaseKeeper) Stop() {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	if !lk.started {
		return
	}

	lk.log.Infof("Leasekeeper shutdown started")
	now := time.Now()
	defer func() {
		lk.log.Infof("Leasekeeper shutdown finished in %s", time.Since(now))
	}()

	if lk.quitAll != nil {
		close(lk.quitAll)
	}
}

// A leaseAgent keeps a single rangelease up to date based on the instructions
// received on the updates chan. Because leaseAgents are stored in a sync.Map
// and created via LoadOrStore(), a leaseAgent's goroutine is not started until
// the first instruction is enqueued.
type leaseAgent struct {
	log       log.Logger
	l         *rangelease.Lease
	ctx       context.Context
	cancel    context.CancelFunc
	quit      chan struct{}
	once      *sync.Once
	updates   chan leaseInstruction
	broadcast chan<- events.Event
}

func (la *leaseAgent) doSingleInstruction(ctx context.Context, instruction leaseInstruction) {
	valid := la.l.Valid()
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	switch instruction.action {
	case Acquire:
		if err := la.l.Lease(ctx); err != nil {
			la.log.Errorf("Error acquiring rangelease (%s): %s %s", la.l.String(), err, instruction)
			return
		}
		if !valid {
			la.broadcastLeaseStatus(events.EventRangeLeaseAcquired)
			la.log.Debugf("Acquired lease [%s] %s after callback (%s)", la.l.String(), time.Since(start), instruction)
		}
	case Drop:
		// This is a no-op if we don't have the lease.
		if err := la.l.Release(ctx); err != nil {
			la.log.Errorf("Error dropping rangelease (%s): %s (%s)", la.l, err, instruction)
			return
		}
		if valid {
			la.broadcastLeaseStatus(events.EventRangeLeaseDropped)
			la.log.Debugf("Dropped lease [%s] %s after callback (%s)", la.l.String(), time.Since(start), instruction)
		}
	}
}

func (la *leaseAgent) stop() {
	la.cancel()
}

func (la *leaseAgent) runloop() {
	for {
		select {
		case instruction := <-la.updates:
			la.doSingleInstruction(la.ctx, instruction)
		case <-la.ctx.Done():
			return
		}
	}
}

func (la *leaseAgent) queueInstruction(instruction leaseInstruction) {
	la.once.Do(func() {
		go la.runloop()
	})
	la.updates <- instruction
}

func (la *leaseAgent) broadcastLeaseStatus(eventType events.EventType) {
	re := events.RangeEvent{
		Type:            eventType,
		RangeDescriptor: la.l.GetRangeDescriptor(),
	}
	select {
	case la.broadcast <- re:
		break
	default:
		la.log.Warningf("dropping lease status update %+v", re)
	}
}

func (lk *LeaseKeeper) newLeaseAgent(rd *rfpb.RangeDescriptor, r *replica.Replica) leaseAgent {
	ctx, cancel := context.WithCancel(context.TODO())
	return leaseAgent{
		log:       lk.log,
		l:         rangelease.New(lk.nodeHost, lk.log, lk.liveness, rd, r),
		ctx:       ctx,
		cancel:    cancel,
		quit:      make(chan struct{}),
		once:      &sync.Once{},
		updates:   make(chan leaseInstruction, 10),
		broadcast: lk.broadcast,
	}
}

func (lk *LeaseKeeper) watchLeases() {
	for {
		select {
		case info := <-lk.leaderUpdates:
			leader := info.LeaderID == info.ReplicaID && info.Term > 0
			shard := shardID(info.ShardID)

			lk.mu.Lock()
			open := lk.open[shard]
			lk.leaders[shard] = leader
			lk.mu.Unlock()

			laI, ok := lk.leases.Load(shard)
			if !ok {
				lk.log.Debugf("Shard %d has not been opened yet (ignoring leader update)", shard)
				continue
			}
			la := laI.(leaseAgent)
			action := Drop
			if open && leader {
				action = Acquire
			}
			la.queueInstruction(leaseInstruction{
				shard:  shard,
				reason: fmt.Sprintf("raft leader change = %t, open = %t", leader, open),
				action: action,
			})
		case <-lk.quitAll:
			lk.leases.Range(func(key, val any) bool {
				la := val.(leaseAgent)
				la.stop()
				return true // continue iterating
			})
			return
		case <-lk.nodeLivenessUpdates:
			lk.leases.Range(func(key, val any) bool {
				shard := key.(shardID)
				la := val.(leaseAgent)

				action := Drop
				lk.mu.Lock()
				if lk.open[shard] && lk.leaders[shard] {
					action = Acquire
				}
				lk.mu.Unlock()

				la.queueInstruction(leaseInstruction{
					shard:  shard,
					reason: "node liveness update",
					action: action,
				})
				return true // continue iterating
			})
		}
	}
}

func (lk *LeaseKeeper) isStopped() bool {
	select {
	case <-lk.quitAll:
		return true
	default:
		return false
	}
}

func (lk *LeaseKeeper) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	if lk.isStopped() {
		return
	}

	// Don't track ranges that have not been setup yet.
	if len(rd.GetReplicas()) == 0 {
		return
	}
	var shard shardID
	for _, rep := range rd.GetReplicas() {
		shard = shardID(rep.GetShardId())
		break
	}
	laI, _ := lk.leases.LoadOrStore(shard, lk.newLeaseAgent(rd, r))

	// When a range is added via AddRange(), the raft leader may already
	// have been chosen, meaning that `watchLeases` will not receive
	// additional callbacks that would trigger range lease acquisition. So
	// for newly added ranges, check if this node is the leader and trigger
	// lease acquisition here.
	lk.mu.Lock()
	lk.open[shard] = true
	leader := lk.leaders[shard]
	lk.mu.Unlock()

	action := Drop
	if leader {
		action = Acquire
	}

	la := laI.(leaseAgent)
	la.queueInstruction(leaseInstruction{
		shard:  shard,
		reason: "Add range",
		action: action,
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
	var shard shardID
	for _, rep := range rd.GetReplicas() {
		shard = shardID(rep.GetShardId())
		break
	}

	lk.mu.Lock()
	lk.open[shard] = false
	lk.mu.Unlock()

	if laI, ok := lk.leases.Load(shard); ok {
		la := laI.(leaseAgent)
		la.queueInstruction(leaseInstruction{
			shard:  shard,
			reason: "remove range",
			action: Drop,
		})
	}
}

func (lk *LeaseKeeper) LeaseCount() int64 {
	leaseCount := int64(0)
	lk.leases.Range(func(key, value any) bool {
		la := value.(leaseAgent)
		if la.l.Valid() {
			leaseCount += 1
		}
		return true
	})
	return leaseCount
}

func (lk *LeaseKeeper) HaveLease(shard uint64) bool {
	if lacI, ok := lk.leases.Load(shardID(shard)); ok {
		la := lacI.(leaseAgent)
		valid := la.l.Valid()

		lk.mu.Lock()
		leader := lk.leaders[shardID(shard)]
		open := lk.open[shardID(shard)]
		lk.mu.Unlock()

		shouldHaveLease := leader && open
		if shouldHaveLease && !valid {
			lk.log.Errorf("HaveLease range: %d valid: %t, should have lease: %t", shard, valid, shouldHaveLease)
			la.queueInstruction(leaseInstruction{
				shard:  shardID(shard),
				reason: "should have range",
				action: Acquire,
			})
		} else if !shouldHaveLease && valid {
			lk.log.Errorf("HaveLease range: %d valid: %t, should have lease: %t", shard, valid, shouldHaveLease)
			la.queueInstruction(leaseInstruction{
				shard:  shardID(shard),
				reason: "should not have range",
				action: Drop,
			})
		}
		return valid
	}
	return false
}
