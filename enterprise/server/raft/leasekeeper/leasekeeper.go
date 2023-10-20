package leasekeeper

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type shardID uint64

type LeaseKeeper struct {
	nodeHost *dragonboat.NodeHost
	liveness *nodeliveness.Liveness
	listener *listener.RaftListener

	// map shardID -> leaseAndContext
	leases sync.Map

	mu      sync.Mutex
	leaders map[shardID]bool
	quitAll chan struct{}

	leaderUpdates       <-chan raftio.LeaderInfo
	cancelLeaderUpdates func()

	nodeLivenessUpdates       <-chan *rfpb.NodeLivenessRecord
	cancelNodeLivenessUpdates func()
}

func New(nodeHost *dragonboat.NodeHost, liveness *nodeliveness.Liveness, listener *listener.RaftListener) *LeaseKeeper {
	return &LeaseKeeper{
		nodeHost: nodeHost,
		liveness: liveness,
		listener: listener,
		mu:       sync.Mutex{},
		leases:   sync.Map{},
		leaders:  make(map[shardID]bool),
	}
}

func (lk *LeaseKeeper) Start() {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	lk.quitAll = make(chan struct{})
	lk.leaderUpdates, lk.cancelLeaderUpdates = lk.listener.AddLeaderChangeListener()
	lk.nodeLivenessUpdates, lk.cancelNodeLivenessUpdates = lk.liveness.AddListener()

	go lk.watchLeases()
}

func (lk *LeaseKeeper) Stop() {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	close(lk.quitAll)
	lk.cancelLeaderUpdates()
	lk.cancelNodeLivenessUpdates()
}

type leaseAndContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	l      *rangelease.Lease
}

func (lac *leaseAndContext) Update(leader bool) {
	if lac.cancel != nil {
		lac.cancel()
	}
	lac.ctx, lac.cancel = context.WithCancel(context.TODO())

	start := time.Now()
	if leader {
		if err := lac.l.Lease(lac.ctx); err != nil {
			log.Errorf("Error while updating rangelease (%s): %s", lac.l.String(), err)
			return
		}
		log.Debugf("Updated lease state [%s] %s after callback", lac.l.String(), time.Since(start))
	} else {
		// This is a no-op if we don't have the lease.
		if err := lac.l.Release(lac.ctx); err != nil {
			log.Errorf("Error dropping rangelease (%s): %s", lac.l, err)
		}
	}
}

func (lk *LeaseKeeper) watchLeases() {
	for {
		select {
		case info := <-lk.leaderUpdates:
			leader := info.LeaderID == info.ReplicaID && info.Term > 0
			lk.mu.Lock()
			lk.leaders[shardID(info.ShardID)] = leader
			lk.mu.Unlock()
			if lacI, ok := lk.leases.Load(shardID(info.ShardID)); ok {
				lac := lacI.(leaseAndContext)
				go lac.Update(leader)
			}
		case <-lk.quitAll:
			lk.leases.Range(func(key, val any) bool {
				lac := val.(leaseAndContext)
				go lac.Update( /*leader=*/ false)
				return true // continue iterating
			})
			return
		case <-lk.nodeLivenessUpdates:
			lk.leases.Range(func(key, val any) bool {
				lk.mu.Lock()
				leader := lk.leaders[key.(shardID)]
				lk.mu.Unlock()
				lac := val.(leaseAndContext)
				go lac.Update(leader)
				return true // continue iterating
			})
		}
	}
}

func (lk *LeaseKeeper) newLeaseAndContext(rd *rfpb.RangeDescriptor) leaseAndContext {
	return leaseAndContext{
		l: rangelease.New(lk.nodeHost, lk.liveness, rd),
	}
}

func (lk *LeaseKeeper) AddRange(rd *rfpb.RangeDescriptor) {
	// Don't track ranges that have not been setup yet.
	if len(rd.GetReplicas()) == 0 {
		return
	}
	var shard shardID
	for _, rep := range rd.GetReplicas() {
		shard = shardID(rep.GetShardId())
		break
	}
	lacI, alreadyExists := lk.leases.LoadOrStore(shard, lk.newLeaseAndContext(rd))
	if alreadyExists {
		log.Warningf("Lease for shard %d was already mapped", shard)
	}

	// When a range is added via AddRange(), the raft leader may already
	// have been chosen, meaning that `watchLeases` will not receive
	// additional callbacks that would trigger range lease acquisition. So
	// for newly added ranges, check if this node is the leader and trigger
	// lease acquisition here.
	lk.mu.Lock()
	leader := lk.leaders[shard]
	lk.mu.Unlock()

	lac := lacI.(leaseAndContext)
	go lac.Update(leader)
}

func (lk *LeaseKeeper) RemoveRange(rd *rfpb.RangeDescriptor) {
	// Don't track ranges that have not been setup yet.
	if len(rd.GetReplicas()) == 0 {
		return
	}
	var shard shardID
	for _, rep := range rd.GetReplicas() {
		shard = shardID(rep.GetShardId())
		break
	}
	lacI, ok := lk.leases.LoadAndDelete(shard)
	if ok {
		lac := lacI.(leaseAndContext)
		go lac.Update( /*leader=*/ false)
	}
}

func (lk *LeaseKeeper) LeaseCount() int64 {
	leaseCount := int64(0)
	lk.leases.Range(func(key, value any) bool {
		leaseCount += 1
		return true
	})
	return leaseCount
}

func (lk *LeaseKeeper) HaveLease(shard uint64) bool {
	if lacI, ok := lk.leases.Load(shardID(shard)); ok {
		lac := lacI.(leaseAndContext)
		return lac.l.Valid()
	}
	return false
}
