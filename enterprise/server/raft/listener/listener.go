package listener

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	defaultListener = NewRaftListener()
)

type LeaderCB func(info raftio.LeaderInfo)
type NodeCB func(info raftio.NodeInfo)
type ConnCB func(info raftio.ConnectionInfo)
type SnapshotCB func(info raftio.SnapshotInfo)
type EntryCB func(info raftio.EntryInfo)

func DefaultListener() *RaftListener {
	return defaultListener
}

type RaftListener struct {
	mu                     sync.Mutex
	nodeReadyCallbacks     map[*NodeCB]struct{}
	leaderUpdatedCallbacks map[*LeaderCB]struct{}
}

func NewRaftListener() *RaftListener {
	return &RaftListener{
		mu:                     sync.Mutex{},
		nodeReadyCallbacks:     make(map[*NodeCB]struct{}),
		leaderUpdatedCallbacks: make(map[*LeaderCB]struct{}),
	}
}

func (rl *RaftListener) LeaderUpdated(info raftio.LeaderInfo) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for cbp := range rl.leaderUpdatedCallbacks {
		cb := *cbp
		cb(info)
	}
}

func (rl *RaftListener) NodeReady(info raftio.NodeInfo) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for cbp := range rl.nodeReadyCallbacks {
		cb := *cbp
		cb(info)
	}
}

func (rl *RaftListener) NodeHostShuttingDown()                            {}
func (rl *RaftListener) NodeUnloaded(info raftio.NodeInfo)                {}
func (rl *RaftListener) MembershipChanged(info raftio.NodeInfo)           {}
func (rl *RaftListener) ConnectionEstablished(info raftio.ConnectionInfo) {}
func (rl *RaftListener) ConnectionFailed(info raftio.ConnectionInfo)      {}
func (rl *RaftListener) SendSnapshotStarted(info raftio.SnapshotInfo)     {}
func (rl *RaftListener) SendSnapshotCompleted(info raftio.SnapshotInfo)   {}
func (rl *RaftListener) SendSnapshotAborted(info raftio.SnapshotInfo)     {}
func (rl *RaftListener) SnapshotReceived(info raftio.SnapshotInfo)        {}
func (rl *RaftListener) SnapshotRecovered(info raftio.SnapshotInfo)       {}
func (rl *RaftListener) SnapshotCreated(info raftio.SnapshotInfo)         {}
func (rl *RaftListener) SnapshotCompacted(info raftio.SnapshotInfo)       {}
func (rl *RaftListener) LogCompacted(info raftio.EntryInfo)               {}
func (rl *RaftListener) LogDBCompacted(info raftio.EntryInfo)             {}

func (rl *RaftListener) RegisterNodeReadyCB(cb *NodeCB) {
	rl.mu.Lock()
	rl.nodeReadyCallbacks[cb] = struct{}{}
	rl.mu.Unlock()
}

func (rl *RaftListener) UnregisterNodeReadyCB(cb *NodeCB) {
	rl.mu.Lock()
	delete(rl.nodeReadyCallbacks, cb)
	rl.mu.Unlock()
}

func (rl *RaftListener) RegisterLeaderUpdatedCB(cb *LeaderCB) {
	rl.mu.Lock()
	rl.leaderUpdatedCallbacks[cb] = struct{}{}
	rl.mu.Unlock()
}

func (rl *RaftListener) UnregisterLeaderUpdatedCB(cb *LeaderCB) {
	rl.mu.Lock()
	delete(rl.leaderUpdatedCallbacks, cb)
	rl.mu.Unlock()
}

func (rl *RaftListener) WaitForClusterReady(ctx context.Context, clusterID uint64) error {
	quitOnce := sync.Once{}
	quit := make(chan struct{})
	cb := NodeCB(func(info raftio.NodeInfo) {
		if info.ClusterID == clusterID {
			quitOnce.Do(func() {
				close(quit)
			})
		}
	})

	rl.RegisterNodeReadyCB(&cb)
	defer rl.UnregisterNodeReadyCB(&cb)

	start := time.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-quit:
		log.Printf("Cluster %d was ready after %s", clusterID, time.Since(start))
		return nil
	}
}
