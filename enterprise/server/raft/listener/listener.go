package listener

import (
	"flag"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/lni/dragonboat/v4/raftio"
)

var (
	leaderUpdatedChanSize = flag.Int64("cache.raft.leader_updated_chan_size", 200, "The length of the leader updated channel")
)

type RaftListener struct {
	mu sync.Mutex

	lastLeaderInfo        *raftio.LeaderInfo
	leaderChangeListeners map[string]chan raftio.LeaderInfo
}

func NewRaftListener() *RaftListener {
	return &RaftListener{
		mu: sync.Mutex{},

		leaderChangeListeners: make(map[string]chan raftio.LeaderInfo),
	}
}

func (rl *RaftListener) RemoveLeaderChangeListener(id string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	ch := rl.leaderChangeListeners[id]
	close(ch)
	delete(rl.leaderChangeListeners, id)
}

// AddLeaderChangeListener returns a channel where updates to raftio.LeaderInfo
// will be published when callbacks are received by the library.
// Listeners *must not block* or they risk dropping updates.
func (rl *RaftListener) AddLeaderChangeListener(id string) <-chan raftio.LeaderInfo {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	ch := make(chan raftio.LeaderInfo, *leaderUpdatedChanSize)
	rl.leaderChangeListeners[id] = ch
	if rl.lastLeaderInfo != nil {
		ch <- *rl.lastLeaderInfo
	}

	return ch
}

// LeaderUpdated implements IRaftEventListener interface and is called by the
// raft library when the leader is updated. It passes the updated leader info
// to leaderChangeListeners.
func (rl *RaftListener) LeaderUpdated(info raftio.LeaderInfo) {
	log.Debugf("leader updated: %+v", info)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.lastLeaderInfo = &info

	for id, ch := range rl.leaderChangeListeners {
		select {
		case ch <- info:
		default:
			log.Warningf("dropping message for %s", id)
		}
	}
}

func (rl *RaftListener) NodeReady(info raftio.NodeInfo)                   {}
func (rl *RaftListener) NodeHostShuttingDown()                            {}
func (rl *RaftListener) NodeUnloaded(info raftio.NodeInfo)                {}
func (rl *RaftListener) NodeDeleted(info raftio.NodeInfo)                 {}
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
