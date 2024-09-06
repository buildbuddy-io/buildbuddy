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
	mu                    sync.Mutex
	log                   log.Logger
	lastLeaderInfo        *raftio.LeaderInfo
	leaderChangeListeners map[string]chan raftio.LeaderInfo
}

func NewRaftListener() *RaftListener {
	return &RaftListener{
		mu:                    sync.Mutex{},
		log:                   log.NamedSubLogger("RaftCallback"),
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
	rl.log.Debugf("LeaderUpdated: %+v", info)
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

func (rl *RaftListener) NodeReady(info raftio.NodeInfo) {
	rl.log.Debugf("NodeReady: %+v", info)
}
func (rl *RaftListener) NodeHostShuttingDown() {
	rl.log.Debugf("NodeHostShuttingDown")
}
func (rl *RaftListener) NodeUnloaded(info raftio.NodeInfo) {
	rl.log.Debugf("NodeUnloaded: %+v", info)
}
func (rl *RaftListener) NodeDeleted(info raftio.NodeInfo) {
	rl.log.Debugf("NodeDeleted: %+v", info)
}
func (rl *RaftListener) MembershipChanged(info raftio.NodeInfo) {
	rl.log.Debugf("MembershipChanged: %+v", info)
}
func (rl *RaftListener) ConnectionEstablished(info raftio.ConnectionInfo) {
	rl.log.Debugf("ConnectionEstablished: %+v", info)
}
func (rl *RaftListener) ConnectionFailed(info raftio.ConnectionInfo) {
	rl.log.Debugf("ConnectionFailed: %+v", info)
}
func (rl *RaftListener) SendSnapshotStarted(info raftio.SnapshotInfo) {
	rl.log.Debugf("SendSnapshotStarted: %+v", info)
}
func (rl *RaftListener) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	rl.log.Debugf("SendSnapshotCompleted: %+v", info)
}
func (rl *RaftListener) SendSnapshotAborted(info raftio.SnapshotInfo) {
	rl.log.Debugf("SendSnapshotAborted: %+v", info)
}
func (rl *RaftListener) SnapshotReceived(info raftio.SnapshotInfo) {
	rl.log.Debugf("SnapshotReceived: %+v", info)
}
func (rl *RaftListener) SnapshotRecovered(info raftio.SnapshotInfo) {
	rl.log.Debugf("SnapshotRecovered: %+v", info)
}
func (rl *RaftListener) SnapshotCreated(info raftio.SnapshotInfo) {
	rl.log.Debugf("SnapshotCreated: %+v", info)
}
func (rl *RaftListener) SnapshotCompacted(info raftio.SnapshotInfo) {
	rl.log.Debugf("SnapshotCompacted: %+v", info)
}
func (rl *RaftListener) LogCompacted(info raftio.EntryInfo) {
	rl.log.Debugf("LogCompacted: %+v", info)
}
func (rl *RaftListener) LogDBCompacted(info raftio.EntryInfo) {
	rl.log.Debugf("LogDBCompacted: %+v", info)
}
