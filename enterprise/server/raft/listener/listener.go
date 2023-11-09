package listener

import (
	"sync"

	"github.com/lni/dragonboat/v4/raftio"
)

type RaftListener struct {
	mu sync.Mutex

	lastLeaderInfo        *raftio.LeaderInfo
	leaderChangeListeners []chan raftio.LeaderInfo
}

func NewRaftListener() *RaftListener {
	return &RaftListener{
		mu: sync.Mutex{},

		leaderChangeListeners: make([]chan raftio.LeaderInfo, 0),
	}
}

// Returns a channel and associated close function. raftio.LeaderInfo updates
// will be published on the channel when callbacks are received by the library.
// Listeners *must not block* or they risk dropping updates.
func (rl *RaftListener) AddLeaderChangeListener() (<-chan raftio.LeaderInfo, func()) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	ch := make(chan raftio.LeaderInfo, 10)
	rl.leaderChangeListeners = append(rl.leaderChangeListeners, ch)
	closeFunc := func() {
		rl.mu.Lock()
		defer rl.mu.Unlock()
		for i, l := range rl.leaderChangeListeners {
			if l == ch {
				rl.leaderChangeListeners = append(rl.leaderChangeListeners[:i], rl.leaderChangeListeners[:i+1]...)
			}
		}
	}

	if rl.lastLeaderInfo != nil {
		ch <- *rl.lastLeaderInfo
	}

	return ch, closeFunc
}

func (rl *RaftListener) LeaderUpdated(info raftio.LeaderInfo) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.lastLeaderInfo = &info

	for _, ch := range rl.leaderChangeListeners {
		ch <- info
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
