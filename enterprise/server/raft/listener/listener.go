package listener

import (
	"flag"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	leaderUpdatedChanSize = flag.Int64("cache.raft.leader_updated_chan_size", 10_000, "The length of the leader updated channel; Should be greather than the max number of ranges on a node.")
	nodeReadyChanSize     = flag.Int64("cache.raft.node_ready_chan_size", 10_000, "The length of the node ready channel")
	nodeUnloadedChanSize  = flag.Int64("cache.raft.node_unloaded_chan_size", 1000, "The length of the node unloaded channel")
)

type leaderUpdate struct {
	mu        sync.Mutex
	lastInfo  *raftio.LeaderInfo
	listeners map[string]chan raftio.LeaderInfo
}
type nodeUpdate struct {
	mu        sync.Mutex
	listeners map[string]chan raftio.NodeInfo
}
type RaftListener struct {
	log log.Logger

	leaderUpdate *leaderUpdate
	nodeReady    *nodeUpdate
	nodeUnloaded *nodeUpdate
}

func NewRaftListener() *RaftListener {
	return &RaftListener{
		leaderUpdate: &leaderUpdate{
			mu:        sync.Mutex{},
			listeners: make(map[string]chan raftio.LeaderInfo),
		},
		nodeReady: &nodeUpdate{
			mu:        sync.Mutex{},
			listeners: make(map[string]chan raftio.NodeInfo),
		},
		nodeUnloaded: &nodeUpdate{
			mu:        sync.Mutex{},
			listeners: make(map[string]chan raftio.NodeInfo),
		},
		log: log.NamedSubLogger("RaftCallback"),
	}
}

func (rl *RaftListener) RemoveLeaderChangeListener(id string) {
	rl.leaderUpdate.mu.Lock()
	defer rl.leaderUpdate.mu.Unlock()

	ch := rl.leaderUpdate.listeners[id]
	close(ch)
	delete(rl.leaderUpdate.listeners, id)
}

// AddLeaderChangeListener returns a channel where updates to raftio.LeaderInfo
// will be published when callbacks are received by the library.
// Listeners *must not block* or they risk dropping updates.
func (rl *RaftListener) AddLeaderChangeListener(id string) <-chan raftio.LeaderInfo {
	rl.leaderUpdate.mu.Lock()
	defer rl.leaderUpdate.mu.Unlock()

	ch := make(chan raftio.LeaderInfo, *leaderUpdatedChanSize)
	rl.leaderUpdate.listeners[id] = ch
	if rl.leaderUpdate.lastInfo != nil {
		ch <- *rl.leaderUpdate.lastInfo
	}

	return ch
}

// LeaderUpdated implements IRaftEventListener interface and is called by the
// raft library when the leader is updated. It passes the updated leader info
// to leaderChangeListeners.
func (rl *RaftListener) LeaderUpdated(info raftio.LeaderInfo) {
	rl.log.Debugf("LeaderUpdated: %+v", info)
	rl.leaderUpdate.mu.Lock()
	defer rl.leaderUpdate.mu.Unlock()
	rl.leaderUpdate.lastInfo = &info

	for id, ch := range rl.leaderUpdate.listeners {
		select {
		case ch <- info:
		default:
			log.Warningf("dropping leaderUpdate message for %s", id)
			metrics.RaftListenerEventsDropped.With(prometheus.Labels{
				metrics.RaftListenerID:        id,
				metrics.RaftListenerEventType: "LeaderUpdated",
			}).Inc()
		}
	}
}

func (nu *nodeUpdate) addListener(id string, chanSize int64) <-chan raftio.NodeInfo {
	nu.mu.Lock()
	defer nu.mu.Unlock()

	ch := make(chan raftio.NodeInfo, chanSize)
	nu.listeners[id] = ch
	return ch
}

func (nu *nodeUpdate) removeListener(id string) {
	nu.mu.Lock()
	defer nu.mu.Unlock()

	ch := nu.listeners[id]
	close(ch)
	delete(nu.listeners, id)
}

func (nu *nodeUpdate) handle(info raftio.NodeInfo, eventType string) {
	nu.mu.Lock()
	defer nu.mu.Unlock()

	for id, ch := range nu.listeners {
		select {
		case ch <- info:
		default:
			metrics.RaftListenerEventsDropped.With(prometheus.Labels{
				metrics.RaftListenerID:        id,
				metrics.RaftListenerEventType: eventType,
			}).Inc()
			log.Warningf("dropping %s message for %s", eventType, id)
		}
	}
}

func (rl *RaftListener) AddNodeReadyListener(id string) <-chan raftio.NodeInfo {
	return rl.nodeReady.addListener(id, *nodeReadyChanSize)
}

func (rl *RaftListener) RemoveNodeReadyListener(id string) {
	rl.nodeReady.removeListener(id)
}

func (rl *RaftListener) NodeReady(info raftio.NodeInfo) {
	rl.log.Debugf("NodeReady: %+v", info)
	rl.nodeReady.handle(info, "NodeReady")
}

func (rl *RaftListener) NodeHostShuttingDown() {
	rl.log.Debugf("NodeHostShuttingDown")
}

func (rl *RaftListener) AddNodeUnloadedListener(id string) <-chan raftio.NodeInfo {
	return rl.nodeUnloaded.addListener(id, *nodeUnloadedChanSize)
}

func (rl *RaftListener) RemoveNodeUnloadedListener(id string) {
	rl.nodeUnloaded.removeListener(id)
}

func (rl *RaftListener) NodeUnloaded(info raftio.NodeInfo) {
	rl.nodeReady.handle(info, "NodeUnloaded")
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
