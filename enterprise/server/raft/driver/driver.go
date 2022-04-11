package driver

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

const (
	// If a replica has not been seen for longer than this, a new replica
	// should be started on a new node.
	defaultReplicaTimeout = 5 * time.Minute

	// Broadcast this nodes set of active replicas after this period.
	defaultBroadcastPeriod = 30 * time.Second

	// Attempt to reconcile / manage clusters after this period.
	defaultManagePeriod = 60 * time.Second

	// Split replicas after they reach this size.
	defaultMaxReplicaSizeBytes = 1e9 // 1GB

	// A node must have this * idealReplicaCount number of replicas to be
	// eligible for moving a replica to another node.
	replicaMoveThreshold = 1.05
)

type Opts struct {
	// ReplicaTimeout is how long a replica must go-unseen before being
	// marked dead.
	ReplicaTimeout time.Duration

	// BroadcastPeriod is how often this node will broadcast its set of
	// active replicas.
	BroadcastPeriod time.Duration

	// ManagePeriod is how often to attempt to re-spawn, clean-up, or
	// split dead or oversize replicas / clusters.
	ManagePeriod time.Duration

	// The maximum size a replica may be before it's considered overloaded
	// and is split.
	MaxReplicaSizeBytes int64
}

// make a replica struct that can be used as a map key because protos cannot be.
type replicaStruct struct {
	clusterID uint64
	nodeID    uint64
}

type observation struct {
	seen      time.Time
	sizeBytes int64
}

type replicaSet map[replicaStruct]struct{}

type clusterMap struct {
	mu           *sync.RWMutex
	nodeReplicas map[string]replicaSet
	observations map[replicaStruct]observation
}

func NewClusterMap() *clusterMap {
	return &clusterMap{
		mu:           &sync.RWMutex{},
		nodeReplicas: make(map[string]replicaSet, 0),
		observations: make(map[replicaStruct]observation, 0),
	}
}

func (cm *clusterMap) ObserveNode(nhid string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	_, ok := cm.nodeReplicas[nhid]
	if !ok {
		cm.nodeReplicas[nhid] = make(replicaSet, 0)
	}
}

func (cm *clusterMap) ObserveReplica(nhid string, ru *rfpb.ReplicaUsage) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	r := ru.GetReplica()
	rs := replicaStruct{r.GetClusterId(), r.GetNodeId()}

	_, ok := cm.nodeReplicas[nhid]
	if !ok {
		cm.nodeReplicas[nhid] = make(replicaSet, 0)
	}

	cm.nodeReplicas[nhid][rs] = struct{}{}
	cm.observations[rs] = observation{
		seen:      time.Now(),
		sizeBytes: ru.GetEstimatedDiskBytesUsed(),
	}
}

func (cm *clusterMap) DeadReplicas(leasedClusterIDs []uint64, timeout time.Duration) []replicaStruct {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	clusterSet := make(map[uint64]struct{}, len(leasedClusterIDs))
	for _, clusterID := range leasedClusterIDs {
		clusterSet[clusterID] = struct{}{}
	}

	dead := make([]replicaStruct, 0)
	now := time.Now()
	for rs, observation := range cm.observations {
		if _, ok := clusterSet[rs.clusterID]; !ok {
			// Skip clusters that this node does not hold the lease
			// for.
			continue
		}
		if now.Sub(observation.seen) > timeout {
			dead = append(dead, rs)
		}
	}
	return dead
}

func (cm *clusterMap) OverloadedReplicas(leasedClusterIDs []uint64, maxSizeBytes int64) []replicaStruct {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	clusterSet := make(map[uint64]struct{}, len(leasedClusterIDs))
	for _, clusterID := range leasedClusterIDs {
		clusterSet[clusterID] = struct{}{}
	}

	overloaded := make([]replicaStruct, 0)
	for rs, observation := range cm.observations {
		if _, ok := clusterSet[rs.clusterID]; !ok {
			// Skip clusters that this node does not hold the lease
			// for.
			continue
		}
		if observation.sizeBytes > maxSizeBytes {
			overloaded = append(overloaded, rs)
		}
	}
	return overloaded
}

func (cm *clusterMap) idealReplicaCount() float64 {
	totalNumReplicas := len(cm.observations)
	numNodes := len(cm.nodeReplicas)

	log.Printf("totalNumReplicas: %d, numNodes: %d", totalNumReplicas, numNodes)
	return float64(totalNumReplicas) / float64(numNodes)
}

// used for sorting only
type replicaSize struct {
	replicaStruct replicaStruct
	sizeBytes     int64
}

func (cm *clusterMap) ExcessReplicas(leasedClusterIDs []uint64, nhid string) []replicaStruct {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	ideal := cm.idealReplicaCount()
	numReplicas := len(cm.nodeReplicas[nhid])
	if numReplicas <= int(ideal*replicaMoveThreshold) {
		return nil
	}

	excessReplicaQuota := numReplicas - int(ideal)
	clusterSet := make(map[uint64]struct{}, len(leasedClusterIDs))
	for _, clusterID := range leasedClusterIDs {
		clusterSet[clusterID] = struct{}{}
	}

	replicaSizes := make([]replicaSize, 0)
	for rs, obs := range cm.observations {
		if _, ok := clusterSet[rs.clusterID]; ok {
			// Skip replicas whose leader resides on this node.
			continue
		}
		replicaSizes = append(replicaSizes, replicaSize{
			replicaStruct: rs,
			sizeBytes:     obs.sizeBytes,
		})
	}
	sort.Slice(replicaSizes, func(i, j int) bool {
		// Sort in *descending* order of size.
		return replicaSizes[i].sizeBytes > replicaSizes[j].sizeBytes
	})
	excess := make([]replicaStruct, 0, excessReplicaQuota)
	for _, replicaSize := range replicaSizes {
		if len(excess) == excessReplicaQuota {
			break
		}
		excess = append(excess, replicaSize.replicaStruct)
	}
	return excess
}

func DefaultOpts() Opts {
	return Opts{
		ReplicaTimeout:      defaultReplicaTimeout,
		BroadcastPeriod:     defaultBroadcastPeriod,
		ManagePeriod:        defaultManagePeriod,
		MaxReplicaSizeBytes: defaultMaxReplicaSizeBytes,
	}
}

func TestingOpts() Opts {
	return Opts{
		ReplicaTimeout:      5 * time.Second,
		BroadcastPeriod:     2 * time.Second,
		ManagePeriod:        5 * time.Second,
		MaxReplicaSizeBytes: 10 * 1e6, // 10MB
	}
}

type Driver struct {
	opts          Opts
	store         rfspb.ApiServer
	gossipManager *gossip.GossipManager
	mu            *sync.Mutex
	started       bool
	broadcastQuit chan struct{}
	manageQuit    chan struct{}
	clusterMap    *clusterMap
	numSamples    int64
}

func New(store rfspb.ApiServer, gossipManager *gossip.GossipManager, opts Opts) *Driver {
	d := &Driver{
		opts:          opts,
		store:         store,
		gossipManager: gossipManager,
		mu:            &sync.Mutex{},
		started:       false,
		clusterMap:    NewClusterMap(),
	}
	// Register the node registry as a gossip listener so that it receives
	// gossip callbacks.
	gossipManager.AddListener(d)
	return d
}

func (d *Driver) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started {
		return nil
	}
	d.broadcastQuit = make(chan struct{})
	go d.broadcastLoop()

	d.manageQuit = make(chan struct{})
	go d.manageLoop()

	d.started = true
	log.Debugf("Driver started")
	return nil
}

func (d *Driver) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.started {
		return nil
	}
	close(d.broadcastQuit)
	close(d.manageQuit)
	d.started = false
	log.Debugf("Driver stopped")
	return nil
}

// broadcastLoop does not return; call it from a goroutine.
func (d *Driver) broadcastLoop() {
	for {
		select {
		case <-d.broadcastQuit:
			return
		case <-time.After(d.opts.BroadcastPeriod):
			err := d.broadcast()
			if err != nil {
				log.Errorf("Broadcast error: %s", err)
			}
		}
	}
}

// manageLoop does not return; call it from a goroutine.
func (d *Driver) manageLoop() {
	for {
		select {
		case <-d.manageQuit:
			return
		case <-time.After(d.opts.ManagePeriod):
			err := d.manageClusters()
			if err != nil {
				log.Errorf("Manage error: %s", err)
			}
		}
	}
}

func (d *Driver) broadcast() error {
	ctx := context.Background()
	rsp, err := d.store.ListCluster(ctx, &rfpb.ListClusterRequest{})
	if err != nil {
		return err
	}

	// Need to be very careful about what is broadcast here because the max
	// allowed UserEvent size is 9K, and broadcasting too much could cause
	// slow rebalancing etc.

	// A max ReplicaUsage should be around 8 bytes * 3 = 24 bytes. So 375
	// replica usages should fit in a single gossip message. Use 350 as the
	// target size so there is room for the NHID and a small margin of
	// safety.
	batchSize := 350
	numReplicas := len(rsp.GetRangeReplicas())
	gossiped := false
	for start := 0; start < numReplicas; start += batchSize {
		nu := &rfpb.NodeUsage{Nhid: rsp.GetNode().GetNhid()}
		end := start + batchSize
		if end > numReplicas {
			end = numReplicas
		}
		for _, rr := range rsp.GetRangeReplicas()[start:end] {
			nu.ReplicaUsage = append(nu.ReplicaUsage, rr.GetReplicaUsage())
		}
		buf, err := proto.Marshal(nu)
		if err != nil {
			return err
		}
		if err := d.gossipManager.SendUserEvent(constants.NodeUsageEvent, buf, false /*=coalesce*/); err != nil {
			return err
		}
		gossiped = true
	}

	// If a node does not yet have any replicas, the loop above will not
	// have gossiped anything. In that case, gossip an "empty" usage event
	// now.
	if !gossiped {
		nu := &rfpb.NodeUsage{Nhid: rsp.GetNode().GetNhid()}
		buf, err := proto.Marshal(nu)
		if err != nil {
			return err
		}
		if err := d.gossipManager.SendUserEvent(constants.NodeUsageEvent, buf, false /*=coalesce*/); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventUser:
		userEvent, _ := event.(serf.UserEvent)
		d.handleEvent(&userEvent)
	default:
		break
	}
}

func (d *Driver) handleEvent(event *serf.UserEvent) {
	if event.Name != constants.NodeUsageEvent {
		return
	}
	nu := &rfpb.NodeUsage{}
	if err := proto.Unmarshal(event.Payload, nu); err != nil {
		return
	}
	if nu.GetNhid() == "" {
		log.Warningf("Ignoring malformed driver node usage: %+v", nu)
		return
	}
	d.clusterMap.ObserveNode(nu.GetNhid())
	for _, ru := range nu.GetReplicaUsage() {
		d.clusterMap.ObserveReplica(nu.GetNhid(), ru)
	}
	atomic.AddInt64(&d.numSamples, 1)
}

func (d *Driver) manageClusters() error {
	if ns := atomic.LoadInt64(&d.numSamples); ns < 5 {
		log.Debugf("Not managing cluster yet, sample count: %d", ns)
		return nil
	}
	ctx := context.Background()
	rsp, err := d.store.ListCluster(ctx, &rfpb.ListClusterRequest{
		LeasedOnly: true,
	})
	if err != nil {
		return err
	}

	leasedClusterIDs := make([]uint64, 0, len(rsp.GetRangeReplicas()))
	for _, rr := range rsp.GetRangeReplicas() {
		for _, replica := range rr.GetRange().GetReplicas() {
			leasedClusterIDs = append(leasedClusterIDs, replica.GetClusterId())
			break
		}
	}
	deadReplicas := d.clusterMap.DeadReplicas(leasedClusterIDs, d.opts.ReplicaTimeout)
	log.Printf("Dead replicas: %+v, my clusters: %+v", deadReplicas, leasedClusterIDs)
	// AddClusterNode()

	overloadedReplicas := d.clusterMap.OverloadedReplicas(leasedClusterIDs, d.opts.MaxReplicaSizeBytes)
	log.Printf("Overloaded replicas: %+v, my clusters: %+v", overloadedReplicas, leasedClusterIDs)
	// SplitCluster()

	excessReplicas := d.clusterMap.ExcessReplicas(leasedClusterIDs, rsp.GetNode().GetNhid())
	log.Printf("Excess replicas: %+v, my clusters: %+v", excessReplicas, leasedClusterIDs)
	// AddClusterNode && RemoveClusterNode

	return nil
}
