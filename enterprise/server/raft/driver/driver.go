package driver

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

var (
	enableSplittingReplicas = flag.Bool("cache.raft.enable_splitting_replicas", true, "If set, allow splitting oversize replicas")
	enableMovingReplicas    = flag.Bool("cache.raft.enable_moving_replicas", false, "If set, allow moving replicas between nodes")
	enableReplacingReplicas = flag.Bool("cache.raft.enable_replacing_replicas", false, "If set, allow replacing dead / down replicas")
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

type moveInstruction struct {
	from    string
	to      string
	replica replicaStruct
}

type uint64Set map[uint64]struct{}

func (s uint64Set) Contains(id uint64) bool {
	_, ok := s[id]
	return ok
}

type replicaSet map[replicaStruct]struct{}

func (rs replicaSet) Add(r replicaStruct) {
	rs[r] = struct{}{}
}
func (rs replicaSet) Remove(r replicaStruct) {
	delete(rs, r)
}
func (rs replicaSet) Contains(r replicaStruct) bool {
	_, ok := rs[r]
	return ok
}
func (rs replicaSet) ContainsCluster(clusterID uint64) bool {
	for r := range rs {
		if r.clusterID == clusterID {
			return true
		}
	}
	return false
}

func variance(samples []float64, mean float64) float64 {
	if len(samples) <= 1 {
		return 0
	}
	var diffSquaredSum float64
	for _, s := range samples {
		diffSquaredSum += math.Pow(s-mean, 2)
	}
	return diffSquaredSum / float64(len(samples)-1)
}

type clusterMap struct {
	mu              *sync.RWMutex
	nodeDescriptors map[string]*rfpb.NodeDescriptor
	nodeReplicas    map[string]replicaSet
	observations    map[replicaStruct]observation
}

func NewClusterMap() *clusterMap {
	return &clusterMap{
		mu:              &sync.RWMutex{},
		nodeDescriptors: make(map[string]*rfpb.NodeDescriptor, 0),
		nodeReplicas:    make(map[string]replicaSet, 0),
		observations:    make(map[replicaStruct]observation, 0),
	}
}

func (cm *clusterMap) String() string {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	nodeStrings := make(map[string][]string, 0)
	nhids := make([]string, 0, len(cm.nodeReplicas))
	for nhid, set := range cm.nodeReplicas {
		nhids = append(nhids, nhid)
		replicas := make([]string, 0, len(set))
		for rs := range set {
			obs := cm.observations[rs]
			replicas = append(replicas, fmt.Sprintf("c%dn%d [%2.2f MB]", rs.clusterID, rs.nodeID, float64(obs.sizeBytes)/1e6))
		}
		sort.Strings(replicas)
		nodeStrings[nhid] = replicas
	}
	sort.Strings(nhids)
	buf := ""
	for _, nhid := range nhids {
		buf += fmt.Sprintf("\t%q: %s\n", nhid, strings.Join(nodeStrings[nhid], ", "))
	}
	return buf
}

func (cm *clusterMap) RemoveNode(nodeID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for _, set := range cm.nodeReplicas {
		for rs := range set {
			if rs.nodeID == nodeID {
				set.Remove(rs)
			}
		}
	}
	for rs, _ := range cm.observations {
		if rs.nodeID == nodeID {
			delete(cm.observations, rs)
		}
	}
}

func (cm *clusterMap) LookupNodehost(nhid string) *rfpb.NodeDescriptor {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.nodeDescriptors[nhid]
}

func (cm *clusterMap) LookupNodehostForReplica(rs replicaStruct) *rfpb.NodeDescriptor {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for nhid, replicas := range cm.nodeReplicas {
		if replicas.Contains(rs) {
			return cm.nodeDescriptors[nhid]
		}
	}
	return nil
}

func (cm *clusterMap) ObserveNode(node *rfpb.NodeDescriptor) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	_, ok := cm.nodeReplicas[node.GetNhid()]
	if !ok {
		cm.nodeReplicas[node.GetNhid()] = make(replicaSet, 0)
	}
	cm.nodeDescriptors[node.GetNhid()] = node
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

	cm.nodeReplicas[nhid].Add(rs)
	cm.observations[rs] = observation{
		seen:      time.Now(),
		sizeBytes: ru.GetEstimatedDiskBytesUsed(),
	}
}

// DeadReplicas returns a slice of replicaStructs that:
//   - Are members of clusters this node manages
//   - Have not been seen in the last `timeout`
func (cm *clusterMap) DeadReplicas(myClusters uint64Set, timeout time.Duration) replicaSet {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	dead := make(replicaSet, 0)
	now := time.Now()
	for rs, observation := range cm.observations {
		if !myClusters.Contains(rs.clusterID) {
			// Skip clusters that this node does not hold the lease
			// for.
			continue
		}
		if now.Sub(observation.seen) > timeout {
			dead.Add(rs)
		}
	}
	return dead
}

// OverloadedReplicas returns a slice of replicaStructs that:
//   - Are members of clusters this node manages
//   - Report sizes greater than `maxSizeBytes`
func (cm *clusterMap) OverloadedReplicas(myClusters uint64Set, maxSizeBytes int64) replicaSet {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	overloaded := make(replicaSet, 0)
	for rs, observation := range cm.observations {
		if !myClusters.Contains(rs.clusterID) {
			// Skip clusters that this node does not hold the lease
			// for.
			continue
		}
		if observation.sizeBytes > maxSizeBytes {
			overloaded.Add(rs)
		}
		// TODO(tylerw): also split if QPS > 1000.
	}
	return overloaded
}

func (cm *clusterMap) idealReplicaCount() float64 {
	totalNumReplicas := len(cm.observations)
	numNodes := len(cm.nodeReplicas)
	return float64(totalNumReplicas) / float64(numNodes)
}

// used for sorting only
type replicaSize struct {
	replicaStruct replicaStruct
	sizeBytes     int64
}

// MoveableReplicas returns a slice of replicaStructs that:
//   - Are members of clusters this node manages
//   - Are not located on this node
//   - If moved, would bring this node back within the ideal # of replicas
//     threshold.
func (cm *clusterMap) MoveableReplicas(myClusters, myNodes uint64Set, nhid string) replicaSet {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	ideal := cm.idealReplicaCount()
	numReplicas := len(cm.nodeReplicas[nhid])
	moveThreshold := int(ideal * replicaMoveThreshold)
	if numReplicas <= moveThreshold {
		return nil
	}
	excessReplicaQuota := numReplicas - int(ideal)

	replicaSizes := make([]replicaSize, 0)
	for rs, obs := range cm.observations {
		// Skip clusters not managed by us.
		if !myClusters.Contains(rs.clusterID) {
			continue
		}
		// Skip nodes on this machine.
		if myNodes.Contains(rs.nodeID) {
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
	misplaced := make(replicaSet, excessReplicaQuota)
	for _, replicaSize := range replicaSizes {
		if len(misplaced) == excessReplicaQuota {
			break
		}
		misplaced.Add(replicaSize.replicaStruct)
	}
	return misplaced
}

func (cm *clusterMap) FindHome(clusterID uint64) []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	potentialHomes := make([]string, 0)

	ideal := cm.idealReplicaCount()
	for nhid, replicaSet := range cm.nodeReplicas {
		numReplicas := len(replicaSet)
		if numReplicas > int(ideal) {
			continue
		}
		if replicaSet.ContainsCluster(clusterID) {
			continue
		}
		potentialHomes = append(potentialHomes, nhid)
	}

	// Sort the potential homes in order of *increasing* fullness.
	// This prefers moving replicas to the least loaded nodes first.
	sort.Slice(potentialHomes, func(i, j int) bool {
		iNHID := potentialHomes[i]
		jNHID := potentialHomes[j]
		return len(cm.nodeReplicas[iNHID]) < len(cm.nodeReplicas[jNHID])
	})
	return potentialHomes
}

func (cm *clusterMap) FitScore(potentialMoves ...moveInstruction) float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	moveOffsets := make(map[string]int, 0)
	for _, move := range potentialMoves {
		if move.to != "" {
			moveOffsets[move.to] += 1
		}
		if move.from != "" {
			moveOffsets[move.from] -= 1
		}
	}
	samples := make([]float64, 0, len(cm.nodeReplicas))
	for nhid, replicaSet := range cm.nodeReplicas {
		count := len(replicaSet)
		if offset, ok := moveOffsets[nhid]; ok {
			count += offset
		}
		samples = append(samples, float64(count))
	}

	vari := variance(samples, cm.idealReplicaCount())
	return vari
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
	statusz.AddSection("raft_driver", "Placement Driver", d)
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

// broadcasts a proto containing usage information about this store's
// replicas.
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
		nu := &rfpb.NodeUsage{Node: rsp.GetNode()}
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
		nu := &rfpb.NodeUsage{Node: rsp.GetNode()}
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

// OnEvent listens for other nodes' gossip events and handles them.
func (d *Driver) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventUser:
		userEvent, _ := event.(serf.UserEvent)
		d.handleEvent(&userEvent)
	default:
		break
	}
}

// handleEvent parses and ingests the data from other nodes' gossip events.
func (d *Driver) handleEvent(event *serf.UserEvent) {
	if event.Name != constants.NodeUsageEvent {
		return
	}
	nu := &rfpb.NodeUsage{}
	if err := proto.Unmarshal(event.Payload, nu); err != nil {
		return
	}
	if nu.GetNode() == nil {
		log.Warningf("Ignoring malformed driver node usage: %+v", nu)
		return
	}
	node := nu.GetNode()
	d.clusterMap.ObserveNode(node)
	for _, ru := range nu.GetReplicaUsage() {
		d.clusterMap.ObserveReplica(node.GetNhid(), ru)
	}
	atomic.AddInt64(&d.numSamples, 1)
}

type clusterState struct {
	node *rfpb.NodeDescriptor
	// Information about our own replicas.
	myClusters uint64Set
	myNodes    uint64Set

	// map of clusterID -> RangeDescriptor
	managedRanges map[uint64]*rfpb.RangeDescriptor
}

func (cs *clusterState) GetRange(clusterID uint64) *rfpb.RangeDescriptor {
	return cs.managedRanges[clusterID]
}

func (cs *clusterState) GetHeader(clusterID uint64) *rfpb.Header {
	rd := cs.GetRange(clusterID)
	header := &rfpb.Header{
		RangeId:    rd.GetRangeId(),
		Generation: rd.GetGeneration(),
	}
	for _, replica := range rd.GetReplicas() {
		if cs.myNodes.Contains(replica.GetNodeId()) {
			header.Replica = replica
			break
		}
	}
	return header
}

func (d *Driver) computeState(ctx context.Context) (*clusterState, error) {
	rsp, err := d.store.ListCluster(ctx, &rfpb.ListClusterRequest{
		LeasedOnly: true,
	})
	if err != nil {
		return nil, err
	}
	state := &clusterState{
		node:       rsp.GetNode(),
		myClusters: make(uint64Set, len(rsp.GetRangeReplicas())),
		myNodes:    make(uint64Set, len(rsp.GetRangeReplicas())),
		// clusterID -> range
		managedRanges: make(map[uint64]*rfpb.RangeDescriptor, 0),
	}

	for _, rr := range rsp.GetRangeReplicas() {
		for _, replica := range rr.GetRange().GetReplicas() {
			state.myClusters[replica.GetClusterId()] = struct{}{}
			state.managedRanges[replica.GetClusterId()] = rr.GetRange()
			break
		}
		state.myNodes[rr.GetReplicaUsage().GetReplica().GetNodeId()] = struct{}{}
	}
	return state, nil
}

type clusterChanges struct {
	// split these
	overloadedReplicas replicaSet

	// replace these
	deadReplicas replicaSet

	// maybe (if it results in a better fit score), move these
	moveableReplicas replicaSet
}

func (d *Driver) proposeChanges(state *clusterState) *clusterChanges {
	changes := &clusterChanges{
		overloadedReplicas: d.clusterMap.OverloadedReplicas(state.myClusters, d.opts.MaxReplicaSizeBytes),
		deadReplicas:       d.clusterMap.DeadReplicas(state.myClusters, d.opts.ReplicaTimeout),
		moveableReplicas:   d.clusterMap.MoveableReplicas(state.myClusters, state.myNodes, state.node.GetNhid()),
	}
	return changes
}

func (d *Driver) makeMoveInstructions(replicas replicaSet) []moveInstruction {
	// Simulate some potential moves and apply them if they
	// result in a lower overall cluster score than our current score.
	// TODO(tylerw): use beam search or something better here..
	moves := make([]moveInstruction, 0)
	for rs := range replicas {
		currentLocation := d.clusterMap.LookupNodehostForReplica(rs)
		potentialHomes := d.clusterMap.FindHome(rs.clusterID)
		if len(potentialHomes) == 0 {
			continue
		}
		sort.Slice(potentialHomes, func(i, j int) bool {
			iMove := moveInstruction{
				to:   potentialHomes[i],
				from: currentLocation.GetNhid(),
			}
			jMove := moveInstruction{
				to:   potentialHomes[j],
				from: currentLocation.GetNhid(),
			}
			// Sort in *descending* order of size.
			return d.clusterMap.FitScore(append(moves, iMove)...) < d.clusterMap.FitScore(append(moves, jMove)...)
		})
		moves = append(moves, moveInstruction{
			to:      potentialHomes[0],
			from:    currentLocation.GetNhid(),
			replica: rs,
		})
	}
	return moves
}

func (d *Driver) applyMove(ctx context.Context, move moveInstruction, state *clusterState) error {
	toNode := d.clusterMap.LookupNodehost(move.to)
	if toNode == nil {
		return status.FailedPreconditionErrorf("toNode %q not found", move.to)
	}
	rd := state.GetRange(move.replica.clusterID)
	if rd == nil {
		return status.FailedPreconditionErrorf("rd %+v not found", rd)
	}
	rsp, err := d.store.AddClusterNode(ctx, &rfpb.AddClusterNodeRequest{
		Range: rd,
		Node:  toNode,
	})
	if err != nil {
		log.Errorf("AddClusterNode err: %s", err)
		// if the move failed, don't try the remove
		return err
	}
	log.Printf("AddClusterNode succeeded")

	// apply the remove, and if it succeeds, remove the node
	// from the clusterMap to avoid spuriously doing this again.
	_, err = d.store.RemoveClusterNode(ctx, &rfpb.RemoveClusterNodeRequest{
		Range:  rsp.GetRange(),
		NodeId: move.replica.nodeID,
	})
	if err == nil {
		log.Printf("RemoveClusterNode succeeded")
		d.clusterMap.RemoveNode(move.replica.nodeID)
	}
	return err
}

// modifyCluster applies `changes` to the cluster when possible.
func (d *Driver) modifyCluster(ctx context.Context, state *clusterState, changes *clusterChanges) error {
	// Splits are going to happen before anything else.
	if *enableSplittingReplicas {
		overloadedClusters := make(map[uint64]struct{})
		for rs, _ := range changes.overloadedReplicas {
			overloadedClusters[rs.clusterID] = struct{}{}
		}
		for clusterID := range overloadedClusters {
			rd, ok := state.managedRanges[clusterID]
			if !ok {
				continue
			}
			_, err := d.store.SplitCluster(ctx, &rfpb.SplitClusterRequest{
				Header: state.GetHeader(clusterID),
				Range:  rd,
			})
			if err != nil {
				log.Warningf("Error splitting cluster: %s", err)
			} else {
				log.Infof("Successfully split %+v", rd)
			}
			time.Sleep(10 * time.Second)
			if err := d.updateState(ctx, state); err != nil {
				return err
			}
		}
	}

	if *enableReplacingReplicas {
		requiredMoves := d.makeMoveInstructions(changes.deadReplicas)
		for _, move := range requiredMoves {
			log.Printf("Making required move: %+v", move)
			if err := d.applyMove(ctx, move, state); err != nil {
				log.Warningf("Error applying move: %s", err)
			}
			time.Sleep(10 * time.Second)
			if err := d.updateState(ctx, state); err != nil {
				return err
			}
		}
	}
	if *enableMovingReplicas {
		optionalMoves := d.makeMoveInstructions(changes.moveableReplicas)
		currentFitScore := d.clusterMap.FitScore()
		for _, move := range optionalMoves {
			if d.clusterMap.FitScore(move) >= currentFitScore {
				continue
			}
			log.Printf("Making optional move: %+v", move)
			if err := d.applyMove(ctx, move, state); err != nil {
				log.Warningf("Error applying move: %s", err)
			}
			time.Sleep(10 * time.Second)
			if err := d.updateState(ctx, state); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Driver) updateState(ctx context.Context, state *clusterState) error {
	s, err := d.computeState(ctx)
	if err == nil {
		state = s
	}
	return err
}

func (d *Driver) manageClusters() error {
	if ns := atomic.LoadInt64(&d.numSamples); ns < 10 {
		return nil
	}
	ctx := context.Background()
	state, err := d.computeState(ctx)
	if err != nil {
		return err
	}
	if len(state.myClusters) == 0 {
		// If we don't manage any clusters, exit now.
		return nil
	}
	log.Printf("state: %+v", state)
	log.Printf("cluster map:\n%s", d.clusterMap.String())

	changes := d.proposeChanges(state)
	s := len(changes.overloadedReplicas) + len(changes.deadReplicas) + len(changes.moveableReplicas)
	if s == 0 {
		return nil
	}

	log.Printf("proposed changes: %+v", changes)
	return d.modifyCluster(ctx, state, changes)
}

func (d *Driver) Statusz(ctx context.Context) string {
	state, err := d.computeState(ctx)
	if err != nil {
		return fmt.Sprintf("Error computing state: %s", err)
	}
	buf := "<pre>"
	if len(state.myClusters) == 0 {
		buf += "no managed clusters</pre>"
		return buf
	}
	buf += fmt.Sprintf("state: %+v\n", state)
	buf += fmt.Sprintf("cluster map:\n%s\n", d.clusterMap.String())
	changes := d.proposeChanges(state)

	if len(changes.overloadedReplicas) > 0 {
		buf += "Overloaded Replicas:\n"
		for rep, _ := range changes.overloadedReplicas {
			buf += fmt.Sprintf("\t c%dn%d", rep.clusterID, rep.nodeID)
		}
	}
	if len(changes.deadReplicas) > 0 {
		buf += "Dead Replicas:\n"
		for rep, _ := range changes.deadReplicas {
			buf += fmt.Sprintf("\t c%dn%d", rep.clusterID, rep.nodeID)
		}
	}
	if len(changes.moveableReplicas) > 0 {
		buf += "Moveable Replicas:\n"
		for rep, _ := range changes.moveableReplicas {
			buf += fmt.Sprintf("\t c%dn%d", rep.clusterID, rep.nodeID)
		}
	}
	buf += "</pre>"
	return buf
}
