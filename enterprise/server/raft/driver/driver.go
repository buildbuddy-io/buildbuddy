package driver

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
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
	enableDriver            = flag.Bool("cache.raft.enable_driver", false, "If true, enable placement driver")
	enableMovingReplicas    = flag.Bool("cache.raft.enable_moving_replicas", true, "If set, allow moving replicas between nodes")
	enableReplacingReplicas = flag.Bool("cache.raft.enable_replacing_replicas", true, "If set, allow replacing dead / down replicas")
	driverPollInterval      = flag.Duration("cache.raft.driver_poll_interval", 10*time.Second, "Poll the cluster for moves/replacements this often")
	driverStartupDelay      = flag.Duration("cache.raft.driver_startup_delay", 1*time.Minute, "Don't allow driver to propose any changes until this window has passed")
	deadReplicaTimeout      = flag.Duration("cache.raft.dead_replica_timeout", 5*time.Minute, "After this time, consider a node dead")
)

const (
	// A node must have this * idealReplicaCount number of replicas to be
	// eligible for moving a replica to another node.
	replicaMoveThreshold = 1.05

	// If a node has more QPS than it's share * this ratio, leases may
	// be moved to other nodes to more evenly balance load.
	leaseMoveQPSThreshold = 1.05
)

// make a replica struct that can be used as a map key because protos cannot be.
type replicaStruct struct {
	shardID   uint64
	replicaID uint64
	nhid      string
}

type moveInstruction struct {
	from    string
	to      string
	replica replicaStruct
}

type uint64Set map[uint64]struct{}

func (s uint64Set) Add(id uint64) {
	s[id] = struct{}{}
}
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
	mu *sync.RWMutex

	// a map of nhid -> *StoreUsage
	lastUsage map[string]*rfpb.StoreUsage

	// a map of nhid -> last seen time. Cleared when
	// a node is seen, set when it disappears.
	leaveTime map[string]time.Time
}

func NewClusterMap() *clusterMap {
	return &clusterMap{
		mu: &sync.RWMutex{},

		lastUsage: make(map[string]*rfpb.StoreUsage, 0),
		leaveTime: make(map[string]time.Time, 0),
	}
}

func (cm *clusterMap) String() string {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	nodeStrings := make(map[string]string, 0)
	nhids := make([]string, 0, len(cm.lastUsage))
	for nhid, lastUsage := range cm.lastUsage {
		nhids = append(nhids, nhid)
		deadOrAliveString := ""
		if leftAt, ok := cm.leaveTime[nhid]; ok {
			if time.Since(leftAt) > *deadReplicaTimeout {
				deadOrAliveString = "(dead)"
			}
		}
		nodeStrings[nhid] = fmt.Sprintf("%+v %s", lastUsage, deadOrAliveString)
	}
	sort.Strings(nhids)
	buf := ""
	for _, nhid := range nhids {
		buf += fmt.Sprintf("\t%s\n", nodeStrings[nhid])
	}
	return buf
}

func (d *Driver) LookupNodehost(nhid string) *rfpb.NodeDescriptor {
	cm := d.clusterMap

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if lastUsage, ok := cm.lastUsage[nhid]; ok {
		return lastUsage.GetNode()
	}
	return nil
}

func (d *Driver) LookupNodehostForReplica(rs replicaStruct) *rfpb.NodeDescriptor {
	if nhid, _, err := d.nodeRegistry.ResolveNHID(rs.shardID, rs.replicaID); err == nil {
		return d.LookupNodehost(nhid)
	}
	return nil
}

// DeadReplicas returns a slice of replicaStructs that:
//   - Are members of clusters this node manages
//   - Have not been seen in the last `timeout`
func (d *Driver) DeadReplicas(state *clusterState) replicaSet {
	cm := d.clusterMap

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	deadReplicas := make(replicaSet, 0)
	now := time.Now()

	for rep := range state.allReplicas {
		leftAt, ok := d.clusterMap.leaveTime[rep.nhid]
		if !ok {
			continue
		}
		if now.Sub(leftAt) > *deadReplicaTimeout {
			deadReplicas.Add(rep)
		}
	}

	return deadReplicas
}

func (cm *clusterMap) idealReplicaCount() float64 {
	numReplicas := int64(0)
	numNodes := 0
	for _, usage := range cm.lastUsage {
		numReplicas += usage.GetReplicaCount()
		numNodes += 1
	}

	return float64(numReplicas) / float64(numNodes)
}

func (cm *clusterMap) idealProposeQPS() float64 {
	totalProposeQPS := int64(0)
	numNodes := 0
	for _, usage := range cm.lastUsage {
		totalProposeQPS += usage.GetRaftProposeQps()
		numNodes += 1
	}

	return float64(totalProposeQPS) / float64(numNodes)
}

// MoveableReplicas returns a slice of replicaStructs that:
//   - Are members of clusters this node manages
//   - Are not located on this node
//   - If moved, would bring this node back within the ideal # of replicas
//     threshold.
func (d *Driver) MoveableReplicas(state *clusterState) replicaSet {
	cm := d.clusterMap

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	targetNumReplicas := cm.idealReplicaCount()
	moveThreshold := int(targetNumReplicas * replicaMoveThreshold)

	candidates := make(replicaSet, 0)
	for rep := range state.allReplicas {
		// Skip nodes on this machine, since we hold
		// the lease for them if we manage the range.
		if state.myNodes.Contains(rep.replicaID) {
			continue
		}

		lastUsage, ok := cm.lastUsage[rep.nhid]
		if !ok {
			log.Debugf("No last usage found for %q", rep.nhid)
			continue
		}

		numReplicas := lastUsage.GetReplicaCount()
		if numReplicas < int64(moveThreshold) {
			continue
		}

		candidates.Add(rep)
	}

	return candidates
}

// MoveableLeases returns a set of shardIDs that:
//   - Are clusters this node manages
//   - Have more QPS than avg
//   - Contribute to this node's above-average propose QPS
func (d *Driver) MoveableLeases(state *clusterState) uint64Set {
	cm := d.clusterMap

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	idealProposeQPS := cm.idealProposeQPS()
	lastUsage := cm.lastUsage[state.node.GetNhid()]

	if float64(lastUsage.GetRaftProposeQps()) <= idealProposeQPS*leaseMoveQPSThreshold {
		return nil
	}

	localProposeQPS := int64(0)
	for _, rangeReplica := range state.managedRanges {
		replicaUsage := rangeReplica.GetReplicaUsage()
		localProposeQPS += replicaUsage.GetRaftProposeQps()
	}

	avgQPSPerRange := int64(float64(localProposeQPS) / float64(len(state.managedRanges)))

	myClusters := make(uint64Set, 0)
	for shardID, rangeReplica := range state.managedRanges {
		replicaUsage := rangeReplica.GetReplicaUsage()
		if replicaUsage.GetRaftProposeQps() > avgQPSPerRange {
			myClusters.Add(shardID)
		}
	}
	return myClusters
}

func (cm *clusterMap) FindHome(state *clusterState, shardID uint64) []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	potentialHomes := make([]string, 0)
	ideal := int64(cm.idealReplicaCount())

	for nhid, lastUsage := range cm.lastUsage {
		if lastUsage == nil {
			log.Debugf("No last usage found for %q", nhid)
			continue
		}
		// Don't return this node.
		if state.node.GetNhid() == nhid {
			continue
		}
		numReplicas := lastUsage.GetReplicaCount()
		if numReplicas > ideal {
			continue
		}

		// Don't return any node that already contains another
		// tablet in this cluster.
		alreadyHostingCluster := false
		for rep := range state.allReplicas {
			if rep.shardID != shardID {
				continue
			}
			if rep.nhid == nhid {
				alreadyHostingCluster = true
				break
			}
		}
		if alreadyHostingCluster {
			continue
		}
		potentialHomes = append(potentialHomes, nhid)
	}
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
	samples := make([]float64, 0, len(cm.lastUsage))

	for nhid, usage := range cm.lastUsage {
		count := usage.GetReplicaCount()
		if offset, ok := moveOffsets[nhid]; ok {
			count += int64(offset)
		}
		samples = append(samples, float64(count))
	}

	vari := variance(samples, cm.idealReplicaCount())
	return vari
}

type Driver struct {
	store         rfspb.ApiServer
	gossipManager *gossip.GossipManager
	nodeRegistry  registry.NodeRegistry
	mu            *sync.Mutex
	started       bool
	quit          chan struct{}
	clusterMap    *clusterMap
	startTime     time.Time
}

func New(store rfspb.ApiServer, gossipManager *gossip.GossipManager, nodeRegistry registry.NodeRegistry) *Driver {
	d := &Driver{
		store:         store,
		gossipManager: gossipManager,
		nodeRegistry:  nodeRegistry,
		mu:            &sync.Mutex{},
		started:       false,
		clusterMap:    NewClusterMap(),
		startTime:     time.Now(),
	}
	// Register the node registry as a gossip listener so that it receives
	// gossip callbacks.
	gossipManager.AddListener(d)
	statusz.AddSection("raft_driver", "Placement Driver", d)
	return d
}

func (d *Driver) Start() error {
	if !*enableDriver {
		log.Debugf("Driver disabled; not running")
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started {
		return nil
	}

	d.quit = make(chan struct{})
	go d.manageClustersLoop()

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
	close(d.quit)
	d.started = false
	log.Debugf("Driver stopped")
	return nil
}

// manageClustersLoop loops does not return; call it from a goroutine.
// It will loop forever calling "manageClusters" until the quit channel
// is closed by driver.Close().
func (d *Driver) manageClustersLoop() {
	for {
		select {
		case <-d.quit:
			return
		case <-time.After(*driverPollInterval):
			err := d.manageClusters()
			if err != nil {
				log.Errorf("Manage clusters error: %s", err)
			}
		}
	}
}

// OnEvent listens for other nodes' gossip events and handles them.
func (d *Driver) OnEvent(updateType serf.EventType, event serf.Event) {
	memberEvent, ok := event.(serf.MemberEvent)
	if !ok {
		return
	}
	for _, member := range memberEvent.Members {
		usageTag := member.Tags[constants.StoreUsageTag]
		if len(usageTag) == 0 {
			log.Debugf("member %q did not have usage tag yet", member.Name)
			continue
		}
		usageBuf, err := base64.StdEncoding.DecodeString(usageTag)
		if err != nil {
			log.Warningf("error b64 decoding usage tag: %s", err)
			continue
		}
		usage := &rfpb.StoreUsage{}
		if err := proto.Unmarshal(usageBuf, usage); err != nil {
			log.Warningf("error unmarshaling usage buf: %s", err)
			continue
		}
		nhid := usage.GetNode().GetNhid()
		d.clusterMap.lastUsage[nhid] = usage
		if member.Status == serf.StatusAlive {
			delete(d.clusterMap.leaveTime, nhid)
		} else {
			d.clusterMap.leaveTime[nhid] = time.Now()
		}
	}
}

type clusterState struct {
	node *rfpb.NodeDescriptor

	// Information about our own replicas.
	myNodes uint64Set

	// map of shardID -> RangeReplica
	managedRanges map[uint64]*rfpb.RangeReplica

	// set of all replicas we know about and their locations
	allReplicas replicaSet
}

func (cs *clusterState) GetRange(shardID uint64) *rfpb.RangeDescriptor {
	if r, ok := cs.managedRanges[shardID]; ok {
		return r.GetRange()
	}
	return nil
}

func (d *Driver) computeState(ctx context.Context) (*clusterState, error) {
	rsp, err := d.store.ListRange(ctx, &rfpb.ListRangeRequest{
		LeasedOnly: true,
	})
	if err != nil {
		return nil, err
	}
	state := &clusterState{
		node:    rsp.GetNode(),
		myNodes: make(uint64Set, len(rsp.GetRangeReplicas())),

		// shardID -> range
		managedRanges: make(map[uint64]*rfpb.RangeReplica, 0),

		// set of all replicas we've seen
		allReplicas: make(replicaSet, 0),
	}

	for _, rr := range rsp.GetRangeReplicas() {
		for i, replica := range rr.GetRange().GetReplicas() {
			if i == 0 {
				state.managedRanges[replica.GetShardId()] = rr
			}
			nhid, _, err := d.nodeRegistry.ResolveNHID(replica.GetShardId(), replica.GetReplicaId())
			if err != nil {
				log.Warningf("Error resolving NHID of c%dn%d: %s", replica.GetShardId(), replica.GetReplicaId(), err)
				continue
			}
			state.allReplicas.Add(replicaStruct{
				nhid:      nhid,
				shardID:   replica.GetShardId(),
				replicaID: replica.GetReplicaId(),
			})
		}
		state.myNodes.Add(rr.GetReplicaUsage().GetReplica().GetReplicaId())
	}
	return state, nil
}

type clusterChanges struct {
	// replace these
	deadReplicas replicaSet

	// maybe (if it results in a better fit score), move these
	moveableReplicas replicaSet

	// leases that we should consider transferring to another node
	moveableLeases uint64Set
}

func (d *Driver) proposeChanges(state *clusterState) *clusterChanges {
	changes := &clusterChanges{
		deadReplicas:     d.DeadReplicas(state),
		moveableReplicas: d.MoveableReplicas(state),
		moveableLeases:   d.MoveableLeases(state),
	}
	return changes
}

func (d *Driver) makeMoveInstructions(state *clusterState, replicas replicaSet) []moveInstruction {
	// Simulate some potential moves and apply them if they
	// result in a lower overall cluster score than our current score.
	// TODO(tylerw): use beam search or something better here..
	moves := make([]moveInstruction, 0)
	for rep := range replicas {
		currentLocation := rep.nhid
		potentialHomes := d.clusterMap.FindHome(state, rep.shardID)
		if len(potentialHomes) == 0 {
			log.Debugf("No possible homes found for cluster: %d", rep.shardID)
			continue
		}
		sort.Slice(potentialHomes, func(i, j int) bool {
			iMove := moveInstruction{
				to:   potentialHomes[i],
				from: currentLocation,
			}
			jMove := moveInstruction{
				to:   potentialHomes[j],
				from: currentLocation,
			}
			// Sort in *descending* order of size.
			return d.clusterMap.FitScore(append(moves, iMove)...) < d.clusterMap.FitScore(append(moves, jMove)...)
		})
		moves = append(moves, moveInstruction{
			to:      potentialHomes[0],
			from:    currentLocation,
			replica: rep,
		})
	}
	return moves
}

func (d *Driver) applyMove(ctx context.Context, move moveInstruction, state *clusterState) error {
	toNode := d.LookupNodehost(move.to)
	if toNode == nil {
		return status.FailedPreconditionErrorf("toNode %q not found", move.to)
	}
	rd := state.GetRange(move.replica.shardID)
	if rd == nil {
		return status.FailedPreconditionErrorf("rd %+v not found", rd)
	}
	rsp, err := d.store.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node:  toNode,
	})
	if err != nil {
		log.Errorf("AddReplica err: %s", err)
		// if the move failed, don't try the remove
		return err
	}
	log.Printf("Added %q to range: %+v", toNode, rd)

	// apply the remove, and if it succeeds, remove the node
	// from the clusterMap to avoid spuriously doing this again.
	_, err = d.store.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     rsp.GetRange(),
		ReplicaId: move.replica.replicaID,
	})
	if err == nil {
		log.Printf("Removed %q from range: %+v", toNode, rsp.GetRange())
	}
	return err
}

// modifyCluster applies `changes` to the cluster when possible.
func (d *Driver) modifyCluster(ctx context.Context, state *clusterState, changes *clusterChanges) error {
	if *enableReplacingReplicas {
		requiredMoves := d.makeMoveInstructions(state, changes.deadReplicas)
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
		optionalMoves := d.makeMoveInstructions(state, changes.moveableReplicas)
		currentFitScore := d.clusterMap.FitScore()
		for _, move := range optionalMoves {
			if d.clusterMap.FitScore(move) >= currentFitScore {
				continue
			}
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
		*state = *s
	}
	return err
}

func (d *Driver) manageClusters() error {
	ctx := context.Background()
	state, err := d.computeState(ctx)
	if err != nil {
		return err
	}
	if len(state.managedRanges) == 0 {
		// If we don't manage any clusters, exit now.
		return nil
	}
	log.Printf("state: %+v", state)
	log.Printf("cluster map:\n%s", d.clusterMap.String())

	if time.Since(d.startTime) < *driverStartupDelay {
		log.Debugf("not making changes yet; still in startup period")
		return nil
	}
	changes := d.proposeChanges(state)
	s := len(changes.deadReplicas) + len(changes.moveableReplicas)
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
	if len(state.managedRanges) == 0 {
		buf += "no managed ranges</pre>"
		return buf
	}
	buf += fmt.Sprintf("state: %+v\n", state)
	buf += fmt.Sprintf("cluster map:\n%s\n", d.clusterMap.String())
	changes := d.proposeChanges(state)

	if len(changes.deadReplicas) > 0 {
		buf += "Dead Replicas:\n"
		for rep := range changes.deadReplicas {
			buf += fmt.Sprintf("\tc%dn%d\n", rep.shardID, rep.replicaID)
		}
	}
	if len(changes.moveableReplicas) > 0 {
		buf += "Moveable Replicas:\n"
		for rep := range changes.moveableReplicas {
			buf += fmt.Sprintf("\tc%dn%d\n", rep.shardID, rep.replicaID)
		}
	}
	buf += "</pre>"
	return buf
}
