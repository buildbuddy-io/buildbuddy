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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
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
	enableDriver            = flag.Bool("cache.raft.enable_driver", true, "If true, enable placement driver")
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

func rkey(r *rfpb.ReplicaDescriptor) string {
	return fmt.Sprintf("s%05dr%05d", r.GetShardId(), r.GetReplicaId())
}

func nrkey(nr *rfpb.NodeReplica) string {
	return fmt.Sprintf("s%05dr%05d", nr.GetShardId(), nr.GetReplicaId())
}

type replicaSet map[string]*rfpb.NodeReplica

func (rs replicaSet) Add(nr *rfpb.NodeReplica)    { rs[nrkey(nr)] = nr }
func (rs replicaSet) Remove(nr *rfpb.NodeReplica) { delete(rs, nrkey(nr)) }
func (rs replicaSet) Get(nr *rfpb.NodeReplica) (*rfpb.NodeReplica, bool) {
	r, ok := rs[nrkey(nr)]
	return r, ok
}
func (rs replicaSet) List() []*rfpb.NodeReplica {
	values := make([]*rfpb.NodeReplica, 0, len(rs))
	for _, v := range rs {
		values = append(values, v)
	}
	return values
}
func NewReplicaSet() replicaSet {
	return make(map[string]*rfpb.NodeReplica)
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

type ClusterMap struct {
	mu *sync.RWMutex

	// a map of nhid -> *StoreUsage
	lastUsage map[string]*rfpb.StoreUsage

	// a map of nhid -> last seen time. Cleared when
	// a node is seen, set when it disappears.
	leaveTime map[string]time.Time

	// Each node keeps track of the ranges it holds the range lease for.
	// For each leased range it also tracks:
	//   - the range usage
	//   - the other nodes which hold replicas in this range

	// map of rangeID -> rangeDescriptor
	leasedRanges map[uint64]*rfpb.RangeDescriptor

	// map of rangeID -> replicaUsage (same usage for all replicas in range)
	rangeUsage map[uint64]*rfpb.ReplicaUsage

	// map of nhid -> replicas running on node
	replicas map[string]replicaSet
}

func NewClusterMap() *ClusterMap {
	return &ClusterMap{
		mu: &sync.RWMutex{},

		lastUsage:    make(map[string]*rfpb.StoreUsage),
		leaveTime:    make(map[string]time.Time),
		leasedRanges: make(map[uint64]*rfpb.RangeDescriptor),
		rangeUsage:   make(map[uint64]*rfpb.ReplicaUsage),
		replicas:     make(map[string]replicaSet),
	}
}

func (cm *ClusterMap) ObserveNode(nhid string, usage *rfpb.StoreUsage, nodeStatus serf.MemberStatus) error {
	if nhid == "" {
		return status.FailedPreconditionError("empty nodehost ID")
	}
	if usage == nil {
		return status.FailedPreconditionError("nil usage")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if nodeStatus == serf.StatusAlive {
		delete(cm.leaveTime, nhid)
		cm.lastUsage[nhid] = usage
	} else {
		cm.leaveTime[nhid] = time.Now()
	}
	return nil
}

func (cm *ClusterMap) ObserveLocalRangeUsage(usage *rfpb.RangeUsage) error {
	if usage == nil {
		return status.FailedPreconditionError("nil range usage")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.rangeUsage[usage.GetRange().GetRangeId()] = usage.GetReplicaUsage()

	for _, nr := range usage.GetNodeReplicas() {
		nhid := nr.GetNhid()
		if _, ok := cm.replicas[nhid]; !ok {
			cm.replicas[nhid] = NewReplicaSet()
		}
		cm.replicas[nhid].Add(nr)
	}

	return nil
}

func (cm *ClusterMap) OnRangeUsageUpdate(ru *rfpb.RangeUsage) {
	if err := cm.ObserveLocalRangeUsage(ru); err != nil {
		log.Errorf("Error observing local range usage: %s", err)
	}
}
func (cm *ClusterMap) OnRangeLeaseAcquired(rd *rfpb.RangeDescriptor) {
	cm.leasedRanges[rd.GetRangeId()] = rd
}
func (cm *ClusterMap) OnRangeLeaseDropped(rd *rfpb.RangeDescriptor) {
	delete(cm.leasedRanges, rd.GetRangeId())
}

type nodeStatus struct {
	su *rfpb.StoreUsage
}

func (n nodeStatus) String() string {
	buf := fmt.Sprintf("%36s | Replicas: %4d | Leases: %4d | QPS (R): %5d | (W): %5d | Size: %d MB",
		n.su.GetNode().GetNhid(),
		n.su.GetReplicaCount(),
		n.su.GetLeaseCount(),
		n.su.GetReadQps(),
		n.su.GetRaftProposeQps(),
		n.su.GetTotalBytesUsed()/1e6,
	)
	return buf
}

func (cm *ClusterMap) String() string {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	nodeStatuses := make(map[string]nodeStatus)
	nhids := make([]string, 0)
	for nhid, lastUsage := range cm.lastUsage {
		nhids = append(nhids, nhid)
		ns := nodeStatus{
			su: lastUsage,
		}
		nodeStatuses[nhid] = ns
	}
	sort.Strings(nhids)
	buf := ""
	for i, nhid := range nhids {
		nodeStatus := nodeStatuses[nhid]
		buf += nodeStatus.String()
		// if this is not the last one; add a newline.
		if i < len(nhids)-1 {
			buf += "\n"
		}
	}
	return buf
}

func (cm *ClusterMap) MeanReplicaCount() float64 {
	log.Printf("cm:\n%s", cm)
	numReplicas := int64(0)
	numNodes := 0
	for _, usage := range cm.lastUsage {
		numReplicas += usage.GetReplicaCount()
		numNodes += 1
	}

	return float64(numReplicas) / float64(numNodes)
}

func (cm *ClusterMap) MeanProposeQPS() float64 {
	log.Printf("cm:\n%s", cm)
	totalProposeQPS := int64(0)
	numNodes := 0
	for _, usage := range cm.lastUsage {
		totalProposeQPS += usage.GetRaftProposeQps()
		numNodes += 1
	}

	return float64(totalProposeQPS) / float64(numNodes)
}

type Driver struct {
	store         rfspb.ApiServer
	gossipManager *gossip.GossipManager
	mu            *sync.Mutex
	started       bool
	quit          chan struct{}
	ClusterMap    *ClusterMap
	startTime     time.Time
}

func New(store *store.Store, gossipManager *gossip.GossipManager) *Driver {
	d := &Driver{
		store:         store,
		gossipManager: gossipManager,
		mu:            &sync.Mutex{},
		started:       false,
		ClusterMap:    NewClusterMap(),
		startTime:     time.Now(),
	}
	// Register the driver as a gossip listener so that it receives
	// gossip callbacks.
	gossipManager.AddListener(d)
	store.AddRangeUsageListener(d.ClusterMap)
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
		if err := d.ClusterMap.ObserveNode(usage.GetNode().GetNhid(), usage, member.Status); err != nil {
			log.Errorf("Error observing cluster change: %s", err)
		}
	}
}

func (d *Driver) manageClusters() error {
	if time.Since(d.startTime) < *driverStartupDelay {
		log.Debugf("not making changes yet; still in startup period")
		return nil
	}
	// TODO(tylerw): do work here.
	return nil
}

func (d *Driver) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += d.ClusterMap.String()
	buf += "</pre>"
	// TODO(tylerw): list planned moves here.
	return buf
}
