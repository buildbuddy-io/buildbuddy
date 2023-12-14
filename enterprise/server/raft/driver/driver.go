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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/hashicorp/serf/serf"
	"golang.org/x/sync/errgroup"
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

type replicaSet map[string]*rfpb.ReplicaDescriptor

func (rs replicaSet) Add(nr *rfpb.ReplicaDescriptor)    { rs[rkey(nr)] = nr }
func (rs replicaSet) Remove(nr *rfpb.ReplicaDescriptor) { delete(rs, rkey(nr)) }
func (rs replicaSet) Get(nr *rfpb.ReplicaDescriptor) (*rfpb.ReplicaDescriptor, bool) {
	r, ok := rs[rkey(nr)]
	return r, ok
}
func NewReplicaSet() replicaSet {
	return make(map[string]*rfpb.ReplicaDescriptor)
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
	lastUsage map[string]timestampedUsage

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
	replicas replicaSet
}

type timestampedUsage struct {
	*rfpb.StoreUsage
	timestamp time.Time
}

func NewClusterMap() *ClusterMap {
	return &ClusterMap{
		mu: &sync.RWMutex{},

		lastUsage:    make(map[string]timestampedUsage),
		leaveTime:    make(map[string]time.Time),
		leasedRanges: make(map[uint64]*rfpb.RangeDescriptor),
		rangeUsage:   make(map[uint64]*rfpb.ReplicaUsage),
		replicas:     make(replicaSet),
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
		cm.lastUsage[nhid] = timestampedUsage{usage, time.Now()}
	} else {
		cm.leaveTime[nhid] = time.Now()
	}
	return nil
}

func (cm *ClusterMap) ObserveLocalReplicaUsage(usage *rfpb.ReplicaUsage, rd *rfpb.RangeDescriptor) error {
	if usage == nil {
		return status.FailedPreconditionError("nil range usage")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.rangeUsage[rd.GetRangeId()] = usage

	cm.replicas.Add(usage.GetReplica())

	return nil
}

func (cm *ClusterMap) OnRangeLeaseAcquired(rd *rfpb.RangeDescriptor) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.leasedRanges[rd.GetRangeId()] = rd
}

func (cm *ClusterMap) OnRangeLeaseDropped(rd *rfpb.RangeDescriptor) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.leasedRanges, rd.GetRangeId())
}

func (t timestampedUsage) String() string {
	buf := fmt.Sprintf("%36s | Replicas: %4d | Leases: %4d | QPS (R): %5d | (W): %5d | Size: %d MB | Age: %2.2f",
		t.GetNode().GetNhid(),
		t.GetReplicaCount(),
		t.GetLeaseCount(),
		t.GetReadQps(),
		t.GetRaftProposeQps(),
		t.GetTotalBytesUsed()/1e6,
		time.Since(t.timestamp).Seconds(),
	)
	return buf
}

func (cm *ClusterMap) String() string {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	nhids := make([]string, 0)
	for nhid := range cm.lastUsage {
		nhids = append(nhids, nhid)
	}
	sort.Strings(nhids)
	buf := ""
	for i, nhid := range nhids {
		buf += cm.lastUsage[nhid].String()
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
	gossipManager interfaces.GossipService
	updates       <-chan events.Event

	mu         *sync.Mutex
	ClusterMap *ClusterMap
	startTime  time.Time

	eg       *errgroup.Group
	egCancel context.CancelFunc
}

func New(store *store.Store, gossipManager interfaces.GossipService, updates <-chan events.Event) *Driver {
	d := &Driver{
		store:         store,
		gossipManager: gossipManager,
		updates:       updates,
		mu:            &sync.Mutex{},
		ClusterMap:    NewClusterMap(),
		startTime:     time.Now(),
	}
	// Register the driver as a gossip listener so that it receives
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

	ctx, cancelFunc := context.WithCancel(context.Background())
	d.egCancel = cancelFunc

	eg, gctx := errgroup.WithContext(ctx)
	d.eg = eg

	eg.Go(func() error {
		return d.handleEvents(gctx)
	})
	eg.Go(func() error {
		return d.manageClustersLoop(gctx)
	})

	log.Debugf("Driver started")
	return nil
}

func (d *Driver) Stop() error {
	now := time.Now()
	defer func() {
		log.Printf("Driver shutdown finished in %s", time.Since(now))
	}()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.egCancel != nil {
		d.egCancel()
		d.eg.Wait()
	}

	log.Debugf("Driver stopped")
	return nil
}

// manageClustersLoop loops does not return; call it from a goroutine.
// It will loop forever calling "manageClusters" until the quit channel
// is closed by driver.Close().
func (d *Driver) manageClustersLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*driverPollInterval):
			err := d.manageClusters()
			if err != nil {
				log.Errorf("Manage clusters error: %s", err)
			}
		}
	}
}

func (d *Driver) handleEvents(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-d.updates:
			d.processSingleEvent(e)
		}
	}
}

func (d *Driver) processSingleEvent(e events.Event) {
	switch e.EventType() {
	case events.EventRangeUsageUpdated:
		rangeUsageEvent, ok := e.(events.RangeUsageEvent)
		if !ok {
			return
		}
		d.ClusterMap.ObserveLocalReplicaUsage(rangeUsageEvent.ReplicaUsage, rangeUsageEvent.RangeDescriptor)
	case events.EventRangeLeaseAcquired:
		rangeEvent, ok := e.(events.RangeEvent)
		if !ok {
			return
		}
		d.ClusterMap.OnRangeLeaseAcquired(rangeEvent.RangeDescriptor)
	case events.EventRangeLeaseDropped:
		rangeEvent, ok := e.(events.RangeEvent)
		if !ok {
			return
		}
		d.ClusterMap.OnRangeLeaseDropped(rangeEvent.RangeDescriptor)
	default:
		return
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
