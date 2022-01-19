package driver

import (
	"context"
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// If a replica has not been seen for longer than this, a new replica
	// should be started on a new node.
	defaultReplicaTimeoutDuration = 5 * time.Minute

	// A node must have this * idealReplicaCount number of replicas to be
	// eligible for moving a replica to another node.
	replicaMoveThreshold = 1.05

	// Poll every this often to check if ranges need to move, etc.
	driverPollPeriod = 5 * time.Second
)

type IPlacer interface {
	GetManagedClusters(ctx context.Context) []uint64
	GetRangeDescriptor(ctx context.Context, clusterID uint64) (*rfpb.RangeDescriptor, error)
	GetNodeUsage(ctx context.Context, replica *rfpb.ReplicaDescriptor) (*rfpb.NodeUsage, error)
	FindNodes(ctx context.Context, query *rfpb.PlacementQuery) ([]*rfpb.NodeDescriptor, error)
	SeenNodes() int64

	SplitRange(ctx context.Context, clusterID uint64) error
	AddNodeToCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, node *rfpb.NodeDescriptor) error
	RemoveNodeFromCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, targetNodeID uint64) error
}

type replicaStruct struct {
	clusterID uint64
	nodeID    uint64
}

func (rs replicaStruct) ToDescriptor() *rfpb.ReplicaDescriptor {
	return &rfpb.ReplicaDescriptor{
		ClusterId: rs.clusterID,
		NodeId:    rs.nodeID,
	}
}

func structFromDescriptor(rd *rfpb.ReplicaDescriptor) replicaStruct {
	return replicaStruct{
		clusterID: rd.GetClusterId(),
		nodeID:    rd.GetNodeId(),
	}
}

type Driver struct {
	lastSeen map[replicaStruct]time.Time
	placer   IPlacer
	quitChan chan struct{}
	opts     Opts
}

type Opts struct {
	ReplicaTimeoutDuration time.Duration
}

func DefaultOpts() Opts {
	return Opts{
		ReplicaTimeoutDuration: defaultReplicaTimeoutDuration,
	}
}

func New(placer IPlacer, opts Opts) *Driver {
	d := &Driver{
		lastSeen: make(map[replicaStruct]time.Time),
		placer:   placer,
		quitChan: make(chan struct{}),
		opts:     opts,
	}
	go func() {
		// wait for stabilization?
		time.Sleep(5 * time.Second)
		d.pollUntilQuit()
	}()
	return d
}


func (d *Driver) Stop() {
	close(d.quitChan)
}

func (d *Driver) pollUntilQuit() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		select {
		case <-d.quitChan:
			break
		case <-time.After(driverPollPeriod):
			if err := d.LoopOnce(ctx); err != nil {
				log.Warningf("driver error: %s", err)
			}
		}
	}
}

// to prevent thundering herd: a node should be able to *reject* requests to take
// on a range. IF a request gets rejected, we'll just drop it for now.
// Ensure that we only remove a range if there are more than 3.

func (d *Driver) moveReplica(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor, targetNode *rfpb.NodeDescriptor) error {
	if err := d.placer.AddNodeToCluster(ctx, rangeDescriptor, targetNode); err != nil {
		return err
	}
	log.Printf("Added %+v to cluster: %+v", targetNode, rangeDescriptor)
	if err := d.placer.RemoveNodeFromCluster(ctx, rangeDescriptor, replica.GetNodeId()); err != nil {
		return err
	}
	log.Printf("Removed node %d from cluster: %+v", replica.GetNodeId(), rangeDescriptor)
	return nil
}

func (d *Driver) LoopOnce(ctx context.Context) error {
	clustersManaged := d.placer.GetManagedClusters(ctx)
	if len(clustersManaged) == 0 {
		return nil
	}

	numReplicas := int64(0)
	replicas := make(map[string][]*rfpb.ReplicaDescriptor)
	usage := make(map[string]*rfpb.NodeUsage)
	for _, clusterID := range clustersManaged {
		rangeDescriptor, err := d.placer.GetRangeDescriptor(ctx, clusterID)
		if err != nil {
			return err
		}
		for _, replica := range rangeDescriptor.GetReplicas() {
			nodeUsage, err := d.placer.GetNodeUsage(ctx, replica)
			if err == nil {
				nhid := nodeUsage.GetNhid()
				d.lastSeen[structFromDescriptor(replica)] = time.Now()
				if _, ok := usage[nhid]; !ok {
					usage[nhid] = nodeUsage
				}
				replicas[nhid] = append(replicas[nhid], replica)
			}
		}
	}

	// Replace any dead replicas that are found.
	for _, replica := range d.findDeadReplicas() {
		rangeDescriptor, err := d.placer.GetRangeDescriptor(ctx, replica.GetClusterId())
		if err != nil {
			return err
		}
		placementQuery := &rfpb.PlacementQuery{TargetClusterId: replica.GetClusterId()}
		eligibleNodes, err := d.placer.FindNodes(ctx, placementQuery)
		if err != nil {
			return err
		}
		if len(eligibleNodes) == 0 {
			return status.FailedPreconditionErrorf("no eligible nodes to replace dead replica c%dn%d", replica.GetClusterId(), replica.GetNodeId())
		}
		return d.moveReplica(ctx, rangeDescriptor, replica, eligibleNodes[0])
	}

	for _, usage := range usage {
		numReplicas += usage.GetNumReplicas()
	}
	idealReplicaCount := float64(numReplicas) / float64(d.placer.SeenNodes())
	log.Printf("idealReplicaCount was %2.2f (num replicas: %d, total num nodes: %d)", idealReplicaCount, numReplicas, d.placer.SeenNodes())

	// Find the most overloaded nodeHost.
	var maxOverload float64
	var overloadedNhid string
	for nhid, usage := range usage {
		if float64(usage.GetNumReplicas()) > replicaMoveThreshold*idealReplicaCount {
			overload := float64(usage.GetNumReplicas()) - idealReplicaCount
			if overload > maxOverload {
				maxOverload = overload
				overloadedNhid = nhid
			}
		}
	}

	// If none were overloaded, exit early.
	if overloadedNhid == "" {
		return nil
	}

	// Compute the current "fit" score across all nodes we've seen.
	// We'll only move a replica around if it will improve the fit score.
	usages := copyUsageValues(usage)
	currentFitScore := d.globalFitScore(usages, idealReplicaCount)
	log.Printf("cur fit score: %2.2f", currentFitScore)

	replicaToMove := replicas[overloadedNhid][0]
	candidateNodes, err := d.placer.FindNodes(ctx, &rfpb.PlacementQuery{TargetClusterId: replicaToMove.GetClusterId()})
	if err != nil {
		return err
	}
	for _, candidate := range candidateNodes {
		candidateNhid := candidate.GetNhid()
		usages := copyUsageValues(usage)
		// if we don't have usage data for the candidate node, initialize to 0.
		if _, ok := usage[candidateNhid]; !ok {
			usages = append(usages, &rfpb.NodeUsage{
				Nhid: candidateNhid,
				NumReplicas: 0,
			})
		}
		// Simulate the move and ensure the fit score would improve.
		for _, u := range usages {
			if u.GetNhid() == overloadedNhid {
				u.NumReplicas -=1
			}
			if u.GetNhid() == candidateNhid {
				u.NumReplicas +=1
			}
		}
		newFitScore := d.globalFitScore(usages, idealReplicaCount)
		log.Printf("new fit score: %2.2f for move to %s", newFitScore, candidateNhid)
		if newFitScore < currentFitScore {
			log.Printf("new fit score: %2.2f is better than previous: %2.2f, executing move", newFitScore, currentFitScore)
			rangeDescriptor, err := d.placer.GetRangeDescriptor(ctx, replicaToMove.GetClusterId())
			if err != nil {
				return err
			}
			return d.moveReplica(ctx, rangeDescriptor, replicaToMove, candidate)
		}
	}
	return nil
}

func copyUsageValues(in map[string]*rfpb.NodeUsage) []*rfpb.NodeUsage {
	out := make([]*rfpb.NodeUsage, 0, len(in))
	for _, usage := range in {
		usageCopy := proto.Clone(usage).(*rfpb.NodeUsage)
		out = append(out, usageCopy)
	}
	return out
}

func (d *Driver) findDeadReplicas() []*rfpb.ReplicaDescriptor {
	deadReplicas := make([]*rfpb.ReplicaDescriptor, 0)
	for replicaStruct, lastSeen := range d.lastSeen {
		if time.Now().Sub(lastSeen) > d.opts.ReplicaTimeoutDuration {
			deadReplicas = append(deadReplicas, replicaStruct.ToDescriptor())
		}
	}
	return deadReplicas
}

func sttdev(samples []float64, mean float64) float64 {
	var diffSquaredSum float64
	for _, s := range samples {
		diffSquaredSum += math.Pow(s - mean, 2)
	}
	return math.Sqrt(diffSquaredSum/float64(len(samples)))
}

func variance(samples []float64, mean float64) float64 {
	if len(samples) <= 1 {
		return 0
	}
	var diffSquaredSum float64
	for _, s := range samples {
		diffSquaredSum += math.Pow(s - mean, 2)
	}
	return diffSquaredSum/float64(len(samples)-1)
}

func (d *Driver) globalFitScore(usages []*rfpb.NodeUsage, idealReplicaCount float64) float64 {
	samples := make([]float64, 0, len(usages))
	for _, nodeUsage := range usages {
		samples = append(samples, float64(nodeUsage.GetNumReplicas()))
	}

	dev := sttdev(samples, idealReplicaCount)
	vari := variance(samples, idealReplicaCount)
	log.Printf("variance: %2.2f, stdev: %2.2f, num usages: %d", dev, vari, len(usages))

	return vari
}
