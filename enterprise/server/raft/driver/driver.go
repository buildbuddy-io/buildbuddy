package driver

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// If a replica has not been seen for longer than this, a new replica
	// should be started on a new node.
	replicaTimeoutDuration = 5 * time.Minute

	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to receive ranges moved from other nodes.
	maximumDiskCapacity = .95

	// A node must have this * idealRangeCount number of ranges to be
	// eligible for moving ranges to other nodes.
	rangeMoveThreshold = 1.05

	// Poll every this often to check if ranges need to move, etc.
	driverPollPeriod = 5 * time.Second

	// Max pending operations. No more than this many operations will be
	// enqueued at a time, others will be dropped until these have been
	// completed.
	maxPendingOperations = 100
)

type replicaStruct struct {
	clusterID uint64
	nodeID    uint64
}

type Driver struct {
	lastSeen       map[replicaStruct]time.Time
	nodeHost       *dragonboat.NodeHost
	apiClient      *client.APIClient
	gossipManager  *gossip.GossipManager
	sender         *sender.Sender
	registry       *registry.DynamicNodeRegistry
	leaseTracker   LeaseTracker
	pendingOps     map[string]struct{}
	operationQueue chan *rfpb.Operation
	quitChan       chan struct{}
}

type LeaseTracker interface {
	GetRangeLease(clusterID uint64) *rangelease.Lease
}

func New(nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, apiClient *client.APIClient, registry *registry.DynamicNodeRegistry, leaseTracker LeaseTracker) *Driver {
	d := &Driver{
		lastSeen:       make(map[replicaStruct]time.Time),
		nodeHost:       nodeHost,
		apiClient:      apiClient,
		gossipManager:  gossipManager,
		sender:         sender,
		registry:       registry,
		leaseTracker:   leaseTracker,
		pendingOps:     make(map[string]struct{}),
		operationQueue: make(chan *rfpb.Operation, maxPendingOperations),
		quitChan:       make(chan struct{}),
	}
	d.gossipManager.AddListener(d)
	go func() {
		time.Sleep(10 * time.Second)
		d.pollUntilQuit()
	}()
	return d
}

func (d *Driver) Stop() {
	close(d.quitChan)
}

func (d *Driver) pollUntilQuit() {
	for {
		select {
		case <-d.quitChan:
			break
		case <-time.After(driverPollPeriod):
			if err := d.loopOnce(); err != nil {
				log.Warningf("driver poll error: %s", err)
			}
			d.drainOps()
		}
	}
}

func (d *Driver) drainOps() {
	for {
		select {
		case <-d.quitChan:
			break
		case op := <-d.operationQueue:
			if err := d.handleOp(op); err != nil {
				log.Warningf("error handling op: %+v %s", op, err)
			}
		default:
			return
		}
	}
}

func (d *Driver) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		d.handlePlacementDriverQuery(query)
	default:
		break
	}
}

func (d *Driver) handlePlacementDriverQuery(query *serf.Query) {
	if query.Name != constants.PlacementDriverQueryEvent {
		return
	}
	if query.Payload == nil {
		return
	}
	pq := &rfpb.PlacementQuery{}
	if err := proto.Unmarshal(query.Payload, pq); err != nil {
		return
	}
	nodeHostInfo := d.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo != nil {
		for _, logInfo := range nodeHostInfo.LogInfo {
			if pq.GetTargetClusterId() == logInfo.ClusterID {
				return
			}
		}
	}
	buf, err := proto.Marshal(d.sender.MyNodeDescriptor())
	if err != nil {
		return
	}
	if err := query.Respond(buf); err != nil {
		log.Warningf("Error responding to gossip query: %s", err)
	}
}

func getUsage(m serf.Member) (*rfpb.NodeUsage, error) {
	buf, ok := m.Tags[constants.NodeUsageTag]
	if !ok {
		return nil, status.FailedPreconditionErrorf("NodeUsage tag not set for member: %q", m.Name)
	}
	usage := &rfpb.NodeUsage{}
	if err := proto.UnmarshalText(buf, usage); err != nil {
		return nil, err
	}
	return usage, nil
}

func (d *Driver) handleOp(op *rfpb.Operation) error {
	defer func() {
		delete(d.pendingOps, proto.CompactTextString(op))
	}()
	if !d.holdsValidRangeLease(op.GetClusterId()) {
		log.Debugf("Skipping op: %+v, we no longer hold the range lease for this cluster.", op)
		return nil
	}

	log.Printf("handleOp: %+v", op)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if op.GetOpType() == rfpb.Operation_ADD_NODE {
		return d.addNodeToCluster(ctx, op.GetClusterId())
	}
	// to prevent thundering herd: a node should be able to *reject* requests to take
	// on a range. IF a request gets rejected, we'll just drop it for now.
	// Ensure that we only remove a range if there are more than 3.
	return nil
}

func (d *Driver) holdsValidRangeLease(clusterID uint64) bool {
	rl := d.leaseTracker.GetRangeLease(clusterID)
	if rl == nil {
		return false
	}
	return rl.Valid()
}

func (d *Driver) loopOnce() error {
	nodeHostInfo := d.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo == nil {
		return nil
	}

	myUsage, err := getUsage(d.gossipManager.LocalMember())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), driverPollPeriod)
	defer cancel()

	// A list of potential operations to apply. (we'll filter out dupes below)
	potentialOps := make([]*rfpb.Operation, 0)
	ourLogInfos := make([]raftio.NodeInfo, 0, len(nodeHostInfo.LogInfo))
	for _, logInfo := range nodeHostInfo.LogInfo {
		// If we are not the holder of the rangelease for this cluster, ignore it for now.
		if !d.holdsValidRangeLease(logInfo.ClusterID) {
			continue
		}
		ourLogInfos = append(ourLogInfos, logInfo)
	}
	if len(ourLogInfos) == 0 {
		return nil
	}

	for _, logInfo := range ourLogInfos {
		// Check all replicas, if any are down for more than 5 minutes, bring
		// up a new replica, delete the old one
		membership, err := d.nodeHost.GetClusterMembership(ctx, logInfo.ClusterID)
		if err != nil {
			return err
		}
		for nodeID, _ := range membership.Nodes {
			d.lastSeen[replicaStruct{logInfo.ClusterID, nodeID}] = time.Now()
		}
	}

	for replicaStruct, lastSeen := range d.lastSeen {
		if time.Now().Sub(lastSeen) > replicaTimeoutDuration {
			log.Printf("The following replica needs replacement: (cluster: %d, node: %d)", replicaStruct.clusterID, replicaStruct.nodeID)
			potentialOps = append(potentialOps, &rfpb.Operation{
				OpType:    rfpb.Operation_ADD_NODE,
				ClusterId: replicaStruct.clusterID,
			})
			potentialOps = append(potentialOps, &rfpb.Operation{
				OpType:    rfpb.Operation_REMOVE_NODE,
				ClusterId: replicaStruct.clusterID,
				NodeId:    replicaStruct.nodeID,
			})
		}
	}

	// Check if another node would be better off owning any of our replicas
	members := d.gossipManager.Members()
	numRanges := int64(0)
	for _, member := range members {
		u, err := getUsage(member)
		if err != nil {
			log.Debugf("Error getting usage for %+v: %s", member, err)
			continue
		}
		numRanges += u.GetNumRanges()
	}

	idealRangeCount := int64(float64(numRanges) / float64(len(members)))
	myRangeCount := myUsage.GetNumRanges()
	overage := int(myRangeCount - idealRangeCount)

	// if a store has > (idealReplicaCount * rangeMoveThreshold): it can
	// move, otherwise no. if the move doesn't make things better, don't do
	// it.
	if myRangeCount > idealRangeCount {
		log.Printf("This node has %d too many ranges: would move some to other nodes.", overage)
	}

	log.Printf("Ideal range count: %d, my range count: %d", idealRangeCount, myRangeCount)
	log.Printf("My usage: %2.2f", float64(myUsage.GetDiskBytesUsed())/float64(myUsage.GetDiskBytesTotal()))
	for i := 0; i < overage; i++ {
		// don't run off the end of the slice
		if i == len(ourLogInfos) {
			break
		}
		logInfo := ourLogInfos[i]
		potentialOps = append(potentialOps, &rfpb.Operation{
			OpType:    rfpb.Operation_ADD_NODE,
			ClusterId: logInfo.ClusterID,
		})
		potentialOps = append(potentialOps, &rfpb.Operation{
			OpType:    rfpb.Operation_REMOVE_NODE,
			ClusterId: logInfo.ClusterID,
			NodeId:    logInfo.NodeID,
		})
	}

	for _, op := range potentialOps {
		opKey := proto.CompactTextString(op)
		_, ok := d.pendingOps[opKey]
		if ok {
			// don't repeat pending ops.
			continue
		}

		// write the op to operationQueue and mark it as pending.
		select {
		case d.operationQueue <- op:
			d.pendingOps[opKey] = struct{}{}
		default:
			log.Warningf("Dropping ops: op channel is full up.")
		}
	}
	return nil
}

func (d *Driver) reserveNodeIDs(ctx context.Context, n int) ([]uint64, error) {
	newVal, err := d.sender.Increment(ctx, constants.LastNodeIDKey, uint64(n))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, newVal-uint64(i))
	}
	return ids, nil
}

func (d *Driver) addNodeToCluster(ctx context.Context, clusterID uint64) error {
	rl := d.leaseTracker.GetRangeLease(clusterID)
	if rl == nil {
		return status.FailedPreconditionErrorf("rangelease not held for cluster: %d", clusterID)
	}

	// first, reserve a new node ID
	nodeIDs, err := d.reserveNodeIDs(ctx, 1)
	if err != nil {
		return err
	}
	nodeID := nodeIDs[0]

	// then, get the config change index from nodehost locally
	membership, err := d.nodeHost.SyncGetClusterMembership(ctx, clusterID)
	if err != nil {
		return err
	}
	configChangeID := membership.ConfigChangeID

	// then, use gossip to look for a new node to join this cluster
	buf, err := proto.Marshal(&rfpb.PlacementQuery{
		TargetClusterId: clusterID,
		// TODO(tylerw): in future, this would contain constraints
		// read from a zoneconfig.
	})
	if err != nil {
		return err
	}
	rsp, err := d.gossipManager.Query(constants.PlacementDriverQueryEvent, buf, nil)
	if err != nil {
		return err
	}
	node := &rfpb.NodeDescriptor{}
	for nodeRsp := range rsp.ResponseCh() {
		if nodeRsp.Payload == nil {
			continue
		}
		err := proto.Unmarshal(nodeRsp.Payload, node)
		if err == nil {
			break
		}
	}
	if node.GetNhid() == "" {
		return status.FailedPreconditionError("No candidate nodes available")
	}
	log.Printf("Got nodeDescriptor: %+v", node)

	// use gossip to tell other clusters where they can find this node
	// later on.
	d.registry.AddWithAddr(clusterID, nodeID, node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// propose the config change (this adds the node)
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		err := d.nodeHost.SyncRequestAddNode(ctx, clusterID, nodeID, node.GetNhid(), configChangeID)
		if err != nil {
			if dragonboat.IsTempError(err) {
				continue
			}
			return err
		}
		break
	}

	// finally, start the cluster on the new nodehost.
	c, err := d.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return err
	}
	_, err = c.StartCluster(ctx, &rfpb.StartClusterRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
		Join:      true,
	})
	return err
}
