package placer

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to receive ranges moved from other nodes.
	maximumDiskCapacity = .95
)

type LeaseTracker interface {
	GetRangeLease(clusterID uint64) *rangelease.Lease
	GetRange(clusterID uint64) *rfpb.RangeDescriptor
}

type Placer struct {
	nodeHost      *dragonboat.NodeHost
	apiClient     *client.APIClient
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	registry      *registry.DynamicNodeRegistry
	leaseTracker  LeaseTracker
}

func New(nodeHost *dragonboat.NodeHost, apiClient *client.APIClient, gossipManager *gossip.GossipManager, sender *sender.Sender, registry *registry.DynamicNodeRegistry, leaseTracker LeaseTracker) *Placer {
	p := &Placer{
		nodeHost:      nodeHost,
		apiClient:     apiClient,
		gossipManager: gossipManager,
		sender:        sender,
		registry:      registry,
		leaseTracker:  leaseTracker,
	}
	gossipManager.AddListener(p)
	return p
}

func (p *Placer) holdsValidRangeLease(clusterID uint64) bool {
	rl := p.leaseTracker.GetRangeLease(clusterID)
	if rl == nil {
		return false
	}
	return rl.Valid()
}

func (p *Placer) OnEvent(updateType serf.EventType, event serf.Event) {
	if updateType != serf.EventQuery {
		return
	}

	query, ok := event.(*serf.Query)
	if !ok || query.Payload == nil {
		return
	}

	ctx, cancel := context.WithDeadline(context.Background(), query.Deadline())
	defer cancel()

	switch query.Name {
	case constants.PlacementDriverQueryEvent:
		p.handlePlacementQuery(ctx, query)
	}
}

func (p *Placer) handlePlacementQuery(ctx context.Context, query *serf.Query) {
	pq := &rfpb.PlacementQuery{}
	if err := proto.Unmarshal(query.Payload, pq); err != nil {
		return
	}
	nodeHostInfo := p.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo != nil {
		for _, logInfo := range nodeHostInfo.LogInfo {
			if pq.GetTargetClusterId() == logInfo.ClusterID {
				return
			}
		}
	}

	// Do not respond if this node is over 95% full.
	member := p.gossipManager.LocalMember()
	usageBuf, ok := member.Tags[constants.NodeUsageTag]
	if !ok {
		return
	}
	usage := &rfpb.NodeUsage{}
	if err := proto.UnmarshalText(usageBuf, usage); err != nil {
		return
	}
	myDiskUsage := float64(usage.GetDiskBytesUsed()) / float64(usage.GetDiskBytesTotal())
	if myDiskUsage > maximumDiskCapacity {
		return
	}

	nodeBuf, err := proto.Marshal(p.sender.MyNodeDescriptor())
	if err != nil {
		return
	}
	if err := query.Respond(nodeBuf); err != nil {
		log.Warningf("Error responding to gossip query: %s", err)
	}
}

func (p *Placer) GetManagedClusters(ctx context.Context) []uint64 {
	clusters := make([]uint64, 0)
	nodeHostInfo := p.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo != nil {
		for _, logInfo := range nodeHostInfo.LogInfo {
			rl := p.leaseTracker.GetRangeLease(logInfo.ClusterID)
			if rl != nil && rl.Valid() {
				clusters = append(clusters, logInfo.ClusterID)
			}
		}
	}
	return clusters
}

func (p *Placer) GetRangeDescriptor(ctx context.Context, clusterID uint64) (*rfpb.RangeDescriptor, error) {
	// could also look this up from the range?
	return p.leaseTracker.GetRange(clusterID), nil
}

func (p *Placer) GetNodeUsage(ctx context.Context, replica *rfpb.ReplicaDescriptor) (*rfpb.NodeUsage, error) {
	targetNHID, _, err := p.registry.ResolveNHID(replica.GetClusterId(), replica.GetNodeId())
	if err != nil {
		return nil, err
	}
	for _, member := range p.membersInState(serf.StatusAlive) {
		if member.Tags[constants.NodeHostIDTag] != targetNHID {
			continue
		}
		if buf, ok := member.Tags[constants.NodeUsageTag]; ok {
			usage := &rfpb.NodeUsage{}
			if err := proto.UnmarshalText(buf, usage); err != nil {
				return nil, err
			}
			return usage, nil
		}
	}
	return nil, status.NotFoundErrorf("Usage not found for c%dn%d", replica.GetClusterId(), replica.GetNodeId())
}

func (p *Placer) FindNodes(ctx context.Context, query *rfpb.PlacementQuery) ([]*rfpb.NodeDescriptor, error) {
	buf, err := proto.Marshal(query)
	if err != nil {
		return nil, err
	}
	rsp, err := p.gossipManager.Query(constants.PlacementDriverQueryEvent, buf, nil)
	if err != nil {
		return nil, err
	}

	foundNodes := make([]*rfpb.NodeDescriptor, 0)
	for nodeRsp := range rsp.ResponseCh() {
		if nodeRsp.Payload == nil {
			continue
		}
		node := &rfpb.NodeDescriptor{}
		if err := proto.Unmarshal(nodeRsp.Payload, node); err == nil {
			foundNodes = append(foundNodes, node)
		}
	}
	return foundNodes, nil
}

func (p *Placer) membersInState(s serf.MemberStatus) []serf.Member {
	members := make([]serf.Member, 0)
	for _, member := range p.gossipManager.Members() {
		if member.Status == s {
			members = append(members, member)
		}
	}
	return members
}

func (p *Placer) SeenNodes() int64 {
	return int64(len(p.membersInState(serf.StatusAlive)))
}

func (p *Placer) SplitRange(ctx context.Context, clusterID uint64) error {
	// TODO(tylerw): implement
	return nil
}

func (p *Placer) AddNodeToCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, node *rfpb.NodeDescriptor) error {
	if len(rangeDescriptor.GetReplicas()) == 0 {
		return status.FailedPreconditionErrorf("No replicas in range: %+v", rangeDescriptor)
	}
	clusterID := rangeDescriptor.GetReplicas()[0].GetClusterId()

	// Reserve a new node ID for the node about to be added.
	nodeIDs, err := p.reserveNodeIDs(ctx, 1)
	if err != nil {
		return err
	}
	nodeID := nodeIDs[0]

	// Get the config change index for this cluster.
	membership, err := p.nodeHost.SyncGetClusterMembership(ctx, clusterID)
	if err != nil {
		return err
	}
	configChangeID := membership.ConfigChangeID

	// Gossip the address of the node that is about to be added.
	p.registry.AddWithAddr(clusterID, nodeID, node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node to the raft cluster).
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		err := p.nodeHost.SyncRequestAddNode(ctx, clusterID, nodeID, node.GetNhid(), configChangeID)
		if err != nil {
			if dragonboat.IsTempError(err) {
				continue
			}
			return err
		}
		break
	}

	// Start the cluster on the newly added node.
	c, err := p.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return err
	}
	_, err = c.StartCluster(ctx, &rfpb.StartClusterRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
		Join:      true,
	})
	if err != nil {
		return err
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	return p.addReplicaToRangeDescriptor(ctx, clusterID, nodeID, rangeDescriptor)
}

func (p *Placer) RemoveNodeFromCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, targetNodeID uint64) error {
	var clusterID, nodeID uint64
	for _, replica := range rangeDescriptor.GetReplicas() {
		if replica.GetNodeId() == targetNodeID {
			clusterID = replica.GetClusterId()
			nodeID = replica.GetNodeId()
			break
		}
	}
	if clusterID == 0 && nodeID == 0 {
		return status.FailedPreconditionErrorf("No node with id %d found in range: %+v", targetNodeID, rangeDescriptor)
	}

	grpcAddr, _, err := p.registry.ResolveGRPC(clusterID, nodeID)
	if err != nil {
		return err
	}

	// Get the config change index for this cluster.
	membership, err := p.nodeHost.SyncGetClusterMembership(ctx, clusterID)
	if err != nil {
		return err
	}
	configChangeID := membership.ConfigChangeID

	// Propose the config change (this removes the node from the raft cluster).
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		err := p.nodeHost.SyncRequestDeleteNode(ctx, clusterID, nodeID, configChangeID)
		if err != nil {
			if dragonboat.IsTempError(err) {
				continue
			}
			return err
		}
		break
	}

	// Remove the data from the now stopped node.
	c, err := p.apiClient.Get(ctx, grpcAddr)
	if err != nil {
		return err
	}
	_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	if err != nil {
		return err
	}

	// Finally, update the range descriptor information to reflect the
	// new membership of this range without the removed node.
	return p.removeReplicaFromRangeDescriptor(ctx, clusterID, nodeID, rangeDescriptor)
}

func (p *Placer) reserveNodeIDs(ctx context.Context, n int) ([]uint64, error) {
	newVal, err := p.sender.Increment(ctx, constants.LastNodeIDKey, uint64(n))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, newVal-uint64(i))
	}
	return ids, nil
}

func (p *Placer) updateRangeDescriptor(ctx context.Context, clusterID uint64, old, new *rfpb.RangeDescriptor) error {
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return err
	}
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return err
	}
	rangeLocalBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}).ToProto()
	if err != nil {
		return err
	}

	metaRangeDescriptorKey := keys.RangeMetaKey(new.GetRight())
	metaRangeBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaRangeDescriptorKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}).ToProto()
	if err != nil {
		return err
	}

	// first update the range descriptor in the range itself
	rangeLocalRsp, err := client.SyncProposeLocal(ctx, p.nodeHost, clusterID, rangeLocalBatch)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(rangeLocalRsp).CASResponse(0)
	if err != nil {
		return err
	}

	// then update the metarange
	metaRangeRsp, err := p.sender.SyncPropose(ctx, metaRangeDescriptorKey, metaRangeBatch)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(metaRangeRsp).CASResponse(0)
	if err != nil {
		return err
	}
	return nil
}

func (p *Placer) addReplicaToRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) error {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	return p.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor)
}

func (p *Placer) removeReplicaFromRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) error {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Replicas {
		if replica.GetNodeId() == nodeID {
			newDescriptor.Replicas = append(newDescriptor.Replicas[:i], newDescriptor.Replicas[i+1:]...)
			break
		}
	}
	return p.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor)
}
