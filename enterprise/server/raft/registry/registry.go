package registry

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v4/raftio"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbConfig "github.com/lni/dragonboat/v4/config"
)

var targetAddressUnknownErrorMsg = "target address unknown"

func targetAddressUnknownError(rangeID, replica uint64) error {
	return status.NotFoundErrorf("%s: c%dn%d", targetAddressUnknownErrorMsg, rangeID, replica)
}

type NodeRegistry interface {
	raftio.INodeRegistry

	// Used by store.go and sender.go.
	ResolveGRPC(rangeID uint64, replicaID uint64) (string, string, error)

	// Used by store.go only.
	AddNode(target, raftAddress, grpcAddress string)
	ResolveNHID(rangeID uint64, replicaID uint64) (string, string, error)
	String() string
}

// fixedPartitioner is the IPartitioner with fixed capacity and naive
// partitioning strategy.
type fixedPartitioner struct {
	capacity uint64
}

// GetPartitionID returns the partition ID for the specified raft cluster.
func (p *fixedPartitioner) GetPartitionID(rangeID uint64) uint64 {
	return rangeID % p.capacity
}

type connInfo struct {
	Target      string
	RaftAddress string
	GRPCAddress string
}

// StaticRegistry is used to manage all known node addresses in the multi raft
// system. The transport layer uses this address registry to locate nodes.
type StaticRegistry struct {
	partitioner *fixedPartitioner
	validate    dbConfig.TargetValidator

	nodeTargets sync.Map // map of raftio.NodeInfo => string
	targetRafts sync.Map // map of string => string
	targetGrpcs sync.Map // map of string => string
}

// NewStaticNodeRegistry returns a new StaticRegistry object.
func NewStaticNodeRegistry(streamConnections uint64, v dbConfig.TargetValidator) *StaticRegistry {
	n := &StaticRegistry{validate: v}
	if streamConnections > 1 {
		n.partitioner = &fixedPartitioner{capacity: streamConnections}
	}
	return n
}

// Add adds the specified node and its target info to the registry.
func (n *StaticRegistry) Add(rangeID uint64, replicaID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		log.Errorf("invalid target %s", target)
		return
	}
	key := raftio.GetNodeInfo(rangeID, replicaID)
	existingTarget, ok := n.nodeTargets.LoadOrStore(key, target)
	if ok {
		if existingTarget != target {
			log.Errorf("inconsistent target for c%dn%d, %s:%s", rangeID, replicaID, existingTarget, target)
		}
	} else {
		log.Debugf("added target for c%dn%d:%s", rangeID, replicaID, target)
	}
}

func (n *StaticRegistry) getConnectionKey(addr string, rangeID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	return fmt.Sprintf("%s-%d", addr, n.partitioner.GetPartitionID(rangeID))
}

// Remove removes a remote from the node registry.
func (n *StaticRegistry) Remove(rangeID uint64, replicaID uint64) {
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (n *StaticRegistry) RemoveShard(rangeID uint64) {
}

// ResolveRaft returns the raft address and the connection key of the specified node.
func (n *StaticRegistry) Resolve(rangeID uint64, replicaID uint64) (string, string, error) {
	return n.ResolveRaft(rangeID, replicaID)
}

// ResolveRaft returns the raft address and the connection key of the specified node.
func (n *StaticRegistry) ResolveRaft(rangeID uint64, replicaID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(rangeID, replicaID)
	if target, ok := n.nodeTargets.Load(key); ok {
		if r, ok := n.targetRafts.Load(target); ok {
			raftAddr := r.(string)
			return raftAddr, n.getConnectionKey(raftAddr, rangeID), nil
		}
	}
	return "", "", targetAddressUnknownError(rangeID, replicaID)
}

// ResolveGRPC returns the gRPC address and the connection key of the specified node.
func (n *StaticRegistry) ResolveGRPC(rangeID uint64, replicaID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(rangeID, replicaID)
	if target, ok := n.nodeTargets.Load(key); ok {
		if g, ok := n.targetGrpcs.Load(target); ok {
			grpcAddr := g.(string)
			return grpcAddr, n.getConnectionKey(grpcAddr, rangeID), nil
		}
	}
	return "", "", targetAddressUnknownError(rangeID, replicaID)
}

// ResolveNHID returns the NodeHost ID (NHID) and the connection key of the
// specified node.
func (n *StaticRegistry) ResolveNHID(rangeID uint64, replicaID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(rangeID, replicaID)
	if t, ok := n.nodeTargets.Load(key); ok {
		target := t.(string)
		return target, n.getConnectionKey(target, rangeID), nil
	}
	return "", "", targetAddressUnknownError(rangeID, replicaID)
}

// AddNode adds the raftAddress and grpcAddr for the specified target to the
// registry.
func (n *StaticRegistry) AddNode(target, raftAddress, grpcAddress string) {
	if n.validate != nil && !n.validate(target) {
		log.Errorf("invalid target %s", target)
		return
	}
	if raftAddress != "" {
		n.targetRafts.Store(target, raftAddress)
	}
	if grpcAddress != "" {
		n.targetGrpcs.Store(target, grpcAddress)
	}
}

func (n *StaticRegistry) Close() error {
	return nil
}

func (n *StaticRegistry) String() string {
	nodeHostClusters := make(map[string][]raftio.NodeInfo)
	n.nodeTargets.Range(func(k, v interface{}) bool {
		nh := v.(string)
		ni := k.(raftio.NodeInfo)
		nodeHostClusters[nh] = append(nodeHostClusters[nh], ni)
		return true
	})
	for _, clusters := range nodeHostClusters {
		sort.Slice(clusters, func(i, j int) bool {
			if clusters[i].ShardID == clusters[j].ShardID {
				return clusters[i].ReplicaID < clusters[j].ReplicaID
			}
			return clusters[i].ShardID < clusters[j].ShardID
		})
	}

	buf := "\nRegistry\n"
	for nodeHost, clusters := range nodeHostClusters {
		var raftAddr, grpcAddr string
		if r, ok := n.targetRafts.Load(nodeHost); ok {
			raftAddr = r.(string)
		}
		if g, ok := n.targetGrpcs.Load(nodeHost); ok {
			grpcAddr = g.(string)
		}
		buf += fmt.Sprintf("  Node: %q [raftAddr: %q, grpcAddr: %q]\n", nodeHost, raftAddr, grpcAddr)
		buf += fmt.Sprintf("   %+v\n", clusters)
	}
	return buf
}

// DynamicNodeRegistry is a node registry backed by gossip. It is capable of
// supporting NodeHosts with dynamic RaftAddress values.
type DynamicNodeRegistry struct {
	gossipManager interfaces.GossipService
	sReg          *StaticRegistry
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. It will create a single
// DynamicNodeRegistry and use it to resolve all other raft nodes until the
// process shuts down.
func NewDynamicNodeRegistry(gossipManager interfaces.GossipService, streamConnections uint64, v dbConfig.TargetValidator) *DynamicNodeRegistry {
	dnr := &DynamicNodeRegistry{
		gossipManager: gossipManager,
		sReg:          NewStaticNodeRegistry(streamConnections, v),
	}
	// Register the node registry as a gossip listener so that it receives
	// gossip callbacks.
	gossipManager.AddListener(dnr)
	return dnr
}

func (d *DynamicNodeRegistry) handleEvent(event *serf.UserEvent) {
	if event.Name != constants.RegistryUpdateEvent {
		return
	}
	req := &rfpb.RegistryPushRequest{}
	if err := proto.Unmarshal(event.Payload, req); err != nil {
		return
	}
	if req.GetNhid() == "" {
		log.Warningf("Ignoring malformed registry push request: %+v", req)
		return
	}
	if req.GetGrpcAddress() != "" || req.GetRaftAddress() != "" {
		d.sReg.AddNode(req.GetNhid(), req.GetRaftAddress(), req.GetGrpcAddress())
	}
	for _, r := range req.GetReplicas() {
		d.sReg.Add(r.GetRangeId(), r.GetReplicaId(), req.GetNhid())
	}
}

// handleQuery handles a registry query event. It looks up the NHID, the raft
// address and the gRPC address for the node specified by the range ID and the
// replica ID.
func (d *DynamicNodeRegistry) handleQuery(query *serf.Query) {
	if query.Name != constants.RegistryQueryEvent {
		return
	}
	if query.Payload == nil {
		return
	}
	req := &rfpb.RegistryQueryRequest{}
	if err := proto.Unmarshal(query.Payload, req); err != nil {
		return
	}
	if req.GetRangeId() == 0 || req.GetReplicaId() == 0 {
		log.Warningf("Ignoring malformed registry query: %+v", req)
		return
	}
	n, _, err := d.sReg.ResolveNHID(req.GetRangeId(), req.GetReplicaId())
	if err != nil {
		return
	}
	r, _, err := d.sReg.ResolveRaft(req.GetRangeId(), req.GetReplicaId())
	if err != nil {
		return
	}
	g, _, err := d.sReg.ResolveGRPC(req.GetRangeId(), req.GetReplicaId())
	if err != nil {
		return
	}
	rsp := &rfpb.RegistryQueryResponse{
		Nhid:        n,
		RaftAddress: r,
		GrpcAddress: g,
	}
	buf, err := proto.Marshal(rsp)
	if err != nil {
		return
	}
	if err := query.Respond(buf); err != nil {
		log.Debugf("Error responding to gossip query: %s", err)
	}
}

// OnEvent is called when a node joins, leaves, or is updated.
func (d *DynamicNodeRegistry) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		d.handleQuery(query)
	case serf.EventUser:
		userEvent, _ := event.(serf.UserEvent)
		d.handleEvent(&userEvent)
	default:
		break
	}
}

func (d *DynamicNodeRegistry) pushUpdate(req *rfpb.RegistryPushRequest) {
	buf, err := proto.Marshal(req)
	if err != nil {
		log.Errorf("error marshaling proto: %s", err)
		return
	}
	err = d.gossipManager.SendUserEvent(constants.RegistryUpdateEvent, buf, true)
	if err != nil {
		log.Errorf("error pushing gossip update: %s", err)
	}
}

// queryPeers queries the gossip network for node that hold the specified rangeID
// and replicaID. If any nodes are found, they are added to the static registry.
func (d *DynamicNodeRegistry) queryPeers(rangeID uint64, replicaID uint64) {
	req := &rfpb.RegistryQueryRequest{
		RangeId:   rangeID,
		ReplicaId: replicaID,
	}
	buf, err := proto.Marshal(req)
	if err != nil {
		return
	}
	stream, err := d.gossipManager.Query(constants.RegistryQueryEvent, buf, nil)
	if err != nil {
		log.Warningf("gossip Query returned err: %s", err)
		return
	}
	for p := range stream.ResponseCh() {
		rsp := &rfpb.RegistryQueryResponse{}
		if err := proto.Unmarshal(p.Payload, rsp); err != nil {
			continue
		}
		if rsp.GetNhid() == "" {
			log.Warningf("ignoring malformed query response: %+v", rsp)
			continue
		}
		d.sReg.Add(rangeID, replicaID, rsp.GetNhid())
		d.sReg.AddNode(rsp.GetNhid(), rsp.GetRaftAddress(), rsp.GetGrpcAddress())
		// Since only one nodehost can be identified by the specified rangeID and
		// replicaID. We can close the query after found one nodehost.
		stream.Close()
		return
	}
}

// Add adds the specified node and its target info to the registry.
func (d *DynamicNodeRegistry) Add(rangeID uint64, replicaID uint64, target string) {
	d.sReg.Add(rangeID, replicaID, target)

	// Raft library calls this method very often (bug?), so don't gossip
	// these adds for now. Another option would be to track which ones have
	// been gossipped and only gossip new ones, but for simplicity's sake
	// we'll just skip it for now.
}

// Remove removes a remote from the node registry.
func (d *DynamicNodeRegistry) Remove(rangeID uint64, replicaID uint64) {
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (d *DynamicNodeRegistry) RemoveShard(rangeID uint64) {
}

// Resolve returns the raft address and the connection key of the specified node.
func (d *DynamicNodeRegistry) Resolve(rangeID uint64, replicaID uint64) (string, string, error) {
	return d.ResolveRaft(rangeID, replicaID)
}

// ResolveRaft returns the raft address and the connection key of the specified node.
func (d *DynamicNodeRegistry) ResolveRaft(rangeID uint64, replicaID uint64) (string, string, error) {
	r, k, err := d.sReg.ResolveRaft(rangeID, replicaID)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		d.queryPeers(rangeID, replicaID)
		return d.sReg.ResolveRaft(rangeID, replicaID)
	}
	return r, k, err
}

// ResolveGRPC returns the gRPC address and the connection key of the specified node.
func (d *DynamicNodeRegistry) ResolveGRPC(rangeID uint64, replicaID uint64) (string, string, error) {
	g, k, err := d.sReg.ResolveGRPC(rangeID, replicaID)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		d.queryPeers(rangeID, replicaID)
		return d.sReg.ResolveGRPC(rangeID, replicaID)
	}
	return g, k, err
}

// ResolveNHID returns the NodeHost ID (NHID) and the connection key of the
// specified node.
func (d *DynamicNodeRegistry) ResolveNHID(rangeID uint64, replicaID uint64) (string, string, error) {
	n, k, err := d.sReg.ResolveNHID(rangeID, replicaID)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		d.queryPeers(rangeID, replicaID)
		return d.sReg.ResolveNHID(rangeID, replicaID)
	}
	return n, k, err
}

// AddNode adds the raftAddress and grpcAddr for the specified target to the
// registry.
func (d *DynamicNodeRegistry) AddNode(target, raftAddress, grpcAddress string) {
	d.sReg.AddNode(target, raftAddress, grpcAddress)
	d.pushUpdate(&rfpb.RegistryPushRequest{
		Nhid:        target,
		RaftAddress: raftAddress,
		GrpcAddress: grpcAddress,
	})
}

func (d *DynamicNodeRegistry) Close() error {
	d.sReg.Close()
	return nil
}

func (d *DynamicNodeRegistry) String() string {
	return d.sReg.String()
}
