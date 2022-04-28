package registry

import (
	"fmt"
	"sort"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3/raftio"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbConfig "github.com/lni/dragonboat/v3/config"
)

var TargetAddressUnknownError = status.NotFoundError("target address unknown")

type NodeRegistry interface {
	raftio.INodeRegistry

	// Used by store.go and sender.go.
	ResolveGRPC(clusterID uint64, nodeID uint64) (string, string, error)

	// Used by store.go only.
	AddNode(target, raftAddress, grpcAddress string)
	ResolveNHID(clusterID uint64, nodeID uint64) (string, string, error)
	String() string
}

// fixedPartitioner is the IPartitioner with fixed capacity and naive
// partitioning strategy.
type fixedPartitioner struct {
	capacity uint64
}

// GetPartitionID returns the partition ID for the specified raft cluster.
func (p *fixedPartitioner) GetPartitionID(clusterID uint64) uint64 {
	return clusterID % p.capacity
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

// NewNodeStaticRegistry returns a new StaticRegistry object.
func NewStaticNodeRegistry(streamConnections uint64, v dbConfig.TargetValidator) *StaticRegistry {
	n := &StaticRegistry{validate: v}
	if streamConnections > 1 {
		n.partitioner = &fixedPartitioner{capacity: streamConnections}
	}
	return n
}

// Add adds the specified node and its target info to the registry.
func (n *StaticRegistry) Add(clusterID uint64, nodeID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		log.Errorf("invalid target %s", target)
		return
	}
	key := raftio.GetNodeInfo(clusterID, nodeID)
	existingTarget, ok := n.nodeTargets.LoadOrStore(key, target)
	if ok {
		if existingTarget != target {
			log.Errorf("inconsistent target for %d %d, %s:%s", clusterID, nodeID, existingTarget, target)
		}
	}
}

func (n *StaticRegistry) getConnectionKey(addr string, clusterID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	return fmt.Sprintf("%s-%d", addr, n.partitioner.GetPartitionID(clusterID))
}

// Remove removes a remote from the node registry.
func (n *StaticRegistry) Remove(clusterID uint64, nodeID uint64) {
	return
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (n *StaticRegistry) RemoveCluster(clusterID uint64) {
	return
}

func (n *StaticRegistry) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	return n.ResolveRaft(clusterID, nodeID)
}

// Resolve looks up the Addr of the specified node.
func (n *StaticRegistry) ResolveRaft(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	if target, ok := n.nodeTargets.Load(key); ok {
		if r, ok := n.targetRafts.Load(target); ok {
			raftAddr := r.(string)
			return raftAddr, n.getConnectionKey(raftAddr, clusterID), nil
		}
	}
	return "", "", TargetAddressUnknownError
}

func (n *StaticRegistry) ResolveGRPC(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	if target, ok := n.nodeTargets.Load(key); ok {
		if g, ok := n.targetGrpcs.Load(target); ok {
			grpcAddr := g.(string)
			return grpcAddr, n.getConnectionKey(grpcAddr, clusterID), nil
		}
	}
	return "", "", TargetAddressUnknownError
}

func (n *StaticRegistry) ResolveNHID(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	if t, ok := n.nodeTargets.Load(key); ok {
		target := t.(string)
		return target, n.getConnectionKey(target, clusterID), nil
	}
	return "", "", TargetAddressUnknownError
}

func (n *StaticRegistry) AddNode(target, raftAddress, grpcAddress string) {
	if n.validate != nil && !n.validate(target) {
		log.Errorf("invalid target %s", target)
		return
	}
	if raftAddress != "" {
		r, ok := n.targetRafts.LoadOrStore(target, raftAddress)
		if ok && r.(string) != raftAddress {
			log.Errorf("inconsistent raft for %s:%s", r, target)
			return
		}
	}
	if grpcAddress != "" {
		g, ok := n.targetGrpcs.LoadOrStore(target, grpcAddress)
		if ok && g.(string) != grpcAddress {
			log.Errorf("inconsistent grpc for %s:%s", g, target)
			return
		}
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
			if clusters[i].ClusterID == clusters[j].ClusterID {
				return clusters[i].NodeID < clusters[j].NodeID
			}
			return clusters[i].ClusterID < clusters[j].ClusterID
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
	gossipManager *gossip.GossipManager
	sReg          *StaticRegistry
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. It will create a single
// DynamicNodeRegistry and use it to resolve all other raft nodes until the
// process shuts down.
func NewDynamicNodeRegistry(gossipManager *gossip.GossipManager, streamConnections uint64, v dbConfig.TargetValidator) *DynamicNodeRegistry {
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
		d.sReg.Add(r.GetClusterId(), r.GetNodeId(), req.GetNhid())
	}
}

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
	if req.GetClusterId() == 0 || req.GetNodeId() == 0 {
		log.Warningf("Ignoring malformed registry query: %+v", req)
		return
	}
	n, _, err := d.sReg.ResolveNHID(req.GetClusterId(), req.GetNodeId())
	if err != nil {
		return
	}
	r, _, err := d.sReg.ResolveRaft(req.GetClusterId(), req.GetNodeId())
	if err != nil {
		return
	}
	g, _, err := d.sReg.ResolveGRPC(req.GetClusterId(), req.GetNodeId())
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

func (d *DynamicNodeRegistry) queryPeers(clusterID uint64, nodeID uint64) {
	req := &rfpb.RegistryQueryRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
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
		d.sReg.Add(clusterID, nodeID, rsp.GetNhid())
		d.sReg.AddNode(rsp.GetNhid(), rsp.GetRaftAddress(), rsp.GetGrpcAddress())
		stream.Close()
		return
	}
}

// Add adds the specified node and its target info to the registry.
func (d *DynamicNodeRegistry) Add(clusterID uint64, nodeID uint64, target string) {
	d.sReg.Add(clusterID, nodeID, target)

	// Raft library calls this method very often (bug?), so don't gossip
	// these adds for now. Another option would be to track which ones have
	// been gossipped and only gossip new ones, but for simplicity's sake
	// we'll just skip it for now.
}

// Remove removes a remote from the node registry.
func (d *DynamicNodeRegistry) Remove(clusterID uint64, nodeID uint64) {
	return
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (d *DynamicNodeRegistry) RemoveCluster(clusterID uint64) {
	return
}

func (d *DynamicNodeRegistry) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	return d.ResolveRaft(clusterID, nodeID)
}

// Resolve looks up the Addr of the specified node.
func (d *DynamicNodeRegistry) ResolveRaft(clusterID uint64, nodeID uint64) (string, string, error) {
	r, k, err := d.sReg.ResolveRaft(clusterID, nodeID)
	if err == TargetAddressUnknownError {
		d.queryPeers(clusterID, nodeID)
		return d.sReg.ResolveRaft(clusterID, nodeID)
	}
	return r, k, err
}

func (d *DynamicNodeRegistry) ResolveGRPC(clusterID uint64, nodeID uint64) (string, string, error) {
	g, k, err := d.sReg.ResolveGRPC(clusterID, nodeID)
	if err == TargetAddressUnknownError {
		d.queryPeers(clusterID, nodeID)
		return d.sReg.ResolveGRPC(clusterID, nodeID)
	}
	return g, k, err
}

func (d *DynamicNodeRegistry) ResolveNHID(clusterID uint64, nodeID uint64) (string, string, error) {
	n, k, err := d.sReg.ResolveNHID(clusterID, nodeID)
	if err == TargetAddressUnknownError {
		d.queryPeers(clusterID, nodeID)
		return d.sReg.ResolveNHID(clusterID, nodeID)
	}
	return n, k, err
}

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
