package registry

import (
	"fmt"
	"sync"
	//	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3/raftio"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbConfig "github.com/lni/dragonboat/v3/config"
)

// DynamicNodeRegistry is a node registry backed by gossip. It is capable of
// supporting NodeHosts with dynamic RaftAddress values.
type DynamicNodeRegistry struct {
	nhid        string
	raftAddress string
	grpcAddress string

	log           log.Logger
	gossipManager *gossip.GossipManager
	validator     dbConfig.TargetValidator
	partitioner   *fixedPartitioner

	mu sync.RWMutex // PROTECTS(nodeTargets, nodeAddrs)

	// All known node descriptors.
	nodeDescriptors map[raftio.NodeInfo]*tsNodeDescriptor

	// NodeInfo is (clusterID, nodeID)
	// nodeTargets contains a map of nodeInfo -> target
	nodeTargets map[raftio.NodeInfo]string

	// raftAddrs contains a map of nodehostID -> raftAddress
	// this map is populated by gossip data from serf.
	raftAddrs map[string]string

	// grpcAddrs contains a map of nodehostID -> grpcAddress
	// this map is populated by gossip data from serf.
	grpcAddrs map[string]string
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. It will create a single
// DynamicNodeRegistry and use it to resolve all other raft nodes until the
// process shuts down.
func NewDynamicNodeRegistry(nhid, raftAddress, grpcAddress string, gossipManager *gossip.GossipManager, streamConnections uint64, v dbConfig.TargetValidator) (*DynamicNodeRegistry, error) {
	dnr := &DynamicNodeRegistry{
		nhid:            nhid,
		raftAddress:     raftAddress,
		grpcAddress:     grpcAddress,
		validator:       v,
		log:             log.NamedSubLogger(nhid),
		gossipManager:   gossipManager,
		mu:              sync.RWMutex{},
		nodeDescriptors: make(map[raftio.NodeInfo]*tsNodeDescriptor, 0),
		nodeTargets:     make(map[raftio.NodeInfo]string, 0),
		raftAddrs:       make(map[string]string, 0),
		grpcAddrs:       make(map[string]string, 0),
	}
	if streamConnections > 1 {
		dnr.partitioner = &fixedPartitioner{capacity: streamConnections}
	}
	return dnr, nil
}

type tsNodeDescriptor struct {
	*rfpb.NodeDescriptor
	timestamp serf.LamportTime
}

func (tnd *tsNodeDescriptor) LamportTime() serf.LamportTime {
	return tnd.timestamp
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

func (dnr *DynamicNodeRegistry) processRegistryUpdate(update *rfpb.RegistryUpdate) {
	dnr.addRaftNodeHost(update.GetNhid(), update.GetRaftAddress())
	dnr.addGRPCNodeHost(update.GetNhid(), update.GetGrpcAddress())
	for _, add := range update.GetAdds() {
		dnr.add(add.GetClusterId(), add.GetNodeId(), add.GetTarget())
	}
	for _, remove := range update.GetRemoves() {
		dnr.remove(remove.GetClusterId(), remove.GetNodeId())
	}
	for _, clusterRemove := range update.GetClusterRemoves() {
		dnr.removeCluster(clusterRemove.GetClusterId())
	}
}

func (dnr *DynamicNodeRegistry) assembleRegistryUpdate(clusterID, nodeID uint64, allTargets bool) *rfpb.RegistryUpdate {
	rsp := &rfpb.RegistryUpdate{
		Nhid:        dnr.nhid,
		GrpcAddress: dnr.grpcAddress,
		RaftAddress: dnr.raftAddress,
	}
	for nodeInfo, target := range dnr.nodeTargets {
		if !allTargets && nodeInfo.ClusterID != clusterID {
			continue
		}
		if !allTargets && nodeInfo.NodeID != nodeID {
			continue
		}
		rsp.Adds = append(rsp.Adds, &rfpb.RegistryUpdate_Add{
			ClusterId: nodeInfo.ClusterID,
			NodeId:    nodeInfo.NodeID,
			Target:    target,
		})
	}
	return rsp
}

func (dnr *DynamicNodeRegistry) handleGossipQuery(query *serf.Query) {
	if query.Name != constants.RegistryQueryEvent {
		return
	}
	if query.Payload == nil {
		return
	}
	rq := &rfpb.RegistryQuery{}
	if err := proto.Unmarshal(query.Payload, rq); err != nil {
		return
	}
	rsp := dnr.assembleRegistryUpdate(rq.GetClusterId(), rq.GetNodeId(), false)
	if len(rsp.GetAdds()) == 0 {
		dnr.log.Debugf("Ignoring registry query for %d %d, don't know about that target.", rq.GetClusterId(), rq.GetNodeId())
		return
	}
	buf, err := proto.Marshal(rsp)
	if err != nil {
		return
	}
	if err := query.Respond(buf); err != nil {
		dnr.log.Debugf("Error responding to gossip query: %s", err)
	}
	dnr.log.Debugf("Responded to registry query for %d %d!", rq.GetClusterId(), rq.GetNodeId())
}

// OnEvent is called when a node joins, leaves, or is updated.
func (dnr *DynamicNodeRegistry) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		dnr.handleGossipQuery(query)
	case serf.EventUser:
		userEvent, _ := event.(serf.UserEvent)
		if userEvent.Name != constants.RegistryUpdateEvent {
			return
		}
		update := &rfpb.RegistryUpdate{}
		if err := proto.Unmarshal(userEvent.Payload, update); err != nil {
			return
		}
		dnr.processRegistryUpdate(update)
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			nhid, nhidOK := member.Tags[constants.NodeHostIDTag]
			if !nhidOK {
				continue
			}
			if raftAddr, ok := member.Tags[constants.RaftAddressTag]; ok {
				dnr.addRaftNodeHost(nhid, raftAddr)
			}
			if grpcAddr, ok := member.Tags[constants.GRPCAddressTag]; ok {
				dnr.addGRPCNodeHost(nhid, grpcAddr)
			}
		}
	default:
		break
	}
}

func (dnr *DynamicNodeRegistry) addRaftNodeHost(nhid, raftAddress string) {
	dnr.mu.Lock()
	existing, ok := dnr.raftAddrs[nhid]
	if !ok || existing != raftAddress {
		dnr.raftAddrs[nhid] = raftAddress
	}
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) removeRaftNodeHost(nhid string) {
	dnr.mu.Lock()
	delete(dnr.raftAddrs, nhid)
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) addGRPCNodeHost(nhid, grpcAddress string) {
	dnr.mu.Lock()
	_, ok := dnr.grpcAddrs[nhid]
	if !ok {
		dnr.grpcAddrs[nhid] = grpcAddress
	}
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) removeGRPCNodeHost(nhid string) {
	dnr.mu.Lock()
	delete(dnr.grpcAddrs, nhid)
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) Close() error {
	return nil
}

func (dnr *DynamicNodeRegistry) gossipUpdate(up *rfpb.RegistryUpdate) error {
	up.Nhid = dnr.nhid
	up.GrpcAddress = dnr.grpcAddress
	up.RaftAddress = dnr.raftAddress
	buf, err := proto.Marshal(up)
	if err != nil {
		return err
	}
	return dnr.gossipManager.SendUserEvent(constants.RegistryUpdateEvent, buf, true)
}

func (dnr *DynamicNodeRegistry) gossipAdd(clusterID, nodeID uint64, target string) {
	go func() {
		err := dnr.gossipUpdate(&rfpb.RegistryUpdate{
			Adds: []*rfpb.RegistryUpdate_Add{{
				ClusterId: clusterID,
				NodeId:    nodeID,
				Target:    target,
			}},
		})
		if err != nil {
			dnr.log.Warningf("Error sending registry update: %s", err)
		}
	}()
}

func (dnr *DynamicNodeRegistry) gossipRemove(clusterID, nodeID uint64) {
	go func() {
		err := dnr.gossipUpdate(&rfpb.RegistryUpdate{
			Removes: []*rfpb.RegistryUpdate_Remove{{
				ClusterId: clusterID,
				NodeId:    nodeID,
			}},
		})
		if err != nil {
			dnr.log.Warningf("Error sending registry update: %s", err)
		}
	}()
}

func (dnr *DynamicNodeRegistry) gossipRemoveCluster(clusterID uint64) {
	go func() {
		err := dnr.gossipUpdate(&rfpb.RegistryUpdate{
			ClusterRemoves: []*rfpb.RegistryUpdate_RemoveCluster{{
				ClusterId: clusterID,
			}},
		})
		if err != nil {
			dnr.log.Warningf("Error sending registry update: %s", err)
		}
	}()
}

func (dnr *DynamicNodeRegistry) add(clusterID uint64, nodeID uint64, target string) bool {
	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)

	dnr.mu.Lock()
	defer dnr.mu.Unlock()

	existing, ok := dnr.nodeTargets[nodeInfo]
	if !ok || existing != target {
		dnr.nodeTargets[nodeInfo] = target
		return true
	}
	return false
}

// Add adds a new node with its known NodeHostID to the registry.
func (dnr *DynamicNodeRegistry) Add(clusterID uint64, nodeID uint64, target string) {
	if dnr.validator != nil && !dnr.validator(target) {
		dnr.log.Errorf("Add(%d, %d, %q) failed, target did not validate.", clusterID, nodeID, target)
		return
	}
	added := dnr.add(clusterID, nodeID, target)
	if added {
		// Only gossip the add if it's new to us.
		dnr.gossipAdd(clusterID, nodeID, target)
	}
}

func (dnr *DynamicNodeRegistry) remove(clusterID uint64, nodeID uint64) bool {
	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)

	dnr.mu.Lock()
	defer dnr.mu.Unlock()
	if _, ok := dnr.nodeTargets[nodeInfo]; ok {
		delete(dnr.nodeTargets, nodeInfo)
		return true
	}
	return false
}

// Remove removes the specified node from the registry.
func (dnr *DynamicNodeRegistry) Remove(clusterID uint64, nodeID uint64) {
	removed := dnr.remove(clusterID, nodeID)
	if removed {
		// Only gossip the remove if it's new to us.
		dnr.gossipRemove(clusterID, nodeID)
	}
}

func (dnr *DynamicNodeRegistry) removeCluster(clusterID uint64) {
	toRemove := make([]raftio.NodeInfo, 0)
	dnr.mu.RLock()
	for ni, _ := range dnr.nodeTargets {
		if ni.ClusterID == clusterID {
			toRemove = append(toRemove, ni)
		}
	}
	dnr.mu.RUnlock()

	dnr.mu.Lock()
	for _, nodeInfo := range toRemove {
		delete(dnr.nodeTargets, nodeInfo)
	}
	dnr.mu.Unlock()
}

// RemoveCluster removes the specified node from the registry.
func (dnr *DynamicNodeRegistry) RemoveCluster(clusterID uint64) {
	dnr.removeCluster(clusterID)
	dnr.gossipRemoveCluster(clusterID)
}

func (dnr *DynamicNodeRegistry) getConnectionKey(addr string, clusterID uint64) string {
	if dnr.partitioner == nil {
		return addr
	}
	return fmt.Sprintf("%s-%d", addr, dnr.partitioner.GetPartitionID(clusterID))
}

// ResolveGRPCAddress returns the current GRPC address for a nodeHostID.
func (dnr *DynamicNodeRegistry) ResolveGRPCAddress(nhid string) (string, error) {
	grpcAddr, ok := dnr.grpcAddrs[nhid]
	if !ok {
		return "", status.NotFoundError("target address unknown")
	}
	return grpcAddr, nil
}

func (dnr *DynamicNodeRegistry) resolveNHID(clusterID uint64, nodeID uint64) (string, string, error) {
	dnr.mu.RLock()
	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)
	target, ok := dnr.nodeTargets[nodeInfo]
	dnr.mu.RUnlock()

	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	key := dnr.getConnectionKey(target, clusterID)
	return target, key, nil
}

func (dnr *DynamicNodeRegistry) resolveWithGossip(clusterID uint64, nodeID uint64) (string, string, error) {
	rq := &rfpb.RegistryQuery{
		ClusterId: clusterID,
		NodeId:    nodeID,
	}
	buf, err := proto.Marshal(rq)
	if err != nil {
		return "", "", err
	}
	rsp, err := dnr.gossipManager.Query(constants.RegistryQueryEvent, buf, nil)
	if err != nil {
		return "", "", err
	}
	for nodeRsp := range rsp.ResponseCh() {
		if nodeRsp.Payload == nil {
			continue
		}
		update := &rfpb.RegistryUpdate{}
		if err := proto.Unmarshal(nodeRsp.Payload, update); err != nil {
			continue
		}
		dnr.processRegistryUpdate(update)
		break // exit as soon as any other node answers our query
	}
	return dnr.resolveNHID(clusterID, nodeID)
}

// Resolve returns the current RaftAddress and connection key of the specified
// node. It returns ErrUnknownTarget when the RaftAddress is unknown.

// Lookups are a two-step process. First we lookup the target and partition key.
// Same as "Resolve" method:
//     target, key, err := nodeTargets(clusterID, nodeID)
// and then we take that target and lookup the actual raft address. Same as
// "GetRaftAddress" method:
//     addr, ok := raftAddrs(target)
// Finally we return the address, partitionKey, and any error.
func (dnr *DynamicNodeRegistry) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	// try to resolve locally first
	target, key, err := dnr.resolveNHID(clusterID, nodeID)
	if err != nil {
		// if that fails, try to resolve with gossip
		target, key, err = dnr.resolveWithGossip(clusterID, nodeID)
	}
	// if that still fails, we're out of options.
	if err != nil {
		dnr.log.Errorf("Error resolving %d %d: %s", clusterID, nodeID, err)
		return "", "", err
	}

	if target == dnr.nhid {
		return dnr.raftAddress, key, nil
	}

	dnr.mu.RLock()
	addr, ok := dnr.raftAddrs[target]
	dnr.mu.RUnlock()
	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	return addr, key, nil
}

func (dnr *DynamicNodeRegistry) ResolveGRPC(clusterID uint64, nodeID uint64) (string, string, error) {
	// try to resolve locally
	target, key, err := dnr.resolveNHID(clusterID, nodeID)
	if err != nil {
		// if that fails, try to resolve with gossip
		target, key, err = dnr.resolveWithGossip(clusterID, nodeID)
	}
	// if that still fails, we're out of options.
	if err != nil {
		dnr.log.Errorf("ResolveGRPC error resolving %d %d: %s", clusterID, nodeID, err)
		return "", "", err
	}

	if target == dnr.nhid {
		return dnr.grpcAddress, key, nil
	}

	dnr.mu.RLock()
	addr, ok := dnr.grpcAddrs[target]
	dnr.mu.RUnlock()
	if !ok {
		dnr.log.Errorf("ResolveGRPC %d %d returning NotFoundError", clusterID, nodeID)
		return "", "", status.NotFoundError("target address unknown")
	}

	return addr, key, nil
}
