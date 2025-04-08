package registry

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
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
	ResolveGRPC(ctx context.Context, nhid string) (string, error)

	// Used by store.go only.
	AddNode(target, raftAddress, grpcAddress string)
	String() string
	ListNodes() []*rfpb.ConnectionInfo
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

type addresses struct {
	grpc string
	raft string
}

type nodeInfoSet map[raftio.NodeInfo]struct{}

// StaticRegistry is used to manage all known node addresses in the multi raft
// system. The transport layer uses this address registry to locate nodes.
type StaticRegistry struct {
	partitioner *fixedPartitioner
	validate    dbConfig.TargetValidator

	mu          sync.Mutex                 // protects shards and nodeTargets
	shards      map[uint64]nodeInfoSet     // map of rangeID to a set of nodeInfo
	nodeTargets map[raftio.NodeInfo]string // map of raftio.NodeInfo => NHID

	targetAddresses sync.Map // map of NHID(string) => addresses

	log log.Logger
}

// NewStaticNodeRegistry returns a new StaticRegistry object.
func NewStaticNodeRegistry(streamConnections uint64, v dbConfig.TargetValidator, nhlogger log.Logger) *StaticRegistry {
	n := &StaticRegistry{
		validate:    v,
		shards:      make(map[uint64]nodeInfoSet),
		nodeTargets: make(map[raftio.NodeInfo]string),
		log:         nhlogger,
	}
	if streamConnections > 1 {
		n.partitioner = &fixedPartitioner{capacity: streamConnections}
	}
	return n
}

// Add adds the specified node and its target info to the registry.
func (n *StaticRegistry) Add(rangeID uint64, replicaID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		n.log.Errorf("invalid target %s", target)
		return
	}

	key := raftio.GetNodeInfo(rangeID, replicaID)

	n.mu.Lock()
	defer n.mu.Unlock()
	s, ok := n.shards[rangeID]
	if !ok {
		s = make(map[raftio.NodeInfo]struct{})
		n.shards[rangeID] = s
	}
	s[key] = struct{}{}

	existingTarget, ok := n.nodeTargets[key]

	if ok {
		if existingTarget != target {
			n.log.Errorf("inconsistent target for c%dn%d, %s:%s", rangeID, replicaID, existingTarget, target)
		}
	} else {
		n.nodeTargets[key] = target
		n.log.Debugf("added target for c%dn%d:%s", rangeID, replicaID, target)
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
	key := raftio.GetNodeInfo(rangeID, replicaID)
	n.mu.Lock()
	defer n.mu.Unlock()
	s, ok := n.shards[rangeID]
	if ok {
		delete(s, key)
	}
	delete(n.nodeTargets, key)
	n.log.Debugf("removed targets for c%dn%d", rangeID, replicaID)
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (n *StaticRegistry) RemoveShard(rangeID uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	s, ok := n.shards[rangeID]
	if ok {
		for ni, _ := range s {
			delete(n.nodeTargets, ni)
		}
	}
	delete(n.shards, rangeID)
	n.log.Debugf("removed targets for range %d", rangeID)
}

// ResolveRaft returns the raft address and the connection key of the specified node.
func (n *StaticRegistry) Resolve(rangeID uint64, replicaID uint64) (string, string, error) {
	ci, err := n.Lookup(rangeID, replicaID)
	if err != nil {
		return "", "", err
	}
	return ci.GetRaftAddress(), n.getConnectionKey(ci.GetRaftAddress(), rangeID), nil
}

// ResolveRaft returns the raft address and the connection key of the specified node.
func (n *StaticRegistry) Lookup(rangeID uint64, replicaID uint64) (*rfpb.ConnectionInfo, error) {
	key := raftio.GetNodeInfo(rangeID, replicaID)
	n.mu.Lock()
	target, ok := n.nodeTargets[key]
	n.mu.Unlock()

	if ok {
		ci := &rfpb.ConnectionInfo{
			Nhid: target,
		}
		if a, ok := n.targetAddresses.Load(target); ok {
			addr := a.(addresses)
			ci.RaftAddress = addr.raft
			ci.GrpcAddress = addr.grpc
			return ci, nil
		}
		return ci, targetAddressUnknownError(rangeID, replicaID)
	}
	return nil, targetAddressUnknownError(rangeID, replicaID)
}

func (s *StaticRegistry) LookupByNHID(nhid string) (*rfpb.ConnectionInfo, error) {
	if a, ok := s.targetAddresses.Load(nhid); ok {
		addr := a.(addresses)
		return &rfpb.ConnectionInfo{
			Nhid:        nhid,
			RaftAddress: addr.raft,
			GrpcAddress: addr.grpc,
		}, nil
	}
	return nil, status.NotFoundErrorf("%s: %s", targetAddressUnknownErrorMsg, nhid)
}

// ResolveGRPC returns the gRPC address and the connection key of the specified node.
func (n *StaticRegistry) ResolveGRPC(ctx context.Context, nhid string) (returnedAddr string, returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006

	defer func() {
		tracing.RecordErrorToSpan(spn, returnedErr)
		spn.End()
	}()

	if a, ok := n.targetAddresses.Load(nhid); ok {
		grpcAddr := a.(addresses).grpc
		return grpcAddr, nil
	}
	return "", status.NotFoundErrorf("%s: %s", targetAddressUnknownErrorMsg, nhid)
}

// AddNode adds the raftAddress and grpcAddr for the specified target to the
// registry.
func (n *StaticRegistry) AddNode(target, raftAddress, grpcAddress string) {
	if n.validate != nil && !n.validate(target) {
		log.Errorf("invalid target %s", target)
		return
	}
	if raftAddress == "" || grpcAddress == "" {
		return
	}
	a := addresses{
		raft: raftAddress,
		grpc: grpcAddress,
	}
	n.targetAddresses.Store(target, a)
}

// ListNodes lists all the {NHID, raftAddress, grpcAddress} available in the
// registry.
func (n *StaticRegistry) ListNodes() []*rfpb.ConnectionInfo {
	results := make([]*rfpb.ConnectionInfo, 0)
	n.targetAddresses.Range(func(k, v interface{}) bool {
		nhid := k.(string)
		a := v.(addresses)
		results = append(results, &rfpb.ConnectionInfo{
			Nhid:        nhid,
			RaftAddress: a.raft,
			GrpcAddress: a.grpc,
		})
		return true
	})
	return results
}

func (n *StaticRegistry) Close() error {
	return nil
}

func (n *StaticRegistry) String() string {
	nhidToReplicas := make(map[string][]*rfpb.ReplicaDescriptor)
	n.mu.Lock()
	for ni, nhid := range n.nodeTargets {
		nhidToReplicas[nhid] = append(nhidToReplicas[nhid], &rfpb.ReplicaDescriptor{
			RangeId:   ni.ShardID,
			ReplicaId: ni.ReplicaID,
		})
	}
	n.mu.Unlock()
	for _, replicas := range nhidToReplicas {
		sort.Slice(replicas, func(i, j int) bool {
			if replicas[i].GetRangeId() == replicas[j].GetRangeId() {
				return replicas[i].GetReplicaId() < replicas[j].GetReplicaId()
			}
			return replicas[i].GetRangeId() < replicas[j].GetRangeId()
		})
	}
	buf := "\nRegistry\n"
	for nhid, replicas := range nhidToReplicas {
		a, ok := n.targetAddresses.Load(nhid)
		if !ok {
			continue
		}
		addr := a.(addresses)

		buf += fmt.Sprintf("  Node: %q [raftAddr: %q, grpcAddr: %q]\n", nhid, addr.raft, addr.grpc)
		buf += fmt.Sprintf("   %+v\n", replicas)
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
func NewDynamicNodeRegistry(gossipManager interfaces.GossipService, streamConnections uint64, v dbConfig.TargetValidator, nhlog log.Logger) *DynamicNodeRegistry {
	dnr := &DynamicNodeRegistry{
		gossipManager: gossipManager,
		sReg:          NewStaticNodeRegistry(streamConnections, v, nhlog),
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
		d.sReg.log.Warningf("failed to unmarshal payload: %s", err)
		return
	}
	if req.GetNhid() == "" || req.GetGrpcAddress() == "" || req.GetRaftAddress() == "" {
		d.sReg.log.Warningf("Ignoring malformed registry push request: %+v", req)
		return
	}
	if req.GetGrpcAddress() != "" && req.GetRaftAddress() != "" {
		d.sReg.log.Infof("handle registry update event, add %+v", req)
		d.sReg.AddNode(req.GetNhid(), req.GetRaftAddress(), req.GetGrpcAddress())
	}
}

// handleQuery handles a registry query event. It looks up the NHID, the raft
// address and the gRPC address for the node specified by the range ID and the
// replica ID.
func (d *DynamicNodeRegistry) handleQuery(ctx context.Context, query *serf.Query) {
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

	var ci *rfpb.ConnectionInfo
	var err error

	if req.GetNhid() != "" {
		ci, err = d.sReg.LookupByNHID(req.GetNhid())
		if err != nil {
			return
		}
	} else if req.GetRangeId() != 0 && req.GetReplicaId() != 0 {
		ci, err = d.sReg.Lookup(req.GetRangeId(), req.GetReplicaId())
		if err != nil {
			return
		}
	} else {
		d.sReg.log.Warningf("Ignoring malformed registry query: %+v", req)
		return
	}
	rsp := &rfpb.RegistryQueryResponse{
		Nhid:        ci.GetNhid(),
		RaftAddress: ci.GetRaftAddress(),
		GrpcAddress: ci.GetGrpcAddress(),
	}
	buf, err := proto.Marshal(rsp)
	if err != nil {
		return
	}
	if err := query.Respond(buf); err != nil {
		d.sReg.log.Debugf("Error responding to gossip query: %s", err)
	}
}

// OnEvent is called when a node joins, leaves, or is updated.
func (d *DynamicNodeRegistry) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		d.handleQuery(context.TODO(), query)
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
		d.sReg.log.Errorf("error marshaling proto: %s", err)
		return
	}
	err = d.gossipManager.SendUserEvent(constants.RegistryUpdateEvent, buf /*coalesce=*/, false)
	if err != nil {
		d.sReg.log.Errorf("error pushing gossip update: %s", err)
	}
}

// queryPeers queries the gossip network to get the (nhid, raft address, grpc
// address) of the node that holds the specified rangeID and replicaID or the
// node with the specified NHID.
// If any nodes are found, they are added to the static registry.
func (d *DynamicNodeRegistry) queryPeers(ctx context.Context, req *rfpb.RegistryQueryRequest) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	buf, err := proto.Marshal(req)
	if err != nil {
		return
	}

	stream, err := d.gossipManager.Query(constants.RegistryQueryEvent, buf, nil)
	if err != nil {
		d.sReg.log.Warningf("queryPeers failed: gossip Query returned err: %s", err)
		return
	}
	for p := range stream.ResponseCh() {
		rsp := &rfpb.RegistryQueryResponse{}
		if err := proto.Unmarshal(p.Payload, rsp); err != nil {
			continue
		}
		if rsp.GetGrpcAddress() == "" || rsp.GetRaftAddress() == "" {
			d.sReg.log.Warningf("queryPeers ignoring malformed query response: %+v", rsp)
			continue
		}
		d.sReg.AddNode(rsp.GetNhid(), rsp.GetRaftAddress(), rsp.GetGrpcAddress())

		if req.GetRangeId() != 0 && req.GetReplicaId() != 0 {
			d.sReg.Add(req.GetRangeId(), req.GetReplicaId(), rsp.GetNhid())
		}
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
	d.sReg.Remove(rangeID, replicaID)
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (d *DynamicNodeRegistry) RemoveShard(rangeID uint64) {
	d.sReg.RemoveShard(rangeID)
}

// ListNodes lists all entries from the registry.
func (d *DynamicNodeRegistry) ListNodes() []*rfpb.ConnectionInfo {
	return d.sReg.ListNodes()
}

// Resolve returns the raft address and the connection key of the specified node.
func (d *DynamicNodeRegistry) Resolve(rangeID uint64, replicaID uint64) (string, string, error) {
	ci, err := d.Lookup(context.TODO(), rangeID, replicaID)
	if err != nil {
		return "", "", err
	}
	return ci.GetRaftAddress(), d.sReg.getConnectionKey(ci.GetRaftAddress(), rangeID), nil
}

// Lookup returns the connectionInfo of the specified node
func (d *DynamicNodeRegistry) Lookup(ctx context.Context, rangeID uint64, replicaID uint64) (*rfpb.ConnectionInfo, error) {
	ci, err := d.sReg.Lookup(rangeID, replicaID)
	if err == nil {
		return ci, nil
	}
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		var req *rfpb.RegistryQueryRequest
		if ci == nil {
			req = &rfpb.RegistryQueryRequest{
				RangeId:   rangeID,
				ReplicaId: replicaID,
			}
		} else if ci.GetNhid() != "" {
			req = &rfpb.RegistryQueryRequest{
				Nhid: ci.GetNhid(),
			}
		}
		d.queryPeers(ctx, req)
		return d.sReg.Lookup(rangeID, replicaID)
	}
	return nil, err
}

// ResolveGRPC returns the gRPC address and the connection key of the specified node.
func (d *DynamicNodeRegistry) ResolveGRPC(ctx context.Context, nhid string) (addr string, returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer func() {
		tracing.RecordErrorToSpan(spn, returnedErr)
		spn.End()
	}()
	g, err := d.sReg.ResolveGRPC(ctx, nhid)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		req := &rfpb.RegistryQueryRequest{
			Nhid: nhid,
		}
		d.queryPeers(ctx, req)
		return d.sReg.ResolveGRPC(ctx, nhid)
	}
	return g, err
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
