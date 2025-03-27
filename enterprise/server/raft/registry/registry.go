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
	"go.opentelemetry.io/otel/attribute"

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
	ResolveGRPC(ctx context.Context, rangeID uint64, replicaID uint64) (string, string, error)

	// Used by store.go only.
	AddNode(target, raftAddress, grpcAddress string)
	ResolveNHID(ctx context.Context, rangeID uint64, replicaID uint64) (string, string, error)
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

// StaticRegistry is used to manage all known node addresses in the multi raft
// system. The transport layer uses this address registry to locate nodes.
type StaticRegistry struct {
	partitioner *fixedPartitioner
	validate    dbConfig.TargetValidator

	nodeTargets     sync.Map // map of raftio.NodeInfo => NHID(string)
	targetAddresses sync.Map // map of NHID(string) => addresses
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
		if a, ok := n.targetAddresses.Load(target); ok {
			raftAddr := a.(addresses).raft
			return raftAddr, n.getConnectionKey(raftAddr, rangeID), nil
		}
	}
	return "", "", targetAddressUnknownError(rangeID, replicaID)
}

// ResolveGRPC returns the gRPC address and the connection key of the specified node.
func (n *StaticRegistry) ResolveGRPC(ctx context.Context, rangeID uint64, replicaID uint64) (returnedAddr string, returnedKey string, returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	rangeIDAttr := attribute.Int64("range_id", int64(rangeID))
	replicaIDAttr := attribute.Int64("replica_id", int64(replicaID))
	spn.SetAttributes(rangeIDAttr, replicaIDAttr)

	defer func() {
		tracing.RecordErrorToSpan(spn, returnedErr)
		spn.End()
	}()

	key := raftio.GetNodeInfo(rangeID, replicaID)
	if target, ok := n.nodeTargets.Load(key); ok {
		if a, ok := n.targetAddresses.Load(target); ok {
			grpcAddr := a.(addresses).grpc
			return grpcAddr, n.getConnectionKey(grpcAddr, rangeID), nil
		}
	}
	return "", "", targetAddressUnknownError(rangeID, replicaID)
}

// ResolveNHID returns the NodeHost ID (NHID) and the connection key of the
// specified node.
func (n *StaticRegistry) ResolveNHID(ctx context.Context, rangeID uint64, replicaID uint64) (string, string, error) {
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
	if raftAddress == "" && grpcAddress == "" {
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
	n.nodeTargets.Range(func(k, v interface{}) bool {
		nhid := v.(string)
		ni := k.(raftio.NodeInfo)
		nhidToReplicas[nhid] = append(nhidToReplicas[nhid], &rfpb.ReplicaDescriptor{
			RangeId:   ni.ShardID,
			ReplicaId: ni.ReplicaID,
		})
		return true
	})
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
	if req.GetRangeId() == 0 || req.GetReplicaId() == 0 {
		log.Warningf("Ignoring malformed registry query: %+v", req)
		return
	}
	n, _, err := d.sReg.ResolveNHID(ctx, req.GetRangeId(), req.GetReplicaId())
	if err != nil {
		return
	}
	r, _, err := d.sReg.ResolveRaft(req.GetRangeId(), req.GetReplicaId())
	if err != nil {
		return
	}
	g, _, err := d.sReg.ResolveGRPC(ctx, req.GetRangeId(), req.GetReplicaId())
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
func (d *DynamicNodeRegistry) queryPeers(ctx context.Context, rangeID uint64, replicaID uint64) {
	log.Debugf("queryPeers for c%dn%d", rangeID, replicaID)
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	rangeIDAttr := attribute.Int64("range_id", int64(rangeID))
	replicaIDAttr := attribute.Int64("replica_id", int64(replicaID))
	spn.SetAttributes(rangeIDAttr, replicaIDAttr)
	defer spn.End()

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
		log.Warningf("queryPeers failed: gossip Query returned err: %s", err)
		return
	}
	for p := range stream.ResponseCh() {
		rsp := &rfpb.RegistryQueryResponse{}
		if err := proto.Unmarshal(p.Payload, rsp); err != nil {
			continue
		}
		if rsp.GetNhid() == "" {
			log.Warningf("queryPeers ignoring malformed query response: %+v", rsp)
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

// ListNodes lists all entries from the registry.
func (d *DynamicNodeRegistry) ListNodes() []*rfpb.ConnectionInfo {
	return d.sReg.ListNodes()
}

// Resolve returns the raft address and the connection key of the specified node.
func (d *DynamicNodeRegistry) Resolve(rangeID uint64, replicaID uint64) (string, string, error) {
	return d.ResolveRaft(context.TODO(), rangeID, replicaID)
}

// ResolveRaft returns the raft address and the connection key of the specified node.
func (d *DynamicNodeRegistry) ResolveRaft(ctx context.Context, rangeID uint64, replicaID uint64) (string, string, error) {
	r, k, err := d.sReg.ResolveRaft(rangeID, replicaID)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		d.queryPeers(ctx, rangeID, replicaID)
		return d.sReg.ResolveRaft(rangeID, replicaID)
	}
	return r, k, err
}

// ResolveGRPC returns the gRPC address and the connection key of the specified node.
func (d *DynamicNodeRegistry) ResolveGRPC(ctx context.Context, rangeID uint64, replicaID uint64) (addr string, key string, returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	rangeIDAttr := attribute.Int64("range_id", int64(rangeID))
	replicaIDAttr := attribute.Int64("replica_id", int64(replicaID))
	spn.SetAttributes(rangeIDAttr, replicaIDAttr)

	defer func() {
		tracing.RecordErrorToSpan(spn, returnedErr)
		spn.End()
	}()
	g, k, err := d.sReg.ResolveGRPC(ctx, rangeID, replicaID)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		d.queryPeers(ctx, rangeID, replicaID)
		return d.sReg.ResolveGRPC(ctx, rangeID, replicaID)
	}
	return g, k, err
}

// ResolveNHID returns the NodeHost ID (NHID) and the connection key of the
// specified node.
func (d *DynamicNodeRegistry) ResolveNHID(ctx context.Context, rangeID uint64, replicaID uint64) (string, string, error) {
	n, k, err := d.sReg.ResolveNHID(ctx, rangeID, replicaID)
	if strings.HasPrefix(status.Message(err), targetAddressUnknownErrorMsg) {
		d.queryPeers(ctx, rangeID, replicaID)
		return d.sReg.ResolveNHID(ctx, rangeID, replicaID)
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
