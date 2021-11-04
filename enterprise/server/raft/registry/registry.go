package registry

import (
	"fmt"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3/raftio"

	dbConfig "github.com/lni/dragonboat/v3/config"
)

// DynamicNodeRegistry is a node registry backed by gossip. It is capable of
// supporting NodeHosts with dynamic RaftAddress values.
type DynamicNodeRegistry struct {
	nhid        string
	raftAddress string
	grpcAddress string

	validator   dbConfig.TargetValidator
	partitioner *fixedPartitioner

	mu sync.RWMutex // PROTECTS(nodeTargets, nodeAddrs)

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
func NewDynamicNodeRegistry(nhid, raftAddress, grpcAddress string, streamConnections uint64, v dbConfig.TargetValidator) (*DynamicNodeRegistry, error) {
	dnr := &DynamicNodeRegistry{
		nhid:        nhid,
		raftAddress: raftAddress,
		grpcAddress: grpcAddress,
		validator:   v,
		mu:          sync.RWMutex{},
		nodeTargets: make(map[raftio.NodeInfo]string, 0),
		raftAddrs:   make(map[string]string, 0),
		grpcAddrs:   make(map[string]string, 0),
	}
	if streamConnections > 1 {
		dnr.partitioner = &fixedPartitioner{capacity: streamConnections}
	}
	return dnr, nil
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

// OnEvent is called when a node joins, leaves, or is updated.
func (dnr *DynamicNodeRegistry) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			if nhid, ok := member.Tags[constants.NodeHostIDTag]; ok {
				if raftAddress, ok := member.Tags[constants.RaftAddressTag]; ok {
					if raftAddress != "" {
						dnr.addRaftNodeHost(nhid, raftAddress)
					} else {
						dnr.removeRaftNodeHost(nhid)
					}
				}

				if grpcAddress, ok := member.Tags[constants.GRPCAddressTag]; ok {
					if grpcAddress != "" {
						dnr.addGRPCNodeHost(nhid, grpcAddress)
					} else {
						dnr.removeGRPCNodeHost(nhid)
					}
				}
			}
		}
	case serf.EventMemberLeave:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			if nhid, ok := member.Tags[constants.NodeHostIDTag]; ok {
				dnr.removeRaftNodeHost(nhid)
				dnr.removeGRPCNodeHost(nhid)
			}
		}
	default:
		break
	}
}

func (dnr *DynamicNodeRegistry) addRaftNodeHost(nhid, raftAddress string) {
	dnr.mu.Lock()
	_, ok := dnr.raftAddrs[nhid]
	if !ok {
		dnr.raftAddrs[nhid] = raftAddress
	}
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) removeRaftNodeHost(nhid string) {
	dnr.mu.Lock()
	delete(dnr.raftAddrs, nhid)
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) addGRPCNodeHost(nhid, raftAddress string) {
	dnr.mu.Lock()
	_, ok := dnr.grpcAddrs[nhid]
	if !ok {
		dnr.grpcAddrs[nhid] = raftAddress
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

func (dnr *DynamicNodeRegistry) NumMembers() int {
	dnr.mu.RLock()
	defer dnr.mu.RUnlock()
	return len(dnr.raftAddrs)
}

// Add adds a new node with its known NodeHostID to the registry.
func (dnr *DynamicNodeRegistry) Add(clusterID uint64, nodeID uint64, target string) {
	if dnr.validator != nil && !dnr.validator(target) {
		log.Errorf("Add(%d, %d, %q) failed, target did not validate.", clusterID, nodeID, target)
		return
	}
	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)

	dnr.mu.Lock()
	defer dnr.mu.Unlock()
	dnr.nodeTargets[nodeInfo] = target
}

// Remove removes the specified node from the registry.
func (dnr *DynamicNodeRegistry) Remove(clusterID uint64, nodeID uint64) {
	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)

	dnr.mu.Lock()
	defer dnr.mu.Unlock()
	delete(dnr.nodeTargets, nodeInfo)
}

// RemoveCluster removes the specified node from the registry.
func (dnr *DynamicNodeRegistry) RemoveCluster(clusterID uint64) {
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
	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)
	target, ok := dnr.nodeTargets[nodeInfo]
	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	key := dnr.getConnectionKey(target, clusterID)
	return target, key, nil
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
	dnr.mu.RLock()
	defer dnr.mu.RUnlock()

	target, key, err := dnr.resolveNHID(clusterID, nodeID)
	if err != nil {
		return "", "", err
	}

	if target == dnr.nhid {
		return dnr.raftAddress, key, nil
	}

	addr, ok := dnr.raftAddrs[target]
	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	return addr, key, nil
}

func (dnr *DynamicNodeRegistry) ResolveGRPC(clusterID uint64, nodeID uint64) (string, string, error) {
	dnr.mu.RLock()
	defer dnr.mu.RUnlock()

	target, key, err := dnr.resolveNHID(clusterID, nodeID)
	if err != nil {
		return "", "", err
	}

	if target == dnr.nhid {
		return dnr.grpcAddress, key, nil
	}

	addr, ok := dnr.grpcAddrs[target]
	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	return addr, key, nil
}
