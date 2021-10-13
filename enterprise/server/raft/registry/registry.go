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
	nhid string
	raftAddress string
	setTagFn func(tagName, tagValue string) error

	validator dbConfig.TargetValidator
	partitioner *fixedPartitioner
	
	mu sync.RWMutex  // PROTECTS(nodeTargets, nodeAddrs)

	// NodeInfo is (clusterID, nodeID)
	// nodeTargets contains a map of nodeInfo -> target
	nodeTargets map[raftio.NodeInfo]string

	// nodeAddrs contains a map of nodehostID -> raftAddress
	// this map is populated by gossip data from serf.
	nodeAddrs map[string]string
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. It will create a single
// DynamicNodeRegistry and use it to resolve all other raft nodes until the
// process shuts down.
func NewDynamicNodeRegistry(nhid, raftAddress string, streamConnections uint64, v dbConfig.TargetValidator) (*DynamicNodeRegistry, error) {
	dnr := &DynamicNodeRegistry{		
		nhid: nhid,
		raftAddress: raftAddress,
		validator: v,
		mu: sync.RWMutex{},
		nodeTargets: make(map[raftio.NodeInfo]string, 0),
		nodeAddrs: make(map[string]string, 0),
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

// MemberEvent is called when a node joins, leaves, or is updated.
func (dnr *DynamicNodeRegistry) MemberEvent(updateType serf.EventType, member *serf.Member) {
	switch updateType {
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		if nhid, ok := member.Tags[constants.NodeHostIDTag]; ok {
			if raftAddress, ok := member.Tags[constants.RaftAddressTag]; ok {
				if raftAddress != "" {
					dnr.addNodeHost(nhid, raftAddress)
				} else {
					dnr.removeNodeHost(nhid)
				}
			}
		}
	case serf.EventMemberLeave:
		if nhid, ok := member.Tags[constants.NodeHostIDTag]; ok {
			dnr.removeNodeHost(nhid)
		}
	default:
		break
	}
}		

func (dnr *DynamicNodeRegistry) addNodeHost(nhid, raftAddress string) {
	dnr.mu.Lock()
	_, ok := dnr.nodeAddrs[nhid]
	if !ok {
		dnr.nodeAddrs[nhid] = raftAddress
	}
	dnr.mu.Unlock()
}

func (dnr *DynamicNodeRegistry) removeNodeHost(nhid string) {
	dnr.mu.Lock()
	delete(dnr.nodeAddrs, nhid)
	dnr.mu.Unlock()
}

// RegisterTagProviderFn gets a callback function that can  be used to set tags.
func (dnr *DynamicNodeRegistry) RegisterTagProviderFn(setTagFn func(tagName, tagValue string) error) {
	dnr.setTagFn = setTagFn	
}

func (dnr *DynamicNodeRegistry) Close() error {
	return dnr.setTagFn(constants.RaftAddressTag, "")
}

func (dnr *DynamicNodeRegistry) NumMembers() int {
	dnr.mu.RLock()
	defer dnr.mu.RUnlock()
	return len(dnr.nodeAddrs)
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

// Resolve returns the current RaftAddress and connection key of the specified
// node. It returns ErrUnknownTarget when the RaftAddress is unknown.

// Lookups are a two-step process. First we lookup the target and partition key.
// Same as "Resolve" method:
//     target, key, err := nodeTargets(clusterID, nodeID)
// and then we take that target and lookup the actual raft address. Same as
// "GetRaftAddress" method:
//     addr, ok := nodeAddrs(target)
// Finally we return the address, partitionKey, and any error.
func (dnr *DynamicNodeRegistry) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	dnr.mu.RLock()
	defer dnr.mu.RUnlock()

	nodeInfo := raftio.GetNodeInfo(clusterID, nodeID)
	target, ok := dnr.nodeTargets[nodeInfo]
	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	key := dnr.getConnectionKey(target, clusterID)

	if target == dnr.nhid {
		return dnr.raftAddress, key, nil
	}

        addr, ok := dnr.nodeAddrs[target]
	if !ok {
		return "", "", status.NotFoundError("target address unknown")
	}
	return addr, key, nil
}
