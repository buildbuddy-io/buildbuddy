package driver

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

// FindRebalanceLeaseOpForSimulation wraps the production lease rebalancing logic
// for use in simulators and tests. Assumes all nodes are reachable.
//
// Parameters:
//   - myNhid: the node ID making the decision
//   - shardID: the shard ID being evaluated
//   - replicas: the 3 node IDs that have replicas for this shard
//   - leaseCounts: lease counts for all nodes in the cluster
//
// Returns the target nhid to transfer lease to, or empty string if no transfer recommended.
func FindRebalanceLeaseOpForSimulation(myNhid string, shardID int64, replicas []string, leaseCounts map[string]int64) string {
	// Create mock Queue with fake storeMap
	mockQueue := &Queue{
		storeMap:  newMockStoreMap(leaseCounts),
		apiClient: newMockAPIClient(),
	}

	// Build RangeDescriptor with ONLY the 3 replicas for this shard
	rd := &rfpb.RangeDescriptor{
		RangeId:  uint64(shardID),
		Replicas: []*rfpb.ReplicaDescriptor{},
	}
	replicaID := uint64(1)
	var localReplicaID uint64
	for _, nhid := range replicas {
		nhidCopy := nhid
		rd.Replicas = append(rd.Replicas, &rfpb.ReplicaDescriptor{
			ReplicaId: replicaID,
			RangeId:   uint64(shardID),
			Nhid:      &nhidCopy,
		})
		if nhid == myNhid {
			localReplicaID = replicaID
		}
		replicaID++
	}

	// Call the REAL production function
	op := mockQueue.findRebalanceLeaseOp(context.Background(), rd, localReplicaID)

	if op == nil {
		return "" // No transfer recommended
	}

	return op.to.nhid
}

// FindRebalanceLeaseOpWithMeanOverride wraps production logic with optional mean override
// for testing alternative mean calculation strategies.
//
// Parameters:
//   - myNhid: the node ID making the decision
//   - shardID: the shard ID being evaluated
//   - replicas: the 3 node IDs that have replicas for this shard
//   - leaseCounts: lease counts for all nodes in the cluster
//   - meanOverride: if provided, overrides the calculated mean lease count
//
// Returns the target nhid to transfer lease to, or empty string if no transfer recommended.
func FindRebalanceLeaseOpWithMeanOverride(myNhid string, shardID int64, replicas []string, leaseCounts map[string]int64, meanOverride *float64) string {
	var storeMap storemap.IStoreMap

	if meanOverride != nil {
		storeMap = newMockStoreMapWithMeanOverride(leaseCounts, *meanOverride)
	} else {
		storeMap = newMockStoreMap(leaseCounts)
	}

	mockQueue := &Queue{
		storeMap:  storeMap,
		apiClient: newMockAPIClient(),
	}

	// Build RangeDescriptor with ONLY the 3 replicas for this shard
	rd := &rfpb.RangeDescriptor{
		RangeId:  uint64(shardID),
		Replicas: []*rfpb.ReplicaDescriptor{},
	}
	replicaID := uint64(1)
	var localReplicaID uint64
	for _, nhid := range replicas {
		nhidCopy := nhid
		rd.Replicas = append(rd.Replicas, &rfpb.ReplicaDescriptor{
			ReplicaId: replicaID,
			RangeId:   uint64(shardID),
			Nhid:      &nhidCopy,
		})
		if nhid == myNhid {
			localReplicaID = replicaID
		}
		replicaID++
	}

	// Call the REAL production function
	op := mockQueue.findRebalanceLeaseOp(context.Background(), rd, localReplicaID)

	if op == nil {
		return "" // No transfer recommended
	}

	return op.to.nhid
}

// mockStoreMap implements storemap.IStoreMap for testing
type mockStoreMap struct {
	leaseCounts  map[string]int64
	meanOverride *float64 // Optional mean override
}

func newMockStoreMap(leaseCounts map[string]int64) storemap.IStoreMap {
	return &mockStoreMap{leaseCounts: leaseCounts, meanOverride: nil}
}

func newMockStoreMapWithMeanOverride(leaseCounts map[string]int64, meanOverride float64) storemap.IStoreMap {
	return &mockStoreMap{leaseCounts: leaseCounts, meanOverride: &meanOverride}
}

func (m *mockStoreMap) GetStoresWithStats() *storemap.StoresWithStats {
	return m.GetStoresWithStatsFromIDs(nil)
}

func (m *mockStoreMap) GetStoresWithStatsFromIDs(nhids []string) *storemap.StoresWithStats {
	usages := []*rfpb.StoreUsage{}

	for nhid, count := range m.leaseCounts {
		usages = append(usages, &rfpb.StoreUsage{
			Node: &rfpb.NodeDescriptor{
				Nhid: nhid,
			},
			LeaseCount: count,
		})
	}

	stats := storemap.CreateStoresWithStats(usages)

	// Override the mean if provided (for theoretical mean testing)
	if m.meanOverride != nil {
		stats.LeaseCount.Mean = *m.meanOverride
	}

	return stats
}

func (m *mockStoreMap) DivideByStatus(repls []*rfpb.ReplicaDescriptor) *storemap.ReplicasByStatus {
	// Not needed for lease rebalancing
	return &storemap.ReplicasByStatus{}
}

func (m *mockStoreMap) AllAvailableStoresReady() bool {
	return true
}

// mockAPIClient implements IClient for testing
type mockAPIClient struct{}

func newMockAPIClient() IClient {
	return &mockAPIClient{}
}

func (m *mockAPIClient) HaveReadyConnections(ctx context.Context, rd *rfpb.ReplicaDescriptor) (bool, error) {
	return true, nil // All nodes reachable
}

func (m *mockAPIClient) GetForReplica(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	// Not needed for lease rebalancing decision
	return nil, nil
}
