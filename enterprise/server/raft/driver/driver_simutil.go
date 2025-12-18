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
// Returns the target nhid to transfer lease to, or empty string if no transfer recommended.
func FindRebalanceLeaseOpForSimulation(myNhid string, leaseCounts map[string]int64) string {
	// Create mock Queue with fake storeMap
	mockQueue := &Queue{
		storeMap:  newMockStoreMap(leaseCounts),
		apiClient: newMockAPIClient(),
	}

	// Build fake RangeDescriptor with replicas for each nhid
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: []*rfpb.ReplicaDescriptor{},
	}
	replicaID := uint64(1)
	var localReplicaID uint64
	for nhid := range leaseCounts {
		nhidCopy := nhid
		rd.Replicas = append(rd.Replicas, &rfpb.ReplicaDescriptor{
			ReplicaId: replicaID,
			RangeId:   1,
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
	leaseCounts map[string]int64
}

func newMockStoreMap(leaseCounts map[string]int64) storemap.IStoreMap {
	return &mockStoreMap{leaseCounts: leaseCounts}
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

	return storemap.CreateStoresWithStats(usages)
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
