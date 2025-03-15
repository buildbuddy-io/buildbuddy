package driver

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type testStoreMap struct {
	usages map[string]*rfpb.StoreUsage
}

func newTestStoreMap(usages []*rfpb.StoreUsage) *testStoreMap {
	m := make(map[string]*rfpb.StoreUsage)
	for _, su := range usages {
		m[su.GetNode().GetNhid()] = su
	}
	return &testStoreMap{usages: m}
}

func (tsm *testStoreMap) GetStoresWithStats() *storemap.StoresWithStats { return nil }

func (tsm *testStoreMap) GetStoresWithStatsFromIDs(nhids []string) *storemap.StoresWithStats {
	usages := make([]*rfpb.StoreUsage, 0, len(nhids))
	for _, nhid := range nhids {
		if su, ok := tsm.usages[nhid]; ok {
			usages = append(usages, su)
		}
	}
	return storemap.CreateStoresWithStats(usages)
}

func (tsm *testStoreMap) DivideByStatus(repls []*rfpb.ReplicaDescriptor) *storemap.ReplicasByStatus {
	return nil
}

func (tsm *testStoreMap) AllAvailableStoresReady() bool {
	return true
}

func TestCandidateComparison(t *testing.T) {
	expected := []*candidate{
		// Candidate with full disk
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-1"}},
			fullDisk:              true,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          1010,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-2"}},
			fullDisk:              true,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          1000,
		},
		// Candidate with range count far above the mean
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-3"}},
			fullDisk:              false,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          1000,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-4"}},
			fullDisk:              false,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          990,
		},
		// Candidate with range count around the mean
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-5"}},
			fullDisk:              false,
			replicaCountMeanLevel: aroundMean,
			replicaCount:          810,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-6"}},
			fullDisk:              false,
			replicaCountMeanLevel: aroundMean,
			replicaCount:          800,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-7"}},
			fullDisk:              false,
			replicaCountMeanLevel: aroundMean,
			replicaCount:          790,
		},
		// Candidate with range count far below the mean
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-8"}},
			fullDisk:              false,
			replicaCountMeanLevel: belowMean,
			replicaCount:          500,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-9"}},
			fullDisk:              false,
			replicaCountMeanLevel: belowMean,
			replicaCount:          400,
		},
	}

	candidates := make([]*candidate, len(expected))
	copy(candidates, expected)

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	require.Equal(t, len(expected), len(candidates))

	slices.SortFunc(candidates, compareByScore)

	expectedOrder := make([]string, 0, len(expected))
	for _, c := range expected {
		expectedOrder = append(expectedOrder, c.usage.GetNode().GetNhid())
	}
	actualOrder := make([]string, 0, len(candidates))
	for _, c := range candidates {
		actualOrder = append(actualOrder, c.usage.GetNode().GetNhid())
	}

	require.Equal(t, expectedOrder, actualOrder)

}

func TestFindNodeForAllocation(t *testing.T) {
	tests := []struct {
		desc     string
		usages   []*rfpb.StoreUsage
		rd       *rfpb.RangeDescriptor
		expected *rfpb.NodeDescriptor
	}{
		{
			desc: "skip-node-with-range",
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   1,
					TotalBytesUsed: 5,
					TotalBytesFree: 990,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   4,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   3,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			desc: "skip-node-with-range-to-be-removed",
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   1,
					TotalBytesUsed: 5,
					TotalBytesFree: 990,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   3,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   4,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
				},
				Removed: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			desc: "skip-node-with-full-disk",
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   1,
					TotalBytesUsed: 990,
					TotalBytesFree: 10,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   4,
					TotalBytesUsed: 960,
					TotalBytesFree: 10,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   3,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			desc: "find-node-with-least-ranges",
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   10,
					TotalBytesUsed: 990,
					TotalBytesFree: 10,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   5,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   3,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rq := &Queue{log: log.NamedSubLogger("test")}
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			actual := rq.findNodeForAllocation(tc.rd, storesWithStats)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}

func TestFindReplicaForRemoval(t *testing.T) {
	localReplicaID := uint64(1)
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	withinGracePeriodTS := now.Add(-3 * time.Minute).UnixMicro()
	outsideGracePeriodTS := now.Add(-10 * time.Minute).UnixMicro()
	tests := []struct {
		desc            string
		rd              *rfpb.RangeDescriptor
		replicaStateMap map[uint64]constants.ReplicaState
		usages          []*rfpb.StoreUsage
		expected        *rfpb.ReplicaDescriptor
	}{
		{
			// 4 replicas and 2 of them are current, so we can only delete replicas
			// that are behind; but we don't want to consider the newly added
			// replica within the grace period as behind.
			desc: "do-not-delete-newly-added-replica",
			rd: &rfpb.RangeDescriptor{
				RangeId:                1,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(withinGracePeriodTS),
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
					{RangeId: 1, ReplicaId: 4, Nhid: proto.String("nhid-4")},
				},
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateBehind,
				4: constants.ReplicaStateBehind,
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   10,
					TotalBytesUsed: 990,
					TotalBytesFree: 10,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   5,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   3,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
			},
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   1,
				ReplicaId: 3,
				Nhid:      proto.String("nhid-3"),
			},
		},
		{
			// 4 replicas that are all current, we can delete any of them; but
			// we prefer to delete the one with full disk. When there are multiple
			// nodes with full disk, we choose one with higher replica count, even
			// if this is a newly added replica.
			desc: "delete-replica-with-full-disk",
			rd: &rfpb.RangeDescriptor{
				RangeId:                1,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
					{RangeId: 1, ReplicaId: 4, Nhid: proto.String("nhid-4")},
				},
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateCurrent,
				4: constants.ReplicaStateCurrent,
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   10,
					TotalBytesUsed: 955,
					TotalBytesFree: 45,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   5,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   12,
					TotalBytesUsed: 960,
					TotalBytesFree: 40,
				},
			},
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   1,
				ReplicaId: 4,
				Nhid:      proto.String("nhid-4"),
			},
		},
		{
			// Newly added replica will be considered behind if grace period passed
			// and it is still behind.
			// 4 replicas and 2 of them are current, so we can only delete replicas
			// that are behind, which is replica 3 and 4. In this case, we want
			// to delete replica 4 because it's far above mean replica count.
			desc: "newly-added-replica-grace-period-pass",
			rd: &rfpb.RangeDescriptor{
				RangeId:                1,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
					{RangeId: 1, ReplicaId: 4, Nhid: proto.String("nhid-4")},
				},
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateBehind,
				4: constants.ReplicaStateBehind,
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   10,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   11,
					TotalBytesUsed: 955,
					TotalBytesFree: 45,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   5,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   14,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
			},
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   1,
				ReplicaId: 4,
				Nhid:      proto.String("nhid-4"),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			storeMap := newTestStoreMap(tc.usages)
			rq := &Queue{log: log.NamedSubLogger("test"), clock: clock, storeMap: storeMap}
			actual := rq.findReplicaForRemoval(tc.rd, tc.replicaStateMap, localReplicaID)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}

func TestRebalanceReplica(t *testing.T) {
	localReplicaID := uint64(1)
	tests := []struct {
		desc     string
		usages   []*rfpb.StoreUsage
		rd       *rfpb.RangeDescriptor
		expected *rebalanceOp
	}{
		{
			desc: "move-range-to-new-node",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   700,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   600,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   500,
					TotalBytesUsed: 50,
					TotalBytesFree: 950,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   200,
					TotalBytesUsed: 50,
					TotalBytesFree: 950,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-5"},
					ReplicaCount:   0,
					TotalBytesUsed: 0,
					TotalBytesFree: 1000,
				},
			},
			// Even though it's better to move range from nhid-1 to nhid-5. Since
			// we are running on nhid-1, we will skip nhid-1 to choose the second-
			// best option.
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			desc: "not-select-node-with-replica-to-be-removed",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
				Removed: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 5, Nhid: proto.String("nhid-5")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   700,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   600,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   500,
					TotalBytesUsed: 50,
					TotalBytesFree: 950,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   0,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-5"},
					ReplicaCount:   0,
					TotalBytesUsed: 0,
					TotalBytesFree: 1000,
				},
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-4"},
			},
		},
		{
			desc: "move-range-to-node-far-below-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   400,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				// Replica count is slightly above the mean: 400, but below
				// overfull threshold: 420.
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   410,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				// Replica count is slightly above the mean: 400, but below
				// overfull threshold: 420.
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   405,
					TotalBytesUsed: 50,
					TotalBytesFree: 950,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   595,
					TotalBytesUsed: 50,
					TotalBytesFree: 950,
				},
				// Replica count is far below the mean.
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-5"},
					ReplicaCount:   190,
					TotalBytesUsed: 50,
					TotalBytesFree: 950,
				},
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			desc: "move-range-from-full-disk",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   400,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   400,
					TotalBytesUsed: 800,
					TotalBytesFree: 200,
				},
				// disk usage percent: 95.5%
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   400,
					TotalBytesUsed: 955,
					TotalBytesFree: 45,
				},
				// disk usage percent: 93% > maxDiskCapacityForRebalance. We should
				// not choose this node to rebalance to.
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   350,
					TotalBytesUsed: 930,
					TotalBytesFree: 70,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-5"},
					ReplicaCount:   650,
					TotalBytesUsed: 900,
					TotalBytesFree: 1100,
				},
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-3"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			desc: "no-reblance-when-around-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					ReplicaCount:   400,
					TotalBytesUsed: 100,
					TotalBytesFree: 900,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					ReplicaCount:   395,
					TotalBytesUsed: 800,
					TotalBytesFree: 200,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					ReplicaCount:   410,
					TotalBytesUsed: 900,
					TotalBytesFree: 100,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					ReplicaCount:   405,
					TotalBytesUsed: 900,
					TotalBytesFree: 100,
				},
				{
					Node:           &rfpb.NodeDescriptor{Nhid: "nhid-5"},
					ReplicaCount:   390,
					TotalBytesUsed: 900,
					TotalBytesFree: 1100,
				},
			},
			expected: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			storeMap := newTestStoreMap(tc.usages)
			rq := &Queue{log: log.NamedSubLogger("test"), storeMap: storeMap}
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			actual := rq.findRebalanceReplicaOp(tc.rd, storesWithStats, localReplicaID)
			if tc.expected != nil {
				require.NotNil(t, actual)
				require.Equal(t, tc.expected.from.nhid, actual.from.nhid)
				require.Equal(t, tc.expected.to.nhid, actual.to.nhid)
			} else {
				require.Nil(t, actual)
			}
		})
	}
}

func TestRebalanceLeases(t *testing.T) {
	localReplicaID := uint64(1)
	tests := []struct {
		desc     string
		usages   []*rfpb.StoreUsage
		rd       *rfpb.RangeDescriptor
		expected *rebalanceOp
	}{
		{
			desc: "move-lease-to-node-far-below-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					LeaseCount: 70,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					LeaseCount: 10,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					LeaseCount: 20,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					LeaseCount: 20,
				},
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-1"},
				to:   &candidate{nhid: "nhid-2"},
			},
		},
		{
			desc: "no-reblance-when-around-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					LeaseCount: 30,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					LeaseCount: 31,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					LeaseCount: 29,
				},
			},
			expected: nil,
		},
		{
			desc: "no-rebalance-with-good-choice",
			rd: &rfpb.RangeDescriptor{
				RangeId: 1,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")}, // local
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			usages: []*rfpb.StoreUsage{
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-1"},
					LeaseCount: 70,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-2"},
					LeaseCount: 69,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-3"},
					LeaseCount: 69,
				},
				{
					Node:       &rfpb.NodeDescriptor{Nhid: "nhid-4"},
					LeaseCount: 5,
				},
			},
			expected: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			storeMap := newTestStoreMap(tc.usages)
			rq := &Queue{log: log.NamedSubLogger("test"), storeMap: storeMap}
			actual := rq.findRebalanceLeaseOp(tc.rd, localReplicaID)
			if tc.expected != nil {
				require.NotNil(t, actual)
				require.Equal(t, tc.expected.from.nhid, actual.from.nhid)
				require.Equal(t, tc.expected.to.nhid, actual.to.nhid)
			} else {
				if actual != nil {
					log.Infof("actual: from: %s to %s", actual.from.nhid, actual.to.nhid)
				}
				require.Nil(t, actual)
			}
		})
	}
}
