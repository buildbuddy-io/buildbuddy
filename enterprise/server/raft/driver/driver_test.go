package driver

import (
	"context"
	"fmt"
	"math"
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
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

type testStoreMap struct {
	usages           map[string]*rfpb.StoreUsage
	replicasByStatus *storemap.ReplicasByStatus
}

func newTestStoreMap(usages []*rfpb.StoreUsage, replicasByStatus *storemap.ReplicasByStatus) *testStoreMap {
	m := make(map[string]*rfpb.StoreUsage)
	for _, su := range usages {
		m[su.GetNode().GetNhid()] = su
	}
	return &testStoreMap{usages: m, replicasByStatus: replicasByStatus}
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
	return tsm.replicasByStatus
}

func (tsm *testStoreMap) AllStoresAvailableAndReady() bool {
	return true
}

func (tsm *testStoreMap) CheckLeaseRebalancePrecondition() (float64, bool) {
	totalReplicaCount := 0
	totalLeaseCount := 0
	for _, usage := range tsm.usages {
		totalReplicaCount += int(usage.GetReplicaCount())
		totalLeaseCount += int(usage.GetLeaseCount())
	}
	// Each range (except for the meta range) by default has 3
	// replicas and the meta range have 5 replicas.
	// Therefore,
	// total replica count = (total shard count - 1) * 3 + 5.
	totalShardCount := (totalReplicaCount-5)/3 + 1
	// Estimate the number of ranges without a lease.
	delta := totalShardCount - totalLeaseCount
	if delta > 10 {
		return 0.0, false
	}
	return float64(totalShardCount) / float64(len(tsm.usages)), true
}

func replicaKey(rd *rfpb.ReplicaDescriptor) string {
	return fmt.Sprintf("c%dn%d", rd.GetRangeId(), rd.GetReplicaId())
}

type testClient struct {
	repls map[string]bool
}

func (tc *testClient) HaveReadyConnections(ctx context.Context, rd *rfpb.ReplicaDescriptor) (bool, error) {
	key := replicaKey(rd)
	return tc.repls[key], nil
}

func (tc *testClient) GetForReplica(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	return nil, nil
}

// usage builds a StoreUsage. Tests that don't care about disk usage
// commonly pass 100/900 (well below the full-disk threshold).
func usage(nhid, zone string, replicaCount, used, free int64) *rfpb.StoreUsage {
	return &rfpb.StoreUsage{
		Node:           &rfpb.NodeDescriptor{Nhid: nhid, Zone: zone},
		ReplicaCount:   replicaCount,
		TotalBytesUsed: used,
		TotalBytesFree: free,
	}
}

// leaseUsage builds a StoreUsage for lease-rebalance tests (no zone, no disk).
func leaseUsage(nhid string, leaseCount, replicaCount int64) *rfpb.StoreUsage {
	return &rfpb.StoreUsage{
		Node:         &rfpb.NodeDescriptor{Nhid: nhid},
		LeaseCount:   leaseCount,
		ReplicaCount: replicaCount,
	}
}

// replicas builds ReplicaDescriptors with sequential replica IDs starting at 1.
func replicas(rangeID uint64, nhids ...string) []*rfpb.ReplicaDescriptor {
	out := make([]*rfpb.ReplicaDescriptor, len(nhids))
	for i, nhid := range nhids {
		out[i] = &rfpb.ReplicaDescriptor{
			RangeId:   rangeID,
			ReplicaId: uint64(i + 1),
			Nhid:      proto.String(nhid),
		}
	}
	return out
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
		desc                string
		usages              []*rfpb.StoreUsage
		rd                  *rfpb.RangeDescriptor
		expected            *rfpb.NodeDescriptor
		minReplicasPerRange int
	}{
		{
			desc: "skip-node-with-range",
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 1, 5, 990),
				usage("nhid-3", "", 4, 100, 900),
				usage("nhid-4", "", 3, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			desc: "skip-node-with-range-to-be-removed",
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 1, 5, 990),
				usage("nhid-3", "", 3, 100, 900),
				usage("nhid-4", "", 4, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2"),
				Removed: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			desc: "skip-node-with-full-disk",
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 1, 990, 10),
				usage("nhid-3", "", 4, 960, 10),
				usage("nhid-4", "", 3, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1"),
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			desc: "choose-node-with-staging",
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 1, 100, 900),
				usage("nhid-3", "", 4, 100, 10),
				usage("nhid-4", "", 3, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1"),
				Staging: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-2"},
		},
		{
			desc: "find-node-with-least-ranges",
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 10, 990, 10),
				usage("nhid-3", "", 5, 100, 900),
				usage("nhid-4", "", 3, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1"),
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
		{
			// 3 zones, 2 existing replicas in zone-a. Should allocate in
			// zone-b or zone-c (not zone-a) to balance across zones.
			// targetMax = ceil(3/3) = 1, zone-a already has 2 > 1.
			desc:                "zone-aware-prefer-underrepresented-zone",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 10, 100, 900),
				usage("nhid-3", "zone-a", 1, 10, 990),
				usage("nhid-4", "zone-b", 5, 100, 900),
				usage("nhid-5", "zone-c", 3, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2"),
			},
			// nhid-3 is in zone-a (already at 2, targetMax=1), filtered out.
			// nhid-5 (zone-c, replicaCount=3) beats nhid-4 (zone-b, replicaCount=5).
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-5", Zone: "zone-c"},
		},
		{
			// 2 zones, 1 existing replica in zone-a, 0 in zone-b.
			// targetMin = 3/2 = 1, targetMax = ceil(3/2) = 2.
			// zone-b has 0 replicas < targetMin=1, so zone-b is preferred
			// even though zone-a also has room (1 < targetMax=2).
			desc:                "zone-aware-prefer-empty-zone",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 1, 10, 990),
				usage("nhid-3", "zone-b", 5, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1"),
			},
			// zone-b (0 replicas) is below targetMin=1, so only zone-b
			// candidates are considered. nhid-3 is the only option.
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-3", Zone: "zone-b"},
		},
		{
			// 2 zones, both at targetMin=1. targetMax=2.
			// First filter (< targetMin=1) finds nothing.
			// Second filter (< targetMax=2) passes both zones.
			// Best candidate by score wins.
			desc:                "zone-aware-both-at-min-falls-through-to-max",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 1, 10, 990),
				usage("nhid-3", "zone-b", 5, 100, 900),
				usage("nhid-4", "zone-b", 10, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 2,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 2, ReplicaId: 1, Nhid: proto.String("nhid-1")},
					{RangeId: 2, ReplicaId: 2, Nhid: proto.String("nhid-4")},
				},
			},
			// Both zones at 1 replica (= targetMin). First filter empty.
			// Second filter (< targetMax=2) passes nhid-2 (zone-a) and
			// nhid-3 (zone-b). nhid-2 wins by score (replicaCount=1).
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-2", Zone: "zone-a"},
		},
		{
			// All candidates are in zone-a which already has 2 replicas.
			// zone-b exists (has a replica) but no candidates there.
			// targetMax = ceil(3/2) = 2, zone-a has 2 which is NOT < 2,
			// so all candidates are filtered out → fallback to unfiltered.
			desc:                "zone-aware-fallback-all-candidates-filtered",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 10, 100, 900),
				usage("nhid-3", "zone-a", 1, 10, 990),
				usage("nhid-4", "zone-b", 5, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 2,
				Replicas: []*rfpb.ReplicaDescriptor{
					{RangeId: 2, ReplicaId: 1, Nhid: proto.String("nhid-1")},
					{RangeId: 2, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{RangeId: 2, ReplicaId: 3, Nhid: proto.String("nhid-4")},
				},
			},
			// Only candidate is nhid-3 (zone-a). Zone-a has 2 replicas,
			// targetMax=2, 2 < 2 is false → filtered out → fallback picks nhid-3.
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-3", Zone: "zone-a"},
		},
		{
			// 4 zones in 4-3-2-1 distribution. The absolute-min zone (zone-d,
			// count 1) has no available candidate because its only store
			// already holds the replica. The next-best target is zone-c
			// (count 2), not zone-b (count 3). Even though zone-b's
			// candidate has the best load score, zone count dominates.
			desc:                "zone-aware-fall-back-to-next-under-zone",
			minReplicasPerRange: 10,
			usages: []*rfpb.StoreUsage{
				// zone-a: 4 stores, all holding replicas.
				usage("nhid-a1", "zone-a", 10, 100, 900),
				usage("nhid-a2", "zone-a", 10, 100, 900),
				usage("nhid-a3", "zone-a", 10, 100, 900),
				usage("nhid-a4", "zone-a", 10, 100, 900),
				// zone-b: 4 stores, 3 hold replicas. nhid-b4 is a candidate
				// with very low load — would win on load tiebreak.
				usage("nhid-b1", "zone-b", 10, 100, 900),
				usage("nhid-b2", "zone-b", 10, 100, 900),
				usage("nhid-b3", "zone-b", 10, 100, 900),
				usage("nhid-b4", "zone-b", 1, 10, 990),
				// zone-c: 3 stores, 2 hold replicas. nhid-c3 is a candidate.
				usage("nhid-c1", "zone-c", 10, 100, 900),
				usage("nhid-c2", "zone-c", 10, 100, 900),
				usage("nhid-c3", "zone-c", 10, 100, 900),
				// zone-d: 1 store, holds the replica. No candidate available.
				usage("nhid-d1", "zone-d", 10, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId: 2,
				Replicas: replicas(2,
					"nhid-a1", "nhid-a2", "nhid-a3", "nhid-a4",
					"nhid-b1", "nhid-b2", "nhid-b3",
					"nhid-c1", "nhid-c2",
					"nhid-d1",
				),
			},
			// replicasByZone: a=4, b=3, c=2, d=1. Candidates: nhid-b4 (zone-b,
			// count 3), nhid-c3 (zone-c, count 2). zone-c has fewer replicas
			// so nhid-c3 wins despite nhid-b4 having far better load.
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-c3", Zone: "zone-c"},
		},
		{
			// 3 zones with 3 distinct zone-counts (2-1-0). Adding the next
			// replica must go to zone-c (count 0) to reach 2-1-1, even though
			// zone-b's candidate has a lower replica count. Without the
			// "absolute-min" preference, the load tiebreak would pick
			// zone-b's nhid-4 and overshoot to 2-2-0.
			desc:                "zone-aware-prefer-most-under-represented-zone",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 10, 100, 900),
				usage("nhid-3", "zone-b", 10, 100, 900),
				// zone-b candidate with low load — would beat nhid-5 by score
				// if both were considered, but zone-b is already at currentMin+1.
				usage("nhid-4", "zone-b", 1, 10, 990),
				// zone-c is the empty zone (most under-represented).
				usage("nhid-5", "zone-c", 5, 100, 900),
			},
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3"),
			},
			// replicasByZone: a=2, b=1, c=0. currentMin=0, currentMax=2.
			// First try keeps only zone-c (count <= 0) → nhid-5.
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-5", Zone: "zone-c"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rq := &Queue{
				minReplicasPerRange: tc.minReplicasPerRange,
			}
			rq.baseQueue = &baseQueue{log: log.NamedSubLogger("test"), impl: rq}
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			actual := rq.findNodeForAllocation(tc.rd, storesWithStats)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}

func TestFindNodesForAllocation(t *testing.T) {
	tests := []struct {
		desc                string
		usages              []*rfpb.StoreUsage
		minReplicasPerRange int
		expectedNhids       []string
	}{
		{
			// 3 zones with 5 candidates, minReplicas=3.
			// targetMax = ceil(3/3) = 1, so pick one per zone.
			desc:                "spread-across-3-zones",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 1, 10, 990),
				usage("nhid-2", "zone-a", 5, 100, 900),
				usage("nhid-3", "zone-b", 2, 20, 980),
				usage("nhid-4", "zone-b", 6, 100, 900),
				usage("nhid-5", "zone-c", 3, 50, 950),
			},
			// Sorted by score (best first): nhid-1, nhid-3, nhid-5, nhid-2, nhid-4.
			// Greedy: nhid-1 (zone-a: 0<1), nhid-3 (zone-b: 0<1), nhid-5 (zone-c: 0<1). Done.
			expectedNhids: []string{"nhid-1", "nhid-3", "nhid-5"},
		},
		{
			// 2 zones with 4 candidates, minReplicas=3.
			// targetMax = ceil(3/2) = 2, so pick at most 2 per zone.
			desc:                "spread-across-2-zones",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 1, 10, 990),
				usage("nhid-2", "zone-a", 2, 20, 980),
				usage("nhid-3", "zone-b", 3, 50, 950),
				usage("nhid-4", "zone-b", 10, 100, 900),
			},
			// Sorted by score (best first): nhid-1, nhid-2, nhid-3, nhid-4.
			// Pass 1 (fill to targetMin=1): nhid-1 (zone-a: 0<1)✓,
			//   nhid-2 (zone-a: 1<1)✗, nhid-3 (zone-b: 0<1)✓.
			// Pass 2 (top up to targetMax=2): nhid-2 (zone-a: 1<2)✓.
			expectedNhids: []string{"nhid-1", "nhid-3", "nhid-2"},
		},
		{
			// 2 zones but one zone has too few candidates to fill its share.
			// targetMin = 5/2 = 2, targetMax = ceil(5/2) = 3. zone-b only has
			// 1 candidate, so the first pass fills zone-a to 2 and zone-b to
			// 1; the second pass tops up zone-a to 3; the third pass picks the
			// remaining zone-a candidate to reach 5.
			desc:                "second-pass-needed-uneven-zones",
			minReplicasPerRange: 5,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 1, 10, 990),
				usage("nhid-2", "zone-a", 2, 20, 980),
				usage("nhid-3", "zone-a", 3, 50, 950),
				usage("nhid-4", "zone-a", 4, 100, 900),
				usage("nhid-5", "zone-b", 5, 100, 900),
			},
			// Sorted by score: nhid-1, nhid-2, nhid-3, nhid-4, nhid-5.
			// Pass 1 (fill to targetMin=2): nhid-1 (a:0<2)✓, nhid-2 (a:1<2)✓,
			//   nhid-3 (a:2<2)✗, nhid-4 (a:2<2)✗, nhid-5 (b:0<2)✓ → 3 selected.
			// Pass 2 (top up to targetMax=3): nhid-3 (a:2<3)✓ → 4 selected.
			// Pass 3 (fill remaining): nhid-4 fills the 5th slot.
			expectedNhids: []string{"nhid-1", "nhid-2", "nhid-5", "nhid-3", "nhid-4"},
		},
		{
			// 4 replicas across 3 zones. targetMin = 4/3 = 1, targetMax =
			// ceil(4/3) = 2. zone-a has 3 high-scoring candidates; without a
			// targetMin pass, the greedy algorithm would fill zone-a and zone-b
			// to targetMax before reaching zone-c, ending up with 2-2-0 instead
			// of 2-1-1.
			desc:                "non-divisible-spread-across-3-zones",
			minReplicasPerRange: 4,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 1, 10, 990),
				usage("nhid-2", "zone-a", 2, 20, 980),
				usage("nhid-3", "zone-b", 3, 30, 970),
				usage("nhid-4", "zone-b", 4, 40, 960),
				usage("nhid-5", "zone-a", 5, 50, 950),
				usage("nhid-6", "zone-c", 6, 60, 940),
			},
			// Sorted by score: nhid-1 (a), nhid-2 (a), nhid-3 (b), nhid-4 (b),
			// nhid-5 (a), nhid-6 (c).
			// Pass 1 (fill to targetMin=1): nhid-1 (a:0<1)✓, nhid-3 (b:0<1)✓,
			//   nhid-6 (c:0<1)✓ → 3 selected.
			// Pass 2 (top up to targetMax=2): nhid-2 (a:1<2)✓ → 4 selected.
			// Distribution: 2-1-1 across zone-a, zone-b, zone-c.
			expectedNhids: []string{"nhid-1", "nhid-3", "nhid-6", "nhid-2"},
		},
		{
			// Single zone: all candidates in zone-a. numZones=1,
			// targetMax = ceil(3/1) = 3. First pass picks top 3 by score.
			desc:                "single-zone",
			minReplicasPerRange: 3,
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 1, 10, 990),
				usage("nhid-2", "zone-a", 2, 20, 980),
				usage("nhid-3", "zone-a", 3, 50, 950),
				usage("nhid-4", "zone-a", 10, 100, 900),
			},
			expectedNhids: []string{"nhid-1", "nhid-2", "nhid-3"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rq := &Queue{
				minReplicasPerRange: tc.minReplicasPerRange,
			}
			rq.baseQueue = &baseQueue{log: log.NamedSubLogger("test"), impl: rq}
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			nodes := rq.findNodesForAllocation(storesWithStats)
			require.Len(t, nodes, tc.minReplicasPerRange)
			actualNhids := make([]string, 0, len(nodes))
			for _, n := range nodes {
				actualNhids = append(actualNhids, n.GetNhid())
			}
			require.ElementsMatch(t, tc.expectedNhids, actualNhids)
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
		desc                string
		rd                  *rfpb.RangeDescriptor
		replicaStateMap     map[uint64]constants.ReplicaState
		replicasByStatus    *storemap.ReplicasByStatus
		usages              []*rfpb.StoreUsage
		expected            *rfpb.ReplicaDescriptor
		minReplicasPerRange int
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
				Replicas:               replicas(1, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateBehind,
				4: constants.ReplicaStateBehind,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 10, 990, 10),
				usage("nhid-3", "", 5, 100, 900),
				usage("nhid-4", "", 3, 100, 900),
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
				Replicas:               replicas(1, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateCurrent,
				4: constants.ReplicaStateCurrent,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 10, 955, 45),
				usage("nhid-3", "", 5, 100, 900),
				usage("nhid-4", "", 12, 960, 40),
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
				Replicas:               replicas(1, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateBehind,
				4: constants.ReplicaStateBehind,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 10, 100, 900),
				usage("nhid-2", "", 11, 955, 45),
				usage("nhid-3", "", 5, 100, 900),
				usage("nhid-4", "", 14, 100, 900),
			},
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   1,
				ReplicaId: 4,
				Nhid:      proto.String("nhid-4"),
			},
		},
		{
			// 3 zones, distribution 2-1-1 with 4 replicas (removing to reach 3).
			// targetMax = ceil(3/3) = 1, zone-a has 2 > 1 → remove from zone-a.
			desc:                "zone-aware-remove-from-overrepresented-zone",
			minReplicasPerRange: 3,
			rd: &rfpb.RangeDescriptor{
				RangeId:                2,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				// nhid-1 local: zone-a, nhid-2: zone-a, nhid-3: zone-b, nhid-4: zone-c.
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateCurrent,
				4: constants.ReplicaStateCurrent,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 8, 100, 900),
				usage("nhid-3", "zone-b", 5, 100, 900),
				usage("nhid-4", "zone-c", 3, 100, 900),
			},
			// Only zone-a candidates (nhid-2) should be considered. nhid-1 is local.
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   2,
				ReplicaId: 2,
				Nhid:      proto.String("nhid-2"),
			},
		},
		{
			// 2 zones, distribution 3-1 with 4 replicas (removing to reach 3).
			// targetMax = ceil(3/2) = 2, zone-a has 3 > 2 → remove from zone-a.
			desc:                "zone-aware-remove-from-zone-with-3",
			minReplicasPerRange: 3,
			rd: &rfpb.RangeDescriptor{
				RangeId:                2,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				// nhid-1..3 in zone-a (nhid-1 local), nhid-4 in zone-b.
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateCurrent,
				4: constants.ReplicaStateCurrent,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 12, 100, 900),
				usage("nhid-3", "zone-a", 5, 100, 900),
				usage("nhid-4", "zone-b", 3, 100, 900),
			},
			// nhid-2 has highest replica count in zone-a (overrepresented).
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   2,
				ReplicaId: 2,
				Nhid:      proto.String("nhid-2"),
			},
		},
		{
			// 2 zones, distribution 2-2 with 4 replicas (removing to reach 3).
			// targetMax = ceil(3/2) = 2, no zone > 2 → zone filter produces
			// empty list → fallback to score-based removal.
			desc:                "zone-aware-balanced-no-filtering",
			minReplicasPerRange: 3,
			rd: &rfpb.RangeDescriptor{
				RangeId:                2,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				// nhid-1..2 in zone-a (nhid-1 local), nhid-3..4 in zone-b.
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateCurrent,
				4: constants.ReplicaStateCurrent,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 12, 100, 900),
				usage("nhid-3", "zone-b", 5, 100, 900),
				usage("nhid-4", "zone-b", 3, 100, 900),
			},
			// Both zones have 2 replicas = targetMax. No zone filtering.
			// Score-based: nhid-2 (replicaCount=12) is worst → removed.
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   2,
				ReplicaId: 2,
				Nhid:      proto.String("nhid-2"),
			},
		},
		{
			// 4 replicas in 2-1-1 distribution. zone-a (count 2, the max) has
			// no removable replicas because both zone-a replicas are current
			// and numUpToDate == quorum, so only behind replicas (in zone-b
			// and zone-c) are removable. The current code filters candidates
			// to the absolute-max zone (zone-a) → empty → returns nil instead
			// of removing the most-evictable behind replica.
			desc:                "zone-aware-fall-back-when-max-zone-not-removable",
			minReplicasPerRange: 3,
			rd: &rfpb.RangeDescriptor{
				RangeId:                2,
				LastAddedReplicaId:     proto.Uint64(4),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				// nhid-1..2 in zone-a (nhid-1 local), nhid-3 in zone-b, nhid-4 in zone-c.
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateBehind,
				4: constants.ReplicaStateBehind,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 10, 100, 900),
				// nhid-3 has the highest load → most evictable among removable.
				usage("nhid-3", "zone-b", 15, 100, 900),
				usage("nhid-4", "zone-c", 5, 100, 900),
			},
			// Removable = {r3, r4} (the behind replicas). zone-b and zone-c
			// both have count 1. Pick the worst by load: r3 (replicaCount=15).
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   2,
				ReplicaId: 3,
				Nhid:      proto.String("nhid-3"),
			},
		},
		{
			// 6 replicas in 3-2-1 distribution. zone-a (count 3, the max) has
			// no removable replicas. zone-b (count 2) and zone-c (count 1)
			// both have removable replicas. We should fall back to zone-b
			// (the next-most-over-represented zone among candidates), not
			// to zone-c. The current code filters to the absolute-max zone
			// (zone-a) → empty → returns nil.
			desc:                "zone-aware-fall-back-to-next-most-over-zone",
			minReplicasPerRange: 5,
			rd: &rfpb.RangeDescriptor{
				RangeId:                2,
				LastAddedReplicaId:     proto.Uint64(6),
				LastReplicaAddedAtUsec: proto.Int64(outsideGracePeriodTS),
				// nhid-1..3 in zone-a (nhid-1 local), nhid-4..5 in zone-b, nhid-6 in zone-c.
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3", "nhid-4", "nhid-5", "nhid-6"),
			},
			replicaStateMap: map[uint64]constants.ReplicaState{
				1: constants.ReplicaStateCurrent,
				2: constants.ReplicaStateCurrent,
				3: constants.ReplicaStateCurrent,
				4: constants.ReplicaStateBehind,
				5: constants.ReplicaStateBehind,
				6: constants.ReplicaStateBehind,
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 10, 100, 900),
				usage("nhid-2", "zone-a", 10, 100, 900),
				usage("nhid-3", "zone-a", 10, 100, 900),
				// nhid-4 (zone-b): higher load than nhid-5.
				usage("nhid-4", "zone-b", 20, 100, 900),
				usage("nhid-5", "zone-b", 10, 100, 900),
				// nhid-6 (zone-c): worst load overall — would win on load
				// alone, but zone-c is more under-represented than zone-b.
				usage("nhid-6", "zone-c", 50, 100, 900),
			},
			// Removable = {r4, r5, r6} (the behind replicas). Among them,
			// zone-b has count 2 and zone-c has count 1. We want zone-b,
			// then the worst-fit there: r4 (replicaCount=20).
			expected: &rfpb.ReplicaDescriptor{
				RangeId:   2,
				ReplicaId: 4,
				Nhid:      proto.String("nhid-4"),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rbs := tc.replicasByStatus
			if rbs == nil {
				rbs = &storemap.ReplicasByStatus{LiveReplicas: tc.rd.GetReplicas()}
			}
			storeMap := newTestStoreMap(tc.usages, rbs)
			rq := &Queue{
				storeMap:            storeMap,
				minReplicasPerRange: tc.minReplicasPerRange,
			}
			rq.baseQueue = newBaseQueue(log.NamedSubLogger("test"), clock, rq)
			actual := rq.findReplicaForRemoval(tc.rd, tc.replicaStateMap, localReplicaID)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}

func TestRebalanceReplica(t *testing.T) {
	localReplicaID := uint64(1)
	tests := []struct {
		desc             string
		usages           []*rfpb.StoreUsage
		replicasByStatus *storemap.ReplicasByStatus
		rd               *rfpb.RangeDescriptor
		// minReplicas overrides minReplicasPerRange / minMetaRangeReplicas
		// (which default to 3 when zero).
		minReplicas int
		expected    *rebalanceOp
	}{
		{
			desc: "move-range-to-new-node",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 700, 100, 900),
				usage("nhid-2", "", 600, 100, 900),
				usage("nhid-3", "", 500, 50, 950),
				usage("nhid-4", "", 200, 50, 950),
				usage("nhid-5", "", 0, 0, 1000),
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
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
				Removed: []*rfpb.ReplicaDescriptor{
					{RangeId: 1, ReplicaId: 5, Nhid: proto.String("nhid-5")},
				},
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 700, 100, 900),
				usage("nhid-2", "", 600, 100, 900),
				usage("nhid-3", "", 500, 50, 950),
				usage("nhid-4", "", 0, 100, 900),
				usage("nhid-5", "", 0, 0, 1000),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-4"},
			},
		},
		{
			desc: "move-range-to-node-far-below-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 400, 100, 900),
				// Replica count is slightly above the mean: 400, but below
				// overfull threshold: 420.
				usage("nhid-2", "", 410, 100, 900),
				// Replica count is slightly above the mean: 400, but below
				// overfull threshold: 420.
				usage("nhid-3", "", 405, 50, 950),
				usage("nhid-4", "", 595, 50, 950),
				// Replica count is far below the mean.
				usage("nhid-5", "", 190, 50, 950),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			desc: "move-range-from-full-disk",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 400, 100, 900),
				usage("nhid-2", "", 400, 800, 200),
				// disk usage percent: 95.5%
				usage("nhid-3", "", 400, 955, 45),
				// disk usage percent: 93% > maxDiskCapacityForRebalance. We should
				// not choose this node to rebalance to.
				usage("nhid-4", "", 350, 930, 70),
				usage("nhid-5", "", 650, 900, 1100),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-3"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			// Two target candidates: nhid-4 (200 replicas, below mean)
			// and nhid-5 (600 replicas, above mean). The best target is
			// nhid-4 because it has far fewer replicas. This test
			// verifies that candidate scoring fields (replicaCount,
			// replicaCountMeanLevel) are populated before picking the
			// best candidate, not after. Without that, MaxFunc would
			// fall through to the nhid string tiebreaker and
			// incorrectly pick nhid-5.
			desc: "pick-target-with-fewer-replicas",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 500, 100, 900),
				usage("nhid-2", "", 600, 100, 900),
				usage("nhid-3", "", 500, 100, 900),
				usage("nhid-4", "", 200, 100, 900),
				usage("nhid-5", "", 600, 100, 900),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-4"},
			},
		},
		{
			// 2 zones: all 3 replicas in zone-a, zone-b is empty.
			// targetMax=ceil(3/2)=2, targetMin=1. max=3>2 triggers zone
			// move. Should move from the most loaded source in zone-a to
			// the less loaded node in zone-b.
			desc: "2-zones-rebalance-across-zones",
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 500, 100, 900),
				usage("nhid-2", "zone-a", 600, 100, 900),
				usage("nhid-3", "zone-a", 400, 100, 900),
				usage("nhid-4", "zone-b", 200, 100, 900),
				usage("nhid-5", "zone-b", 300, 100, 900),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-4"},
			},
		},
		{
			// 3 zones: all 3 replicas in zone-a, zone-b and zone-c are
			// empty. targetMax=1, targetMin=1. max=3>1 triggers zone
			// move. Two empty zones with different node loads; should
			// pick the less loaded node (nhid-5 in zone-c).
			desc: "3-zones-two-empty-pick-less-loaded",
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 500, 100, 900),
				usage("nhid-2", "zone-a", 600, 100, 900),
				usage("nhid-3", "zone-a", 400, 100, 900),
				usage("nhid-4", "zone-b", 300, 100, 900),
				usage("nhid-5", "zone-c", 100, 100, 900),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			// 3 zones with 2-1-0 distribution. targetMax=1, targetMin=1.
			// max=2>1 (over) and min=0<1 (under). Should move from zone-a
			// (the only over-represented zone) to zone-c (the only
			// under-represented zone), even though zone-b is also a valid
			// target by load (would create 1-2-0 and oscillate).
			desc: "3-zones-prefer-empty-zone-over-load",
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 500, 100, 900),
				usage("nhid-2", "zone-a", 600, 100, 900),
				usage("nhid-3", "zone-b", 400, 100, 900),
				// zone-b store with low load — would be preferred as target
				// by raw load, but is in a zone that's already at targetMin.
				usage("nhid-4", "zone-b", 50, 100, 900),
				// zone-c is empty — the correct target.
				usage("nhid-5", "zone-c", 200, 200, 900),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			// 4 replicas across 3 zones in 2-2-0. targetMax=ceil(4/3)=2,
			// targetMin=4/3=1. min=0<1 triggers a move, but no zone is
			// strictly above targetMax. We should still pick a source from
			// one of the at-max zones to fill the empty zone (final state
			// 2-1-1).
			desc:        "3-zones-2-2-0-fill-empty-zone",
			minReplicas: 4,
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3", "nhid-4"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 500, 100, 900),
				usage("nhid-2", "zone-a", 600, 100, 900),
				usage("nhid-3", "zone-b", 500, 100, 900),
				usage("nhid-4", "zone-b", 550, 100, 900),
				// Empty zone-c — the correct target.
				usage("nhid-5", "zone-c", 100, 100, 900),
			},
			// Source must come from one of the at-max zones. Both zone-a
			// (nhid-2, count 600) and zone-b (nhid-4, count 550) are
			// candidates; nhid-2 has the highest load. Target is the empty
			// zone-c (nhid-5).
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			// 4 zones: 2 replicas in zone-a, 1 in zone-b, zone-c and
			// zone-d are empty. targetMax=ceil(3/4)=1, targetMin=0.
			// max=2>1 triggers zone move. Should move from zone-a to
			// the less loaded empty zone.
			desc: "4-zones-rebalance-across-zones",
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 500, 100, 900),
				usage("nhid-2", "zone-a", 600, 100, 900),
				usage("nhid-3", "zone-b", 400, 100, 900),
				usage("nhid-4", "zone-c", 200, 100, 900),
				usage("nhid-5", "zone-d", 100, 100, 900),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
			},
		},
		{
			// 3 zones with balanced distribution (1-1-1). No zone
			// rebalance is triggered. Normal rebalancing still applies:
			// source is picked from {nhid-2, nhid-3} (both above mean,
			// tied) and target from {nhid-4, nhid-5} (both below mean,
			// tied); the rendezvous tie-breaker resolves to nhid-3 and
			// nhid-4 respectively.
			desc: "3-zones-balanced-normal-rebalance",
			rd: &rfpb.RangeDescriptor{
				RangeId:  2,
				Replicas: replicas(2, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "zone-a", 600, 100, 900),
				usage("nhid-2", "zone-b", 600, 100, 900),
				usage("nhid-3", "zone-c", 600, 100, 900),
				usage("nhid-4", "zone-a", 100, 100, 900),
				usage("nhid-5", "zone-b", 100, 100, 900),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-3"},
				to:   &candidate{nhid: "nhid-4"},
			},
		},
		{
			desc: "no-rebalance-when-around-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-1", "", 400, 100, 900),
				usage("nhid-2", "", 395, 800, 200),
				usage("nhid-3", "", 410, 900, 100),
				usage("nhid-4", "", 405, 900, 100),
				usage("nhid-5", "", 390, 900, 1100),
			},
			expected: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rbs := tc.replicasByStatus
			if rbs == nil {
				rbs = &storemap.ReplicasByStatus{LiveReplicas: tc.rd.GetReplicas()}
			}
			storeMap := newTestStoreMap(tc.usages, rbs)
			minReplicas := tc.minReplicas
			if minReplicas == 0 {
				minReplicas = 3
			}
			rq := &Queue{
				storeMap:             storeMap,
				minReplicasPerRange:  minReplicas,
				minMetaRangeReplicas: minReplicas,
			}
			rq.baseQueue = &baseQueue{
				log:  log.NamedSubLogger("test"),
				impl: rq,
			}
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			actual := rq.findRebalanceReplicaOp(tc.rd, storesWithStats, localReplicaID)
			if tc.expected != nil {
				require.NotNilf(t, actual, "wanted %+v", tc.expected)
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
	client := &testClient{
		repls: map[string]bool{
			"c1n1": true,
			"c1n2": true,
			"c1n3": false,
		},
	}
	ctx := context.Background()
	tests := []struct {
		desc             string
		usages           []*rfpb.StoreUsage
		rd               *rfpb.RangeDescriptor
		replicasByStatus *storemap.ReplicasByStatus
		expected         *rebalanceOp
	}{
		{
			desc: "move-lease-to-node-far-below-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				leaseUsage("nhid-1", 70, 91),
				leaseUsage("nhid-2", 10, 91),
				leaseUsage("nhid-3", 20, 90),
				leaseUsage("nhid-4", 20, 90),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-1"},
				to:   &candidate{nhid: "nhid-2"},
			},
		},
		{
			desc: "not-move-to-machine-unable-to-connect",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				leaseUsage("nhid-1", 70, 91),
				leaseUsage("nhid-2", 20, 91),
				leaseUsage("nhid-3", 10, 90),
				leaseUsage("nhid-4", 20, 90),
			},
			expected: &rebalanceOp{
				from: &candidate{nhid: "nhid-1"},
				to:   &candidate{nhid: "nhid-2"},
			},
		},
		{
			desc: "no-reblance-when-around-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				leaseUsage("nhid-1", 30, 91),
				leaseUsage("nhid-2", 31, 91),
				leaseUsage("nhid-3", 29, 90),
			},
			expected: nil,
		},
		{
			desc: "no-rebalance-with-good-choice",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				leaseUsage("nhid-1", 70, 161),
				leaseUsage("nhid-2", 69, 160),
				leaseUsage("nhid-3", 69, 160),
				leaseUsage("nhid-4", 5, 160),
			},
			expected: nil,
		},
		{
			desc: "no-rebalance-when-all-nodes-above-mean",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				leaseUsage("nhid-1", 75, 161),
				leaseUsage("nhid-2", 64, 160),
				leaseUsage("nhid-3", 69, 160),
				leaseUsage("nhid-4", 5, 160),
			},
			expected: nil,
		},
		{
			desc: "no-move-too-many-missing-leases",
			rd: &rfpb.RangeDescriptor{
				RangeId:  1,
				Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
			},
			usages: []*rfpb.StoreUsage{
				leaseUsage("nhid-1", 60, 91),
				leaseUsage("nhid-2", 5, 91),
				leaseUsage("nhid-3", 10, 90),
				leaseUsage("nhid-4", 20, 90),
			},
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rbs := tc.replicasByStatus
			if rbs == nil {
				rbs = &storemap.ReplicasByStatus{LiveReplicas: tc.rd.GetReplicas()}
			}
			storeMap := newTestStoreMap(tc.usages, rbs)
			rq := &Queue{
				storeMap:  storeMap,
				apiClient: client,
			}
			rq.baseQueue = &baseQueue{
				log:  log.NamedSubLogger("test"),
				impl: rq,
			}
			actual := rq.findRebalanceLeaseOp(ctx, tc.rd, localReplicaID)
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

type testReplica struct {
	rangeID   uint64
	replicaID uint64
}

func (tr *testReplica) RangeID() uint64   { return tr.rangeID }
func (tr *testReplica) ReplicaID() uint64 { return tr.replicaID }
func (tr *testReplica) RangeDescriptor() *rfpb.RangeDescriptor {
	return &rfpb.RangeDescriptor{RangeId: tr.rangeID}
}
func (tr *testReplica) Usage() (*rfpb.ReplicaUsage, error) { return nil, nil }

type instruction struct {
	action      DriverAction
	priority    float64
	requeueType RequeueType
}

type testQueue struct {
	*baseQueue
	instructions map[string]instruction
	repls        map[uint64]*testReplica
}

func (tq *testQueue) processTask(ctx context.Context, task *driverTask, action DriverAction) RequeueType {
	if task.key.taskType != RangeTaskType {
		return RequeueNoop
	}
	rangeID := task.key.rangeID
	replicaID := task.rangeTask.repl.ReplicaID()
	key := fmt.Sprintf("%d-%d", rangeID, replicaID)
	i, ok := tq.instructions[key]
	if !ok {
		return RequeueNoop
	}
	return i.requeueType
}

func (tq *testQueue) computeAction(ctx context.Context, task *driverTask) (DriverAction, float64) {
	if task.key.taskType != RangeTaskType {
		return DriverNoop, 0.0
	}
	rangeID := task.key.rangeID
	replicaID := task.rangeTask.repl.ReplicaID()
	key := fmt.Sprintf("%d-%d", rangeID, replicaID)
	i, ok := tq.instructions[key]
	if !ok {
		return DriverNoop, 0.0
	}
	return i.action, i.priority
}

func (tq *testQueue) getReplica(rangeID uint64) (IReplica, error) {
	return tq.repls[rangeID], nil
}

func TestBaseQueueRetry(t *testing.T) {
	tr := &testReplica{rangeID: 1, replicaID: 1}
	ctx := context.Background()
	clock := clockwork.NewFakeClock()
	instructions := map[string]instruction{
		"1-1": {
			action:      DriverSplitRange,
			priority:    10.0,
			requeueType: RequeueRetry,
		},
	}
	tq := &testQueue{
		instructions: instructions, // nolint
		repls:        map[uint64]*testReplica{1: tr},
	}
	tq.baseQueue = newBaseQueue(log.NamedSubLogger("test"), clock, tq)
	tq.maybeAddRangeTask(ctx, &rangeTask{repl: tr}, attemptRecord{})

	// ProcessQueue should add c1n1 back to the queue with an attempt record
	tq.processQueue()

	// we should not retry in a second.
	clock.Advance(1 * time.Second)
	task := tq.pop()
	require.NotNil(t, task)
	requeueType := tq.process(ctx, task)
	require.Equal(t, RequeueWait, requeueType)
	tq.postProcess(ctx, task, requeueType)

	// after two seconds, we can retry the task
	clock.Advance(1 * time.Second)
	task = tq.pop()
	require.NotNil(t, task)
	requeueType = tq.process(ctx, task)
	require.Equal(t, RequeueRetry, requeueType)
	tq.postProcess(ctx, task, requeueType)

	for i := 2; i < 10; i++ {
		require.Equal(t, 1, tq.Len())
		clock.Advance(time.Duration(1*math.Pow(2, float64(i))) * time.Second)
		tq.processQueue()
	}
	require.Equal(t, 0, tq.Len())
}

func TestBaseQueueAttemptRecordRetain(t *testing.T) {
	tr := &testReplica{rangeID: 1, replicaID: 1}
	ctx := context.Background()
	clock := clockwork.NewFakeClock()
	instructions := map[string]instruction{
		"1-1": {
			action:      DriverSplitRange,
			priority:    10.0,
			requeueType: RequeueRetry,
		},
	}
	tq := &testQueue{
		instructions: instructions, // nolint
		repls:        map[uint64]*testReplica{1: tr},
	}
	tq.baseQueue = newBaseQueue(log.NamedSubLogger("test"), clock, tq)

	tq.maybeAddRangeTask(ctx, &rangeTask{repl: tr}, attemptRecord{})

	// ProcessQueue should add c1n1 back to the queue with an attempt record
	tq.processQueue()

	task, ok := tq.taskMap[taskKey{taskType: RangeTaskType, rangeID: 1}]
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, 1, task.attemptRecord.attempts)
	require.Equal(t, DriverSplitRange, task.attemptRecord.action)

	tq.maybeAddRangeTask(ctx, &rangeTask{repl: tr}, attemptRecord{})
	task, ok = tq.taskMap[taskKey{taskType: RangeTaskType, rangeID: 1}]
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, 1, task.attemptRecord.attempts)
	require.Equal(t, DriverSplitRange, task.attemptRecord.action)
}

func TestBaseQueueAttemptRecordReset(t *testing.T) {
	tr := &testReplica{rangeID: 1, replicaID: 1}
	ctx := context.Background()
	clock := clockwork.NewFakeClock()
	instructions := map[string]instruction{
		"1-1": {
			action:      DriverSplitRange,
			priority:    10.0,
			requeueType: RequeueRetry,
		},
	}
	tq := &testQueue{
		instructions: instructions, // nolint
		repls:        map[uint64]*testReplica{1: tr},
	}
	tq.baseQueue = newBaseQueue(log.NamedSubLogger("test"), clock, tq)

	tq.maybeAddRangeTask(ctx, &rangeTask{repl: tr}, attemptRecord{})

	// ProcessQueue should add c1n1 back to the queue with an attempt record
	tq.processQueue()

	task, ok := tq.taskMap[taskKey{taskType: RangeTaskType, rangeID: 1}]
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, 1, task.attemptRecord.attempts)
	require.Equal(t, DriverSplitRange, task.attemptRecord.action)

	tq.instructions["1-1"] = instruction{
		action:      DriverAddReplica,
		priority:    15.0,
		requeueType: RequeueCheckOtherActions,
	}
	// We try to queue replica c1n1 again, but this time, there is a different
	// action to do.
	tq.maybeAddRangeTask(ctx, &rangeTask{repl: tr}, attemptRecord{})

	task, ok = tq.taskMap[taskKey{taskType: RangeTaskType, rangeID: 1}]
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, 0, task.attemptRecord.attempts)
	require.Equal(t, DriverAddReplica, task.attemptRecord.action)
}
