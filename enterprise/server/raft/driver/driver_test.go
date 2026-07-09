package driver

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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

func (tsm *testStoreMap) GetStoresWithStats() *storemap.StoresWithStats {
	return storemap.CreateStoresWithStats(slices.Collect(maps.Values(tsm.usages)))
}

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
			rbs := &storemap.ReplicasByStatus{LiveReplicas: tc.rd.GetReplicas()}
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
	ctx := context.Background()
	localReplicaID := uint64(1)
	tests := []struct {
		desc   string
		usages []*rfpb.StoreUsage
		rd     *rfpb.RangeDescriptor
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
			// 3 zones in 3-1-2 distribution (6 replicas). The absolute-min
			// zone (zone-b, count 1) has only one store and it already
			// holds the replica, so zone-b can't accept a target. The best
			// available target is in zone-c (count 2). Moving zone-a (3)
			// → zone-c (2) would produce 2-1-3 — spread unchanged, and the
			// next pass picks zone-c → zone-a and oscillates. The post-
			// selection guard (sourceZoneCount - targetZoneCount >= 2)
			// catches it: 3-2=1 < 2, so it falls through to the load-only
			// fallback. With all loads at the mean, that bails too → nil.
			desc:        "no-move-when-min-zone-has-no-available-target",
			minReplicas: 6,
			rd: &rfpb.RangeDescriptor{
				RangeId: 2,
				// a1 (local), a2, a3 in zone-a; b1 in zone-b; c1, c2 in zone-c.
				Replicas: replicas(2, "nhid-a1", "nhid-a2", "nhid-a3", "nhid-b1", "nhid-c1", "nhid-c2"),
			},
			usages: []*rfpb.StoreUsage{
				usage("nhid-a1", "zone-a", 500, 100, 900),
				usage("nhid-a2", "zone-a", 500, 100, 900),
				usage("nhid-a3", "zone-a", 500, 100, 900),
				// nhid-a4 is an available target in the over-full zone-a.
				usage("nhid-a4", "zone-a", 500, 100, 900),
				// zone-b has only one store, and it already holds the replica.
				usage("nhid-b1", "zone-b", 500, 100, 900),
				usage("nhid-c1", "zone-c", 500, 100, 900),
				usage("nhid-c2", "zone-c", 500, 100, 900),
				// nhid-c3 is an available target in zone-c (count 2).
				usage("nhid-c3", "zone-c", 500, 100, 900),
			},
			expected: nil,
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
			// rebalance is triggered. A normal count move must preserve
			// zone diversity: the only diversity-preserving move is
			// nhid-2 (zone-b) -> nhid-5 (zone-b), a same-zone move.
			// nhid-3 (zone-c) has no zone-c target, so every move off it
			// would drop zone-c to 0 and is skipped by zonePairOK.
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
				from: &candidate{nhid: "nhid-2"},
				to:   &candidate{nhid: "nhid-5"},
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
			rbs := &storemap.ReplicasByStatus{LiveReplicas: tc.rd.GetReplicas()}
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
			localRepl := &testReplica{rangeID: tc.rd.GetRangeId(), replicaID: localReplicaID}
			actual := rq.findRebalanceReplicaOp(ctx, tc.rd, storesWithStats, localRepl, false)
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
		desc     string
		usages   []*rfpb.StoreUsage
		rd       *rfpb.RangeDescriptor
		expected *rebalanceOp
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
			rbs := &storemap.ReplicasByStatus{LiveReplicas: tc.rd.GetReplicas()}
			storeMap := newTestStoreMap(tc.usages, rbs)
			rq := &Queue{
				storeMap:  storeMap,
				apiClient: client,
			}
			rq.baseQueue = &baseQueue{
				log:  log.NamedSubLogger("test"),
				impl: rq,
			}
			localRepl := &testReplica{rangeID: tc.rd.GetRangeId(), replicaID: localReplicaID}
			actual := rq.findRebalanceLeaseOp(ctx, tc.rd, localRepl).op
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
	usage     *rfpb.ReplicaUsage
	// usageErr, when set, makes Usage() fail (Usage() opens a Pebble handle in
	// production, so a transient error is realistic).
	usageErr error
}

func (tr *testReplica) RangeID() uint64   { return tr.rangeID }
func (tr *testReplica) ReplicaID() uint64 { return tr.replicaID }
func (tr *testReplica) RangeDescriptor() *rfpb.RangeDescriptor {
	return &rfpb.RangeDescriptor{RangeId: tr.rangeID}
}
func (tr *testReplica) Usage() (*rfpb.ReplicaUsage, error) {
	if tr.usageErr != nil {
		return nil, tr.usageErr
	}
	return tr.usage, nil
}

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

func (tq *testQueue) processTask(ctx context.Context, task *driverTask, action DriverAction, leaseBlocked bool) RequeueType {
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

func (tq *testQueue) computeAction(ctx context.Context, task *driverTask) (DriverAction, float64, bool) {
	if task.key.taskType != RangeTaskType {
		return DriverNoop, 0.0, false
	}
	rangeID := task.key.rangeID
	replicaID := task.rangeTask.repl.ReplicaID()
	key := fmt.Sprintf("%d-%d", rangeID, replicaID)
	i, ok := tq.instructions[key]
	if !ok {
		return DriverNoop, 0.0, false
	}
	return i.action, i.priority, false
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

// qpsUsage builds a StoreUsage for load-based rebalance tests. Replica and
// lease counts are set so the lease-rebalance precondition passes and counts
// are balanced (so only QPS drives decisions).
func qpsUsage(nhid, zone string, readQPS, proposeQPS int64) *rfpb.StoreUsage {
	return &rfpb.StoreUsage{
		Node:           &rfpb.NodeDescriptor{Nhid: nhid, Zone: zone},
		ReplicaCount:   30,
		LeaseCount:     10,
		ReadQps:        readQPS,
		RaftProposeQps: proposeQPS,
		TotalBytesUsed: 100,
		TotalBytesFree: 900,
	}
}

func newQPSTestQueue(t *testing.T, usages []*rfpb.StoreUsage, rd *rfpb.RangeDescriptor) *Queue {
	flags.Set(t, "cache.raft.enable_load_based_rebalance", true)
	rbs := &storemap.ReplicasByStatus{LiveReplicas: rd.GetReplicas()}
	client := &testClient{repls: map[string]bool{}}
	for _, repl := range rd.GetReplicas() {
		client.repls[replicaKey(repl)] = true
	}
	rq := &Queue{
		storeMap:             newTestStoreMap(usages, rbs),
		apiClient:            client,
		minReplicasPerRange:  3,
		minMetaRangeReplicas: 3,
	}
	rq.baseQueue = &baseQueue{
		log:   log.NamedSubLogger("test"),
		impl:  rq,
		clock: clockwork.NewFakeClock(),
	}
	return rq
}

func TestRebalanceLeasesQPS(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	tests := []struct {
		desc         string
		usages       []*rfpb.StoreUsage
		rangeReadQPS int64
		expected     *rebalanceOp
		blocked      bool
	}{
		{
			// nhid-1 is read-overfull (5000 > mean 2000 + band 200); both
			// candidates pass the improvement check; the coldest (nhid-3)
			// wins.
			desc: "shed-to-coldest-replica-store",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 5000, 0),
				qpsUsage("nhid-2", "", 1000, 0),
				qpsUsage("nhid-3", "", 500, 0),
				qpsUsage("nhid-4", "", 1500, 0),
			},
			rangeReadQPS: 800,
			expected:     &rebalanceOp{from: &candidate{nhid: "nhid-1"}, to: &candidate{nhid: "nhid-3"}},
		},
		{
			// All replica stores are hot and close together: moving an
			// 900-qps lease over a 120-qps gap would invert the order and
			// ping-pong, so the result is blocked (no viable candidate).
			desc: "improvement-check-blocks-and-signals",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 2540, 0),
				qpsUsage("nhid-2", "", 2420, 0),
				qpsUsage("nhid-3", "", 2420, 0),
				qpsUsage("nhid-4", "", 620, 0),
			},
			rangeReadQPS: 900,
			expected:     nil,
			blocked:      true,
		},
		{
			// nhid-2 is the coldest on reads but overfull on propose QPS;
			// the cross-dimension veto rejects it and nhid-3 wins.
			desc: "write-hot-target-vetoed",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 5000, 0),
				qpsUsage("nhid-2", "", 500, 3000),
				qpsUsage("nhid-3", "", 1000, 100),
				qpsUsage("nhid-4", "", 1500, 0),
			},
			rangeReadQPS: 800,
			expected:     &rebalanceOp{from: &candidate{nhid: "nhid-1"}, to: &candidate{nhid: "nhid-3"}},
		},
		{
			// nhid-3 is the coldest read target but carries a little propose
			// QPS. The propose mean (200/4=50) is below the 100 floor, so
			// there's no write signal and the cross-dimension veto must NOT
			// fire: nhid-3 wins even though its propose QPS (200) exceeds
			// mean+floor (150). Without the qpsSignalPresent guard the veto
			// would reject nhid-3 and the lease would go to nhid-2.
			desc: "write-signal-absent-does-not-veto",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 5000, 0),
				qpsUsage("nhid-2", "", 1000, 0),
				qpsUsage("nhid-3", "", 500, 200),
				qpsUsage("nhid-4", "", 1500, 0),
			},
			rangeReadQPS: 800,
			expected:     &rebalanceOp{from: &candidate{nhid: "nhid-1"}, to: &candidate{nhid: "nhid-3"}},
		},
		{
			// This range's lease carries less than min_lease_load_fraction
			// (0.5%) of the store's read QPS; not worth churning.
			desc: "worth-it-floor-skips-tiny-range",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 5000, 0),
				qpsUsage("nhid-2", "", 1000, 0),
				qpsUsage("nhid-3", "", 500, 0),
				qpsUsage("nhid-4", "", 1500, 0),
			},
			rangeReadQPS: 10,
			expected:     nil,
		},
		{
			// Mean read QPS (25) is below the 100-qps floor: no load signal,
			// so the count path runs and moves the lease to the store with
			// the fewest leases.
			desc: "no-signal-falls-back-to-count",
			usages: []*rfpb.StoreUsage{
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-1"}, ReplicaCount: 30, LeaseCount: 60, ReadQps: 50},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-2"}, ReplicaCount: 30, LeaseCount: 5, ReadQps: 20},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-3"}, ReplicaCount: 30, LeaseCount: 10, ReadQps: 10},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-4"}, ReplicaCount: 30, LeaseCount: 20, ReadQps: 20},
			},
			rangeReadQPS: 10,
			expected:     &rebalanceOp{from: &candidate{nhid: "nhid-1"}, to: &candidate{nhid: "nhid-2"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rq := newQPSTestQueue(t, tc.usages, rd)
			localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: tc.rangeReadQPS}}
			res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
			require.Equal(t, tc.blocked, res.blocked)
			if tc.expected != nil {
				require.NotNil(t, res.op)
				require.Equal(t, tc.expected.from.nhid, res.op.from.nhid)
				require.Equal(t, tc.expected.to.nhid, res.op.to.nhid)
			} else {
				require.Nil(t, res.op)
			}
		})
	}
}

func TestRebalanceReplicaQPS(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withBytes := func(su *rfpb.StoreUsage, used int64) *rfpb.StoreUsage {
		su.TotalBytesUsed = used
		return su
	}
	tests := []struct {
		desc         string
		usages       []*rfpb.StoreUsage
		rangeUsage   *rfpb.ReplicaUsage
		leaseBlocked bool
		expected     *rebalanceOp
	}{
		{
			// nhid-2 (follower) is propose-overfull; its apply load moves to
			// the coldest target nhid-4.
			desc: "move-apply-load-to-coldest",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 0, 3000),
				qpsUsage("nhid-2", "", 0, 3000),
				qpsUsage("nhid-3", "", 0, 1000),
				qpsUsage("nhid-4", "", 0, 200),
				qpsUsage("nhid-5", "", 0, 800),
			},
			rangeUsage: &rfpb.ReplicaUsage{RaftProposeQps: 600},
			expected:   &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-4"}},
		},
		{
			// The gap between any source and target is smaller than the
			// range's apply load; nothing moves (and counts are balanced so
			// the count fallback stays quiet too).
			desc: "improvement-check-blocks-move",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 0, 1000),
				qpsUsage("nhid-2", "", 0, 1000),
				qpsUsage("nhid-3", "", 0, 900),
				qpsUsage("nhid-4", "", 0, 800),
				qpsUsage("nhid-5", "", 0, 850),
			},
			rangeUsage: &rfpb.ReplicaUsage{RaftProposeQps: 600},
			expected:   nil,
		},
		{
			// nhid-4 is the coldest on propose QPS but read-hot; the
			// cross-dimension veto rejects it and nhid-5 wins.
			desc: "read-hot-target-vetoed",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 1000, 3000),
				qpsUsage("nhid-2", "", 1000, 3000),
				qpsUsage("nhid-3", "", 1000, 1000),
				qpsUsage("nhid-4", "", 5000, 200),
				qpsUsage("nhid-5", "", 1000, 800),
			},
			rangeUsage: &rfpb.ReplicaUsage{RaftProposeQps: 600},
			expected:   &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-5"}},
		},
		{
			// nhid-4 is the coldest propose target and carries a little read
			// QPS. The read mean (250/5=50) is below the 100 floor, so there's
			// no read signal and the cross-dimension veto must NOT fire: the
			// apply load moves to nhid-4 even though its read QPS (250) exceeds
			// mean+floor (150). Without the qpsSignalPresent guard the veto
			// would reject nhid-4 and the move would go to nhid-5.
			desc: "read-signal-absent-does-not-veto",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 0, 3000),
				qpsUsage("nhid-2", "", 0, 3000),
				qpsUsage("nhid-3", "", 0, 1000),
				qpsUsage("nhid-4", "", 250, 200),
				qpsUsage("nhid-5", "", 0, 800),
			},
			rangeUsage: &rfpb.ReplicaUsage{RaftProposeQps: 600},
			expected:   &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-4"}},
		},
		{
			// nhid-4 is the coldest but already byte-heavy; eviction is
			// per-store, so don't stack more data on it.
			desc: "byte-heavy-target-vetoed",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 0, 3000),
				qpsUsage("nhid-2", "", 0, 3000),
				qpsUsage("nhid-3", "", 0, 1000),
				withBytes(qpsUsage("nhid-4", "", 0, 200), 5000),
				qpsUsage("nhid-5", "", 0, 800),
			},
			rangeUsage: &rfpb.ReplicaUsage{RaftProposeQps: 600},
			expected:   &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-5"}},
		},
		{
			// One replica per zone. nhid-4 (zone-1) is the coldest, but
			// moving nhid-2's replica (zone-2) there would put two replicas
			// in zone-1; the same-zone swap to nhid-5 is chosen instead.
			desc: "zone-spread-preserved-pairwise",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "zone-1", 0, 3000),
				qpsUsage("nhid-2", "zone-2", 0, 3000),
				qpsUsage("nhid-3", "zone-3", 0, 1000),
				qpsUsage("nhid-4", "zone-1", 0, 0),
				qpsUsage("nhid-5", "zone-2", 0, 200),
			},
			rangeUsage: &rfpb.ReplicaUsage{RaftProposeQps: 600},
			expected:   &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-5"}},
		},
		{
			// No QPS signal on either dimension, but replica counts are
			// imbalanced: the count-based fallback still runs with the flag
			// on, so cold clusters converge to even counts.
			desc: "count-fallback-when-no-signal",
			usages: []*rfpb.StoreUsage{
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-1"}, ReplicaCount: 30, TotalBytesUsed: 100, TotalBytesFree: 900},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-2"}, ReplicaCount: 50, TotalBytesUsed: 100, TotalBytesFree: 900},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-3"}, ReplicaCount: 20, TotalBytesUsed: 100, TotalBytesFree: 900},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-4"}, ReplicaCount: 3, TotalBytesUsed: 100, TotalBytesFree: 900},
				&rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-5"}, ReplicaCount: 20, TotalBytesUsed: 100, TotalBytesFree: 900},
			},
			rangeUsage: &rfpb.ReplicaUsage{},
			expected:   &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-4"}},
		},
		{
			// Lease-unblock mode: the local store (nhid-1) is read-hot but
			// no replica store can take the lease. Move a follower to the
			// store with the most read headroom (nhid-4), judged on the
			// range's read QPS, so a later pass can transfer the lease.
			desc: "lease-blocked-creates-landing-spot",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 5000, 1000),
				qpsUsage("nhid-2", "", 2500, 2000),
				qpsUsage("nhid-3", "", 2400, 1000),
				qpsUsage("nhid-4", "", 300, 100),
				qpsUsage("nhid-5", "", 2000, 400),
			},
			rangeUsage:   &rfpb.ReplicaUsage{ReadQps: 800, RaftProposeQps: 300},
			leaseBlocked: true,
			expected:     &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-4"}},
		},
		{
			// Lease-unblock mode, but every non-replica store sits at or above
			// the read mean (3000). A follower move to nhid-4/5 would clear the
			// read-gap check yet the follow-up lease transfer would refuse the
			// lease (its srcOverfull gate needs the target below the read
			// mean), so no viable landing spot exists. The read-target
			// viability check must reject both and return no move; without it a
			// doomed, cooldown-skipping move would be issued.
			desc: "lease-unblock-skips-target-not-below-read-mean",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 9000, 0),
				qpsUsage("nhid-2", "", 0, 0),
				qpsUsage("nhid-3", "", 0, 0),
				qpsUsage("nhid-4", "", 3000, 0),
				qpsUsage("nhid-5", "", 3000, 0),
			},
			rangeUsage:   &rfpb.ReplicaUsage{ReadQps: 800, RaftProposeQps: 0},
			leaseBlocked: true,
			expected:     nil,
		},
		{
			// Lease-unblock mode: nhid-4 is a viable read landing spot (read
			// 500, below the mean 2400) but carries a little propose QPS. The
			// propose mean (200/5=40) is below the 100 floor, so there's no
			// write signal and the cross-dimension veto must NOT fire: the
			// follower move to nhid-4 proceeds. Without the qpsSignalPresent
			// guard the veto would reject nhid-4 (nhid-5 fails the read-gap
			// check) and no landing spot would be created.
			desc: "lease-unblock-write-signal-absent-does-not-veto",
			usages: []*rfpb.StoreUsage{
				qpsUsage("nhid-1", "", 6000, 0),
				qpsUsage("nhid-2", "", 0, 0),
				qpsUsage("nhid-3", "", 0, 0),
				qpsUsage("nhid-4", "", 500, 200),
				qpsUsage("nhid-5", "", 5500, 0),
			},
			rangeUsage:   &rfpb.ReplicaUsage{ReadQps: 800, RaftProposeQps: 0},
			leaseBlocked: true,
			expected:     &rebalanceOp{from: &candidate{nhid: "nhid-2"}, to: &candidate{nhid: "nhid-4"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rq := newQPSTestQueue(t, tc.usages, rd)
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: tc.rangeUsage}
			actual := rq.findRebalanceReplicaOp(ctx, rd, storesWithStats, localRepl, tc.leaseBlocked)
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

func TestRebalanceCooldownAndLedger(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	usages := []*rfpb.StoreUsage{
		qpsUsage("nhid-1", "", 5000, 0),
		qpsUsage("nhid-2", "", 1000, 0),
		qpsUsage("nhid-3", "", 500, 0),
		qpsUsage("nhid-4", "", 1500, 0),
	}
	rq := newQPSTestQueue(t, usages, rd)
	clock := rq.clock.(*clockwork.FakeClock)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 800}}

	// The ledger shifts nhid-3's effective read QPS.
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 2, fromNHID: "nhid-1", toNHID: "nhid-3", readQPS: 800})
	readAdj, proposeAdj := rq.pendingAdjustment("nhid-3")
	require.Equal(t, float64(800), readAdj)
	require.Equal(t, float64(0), proposeAdj)
	readAdj, _ = rq.pendingAdjustment("nhid-1")
	require.Equal(t, float64(-800), readAdj)

	// Range 1 is not in cooldown (range 2 is), so a lease op is still found;
	// but the ledger has warmed nhid-3 (500+800=1300), so nhid-2 (1000-0)
	// is now the coldest viable target.
	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op)
	require.Equal(t, "nhid-2", res.op.to.nhid)

	// Range 2 is in cooldown.
	require.True(t, rq.inCooldown(2))

	// The ledger must outlive gossip lag: with the default 2m EWMA time
	// constant the TTL is two time constants.
	require.Equal(t, 4*time.Minute, pendingMoveTTL())

	// After the TTL the ledger entries expire and nhid-3 wins again.
	clock.Advance(pendingMoveTTL() + time.Second)
	readAdj, _ = rq.pendingAdjustment("nhid-3")
	require.Equal(t, float64(0), readAdj)
	res = rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op)
	require.Equal(t, "nhid-3", res.op.to.nhid)

	// A rebalance of range 1 puts it in cooldown: no further ops.
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3", readQPS: 800})
	res = rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.Nil(t, res.op)
	// The cooldown expires with time.
	clock.Advance(6 * time.Minute)
	res = rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op)

	// A lease-unblock replica move records with skipCooldown: the follow-up
	// lease transfer on the next pass must not be blocked.
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 3, fromNHID: "nhid-2", toNHID: "nhid-4", proposeQPS: 300, skipCooldown: true})
	require.False(t, rq.inCooldown(3))
}

func TestLeaseCountFallbackWithQPSSignal(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withLeases := func(su *rfpb.StoreUsage, leases int64) *rfpb.StoreUsage {
		su.LeaseCount = leases
		return su
	}
	// Read QPS is present (mean 250 >= floor 100) but perfectly balanced, so
	// the QPS lease path finds nothing. Lease counts are skewed: the count
	// fallback must still rebalance them.
	usages := []*rfpb.StoreUsage{
		withLeases(qpsUsage("nhid-1", "", 250, 0), 60),
		withLeases(qpsUsage("nhid-2", "", 250, 0), 5),
		withLeases(qpsUsage("nhid-3", "", 250, 0), 10),
		withLeases(qpsUsage("nhid-4", "", 250, 0), 45),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 10}}
	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op)
	require.Equal(t, "nhid-1", res.op.from.nhid)
	require.Equal(t, "nhid-2", res.op.to.nhid)

	// If every count-viable candidate is hot on a QPS dimension, the veto
	// stops the count move from undoing load balance (both nhid-2 and nhid-3
	// are read-hot, so there is no cool candidate to fall through to).
	usages = []*rfpb.StoreUsage{
		withLeases(qpsUsage("nhid-1", "", 250, 0), 60),
		withLeases(qpsUsage("nhid-2", "", 900, 0), 5),
		withLeases(qpsUsage("nhid-3", "", 900, 0), 10),
		withLeases(qpsUsage("nhid-4", "", 100, 0), 45),
	}
	rq = newQPSTestQueue(t, usages, rd)
	res = rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.Nil(t, res.op)
}

func TestInBandLeaseSourceDoesNotChurn(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	// nhid-1 (2150) is above the mean (2000) but inside the band (±200).
	// nhid-3 is underfull on reads but write-hot (vetoed); nhid-2 sits just
	// below nhid-1 with a gap bigger than the range's read QPS. Under "any
	// underfull candidate opens the gate" logic the lease would churn
	// nhid-1→nhid-2 — a move between two in-band stores. It must not.
	usages := []*rfpb.StoreUsage{
		qpsUsage("nhid-1", "", 2150, 0),
		qpsUsage("nhid-2", "", 1900, 0),
		qpsUsage("nhid-3", "", 1500, 3000),
		qpsUsage("nhid-4", "", 2450, 0),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 300}}
	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.Nil(t, res.op)
	// An in-band source never forces replica moves either.
	require.False(t, res.blocked)
}

func TestDiskEvacuationNotBlockedByCooldown(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withDisk := func(su *rfpb.StoreUsage, used, free int64) *rfpb.StoreUsage {
		su.TotalBytesUsed = used
		su.TotalBytesFree = free
		return su
	}
	// nhid-2's disk is over the eviction threshold; the count/disk path must
	// evacuate its replica even though range 1 just had a load-based op and
	// is in cooldown.
	usages := []*rfpb.StoreUsage{
		qpsUsage("nhid-1", "", 1000, 1000),
		withDisk(qpsUsage("nhid-2", "", 1000, 1000), 960, 40),
		qpsUsage("nhid-3", "", 1000, 1000),
		qpsUsage("nhid-4", "", 1000, 1000),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 100, RaftProposeQps: 100}}
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3", readQPS: 100})
	require.True(t, rq.inCooldown(1))
	storesWithStats := storemap.CreateStoresWithStats(usages)
	op := rq.findRebalanceReplicaOp(ctx, rd, storesWithStats, localRepl, false)
	require.NotNil(t, op)
	require.Equal(t, "nhid-2", op.from.nhid)
}

// TestCountFallbackVetoHonoredDuringCooldown confirms that the count-based
// replica-move hot-target veto still sees effective QPS when the range is in
// cooldown. If candidate QPS were populated only in the (non-cooldown)
// load-based block, bestTarget.readQPS would stay zero during cooldown and the
// veto would be bypassed, landing a move on the read-hot store. With the fix
// the read-hot count-best target is skipped and the move falls through to the
// cool alternative — identically in and out of cooldown.
func TestCountFallbackVetoHonoredDuringCooldown(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	// Replica counts are skewed (nhid-2 overfull), so the count fallback wants
	// to shed one of nhid-2's replicas. The count-best target nhid-4 is
	// read-hot (5000 vs read mean 1160, band top 1276), so the veto must skip
	// it and move to the cool nhid-5 instead — regardless of cooldown. Propose
	// QPS is zero everywhere (no write signal), leaving only the read veto in
	// play. If nhid-4's QPS weren't populated during cooldown it would look
	// cold and wrongly win.
	newUsages := func() []*rfpb.StoreUsage {
		return []*rfpb.StoreUsage{
			withReplicas(qpsUsage("nhid-1", "", 200, 0), 30),
			withReplicas(qpsUsage("nhid-2", "", 200, 0), 60),
			withReplicas(qpsUsage("nhid-3", "", 200, 0), 30),
			withReplicas(qpsUsage("nhid-4", "", 5000, 0), 3),
			withReplicas(qpsUsage("nhid-5", "", 200, 0), 4),
		}
	}
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 100}}

	// Control: not in cooldown — QPS fields are populated and the read-hot
	// nhid-4 is skipped in favor of the cool nhid-5.
	usages := newUsages()
	rq := newQPSTestQueue(t, usages, rd)
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op)
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-5", op.to.nhid, "read-hot nhid-4 must be skipped when not in cooldown")

	// In cooldown: effective QPS is still populated, so nhid-4 stays hot and
	// the move again goes to nhid-5 (not to the read-hot nhid-4).
	usages = newUsages()
	rq = newQPSTestQueue(t, usages, rd)
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3"})
	require.True(t, rq.inCooldown(1))
	op = rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op, "a move must still be found during cooldown")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-5", op.to.nhid, "read-hot nhid-4 must stay skipped during cooldown")
}

// TestLeaseCountFallbackVetoUsesEffectiveQPS confirms that the count-based
// lease-move hot-target veto uses effective (ledger-adjusted) QPS, not raw
// gossiped usage. A store whose raw read QPS is cold but which has a large
// pending inbound move in the ledger is effectively read-hot; the veto must
// treat it as hot and skip it, transferring to the next-best cool candidate
// instead. If the veto used raw usage, the effectively-hot store would be
// chosen as the count-best target.
func TestLeaseCountFallbackVetoUsesEffectiveQPS(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withLeases := func(su *rfpb.StoreUsage, leases int64) *rfpb.StoreUsage {
		su.LeaseCount = leases
		return su
	}
	// Reads are balanced (mean 250, band top 350), so the QPS lease path finds
	// nothing and the count fallback runs. Lease counts are skewed: nhid-2 (5)
	// is the count-best candidate and nhid-3 (10) the next-best. nhid-2's raw
	// read QPS (250) is below the band, but a pending move of 500 read QPS to
	// it in the ledger makes its effective read QPS 750 — above the band. The
	// veto must skip nhid-2 and transfer to the cool nhid-3.
	usages := []*rfpb.StoreUsage{
		withLeases(qpsUsage("nhid-1", "", 250, 0), 60),
		withLeases(qpsUsage("nhid-2", "", 250, 0), 5),
		withLeases(qpsUsage("nhid-3", "", 250, 0), 10),
		withLeases(qpsUsage("nhid-4", "", 250, 0), 45),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 10}}

	// A recent move of 500 read QPS to nhid-2 for a different range. This does
	// not put range 1 in cooldown, but it makes nhid-2 effectively read-hot.
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 2, fromNHID: "nhid-4", toNHID: "nhid-2", readQPS: 500})
	adjRead, _ := rq.pendingAdjustment("nhid-2")
	require.Equal(t, float64(500), adjRead)

	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op, "count fallback should transfer to the cool candidate")
	require.Equal(t, "nhid-1", res.op.from.nhid)
	require.Equal(t, "nhid-3", res.op.to.nhid, "effectively-hot nhid-2 must be skipped in favor of cool nhid-3")
}

// TestDiskFullSourceEvacuatesPastReadVeto confirms a disk-full source sheds a
// replica even when the count-best target is read-hot: evacuation outranks the
// QPS hot-target veto (F1).
func TestDiskFullSourceEvacuatesPastReadVeto(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	withDisk := func(su *rfpb.StoreUsage, used, free int64) *rfpb.StoreUsage {
		su.TotalBytesUsed = used
		su.TotalBytesFree = free
		return su
	}
	// nhid-2 (a follower) has a disk over the eviction threshold. The
	// count-best target nhid-4 is read-hot (5000 vs read mean 1160, band top
	// 1276). With a read signal but no propose signal, the count fallback
	// runs; the read veto would normally reject nhid-4, but a disk-full source
	// must still evacuate to it.
	usages := []*rfpb.StoreUsage{
		withReplicas(qpsUsage("nhid-1", "", 200, 0), 30),
		withDisk(withReplicas(qpsUsage("nhid-2", "", 200, 0), 30), 960, 40),
		withReplicas(qpsUsage("nhid-3", "", 200, 0), 30),
		withReplicas(qpsUsage("nhid-4", "", 5000, 0), 3),
		withReplicas(qpsUsage("nhid-5", "", 200, 0), 30),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 100}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op, "disk-full source must evacuate even to a read-hot target")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-4", op.to.nhid)
}

// TestCountFallbackTriesCoolerTargetWhenBestIsHot confirms the count-based
// replica veto skips a QPS-hot best target and moves to the next-best cool
// target instead of abandoning the move (F4).
func TestCountFallbackTriesCoolerTargetWhenBestIsHot(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	// nhid-2 is count-overfull (60). The count-best target by replica count is
	// nhid-4 (3) but it is read-hot; nhid-5 (4) is nearly as empty and read
	// cold. The move must go to nhid-5, not be dropped because the single best
	// target was hot.
	usages := []*rfpb.StoreUsage{
		withReplicas(qpsUsage("nhid-1", "", 200, 0), 30),
		withReplicas(qpsUsage("nhid-2", "", 200, 0), 60),
		withReplicas(qpsUsage("nhid-3", "", 200, 0), 30),
		withReplicas(qpsUsage("nhid-4", "", 5000, 0), 3),
		withReplicas(qpsUsage("nhid-5", "", 200, 0), 4),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 100}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op, "move must fall through to the cooler target, not be dropped")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-5", op.to.nhid)
}

// TestReplicaQPSSourceMustBeAboveMean confirms findRebalanceReplicaOpQPS does
// not move a replica off a below-mean source just because a target is
// underfull — equalizing two below-mean stores burns a snapshot for nothing
// (F3).
func TestReplicaQPSSourceMustBeAboveMean(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	// Propose mean 2000, band ±200. Both followers (nhid-2=1700, nhid-3=1600)
	// are below the mean; nhid-4=1000 is underfull. Replica counts are even so
	// the count fallback stays quiet. No store is overfull, so no replica move
	// should happen.
	usages := []*rfpb.StoreUsage{
		qpsUsage("nhid-1", "", 0, 3800),
		qpsUsage("nhid-2", "", 0, 1700),
		qpsUsage("nhid-3", "", 0, 1600),
		qpsUsage("nhid-4", "", 0, 1000),
		qpsUsage("nhid-5", "", 0, 1900),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{RaftProposeQps: 500}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.Nil(t, op, "a below-mean source must not shed to an underfull target")
}

// TestLeaseCountFallbackTriesCoolerCandidate confirms the count-based lease
// veto skips a QPS-hot best candidate and transfers to the next-best cool
// candidate instead of abandoning the move (F5).
func TestLeaseCountFallbackTriesCoolerCandidate(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withLeases := func(su *rfpb.StoreUsage, leases int64) *rfpb.StoreUsage {
		su.LeaseCount = leases
		return su
	}
	// The leaseholder nhid-1 (read 250) is below the read mean (412.5), so the
	// QPS lease path finds nothing and the count fallback runs. The
	// fewest-lease candidate nhid-2 (5) is read-hot (900 > band top 512.5);
	// nhid-3 (10 leases) is read cold. The lease must move to nhid-3, not be
	// dropped because the single best-count candidate was hot.
	usages := []*rfpb.StoreUsage{
		withLeases(qpsUsage("nhid-1", "", 250, 0), 60),
		withLeases(qpsUsage("nhid-2", "", 900, 0), 5),
		withLeases(qpsUsage("nhid-3", "", 250, 0), 10),
		withLeases(qpsUsage("nhid-4", "", 250, 0), 45),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 10}}
	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op, "lease move must fall through to the cooler candidate")
	require.Equal(t, "nhid-1", res.op.from.nhid)
	require.Equal(t, "nhid-3", res.op.to.nhid)
}

// TestCountReplicaVetoIsPostMove confirms the count replica veto is post-move:
// a target that is below the propose band now but would cross it after taking
// on this range's apply load is skipped in favor of a cooler target, so count
// never re-heats a store (Option B).
func TestCountReplicaVetoIsPostMove(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	// No source is propose-overfull (the load replica path finds nothing — the
	// followers nhid-2/3 sit below the propose mean), so the count fallback
	// runs. nhid-2 is replica-count-overfull. The count-best target nhid-4
	// (fewest replicas) is below the propose band now (200 < 380) but would
	// cross it after this range's 200 apply QPS (400 > 380); nhid-5 stays cold.
	// The move must go to nhid-5, not nhid-4.
	usages := []*rfpb.StoreUsage{
		withReplicas(qpsUsage("nhid-1", "", 0, 1000), 30),
		withReplicas(qpsUsage("nhid-2", "", 0, 100), 60),
		withReplicas(qpsUsage("nhid-3", "", 0, 100), 30),
		withReplicas(qpsUsage("nhid-4", "", 0, 200), 3),
		withReplicas(qpsUsage("nhid-5", "", 0, 0), 4),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{RaftProposeQps: 200}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op, "a QPS-neutral target exists, so the move must not be dropped")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-5", op.to.nhid, "post-move check must skip the would-be-hot nhid-4")
}

// TestCountReplicaDefersHotRangeInCooldown confirms the count replica path
// leaves a hot range alone during its load cooldown (so count can't undo a
// load move), while still relocating a cold range (Option B).
func TestCountReplicaDefersHotRangeInCooldown(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	// nhid-2 is replica-count-overfull; nhid-4 is a cold, count-underfull
	// target — absent the deferral the count fallback would move nhid-2's
	// replica to nhid-4 even in cooldown.
	newUsages := func() []*rfpb.StoreUsage {
		return []*rfpb.StoreUsage{
			withReplicas(qpsUsage("nhid-1", "", 0, 1000), 30),
			withReplicas(qpsUsage("nhid-2", "", 0, 1000), 60),
			withReplicas(qpsUsage("nhid-3", "", 0, 1000), 30),
			withReplicas(qpsUsage("nhid-4", "", 0, 0), 3),
			withReplicas(qpsUsage("nhid-5", "", 0, 0), 4),
		}
	}

	// Hot range in cooldown → deferred, no move.
	usages := newUsages()
	rq := newQPSTestQueue(t, usages, rd)
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3"})
	require.True(t, rq.inCooldown(1))
	hot := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{RaftProposeQps: 300}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), hot, false)
	require.Nil(t, op, "a hot range must not be count-moved during its cooldown")

	// Cold range in cooldown → not deferred, count move still happens.
	usages = newUsages()
	rq = newQPSTestQueue(t, usages, rd)
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3"})
	require.True(t, rq.inCooldown(1))
	cold := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{RaftProposeQps: 10}}
	op = rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), cold, false)
	require.NotNil(t, op, "a cold range may still be count-moved during cooldown")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-4", op.to.nhid)

	// Idle range (zero apply load) in cooldown, with a propose signal from
	// another store and a zero-propose source. `0 >= minFraction*0` is 0>=0,
	// which must NOT defer — an idle range is not "meaningful" and still moves.
	idleUsages := []*rfpb.StoreUsage{
		withReplicas(qpsUsage("nhid-1", "", 0, 1000), 30),
		withReplicas(qpsUsage("nhid-2", "", 0, 0), 60),
		withReplicas(qpsUsage("nhid-3", "", 0, 0), 30),
		withReplicas(qpsUsage("nhid-4", "", 0, 0), 3),
		withReplicas(qpsUsage("nhid-5", "", 0, 0), 4),
	}
	rq = newQPSTestQueue(t, idleUsages, rd)
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3"})
	require.True(t, rq.inCooldown(1))
	idle := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{RaftProposeQps: 0}}
	op = rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(idleUsages), idle, false)
	require.NotNil(t, op, "an idle (zero-QPS) range must not be deferred by the 0>=0 edge")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-4", op.to.nhid)
}

// TestCountLeaseVetoIsPostMove confirms the count lease veto is post-move: a
// candidate below the read band now but over it after taking the lease's reads
// is skipped for a cooler candidate (Option B).
func TestCountLeaseVetoIsPostMove(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withLeases := func(su *rfpb.StoreUsage, leases int64) *rfpb.StoreUsage {
		su.LeaseCount = leases
		return su
	}
	// Reads are near-balanced so the QPS lease path is quiet (leaseholder
	// nhid-1 at 250 is not above the read mean 262.5) and the count fallback
	// runs. Lease counts are skewed: nhid-2 (5) is the count-best candidate but
	// its read 300 + this lease's 100 reads = 400 crosses the read band (362.5);
	// nhid-3 (250+100=350) stays under. The lease must go to nhid-3, not nhid-2.
	usages := []*rfpb.StoreUsage{
		withLeases(qpsUsage("nhid-1", "", 250, 0), 60),
		withLeases(qpsUsage("nhid-2", "", 300, 0), 5),
		withLeases(qpsUsage("nhid-3", "", 250, 0), 10),
		withLeases(qpsUsage("nhid-4", "", 250, 0), 45),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 100}}
	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op, "a QPS-neutral candidate exists, so the move must not be dropped")
	require.Equal(t, "nhid-1", res.op.from.nhid)
	require.Equal(t, "nhid-3", res.op.to.nhid, "post-move check must skip the would-be-hot nhid-2")
}

// TestCountLeaseGatedByCooldown confirms the count lease path fully respects
// the cooldown (a lease move costs a read blip; there's no urgent reason to
// move a lease), unlike the count replica path which only defers hot ranges.
func TestCountLeaseGatedByCooldown(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withLeases := func(su *rfpb.StoreUsage, leases int64) *rfpb.StoreUsage {
		su.LeaseCount = leases
		return su
	}
	// Reads balanced (QPS lease path quiet), lease counts skewed toward nhid-2.
	newUsages := func() []*rfpb.StoreUsage {
		return []*rfpb.StoreUsage{
			withLeases(qpsUsage("nhid-1", "", 250, 0), 60),
			withLeases(qpsUsage("nhid-2", "", 250, 0), 5),
			withLeases(qpsUsage("nhid-3", "", 250, 0), 10),
			withLeases(qpsUsage("nhid-4", "", 250, 0), 45),
		}
	}
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{ReadQps: 10}}

	// Not in cooldown → count lease move happens.
	usages := newUsages()
	rq := newQPSTestQueue(t, usages, rd)
	res := rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.NotNil(t, res.op)
	require.Equal(t, "nhid-1", res.op.from.nhid)
	require.Equal(t, "nhid-2", res.op.to.nhid)

	// In cooldown → the count lease path is gated: no move.
	usages = newUsages()
	rq = newQPSTestQueue(t, usages, rd)
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-3", toNHID: "nhid-4"})
	require.True(t, rq.inCooldown(1))
	res = rq.findRebalanceLeaseOp(ctx, rd, localRepl)
	require.Nil(t, res.op, "a range in cooldown must not take a count lease move (read blip)")
}

// TestCountReplicaPreservesZoneDiversity confirms the count fallback won't move
// a replica into a zone that already holds as many of the range's replicas as
// the source's zone (which would drop a fault domain); it picks the same-zone
// count-underfull target instead.
func TestCountReplicaPreservesZoneDiversity(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	// Replicas: nhid-1 (zone-A, local), nhid-2 (zone-B), nhid-3 (zone-C,
	// count-overfull -> bestSource). Targets: nhid-4 (zone-B, most count-
	// underfull) and nhid-5 (zone-C, count-underfull). Moving nhid-3 (C) to
	// nhid-4 (B) would drop zone C to 0 and give B two -> zonePairOK must skip
	// it; the same-zone nhid-5 (C->C) preserves diversity and is chosen.
	usages := []*rfpb.StoreUsage{
		withReplicas(qpsUsage("nhid-1", "zone-A", 0, 0), 30),
		withReplicas(qpsUsage("nhid-2", "zone-B", 0, 0), 30),
		withReplicas(qpsUsage("nhid-3", "zone-C", 0, 0), 60),
		withReplicas(qpsUsage("nhid-4", "zone-B", 0, 0), 3),
		withReplicas(qpsUsage("nhid-5", "zone-C", 0, 0), 4),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op)
	require.Equal(t, "nhid-3", op.from.nhid)
	require.Equal(t, "nhid-5", op.to.nhid, "must not move into zone-B (would drop zone-C); use same-zone nhid-5")
}

// TestCountReplicaZoneRepairBypassesQPSVeto confirms a cross-zone repair move
// is not blocked by the count QPS veto: even a QPS-hot target is used when the
// move improves fault-domain spread (master moved unconditionally).
func TestCountReplicaZoneRepairBypassesQPSVeto(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	// zone-1 holds two replicas (nhid-1 local, nhid-2), zone-2 one (nhid-3).
	// The only target that improves spread is nhid-4 in zone-3, and it is
	// read-hot. The move must still happen (zone repair is urgent).
	usages := []*rfpb.StoreUsage{
		qpsUsage("nhid-1", "zone-1", 200, 0),
		qpsUsage("nhid-2", "zone-1", 200, 0),
		qpsUsage("nhid-3", "zone-2", 200, 0),
		qpsUsage("nhid-4", "zone-3", 5000, 0),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), localRepl, false)
	require.NotNil(t, op, "a cross-zone repair move must not be blocked by the QPS veto")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-4", op.to.nhid)
}

// TestCountReplicaZoneRepairNotDeferredInCooldown confirms a cross-zone repair
// move happens even for a hot range in cooldown (urgent moves bypass the
// hot-range deferral).
func TestCountReplicaZoneRepairNotDeferredInCooldown(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	// zone-1 over-represented (nhid-1 local, nhid-2); zone-3 target nhid-4
	// improves spread. The range is hot and in cooldown, but zone repair is
	// urgent so it must still move.
	usages := []*rfpb.StoreUsage{
		qpsUsage("nhid-1", "zone-1", 0, 1000),
		qpsUsage("nhid-2", "zone-1", 0, 1000),
		qpsUsage("nhid-3", "zone-2", 0, 1000),
		qpsUsage("nhid-4", "zone-3", 0, 0),
	}
	rq := newQPSTestQueue(t, usages, rd)
	rq.recordRebalance(ctx, &rebalanceRecord{rangeID: 1, fromNHID: "nhid-1", toNHID: "nhid-3"})
	require.True(t, rq.inCooldown(1))
	hot := &testReplica{rangeID: 1, replicaID: 1, usage: &rfpb.ReplicaUsage{RaftProposeQps: 300}}
	op := rq.findRebalanceReplicaOp(ctx, rd, storemap.CreateStoresWithStats(usages), hot, false)
	require.NotNil(t, op, "a cross-zone repair move must not be deferred by cooldown")
	require.Equal(t, "nhid-2", op.from.nhid)
	require.Equal(t, "nhid-4", op.to.nhid)
}

// TestRebalanceReplicaRecordsCooldownOnUsageError confirms an applied replica
// move still starts the range's cooldown when Usage() fails. Without the
// record, recordRebalance never runs and the range can be moved again
// immediately (thrash).
func TestRebalanceReplicaRecordsCooldownOnUsageError(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withReplicas := func(su *rfpb.StoreUsage, count int64) *rfpb.StoreUsage {
		su.ReplicaCount = count
		return su
	}
	// No QPS signal, but replica counts are skewed, so the count fallback finds
	// a move (nhid-2 -> nhid-4) even though Usage() errors.
	usages := []*rfpb.StoreUsage{
		withReplicas(qpsUsage("nhid-1", "", 0, 0), 30),
		withReplicas(qpsUsage("nhid-2", "", 0, 0), 60),
		withReplicas(qpsUsage("nhid-3", "", 0, 0), 30),
		withReplicas(qpsUsage("nhid-4", "", 0, 0), 3),
		withReplicas(qpsUsage("nhid-5", "", 0, 0), 4),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usageErr: fmt.Errorf("pebble unavailable")}
	c := rq.rebalanceReplica(ctx, rd, localRepl, false)
	require.NotNil(t, c, "the move should still be produced")
	require.NotNil(t, c.rebalanceRecord, "an applied move must record its cooldown even when Usage() fails")
	require.Equal(t, uint64(1), c.rebalanceRecord.rangeID)
	require.False(t, c.rebalanceRecord.skipCooldown)
	require.Equal(t, int64(0), c.rebalanceRecord.proposeQPS, "ledger QPS is lost on a Usage error, but the cooldown is not")
}

// TestRebalanceLeaseRecordsCooldownOnUsageError is the lease-lever equivalent.
func TestRebalanceLeaseRecordsCooldownOnUsageError(t *testing.T) {
	ctx := context.Background()
	rd := &rfpb.RangeDescriptor{
		RangeId:  1,
		Replicas: replicas(1, "nhid-1", "nhid-2", "nhid-3"),
	}
	withLeases := func(su *rfpb.StoreUsage, leases int64) *rfpb.StoreUsage {
		su.LeaseCount = leases
		return su
	}
	// No read signal, but lease counts are skewed, so the count lease fallback
	// finds a move even though Usage() errors.
	usages := []*rfpb.StoreUsage{
		withLeases(qpsUsage("nhid-1", "", 0, 0), 60),
		withLeases(qpsUsage("nhid-2", "", 0, 0), 5),
		withLeases(qpsUsage("nhid-3", "", 0, 0), 10),
		withLeases(qpsUsage("nhid-4", "", 0, 0), 45),
	}
	rq := newQPSTestQueue(t, usages, rd)
	localRepl := &testReplica{rangeID: 1, replicaID: 1, usageErr: fmt.Errorf("pebble unavailable")}
	c := rq.rebalanceLease(ctx, rd, localRepl)
	require.NotNil(t, c, "the lease transfer should still be produced")
	require.NotNil(t, c.rebalanceRecord, "an applied lease move must record its cooldown even when Usage() fails")
	require.Equal(t, uint64(1), c.rebalanceRecord.rangeID)
	require.Equal(t, int64(0), c.rebalanceRecord.readQPS)
}
