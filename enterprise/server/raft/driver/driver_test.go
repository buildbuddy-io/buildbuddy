package driver

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

func TestCandidateComparison(t *testing.T) {
	expected := []*candidate{
		// Candidate with full disk
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-1"}},
			fullDisk:              true,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          1000,
		},
		// Candidate with range count far above the mean
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-2"}},
			fullDisk:              false,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          1000,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-3"}},
			fullDisk:              false,
			replicaCountMeanLevel: aboveMean,
			replicaCount:          990,
		},
		// Candidate with range count around the mean
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-4"}},
			fullDisk:              false,
			replicaCountMeanLevel: aroundMean,
			replicaCount:          810,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-5"}},
			fullDisk:              false,
			replicaCountMeanLevel: aroundMean,
			replicaCount:          800,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-6"}},
			fullDisk:              false,
			replicaCountMeanLevel: aroundMean,
			replicaCount:          790,
		},
		// Candidate with range count far below the mean
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-7"}},
			fullDisk:              false,
			replicaCountMeanLevel: belowMean,
			replicaCount:          500,
		},
		{
			usage:                 &rfpb.StoreUsage{Node: &rfpb.NodeDescriptor{Nhid: "nhid-8"}},
			fullDisk:              false,
			replicaCountMeanLevel: belowMean,
			replicaCount:          400,
		},
	}

	candidates := make([]*candidate, 0, len(expected))
	copy(expected, candidates)
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	slices.SortFunc(candidates, compareByScore)

	for i, c := range candidates {
		require.EqualExportedValuesf(t, expected[i], c, "candidates[%d] should match expected[%d]")
	}
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
					{ShardId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
					{ShardId: 1, ReplicaId: 2, Nhid: proto.String("nhid-2")},
					{ShardId: 1, ReplicaId: 3, Nhid: proto.String("nhid-3")},
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
					{ShardId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
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
					{ShardId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
				},
			},
			expected: &rfpb.NodeDescriptor{Nhid: "nhid-4"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rq := &Queue{log: log.Logger{}}
			storesWithStats := storemap.CreateStoresWithStats(tc.usages)
			actual := rq.findNodeForAllocation(tc.rd, storesWithStats)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}
