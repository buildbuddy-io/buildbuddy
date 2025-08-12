package sender_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func requireProtoEqual(t *testing.T, expected, actual proto.Message) {
	require.True(t, proto.Equal(expected, actual), "expected proto: %+v, actual: %+v", expected, actual)
}

func TestLookupRangeDescriptor(t *testing.T) {
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	// Start shard to set up meta range
	stores := []*testutil.TestingStore{s1}
	sf.StartShard(t, ctx, s1)
	testutil.WaitForRangeLease(t, ctx, stores, 1)
	testutil.WaitForRangeLease(t, ctx, stores, 2)

	// Get sender instance
	sender := s1.Sender()

	rd := s1.GetRange(2)

	gotRD, err := sender.LookupRangeDescriptor(ctx, []byte("PTdefault/"), true)
	require.NoError(t, err)
	requireProtoEqual(t, rd, gotRD)

	gotRD, err = sender.LookupRangeDescriptor(ctx, []byte("PTdefault/aaaa"), true)
	require.NoError(t, err)
	requireProtoEqual(t, rd, gotRD)

	gotRD, err = sender.LookupRangeDescriptor(ctx, []byte("PTdefault/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/cas/v5"), true)
	require.NoError(t, err)
	requireProtoEqual(t, rd, gotRD)
}

func TestLookupRangeDescriptorsForPartition(t *testing.T) {
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	// Start shard to set up meta range
	stores := []*testutil.TestingStore{s1}
	sf.InitializeShardsForMetaRange(t, ctx, stores...)
	partitions := []disk.Partition{
		{
			ID:        "default",
			NumRanges: 3,
		},
		{
			ID:        "foo",
			NumRanges: 2,
		},
	}

	for _, p := range partitions {
		sf.InitializeShardsForPartition(t, ctx, p, s1)
	}

	for i := 1; i <= 6; i++ {
		testutil.WaitForRangeLease(t, ctx, stores, uint64(i))
	}

	sender := s1.Sender()

	res, err := sender.LookupRangeDescriptorsForPartition(ctx, "default", 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	requireProtoEqual(t, s1.GetRange(2), res[0])
	requireProtoEqual(t, s1.GetRange(3), res[1])

	res, err = sender.LookupRangeDescriptorsForPartition(ctx, "foo", 3)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	requireProtoEqual(t, s1.GetRange(5), res[0])
	requireProtoEqual(t, s1.GetRange(6), res[1])

	res, err = sender.LookupRangeDescriptorsForPartition(ctx, "bar", 1)
	require.NoError(t, err)
	require.Equal(t, 0, len(res))
}
