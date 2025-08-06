package sender_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

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
	require.True(t, proto.Equal(rd, gotRD))

	gotRD, err = sender.LookupRangeDescriptor(ctx, []byte("PTdefault/aaaa"), true)
	require.NoError(t, err)
	require.True(t, proto.Equal(rd, gotRD))

	gotRD, err = sender.LookupRangeDescriptor(ctx, []byte("PTdefault/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/cas/v5"), true)
	require.NoError(t, err)
	require.True(t, proto.Equal(rd, gotRD))
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
	startingRanges := []*rfpb.RangeDescriptor{
		&rfpb.RangeDescriptor{
			Start:      constants.MetaRangePrefix,
			End:        keys.Key{constants.UnsplittableMaxByte},
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      []byte("PTdefault/"),
			End:        []byte("PTdefault/d"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      []byte("PTdefault/d"),
			End:        []byte("PTdefault/h"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      []byte("PTdefault/d"),
			End:        keys.MakeKey([]byte("PTdefault/z"), keys.MaxByte),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      []byte("PTfoo/"),
			End:        []byte("PTfoo/h"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      []byte("PTfoo/d"),
			End:        keys.MakeKey([]byte("PTfoo/z"), keys.MaxByte),
			Generation: 1,
		},
	}
	sf.StartShardWithRanges(t, ctx, startingRanges, stores...)

	for i := 1; i <= len(startingRanges); i++ {
		testutil.WaitForRangeLease(t, ctx, stores, uint64(i))
	}

	sender := s1.Sender()

	res, err := sender.LookupRangeDescriptorsForPartition(ctx, disk.Partition{
		ID:        "default",
		NumRanges: 2,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.True(t, proto.Equal(startingRanges[1], res[0]))
	require.True(t, proto.Equal(startingRanges[2], res[1]))

	res, err = sender.LookupRangeDescriptorsForPartition(ctx, disk.Partition{
		ID:        "foo",
		NumRanges: 3,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.True(t, proto.Equal(startingRanges[4], res[0]))
	require.True(t, proto.Equal(startingRanges[5], res[1]))

	res, err = sender.LookupRangeDescriptorsForPartition(ctx, disk.Partition{
		ID:        "bar",
		NumRanges: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(res))
}
