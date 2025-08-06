package sender_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
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
