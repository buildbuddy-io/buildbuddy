package sender_test

import (
	"context"
	"encoding/binary"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	gstatus "google.golang.org/grpc/status"
)

func requireProtoEqual(t *testing.T, expected, actual proto.Message) {
	require.True(t, proto.Equal(expected, actual), "expected proto: %+v, actual: %+v", expected, actual)
}

func TestLookupRangeDescriptor(t *testing.T) {
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "cache.raft.target_range_size_bytes", 0)
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
	flags.Set(t, "cache.raft.target_range_size_bytes", 0)
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

	res, err := sender.LookupRangeDescriptorsForPartition(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, 3, len(res))
	requireProtoEqual(t, s1.GetRange(2), res[0])
	requireProtoEqual(t, s1.GetRange(3), res[1])
	requireProtoEqual(t, s1.GetRange(4), res[2])

	res, err = sender.LookupRangeDescriptorsForPartition(ctx, "foo")
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	requireProtoEqual(t, s1.GetRange(5), res[0])
	requireProtoEqual(t, s1.GetRange(6), res[1])

	res, err = sender.LookupRangeDescriptorsForPartition(ctx, "bar")
	require.NoError(t, err)
	require.Equal(t, 0, len(res))
}

func TestFetchPartitionDescriptors(t *testing.T) {
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "cache.raft.target_range_size_bytes", 0)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	// Start shard to set up meta range
	stores := []*testutil.TestingStore{s1}
	sf.InitializeShardsForMetaRange(t, ctx, stores...)

	// Set up two partitions
	partitions := []disk.Partition{
		{
			ID:        "default",
			NumRanges: 2,
		},
		{
			ID:        "foo",
			NumRanges: 1,
		},
	}

	for _, p := range partitions {
		sf.InitializeShardsForPartition(t, ctx, p, s1)
	}

	// Wait for range leases (meta range + 2 + 1 = 4 ranges total)
	for i := 1; i <= 4; i++ {
		testutil.WaitForRangeLease(t, ctx, stores, uint64(i))
	}

	sender := s1.Sender()
	partitionDescriptors, err := sender.FetchPartitionDescriptors(ctx)
	require.NoError(t, err)

	require.Len(t, partitionDescriptors, 2)

	pd1 := partitionDescriptors[0]
	require.Equal(t, "default", pd1.GetId())
	require.Equal(t, int64(2), pd1.GetInitialNumRanges())
	require.Equal(t, uint64(2), pd1.GetFirstRangeId())

	pd2 := partitionDescriptors[1]
	require.Equal(t, "foo", pd2.GetId())
	require.Equal(t, int64(1), pd2.GetInitialNumRanges())
	require.Equal(t, uint64(4), pd2.GetFirstRangeId())
}

func TestDuplicateApplyOnReplicaRetry(t *testing.T) {
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "cache.raft.target_range_size_bytes", 0)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)

	key := []byte("PTdefault/dup-apply")
	var faultInjected atomic.Bool
	interceptor := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		rsp, err := handler(ctx, req)
		syncReq, ok := req.(*rfpb.SyncProposeRequest)
		shouldFault := ok &&
			len(syncReq.GetBatch().GetUnion()) == 1 &&
			syncReq.GetBatch().GetUnion()[0].GetIncrement() != nil &&
			string(syncReq.GetBatch().GetUnion()[0].GetIncrement().GetKey()) == string(key)
		if err == nil &&
			strings.HasSuffix(info.FullMethod, "/SyncPropose") &&
			shouldFault &&
			faultInjected.CompareAndSwap(false, true) {
			return nil, gstatus.Error(codes.Unavailable, "injected fault after apply")
		}
		return rsp, err
	}

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStoreWithGRPCServerConfig(t, grpc_server.GRPCServerConfig{
		ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{interceptor},
	})
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1}
	sf.StartShard(t, ctx, s1)
	testutil.WaitForRangeLease(t, ctx, stores, 1)
	testutil.WaitForRangeLease(t, ctx, stores, 2)

	value, err := s1.Sender().Increment(ctx, key, 1)
	require.NoError(t, err)
	// This asserts the current buggy behavior: the retried Increment is applied
	// twice after the interceptor returns Unavailable post-apply.
	require.Equal(t, uint64(2), value)

	buf, err := s1.Sender().DirectRead(ctx, key)
	require.NoError(t, err)
	require.Equal(t, uint64(2), binary.LittleEndian.Uint64(buf))
}
