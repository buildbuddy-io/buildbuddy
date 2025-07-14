package store_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func getMembership(t *testing.T, ts *testutil.TestingStore, ctx context.Context, rangeID uint64) []*rfpb.ReplicaDescriptor {
	membership, err := ts.GetMembership(ctx, rangeID)
	if err != nil {
		log.Errorf("shard not found on %s", ts.NHID())
		if errors.Is(err, dragonboat.ErrShardNotFound) {
			return []*rfpb.ReplicaDescriptor{}
		}
	}
	require.NoError(t, err, "failed to get membership for range %d on store %q", rangeID, ts.NHID())

	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(membership.Nodes))
	for replicaID := range membership.Nodes {
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			RangeId:   rangeID,
			ReplicaId: replicaID,
		})
	}
	return replicas
}

type addToRaftFunc func(t *testing.T, ts *testutil.TestingStore, ctx context.Context, rangeID uint64, replicaID uint64, nhid string)

func addNonVoting(t *testing.T, ts *testutil.TestingStore, ctx context.Context, rangeID uint64, replicaID uint64, nhid string) {
	membership, err := ts.GetMembership(ctx, rangeID)
	require.NoError(t, err)
	ccid := membership.ConfigChangeID
	maxSingleOpTimeout := 3 * time.Second
	err = client.RunNodehostFn(ctx, maxSingleOpTimeout, func(ctx context.Context) error {
		return ts.NodeHost().SyncRequestAddNonVoting(ctx, rangeID, replicaID, nhid, ccid)
	})
	require.NoError(t, err)
}

func addNode(t *testing.T, ts *testutil.TestingStore, ctx context.Context, rangeID uint64, replicaID uint64, nhid string) {
	membership, err := ts.GetMembership(ctx, rangeID)
	require.NoError(t, err)
	ccid := membership.ConfigChangeID
	maxSingleOpTimeout := 3 * time.Second
	err = client.RunNodehostFn(ctx, maxSingleOpTimeout, func(ctx context.Context) error {
		return ts.NodeHost().SyncRequestAddReplica(ctx, rangeID, replicaID, nhid, ccid)
	})
	require.NoError(t, err)
}

func TestConfiguredClusters(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()
	sf.StartShard(t, ctx, s1)
	s1.Stop()
	sf.RecreateStore(t, s1)
	require.Equal(t, 2, s1.ConfiguredClusters())
}

func TestAddGetRemoveRange(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	r1 := s1.NewReplica(1, 1)

	rd := &rfpb.RangeDescriptor{
		Start:   []byte("a"),
		End:     []byte("z"),
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{RangeId: 1, ReplicaId: 1},
			{RangeId: 1, ReplicaId: 2},
			{RangeId: 1, ReplicaId: 3},
		},
	}
	s1.UpdateRange(rd, r1)

	gotRd := s1.GetRange(1)
	require.Equal(t, rd, gotRd)

	s1.RemoveRange(rd, r1)
	gotRd = s1.GetRange(1)
	require.Nil(t, gotRd)
}

func TestCleanupZombieReplicaNotInRangeDescriptor(t *testing.T) {
	// Prevent driver kicks in to add the replica back to the store.
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "gossip.retransmit_mult", 10)
	clock := clockwork.NewFakeClock()

	// set up r1 and r2 on s1, s2, s3; start r2 on s4 as voter; but r2 is not
	// in range descriptor.
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	s4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)

	addNode(t, s, ctx, 2, 4, s4.NHID())

	_, err := s4.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:   2,
		ReplicaId: 4,
		Join:      true,
	})
	require.NoError(t, err)

	now := time.Now()
	for {
		list := s4.ListOpenReplicasForTest()
		if len(list) == 1 {
			repl := list[0]
			// nh1 only has shard 2
			require.Equal(t, uint64(2), repl.GetRangeId())
			break
		} else {
			log.Infof("list open replicas for test: %d", len(list))
		}
		if time.Since(now) > 30*time.Second {
			require.FailNowf(t, "timeout waiting for c2n4 to open on s4: ", "nhid:%s", s3.NHID())
		}
		time.Sleep(50 * time.Millisecond)
	}

	log.Info("====test setup complete====")

	now = time.Now()
	for {
		clock.Advance(11 * time.Second)
		list := s4.ListAllReplicasInHistoryForTest()
		if len(list) == 0 {
			break
		}
		if time.Since(now) > 30*time.Second {
			require.FailNow(t, "timeout waiting for zombie janitor clean up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestCleanupZombieInitialMembersNotSetUp(t *testing.T) {
	// Prevent driver kicks in to add the replica back to the store.
	flags.Set(t, "cache.raft.min_replicas_per_range", 1)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)

	clock := clockwork.NewFakeClock()

	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	startingRanges := []*rfpb.RangeDescriptor{
		&rfpb.RangeDescriptor{
			Start:      constants.MetaRangePrefix,
			End:        keys.Key{constants.UnsplittableMaxByte},
			Generation: 1,
		},
	}
	sf.StartShardWithRanges(t, ctx, startingRanges, stores...)
	testutil.WaitForRangeLease(t, ctx, stores, 1)
	poolB := testutil.MakeNodeGRPCAddressesMap(s1, s2, s3)

	bootstrapInfo := bringup.MakeBootstrapInfo(2, 1, poolB)

	replicaID := uint64(0)
	for _, repl := range bootstrapInfo.Replicas {
		if repl.GetNhid() == s1.NHID() {
			replicaID = repl.GetReplicaId()
		}
	}
	_, err := s1.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:       2,
		ReplicaId:     replicaID,
		InitialMember: bootstrapInfo.InitialMembersForTesting(),
	})
	require.NoError(t, err)

	for {
		clock.Advance(11 * time.Second)
		list := s1.ListAllReplicasInHistoryForTest()
		if len(list) == 1 {
			break
		}
	}
}

func TestCleanupZombieRangeDescriptorNotInMetaRange(t *testing.T) {
	// Prevent driver kicks in to add the replica back to the store.
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)

	clock := clockwork.NewFakeClock()

	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	var rd2 *rfpb.RangeDescriptor
	start := time.Now()
	for {
		rd2 = s1.GetRange(2)
		if len(rd2.GetEnd()) > 0 {
			break
		}
		if time.Since(start) > 30*time.Second {
			require.Fail(t, "failed to get non-empty range descriptor for range 2")
		}
		time.Sleep(50 * time.Millisecond)
	}

	deleteRDBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectDeleteRequest{
		Key: keys.RangeMetaKey(rd2.GetEnd()),
	}).ToProto()
	require.NoError(t, err)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)

	writeRsp, err := s.Sender().SyncPropose(ctx, constants.MetaRangePrefix, deleteRDBatch)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	for {
		clock.Advance(11 * time.Second)
		list1 := s1.ListAllReplicasInHistoryForTest()
		if len(list1) != 1 {
			time.Sleep(10 * time.Millisecond)
			log.Infof("s1(%s) has more than 1 replica", s1.NHID())
			continue
		}
		list2 := s2.ListAllReplicasInHistoryForTest()
		if len(list2) != 1 {
			time.Sleep(10 * time.Millisecond)
			log.Infof("s2(%s) has more than 1 replica", s2.NHID())
			continue
		}
		list3 := s3.ListAllReplicasInHistoryForTest()
		if len(list3) != 1 {
			time.Sleep(10 * time.Millisecond)
			log.Infof("s3(%s) has more than 1 replica", s3.NHID())
			continue
		}
		require.Equal(t, uint64(1), list1[0].GetRangeId())
		require.Equal(t, uint64(1), list2[0].GetRangeId())
		require.Equal(t, uint64(1), list3[0].GetRangeId())
		break
	}
}

func TestAutomaticSplitting(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.max_range_size_bytes", 8000)
	flags.Set(t, "cache.raft.min_replicas_per_range", 1)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1}
	sf.StartShard(t, ctx, stores...)

	testutil.WaitForRangeLease(t, ctx, stores, 1)
	testutil.WaitForRangeLease(t, ctx, stores, 2)
	writeNRecordsAndFlush(ctx, t, s1, 20, 1) // each write is 1000 bytes

	// Advance the clock to trigger scan the queue.
	clock.Advance(61 * time.Second)
	mrd := s1.GetRange(1)
	for {
		ranges := fetchRangeDescriptorsFromMetaRange(ctx, t, s1, mrd)
		if len(ranges) == 2 && s1.HaveLease(ctx, 3) {
			rd2 := s1.GetRange(2)
			rd3 := s1.GetRange(3)
			if !bytes.Equal(rd2.GetEnd(), keys.MaxByte) && bytes.Equal(rd3.GetEnd(), keys.MaxByte) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		clock.Advance(1 * time.Minute)
	}
}

func TestAddReplica(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	storesBefore := []*testutil.TestingStore{s1, s2}
	storesAfter := []*testutil.TestingStore{s1, s2, s3}
	s := testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)

	rd := s.GetRange(2)
	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 2)
	rd = s.GetRange(2)
	require.Equal(t, 3, len(rd.GetReplicas()))
	require.Empty(t, rd.GetStaging())
	{
		maxReplicaID := uint64(0)
		for _, repl := range rd.GetReplicas() {
			if repl.GetReplicaId() > maxReplicaID {
				maxReplicaID = repl.GetReplicaId()
			}
		}
		require.Equal(t, uint64(3), maxReplicaID)
	}
}

func TestAddReplica_MetaRange(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	storesBefore := []*testutil.TestingStore{s1, s2}
	storesAfter := []*testutil.TestingStore{s1, s2, s3}

	s := testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 1)
	mrd := s.GetRange(1)
	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: mrd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 1)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 1)
	rd := s.GetRange(1)
	require.Equal(t, 3, len(rd.GetReplicas()))
	require.Empty(t, rd.GetStaging())
	{
		maxReplicaID := uint64(0)
		for _, repl := range rd.GetReplicas() {
			if repl.GetReplicaId() > maxReplicaID {
				maxReplicaID = repl.GetReplicaId()
			}
		}
		require.Equal(t, uint64(3), maxReplicaID)
	}
}

// This test tests the case where the replica is added to staging in range
// descriptor, but haven't been added to raft at all.
func TestAddReplica_ExistingStaging(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	storesBefore := []*testutil.TestingStore{s1, s2}
	storesAfter := []*testutil.TestingStore{s1, s2, s3}
	s := testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)

	rd := s.GetRange(2)

	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, &rfpb.ReplicaDescriptor{
		RangeId:   2,
		ReplicaId: 3,
		Nhid:      proto.String(s3.NHID()),
	})
	newRD.Generation++
	err := s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)
	rd = s.GetRange(2)
	_, err = s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 2)
	rd = s.GetRange(2)
	require.Equal(t, 3, len(rd.GetReplicas()))
	{
		maxReplicaID := uint64(0)
		for _, repl := range rd.GetReplicas() {
			if repl.GetReplicaId() > maxReplicaID {
				maxReplicaID = repl.GetReplicaId()
			}
		}
		require.Equal(t, uint64(3), maxReplicaID)
	}
}

// This test adds a staging replica that is already been added to raft as a
// non-voter, but the shard is not started.
func TestAddReplica_NonVoterNotStarted(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	storesBefore := []*testutil.TestingStore{s1, s2}
	storesAfter := []*testutil.TestingStore{s1, s2, s3}
	s := testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)

	// add a non-voter c2n3
	addNonVoting(t, s1, ctx, 2, 3, s3.NHID())

	rd := s.GetRange(2)

	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, &rfpb.ReplicaDescriptor{
		RangeId:   2,
		ReplicaId: 3,
		Nhid:      proto.String(s3.NHID()),
	})
	newRD.Generation++
	err := s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)
	rd = s.GetRange(2)
	_, err = s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 2)
	rd = s.GetRange(2)
	require.Equal(t, 3, len(rd.GetReplicas()))
	require.Empty(t, rd.GetStaging())
	{
		maxReplicaID := uint64(0)
		for _, repl := range rd.GetReplicas() {
			if repl.GetReplicaId() > maxReplicaID {
				maxReplicaID = repl.GetReplicaId()
			}
		}
		require.Equal(t, uint64(3), maxReplicaID)
	}
}

// This test adds a staging replica that is already been added to raft as a
// non-voter and the shard is started.
func TestAddReplica_NonVoterStarted(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	storesBefore := []*testutil.TestingStore{s1, s2}
	storesAfter := []*testutil.TestingStore{s1, s2, s3}
	s := testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)

	// add a non-voter c2n3
	addNonVoting(t, s1, ctx, 2, 3, s3.NHID())

	rd := s.GetRange(2)

	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, &rfpb.ReplicaDescriptor{
		RangeId:   2,
		ReplicaId: 3,
		Nhid:      proto.String(s3.NHID()),
	})
	newRD.Generation++
	err := s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	_, err = s3.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:     2,
		ReplicaId:   3,
		Join:        true,
		IsNonVoting: true,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)
	rd = s.GetRange(2)
	_, err = s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 2)
	rd = s.GetRange(2)
	require.Equal(t, 3, len(rd.GetReplicas()))
	require.Empty(t, rd.GetStaging())
	{
		maxReplicaID := uint64(0)
		for _, repl := range rd.GetReplicas() {
			if repl.GetReplicaId() > maxReplicaID {
				maxReplicaID = repl.GetReplicaId()
			}
		}
		require.Equal(t, uint64(3), maxReplicaID)
	}
}

// This test adds a staging replica that is already been added to raft as a
// voter, but the range descriptor has not been updated.
func TestAddReplica_Voter(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	storesBefore := []*testutil.TestingStore{s1, s2}
	storesAfter := []*testutil.TestingStore{s1, s2, s3}
	s := testutil.GetStoreWithRangeLease(t, ctx, storesBefore, 2)

	rd := s.GetRange(2)

	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, &rfpb.ReplicaDescriptor{
		RangeId:   2,
		ReplicaId: 3,
		Nhid:      proto.String(s3.NHID()),
	})
	newRD.Generation++
	err := s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	addNode(t, s, ctx, 2, 3, s3.NHID())

	_, err = s3.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:     2,
		ReplicaId:   3,
		Join:        true,
		IsNonVoting: false,
	})
	require.NoError(t, err)

	for {
		// transfer leadership if the staging replica is the leader.
		s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 2)
		if s.NHID() != newRD.GetStaging()[0].GetNhid() {
			break
		}
		s.TransferLeadership(ctx, &rfpb.TransferLeadershipRequest{
			RangeId:         uint64(2),
			TargetReplicaId: newRD.GetReplicas()[0].GetReplicaId(),
		})
	}

	log.Infof("====test setup complete====")

	rd = s.GetRange(2)
	_, err = s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, storesAfter, 2)
	rd = s.GetRange(2)
	require.Equal(t, 3, len(rd.GetReplicas()))
	require.Empty(t, rd.GetStaging())
	{
		maxReplicaID := uint64(0)
		for _, repl := range rd.GetReplicas() {
			if repl.GetReplicaId() > maxReplicaID {
				maxReplicaID = repl.GetReplicaId()
			}
		}
		require.Equal(t, uint64(3), maxReplicaID)
	}
}

// This test tests removing a normal replica that's started on raft.
func TestRemoveReplicaRemoveData(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	remaining := make([]*testutil.TestingStore, 0, 2)

	// RemoveReplica can't remove the replica on its own machine.
	rd := s.GetRange(2)
	var replicaToRemove *rfpb.ReplicaDescriptor
	var storeRemoveFrom *testutil.TestingStore

	for _, repl := range rd.GetReplicas() {
		if repl.GetNhid() != s.NHID() {
			replicaToRemove = repl
			break
		}
	}
	for _, store := range stores {
		if store.NHID() == replicaToRemove.GetNhid() {
			storeRemoveFrom = store
		} else {
			remaining = append(remaining, store)
		}
	}
	require.NotNil(t, storeRemoveFrom)

	log.Infof("remove replica c%dn%d", rd.GetRangeId(), replicaToRemove.GetReplicaId())
	_, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: replicaToRemove.GetReplicaId(),
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 2, len(replicas))

	rd = s.GetRange(2)
	require.Equal(t, 2, len(rd.GetReplicas()))
	require.Equal(t, 1, len(rd.GetRemoved()))

	list := storeRemoveFrom.ListAllReplicasInHistoryForTest()
	require.Equal(t, 2, len(list))

	log.Infof("=== remove data ===")
	_, err = storeRemoveFrom.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ReplicaId: replicaToRemove.GetReplicaId(),
		Range:     rd,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetRemoved()))

	list = storeRemoveFrom.ListAllReplicasInHistoryForTest()
	require.Equal(t, 1, len(list))

	// Now add the removed replica back to range descriptor to simulate the case
	// where removeData is called on raft, but we failed to update the range
	// descriptor and we retry the RemoveData.
	log.Infof("=== set up test env to test 2nd RemoveData ===")

	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	rd = s.GetRange(2)

	newRD := rd.CloneVT()
	newRD.Removed = append(newRD.Removed, replicaToRemove)
	newRD.Generation++
	err = s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	rd = s.GetRange(2)

	log.Infof("=== remove data #2 ===")
	_, err = storeRemoveFrom.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ReplicaId: 3,
		Range:     rd,
	})
	require.NoError(t, err)
}

func TestRemoveData_ShardStartedNoRangeDescriptor(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	pool := testutil.MakeNodeGRPCAddressesMap(s1, s2, s3)
	bootstrapInfo := bringup.MakeBootstrapInfo(2, 1, pool)
	_, err := s1.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:       3,
		ReplicaId:     1,
		InitialMember: bootstrapInfo.InitialMembersForTesting(),
	})
	require.NoError(t, err)

	_, err = s1.RemoveData(ctx, &rfpb.RemoveDataRequest{
		RangeId:   3,
		ReplicaId: 1,
	})
	require.NoError(t, err)
	list := s1.ListAllReplicasInHistoryForTest()
	require.Equal(t, 2, len(list))
}

// This test tests remove a staging replica that's added and started on raft.
func TestRemoveReplicaRemoveData_StagingReplicaStarted(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)

	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, newRD.Replicas[0])
	newRD.Replicas = newRD.Replicas[1:]
	newRD.Generation++

	log.Infof("new rd: %+v", newRD)
	err := s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	for {
		// transfer leadership if the staging replica is the leader.
		s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
		if s.NHID() != newRD.GetStaging()[0].GetNhid() {
			break
		}
		s.TransferLeadership(ctx, &rfpb.TransferLeadershipRequest{
			RangeId:         uint64(2),
			TargetReplicaId: newRD.GetReplicas()[0].GetReplicaId(),
		})
	}

	log.Infof("=== test setup completed ===")
	replicaID := newRD.GetStaging()[0].GetReplicaId()
	nhid := newRD.GetStaging()[0].GetNhid()
	log.Infof("call nhid %s to remove replica c%dn%d", s.NHID(), rd.GetRangeId(), replicaID)
	removeRsp, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     newRD,
		ReplicaId: replicaID,
	})
	require.NoError(t, err)

	remaining := make([]*testutil.TestingStore, 0, 2)
	var storeRemoveFrom *testutil.TestingStore
	for _, store := range stores {
		if store.NHID() == nhid {
			storeRemoveFrom = store
		} else {
			remaining = append(remaining, store)
		}
	}
	require.NotNil(t, storeRemoveFrom)

	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 2, len(replicas))
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetStaging()))
	require.Equal(t, 1, len(rd.GetRemoved()))

	list := storeRemoveFrom.ListAllReplicasInHistoryForTest()
	require.Equal(t, 2, len(list))

	log.Infof("=== remove data ===")
	_, err = storeRemoveFrom.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ReplicaId: replicaID,
		Range:     removeRsp.GetRange(),
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetRemoved()))

	list = storeRemoveFrom.ListAllReplicasInHistoryForTest()
	require.Equal(t, 1, len(list))
}

// This test tests removing a staging replica that's added to raft but not started.
func testRemoveReplicaRemoveData_StagingReplicaNotStarted(t *testing.T, addFunc addToRaftFunc) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)

	// Set up 3 stores with range 1 on all three stores and range 2 on s1 and s2.
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	stores := []*testutil.TestingStore{s1, s2}

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	rd := s.GetRange(1)
	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	// Add staging replica c2n3.
	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd = s.GetRange(2)
	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, &rfpb.ReplicaDescriptor{
		RangeId:   2,
		ReplicaId: 3,
		Nhid:      proto.String(s3.NHID()),
	})
	newRD.Generation++
	err = s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	// add c2n3 on s3.
	addFunc(t, s, ctx, 2, 3, s3.NHID())

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd = s.GetRange(2)
	removeRsp, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: 3,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 2, len(replicas))
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetStaging()))
	require.Equal(t, 1, len(rd.GetRemoved()))

	list := s3.ListAllReplicasInHistoryForTest()
	require.Equal(t, 1, len(list))

	log.Infof("=== remove data ===")
	_, err = s3.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ReplicaId: 3,
		Range:     removeRsp.GetRange(),
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetRemoved()))

	list = s3.ListAllReplicasInHistoryForTest()
	require.Equal(t, 1, len(list))
}

// This test tests remove a replica that's been added to raft as a voter but
// not started.
func TestRemoveReplicaRemoveData_StagingReplicaVoterNotStarted(t *testing.T, addFunc addToRaftFunc) {
	testRemoveReplicaRemoveData_StagingReplicaNotStarted(t, addNode)
}

// This test tests remove a replica that's been added to raft as a non-voter but
// not started.
func TestRemoveReplicaRemoveData_StagingReplicaNonVoterNotStarted(t *testing.T, addFunc addToRaftFunc) {
	testRemoveReplicaRemoveData_StagingReplicaNotStarted(t, addNonVoting)
}

// This test tests removing a staging replica that's not added to raft.
func TestRemoveReplicaRemoveData_StagingReplicaNotAdded(t *testing.T) {
	// Set up 3 stores with range 1 on all three stores; range 2 on s1 and s2;
	// but not opened on s3.
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	rd := s.GetRange(1)
	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd = s.GetRange(2)
	newRD := rd.CloneVT()
	newRD.Staging = append(newRD.Staging, &rfpb.ReplicaDescriptor{
		RangeId:   2,
		ReplicaId: 3,
		Nhid:      proto.String(s3.NHID()),
	})
	newRD.Generation++

	log.Infof("new rd: %+v", newRD)
	err = s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)
	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)

	log.Infof("=== test setup completed ===")
	removeRsp, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     newRD,
		ReplicaId: 3,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 2, len(replicas))
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetStaging()))
	require.Equal(t, 1, len(rd.GetRemoved()))

	list := s3.ListAllReplicasInHistoryForTest()
	require.Equal(t, 1, len(list))

	log.Infof("=== remove data ===")
	_, err = s3.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ReplicaId: 3,
		Range:     removeRsp.GetRange(),
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd = s.GetRange(2)
	require.Equal(t, 0, len(rd.GetRemoved()))

	list = s3.ListAllReplicasInHistoryForTest()
	require.Equal(t, 1, len(list))
}

func TestAddRangeBack(t *testing.T) {
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.enable_driver", false)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	s4 := sf.NewStore(t)
	ctx := context.Background()
	stores := []*testutil.TestingStore{s1, s2, s3, s4}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)

	// RemoveReplica can't remove the replica on its own machine.
	rd := s.GetRange(2)
	var replicaToRemove *rfpb.ReplicaDescriptor
	remaining := make([]*testutil.TestingStore, 0, 2)
	var testStore *testutil.TestingStore
	for _, repl := range rd.GetReplicas() {
		if repl.GetNhid() != s.NHID() {
			replicaToRemove = repl
			break
		}
	}

	for _, store := range stores {
		if store.NHID() != replicaToRemove.GetNhid() {
			remaining = append(remaining, store)
		} else {
			testStore = store
		}
	}
	require.NotNil(t, testStore)
	log.Infof("remove replica c%dn%d on nodehost %s", rd.GetRangeId(), replicaToRemove.GetReplicaId(), testStore.NHID())

	start := time.Now()
	for {
		rsp, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
			Range:     rd,
			ReplicaId: replicaToRemove.GetReplicaId(),
		})
		if rsp.GetRange() != nil {
			rd = rsp.GetRange()
		}
		if err != nil {
			log.Infof("RemoveReplica failed:%s", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		removeDataRsp, err := testStore.RemoveData(ctx, &rfpb.RemoveDataRequest{
			ReplicaId: replicaToRemove.GetReplicaId(),
			Range:     rd,
		})
		if removeDataRsp.GetRange() != nil {
			rd = removeDataRsp.GetRange()
		}
		if err != nil {
			log.Infof("RemoveData failed:%s", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if time.Since(start) > 60*time.Second {
			require.Fail(t, "unable to remove replica and data")
		}
		break
	}
	s = testutil.GetStoreWithRangeLease(t, ctx, remaining, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 3, len(replicas))

	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: s.GetRange(2),
		Node: &rfpb.NodeDescriptor{
			Nhid:        testStore.NHID(),
			GrpcAddress: testStore.GRPCAddress,
			RaftAddress: testStore.RaftAddress,
		},
	})
	require.NoError(t, err)

	r1, err := s.GetReplica(2)
	require.NoError(t, err)

	lastAppliedIndex, err := r1.LastAppliedIndex()
	require.NoError(t, err)

	r2 := getReplica(t, testStore, 2)
	require.NotEqual(t, replicaToRemove.GetReplicaId(), r2.ReplicaID())

	// Wait for raft replication to finish bringing the new node up to date.
	waitForReplicaToCatchUp(t, ctx, r2, lastAppliedIndex)

	for {
		// Transfer Leadership to the new node
		log.Info("transfer leader")
		_, err = s.TransferLeadership(ctx, &rfpb.TransferLeadershipRequest{
			RangeId:         2,
			TargetReplicaId: r2.ReplicaID(),
		})
		require.NoError(t, err)
		start := time.Now()

		for {
			if testStore.HaveLease(ctx, 2) {
				return
			}
			time.Sleep(10 * time.Millisecond)
			if time.Since(start) > 15*time.Second {
				break
			}
		}
	}
}

func writeRecord(ctx context.Context, t *testing.T, ts *testutil.TestingStore, groupID string, sizeBytes int64) *sgpb.FileRecord {
	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	fr := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   r.GetCacheType(),
			PartitionId: groupID,
		},
		Digest:         r.GetDigest(),
		DigestFunction: r.GetDigestFunction(),
	}

	fs := filestore.New()
	fileMetadataKey := metadataKey(t, fr)

	_, err := ts.APIClient().Get(ctx, ts.GRPCAddress)
	require.NoError(t, err)

	writeCloserMetadata := fs.InlineWriter(ctx, r.GetDigest().GetSizeBytes())
	bytesWritten, err := writeCloserMetadata.Write(buf)
	require.NoError(t, err)

	now := time.Now()
	md := &sgpb.FileMetadata{
		FileRecord:      fr,
		StorageMetadata: writeCloserMetadata.Metadata(),
		StoredSizeBytes: int64(bytesWritten),
		LastModifyUsec:  now.UnixMicro(),
		LastAccessUsec:  now.UnixMicro(),
	}
	protoBytes, err := proto.Marshal(md)
	require.NoError(t, err)

	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: protoBytes,
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := ts.Sender().SyncPropose(ctx, fileMetadataKey, writeReq)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	return fr
}

func metadataKey(t *testing.T, fr *sgpb.FileRecord) []byte {
	fs := filestore.New()
	pebbleKey, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	keyBytes, err := pebbleKey.Bytes(filestore.Version5)
	require.NoError(t, err)
	return keyBytes
}

func fetchRangeDescriptorsFromMetaRange(ctx context.Context, t *testing.T, ts *testutil.TestingStore, metaRangeDescriptor *rfpb.RangeDescriptor) []*rfpb.RangeDescriptor {
	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    keys.RangeMetaKey([]byte{constants.UnsplittableMaxByte}),
		End:      constants.SystemPrefix,
		ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
	}).ToProto()
	require.NoError(t, err)
	repl := metaRangeDescriptor.GetReplicas()[0]
	c, err := ts.APIClient().GetForReplica(ctx, repl)
	require.NoError(t, err)
	for {
		rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: header.New(metaRangeDescriptor, repl, rfpb.Header_LINEARIZABLE),
			Batch:  batchReq,
		})
		if status.IsOutOfRangeError(err) {
			log.Debugf("fetchRangeDescriptorFromMetaRange error: %s", err)
			continue
		} else {
			require.NoError(t, err)
		}
		scanRsp, err := rbuilder.NewBatchResponseFromProto(rsp.GetBatch()).ScanResponse(0)
		require.NoError(t, err)
		res := []*rfpb.RangeDescriptor{}
		for _, kv := range scanRsp.GetKvs() {
			rd := &rfpb.RangeDescriptor{}
			err = proto.Unmarshal(kv.GetValue(), rd)
			require.NoError(t, err)
			res = append(res, rd)
		}
		return res
	}
}

func readRecord(ctx context.Context, t *testing.T, ts *testutil.TestingStore, fr *sgpb.FileRecord) {
	fs := filestore.New()
	fk := metadataKey(t, fr)

	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.GetRequest{
		Key: fk,
	}).ToProto()
	require.NoError(t, err)

	rsp, err := ts.Sender().SyncRead(ctx, fk, readReq)
	require.NoError(t, err)

	rspBatch := rbuilder.NewBatchResponseFromProto(rsp)
	require.NoError(t, rspBatch.AnyError())

	getRsp, err := rspBatch.GetResponse(0)
	require.NoError(t, err)

	md := getRsp.GetFileMetadata()
	rc, err := fs.InlineReader(md.GetStorageMetadata().GetInlineMetadata(), 0, 0)
	require.NoError(t, err)
	d := testdigest.ReadDigestAndClose(t, rc)
	require.True(t, proto.Equal(d, fr.GetDigest()))
}

func writeNRecords(ctx context.Context, t *testing.T, store *testutil.TestingStore, n int) []*sgpb.FileRecord {
	return writeNRecordsAndFlush(ctx, t, store, n, 0)
}
func writeNRecordsAndFlush(ctx context.Context, t *testing.T, store *testutil.TestingStore, n int, flushFreq int) []*sgpb.FileRecord {
	out := make([]*sgpb.FileRecord, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, writeRecord(ctx, t, store, "default", 1000))
		if flushFreq != 0 && (i+1)%flushFreq == 0 {
			store.DB().Flush()
		}
	}
	return out
}

func TestSplitMetaRange(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1)

	rd := s1.GetRange(1)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	writeNRecords(ctx, t, s1, 10)

	// Attempting to Split the metarange should fail.
	_, err := s1.SplitRange(ctx, &rfpb.SplitRangeRequest{
		Range: rd,
	})
	require.Error(t, err)
}

func headerFromRangeDescriptor(rd *rfpb.RangeDescriptor) *rfpb.Header {
	return &rfpb.Header{RangeId: rd.GetRangeId(), Generation: rd.GetGeneration()}
}

func waitForReplicaToCatchUp(t testing.TB, ctx context.Context, r *replica.Replica, desiredLastAppliedIndex uint64) {
	// Wait for raft replication to finish bringing the new node up to date.
	waitStart := time.Now()
	for {
		newReplicaIndex, err := r.LastAppliedIndex()
		require.True(t, err == nil || status.IsNotFoundError(err))
		if err == nil && newReplicaIndex >= desiredLastAppliedIndex {
			log.Infof("Replica caught up in %s", time.Since(waitStart))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func includeReplicaWithNHID(rd *rfpb.RangeDescriptor, nhid string) bool {
	for _, r := range rd.GetReplicas() {
		if r.GetNhid() == nhid {
			return true
		}
	}
	return false
}

func getReplica(t testing.TB, s *testutil.TestingStore, rangeID uint64) *replica.Replica {
	for {
		res, err := s.GetReplica(rangeID)
		if err == nil {
			return res
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSplitNonMetaRange(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	storemap := map[string]*testutil.TestingStore{
		s1.NHID(): s1,
		s2.NHID(): s2,
		s3.NHID(): s3,
	}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)
	// Veirfy that nhid in the range descriptor matches the registry.
	for _, repl := range rd.GetReplicas() {
		raftAddr, _, err := s.Registry.Resolve(repl.GetRangeId(), repl.GetReplicaId())
		require.NoError(t, err)
		replStore := storemap[repl.GetNhid()]
		require.NotNil(t, replStore)
		require.Equal(t, replStore.RaftAddress, raftAddr)
	}
	header := headerFromRangeDescriptor(rd)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, s, 50)
	_, err := s.SplitRange(ctx, &rfpb.SplitRangeRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 3)
	rd = s.GetRange(3)
	// Veirfy that nhid in the range descriptor matches the registry.
	for _, repl := range rd.GetReplicas() {
		raftAddr, _, err := s.Registry.Resolve(repl.GetRangeId(), repl.GetReplicaId())
		require.NoError(t, err)
		replStore := storemap[repl.GetNhid()]
		require.NotNil(t, replStore)
		require.Equal(t, replStore.RaftAddress, raftAddr)
	}
	header = headerFromRangeDescriptor(rd)
	require.Equal(t, 3, len(rd.GetReplicas()))

	// Expect that a new cluster was added with rangeID = 3
	// having 3 replicas.
	replicas := getMembership(t, s1, ctx, 3)
	require.Equal(t, 3, len(replicas))

	// Check that all files are still found.
	for _, fr := range written {
		readRecord(ctx, t, s, fr)
	}

	// Write some more records to the new end range.
	written = append(written, writeNRecords(ctx, t, s1, 50)...)
	_, err = s.SplitRange(ctx, &rfpb.SplitRangeRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	testutil.WaitForRangeLease(t, ctx, stores, 4)

	// Expect that a new cluster was added with rangeID = 4
	// having 3 replicas.
	replicas = getMembership(t, s1, ctx, 4)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s, fr)
	}
}

func TestPostFactoSplit(t *testing.T) {
	flags.Set(t, "cache.raft.min_replicas_per_range", 2)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, s1)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)
	header := headerFromRangeDescriptor(rd)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, s1, 50)

	splitResponse, err := s.SplitRange(ctx, &rfpb.SplitRangeRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	// Expect that a new cluster was added with a replica.
	replicas := getMembership(t, s1, ctx, 3)
	require.Equal(t, 1, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s, fr)
	}

	// Now bring up a new replica in the original cluster.
	_, err = s1.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: s1.GetRange(2),
		Node: &rfpb.NodeDescriptor{
			Nhid:        s2.NHID(),
			RaftAddress: s2.RaftAddress,
			GrpcAddress: s2.GRPCAddress,
		},
	})
	require.NoError(t, err)

	r1, err := s1.GetReplica(2)
	require.NoError(t, err)

	lastAppliedIndex, err := r1.LastAppliedIndex()
	require.NoError(t, err)

	r2 := getReplica(t, s2, 2)

	// Wait for raft replication to finish bringing the new node up to date.
	waitForReplicaToCatchUp(t, ctx, r2, lastAppliedIndex)

	// Transfer Leadership to the new node
	_, err = s.TransferLeadership(ctx, &rfpb.TransferLeadershipRequest{
		RangeId:         2,
		TargetReplicaId: 4,
	})
	require.NoError(t, err)

	// Now verify that all keys that should be on the new node are present.
	for _, fr := range written {
		fmk := metadataKey(t, fr)
		if bytes.Compare(fmk, splitResponse.GetLeft().GetEnd()) >= 0 {
			continue
		}
		readRecord(ctx, t, s, fr)
	}
}

func TestManySplits(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()
	stores := []*testutil.TestingStore{s1}

	sf.StartShard(t, ctx, stores...)
	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)

	var written []*sgpb.FileRecord
	for i := 0; i < 4; i++ {
		written = append(written, writeNRecords(ctx, t, stores[0], 100)...)

		var clusters []uint64
		var seen = make(map[uint64]struct{})
		list := s1.ListOpenReplicasForTest()

		for _, replica := range list {
			rangeID := replica.GetRangeId()
			if _, ok := seen[rangeID]; !ok {
				clusters = append(clusters, rangeID)
				seen[rangeID] = struct{}{}
			}
		}

		for _, rangeID := range clusters {
			if rangeID == 1 {
				continue
			}
			rd := s.GetRange(rangeID)
			header := headerFromRangeDescriptor(rd)
			rsp, err := s.SplitRange(ctx, &rfpb.SplitRangeRequest{
				Header: header,
				Range:  rd,
			})
			require.NoError(t, err)

			testutil.WaitForRangeLease(t, ctx, stores, rsp.GetLeft().GetRangeId())
			testutil.WaitForRangeLease(t, ctx, stores, rsp.GetRight().GetRangeId())

			// Expect that a new cluster was added with the new
			// rangeID and the replica.
			replicas := getMembership(t, s, ctx, rangeID)
			require.Equal(t, 1, len(replicas))
		}

		// Check that all files are found.
		for _, fr := range written {
			readRecord(ctx, t, s, fr)
		}
	}
}

func readSessionIDs(t *testing.T, ctx context.Context, rangeID uint64, store *testutil.TestingStore) []string {
	rd := store.GetRange(rangeID)
	start, end := keys.Range(constants.SessionPrefix)
	req, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    start,
		End:      end,
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).SetHeader(&rfpb.Header{
		RangeId:         rd.GetRangeId(),
		Generation:      rd.GetGeneration(),
		ConsistencyMode: rfpb.Header_STALE,
	}).ToProto()
	require.NoError(t, err)

	maxSingleOpTimeout := 3 * time.Second
	rsp, err := client.SyncReadLocal(ctx, store.NodeHost(), rangeID, req, maxSingleOpTimeout)
	require.NoError(t, err)
	readBatch := rbuilder.NewBatchResponseFromProto(rsp)
	scanRsp, err := readBatch.ScanResponse(0)
	require.NoError(t, err)
	sessionIDs := make([]string, 0, len(scanRsp.GetKvs()))
	for _, kv := range scanRsp.GetKvs() {
		session := &rfpb.Session{}
		err := proto.Unmarshal(kv.GetValue(), session)
		require.NoError(t, err)
		sessionIDs = append(sessionIDs, string(session.GetId()))
	}
	return sessionIDs
}

func TestCleanupExpiredSessions(t *testing.T) {
	flags.Set(t, "cache.raft.client_session_ttl", 5*time.Hour)
	flags.Set(t, "cache.raft.enable_driver", false)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	clock := clockwork.NewFakeClock()

	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, stores...)

	// write some records
	writeNRecords(ctx, t, s1, 10)

	sessionIDsShard1S1 := readSessionIDs(t, ctx, 1, s1)
	sessionIDsShard1S2 := readSessionIDs(t, ctx, 1, s2)
	require.Greater(t, len(sessionIDsShard1S1), 0)
	require.ElementsMatch(t, sessionIDsShard1S1, sessionIDsShard1S2)
	sessionIDsShard2S1 := readSessionIDs(t, ctx, 2, s1)
	sessionIDsShard2S2 := readSessionIDs(t, ctx, 2, s2)
	require.Greater(t, len(sessionIDsShard2S1), 0)
	require.ElementsMatch(t, sessionIDsShard2S1, sessionIDsShard2S2)

	// Returns true if l1 contains at least one element from l2.
	containsAny := func(l1 []string, l2 []string) bool {
		for _, s := range l2 {
			if slices.Contains(l1, s) {
				return true
			}
		}
		return false
	}

	clock.Advance(5*time.Hour + 10*time.Minute)
	for {
		sessionIDsShard1S1After := readSessionIDs(t, ctx, 1, s1)
		if !containsAny(sessionIDsShard1S1After, sessionIDsShard1S1) {
			break
		}
		sessionIDsShard1S2After := readSessionIDs(t, ctx, 1, s2)
		if !containsAny(sessionIDsShard1S2After, sessionIDsShard1S2) {
			break
		}
		sessionIDsShard2S1After := readSessionIDs(t, ctx, 2, s1)
		if !containsAny(sessionIDsShard2S1After, sessionIDsShard2S1) {
			break
		}
		sessionIDsShard2S2After := readSessionIDs(t, ctx, 2, s2)
		if !containsAny(sessionIDsShard2S2After, sessionIDsShard2S2) {
			break
		}
		clock.Advance(65 * time.Second)
	}

}

func TestSplitAcrossClusters(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	poolB := testutil.MakeNodeGRPCAddressesMap(s2)

	startingRanges := []*rfpb.RangeDescriptor{
		&rfpb.RangeDescriptor{
			Start:      constants.MetaRangePrefix,
			End:        keys.Key{constants.UnsplittableMaxByte},
			Generation: 1,
		},
	}
	sf.StartShardWithRanges(t, ctx, startingRanges, s1)
	testutil.WaitForRangeLease(t, ctx, stores, 1)

	// Bringup new peers.
	initialRD := &rfpb.RangeDescriptor{
		Start:      keys.Key{constants.UnsplittableMaxByte},
		End:        keys.MaxByte,
		RangeId:    2,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{RangeId: 2, ReplicaId: 1, Nhid: proto.String(s2.NHID())},
		},
	}
	protoBytes, err := proto.Marshal(initialRD)
	require.NoError(t, err)
	initalRDBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: protoBytes,
		},
	})

	bootstrapInfo := bringup.MakeBootstrapInfo(2, 1, poolB)
	err = bringup.StartShard(ctx, s2, bootstrapInfo, initalRDBatch)
	require.NoError(t, err)

	metaRDBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(initialRD.GetEnd()),
			Value: protoBytes,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   keys.MakeKey(constants.LastReplicaIDKeyPrefix, []byte("2")),
		Delta: uint64(1),
	}).ToProto()
	require.NoError(t, err)

	writeRsp, err := s1.Sender().SyncPropose(ctx, constants.MetaRangePrefix, metaRDBatch)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)
	header := headerFromRangeDescriptor(rd)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, s1, 50)
	_, err = s.SplitRange(ctx, &rfpb.SplitRangeRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 3)

	// Expect that a new cluster was added with rangeID = 3
	// having one replica.
	replicas := getMembership(t, s, ctx, 3)
	require.Equal(t, 1, len(replicas))

	// Write some more records to the new end range.
	written = append(written, writeNRecords(ctx, t, s1, 50)...)

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s1, fr)
	}
}

func TestUpReplicate(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)
	flags.Set(t, "gossip.retransmit_mult", 10)

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	// start shards for s1 and s2
	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, stores...)

	{ // Verify that there are 2 replicas for range 1
		s := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
		replicas := getMembership(t, s, ctx, 1)
		require.Equal(t, 2, len(replicas))
		rd := s.GetRange(1)
		require.Equal(t, 2, len(rd.GetReplicas()))
	}

	{ // Verify that there are 2 replicas for range 2, and also write 10 records
		s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
		writeNRecords(ctx, t, s, 10)
		replicas := getMembership(t, s, ctx, 2)
		require.Equal(t, 2, len(replicas))
		rd := s.GetRange(2)
		require.Equal(t, 2, len(rd.GetReplicas()))
	}

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	r, err := s.GetReplica(2)
	require.NoError(t, err)
	desiredAppliedIndex, err := r.LastAppliedIndex()
	require.NoError(t, err)

	s3 := sf.NewStore(t)
	for {
		// advance the clock to trigger scan replicas
		clock.Advance(61 * time.Second)
		// wait some time to allow let driver queue execute
		time.Sleep(100 * time.Millisecond)
		list := s3.ListOpenReplicasForTest()
		if len(list) < 2 {
			continue
		}

		if len(s1.GetRange(1).GetReplicas()) == 3 &&
			len(s1.GetRange(2).GetReplicas()) == 3 &&
			len(s2.GetRange(1).GetReplicas()) == 3 &&
			len(s2.GetRange(2).GetReplicas()) == 3 {
			break
		}
	}
	r2 := getReplica(t, s3, 2)
	waitForReplicaToCatchUp(t, ctx, r2, desiredAppliedIndex)
	waitStart := time.Now()
	for {
		rd2 := s3.GetRange(2)
		l := len(rd2.GetReplicas())
		if l == 3 {
			break
		}
		if time.Since(waitStart) > 30*time.Second {
			require.Failf(t, "unexpected range descriptor", "Range 2 on s3 has %d replicas, expected 3", l)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestDownReplicate(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "gossip.retransmit_mult", 10)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	// start shards for s1, s2, s3
	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	s4 := sf.NewStore(t)

	// Added a replica for range 2, so the number of replicas for range 2 exceeds the cache.raft.min_replicas_per_range
	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)
	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s4.NHID(),
			RaftAddress: s4.RaftAddress,
			GrpcAddress: s4.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 4, len(replicas))

	stores = append(stores, s4)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	writeNRecords(ctx, t, s, 10)
	db := s.DB()
	db.Flush()
	iter1, err := db.NewIter(&pebble.IterOptions{
		LowerBound: rd.GetStart(),
		UpperBound: rd.GetEnd(),
	})
	require.NoError(t, err)
	defer iter1.Close()
	keysSeen := 0
	for iter1.First(); iter1.Valid(); iter1.Next() {
		keysSeen++
	}
	require.Greater(t, keysSeen, 0)

	rd = s.GetRange(2)
	require.Equal(t, 4, len(rd.GetReplicas()))

	// Advance the clock to trigger scan replicas
	clock.Advance(61 * time.Second)
	nhidToReplicaIDs := make(map[string]uint64)
	for _, r := range rd.GetReplicas() {
		nhidToReplicaIDs[r.GetNhid()] = r.GetReplicaId()
	}
	existingNHIDs := make(map[string]struct{})

	for {
		clock.Advance(3 * time.Second)
		time.Sleep(100 * time.Millisecond)

		s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
		replicas := getMembership(t, s, ctx, 2)
		if len(replicas) > 3 {
			continue
		}

		if len(replicas) == 0 {
			// It's possible that between the function call GetStoreWithRangeLease
			// and getMembership, the replica is removed from the stores and the
			// range lease is deleted. In this case, we want to continue the
			// for loop.
			continue
		}
		rd = s.GetRange(2)
		if len(rd.GetReplicas()) == 3 {
			for _, r := range rd.GetReplicas() {
				existingNHIDs[r.GetNhid()] = struct{}{}
			}
			break
		}
	}

	var removed *testutil.TestingStore
	var removedReplicaID uint64
	for _, s := range stores {
		if _, ok := existingNHIDs[s.NHID()]; !ok {
			removed = s
			removedReplicaID = nhidToReplicaIDs[s.NHID()]
		}
	}
	require.NotNil(t, removed)
	db = removed.DB()

	for {
		clock.Advance(3 * time.Second)
		db.Flush()
		iter2, err := db.NewIter(&pebble.IterOptions{
			LowerBound: rd.GetStart(),
			UpperBound: rd.GetEnd(),
		})
		require.NoError(t, err)
		keysSeen = 0
		for iter2.First(); iter2.Valid(); iter2.Next() {
			keysSeen++
		}
		iter2.Close()
		if keysSeen > 0 {
			continue
		}

		localStart, localEnd := keys.Range(replica.LocalKeyPrefix(2, removedReplicaID))
		iter3, err := db.NewIter(&pebble.IterOptions{
			LowerBound: localStart,
			UpperBound: localEnd,
		})
		require.NoError(t, err)
		localKeysSeen := 0
		for iter3.First(); iter3.Valid(); iter3.Next() {
			localKeysSeen++
		}
		iter3.Close()
		if localKeysSeen == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestReplaceDeadReplica(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "gossip.retransmit_mult", 10)

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	// start shards for s1, s2, s3
	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	{ // Verify that there are 3 replicas for range 2, and also write 10 records
		s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
		writeNRecords(ctx, t, s, 10)
		replicas := getMembership(t, s, ctx, 2)
		require.Equal(t, 3, len(replicas))
		rd := s.GetRange(2)
		require.Equal(t, 3, len(rd.GetReplicas()))
	}

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	r, err := s.GetReplica(2)
	require.NoError(t, err)
	desiredAppliedIndex, err := r.LastAppliedIndex()
	require.NoError(t, err)

	s4 := sf.NewStore(t)
	// Stop store 3
	s3.Stop()

	nhid3 := s3.NodeHost().ID()
	nhid4 := s4.NodeHost().ID()

	// Advance the clock pass the cache.raft.dead_store_timeout so s3 is considered dead.
	clock.Advance(5*time.Minute + 1*time.Second)
	for {
		// advance the clock to trigger scan replicas
		clock.Advance(61 * time.Second)
		// wait some time to allow let driver queue execute
		time.Sleep(100 * time.Millisecond)
		list := s4.ListOpenReplicasForTest()
		if len(list) < 2 {
			continue
		}

		// nhid4 should be added to range 1 and range2, and nhid3 removed
		if !includeReplicaWithNHID(s1.GetRange(1), nhid3) &&
			!includeReplicaWithNHID(s1.GetRange(2), nhid3) &&
			!includeReplicaWithNHID(s2.GetRange(1), nhid3) &&
			!includeReplicaWithNHID(s2.GetRange(2), nhid3) &&
			includeReplicaWithNHID(s1.GetRange(1), nhid4) &&
			includeReplicaWithNHID(s1.GetRange(2), nhid4) &&
			includeReplicaWithNHID(s2.GetRange(1), nhid4) &&
			includeReplicaWithNHID(s2.GetRange(2), nhid4) {
			break
		}
	}

	r2 := getReplica(t, s4, 2)
	waitForReplicaToCatchUp(t, ctx, r2, desiredAppliedIndex)
}

func TestRemoveDeadReplica(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "gossip.retransmit_mult", 10)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	s4 := sf.NewStore(t)
	ctx := context.Background()

	// start shards for s1, s2, s3, s4
	stores := []*testutil.TestingStore{s1, s2, s3, s4}
	sf.StartShard(t, ctx, stores...)

	{ // Verify that there are 4 replicas for range 2
		s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
		replicas := getMembership(t, s, ctx, 2)
		require.Equal(t, 4, len(replicas))
		rd := s.GetRange(2)
		require.Equal(t, 4, len(rd.GetReplicas()))
	}

	// Stop store 4
	log.Infof("=====remove store 4: %q", s4.NHID())
	s4.Stop()

	nhid4 := s4.NodeHost().ID()

	// Advance the clock pass the cache.raft.dead_store_timeout so s4 is considered dead.
	log.Infof("=====verifying that range 1 and 2 are removed from store 4")
	clock.Advance(5*time.Minute + 1*time.Second)
	for {
		// advance the clock to trigger scan replicas
		clock.Advance(61 * time.Second)
		// wait some time to allow let driver queue execute
		time.Sleep(100 * time.Millisecond)

		if includeReplicaWithNHID(s1.GetRange(1), nhid4) {
			log.Infof("range 1 on s1(%q) include replica on nhid4", s1.NHID())
			continue
		}
		if includeReplicaWithNHID(s2.GetRange(1), nhid4) {
			log.Infof("range 1 on s2(%q) include replica on nhid4", s2.NHID())
			continue
		}
		if includeReplicaWithNHID(s3.GetRange(1), nhid4) {
			log.Infof("range 1 on s3(%q) include replica on nhid4", s3.NHID())
			continue
		}
		if includeReplicaWithNHID(s1.GetRange(2), nhid4) {
			log.Infof("range 2 on s1(%q) include replica on nhid4", s1.NHID())
			continue
		}
		if includeReplicaWithNHID(s1.GetRange(2), nhid4) {
			log.Infof("range 2 on s2(%q) include replica on nhid4", s2.NHID())
			continue
		}
		if includeReplicaWithNHID(s1.GetRange(2), nhid4) {
			log.Infof("range 2 on s3(%q) include replica on nhid4", s3.NHID())
			continue
		}
		break
	}
}

func TestRebalance(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "gossip.retransmit_mult", 10)
	// store_test is sensitive to cpu pressure stall on remote executor. Increase
	// the single op timeout to make it less sensitive.
	flags.Set(t, "cache.raft.op_timeout", 3*time.Second)

	startingRanges := []*rfpb.RangeDescriptor{
		&rfpb.RangeDescriptor{
			Start:      constants.MetaRangePrefix,
			End:        keys.Key{constants.UnsplittableMaxByte},
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      keys.Key{constants.UnsplittableMaxByte},
			End:        keys.Key("a"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      keys.Key("a"),
			End:        keys.Key("b"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      keys.Key("b"),
			End:        keys.Key("c"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      keys.Key("c"),
			End:        keys.Key("d"),
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      keys.Key("d"),
			End:        keys.MaxByte,
			Generation: 1,
		},
	}

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	s4 := sf.NewStore(t)
	ctx := context.Background()

	// start shards for s1, s2, s3
	log.Infof("==== start 3 shards====")
	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShardWithRanges(t, ctx, startingRanges, stores...)

	{ // Verify that there are 3 replicas for range 2
		log.Infof("==== verify range 2 has 3 replicas ====")
		s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
		replicas := getMembership(t, s, ctx, 2)
		require.Equal(t, 3, len(replicas))
		rd := s.GetRange(2)
		require.Equal(t, 3, len(rd.GetReplicas()))
	}

	for {
		// advance the clock to trigger scan replicas
		clock.Advance(61 * time.Second)
		// wait some time to allow let driver queue execute
		time.Sleep(100 * time.Millisecond)
		l1 := s1.ListOpenReplicasForTest()
		l2 := s2.ListOpenReplicasForTest()
		l3 := s3.ListOpenReplicasForTest()
		l4 := s4.ListOpenReplicasForTest()

		// store 4 should have at least one replica
		if len(l4) == 0 {
			log.Infof("==== store 4 doesn't have replicas yet ====")
			continue
		}

		size := len(startingRanges)
		if len(l1) < size || len(l2) < size || len(l3) < size {
			break
		}
		log.Infof("==== store 1 has %d replicas; store 2 has %d replicas; store 3 has %d replicas; store 4 has %d replicas", len(l1), len(l2), len(l3), len(l4))
	}
}

func TestBringupSetRanges(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.max_range_size_bytes", 8000)
	flags.Set(t, "cache.raft.min_replicas_per_range", 1)
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "raft.bringup.partition_splits", []bringup.SplitConfig{
		{
			Start:  []byte("PTdefault/0000000000000000000000000000000000000000000000000000000000000000/9/cas/v5"),
			End:    []byte("PTdefault/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/9/cas/v5"),
			Splits: 3,
		},
	})

	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1}
	sf.StartShard(t, ctx, stores...)

	testutil.WaitForRangeLease(t, ctx, stores, 1) // metarange
	testutil.WaitForRangeLease(t, ctx, stores, 2) // start -> 1st split
	testutil.WaitForRangeLease(t, ctx, stores, 3) // 1st split -> 2nd split
	testutil.WaitForRangeLease(t, ctx, stores, 4) // 2nd split -> 3rd split
	testutil.WaitForRangeLease(t, ctx, stores, 5) // 3rd split -> end

	writeNRecordsAndFlush(ctx, t, s1, 20, 1) // each write is 1000 bytes

	mrd := s1.GetRange(1)
	ranges := fetchRangeDescriptorsFromMetaRange(ctx, t, s1, mrd)

	require.Len(t, ranges, 4)
	require.Equal(t, "PTdefault/", string(ranges[0].GetStart()))
	require.Equal(t, "PTdefault/3fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", string(ranges[0].GetEnd()))

	require.Equal(t, "PTdefault/3fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", string(ranges[1].GetStart()))
	require.Equal(t, "PTdefault/7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe", string(ranges[1].GetEnd()))

	require.Equal(t, "PTdefault/7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe", string(ranges[2].GetStart()))
	require.Equal(t, "PTdefault/bffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd", string(ranges[2].GetEnd()))

	require.Equal(t, "PTdefault/bffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd", string(ranges[3].GetStart()))
	require.Equal(t, "PTdefault/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", string(ranges[3].GetEnd()))
}
