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
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		membership, err = ts.NodeHost().SyncGetShardMembership(ctx, rangeID)
		if err != nil {
			return err
		}
		// Trick client.RunNodehostFn into running this again if we got a nil
		// membership back
		if membership == nil {
			return status.OutOfRangeErrorf("cluster not ready")
		}
		return nil
	})

	if err != nil {
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
	s1.AddRange(rd, r1)

	gotRd := s1.GetRange(1)
	require.Equal(t, rd, gotRd)

	s1.RemoveRange(rd, r1)
	gotRd = s1.GetRange(1)
	require.Nil(t, gotRd)
}

func TestCleanupZombieReplicas(t *testing.T) {
	// Prevent driver kicks in to add the replica back to the store.
	flags.Set(t, "cache.raft.min_replicas_per_range", 1)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)

	clock := clockwork.NewFakeClock()

	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, stores...)

	testutil.WaitForRangeLease(t, ctx, stores, 2)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)
	newRD := rd.CloneVT()

	require.Equal(t, len(newRD.GetReplicas()), 2)

	// Remove replica of range 2 on nh1 in meta range
	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(rd.GetReplicas())-1)
	for _, repl := range rd.GetReplicas() {
		if repl.GetNhid() == s1.NHID() {
			continue
		}
		replicas = append(replicas, repl)
	}
	newRD.Replicas = replicas
	require.Equal(t, 1, len(replicas))
	newRD.Generation = rd.GetGeneration() + 1

	err := s.UpdateRangeDescriptor(ctx, 2, rd, newRD)
	require.NoError(t, err)

	for {
		clock.Advance(11 * time.Second)
		list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list.GetReplicas()) == 1 {
			repl := list.GetReplicas()[0]
			// nh1 only has shard 1
			require.Equal(t, uint64(1), repl.GetRangeId())
			break
		}
	}

	for i := 0; i <= 30; i++ {
		rd := s1.GetRange(2)
		if rd == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, err = s1.GetReplica(2)
	require.True(t, status.IsOutOfRangeError(err))
	// verify that the range and replica is not removed from s2
	list, err := s2.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, len(list.GetReplicas()))
	require.NotNil(t, s2.GetRange(2))
	_, err = s2.GetReplica(2)
	require.NoError(t, err)
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

	c1, err := s1.APIClient().Get(ctx, s1.GRPCAddress)
	require.NoError(t, err)
	bootstrapInfo := bringup.MakeBootstrapInfo(2, 1, poolB)
	rd := &rfpb.RangeDescriptor{
		Start:      keys.MakeKey([]byte("a")),
		End:        keys.MakeKey([]byte("z")),
		RangeId:    2,
		Generation: 1,
	}
	protoBytes, err := proto.Marshal(rd)
	require.NoError(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: protoBytes,
		},
	}).ToProto()
	require.NoError(t, err)
	_, err = c1.StartShard(ctx, &rfpb.StartShardRequest{
		RangeId:       2,
		ReplicaId:     1,
		InitialMember: bootstrapInfo.InitialMembersForTesting(),
		Batch:         batchProto,
	})
	require.NoError(t, err)

	for {
		clock.Advance(11 * time.Second)
		list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list.GetReplicas()) == 1 {
			break
		}
	}
}

func TestCleanupZombieRangeDescriptorNotInMetaRange(t *testing.T) {
	// Prevent driver kicks in to add the replica back to the store.
	flags.Set(t, "cache.raft.min_replicas_per_range", 1)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)

	clock := clockwork.NewFakeClock()

	sf := testutil.NewStoreFactoryWithClock(t, clock)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, stores...)

	rd2 := s1.GetRange(2)

	deleteRDBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectDeleteRequest{
		Key: keys.RangeMetaKey(rd2.GetEnd()),
	}).ToProto()
	require.NoError(t, err)

	writeRsp, err := s1.Sender().SyncPropose(ctx, constants.MetaRangePrefix, deleteRDBatch)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	for {
		clock.Advance(11 * time.Second)
		list1, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list1.GetReplicas()) != 1 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		list2, err := s2.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list2.GetReplicas()) != 1 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		require.Equal(t, uint64(1), list1.GetReplicas()[0].GetRangeId())
		require.Equal(t, uint64(1), list2.GetReplicas()[0].GetRangeId())
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

func TestAddNodeToCluster(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	sf.StartShard(t, ctx, s1, s2)

	stores := []*testutil.TestingStore{s1, s2}
	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)

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

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
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

	// Add Replica for meta range
	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	mrd := s.GetRange(1)
	_, err = s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: mrd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s3.NHID(),
			RaftAddress: s3.RaftAddress,
			GrpcAddress: s3.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas = getMembership(t, s, ctx, 1)
	require.Equal(t, 3, len(replicas))

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	rd = s.GetRange(1)
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

func TestRemoveNodeFromCluster(t *testing.T) {
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup.
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)

	// RemoveReplica can't remove the replica on its own machine.
	rd := s.GetRange(2)
	replicaIdToRemove := uint64(0)
	for _, repl := range rd.GetReplicas() {
		if repl.GetNhid() != s.NHID() {
			replicaIdToRemove = repl.GetReplicaId()
			break
		}
	}
	log.Infof("remove replica c%dn%d", rd.GetRangeId(), replicaIdToRemove)
	_, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: replicaIdToRemove,
	})
	require.NoError(t, err)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 1, len(replicas))
	rd = s.GetRange(2)
	require.Equal(t, 1, len(rd.GetReplicas()))
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
	c, err := ts.APIClient().GetForReplica(ctx, metaRangeDescriptor.GetReplicas()[0])
	require.NoError(t, err)
	for {
		rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: header.New(metaRangeDescriptor, 0, rfpb.Header_LINEARIZABLE),
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
		require.False(t, status.IsOutOfRangeError(err))
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSplitNonMetaRange(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)
	// Veirfy that nhid in the range descriptor matches the registry.
	for _, repl := range rd.GetReplicas() {
		nhid, _, err := sf.Registry().ResolveNHID(ctx, repl.GetRangeId(), repl.GetReplicaId())
		require.NoError(t, err)
		require.Equal(t, repl.GetNhid(), nhid)
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
	// Veirfy that nhid in the rangea descriptor matches the registry.
	for _, repl := range rd.GetReplicas() {
		nhid, _, err := sf.Registry().ResolveNHID(ctx, repl.GetRangeId(), repl.GetReplicaId())
		require.NoError(t, err)
		require.Equal(t, repl.GetNhid(), nhid)
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

func TestListReplicas(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)
	testutil.WaitForRangeLease(t, ctx, stores, 2)

	list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, len(list.GetReplicas()))
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
		list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)

		for _, replica := range list.GetReplicas() {
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

	rsp, err := client.SyncReadLocal(ctx, store.NodeHost(), rangeID, req)
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
	err = bringup.StartShard(ctx, s2.APIClient(), bootstrapInfo, initalRDBatch)
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
		list, err := s3.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list.GetReplicas()) < 2 {
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
		l := len(r2.RangeDescriptor().GetReplicas())
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
			// and getMembership, the replica is removed from the store s and the
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

	for i := 0; ; i++ {
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
		if i >= 5 {
			require.Zero(t, keysSeen, 0, "range is expected to be empty but have %d keys", keysSeen)
			require.Zero(t, localKeysSeen, 0, "local range is expected to be empty but have %d keys", localKeysSeen)
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
		list, err := s4.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list.GetReplicas()) < 2 {
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
	s4.Stop()

	nhid4 := s4.NodeHost().ID()

	// Advance the clock pass the cache.raft.dead_store_timeout so s4 is considered dead.
	clock.Advance(5*time.Minute + 1*time.Second)
	for {
		// advance the clock to trigger scan replicas
		clock.Advance(61 * time.Second)
		// wait some time to allow let driver queue execute
		time.Sleep(100 * time.Millisecond)

		if !includeReplicaWithNHID(s1.GetRange(1), nhid4) &&
			!includeReplicaWithNHID(s1.GetRange(2), nhid4) &&
			!includeReplicaWithNHID(s2.GetRange(1), nhid4) &&
			!includeReplicaWithNHID(s2.GetRange(2), nhid4) &&
			!includeReplicaWithNHID(s3.GetRange(1), nhid4) &&
			!includeReplicaWithNHID(s3.GetRange(2), nhid4) {
			break
		}
	}
}

func TestRebalance(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	// disable txn cleanup and zombie scan, because advance the fake clock can
	// prematurely trigger txn cleanup and zombie cleanup
	flags.Set(t, "cache.raft.enable_txn_cleanup", false)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)

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

	// start shards for s1, s2, s3, s4
	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShardWithRanges(t, ctx, startingRanges, stores...)

	{ // Verify that there are 3 replicas for range 2
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
		l1, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		l2, err := s2.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		l3, err := s3.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		l4, err := s4.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)

		// store 4 should have at least one replica
		if len(l4.GetReplicas()) == 0 {
			continue
		}

		size := len(startingRanges)
		if len(l1.GetReplicas()) < size || len(l2.GetReplicas()) < size || len(l3.GetReplicas()) < size {
			break
		}
	}
}
