package store_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
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
)

func getMembership(t *testing.T, ts *testutil.TestingStore, ctx context.Context, shardID uint64) []*rfpb.ReplicaDescriptor {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		membership, err = ts.NodeHost().SyncGetShardMembership(ctx, shardID)
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
	require.NoError(t, err)

	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(membership.Nodes))
	for replicaID := range membership.Nodes {
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			ShardId:   shardID,
			ReplicaId: replicaID,
		})
	}
	return replicas
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
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}
	s1.AddRange(rd, r1)

	gotRd := s1.GetRange(1)
	require.Equal(t, rd, gotRd)

	s1.RemoveRange(rd, r1)
	gotRd = s1.GetRange(1)
	require.Nil(t, gotRd)
}

func TestCleanupZombieShards(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStoreWithClock(t, clock)
	ctx := context.Background()

	testutil.StartShard(t, ctx, s1)

	stores := []*testutil.TestingStore{s1}
	waitForRangeLease(t, stores, 2)

	// Reach in and zero out the range descriptor on one of the ranges,
	// effectively making it a zombie range.
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: nil,
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, append([]byte("a"), constants.LocalRangeKey...), writeReq)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
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

func TestCleanupZombieReplicas(t *testing.T) {
	clock := clockwork.NewFakeClock()

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStoreWithClock(t, clock)
	s2 := sf.NewStoreWithClock(t, clock)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	testutil.StartShard(t, ctx, stores...)

	waitForRangeLease(t, stores, 2)

	s := getStoreWithRangeLease(t, stores, 2)
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
	protoBytes, err := proto.Marshal(newRD)
	require.NoError(t, err)

	// Write the range descriptor the meta range
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newRD.GetEnd()),
			Value: protoBytes,
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, constants.MetaRangePrefix, writeReq)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	for {
		clock.Advance(11 * time.Second)
		list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list.GetReplicas()) == 1 {
			repl := list.GetReplicas()[0]
			// nh1 only has shard 1
			require.Equal(t, uint64(1), repl.GetShardId())
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

func TestAutomaticSplitting(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.max_range_size_bytes", 10000)

	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1}
	testutil.StartShard(t, ctx, stores...)

	waitForRangeLease(t, stores, 2)
	writeNRecords(ctx, t, s1, 15) // each write is 1000 bytes

	waitForRangeLease(t, stores, 4)
}

func TestAddNodeToCluster(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	testutil.StartShard(t, ctx, s1)

	stores := []*testutil.TestingStore{s1, s2}
	s := getStoreWithRangeLease(t, stores, 2)

	rd := s.GetRange(2)
	_, err := s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s2.NHID(),
			RaftAddress: s2.RaftAddress,
			GrpcAddress: s2.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 2, len(replicas))

	s = getStoreWithRangeLease(t, stores, 2)
	rd = s.GetRange(2)
	require.Equal(t, 2, len(rd.GetReplicas()))

	// Add Replica for meta range
	mrd := s.GetRange(1)
	_, err = s.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: mrd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        s2.NHID(),
			RaftAddress: s2.RaftAddress,
			GrpcAddress: s2.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas = getMembership(t, s, ctx, 1)
	require.Equal(t, 2, len(replicas))

	s = getStoreWithRangeLease(t, stores, 1)
	rd = s.GetRange(1)
	require.Equal(t, 2, len(rd.GetReplicas()))
}

func TestRemoveNodeFromCluster(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	testutil.StartShard(t, ctx, stores...)

	s := getStoreWithRangeLease(t, stores, 2)

	rd := s.GetRange(2)
	_, err := s.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: 4,
	})
	require.NoError(t, err)

	s = getStoreWithRangeLease(t, stores, 2)
	replicas := getMembership(t, s, ctx, 2)
	require.Equal(t, 1, len(replicas))
	rd = s.GetRange(2)
	require.Equal(t, 1, len(rd.GetReplicas()))
}

func writeRecord(ctx context.Context, t *testing.T, ts *testutil.TestingStore, groupID string, sizeBytes int64) *rfpb.FileRecord {
	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	fr := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
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
	md := &rfpb.FileMetadata{
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

func metadataKey(t *testing.T, fr *rfpb.FileRecord) []byte {
	fs := filestore.New()
	pebbleKey, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	keyBytes, err := pebbleKey.Bytes(filestore.Version5)
	require.NoError(t, err)
	return keyBytes
}

func readRecord(ctx context.Context, t *testing.T, ts *testutil.TestingStore, fr *rfpb.FileRecord) {
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

func writeNRecords(ctx context.Context, t *testing.T, store *testutil.TestingStore, n int) []*rfpb.FileRecord {
	out := make([]*rfpb.FileRecord, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, writeRecord(ctx, t, store, "default", 1000))
	}
	return out
}

func TestSplitMetaRange(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	ctx := context.Background()

	testutil.StartShard(t, ctx, s1)

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

func getStoreWithRangeLease(t testing.TB, stores []*testutil.TestingStore, rangeID uint64) *testutil.TestingStore {
	t.Helper()

	start := time.Now()
	for {
		for _, store := range stores {
			if store.HaveLease(rangeID) {
				return store
			}
		}
		time.Sleep(10 * time.Millisecond)
		if time.Since(start) > 60*time.Second {
			break
		}
	}

	t.Fatalf("No store found holding rangelease for range: %d", rangeID)
	return nil
}

func waitForRangeLease(t testing.TB, stores []*testutil.TestingStore, rangeID uint64) {
	t.Helper()
	s := getStoreWithRangeLease(t, stores, rangeID)
	log.Printf("%s got range lease for range: %d", s.NHID(), rangeID)
}

func TestSplitNonMetaRange(t *testing.T) {
	flags.Set(t, "cache.raft.max_range_size_bytes", 0) // disable auto splitting
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	testutil.StartShard(t, ctx, stores...)

	s := getStoreWithRangeLease(t, stores, 2)
	rd := s.GetRange(2)
	// Veirfy that nhid in the rangea descriptor matches the registry.
	for _, repl := range rd.GetReplicas() {
		nhid, _, err := sf.Registry().ResolveNHID(repl.GetShardId(), repl.GetReplicaId())
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

	s = getStoreWithRangeLease(t, stores, 4)
	rd = s.GetRange(4)
	// Veirfy that nhid in the rangea descriptor matches the registry.
	for _, repl := range rd.GetReplicas() {
		nhid, _, err := sf.Registry().ResolveNHID(repl.GetShardId(), repl.GetReplicaId())
		require.NoError(t, err)
		require.Equal(t, repl.GetNhid(), nhid)
	}
	header = headerFromRangeDescriptor(rd)
	require.Equal(t, 3, len(rd.GetReplicas()))

	// Expect that a new cluster was added with shardID = 4
	// having 3 replicas.
	replicas := getMembership(t, s1, ctx, 4)
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

	waitForRangeLease(t, stores, 5)

	// Expect that a new cluster was added with shardID = 5
	// having 3 replicas.
	replicas = getMembership(t, s1, ctx, 5)
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
	testutil.StartShard(t, ctx, stores...)
	waitForRangeLease(t, stores, 2)

	list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, len(list.GetReplicas()))
}

func TestPostFactoSplit(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2}
	testutil.StartShard(t, ctx, s1)

	s := getStoreWithRangeLease(t, stores, 2)
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
	replicas := getMembership(t, s1, ctx, 4)
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

	var r2 *replica.Replica
	for {
		r2, err = s2.GetReplica(2)
		if err == nil {
			break
		}
		if !status.IsOutOfRangeError(err) {
			require.FailNowf(t, "unexpected error", "unexpected error %s", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for raft replication to finish bringing the new node up to date.
	waitStart := time.Now()
	for {
		newReplicaIndex, err := r2.LastAppliedIndex()
		if err == nil && newReplicaIndex >= lastAppliedIndex {
			log.Infof("Replica caught up in %s", time.Since(waitStart))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Transfer Leadership to the new node
	_, err = s.TransferLeadership(ctx, &rfpb.TransferLeadershipRequest{
		ShardId:         2,
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

	testutil.StartShard(t, ctx, stores...)
	s := getStoreWithRangeLease(t, stores, 2)

	var written []*rfpb.FileRecord
	for i := 0; i < 4; i++ {
		written = append(written, writeNRecords(ctx, t, stores[0], 100)...)

		var clusters []uint64
		var seen = make(map[uint64]struct{})
		list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)

		for _, replica := range list.GetReplicas() {
			shardID := replica.GetShardId()
			if _, ok := seen[shardID]; !ok {
				clusters = append(clusters, shardID)
				seen[shardID] = struct{}{}
			}
		}

		for _, shardID := range clusters {
			if shardID == 1 {
				continue
			}
			rd := s.GetRange(shardID)
			header := headerFromRangeDescriptor(rd)
			rsp, err := s.SplitRange(ctx, &rfpb.SplitRangeRequest{
				Header: header,
				Range:  rd,
			})
			require.NoError(t, err)

			waitForRangeLease(t, stores, rsp.GetLeft().GetRangeId())
			waitForRangeLease(t, stores, rsp.GetRight().GetRangeId())

			// Expect that a new cluster was added with the new
			// shardID and the replica.
			replicas := getMembership(t, s, ctx, shardID)
			require.Equal(t, 1, len(replicas))
		}

		// Check that all files are found.
		for _, fr := range written {
			readRecord(ctx, t, s, fr)
		}
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
			Start:      keys.MinByte,
			End:        keys.Key{constants.UnsplittableMaxByte},
			Generation: 1,
		},
	}
	testutil.StartShardWithRanges(t, ctx, startingRanges, s1)
	waitForRangeLease(t, stores, 1)

	// Bringup new peers.
	initialRD := &rfpb.RangeDescriptor{
		Start:      keys.Key{constants.UnsplittableMaxByte},
		End:        keys.MaxByte,
		RangeId:    2,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 2, ReplicaId: 1, Nhid: proto.String(s2.NHID())},
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
	}).ToProto()
	require.NoError(t, err)

	writeRsp, err := s1.Sender().SyncPropose(ctx, constants.MetaRangePrefix, metaRDBatch)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	s := getStoreWithRangeLease(t, stores, 2)
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

	s = getStoreWithRangeLease(t, stores, 3)

	// Expect that a new cluster was added with shardID = 3
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
