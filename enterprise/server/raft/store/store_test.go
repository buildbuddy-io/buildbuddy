package store_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbConfig "github.com/lni/dragonboat/v4/config"
)

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func newGossipManager(t testing.TB, nodeAddr string, seeds []string) *gossip.GossipManager {
	node, err := gossip.New("name-"+nodeAddr, nodeAddr, seeds)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Shutdown()
	})
	return node
}

type storeFactory struct {
	rootDir     string
	fileDir     string
	gossipAddrs []string
	reg         registry.NodeRegistry
}

func newStoreFactory(t *testing.T) *storeFactory {
	rootDir := testfs.MakeTempDir(t)
	fileDir := filepath.Join(rootDir, "files")
	if err := disk.EnsureDirectoryExists(fileDir); err != nil {
		t.Fatal(err)
	}
	return &storeFactory{
		rootDir: rootDir,
		fileDir: fileDir,
		reg:     registry.NewStaticNodeRegistry(1, nil),
	}
}

type nodeRegistryFactory func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error)

func (nrf nodeRegistryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	return nrf(nhid, streamConnections, v)
}

type TestingStore struct {
	*store.Store
	NodeHost  *dragonboat.NodeHost
	APIClient *client.APIClient
	Sender    *sender.Sender

	RootDir     string
	RaftAddress string
	GRPCAddress string
}

func (ts *TestingStore) NewReplica(shardID, replicaID uint64) *replica.Replica {
	sm := ts.Store.ReplicaFactoryFn(shardID, replicaID)
	return sm.(*replica.Replica)
}

func (sf *storeFactory) NewStore(t *testing.T) (*TestingStore, *dragonboat.NodeHost) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, sf.gossipAddrs)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	ts := &TestingStore{
		RaftAddress: localAddr(t),
		GRPCAddress: localAddr(t),
		RootDir:     filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs))),
	}
	require.Nil(t, disk.EnsureDirectoryExists(ts.RootDir))

	reg := sf.reg
	nrf := nodeRegistryFactory(func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
		return reg, nil
	})

	raftListener := listener.NewRaftListener()
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(ts.RootDir, "wal"),
		NodeHostDir:    filepath.Join(ts.RootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    ts.RaftAddress,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: nrf,
		},
		AddressByNodeHostID: false,
		RaftEventListener:   raftListener,
		SystemEventListener: raftListener,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("error creating NodeHost: %s", err)
	}
	ts.NodeHost = nodeHost

	te := testenv.GetTestEnv(t)
	apiClient := client.NewAPIClient(te, nodeHost.ID())
	ts.APIClient = apiClient

	rc := rangecache.New()
	ts.Sender = sender.New(rc, reg, apiClient)
	reg.AddNode(nodeHost.ID(), ts.RaftAddress, ts.GRPCAddress)
	s, err := store.New(te, ts.RootDir, nodeHost, gm, ts.Sender, reg, raftListener, apiClient, ts.GRPCAddress, []disk.Partition{})
	require.NoError(t, err)
	require.NotNil(t, s)
	s.Start()
	ts.Store = s

	t.Cleanup(func() {
		s.Stop(context.TODO())
		nodeHost.Close()
	})
	return ts, nodeHost
}

func TestAddGetRemoveRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, _ := sf.NewStore(t)
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

func TestStartShard(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)
}

func TestCleanupZombieShards(t *testing.T) {
	flag.Set("cache.raft.zombie_node_scan_interval", "100ms")

	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	stores := []*TestingStore{s1, s2, s3}
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
	writeRsp, err := s1.Sender.SyncPropose(ctx, append([]byte("a"), constants.LocalRangeKey...), writeReq)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	for i := 0; i < 30; i++ {
		list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
		require.NoError(t, err)
		if len(list.GetReplicas()) == 1 {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("Zombie killer never cleaned up zombie range 2")
}

func TestAutomaticSplitting(t *testing.T) {
	flag.Set("cache.raft.entries_between_usage_checks", "1")
	flag.Set("cache.raft.max_range_size_bytes", "10000")

	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	stores := []*TestingStore{s1, s2, s3}
	waitForRangeLease(t, stores, 2)
	writeNRecords(ctx, t, s1, 15) // each write is 1000 bytes

	waitForRangeLease(t, stores, 4)
}

func TestGetMembership(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	replicas, err := s1.GetMembership(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))
}

func TestAddNodeToCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	stores := []*TestingStore{s1, s2, s3, s4}
	waitForRangeLease(t, stores, 2)

	rd := s1.GetRange(1)
	_, err = s1.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        nh4.ID(),
			RaftAddress: s4.RaftAddress,
			GrpcAddress: s4.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas, err := s1.GetMembership(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 4, len(replicas))
}

func TestRemoveNodeFromCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
		nh4.ID(): s4.GRPCAddress,
	})
	require.NoError(t, err)

	stores := []*TestingStore{s1, s2, s3, s4}
	waitForRangeLease(t, stores, 2)

	rd := s1.GetRange(1)
	_, err = s1.RemoveReplica(ctx, &rfpb.RemoveReplicaRequest{
		Range:     rd,
		ReplicaId: 4,
	})
	require.NoError(t, err)

	replicas, err := s1.GetMembership(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))
}

func writeRecord(ctx context.Context, t *testing.T, ts *TestingStore, groupID string, sizeBytes int64) *rfpb.FileRecord {
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

	_, err := ts.APIClient.Get(ctx, ts.GRPCAddress)
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
	writeRsp, err := ts.Sender.SyncPropose(ctx, fileMetadataKey, writeReq)
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

func readRecord(ctx context.Context, t *testing.T, ts *TestingStore, fr *rfpb.FileRecord) {
	fs := filestore.New()
	fk := metadataKey(t, fr)

	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.GetRequest{
		Key: fk,
	}).ToProto()
	require.NoError(t, err)

	rsp, err := ts.Sender.SyncRead(ctx, fk, readReq)
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

func writeNRecords(ctx context.Context, t *testing.T, store *TestingStore, n int) []*rfpb.FileRecord {
	var groupID string
	out := make([]*rfpb.FileRecord, 0, n)
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			g, err := random.RandomString(16)
			require.NoError(t, err)
			groupID = strings.ToLower(g)
		}
		out = append(out, writeRecord(ctx, t, store, groupID, 1000))
	}
	return out
}

func TestSplitMetaRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	rd := s1.GetRange(1)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	writeNRecords(ctx, t, s1, 10)

	// Attempting to Split the metarange should fail.
	_, err = s1.SplitRange(ctx, &rfpb.SplitRangeRequest{
		Range: rd,
	})
	require.Error(t, err)
}

func headerFromRangeDescriptor(rd *rfpb.RangeDescriptor) *rfpb.Header {
	return &rfpb.Header{RangeId: rd.GetRangeId(), Generation: rd.GetGeneration()}
}

func getStoreWithRangeLease(t testing.TB, stores []*TestingStore, rangeID uint64) *TestingStore {
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

func waitForRangeLease(t testing.TB, stores []*TestingStore, rangeID uint64) {
	t.Helper()
	s := getStoreWithRangeLease(t, stores, rangeID)
	log.Printf("%s got range lease for range: %d", s.NodeHost.ID(), rangeID)
}

func TestSplitNonMetaRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
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

	s = getStoreWithRangeLease(t, stores, 4)
	rd = s.GetRange(4)
	header = headerFromRangeDescriptor(rd)

	// Expect that a new cluster was added with shardID = 4
	// having 3 replicas.
	replicas, err := s1.GetMembership(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are still found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
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
	replicas, err = s1.GetMembership(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}
}

func TestListReplicas(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	stores := []*TestingStore{s1, s2, s3}
	waitForRangeLease(t, stores, 2)

	list, err := s1.ListReplicas(ctx, &rfpb.ListReplicasRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, len(list.GetReplicas()))
}

func bytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func TestPostFactoSplit(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3, s4}
	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

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

	// Expect that a new cluster was added with 3 replicas.
	replicas, err := s1.GetMembership(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}

	// Now bring up a new replica in the original cluster.
	_, err = s1.AddReplica(ctx, &rfpb.AddReplicaRequest{
		Range: s1.GetRange(2),
		Node: &rfpb.NodeDescriptor{
			Nhid:        nh4.ID(),
			RaftAddress: s4.RaftAddress,
			GrpcAddress: s4.GRPCAddress,
		},
	})
	require.NoError(t, err)

	r1, err := s1.GetReplica(2)
	require.NoError(t, err)

	lastAppliedIndex, err := r1.LastAppliedIndex()
	require.NoError(t, err)

	var r4 *replica.Replica
	for {
		r4, err = s4.GetReplica(2)
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
		newReplicaIndex, err := r4.LastAppliedIndex()
		if err == nil && newReplicaIndex >= lastAppliedIndex {
			log.Infof("Replica caught up in %s", time.Since(waitStart))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Transfer Leadership to the new node
	_, err = s4.TransferLeadership(ctx, &rfpb.TransferLeadershipRequest{
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
		readRecord(ctx, t, s4, fr)
	}
}

func TestManySplits(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()
	stores := []*TestingStore{s1, s2, s3}

	err := bringup.SendStartShardRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

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
			// shardID and 3 replicas.
			replicas, err := s1.GetMembership(ctx, shardID)
			require.NoError(t, err)
			require.Equal(t, 3, len(replicas))
		}

		// Check that all files are found.
		for _, fr := range written {
			readRecord(ctx, t, s1, fr)
		}
	}
}
