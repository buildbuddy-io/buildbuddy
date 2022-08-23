package store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbConfig "github.com/lni/dragonboat/v3/config"
)

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func newGossipManager(t testing.TB, nodeAddr string, seeds []string) *gossip.GossipManager {
	node, err := gossip.NewGossipManager("name-"+nodeAddr, nodeAddr, seeds)
	require.Nil(t, err)
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
	FileDir     string
	RaftAddress string
	GRPCAddress string
}

func (ts *TestingStore) NewReplica(clusterID, nodeID uint64) *replica.Replica {
	return replica.New(ts.RootDir, ts.FileDir, clusterID, nodeID, ts.Store)
}

func (sf *storeFactory) NewStore(t *testing.T) (*TestingStore, *dragonboat.NodeHost) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, sf.gossipAddrs)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	ts := &TestingStore{
		RaftAddress: localAddr(t),
		GRPCAddress: localAddr(t),
		RootDir:     filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs))),
		FileDir:     filepath.Join(sf.fileDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs))),
	}
	require.Nil(t, disk.EnsureDirectoryExists(ts.RootDir))
	require.Nil(t, disk.EnsureDirectoryExists(ts.FileDir))

	reg := sf.reg
	nrf := nodeRegistryFactory(func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
		return reg, nil
	})

	raftListener := listener.DefaultListener()
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
	gm.AddListener(rc)
	ts.Sender = sender.New(rc, reg, apiClient)
	reg.AddNode(nodeHost.ID(), ts.RaftAddress, ts.GRPCAddress)
	s := store.New(ts.RootDir, ts.FileDir, nodeHost, gm, ts.Sender, reg, apiClient)
	require.NotNil(t, s)
	s.Start(ts.GRPCAddress)
	ts.Store = s
	return ts, nodeHost
}

func TestAddGetRemoveRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, _ := sf.NewStore(t)
	r1 := s1.NewReplica(1, 1)

	rd := &rfpb.RangeDescriptor{
		Left:    []byte("a"),
		Right:   []byte("z"),
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	s1.AddRange(rd, r1)

	gotRd := s1.GetRange(1)
	require.Equal(t, rd, gotRd)

	s1.RemoveRange(rd, r1)
	gotRd = s1.GetRange(1)
	require.Nil(t, gotRd)
}

func TestStartCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
	}
	rdBuf, err := proto.Marshal(&rfpb.RangeDescriptor{
		Left:    []byte{constants.MinByte},
		Right:   []byte{constants.MaxByte},
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	})
	require.Nil(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).ToProto()
	require.Nil(t, err)

	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
			Batch:         batchProto,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}
}

func TestGetClusterMembership(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
	}
	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.Nil(t, err)
	require.Equal(t, 3, len(replicas))
}

func TestAddNodeToCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
	}

	rd := &rfpb.RangeDescriptor{
		Left:    []byte{constants.MinByte},
		Right:   []byte{constants.MaxByte},
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	rdBuf, err := proto.Marshal(rd)
	require.Nil(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastNodeIDKey,
		Delta: uint64(3),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(rd.GetRight()),
			Value: rdBuf,
		},
	}).ToProto()
	require.Nil(t, err)

	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
			Batch:         batchProto,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}

	_, err = s1.AddClusterNode(ctx, &rfpb.AddClusterNodeRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        nh4.ID(),
			RaftAddress: s4.RaftAddress,
			GrpcAddress: s4.GRPCAddress,
		},
	})
	require.Nil(t, err, err)

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.Nil(t, err)
	require.Equal(t, 4, len(replicas))
}

func TestRemoveNodeFromCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3, s4}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
		4: nh4.ID(),
	}

	rd := &rfpb.RangeDescriptor{
		Left:    []byte{constants.MinByte},
		Right:   []byte{constants.MaxByte},
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
			{ClusterId: 1, NodeId: 4},
		},
	}
	rdBuf, err := proto.Marshal(rd)
	require.Nil(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastNodeIDKey,
		Delta: uint64(4),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(rd.GetRight()),
			Value: rdBuf,
		},
	}).ToProto()
	require.Nil(t, err)

	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
			Batch:         batchProto,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}

	_, err = s1.RemoveClusterNode(ctx, &rfpb.RemoveClusterNodeRequest{
		Range:  rd,
		NodeId: 4,
	})
	require.Nil(t, err, err)

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.Nil(t, err)
	require.Equal(t, 3, len(replicas))
}

func writeRecord(ctx context.Context, t *testing.T, stores []*TestingStore, groupID string, sizeBytes int64) *rfpb.FileRecord {
	d, buf := testdigest.NewRandomDigestBuf(t, sizeBytes)
	fr := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: groupID,
		},
		Digest: d,
	}
	fk, err := filestore.New(true /*=isolateByGroupIDs*/).FileMetadataKey(fr)
	require.Nil(t, err)

	rd, err := stores[0].Sender.LookupRangeDescriptor(ctx, fk, true /*skipCache*/)
	require.Nil(t, err, err)
	rangeID := rd.GetRangeId()

	for _, ts := range stores {
		c, err := ts.APIClient.Get(ctx, ts.GRPCAddress)
		require.Nil(t, err)
		wc, err := client.RemoteWriter(ctx, c, &rfpb.Header{RangeId: rangeID}, fr)
		require.Nil(t, err)
		_, err = io.Copy(wc, bytes.NewReader(buf))
		require.Nil(t, err)
		require.Nil(t, wc.Close())
	}

	return fr
}

func readRecord(ctx context.Context, t *testing.T, ts *TestingStore, fr *rfpb.FileRecord) {
	fk, err := filestore.New(true /*=isolateByGroupIDs*/).FileMetadataKey(fr)
	require.Nil(t, err)

	err = ts.Sender.Run(ctx, fk, func(c rfspb.ApiClient, h *rfpb.Header) error {
		rc, err := client.RemoteReader(ctx, c, &rfpb.ReadRequest{
			Header:     h,
			FileRecord: fr,
		})
		if err != nil {
			return err
		}
		d := testdigest.ReadDigestAndClose(t, rc)
		require.True(t, proto.Equal(d, fr.GetDigest()))
		return nil
	})
	require.Nil(t, err, err)
}

func writeNRecords(ctx context.Context, t *testing.T, stores []*TestingStore, n int) []*rfpb.FileRecord {
	var groupID string
	out := make([]*rfpb.FileRecord, 0, n)
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			g, err := random.RandomString(16)
			require.Nil(t, err)
			groupID = strings.ToLower(g)
		}
		out = append(out, writeRecord(ctx, t, stores, groupID, 100))
	}
	return out
}

func TestSplitMetaRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
	}

	rd := &rfpb.RangeDescriptor{
		Left:       []byte{constants.MinByte},
		Right:      []byte{constants.MaxByte},
		RangeId:    1,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	rdBuf, err := proto.Marshal(rd)
	require.Nil(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastNodeIDKey,
		Delta: uint64(3),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(rd.GetRight()),
			Value: rdBuf,
		},
	}).ToProto()
	require.Nil(t, err)

	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
			Batch:         batchProto,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, stores, 10)

	_, err = s1.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Range: rd,
	})
	require.Nil(t, err, err)

	// Expect that a new cluster was added with clusterID = 2
	// having 3 replicas.
	replicas, err := s1.GetClusterMembership(ctx, 2)
	require.Nil(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}
}

func TestSplitNonMetaRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
	}

	rd := &rfpb.RangeDescriptor{
		Left:       []byte{constants.MinByte},
		Right:      []byte{constants.MaxByte},
		RangeId:    1,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	rdBuf, err := proto.Marshal(rd)
	require.Nil(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastNodeIDKey,
		Delta: uint64(3),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(rd.GetRight()),
			Value: rdBuf,
		},
	}).ToProto()
	require.Nil(t, err)

	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
			Batch:         batchProto,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, stores, 5)

	_, err = s1.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Range: rd,
	})
	require.Nil(t, err, err)

	// Expect that a new cluster was added with clusterID = 2
	// having 3 replicas.
	replicas, err := s1.GetClusterMembership(ctx, 2)
	require.Nil(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}

	// Write some more records to the new right range.
	written = append(written, writeNRecords(ctx, t, stores, 5)...)

	_, err = s1.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Range: s1.GetRange(2),
	})
	require.Nil(t, err, err)

	// Expect that a new cluster was added with clusterID = 3
	// having 3 replicas.
	replicas, err = s1.GetClusterMembership(ctx, 3)
	require.Nil(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}
}

func TestListCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	initialMembers := map[uint64]string{
		1: nh1.ID(),
		2: nh2.ID(),
		3: nh3.ID(),
	}

	rd := &rfpb.RangeDescriptor{
		Left:    []byte{constants.MinByte},
		Right:   []byte{constants.MaxByte},
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	rdBuf, err := proto.Marshal(rd)
	require.Nil(t, err)
	batchProto, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastNodeIDKey,
		Delta: uint64(3),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(rd.GetRight()),
			Value: rdBuf,
		},
	}).ToProto()
	require.Nil(t, err)

	for i, s := range stores {
		req := &rfpb.StartClusterRequest{
			ClusterId:     uint64(1),
			NodeId:        uint64(i + 1),
			InitialMember: initialMembers,
			Batch:         batchProto,
		}
		_, err := s.StartCluster(ctx, req)
		require.Nil(t, err)
	}

	list, err := s1.ListCluster(ctx, &rfpb.ListClusterRequest{})
	require.Nil(t, err)
	require.Equal(t, 1, len(list.GetRangeReplicas()))
}
