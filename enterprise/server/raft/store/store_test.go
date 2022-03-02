package store_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	//"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbConfig "github.com/lni/dragonboat/v3/config"
)

func getTmpDir(t *testing.T) string {
	dir, err := ioutil.TempDir("/tmp", "buildbuddy_diskcache_*")
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.EnsureDirectoryExists(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	})
	return dir
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", app.FreePort(t))
}

func newGossipManager(t testing.TB, nodeAddr string, seeds []string) *gossip.GossipManager {
	node, err := gossip.NewGossipManager(nodeAddr, seeds)
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
	raftAddrs   []string
	grpcAddrs   []string
	reg         registry.NodeRegistry
}

func newStoreFactory(t *testing.T) *storeFactory {
	rootDir := getTmpDir(t)
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

func (sf *storeFactory) GetRaftAddress(i int) string {
	return sf.raftAddrs[i]
}
func (sf *storeFactory) GetGrpcAddress(i int) string {
	return sf.grpcAddrs[i]
}

type nodeRegistryFactory func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error)

func (nrf nodeRegistryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	return nrf(nhid, streamConnections, v)
}

func (sf *storeFactory) NewStore(t *testing.T) (*store.Store, *dragonboat.NodeHost) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, sf.gossipAddrs)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	raftAddr := localAddr(t)
	sf.raftAddrs = append(sf.raftAddrs, raftAddr)
	grpcAddr := localAddr(t)
	sf.grpcAddrs = append(sf.grpcAddrs, grpcAddr)

	reg := sf.reg
	nrf := nodeRegistryFactory(func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
		return reg, nil
	})

	rootDir := filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs)))
	fileDir := filepath.Join(sf.fileDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs)))
	require.Nil(t, disk.EnsureDirectoryExists(rootDir))
	require.Nil(t, disk.EnsureDirectoryExists(fileDir))

	raftListener := listener.DefaultListener()
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(rootDir, "wal"),
		NodeHostDir:    filepath.Join(rootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    raftAddr,
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

	te := testenv.GetTestEnv(t)
	apiClient := client.NewAPIClient(te, nodeHost.ID())
	rc := rangecache.New()
	gm.AddListener(rc)
	send := sender.New(rc, reg, apiClient)
	reg.AddNode(nodeHost.ID(), raftAddr, grpcAddr)
	s := store.New(rootDir, fileDir, nodeHost, gm, send, reg, apiClient)
	require.NotNil(t, s)
	s.Start(grpcAddr)
	return s, nodeHost
}

func (sf *storeFactory) NewReplica(clusterID, nodeID uint64, s *store.Store) *replica.Replica {
	rootDir := filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs)))
	fileDir := filepath.Join(sf.fileDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs)))

	return replica.New(rootDir, fileDir, clusterID, nodeID, s)
}

func TestAddGetRemoveRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, _ := sf.NewStore(t)
	r1 := sf.NewReplica(1, 1, s1)

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

	stores := []*store.Store{s1, s2, s3}
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

	stores := []*store.Store{s1, s2, s3}
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

func TestGetNodeUsage(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*store.Store{s1, s2, s3}
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

	usage, err := s1.GetNodeUsage(ctx, &rfpb.ReplicaDescriptor{
		ClusterId: 1,
		NodeId:    2,
	})
	require.Nil(t, err)
	require.NotNil(t, usage)
	require.Equal(t, nh2.ID(), usage.GetNhid())
	require.Greater(t, usage.GetDiskBytesTotal(), usage.GetDiskBytesUsed())
}

func TestFindNodes(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	_, nh4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*store.Store{s1, s2, s3}
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

	nds, err := s1.FindNodes(ctx, &rfpb.PlacementQuery{
		TargetClusterId: 1,
	})
	require.Nil(t, err)
	require.Equal(t, 1, len(nds))
	require.Equal(t, nh4.ID(), nds[0].GetNhid())
}

func TestAddNodeToCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	_, nh4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*store.Store{s1, s2, s3}
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

	err = s1.AddNodeToCluster(ctx, rd, &rfpb.NodeDescriptor{
		Nhid:        nh4.ID(),
		RaftAddress: sf.GetRaftAddress(3),
		GrpcAddress: sf.GetGrpcAddress(3),
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

	stores := []*store.Store{s1, s2, s3, s4}
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

	err = s1.RemoveNodeFromCluster(ctx, rd, 4)
	require.Nil(t, err, err)

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.Nil(t, err)
	require.Equal(t, 3, len(replicas))
}
