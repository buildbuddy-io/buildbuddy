package store_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/stretchr/testify/require"

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
		node.Leave()
		node.Shutdown()
	})
	return node
}

type storeFactory struct {
	rootDir     string
	fileDir     string
	gossipAddrs []string
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
	}
}

type nodeRegistryFactory func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error)

func (nrf nodeRegistryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	return nrf(nhid, streamConnections, v)
}

func (sf *storeFactory) NewStore(t *testing.T) (*store.Store, string) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, sf.gossipAddrs)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	raftAddr := localAddr(t)
	grpcAddr := localAddr(t)

	var err error
	var reg registry.NodeRegistry
	nrf := nodeRegistryFactory(func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
		reg, err = registry.NewDynamicNodeRegistry(nhid, raftAddr, grpcAddr, gm, streamConnections, v)
		return reg, err
	})

	rootDir := filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs)))
	fileDir := filepath.Join(sf.fileDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs)))

	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(rootDir, "wal"),
		NodeHostDir:    filepath.Join(rootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    raftAddr,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: nrf,
		},
		AddressByNodeHostID: false,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("error creating NodeHost: %s", err)
	}

	te := testenv.GetTestEnv(t)
	apiClient := client.NewAPIClient(te, nodeHost.ID())
	rc := rangecache.New()
	send := sender.New(rc, reg, apiClient)

	s := store.New(rootDir, fileDir, nodeHost, gm, send, reg, apiClient)
	require.NotNil(t, s)
	return s, nodeHost.ID()
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

