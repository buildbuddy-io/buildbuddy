package testutil

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/jonboulle/clockwork"
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

type StoreFactory struct {
	rootDir     string
	fileDir     string
	gossipAddrs []string
	reg         registry.NodeRegistry
}

func NewStoreFactory(t *testing.T) *StoreFactory {
	rootDir := testfs.MakeTempDir(t)
	fileDir := filepath.Join(rootDir, "files")
	err := disk.EnsureDirectoryExists(fileDir)
	require.NoError(t, err)
	return &StoreFactory{
		rootDir: rootDir,
		fileDir: fileDir,
		reg:     registry.NewStaticNodeRegistry(1, nil),
	}
}

type nodeRegistryFactory func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error)

func (nrf nodeRegistryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	return nrf(nhid, streamConnections, v)
}

func (sf *StoreFactory) Registry() registry.NodeRegistry {
	return sf.reg
}

func (sf *StoreFactory) NewStore(t *testing.T) *TestingStore {
	return sf.NewStoreWithClock(t, clockwork.NewRealClock())
}

func (sf *StoreFactory) NewStoreWithClock(t *testing.T, clock clockwork.Clock) *TestingStore {
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
	require.NoError(t, err, "unexpected error creating NodeHost")

	te := testenv.GetTestEnv(t)
	apiClient := client.NewAPIClient(te, nodeHost.ID(), reg)

	rc := rangecache.New()
	s := sender.New(rc, apiClient)
	reg.AddNode(nodeHost.ID(), ts.RaftAddress, ts.GRPCAddress)
	partitions := []disk.Partition{
		{
			ID:           "default",
			MaxSizeBytes: int64(1_000_000_000), // 1G
		},
	}
	db, err := pebble.Open(ts.RootDir, "raft_store", &pebble.Options{})
	require.NoError(t, err)
	ts.db = db
	leaser := pebble.NewDBLeaser(db)
	store, err := store.NewWithArgs(te, ts.RootDir, nodeHost, gm, s, reg, raftListener, apiClient, ts.GRPCAddress, partitions, db, leaser, clock)
	require.NoError(t, err)
	require.NotNil(t, store)
	store.Start()
	ts.Store = store

	t.Cleanup(func() {
		store.Stop(context.TODO())
	})
	return ts
}

func MakeNodeGRPCAddressesMap(stores ...*TestingStore) map[string]string {
	res := make(map[string]string, len(stores))
	for _, s := range stores {
		res[s.NHID()] = s.GRPCAddress
	}
	return res
}

type TestingStore struct {
	*store.Store

	db pebble.IPebbleDB

	RootDir     string
	RaftAddress string
	GRPCAddress string
}

func (ts *TestingStore) DB() pebble.IPebbleDB {
	return ts.db
}

func (ts *TestingStore) NewReplica(shardID, replicaID uint64) *replica.Replica {
	sm := ts.Store.ReplicaFactoryFn(shardID, replicaID)
	return sm.(*replica.Replica)
}

func StartShard(t *testing.T, ctx context.Context, stores ...*TestingStore) {
	require.Greater(t, len(stores), 0)
	err := bringup.SendStartShardRequests(ctx, stores[0].NodeHost(), stores[0].APIClient(), MakeNodeGRPCAddressesMap(stores...))
	require.NoError(t, err)
}

func StartShardWithRanges(t *testing.T, ctx context.Context, startingRanges []*rfpb.RangeDescriptor, stores ...*TestingStore) {
	require.Greater(t, len(stores), 0)
	err := bringup.SendStartShardRequestsWithRanges(ctx, stores[0].NodeHost(), stores[0].APIClient(), MakeNodeGRPCAddressesMap(stores...), startingRanges)
	require.NoError(t, err)
}
