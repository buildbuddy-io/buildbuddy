package testutil

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

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
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbcl "github.com/lni/dragonboat/v4/client"
	dbConfig "github.com/lni/dragonboat/v4/config"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
	dbrd "github.com/lni/goutils/random"
)

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

type StoreFactory struct {
	rootDir     string
	fileDir     string
	gossipAddrs []string
	reg         registry.NodeRegistry
	clock       clockwork.Clock
}

func NewStoreFactory(t *testing.T) *StoreFactory {
	return NewStoreFactoryWithClock(t, clockwork.NewRealClock())
}

func NewStoreFactoryWithClock(t *testing.T, clock clockwork.Clock) *StoreFactory {
	rootDir := testfs.MakeTempDir(t)
	fileDir := filepath.Join(rootDir, "files")
	err := disk.EnsureDirectoryExists(fileDir)
	require.NoError(t, err)
	return &StoreFactory{
		rootDir: rootDir,
		fileDir: fileDir,
		reg:     registry.NewStaticNodeRegistry(1, nil),
		clock:   clock,
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
	nodeAddr := localAddr(t)
	gm, err := gossip.New("name-"+nodeAddr, nodeAddr, sf.gossipAddrs)
	require.NoError(t, err)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	ts := &TestingStore{
		gm:          gm,
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
	store, err := store.NewWithArgs(te, ts.RootDir, nodeHost, gm, s, reg, raftListener, apiClient, ts.GRPCAddress, partitions, db, leaser, sf.clock)
	require.NoError(t, err)
	require.NotNil(t, store)
	store.Start()
	ts.Store = store

	t.Cleanup(func() {
		ts.Stop()
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

	gm          *gossip.GossipManager
	RootDir     string
	RaftAddress string
	GRPCAddress string
	closed      bool
}

func (ts *TestingStore) DB() pebble.IPebbleDB {
	return ts.db
}

func (ts *TestingStore) NewReplica(shardID, replicaID uint64) *replica.Replica {
	sm := ts.Store.ReplicaFactoryFn(shardID, replicaID)
	return sm.(*replica.Replica)
}

func (ts *TestingStore) Stop() {
	if ts.closed {
		return
	}
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, 3*time.Second)
	defer cancelFn()
	ts.Store.Stop(ctx)
	ts.gm.Leave()
	ts.gm.Shutdown()
	ts.closed = true
}

func (sf *StoreFactory) StartShard(t *testing.T, ctx context.Context, stores ...*TestingStore) {
	require.Greater(t, len(stores), 0)
	err := bringup.SendStartShardRequests(ctx, client.NewSessionWithClock(sf.clock), stores[0].NodeHost(), stores[0].APIClient(), MakeNodeGRPCAddressesMap(stores...))
	require.NoError(t, err)
}

func (sf *StoreFactory) StartShardWithRanges(t *testing.T, ctx context.Context, startingRanges []*rfpb.RangeDescriptor, stores ...*TestingStore) {
	require.Greater(t, len(stores), 0)
	err := bringup.SendStartShardRequestsWithRanges(ctx, client.NewSessionWithClock(sf.clock), stores[0].NodeHost(), stores[0].APIClient(), MakeNodeGRPCAddressesMap(stores...), startingRanges)
	require.NoError(t, err)
}

// TestingProposer can be used in the place of NodeHost.
type TestingProposer struct {
	t   testing.TB
	id  string
	r   *replica.Replica
	idx uint64
}

func NewTestingProposer(t testing.TB, id string, r *replica.Replica) *TestingProposer {
	return &TestingProposer{
		t:  t,
		id: id,
		r:  r,
	}
}

func (tp *TestingProposer) ID() string {
	return tp.id
}

func (tp *TestingProposer) GetNoOPSession(shardID uint64) *dbcl.Session {
	return dbcl.NewNoOPSession(shardID, dbrd.LockGuardedRand)
}

func (tp *TestingProposer) makeEntry(cmd []byte) dbsm.Entry {
	tp.idx += 1
	return dbsm.Entry{Cmd: cmd, Index: tp.idx}
}

func (tp *TestingProposer) SyncPropose(ctx context.Context, session *dbcl.Session, cmd []byte) (dbsm.Result, error) {
	entries, err := tp.r.Update([]dbsm.Entry{tp.makeEntry(cmd)})
	if err != nil {
		return dbsm.Result{}, err
	}
	return entries[0].Result, nil
}

func (tp *TestingProposer) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *TestingProposer) ReadIndex(shardID uint64, timeout time.Duration) (*dragonboat.RequestState, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *TestingProposer) ReadLocalNode(rs *dragonboat.RequestState, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *TestingProposer) StaleRead(shardID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}

// FakeStore implements replica.IStore without real functionality.
type FakeStore struct{}

func (fs *FakeStore) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica)    {}
func (fs *FakeStore) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {}
func (fs *FakeStore) Sender() *sender.Sender {
	return nil
}
func (fs *FakeStore) SnapshotCluster(ctx context.Context, shardID uint64) error {
	return nil
}
func (fs *FakeStore) NHID() string {
	return ""
}

type TestingReplica struct {
	t testing.TB
	*replica.Replica
	leaser pebble.Leaser
}

func NewTestingReplica(t testing.TB, shardID, replicaID uint64) *TestingReplica {
	rootDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(rootDir, "test", &pebble.Options{})
	require.NoError(t, err)

	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})

	store := &FakeStore{}
	return &TestingReplica{
		t:       t,
		Replica: replica.New(leaser, shardID, replicaID, store, nil /*=usageUpdates=*/),
		leaser:  leaser,
	}
}

func NewTestingReplicaWithLeaser(t testing.TB, shardID, replicaID uint64, leaser pebble.Leaser) *TestingReplica {
	store := &FakeStore{}
	return &TestingReplica{
		t:       t,
		Replica: replica.New(leaser, shardID, replicaID, store, nil /*=usageUpdates=*/),
		leaser:  leaser,
	}
}

func (tr *TestingReplica) Leaser() pebble.Leaser {
	return tr.leaser
}

func (tr *TestingReplica) DB() pebble.IPebbleDB {
	db, err := tr.leaser.DB()
	require.NoError(tr.t, err)
	tr.t.Cleanup(func() {
		db.Close()
	})
	return db
}
