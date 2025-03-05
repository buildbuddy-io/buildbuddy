package testutil

import (
	"context"
	"fmt"
	"log"
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

func (sf *StoreFactory) RecreateStore(t *testing.T, ts *TestingStore) {
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
		DefaultNodeRegistryEnabled: false,
		RaftEventListener:          raftListener,
		SystemEventListener:        raftListener,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	require.NoError(t, err, "unexpected error creating NodeHost")

	te := testenv.GetTestEnv(t)
	te.SetClock(sf.clock)
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
	mc := &pebble.MetricsCollector{}
	db, err := pebble.Open(ts.RootDir, "raft_store", &pebble.Options{
		EventListener: &pebble.EventListener{
			WriteStallBegin: mc.WriteStallBegin,
			WriteStallEnd:   mc.WriteStallEnd,
			DiskSlow:        mc.DiskSlow,
		},
	})
	require.NoError(t, err)
	leaser := pebble.NewDBLeaser(db)
	ts.leaser = leaser
	store, err := store.NewWithArgs(te, ts.RootDir, nodeHost, ts.gm, s, reg, raftListener, apiClient, ts.GRPCAddress, ts.GRPCAddress, partitions, db, leaser, mc)
	require.NoError(t, err)
	require.NotNil(t, store)
	store.Start()
	ts.Store = store

	t.Cleanup(func() {
		ts.Stop()
	})
}

func (sf *StoreFactory) NewStore(t *testing.T) *TestingStore {
	nodeAddr := localAddr(t)
	gm, err := gossip.New("name-"+nodeAddr, nodeAddr, sf.gossipAddrs)
	require.NoError(t, err)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	ts := &TestingStore{
		t:           t,
		gm:          gm,
		RaftAddress: localAddr(t),
		GRPCAddress: localAddr(t),
		RootDir:     filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs))),
	}
	sf.RecreateStore(t, ts)
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
	t testing.TB
	*store.Store

	leaser pebble.Leaser

	gm          *gossip.GossipManager
	RootDir     string
	RaftAddress string
	GRPCAddress string
	closed      bool
}

func (ts *TestingStore) DB() pebble.IPebbleDB {
	db, err := ts.leaser.DB()
	require.NoError(ts.t, err)
	ts.t.Cleanup(func() {
		db.Close()
	})
	return db
}

func (ts *TestingStore) NewReplica(rangeID, replicaID uint64) *replica.Replica {
	sm := ts.Store.ReplicaFactoryFn(rangeID, replicaID)
	return sm.(*replica.Replica)
}

func (ts *TestingStore) Stop() {
	if ts.closed {
		return
	}
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, 3*time.Second)
	defer cancelFn()
	require.NoError(ts.t, ts.Store.Stop(ctx))
	require.NoError(ts.t, ts.gm.Leave())
	require.NoError(ts.t, ts.gm.Shutdown())
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

func GetStoreWithRangeLease(t testing.TB, ctx context.Context, stores []*TestingStore, rangeID uint64) *TestingStore {
	t.Helper()

	start := time.Now()
	for {
		for _, store := range stores {
			if store.HaveLease(ctx, rangeID) {
				return store
			}
		}
		time.Sleep(10 * time.Millisecond)
		if time.Since(start) > 60*time.Second {
			break
		}
	}

	require.Failf(t, "getStoreWithRangeLease failed", "No store found holding rangelease for range: %d", rangeID)
	return nil
}

func WaitForRangeLease(t testing.TB, ctx context.Context, stores []*TestingStore, rangeID uint64) {
	t.Helper()
	s := GetStoreWithRangeLease(t, ctx, stores, rangeID)
	log.Printf("%s got range lease for range: %d", s.NHID(), rangeID)
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

func (tp *TestingProposer) GetNoOPSession(rangeID uint64) *dbcl.Session {
	return dbcl.NewNoOPSession(rangeID, dbrd.LockGuardedRand)
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

func (tp *TestingProposer) SyncRead(ctx context.Context, rangeID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *TestingProposer) ReadIndex(rangeID uint64, timeout time.Duration) (*dragonboat.RequestState, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *TestingProposer) ReadLocalNode(rs *dragonboat.RequestState, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *TestingProposer) StaleRead(rangeID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}

// FakeStore implements replica.IStore without real functionality.
type FakeStore struct{}

func (fs *FakeStore) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica)    {}
func (fs *FakeStore) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {}
func (fs *FakeStore) Sender() *sender.Sender {
	return nil
}
func (fs *FakeStore) SnapshotCluster(ctx context.Context, rangeID uint64) error {
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

func NewTestingReplica(t testing.TB, rangeID, replicaID uint64) *TestingReplica {
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
		Replica: replica.New(leaser, rangeID, replicaID, store, nil /*=usageUpdates=*/),
		leaser:  leaser,
	}
}

func NewTestingReplicaWithLeaser(t testing.TB, rangeID, replicaID uint64, leaser pebble.Leaser) *TestingReplica {
	store := &FakeStore{}
	return &TestingReplica{
		t:       t,
		Replica: replica.New(leaser, rangeID, replicaID, store, nil /*=usageUpdates=*/),
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
