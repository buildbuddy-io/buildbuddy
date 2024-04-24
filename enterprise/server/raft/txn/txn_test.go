package txn_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn"
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
	dbConfig "github.com/lni/dragonboat/v4/config"
)

type fakeStore struct {
	*store.Store
	sender      *sender.Sender
	nhid        string
	db          pebble.IPebbleDB
	leaser      pebble.Leaser
	grpcAddress string
	apiClient   *client.APIClient
	reg         registry.NodeRegistry
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

type nodeRegistryFactory func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error)

func (nrf nodeRegistryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	return nrf(nhid, streamConnections, v)
}

func newGossipManager(t testing.TB, nodeAddr string) *gossip.GossipManager {
	node, err := gossip.New("name-"+nodeAddr, nodeAddr, []string{})
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Shutdown()
	})
	return node
}

func newFakeStore(t *testing.T, rootDir string) *fakeStore {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr)
	rootDir = filepath.Join(rootDir, "store-0")
	raftListener := listener.NewRaftListener()
	reg := registry.NewStaticNodeRegistry(1, nil)
	nrf := nodeRegistryFactory(func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
		return reg, nil
	})
	raftAddress := localAddr(t)
	grpcAddress := localAddr(t)
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(rootDir, "wal"),
		NodeHostDir:    filepath.Join(rootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    raftAddress,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: nrf,
		},
		AddressByNodeHostID: false,
		RaftEventListener:   raftListener,
		SystemEventListener: raftListener,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	require.NoError(t, err)
	te := testenv.GetTestEnv(t)
	apiClient := client.NewAPIClient(te, nodeHost.ID())
	rc := rangecache.New()
	fs := &fakeStore{
		sender:      sender.New(rc, reg, apiClient),
		nhid:        nodeHost.ID(),
		grpcAddress: grpcAddress,
		apiClient:   apiClient,
		reg:         reg,
	}
	reg.AddNode(nodeHost.ID(), raftAddress, grpcAddress)
	db, err := pebble.Open(rootDir, "raft_store", &pebble.Options{})
	require.NoError(t, err)
	fs.db = db
	fs.leaser = pebble.NewDBLeaser(db)
	partitions := []disk.Partition{
		{
			ID:           "default",
			MaxSizeBytes: int64(1_000_000_000), // 1G
		},
	}
	s, err := store.NewWithArgs(te, rootDir, nodeHost, gm, fs.sender, reg, raftListener, apiClient, grpcAddress, partitions, db, fs.leaser)
	require.NoError(t, err)
	fs.Store = s
	t.Cleanup(func() {
		s.Stop(context.TODO())
	})
	err = bringup.SendStartShardRequests(context.TODO(), nodeHost, apiClient, map[string]string{
		fs.nhid: grpcAddress,
	})
	require.NoError(t, err)
	return fs
}

func (fs *fakeStore) Sender() *sender.Sender {
	return fs.sender
}

func (fs *fakeStore) prepareTransaction(t *testing.T, txnID []byte, statement *rfpb.TxnRequest_Statement) {
	repl1, err := fs.Store.GetReplica(statement.GetReplica().GetShardId())
	require.NoError(t, err)
	wb := fs.db.NewBatch()
	_, err = repl1.PrepareTransaction(wb, txnID, statement.GetRawBatch())
	require.NoError(t, err)
	require.NoError(t, wb.Commit(pebble.Sync))
	require.NoError(t, wb.Close())
}

func TestRollbackPendingTxn(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := newFakeStore(t, rootDir)
	ctx := context.Background()

	{ // Do a DirectWrite.
		key := []byte("foo")
		writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   key,
				Value: []byte("bar"),
			},
		}).ToProto()
		require.NoError(t, err)
		writeRsp, err := store.Sender().SyncPropose(ctx, key, writeReq)
		require.NoError(t, err)
		err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
		require.NoError(t, err)
	}

	batch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo"),
		},
	})
	txnProto, err := rbuilder.NewTxn().AddStatement(&rfpb.ReplicaDescriptor{
		ShardId:   2,
		ReplicaId: 2,
	}, batch).ToProto()
	require.NoError(t, err)
	store.prepareTransaction(t, txnProto.GetTransactionId(), txnProto.GetStatements()[0])

	clock := clockwork.NewFakeClock()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}

	tc := txn.NewCoordinator(store, store.reg, store.apiClient, clock)
	err = tc.WriteTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	{ // Do a DirectRead and verify the txn record exists
		key := keys.MakeKey(constants.TxnRecordPrefix, txnProto.GetTransactionId())
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: key,
		}).ToProto()
		require.NoError(t, err)
		readRsp, err := store.Sender().SyncRead(ctx, key, readReq)
		require.NoError(t, err)
		readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
		require.NoError(t, readBatch.AnyError())
	}

	err = tc.ProcessTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	{ // Do a DirectRead and verify the txn record doesn't exist
		key := keys.MakeKey(constants.TxnRecordPrefix, txnProto.GetTransactionId())
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: key,
		}).ToProto()
		require.NoError(t, err)
		readRsp, err := store.Sender().SyncRead(ctx, key, readReq)
		require.NoError(t, err)
		readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
		_, err = readBatch.DirectReadResponse(0)
		require.True(t, status.IsNotFoundError(err))
	}

	{ // Do a DirectRead and verify the value is rolled back.
		key := []byte("foo")
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: key,
		}).ToProto()
		require.NoError(t, err)
		readRsp, err := store.Sender().SyncRead(ctx, key, readReq)
		require.NoError(t, err)
		readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
		require.NoError(t, readBatch.AnyError())
		directRead, err := readBatch.DirectReadResponse(0)
		require.NoError(t, err)
		require.Equal(t, []byte("bar"), directRead.GetKv().GetValue())
	}

	{ // Do a DirectWrite, and it should succeed since the txn is rolledback.
		key := []byte("foo")
		writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   key,
				Value: []byte("bar2"),
			},
		}).ToProto()
		require.NoError(t, err)
		writeRsp, err := store.Sender().SyncPropose(ctx, key, writeReq)
		require.NoError(t, err)
		err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
		require.NoError(t, err)
	}
}

func TestCommitPreparedTxn(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := newFakeStore(t, rootDir)
	ctx := context.Background()

	{ // Do a DirectWrite.
		key := []byte("foo")
		writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   key,
				Value: []byte("bar"),
			},
		}).ToProto()
		require.NoError(t, err)
		writeRsp, err := store.Sender().SyncPropose(ctx, key, writeReq)
		require.NoError(t, err)
		err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
		require.NoError(t, err)
	}

	batch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo"),
		},
	})
	txnProto, err := rbuilder.NewTxn().AddStatement(&rfpb.ReplicaDescriptor{
		ShardId:   2,
		ReplicaId: 2,
	}, batch).ToProto()
	require.NoError(t, err)
	store.prepareTransaction(t, txnProto.GetTransactionId(), txnProto.GetStatements()[0])

	clock := clockwork.NewFakeClock()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PREPARED,
		Op:            rfpb.FinalizeOperation_COMMIT,
		CreatedAtUsec: clock.Now().UnixMicro(),
		Prepared: []*rfpb.ReplicaDescriptor{
			txnProto.GetStatements()[0].GetReplica(),
		},
	}
	tc := txn.NewCoordinator(store, store.reg, store.apiClient, clock)

	err = tc.WriteTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	{ // Do a DirectRead and verify the txn record exists
		key := keys.MakeKey(constants.TxnRecordPrefix, txnProto.GetTransactionId())
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: key,
		}).ToProto()
		require.NoError(t, err)
		readRsp, err := store.Sender().SyncRead(ctx, key, readReq)
		require.NoError(t, err)
		readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
		require.NoError(t, readBatch.AnyError())
	}

	err = tc.ProcessTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	{ // Do a DirectRead and verify the txn record doesn't exist
		key := keys.MakeKey(constants.TxnRecordPrefix, txnProto.GetTransactionId())
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: key,
		}).ToProto()
		require.NoError(t, err)
		readRsp, err := store.Sender().SyncRead(ctx, key, readReq)
		require.NoError(t, err)
		readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
		_, err = readBatch.DirectReadResponse(0)
		require.True(t, status.IsNotFoundError(err))
	}

	{ // Do a DirectRead and verify the value is updated.
		key := []byte("foo")
		readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
			Key: key,
		}).ToProto()
		require.NoError(t, err)
		readRsp, err := store.Sender().SyncRead(ctx, key, readReq)
		require.NoError(t, err)
		readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
		require.NoError(t, readBatch.AnyError())
		directRead, err := readBatch.DirectReadResponse(0)
		require.NoError(t, err)
		require.Equal(t, []byte("zoo"), directRead.GetKv().GetValue())
	}

	{ // Do a DirectWrite, and it should succeed since the txn is committed.
		key := []byte("foo")
		writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   key,
				Value: []byte("bar2"),
			},
		}).ToProto()
		require.NoError(t, err)
		writeRsp, err := store.Sender().SyncPropose(ctx, key, writeReq)
		require.NoError(t, err)
		err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
		require.NoError(t, err)
	}
}

func TestFetchTxnRecordsSkipRecent(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := newFakeStore(t, rootDir)
	ctx := context.Background()
	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(store, store.reg, store.apiClient, clock)

	err := tc.WriteTxnRecord(ctx, &rfpb.TxnRecord{
		TxnRequest:    &rfpb.TxnRequest{TransactionId: []byte("a")},
		CreatedAtUsec: clock.Now().Add(-15 * time.Second).UnixMicro(),
	})
	require.NoError(t, err)
	err = tc.WriteTxnRecord(ctx, &rfpb.TxnRecord{
		TxnRequest:    &rfpb.TxnRequest{TransactionId: []byte("b")},
		CreatedAtUsec: clock.Now().Add(-10 * time.Second).UnixMicro(),
	})
	require.NoError(t, err)
	err = tc.WriteTxnRecord(ctx, &rfpb.TxnRecord{
		TxnRequest:    &rfpb.TxnRequest{TransactionId: []byte("c")},
		CreatedAtUsec: clock.Now().Add(-5 * time.Second).UnixMicro(),
	})
	require.NoError(t, err)
	err = tc.WriteTxnRecord(ctx, &rfpb.TxnRecord{
		TxnRequest:    &rfpb.TxnRequest{TransactionId: []byte("d")},
		CreatedAtUsec: clock.Now().UnixMicro(),
	})
	require.NoError(t, err)

	clock.Advance(6 * time.Second)
	txnRecords, err := tc.FetchTxnRecords(ctx)
	gotTxnIDs := make([][]byte, 0, 3)
	for _, txnRecord := range txnRecords {
		gotTxnIDs = append(gotTxnIDs, txnRecord.GetTxnRequest().GetTransactionId())
	}
	require.ElementsMatch(t, gotTxnIDs, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	require.NoError(t, err)
}
