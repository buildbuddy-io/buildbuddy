package txn_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

func prepareTransaction(t *testing.T, ts *testutil.TestingStore, txnID []byte, statement *rfpb.TxnRequest_Statement) {
	repl1, err := ts.Store.GetReplica(statement.GetRange().GetRangeId())
	require.NoError(t, err)
	wb := ts.DB().NewBatch()
	_, err = repl1.PrepareTransaction(wb, txnID, statement.GetRawBatch())
	require.NoError(t, err)
	require.NoError(t, wb.Commit(pebble.Sync))
	require.NoError(t, wb.Close())
}

func TestRollbackPendingTxn(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	store := sf.NewStore(t)
	ctx := context.Background()
	sf.StartShard(t, ctx, store)

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
	rd := store.GetRange(2)
	txnProto, err := rbuilder.NewTxn().AddStatement(rd, batch).ToProto()
	require.NoError(t, err)
	prepareTransaction(t, store, txnProto.GetTransactionId(), txnProto.GetStatements()[0])

	clock := clockwork.NewFakeClock()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}

	tc := txn.NewCoordinator(store, store.APIClient(), clock)
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
	sf := testutil.NewStoreFactory(t)
	store := sf.NewStore(t)
	ctx := context.Background()
	sf.StartShard(t, ctx, store)

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
	rd := store.GetRange(2)
	txnProto, err := rbuilder.NewTxn().AddStatement(rd, batch).ToProto()
	require.NoError(t, err)
	prepareTransaction(t, store, txnProto.GetTransactionId(), txnProto.GetStatements()[0])

	clock := clockwork.NewFakeClock()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PREPARED,
		Op:            rfpb.FinalizeOperation_COMMIT,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}
	tc := txn.NewCoordinator(store, store.APIClient(), clock)

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
	sf := testutil.NewStoreFactory(t)
	clock := clockwork.NewFakeClock()
	store := sf.NewStore(t)
	ctx := context.Background()
	sf.StartShard(t, ctx, store)
	tc := txn.NewCoordinator(store, store.APIClient(), clock)

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

func TestRunTxn(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()
	sf.StartShard(t, ctx, s1, s2, s3)

	batch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo"),
		},
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("bar"),
			Value: []byte("foo"),
		},
	})

	metaBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.MakeKey(constants.SystemPrefix, []byte("aaa")),
			Value: []byte("bbb"),
		},
	})
	rd1 := s1.GetRange(1)
	rd2 := s1.GetRange(2)
	txnBuilder := rbuilder.NewTxn().AddStatement(rd2, batch).AddStatement(rd1, metaBatch)
	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(s1, s1.APIClient(), clock)
	err := tc.RunTxn(ctx, txnBuilder)
	require.NoError(t, err)
}
