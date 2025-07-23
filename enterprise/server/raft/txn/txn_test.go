package txn_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

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
	testutil.WaitForRangeLease(t, ctx, []*testutil.TestingStore{store}, 2)
	rd := store.GetRange(2)
	tb := rbuilder.NewTxn()
	stmt := tb.AddStatement()
	stmt.SetRangeDescriptor(rd).SetBatch(batch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(store, store.APIClient(), clock)
	tc.PrepareStatement(ctx, txnProto.GetTransactionId(), txnProto.GetStatements()[0])

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}

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
	testutil.WaitForRangeLease(t, ctx, []*testutil.TestingStore{store}, 2)
	rd := store.GetRange(2)
	tb := rbuilder.NewTxn()
	stmt := tb.AddStatement()
	stmt.SetRangeDescriptor(rd).SetBatch(batch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(store, store.APIClient(), clock)
	tc.PrepareStatement(ctx, txnProto.GetTransactionId(), txnProto.GetStatements()[0])

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PREPARED,
		Op:            rfpb.FinalizeOperation_COMMIT,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}

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

func TestRecoverTxnToUpdateRangeDescriptor(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	store := sf.NewStore(t)
	ctx := context.Background()
	sf.StartShard(t, ctx, store)

	testutil.WaitForRangeLease(t, ctx, []*testutil.TestingStore{store}, 2)
	oldRD := store.GetRange(2)
	newRD := oldRD.CloneVT()
	newRD.Generation++
	txnBuilder, err := store.GetTxnForRangeDescriptorUpdate(ctx, 2, oldRD, newRD)
	require.NoError(t, err)
	txnProto, err := txnBuilder.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(store, store.APIClient(), clock)
	for _, stmt := range txnProto.GetStatements() {
		tc.PrepareStatement(ctx, txnProto.GetTransactionId(), stmt)
	}

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PREPARED,
		Op:            rfpb.FinalizeOperation_COMMIT,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}

	err = tc.WriteTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	repl2, err := store.GetReplica(2)
	require.NoError(t, err)

	// commit the transaction statement on range 2
	err = repl2.CommitTransaction(txnProto.GetTransactionId())
	require.NoError(t, err)

	// Tries to finalize the txn again.
	err = tc.ProcessTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	verifyTxnRecordNotExist(t, ctx, store.Sender(), txnProto.GetTransactionId())

	verifyRangeDescriptorInMetaRangeEquals(t, ctx, store.Sender(), newRD)

	{ // Verify that local range descriptor is updated.
		rd := store.GetRange(2)
		require.True(t, proto.Equal(newRD, rd))
	}
}

func verifyTxnRecordNotExist(t *testing.T, ctx context.Context, s *sender.Sender, txid []byte) {
	key := keys.MakeKey(constants.TxnRecordPrefix, txid)
	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: key,
	}).ToProto()
	require.NoError(t, err)
	readRsp, err := s.SyncRead(ctx, key, readReq, sender.WithConsistencyMode(rfpb.Header_LINEARIZABLE))
	require.NoError(t, err)
	readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
	_, err = readBatch.DirectReadResponse(0)
	require.True(t, status.IsNotFoundError(err))
}

func verifyRangeDescriptorInMetaRangeEquals(t *testing.T, ctx context.Context, s *sender.Sender, expectedRD *rfpb.RangeDescriptor) {
	key := keys.RangeMetaKey(expectedRD.GetEnd())
	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: key,
	}).ToProto()
	require.NoError(t, err)
	readRsp, err := s.SyncRead(ctx, key, readReq)
	require.NoError(t, err)
	readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
	require.NoError(t, readBatch.AnyError())
	directRead, err := readBatch.DirectReadResponse(0)
	require.NoError(t, err)
	gotRD := &rfpb.RangeDescriptor{}
	err = proto.Unmarshal(directRead.GetKv().GetValue(), gotRD)
	require.NoError(t, err)
	require.True(t, proto.Equal(expectedRD, gotRD))
}

func TestRecoverSplitTxn(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()

	sf.StartShard(t, ctx, stores...)
	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := s.GetRange(2)

	splitKey := []byte("a")
	splitRangeTxn, err := s.BuildTxnForRangeSplit(rd, 3, splitKey, []uint64{1, 2, 3})
	require.NoError(t, err)
	tb := splitRangeTxn.TxnBuilder

	txnProto, err := tb.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(s, s.APIClient(), clock)
	for _, stmt := range txnProto.GetStatements() {
		tc.PrepareStatement(ctx, txnProto.GetTransactionId(), stmt)
	}

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PREPARED,
		Op:            rfpb.FinalizeOperation_COMMIT,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}

	err = tc.WriteTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	repl2, err := s.GetReplica(2)
	require.NoError(t, err)

	// commit the transaction statement on range 2
	err = repl2.CommitTransaction(txnProto.GetTransactionId())
	require.NoError(t, err)

	// Tries to finalize the txn again.
	err = tc.ProcessTxnRecord(ctx, txnRecord)
	require.NoError(t, err)

	verifyTxnRecordNotExist(t, ctx, s.Sender(), txnProto.GetTransactionId())
	verifyRangeDescriptorInMetaRangeEquals(t, ctx, s.Sender(), splitRangeTxn.UpdatedLeftRange)
	verifyRangeDescriptorInMetaRangeEquals(t, ctx, s.Sender(), splitRangeTxn.NewRightRange)

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd = s.GetRange(2)
	require.True(t, proto.Equal(splitRangeTxn.UpdatedLeftRange, rd))

	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 3)
	rd = s.GetRange(3)
	require.True(t, proto.Equal(splitRangeTxn.NewRightRange, rd))
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
	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

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
	s := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	rd1 := s.GetRange(1)
	s = testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd2 := s.GetRange(2)
	tb := rbuilder.NewTxn()
	tb.AddStatement().SetRangeDescriptor(rd2).SetBatch(batch)
	tb.AddStatement().SetRangeDescriptor(rd1).SetBatch(metaBatch)
	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(s1, s1.APIClient(), clock)
	err := tc.RunTxn(ctx, tb)
	require.NoError(t, err)
}
