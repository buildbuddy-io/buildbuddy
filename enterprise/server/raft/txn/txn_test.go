package txn_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	gstatus "google.golang.org/grpc/status"
)

func TestCommitPreparedTxn(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	store := sf.NewStore(t, testutil.StoreOptions{})
	ctx := context.Background()
	sf.StartShard(t, ctx, store)

	{ // Do a DirectWrite.
		key := []byte("PTdefault/f11")
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
			Key:   []byte("PTdefault/f11"),
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

	err = tc.RecoverTxnRecordForTest(ctx, txnRecord)
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
		key := []byte("PTdefault/f11")
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
		key := []byte("PTdefault/f11")
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
	store := sf.NewStore(t, testutil.StoreOptions{})
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
	err = tc.RecoverTxnRecordForTest(ctx, txnRecord)
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

func TestFetchTxnRecords(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	clock := clockwork.NewFakeClock()
	store := sf.NewStore(t, testutil.StoreOptions{})
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
	{
		txnRecords, err := tc.FetchTxnRecords(ctx, false /*includeLive*/)
		gotTxnIDs := make([][]byte, 0, 3)
		for _, txnRecord := range txnRecords {
			gotTxnIDs = append(gotTxnIDs, txnRecord.GetTxnRequest().GetTransactionId())
		}
		require.ElementsMatch(t, gotTxnIDs, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
		require.NoError(t, err)
	}
	{
		txnRecords, err := tc.FetchTxnRecords(ctx, true /*includeLive*/)
		gotTxnIDs := make([][]byte, 0, 3)
		for _, txnRecord := range txnRecords {
			gotTxnIDs = append(gotTxnIDs, txnRecord.GetTxnRequest().GetTransactionId())
		}
		require.ElementsMatch(t, gotTxnIDs, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})
		require.NoError(t, err)
	}
}

func TestRunTxn(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t, testutil.StoreOptions{})
	s2 := sf.NewStore(t, testutil.StoreOptions{})
	s3 := sf.NewStore(t, testutil.StoreOptions{})
	ctx := context.Background()
	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	batch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("PTdefault/f11"),
			Value: []byte("zoo"),
		},
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("PTdefault/bar"),
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

type faultTiming int

const (
	faultBeforeApply faultTiming = iota
	faultAfterApply
)

type txnFaultRule struct {
	rangeID   uint64
	op        rfpb.FinalizeOperation
	timing    faultTiming
	remaining int
}

type txnFaultHarness struct {
	mu       sync.Mutex
	txnID    []byte
	rules    []txnFaultRule
	injected int
}

func newTxnFaultHarness(rules ...txnFaultRule) *txnFaultHarness {
	return &txnFaultHarness{rules: rules}
}

// assertFired asserts that every configured rule fired its expected number of
// times (remaining hit zero) and that at least one fault was injected. This
// guards against tests passing trivially when the harness fails to match the
// in-flight request (e.g. txn-id mismatch, route bypassing the interceptor).
func (h *txnFaultHarness) assertFired(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.Greater(t, h.injected, 0, "no faults were injected; harness never matched a request")
	for i, rule := range h.rules {
		require.Zerof(t, rule.remaining, "rule %d did not fully fire (remaining=%d): rangeID=%d op=%s timing=%v", i, rule.remaining, rule.rangeID, rule.op, rule.timing)
	}
}

func (h *txnFaultHarness) setTxnID(txnID []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.txnID = append([]byte(nil), txnID...)
}

func (h *txnFaultHarness) match(req *rfpb.SyncProposeRequest) *txnFaultRule {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.txnID) == 0 || !bytes.Equal(h.txnID, req.GetBatch().GetTransactionId()) {
		return nil
	}
	rangeID := req.GetHeader().GetRangeId()
	if replica := req.GetHeader().GetReplica(); replica != nil {
		rangeID = replica.GetRangeId()
	}
	for i := range h.rules {
		rule := &h.rules[i]
		if rule.rangeID != rangeID || rule.op != req.GetBatch().GetFinalizeOperation() {
			continue
		}
		if rule.remaining == 0 {
			continue
		}
		if rule.remaining > 0 {
			rule.remaining--
		}
		h.injected++
		return rule
	}
	return nil
}

func (h *txnFaultHarness) interceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	syncReq, ok := req.(*rfpb.SyncProposeRequest)
	if !ok || !strings.HasSuffix(info.FullMethod, "/SyncPropose") {
		return handler(ctx, req)
	}
	rule := h.match(syncReq)
	if rule == nil {
		return handler(ctx, req)
	}
	injectedErr := gstatus.Error(codes.Unavailable, fmt.Sprintf(
		"injected %s fault for txn op=%s range=%d",
		map[faultTiming]string{
			faultBeforeApply: "before-apply",
			faultAfterApply:  "after-apply",
		}[rule.timing],
		rule.op.String(),
		rule.rangeID,
	))
	if rule.timing == faultBeforeApply {
		return nil, injectedErr
	}
	rsp, err := handler(ctx, req)
	if err == nil {
		return nil, injectedErr
	}
	return rsp, err
}

func newFaultInjectedStoreFactory(t *testing.T, harness *txnFaultHarness) (*testutil.StoreFactory, grpc_server.GRPCServerConfig) {
	t.Helper()
	return testutil.NewStoreFactory(t), grpc_server.GRPCServerConfig{
		ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{harness.interceptor},
	}
}

func setupSplitTxnForRecovery(
	t *testing.T,
	harness *txnFaultHarness,
) (
	context.Context,
	[]*testutil.TestingStore,
	*testutil.TestingStore,
	*rfpb.RangeDescriptor,
	*rfpb.RangeDescriptor,
	*rfpb.TxnRequest,
	*txn.Coordinator,
	*rfpb.TxnRecord,
) {
	t.Helper()

	sf, grpcConfig := newFaultInjectedStoreFactory(t, harness)
	s1 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	s2 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	s3 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()

	sf.StartShard(t, ctx, stores...)
	store := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	rd := store.GetRange(2)

	splitRangeTxn, err := store.BuildTxnForRangeSplit(rd, 3, []byte("a"), []uint64{1, 2, 3})
	require.NoError(t, err)
	txnProto, err := splitRangeTxn.TxnBuilder.ToProto()
	require.NoError(t, err)
	harness.setTxnID(txnProto.GetTransactionId())

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(store, store.APIClient(), clock)
	for _, stmt := range txnProto.GetStatements() {
		require.NoError(t, tc.PrepareStatement(ctx, txnProto.GetTransactionId(), stmt))
	}

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PREPARED,
		Op:            rfpb.FinalizeOperation_COMMIT,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}
	require.NoError(t, tc.WriteTxnRecord(ctx, txnRecord))

	return ctx, stores, store, splitRangeTxn.UpdatedLeftRange, splitRangeTxn.NewRightRange, txnProto, tc, txnRecord
}

func setupPendingRollbackTxnForRecovery(
	t *testing.T,
	harness *txnFaultHarness,
) (
	context.Context,
	*testutil.TestingStore,
	*txn.Coordinator,
	*rfpb.TxnRequest,
	*rfpb.TxnRecord,
	[]byte,
	[]byte,
) {
	t.Helper()

	sf, grpcConfig := newFaultInjectedStoreFactory(t, harness)
	s1 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	s2 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	s3 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()
	sf.StartShard(t, ctx, stores...)

	userKey := []byte("PTdefault/f11")
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("before"),
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, userKey, writeReq)
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponseFromProto(writeRsp).AnyError())

	metaKey := keys.MakeKey(constants.SystemPrefix, []byte("rollback-cas"))
	metaStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	metaRD := metaStore.GetRange(1)
	dataStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	dataRD := dataStore.GetRange(2)

	dataBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("after"),
		},
	})
	metaBatch := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaKey,
			Value: []byte("new"),
		},
		ExpectedValue: []byte("missing"),
	})

	tb := rbuilder.NewTxn()
	tb.AddStatement().SetRangeDescriptor(dataRD).SetBatch(dataBatch)
	tb.AddStatement().SetRangeDescriptor(metaRD).SetBatch(metaBatch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)
	harness.setTxnID(txnProto.GetTransactionId())

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(s1, s1.APIClient(), clock)
	require.NoError(t, tc.PrepareStatement(ctx, txnProto.GetTransactionId(), txnProto.GetStatements()[0]))

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}
	require.NoError(t, tc.WriteTxnRecord(ctx, txnRecord))

	return ctx, s1, tc, txnProto, txnRecord, userKey, metaKey
}

func verifyDirectReadValue(t *testing.T, ctx context.Context, s *sender.Sender, key, value []byte) {
	t.Helper()
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
	require.Equal(t, value, directRead.GetKv().GetValue())
}

func verifyDirectReadNotFound(t *testing.T, ctx context.Context, s *sender.Sender, key []byte) {
	t.Helper()
	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: key,
	}).ToProto()
	require.NoError(t, err)
	readRsp, err := s.SyncRead(ctx, key, readReq)
	require.NoError(t, err)
	readBatch := rbuilder.NewBatchResponseFromProto(readRsp)
	_, err = readBatch.DirectReadResponse(0)
	require.True(t, status.IsNotFoundError(err))
}

func TestSplitTxnRetryMatrix(t *testing.T) {
	testCases := []struct {
		name    string
		rangeID uint64
		op      rfpb.FinalizeOperation
		timing  faultTiming
	}{
		{name: "prepare_left_before_apply", rangeID: 2, op: rfpb.FinalizeOperation_UNKNOWN_OPERATION, timing: faultBeforeApply},
		{name: "prepare_left_after_apply", rangeID: 2, op: rfpb.FinalizeOperation_UNKNOWN_OPERATION, timing: faultAfterApply},
		{name: "prepare_right_before_apply", rangeID: 3, op: rfpb.FinalizeOperation_UNKNOWN_OPERATION, timing: faultBeforeApply},
		{name: "prepare_right_after_apply", rangeID: 3, op: rfpb.FinalizeOperation_UNKNOWN_OPERATION, timing: faultAfterApply},
		{name: "prepare_meta_before_apply", rangeID: 1, op: rfpb.FinalizeOperation_UNKNOWN_OPERATION, timing: faultBeforeApply},
		{name: "prepare_meta_after_apply", rangeID: 1, op: rfpb.FinalizeOperation_UNKNOWN_OPERATION, timing: faultAfterApply},
		{name: "commit_left_before_apply", rangeID: 2, op: rfpb.FinalizeOperation_COMMIT, timing: faultBeforeApply},
		{name: "commit_left_after_apply", rangeID: 2, op: rfpb.FinalizeOperation_COMMIT, timing: faultAfterApply},
		{name: "commit_right_before_apply", rangeID: 3, op: rfpb.FinalizeOperation_COMMIT, timing: faultBeforeApply},
		{name: "commit_right_after_apply", rangeID: 3, op: rfpb.FinalizeOperation_COMMIT, timing: faultAfterApply},
		{name: "commit_meta_before_apply", rangeID: 1, op: rfpb.FinalizeOperation_COMMIT, timing: faultBeforeApply},
		{name: "commit_meta_after_apply", rangeID: 1, op: rfpb.FinalizeOperation_COMMIT, timing: faultAfterApply},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newTxnFaultHarness(txnFaultRule{
				rangeID:   tc.rangeID,
				op:        tc.op,
				timing:    tc.timing,
				remaining: 1,
			})
			sf, grpcConfig := newFaultInjectedStoreFactory(t, harness)
			s1 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
			s2 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
			s3 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
			stores := []*testutil.TestingStore{s1, s2, s3}
			ctx := context.Background()

			sf.StartShard(t, ctx, stores...)
			s := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
			rd := s.GetRange(2)

			splitRangeTxn, err := s.BuildTxnForRangeSplit(rd, 3, []byte("a"), []uint64{1, 2, 3})
			require.NoError(t, err)
			txnProto, err := splitRangeTxn.TxnBuilder.ToProto()
			require.NoError(t, err)
			harness.setTxnID(txnProto.GetTransactionId())

			tc := txn.NewCoordinator(s, s.APIClient(), clockwork.NewFakeClock())
			require.NoError(t, tc.RunTxnWithProto(ctx, txnProto))

			verifyTxnRecordNotExist(t, ctx, s.Sender(), txnProto.GetTransactionId())
			verifyRangeDescriptorInMetaRangeEquals(t, ctx, s.Sender(), splitRangeTxn.UpdatedLeftRange)
			verifyRangeDescriptorInMetaRangeEquals(t, ctx, s.Sender(), splitRangeTxn.NewRightRange)

			leftStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
			require.True(t, proto.Equal(splitRangeTxn.UpdatedLeftRange, leftStore.GetRange(2)))
			rightStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 3)
			require.True(t, proto.Equal(splitRangeTxn.NewRightRange, rightStore.GetRange(3)))
			harness.assertFired(t)
		})
	}
}

func TestTxnRollbackRetryIsIdempotent(t *testing.T) {
	harness := newTxnFaultHarness(txnFaultRule{
		rangeID:   2,
		op:        rfpb.FinalizeOperation_ROLLBACK,
		timing:    faultAfterApply,
		remaining: 1,
	})
	sf, grpcConfig := newFaultInjectedStoreFactory(t, harness)
	s1 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	s2 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	s3 := sf.NewStoreWithGRPCServerConfig(t, grpcConfig)
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()

	sf.StartShard(t, ctx, stores...)

	userKey := []byte("PTdefault/f11")
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("before"),
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, userKey, writeReq)
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponseFromProto(writeRsp).AnyError())

	metaKey := keys.MakeKey(constants.SystemPrefix, []byte("rollback-cas"))
	metaStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	metaRD := metaStore.GetRange(1)
	dataStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	dataRD := dataStore.GetRange(2)

	dataBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("after"),
		},
	})
	metaBatch := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaKey,
			Value: []byte("new"),
		},
		ExpectedValue: []byte("missing"),
	})

	tb := rbuilder.NewTxn()
	tb.AddStatement().SetRangeDescriptor(dataRD).SetBatch(dataBatch)
	tb.AddStatement().SetRangeDescriptor(metaRD).SetBatch(metaBatch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)
	harness.setTxnID(txnProto.GetTransactionId())

	tc := txn.NewCoordinator(s1, s1.APIClient(), clockwork.NewFakeClock())
	err = tc.RunTxnWithProto(ctx, txnProto)
	require.Error(t, err)

	verifyTxnRecordNotExist(t, ctx, s1.Sender(), txnProto.GetTransactionId())
	verifyDirectReadValue(t, ctx, s1.Sender(), userKey, []byte("before"))
	verifyDirectReadNotFound(t, ctx, s1.Sender(), metaKey)
	harness.assertFired(t)
}

func TestRecoverSplitTxnRetryMatrix(t *testing.T) {
	testCases := []struct {
		name    string
		rangeID uint64
		timing  faultTiming
	}{
		{name: "commit_right_before_apply", rangeID: 3, timing: faultBeforeApply},
		{name: "commit_right_after_apply", rangeID: 3, timing: faultAfterApply},
		{name: "commit_meta_before_apply", rangeID: 1, timing: faultBeforeApply},
		{name: "commit_meta_after_apply", rangeID: 1, timing: faultAfterApply},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newTxnFaultHarness(txnFaultRule{
				rangeID:   tc.rangeID,
				op:        rfpb.FinalizeOperation_COMMIT,
				timing:    tc.timing,
				remaining: 1,
			})
			ctx, stores, store, updatedLeftRange, newRightRange, txnProto, coordinator, txnRecord := setupSplitTxnForRecovery(t, harness)

			repl, err := store.GetReplica(2)
			require.NoError(t, err)
			require.NoError(t, repl.CommitTransaction(txnProto.GetTransactionId()))

			require.NoError(t, coordinator.RecoverTxnRecordForTest(ctx, txnRecord))

			verifyTxnRecordNotExist(t, ctx, store.Sender(), txnProto.GetTransactionId())
			verifyRangeDescriptorInMetaRangeEquals(t, ctx, store.Sender(), updatedLeftRange)
			verifyRangeDescriptorInMetaRangeEquals(t, ctx, store.Sender(), newRightRange)

			leftStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
			require.True(t, proto.Equal(updatedLeftRange, leftStore.GetRange(2)))
			rightStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 3)
			require.True(t, proto.Equal(newRightRange, rightStore.GetRange(3)))
			harness.assertFired(t)
		})
	}
}

func TestRecoverRollbackTxnRetryMatrix(t *testing.T) {
	testCases := []struct {
		name   string
		timing faultTiming
	}{
		{name: "rollback_before_apply", timing: faultBeforeApply},
		{name: "rollback_after_apply", timing: faultAfterApply},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newTxnFaultHarness(txnFaultRule{
				rangeID:   2,
				op:        rfpb.FinalizeOperation_ROLLBACK,
				timing:    tc.timing,
				remaining: 1,
			})
			ctx, store, coordinator, txnProto, txnRecord, userKey, metaKey := setupPendingRollbackTxnForRecovery(t, harness)

			require.NoError(t, coordinator.RecoverTxnRecordForTest(ctx, txnRecord))

			verifyTxnRecordNotExist(t, ctx, store.Sender(), txnProto.GetTransactionId())
			verifyDirectReadValue(t, ctx, store.Sender(), userKey, []byte("before"))
			verifyDirectReadNotFound(t, ctx, store.Sender(), metaKey)
			harness.assertFired(t)
		})
	}
}

func TestStalePendingJanitorHelpsCommit(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t, testutil.StoreOptions{})
	s2 := sf.NewStore(t, testutil.StoreOptions{})
	s3 := sf.NewStore(t, testutil.StoreOptions{})
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()
	sf.StartShard(t, ctx, stores...)

	userKey := []byte("PTdefault/f11")
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("before"),
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, userKey, writeReq)
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponseFromProto(writeRsp).AnyError())

	metaKey := keys.MakeKey(constants.SystemPrefix, []byte("stale-pending-commit"))
	metaStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	metaRD := metaStore.GetRange(1)
	dataStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	dataRD := dataStore.GetRange(2)

	dataBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("after"),
		},
	})
	metaBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   metaKey,
			Value: []byte("committed"),
		},
	})

	tb := rbuilder.NewTxn()
	tb.AddStatement().SetRangeDescriptor(dataRD).SetBatch(dataBatch)
	tb.AddStatement().SetRangeDescriptor(metaRD).SetBatch(metaBatch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	tc := txn.NewCoordinator(s1, s1.APIClient(), clock)
	for _, stmt := range txnProto.GetStatements() {
		require.NoError(t, tc.PrepareStatement(ctx, txnProto.GetTransactionId(), stmt))
	}

	stalePendingRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}
	require.NoError(t, tc.WriteTxnRecord(ctx, stalePendingRecord))

	commitRecord := proto.Clone(stalePendingRecord).(*rfpb.TxnRecord)
	commitRecord.TxnState = rfpb.TxnRecord_PREPARED
	commitRecord.Op = rfpb.FinalizeOperation_COMMIT
	require.NoError(t, tc.WriteTxnRecord(ctx, commitRecord))

	require.NoError(t, tc.RecoverTxnRecordForTest(ctx, stalePendingRecord))

	verifyTxnRecordNotExist(t, ctx, s1.Sender(), txnProto.GetTransactionId())
	verifyDirectReadValue(t, ctx, s1.Sender(), userKey, []byte("after"))
	verifyDirectReadValue(t, ctx, s1.Sender(), metaKey, []byte("committed"))
}

func TestRollbackNotFoundPreventsLatePrepare(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t, testutil.StoreOptions{})
	s2 := sf.NewStore(t, testutil.StoreOptions{})
	s3 := sf.NewStore(t, testutil.StoreOptions{})
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()
	sf.StartShard(t, ctx, stores...)

	userKey := []byte("PTdefault/f11")
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("before"),
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, userKey, writeReq)
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponseFromProto(writeRsp).AnyError())

	metaKey := keys.MakeKey(constants.SystemPrefix, []byte("rollback-marker-late-prepare"))
	metaStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	metaRD := metaStore.GetRange(1)
	dataStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	dataRD := dataStore.GetRange(2)

	dataBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   userKey,
			Value: []byte("after"),
		},
	})
	metaBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   metaKey,
			Value: []byte("late-prepare"),
		},
	})

	tb := rbuilder.NewTxn()
	tb.AddStatement().SetRangeDescriptor(dataRD).SetBatch(dataBatch)
	tb.AddStatement().SetRangeDescriptor(metaRD).SetBatch(metaBatch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	coordinator := txn.NewCoordinator(s1, s1.APIClient(), clock)
	require.NoError(t, coordinator.PrepareStatement(ctx, txnProto.GetTransactionId(), txnProto.GetStatements()[0]))

	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: clock.Now().UnixMicro(),
	}
	require.NoError(t, coordinator.WriteTxnRecord(ctx, txnRecord))

	require.NoError(t, coordinator.RecoverTxnRecordForTest(ctx, txnRecord))
	verifyTxnRecordNotExist(t, ctx, s1.Sender(), txnProto.GetTransactionId())
	verifyDirectReadValue(t, ctx, s1.Sender(), userKey, []byte("before"))
	verifyDirectReadNotFound(t, ctx, s1.Sender(), metaKey)

	// B is a normal write that would prepare successfully if rollback had not
	// written a participant-local marker for this txid.
	err = coordinator.PrepareStatement(ctx, txnProto.GetTransactionId(), txnProto.GetStatements()[1])
	require.Error(t, err)
	verifyDirectReadNotFound(t, ctx, s1.Sender(), metaKey)
}

func TestRollbackMarkerStampedWithFinalizeTime(t *testing.T) {
	sf := testutil.NewStoreFactory(t)
	s1 := sf.NewStore(t, testutil.StoreOptions{})
	s2 := sf.NewStore(t, testutil.StoreOptions{})
	s3 := sf.NewStore(t, testutil.StoreOptions{})
	stores := []*testutil.TestingStore{s1, s2, s3}
	ctx := context.Background()
	sf.StartShard(t, ctx, stores...)

	userKey := []byte("PTdefault/f11")
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: userKey, Value: []byte("before")},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := s1.Sender().SyncPropose(ctx, userKey, writeReq)
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponseFromProto(writeRsp).AnyError())

	dataStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	dataRD := dataStore.GetRange(2)
	metaStore := testutil.GetStoreWithRangeLease(t, ctx, stores, 1)
	metaRD := metaStore.GetRange(1)

	dataBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: userKey, Value: []byte("after")},
	})
	metaKey := keys.MakeKey(constants.SystemPrefix, []byte("rollback-marker-finalize-ts"))
	metaBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: metaKey, Value: []byte("x")},
	})

	tb := rbuilder.NewTxn()
	tb.AddStatement().SetRangeDescriptor(dataRD).SetBatch(dataBatch)
	tb.AddStatement().SetRangeDescriptor(metaRD).SetBatch(metaBatch)
	txnProto, err := tb.ToProto()
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	coordinator := txn.NewCoordinator(s1, s1.APIClient(), clock)
	require.NoError(t, coordinator.PrepareStatement(ctx, txnProto.GetTransactionId(), txnProto.GetStatements()[0]))

	createdAtUsec := clock.Now().UnixMicro()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: createdAtUsec,
	}
	require.NoError(t, coordinator.WriteTxnRecord(ctx, txnRecord))

	// Advance the clock so finalize time is distinct from creation time: the
	// marker must record the finalize time, not the txn creation time.
	clock.Advance(time.Hour)
	finalizeAtUsec := clock.Now().UnixMicro()

	// The janitor rolls back the stale PENDING txn, writing a participant-local
	// rollback marker on each range stamped with the finalize timestamp.
	require.NoError(t, coordinator.RecoverTxnRecordForTest(ctx, txnRecord))
	verifyTxnRecordNotExist(t, ctx, s1.Sender(), txnProto.GetTransactionId())
	verifyDirectReadValue(t, ctx, s1.Sender(), userKey, []byte("before"))

	repl, err := dataStore.GetReplica(2)
	require.NoError(t, err)

	// The marker's timestamp is exactly the finalize time: present at a cutoff of
	// finalizeAt, absent one microsecond earlier. A timestamp of 0 (field not
	// plumbed) would be skipped by the GC guard and fail the first assertion.
	has, err := repl.HasTxnRollbackMarkersBeforeForTest(finalizeAtUsec)
	require.NoError(t, err)
	require.True(t, has)
	has, err = repl.HasTxnRollbackMarkersBeforeForTest(finalizeAtUsec - 1)
	require.NoError(t, err)
	require.False(t, has)
	// In particular it is not stamped with the earlier creation time.
	has, err = repl.HasTxnRollbackMarkersBeforeForTest(createdAtUsec)
	require.NoError(t, err)
	require.False(t, has)
}
