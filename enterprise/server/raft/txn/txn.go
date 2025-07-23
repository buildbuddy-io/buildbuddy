package txn

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// txnLivenessThreshold defines the maximum allowable time duration since
	// the transaction was created. If a transaction exceeds this threshold, it
	// is considered expired and subject to cleanup processes."
	txnLivessnessThreshold = 10 * time.Second
	// How often do we scan transaction records and clean them up.
	txnCleanupPeriod = 15 * time.Second
)

type IStore interface {
	HasReplicaAndIsLeader(rangeID uint64) bool
	Sender() *sender.Sender
}

type Coordinator struct {
	store IStore
	// Keeps track of connections to other machines.
	apiClient *client.APIClient
	clock     clockwork.Clock
}

func NewCoordinator(store IStore, apiClient *client.APIClient, clock clockwork.Clock) *Coordinator {
	return &Coordinator{
		store:     store,
		apiClient: apiClient,
		clock:     clock,
	}
}

func (tc *Coordinator) sender() *sender.Sender {
	return tc.store.Sender()
}

func (tc *Coordinator) RunTxn(ctx context.Context, txn *rbuilder.TxnBuilder) error {
	txnProto, err := txn.ToProto()
	if err != nil {
		return err
	}

	txnID := txnProto.GetTransactionId()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: time.Now().UnixMicro(),
	}

	if err = tc.WriteTxnRecord(ctx, txnRecord); err != nil {
		return err
	}

	// Check that each statement addresses a different range. If two statements
	// address a single range, they should be combined, otherwise finalization
	// will fail when attempted twice.
	rangeStatementMap := make(map[uint64]int)
	for i, statement := range txnProto.GetStatements() {
		rangeID := statement.GetRange().GetRangeId()
		existing, ok := rangeStatementMap[rangeID]
		if ok {
			return status.FailedPreconditionErrorf("Statements %d and %d both address range %d. Only one batch per range is allowed", i, existing, rangeID)
		}
		rangeStatementMap[rangeID] = i
	}

	var prepareError error
	for _, statement := range txnProto.GetStatements() {
		err := tc.prepareStatement(ctx, txnID, statement)
		if err != nil {
			prepareError = err
			log.Errorf("failed to prepare txn for %d: %s", statement.GetRange().GetRangeId(), err)
		}
	}

	// Determine whether to ROLLBACK or COMMIT based on whether or not all
	// statements in the transaction were successfully prepared.
	operation := rfpb.FinalizeOperation_ROLLBACK
	if prepareError == nil {
		operation = rfpb.FinalizeOperation_COMMIT
	}

	txnRecord.Op = operation
	txnRecord.TxnState = rfpb.TxnRecord_PREPARED
	if err = tc.WriteTxnRecord(ctx, txnRecord); err != nil {
		return status.InternalErrorf("failed to write txn record (txid=%q): %s", txnID, err)
	}

	for _, stmt := range txnProto.GetStatements() {
		// Finalize each statement.
		if err := tc.finalizeTxn(ctx, txnID, operation, stmt); err != nil {
			if isTxnNotFoundError(err) && operation == rfpb.FinalizeOperation_ROLLBACK {
				// if there is error during preparation for this range, then txn not found is expected during rollback.
				continue
			}
			return status.InternalErrorf("failed to finalize statement in txn(%q)for range_id:%d, operation: %s, %s", txnID, stmt.GetRange().GetRangeId(), operation, err)
		}
	}

	if err := tc.deleteTxnRecord(ctx, txnID); err != nil {
		return status.InternalErrorf("failed to delete txn record (txid=%q): %s", txnID, err)
	}
	if prepareError != nil {
		return prepareError
	}
	return nil
}

func (tc *Coordinator) prepareStatement(ctx context.Context, txnID []byte, statement *rfpb.TxnRequest_Statement) error {
	batch := statement.GetRawBatch()
	batch.TransactionId = txnID
	for _, hook := range statement.GetHooks() {
		if hook.GetPhase() == rfpb.TransactionHook_PREPARE {
			log.Infof("add post commit hook")
			batch.PostCommitHooks = append(batch.PostCommitHooks, hook.GetHook())
		}
	}

	err := tc.run(ctx, statement, batch)
	if err != nil {
		return status.WrapErrorf(err, "unable to prepare statement")
	}
	return nil
}

func (tc *Coordinator) deleteTxnRecord(ctx context.Context, txnID []byte) error {
	key := keys.MakeKey(constants.TxnRecordPrefix, txnID)
	batch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectDeleteRequest{
		Key: key,
	}).ToProto()
	if err != nil {
		return err
	}
	rsp, err := tc.sender().SyncPropose(ctx, key, batch)
	if err != nil {
		return err
	}
	return rbuilder.NewBatchResponseFromProto(rsp).AnyError()
}

func (tc *Coordinator) WriteTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord) error {
	key := keys.MakeKey(constants.TxnRecordPrefix, txnRecord.GetTxnRequest().GetTransactionId())
	buf, err := proto.Marshal(txnRecord)
	if err != nil {
		return err
	}
	batch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   key,
			Value: buf,
		},
	}).ToProto()
	if err != nil {
		return err
	}
	rsp, err := tc.sender().SyncPropose(ctx, key, batch)
	if err != nil {
		return err
	}
	return rbuilder.NewBatchResponseFromProto(rsp).AnyError()
}

func isConflictKeyError(err error) bool {
	return status.IsUnavailableError(err) && strings.Contains(status.Message(err), constants.ConflictKeyMsg)
}

func (tc *Coordinator) run(ctx context.Context, stmt *rfpb.TxnRequest_Statement, batch *rfpb.BatchCmdRequest) error {
	var headerFn header.MakeFunc
	if stmt.GetRangeValidationRequired() {
		if batch.GetFinalizeOperation() == rfpb.FinalizeOperation_COMMIT {
			headerFn = header.MakeLinearizableWithLeaseValidationOnly
		} else {
			headerFn = header.MakeLinearizableWithRangeValidation
		}
	} else {
		headerFn = header.MakeLinearizableWithoutRangeValidation
	}
	retrier := retry.DefaultWithContext(ctx)
	var lastError error
	for retrier.Next() {
		syncRsp, err := tc.sender().SyncProposeWithRangeDescriptor(ctx, stmt.GetRange(), batch, headerFn)
		if err == nil {
			rsp := rbuilder.NewBatchResponseFromProto(syncRsp.GetBatch())
			if err := rsp.AnyError(); err != nil {
				return err
			}
			return nil
		}

		if !status.IsOutOfRangeError(err) && !isConflictKeyError(err) {
			return err
		}
		lastError = err
	}
	return status.UnavailableErrorf("tx.run retries exceeded for txid: %q err: %s", batch.GetTransactionId(), lastError)

}

func (tc *Coordinator) finalizeTxn(ctx context.Context, txnID []byte, op rfpb.FinalizeOperation, stmt *rfpb.TxnRequest_Statement) error {
	batch := rbuilder.NewBatchBuilder().SetTransactionID(txnID)
	batch.SetFinalizeOperation(op)

	batchProto, err := batch.ToProto()
	if err != nil {
		return err
	}

	if op == rfpb.FinalizeOperation_COMMIT {
		for _, hook := range stmt.GetHooks() {
			if hook.GetPhase() == rfpb.TransactionHook_COMMIT {
				batchProto.PostCommitHooks = append(batchProto.PostCommitHooks, hook.GetHook())
			}
		}
	}
	err = tc.run(ctx, stmt, batchProto)
	if err != nil {
		return status.WrapErrorf(err, "unable to finalize txn on stmt")
	}
	return nil
}

func (tj *Coordinator) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tj.clock.After(txnCleanupPeriod):
			tj.processTxnRecords(ctx)
		}
	}
}

func (tc *Coordinator) processTxnRecords(ctx context.Context) {
	if !tc.store.HasReplicaAndIsLeader(constants.MetaRangeID) {
		return
	}
	txnRecords, err := tc.FetchTxnRecords(ctx)
	if err != nil {
		log.Warningf("Failed to fetch txn records: %s", err)
	}

	errCount := 0
	for _, txnRecord := range txnRecords {
		txnID := txnRecord.GetTxnRequest().GetTransactionId()
		if err := tc.ProcessTxnRecord(ctx, txnRecord); err != nil {
			log.Warningf("Failed to processTxnRecords: %s, statements: %+v", err, txnRecord.GetTxnRequest().GetStatements())
			errCount++
		} else {
			log.Debugf("Successfully processed txn record %q", txnID)
		}
	}
	successCount := len(txnRecords) - errCount

	if successCount > 0 {
		metrics.RaftTxnRecordProcessCount.WithLabelValues("success").Add(float64(successCount))
	}

	if errCount > 0 {
		metrics.RaftTxnRecordProcessCount.WithLabelValues("failure").Add(float64(errCount))
	}
}

func (tc *Coordinator) FetchTxnRecords(ctx context.Context) ([]*rfpb.TxnRecord, error) {
	start, end := keys.Range(constants.TxnRecordPrefix)

	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    start,
		End:      end,
		ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
	}).ToProto()
	if err != nil {
		return nil, err
	}

	rsp, err := tc.sender().SyncRead(ctx, constants.TxnRecordPrefix, batchReq)
	if err != nil {
		return nil, err
	}

	batchResp := rbuilder.NewBatchResponseFromProto(rsp)
	scanRsp, err := batchResp.ScanResponse(0)
	if err != nil {
		log.Errorf("Error reading scan response: %s", err)
		return nil, err
	}

	if len(scanRsp.GetKvs()) == 0 {
		return nil, nil
	}
	txnRecords := make([]*rfpb.TxnRecord, 0, len(scanRsp.GetKvs()))
	for _, kv := range scanRsp.GetKvs() {
		txnRecord := &rfpb.TxnRecord{}
		if err := proto.Unmarshal(kv.GetValue(), txnRecord); err != nil {
			log.Errorf("scan returned unparsable kv: %s", err)
			continue
		}
		createdAt := time.UnixMicro(txnRecord.GetCreatedAtUsec())
		if tc.clock.Since(createdAt) < txnLivessnessThreshold {
			// This txn record is created very recently; skip processing
			continue
		}
		txnRecords = append(txnRecords, txnRecord)
	}
	return txnRecords, nil
}

func isTxnNotFoundError(err error) bool {
	return status.IsNotFoundError(err) && strings.Contains(err.Error(), constants.TxnNotFoundMessage)
}

func (tc *Coordinator) ProcessTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord) error {
	txnID := txnRecord.GetTxnRequest().GetTransactionId()
	if txnRecord.GetTxnState() == rfpb.TxnRecord_PENDING {
		// The transaction is not fully prepared. Let's rollback all the statements.
		for _, statement := range txnRecord.GetTxnRequest().GetStatements() {
			err := tc.finalizeTxn(ctx, txnID, rfpb.FinalizeOperation_ROLLBACK, statement)
			if err != nil && !isTxnNotFoundError(err) {
				// if the statement is not prepared, we will get NotFound Error when we rollback and this is fine.
				return status.WrapErrorf(err, "failed to rollback pending statement on range %d", statement.GetRange().GetRangeId())
			}
		}
	} else if txnRecord.GetTxnState() == rfpb.TxnRecord_PREPARED {
		// The transaction is prepared, but not fully finalized. Let's finalize
		// all the prepared statements.
		if txnRecord.GetOp() == rfpb.FinalizeOperation_UNKNOWN_OPERATION {
			return status.InvalidArgumentError("unexpected txnRecord.op")
		}

		for _, stmt := range txnRecord.GetTxnRequest().GetStatements() {
			err := tc.finalizeTxn(ctx, txnID, txnRecord.GetOp(), stmt)
			if err != nil && !isTxnNotFoundError(err) {
				// if the statement is already finalized, we will get NotFound Error when we finalize and this is fine.
				return status.WrapErrorf(err, "failed to finalize prepared statement on range %d", stmt.GetRange().GetRangeId())
			}
		}
	}
	return tc.deleteTxnRecord(ctx, txnID)
}
