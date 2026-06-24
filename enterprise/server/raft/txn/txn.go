package txn

import (
	"context"
	"fmt"
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
	// is considered expired and subject to cleanup processes.
	txnLivenessThreshold = 10 * time.Second
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
	return tc.RunTxnWithProto(ctx, txnProto)
}

// RunTxnWithProto runs a pre-built transaction proto. Prefer RunTxn for normal
// use; this entry point exists so tests can pin the txn_id before submission
// (e.g. for fault-injection harnesses that match on transaction_id).
func (tc *Coordinator) RunTxnWithProto(ctx context.Context, txnProto *rfpb.TxnRequest) error {
	txnID := txnProto.GetTransactionId()
	txnRecord := &rfpb.TxnRecord{
		TxnRequest:    txnProto,
		TxnState:      rfpb.TxnRecord_PENDING,
		CreatedAtUsec: tc.clock.Now().UnixMicro(),
	}

	pendingBytes, err := tc.writeTxnRecord(ctx, txnRecord)
	if err != nil {
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
		err := tc.PrepareStatement(ctx, txnID, statement)
		if err != nil {
			prepareError = err
			log.Errorf("failed to prepare txn %q for %d: %s", txnID, statement.GetRange().GetRangeId(), err)
		}
	}

	// Determine whether to ROLLBACK or COMMIT based on whether or not all
	// statements in the transaction were successfully prepared.
	operation := rfpb.FinalizeOperation_ROLLBACK
	if prepareError == nil {
		operation = rfpb.FinalizeOperation_COMMIT
	}

	// Record our decision by CASing the txn record from PENDING to PREPARED.
	// pendingBytes is the exact value written above, so the CAS lands only if the
	// record is untouched. This is the single linearization point for the txn's
	// outcome: recoverTxnRecords races us for the same record and exactly one wins.
	txnRecord.Op = operation
	txnRecord.TxnState = rfpb.TxnRecord_PREPARED
	matched, currentRecord, err := tc.casTxnRecord(ctx, pendingBytes, txnRecord)
	if err != nil {
		return status.WrapErrorf(err, "failed to write txn decision (txid=%q)", txnID)
	}

	if matched {
		// We won the decision. Finalize our record, then surface any prepare error
		// (nil on a successful COMMIT, so the happy path returns nil).
		if err := tc.finalizeTxnRecord(ctx, txnRecord); err != nil {
			return err
		}
		return prepareError
	}

	// We lost the CAS: another actor (recoverTxnRecords or a peer coordinator)
	// already wrote the decision. A nil currentRecord means it was finalized and
	// deleted out from under us.
	if currentRecord == nil {
		return status.FailedPreconditionErrorf("txn record was deleted before decision (txid=%q)", txnID)
	}
	// Help drive the winner's decision to completion (finalize is idempotent),
	// then report.
	if err := tc.finalizeTxnRecord(ctx, currentRecord); err != nil {
		return err
	}
	// Check prepareError before the op mismatch: a prepare failure means our
	// intended outcome was ROLLBACK, so the caller must learn the txn did not
	// commit regardless of who won; an op mismatch only matters if our prepare
	// succeeded.
	if prepareError != nil {
		return prepareError
	}
	if currentRecord.GetOp() != operation {
		return status.FailedPreconditionErrorf("txn decision already written (txid=%q, op=%s)", txnID, currentRecord.GetOp())
	}
	return nil
}

func txnSessionID(txnID []byte, phase string) []byte {
	return []byte(fmt.Sprintf("txn/%x/%s", txnID, phase))
}

// txnRequestSession builds a deterministic idempotency session for a txn
// statement. The ID is derived from (txn_id, phase) and the index is fixed,
// so any retry of the same logical statement carries the same session and
// the replica's session dedup replays the original response instead of
// re-applying. This is only safe because RunTxn enforces at most one
// statement per range per txn (see the range-collision check there); two
// statements with the same phase on the same range would collide on this
// session ID.
func (tc *Coordinator) txnRequestSession(txnID []byte, phase string) *rfpb.Session {
	return &rfpb.Session{
		Id:            txnSessionID(txnID, phase),
		Index:         1,
		CreatedAtUsec: tc.clock.Now().UnixMicro(),
	}
}

func (tc *Coordinator) PrepareStatement(ctx context.Context, txnID []byte, statement *rfpb.TxnRequest_Statement) error {
	batch := statement.GetRawBatch()
	batch.TransactionId = txnID
	batch.Session = tc.txnRequestSession(txnID, "prepare")
	for _, hook := range statement.GetHooks() {
		if hook.GetPhase() == rfpb.TransactionHook_PREPARE {
			batch.PostCommitHooks = append(batch.PostCommitHooks, hook.GetHook())
		}
	}

	err := tc.run(ctx, statement, batch)
	if err != nil {
		return status.WrapError(err, "unable to prepare statement")
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

// writeTxnRecord persists txnRecord (in the PENDING state) and returns the exact
// marshaled bytes it wrote. The caller passes those bytes back to casTxnRecord as
// the CAS expected-value, so the PENDING->PREPARED decision only lands if the
// record is untouched in between — this is the txn's linearization anchor.
func (tc *Coordinator) writeTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord) ([]byte, error) {
	key := keys.MakeKey(constants.TxnRecordPrefix, txnRecord.GetTxnRequest().GetTransactionId())
	buf, err := proto.Marshal(txnRecord)
	if err != nil {
		return nil, err
	}
	batch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   key,
			Value: buf,
		},
	}).ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := tc.sender().SyncPropose(ctx, key, batch)
	if err != nil {
		return nil, err
	}
	if err := rbuilder.NewBatchResponseFromProto(rsp).AnyError(); err != nil {
		return nil, err
	}
	return buf, nil
}

func (tc *Coordinator) WriteTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord) error {
	_, err := tc.writeTxnRecord(ctx, txnRecord)
	return err
}

// casTxnRecord atomically swaps the txn record from expectedValue to txnRecord's
// marshaled form. The return tuple (matched, current, err) encodes the outcome:
//   - (true, nil, nil):     the CAS succeeded.
//   - (false, current, nil): the CAS failed; current is the record now on disk
//     (the value the caller should help finalize).
//   - (false, nil, nil):    the CAS failed and the record is absent/empty (it was
//     already finalized and deleted).
//   - (false, nil, err):    an RPC or unmarshal error occurred.
func (tc *Coordinator) casTxnRecord(ctx context.Context, expectedValue []byte, txnRecord *rfpb.TxnRecord) (bool, *rfpb.TxnRecord, error) {
	key := keys.MakeKey(constants.TxnRecordPrefix, txnRecord.GetTxnRequest().GetTransactionId())
	buf, err := proto.Marshal(txnRecord)
	if err != nil {
		return false, nil, err
	}
	batch, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   key,
			Value: buf,
		},
		ExpectedValue: expectedValue,
	}).ToProto()
	if err != nil {
		return false, nil, err
	}
	rsp, err := tc.sender().SyncPropose(ctx, key, batch)
	if err != nil {
		return false, nil, err
	}
	casRsp, err := rbuilder.NewBatchResponseFromProto(rsp).CASResponse(0)
	if err == nil {
		return true, nil, nil
	}
	if !status.IsFailedPreconditionError(err) || casRsp == nil {
		return false, nil, err
	}
	kv := casRsp.GetKv()
	if len(kv.GetValue()) == 0 {
		return false, nil, nil
	}
	currentRecord := &rfpb.TxnRecord{}
	if unmarshalErr := proto.Unmarshal(kv.GetValue(), currentRecord); unmarshalErr != nil {
		return false, nil, unmarshalErr
	}
	return false, currentRecord, nil
}

func isConflictKeyError(err error) bool {
	return status.IsUnavailableError(err) && strings.Contains(status.Message(err), constants.ConflictKeyMsg)
}

func (tc *Coordinator) run(ctx context.Context, stmt *rfpb.TxnRequest_Statement, batch *rfpb.BatchCmdRequest) error {
	var headerFn header.MakeFunc
	// We want to ensure the statement is run on the lease holder; but we don't
	// want to check the generation. See go/raft-range-validation-in-txn.
	if stmt.GetRangeValidationRequired() {
		headerFn = header.MakeLinearizableWithLeaseValidationOnly
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
	return status.UnavailableErrorf("tx.run retries exceeded for txid: %q err: %w", batch.GetTransactionId(), lastError)

}

func (tc *Coordinator) finalizeTxn(ctx context.Context, txnID []byte, op rfpb.FinalizeOperation, stmt *rfpb.TxnRequest_Statement) error {
	batch := rbuilder.NewBatchBuilder().SetTransactionID(txnID)
	batch.SetFinalizeOperation(op)

	batchProto, err := batch.ToProto()
	if err != nil {
		return err
	}
	batchProto.Session = tc.txnRequestSession(txnID, "finalize/"+op.String())
	// Proposer-stamped finalize time. On ROLLBACK the replica records this as the
	// rollback marker's retention timestamp; kept separate from the session so
	// marker GC does not depend on session lifetime semantics.
	batchProto.TxnFinalizedAtUsec = tc.clock.Now().UnixMicro()

	if op == rfpb.FinalizeOperation_COMMIT {
		for _, hook := range stmt.GetHooks() {
			if hook.GetPhase() == rfpb.TransactionHook_COMMIT {
				batchProto.PostCommitHooks = append(batchProto.PostCommitHooks, hook.GetHook())
			}
		}
	}
	err = tc.run(ctx, stmt, batchProto)
	if err != nil {
		return status.WrapErrorf(err, "unable to finalize txn on stmt op=%s", op)
	}
	return nil
}

func (tj *Coordinator) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tj.clock.After(txnCleanupPeriod):
			tj.recoverTxnRecords(ctx)
		}
	}
}

func (tc *Coordinator) recoverTxnRecords(ctx context.Context) {
	if !tc.store.HasReplicaAndIsLeader(constants.MetaRangeID) {
		return
	}
	txnRecords, err := tc.fetchTxnRecords(ctx, false /*=includeLive*/)
	if err != nil {
		log.Warningf("Failed to fetch txn records: %s", err)
	}

	errCount := 0
	for _, txnRecord := range txnRecords {
		txnID := txnRecord.record.GetTxnRequest().GetTransactionId()
		if err := tc.recoverTxnRecord(ctx, txnRecord.record, txnRecord.raw); err != nil {
			log.Warningf("Failed to recoverTxnRecord for txn (%q): %s, statements: %+v", txnID, err, txnRecord.record.GetTxnRequest().GetStatements())
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

type fetchedTxnRecord struct {
	record *rfpb.TxnRecord
	raw    []byte
}

func (tc *Coordinator) fetchTxnRecords(ctx context.Context, includeLive bool) ([]*fetchedTxnRecord, error) {
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
	txnRecords := make([]*fetchedTxnRecord, 0, len(scanRsp.GetKvs()))
	for _, kv := range scanRsp.GetKvs() {
		txnRecord := &rfpb.TxnRecord{}
		if err := proto.Unmarshal(kv.GetValue(), txnRecord); err != nil {
			log.Errorf("scan returned unparsable kv (key: %q): %s", kv.GetKey(), err)
			continue
		}
		createdAt := time.UnixMicro(txnRecord.GetCreatedAtUsec())
		if !includeLive && tc.clock.Since(createdAt) < txnLivenessThreshold {
			// This txn record is created very recently; skip processing
			continue
		}
		txnRecords = append(txnRecords, &fetchedTxnRecord{
			record: txnRecord,
			raw:    kv.GetValue(),
		})
	}
	return txnRecords, nil
}

func (tc *Coordinator) FetchTxnRecords(ctx context.Context, includeLive bool) ([]*rfpb.TxnRecord, error) {
	fetched, err := tc.fetchTxnRecords(ctx, includeLive)
	if err != nil {
		return nil, err
	}
	txnRecords := make([]*rfpb.TxnRecord, 0, len(fetched))
	for _, txnRecord := range fetched {
		txnRecords = append(txnRecords, txnRecord.record)
	}
	return txnRecords, nil
}

func isTxnNotFoundError(err error) bool {
	return status.IsNotFoundError(err) && strings.Contains(err.Error(), constants.TxnNotFoundMessage)
}

func (tc *Coordinator) finalizeTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord) error {
	txnID := txnRecord.GetTxnRequest().GetTransactionId()
	if txnRecord.GetOp() == rfpb.FinalizeOperation_UNKNOWN_OPERATION {
		return status.InvalidArgumentError("unexpected txnRecord.op")
	}

	for _, stmt := range txnRecord.GetTxnRequest().GetStatements() {
		err := tc.finalizeTxn(ctx, txnID, txnRecord.GetOp(), stmt)
		if err != nil && !isTxnNotFoundError(err) {
			return status.WrapErrorf(err, "failed to finalize statement on range %d, operation=%s", stmt.GetRange().GetRangeId(), txnRecord.GetOp())
		}
	}
	return tc.deleteTxnRecord(ctx, txnID)
}

// recoverTxnRecord drives a single scanned txn record to completion. A record
// still PENDING means no coordinator recorded a decision, so the recovery routine
// CASes it to PREPARED{ROLLBACK} — fencing any coordinator still racing it —
// and rolls it back; a record already PREPARED is finalized directly. raw is the
// record's exact on-disk value, used as the CAS expected-value.
func (tc *Coordinator) recoverTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord, raw []byte) error {
	if txnRecord.GetTxnState() == rfpb.TxnRecord_PENDING {
		rollbackRecord := txnRecord.CloneVT()
		rollbackRecord.TxnState = rfpb.TxnRecord_PREPARED
		rollbackRecord.Op = rfpb.FinalizeOperation_ROLLBACK

		matched, currentRecord, err := tc.casTxnRecord(ctx, raw, rollbackRecord)
		if err != nil {
			return status.WrapError(err, "failed to write rollback decision")
		}
		if matched {
			// We won the decision; roll the txn back.
			return tc.finalizeTxnRecord(ctx, rollbackRecord)
		}
		// We lost the CAS: a coordinator recorded the decision first. A nil
		// currentRecord means it already finalized and deleted the record, so
		// there is nothing left to do; otherwise help finalize its decision.
		if currentRecord == nil {
			return nil
		}
		return tc.finalizeTxnRecord(ctx, currentRecord)
	}
	if txnRecord.GetTxnState() == rfpb.TxnRecord_PREPARED {
		return tc.finalizeTxnRecord(ctx, txnRecord)
	}
	return status.InvalidArgumentErrorf("unexpected txnRecord.state %s", txnRecord.GetTxnState())
}

// RecoverTxnRecordForTest fences the decision CAS on the marshaled bytes of the
// passed record (the production recovery routine, recoverTxnRecords, uses the raw
// scanned bytes). This lets a test drive a specific, possibly stale recovery view
// — e.g. processing a PENDING snapshot of a record that is already PREPARED.
func (tc *Coordinator) RecoverTxnRecordForTest(ctx context.Context, txnRecord *rfpb.TxnRecord) error {
	raw, err := proto.Marshal(txnRecord)
	if err != nil {
		return err
	}
	return tc.recoverTxnRecord(ctx, txnRecord, raw)
}
