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
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
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

func (tc *Coordinator) syncPropose(ctx context.Context, rd *rfpb.RangeDescriptor, batchCmd *rfpb.BatchCmdRequest) (*rfpb.SyncProposeResponse, error) {
	var syncRsp *rfpb.SyncProposeResponse
	runFn := func(c rfspb.ApiClient, h *rfpb.Header) error {
		r, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchCmd,
		})
		if err != nil {
			return err
		}
		syncRsp = r
		return nil
	}
	_, err := tc.sender().TryReplicas(ctx, rd, runFn, func(rd *rfpb.RangeDescriptor, replicaIdx int) *rfpb.Header {
		return header.NewWithoutRangeInfo(rd, replicaIdx, rfpb.Header_LINEARIZABLE)
	})
	return syncRsp, err
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
		batch := statement.GetRawBatch()
		batch.TransactionId = txnID
		rangeID := statement.GetRange().GetRangeId()

		// Prepare each statement.
		syncRsp, err := tc.syncPropose(ctx, statement.GetRange(), batch)
		if err != nil {
			log.Errorf("Error preparing txn statement for %q (range: %d): %s", txnID, rangeID, err)
			prepareError = err
			break
		}
		rsp := rbuilder.NewBatchResponseFromProto(syncRsp.GetBatch())
		if err := rsp.AnyError(); err != nil {
			log.Errorf("Error preparing txn statement for %q (range: %d): %s", txnID, rangeID, err)
			prepareError = err
			break
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
		rd := stmt.GetRange()
		// Finalize each statement.
		if err := tc.finalizeTxn(ctx, txnID, operation, rd); err != nil {
			if isTxnNotFoundError(err) && operation == rfpb.FinalizeOperation_ROLLBACK {
				// if there is error during preparation for this range, then txn not found is expected during rollback.
				continue
			}
			return status.InternalErrorf("failed to finalize statement in txn(%q)for range_id:%d, operation: %s, %s", txnID, rd.GetRangeId(), operation, err)
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

func (tc *Coordinator) finalizeTxn(ctx context.Context, txnID []byte, op rfpb.FinalizeOperation, rd *rfpb.RangeDescriptor) error {
	batch := rbuilder.NewBatchBuilder().SetTransactionID(txnID)
	batch.SetFinalizeOperation(op)

	batchProto, err := batch.ToProto()
	if err != nil {
		return err
	}

	// Prepare each statement.
	syncRsp, err := tc.syncPropose(ctx, rd, batchProto)
	if err != nil {
		return err
	}
	rsp := rbuilder.NewBatchResponseFromProto(syncRsp.GetBatch())
	return rsp.AnyError()
}

func (tj *Coordinator) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tj.clock.After(txnCleanupPeriod):
			err := tj.processTxnRecords(ctx)
			if err != nil {
				log.Warningf("Failed to processTxnRecords: %s", err)
			}
		}
	}
}

func (tc *Coordinator) processTxnRecords(ctx context.Context) error {
	if !tc.store.HasReplicaAndIsLeader(constants.MetaRangeID) {
		return nil
	}
	txnRecords, err := tc.FetchTxnRecords(ctx)
	if err != nil {
		return status.InternalErrorf("failed to fetch txn records: %s", err)
	}

	log.Infof("fetched %d TxnRecords to process", len(txnRecords))
	for _, txnRecord := range txnRecords {
		txnID := txnRecord.GetTxnRequest().GetTransactionId()
		if err := tc.ProcessTxnRecord(ctx, txnRecord); err != nil {
			return status.InternalErrorf("failed to process txn record %q", txnID)
		}
		log.Debugf("Successfully processed txn record %q", txnID)
	}
	return nil
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
			err := tc.finalizeTxn(ctx, txnID, rfpb.FinalizeOperation_ROLLBACK, statement.GetRange())
			if err != nil && !isTxnNotFoundError(err) {
				// if the statement is not prepared, we will get NotFound Error when we rollback and this is fine.
				return err
			}
		}
	} else if txnRecord.GetTxnState() == rfpb.TxnRecord_PREPARED {
		// The transaction is prepared, but not fully finalized. Let's finalize
		// all the prepared statements.
		if txnRecord.GetOp() == rfpb.FinalizeOperation_UNKNOWN_OPERATION {
			return status.InvalidArgumentError("unexpected txnRecord.op")
		}

		for _, stmt := range txnRecord.GetTxnRequest().GetStatements() {
			err := tc.finalizeTxn(ctx, txnID, txnRecord.GetOp(), stmt.GetRange())
			if err != nil && !isTxnNotFoundError(err) {
				// if the statement is already finalized, we will get NotFound Error when we finalize and this is fine.
				return err
			}
		}
	}
	return tc.deleteTxnRecord(ctx, txnID)
}
