package txnjanitor

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	txnLivessnessThreshold = 10 * time.Second
	txnCleanupPeriod       = 15 * time.Second
)

type IStore interface {
	IsLeader(shardID uint64) bool
	Sender() *sender.Sender
}

type TxnJanitor struct {
	store IStore
}

func New(store IStore) *TxnJanitor {
	return &TxnJanitor{
		store: store,
	}
}

func (tj *TxnJanitor) sender() *sender.Sender {
	return tj.store.Sender()
}

func (tj *TxnJanitor) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(txnCleanupPeriod):
				err := tj.processTxnRecords(ctx)
				if err != nil {
					log.Warningf("Failed to processTxnRecords: %s", err)
				}
			}
		}
	}()
}

func (tj *TxnJanitor) processTxnRecords(ctx context.Context) error {
	if !tj.store.IsLeader(constants.InitialShardID) {
		return nil
	}
	txnRecords, err := tj.fetchTxnRecords(ctx)
	if err != nil {
		return status.InternalErrorf("failed to fetch txn records: %s", err)
	}

	log.Infof("fetched %d TxnRecords to process", len(txnRecords))
	for _, txnRecord := range txnRecords {
		if err := tj.processTxnRecord(ctx, txnRecord); err != nil {
			return err
		}
	}
	return nil
}

func (tj *TxnJanitor) fetchTxnRecords(ctx context.Context) ([]*rfpb.TxnRecord, error) {
	start, end := keys.Range(constants.TxnRecordPrefix)

	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    start,
		End:      end,
		ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
	}).ToProto()
	if err != nil {
		return nil, err
	}

	rsp, err := tj.sender().SyncRead(ctx, constants.TxnRecordPrefix, batchReq)
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
		if time.Since(createdAt) < txnLivessnessThreshold {
			// This txn record is created very recently; skip processing
			continue
		}
		txnRecords = append(txnRecords, txnRecord)
	}
	return txnRecords, nil
}

func (tj *TxnJanitor) processTxnRecord(ctx context.Context, txnRecord *rfpb.TxnRecord) error {
	txnID := txnRecord.GetTxnRequest().GetTransactionId()
	if txnRecord.GetTxnState() == rfpb.TxnRecord_PENDING {
		// The transaction is not fully prepared. Let's rollback all the statements.
		for _, statement := range txnRecord.GetTxnRequest().GetStatements() {
			err := tj.sender().FinalizeTxn(ctx, txnID, rfpb.FinalizeOperation_ROLLBACK, statement.GetReplica())
			if err != nil && !status.IsNotFoundError(err) {
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

		for _, replica := range txnRecord.GetPrepared() {
			err := tj.sender().FinalizeTxn(ctx, txnID, txnRecord.GetOp(), replica)
			if err != nil && !status.IsNotFoundError(err) {
				// if the statement is already finalized, we will get NotFound Error when we finalize and this is fine.
				return err
			}
		}
	}
	txnRecordKey := keys.MakeKey(constants.TxnRecordPrefix, txnID)
	return tj.sender().DeleteTxnRecord(ctx, txnRecordKey)
}
