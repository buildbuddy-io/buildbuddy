package replica

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/docker/go-units"
	"github.com/prometheus/client_golang/prometheus"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const (
	gb = 1 << 30
)

var (
	// Estimated disk usage will be re-computed when more than this many
	// state machine updates have happened since the last check.
	// Assuming 1024 size chunks, checking every 1000 writes will mean
	// re-evaluating our size when it's increased by ~1MB.
	entriesBetweenUsageChecks = flag.Int("cache.raft.entries_between_usage_checks", 1_000, "Re-check usage after this many updates")
)

// Replicas need a reference back to the Store that holds them in order to
// add and remove themselves, read files from peers, etc. In order to make this
// more easily testable in a standalone fashion, IStore mocks out just the
// necessary methods that a Replica requires a Store to have.
type IStore interface {
	AddRange(rd *rfpb.RangeDescriptor, r *Replica)
	RemoveRange(rd *rfpb.RangeDescriptor, r *Replica)
	SnapshotCluster(ctx context.Context, rangeID uint64) error
	NHID() string
}

// Replica implements the interface IOnDiskStateMachine. More details of
// IOnDiskStateMachine can be found at https://pkg.go.dev/github.com/lni/dragonboat/v4/statemachine#IOnDiskStateMachine.
type Replica struct {
	leaser pebble.Leaser

	rootDir   string
	fileDir   string
	rangeID   uint64
	replicaID uint64
	// The ID of the node where this replica resided.
	NHID string

	store               IStore
	lastAppliedIndex    uint64
	lastUsageCheckIndex uint64

	partitionMetadataMu sync.Mutex
	partitionMetadata   map[string]*rfpb.PartitionMetadata

	log             log.Logger
	rangeMu         sync.RWMutex
	rangeDescriptor *rfpb.RangeDescriptor
	mappedRange     *rangemap.Range
	leaseMu         sync.RWMutex
	rangeLease      *rfpb.RangeLeaseRecord

	fileStorer filestore.Store

	quitChan  chan struct{}
	broadcast chan<- events.Event

	readQPS        *qps.Counter
	raftProposeQPS *qps.Counter

	lockedKeys map[string][]byte       // key => txid
	prepared   map[string]pebble.Batch // string(txid) => prepared batch.
}

func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func bytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func isLocalKey(key []byte) bool {
	return bytes.HasPrefix(key, constants.LocalPrefix) ||
		bytes.HasPrefix(key, constants.SystemPrefix) ||
		bytes.HasPrefix(key, constants.MetaRangePrefix)
}

func sizeOf(key []byte, val []byte) (int64, error) {
	if isLocalKey(key) {
		return int64(len(val)), nil
	}

	md := &rfpb.FileMetadata{}
	if err := proto.Unmarshal(val, md); err != nil {
		return 0, err
	}
	size := int64(len(val))
	if md.GetStorageMetadata().GetFileMetadata() != nil {
		size += md.GetStoredSizeBytes()
	}
	return size, nil
}

func isFileRecordKey(keyBytes []byte) bool {
	key := &filestore.PebbleKey{}
	if _, err := key.FromBytes(keyBytes); err == nil {
		return true
	}
	return false
}

func (sm *Replica) batchContainsKey(wb pebble.Batch, key []byte) ([]byte, bool) {
	k := sm.replicaLocalKey(key)
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, value, _, _ := batchReader.Next()
		if bytes.Equal(ukey, k) {
			return value, true
		}
	}
	return nil, false
}

func (sm *Replica) replicaPrefix() []byte {
	return LocalKeyPrefix(sm.rangeID, sm.replicaID)
}

func (sm *Replica) replicaLocalKey(key []byte) []byte {
	if !isLocalKey(key) {
		log.Debugf("Key %q does not need a prefix!", key)
		return key
	}
	prefix := sm.replicaPrefix()
	if bytes.HasPrefix(key, prefix) {
		log.Debugf("Key %q already has replica prefix!", key)
		return key
	}
	return append(prefix, key...)
}

func (sm *Replica) Usage() (*rfpb.ReplicaUsage, error) {
	sm.rangeMu.RLock()
	rd := sm.rangeDescriptor
	sm.rangeMu.RUnlock()

	ru := &rfpb.ReplicaUsage{
		Replica: &rfpb.ReplicaDescriptor{
			RangeId:   sm.rangeID,
			ReplicaId: sm.replicaID,
			Nhid:      proto.String(sm.NHID),
		},
		RangeId: rd.GetRangeId(),
	}
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	sizeBytes, err := db.EstimateDiskUsage(rd.GetStart(), rd.GetEnd())
	if err != nil {
		return nil, err
	}
	sm.partitionMetadataMu.Lock()
	for _, pm := range sm.partitionMetadata {
		ru.Partitions = append(ru.Partitions, pm.CloneVT())
	}
	sm.partitionMetadataMu.Unlock()
	ru.EstimatedDiskBytesUsed = int64(sizeBytes)
	ru.ReadQps = int64(sm.readQPS.Get())
	ru.RaftProposeQps = int64(sm.raftProposeQPS.Get())

	return ru, nil
}

func (sm *Replica) name() string {
	return getName(sm.rangeID, sm.replicaID)
}

func rdString(rd *rfpb.RangeDescriptor) string {
	if rd == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Range(%d) [%q, %q) gen %d", rd.GetRangeId(), rd.GetStart(), rd.GetEnd(), rd.GetGeneration())
}

func (sm *Replica) notifyListenersOfUsage(rd *rfpb.RangeDescriptor, usage *rfpb.ReplicaUsage) {
	if sm.broadcast == nil {
		return
	}
	usage.RangeId = rd.GetRangeId()
	up := events.RangeUsageEvent{
		Type:            events.EventRangeUsageUpdated,
		RangeDescriptor: rd,
		ReplicaUsage:    usage,
	}

	select {
	case sm.broadcast <- up:
		break
	default:
		sm.log.Warningf("dropped usage update: %+v", up)
	}
}

func (sm *Replica) setRangeLease(key, val []byte) error {
	if !bytes.HasPrefix(key, constants.LocalRangeLeaseKey) {
		return status.FailedPreconditionErrorf("[%s] setRangeLease called with non-range-lease key: %s", sm.name(), key)
	}
	lease := &rfpb.RangeLeaseRecord{}
	if err := proto.Unmarshal(val, lease); err != nil {
		return err
	}
	sm.leaseMu.Lock()
	sm.rangeLease = lease
	sm.leaseMu.Unlock()
	return nil
}

func (sm *Replica) GetRangeLease() *rfpb.RangeLeaseRecord {
	sm.leaseMu.RLock()
	defer sm.leaseMu.RUnlock()
	return sm.rangeLease
}

func (sm *Replica) setRange(key, val []byte) error {
	if !bytes.HasPrefix(key, constants.LocalRangeKey) {
		return status.FailedPreconditionErrorf("[%s] setRange called with non-range key: %s", sm.name(), key)
	}

	rangeDescriptor := &rfpb.RangeDescriptor{}
	if err := proto.Unmarshal(val, rangeDescriptor); err != nil {
		return err
	}

	sm.rangeMu.Lock()
	if sm.rangeDescriptor != nil {
		sm.store.RemoveRange(sm.rangeDescriptor, sm)
	}

	sm.log.Infof("Range descriptor is changing from %s to %s", rdString(sm.rangeDescriptor), rdString(rangeDescriptor))
	sm.rangeDescriptor = rangeDescriptor
	sm.mappedRange = &rangemap.Range{
		Start: rangeDescriptor.GetStart(),
		End:   rangeDescriptor.GetEnd(),
	}
	sm.store.AddRange(sm.rangeDescriptor, sm)
	sm.rangeMu.Unlock()

	if usage, err := sm.Usage(); err == nil {
		sm.notifyListenersOfUsage(rangeDescriptor, usage)
	} else {
		sm.log.Errorf("Error computing usage upon opening replica: %s", err)
	}
	return nil
}

func (sm *Replica) rangeCheckedSet(wb pebble.Batch, key, val []byte) error {
	if isLocalKey(key) {
		// Still here? this is a local key, so treat it appropriately.
		key = sm.replicaLocalKey(key)
		return wb.Set(key, val, nil /*ignored write options*/)
	}

	sm.rangeMu.RLock()
	containsKey := sm.mappedRange != nil && sm.mappedRange.Contains(key)
	sm.rangeMu.RUnlock()

	if containsKey {
		if isFileRecordKey(key) {
			if err := sm.updateAndFlushPartitionMetadatas(wb, key, val, nil /*=fileMetadata*/, fileRecordAdd); err != nil {
				return err
			}
		}
		return wb.Set(key, val, nil /*ignored write options*/)
	}
	return status.OutOfRangeErrorf("%s: [%s] range %s does not contain key %q", constants.RangeNotCurrentMsg, sm.name(), sm.mappedRange, string(key))
}

func (sm *Replica) lookup(db ReplicaReader, key []byte) ([]byte, error) {
	if isLocalKey(key) {
		key = sm.replicaLocalKey(key)
	}
	buf, closer, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, status.NotFoundErrorf("[%s] Key not found: %s", sm.name(), err)
		}
		return nil, err
	}
	defer closer.Close()
	if len(buf) == 0 {
		return nil, status.NotFoundErrorf("[%s] Key not found (empty)", sm.name())
	}

	// We need to copy the value from pebble before
	// closer is closed.
	val := make([]byte, len(buf))
	copy(val, buf)
	return val, nil
}

func (sm *Replica) LastAppliedIndex() (uint64, error) {
	readDB, err := sm.leaser.DB()
	if err != nil {
		return 0, err
	}
	defer readDB.Close()
	return sm.getLastAppliedIndex(readDB)
}

func (sm *Replica) getLastAppliedIndex(db ReplicaReader) (uint64, error) {
	val, err := sm.lookup(db, constants.LastAppliedIndexKey)
	if err != nil {
		if status.IsNotFoundError(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	i := bytesToUint64(val)
	return i, nil
}

func (sm *Replica) getPartitionMetadatas(db ReplicaReader) (*rfpb.PartitionMetadatas, error) {
	val, err := sm.lookup(db, constants.PartitionMetadatasKey)
	if err != nil {
		if status.IsNotFoundError(err) {
			return &rfpb.PartitionMetadatas{}, nil
		}
		return nil, err
	}
	var pm rfpb.PartitionMetadatas
	if err := proto.Unmarshal(val, &pm); err != nil {
		return nil, err
	}
	return &pm, nil
}

type fileRecordOp int

const (
	fileRecordAdd fileRecordOp = iota
	fileRecordDelete
)

func (sm *Replica) flushPartitionMetadatas(wb pebble.Batch) error {
	sm.partitionMetadataMu.Lock()
	defer sm.partitionMetadataMu.Unlock()
	var cs rfpb.PartitionMetadatas
	for _, pm := range sm.partitionMetadata {
		cs.Metadata = append(cs.Metadata, pm)
	}
	bs, err := proto.Marshal(&cs)
	if err != nil {
		return err
	}
	if err := sm.rangeCheckedSet(wb, constants.PartitionMetadatasKey, bs); err != nil {
		return err
	}
	return nil
}

func (sm *Replica) updatePartitionMetadata(wb pebble.Batch, key, val []byte, fileMetadata *rfpb.FileMetadata, op fileRecordOp) error {
	if fileMetadata == nil {
		fileMetadata = &rfpb.FileMetadata{}
		if err := proto.Unmarshal(val, fileMetadata); err != nil {
			return err
		}
	}
	sm.partitionMetadataMu.Lock()
	defer sm.partitionMetadataMu.Unlock()

	partID := fileMetadata.GetFileRecord().GetIsolation().GetPartitionId()
	pm, ok := sm.partitionMetadata[partID]
	if !ok {
		pm = &rfpb.PartitionMetadata{PartitionId: partID}
		sm.partitionMetadata[partID] = pm
	}

	if op == fileRecordDelete {
		pm.TotalCount--
		pm.SizeBytes -= fileMetadata.GetStoredSizeBytes()
	} else {
		readDB, err := sm.leaser.DB()
		if err != nil {
			return err
		}
		defer readDB.Close()
		_, closer, err := readDB.Get(key)
		if err == nil {
			// Skip increment on duplicate write.
			return closer.Close()
		}
		if err != pebble.ErrNotFound {
			return err
		}
		pm.TotalCount++
		pm.SizeBytes += fileMetadata.GetStoredSizeBytes()
	}
	return nil
}

func (sm *Replica) updateAndFlushPartitionMetadatas(wb pebble.Batch, key, val []byte, fileMetadata *rfpb.FileMetadata, op fileRecordOp) error {
	if err := sm.updatePartitionMetadata(wb, key, val, fileMetadata, op); err != nil {
		return err
	}
	return sm.flushPartitionMetadatas(wb)
}

type ReplicaReader interface {
	pebble.Reader
	io.Closer
}

type ReplicaWriter interface {
	pebble.Writer
	io.Closer

	// Would prefer to just use pebble.Writer here but the interface offers
	// no functionality for actually creating a new batch, so we amend it.
	NewBatch() pebble.Batch
	NewIndexedBatch() pebble.Batch
	NewSnapshot() *pebble.Snapshot
}

// checkLocks checks if any keys in this batch are already locked and returns an
// error if so: the txn must abort.
func (sm *Replica) checkLocks(wb pebble.Batch, txid []byte) error {
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, _, _, _ := batchReader.Next()
		keyString := string(ukey)
		lockingTxid, ok := sm.lockedKeys[keyString]
		if ok && !bytes.Equal(txid, lockingTxid) {
			return status.UnavailableErrorf("[%s] Conflict on key %q, locked by %q", sm.name(), keyString, string(lockingTxid))
		}
	}
	return nil
}

// acquireLocks locks all of the keys in batch `wb`.
func (sm *Replica) acquireLocks(wb pebble.Batch, txid []byte) {
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, _, _, _ := batchReader.Next()
		keyString := string(ukey)
		sm.lockedKeys[keyString] = append(make([]byte, 0, len(txid)), txid...)
	}
}

// releaseLocks unlocks all the keys in the batch `wb` locked with `txid`. If a
// row in `wb` is locked by another transaction, an error is logged and the row
// lock is left unchanged.
func (sm *Replica) releaseLocks(wb pebble.Batch, txid []byte) {
	// Unlock all the keys in this batch.
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, _, _, _ := batchReader.Next()
		keyString := string(ukey)
		lockingTxid, ok := sm.lockedKeys[keyString]
		if !ok || !bytes.Equal(txid, lockingTxid) {
			sm.log.Errorf("Key %q was not locked by %q; should have been", keyString, string(txid))
		} else {
			delete(sm.lockedKeys, keyString)
		}
	}
}

func (sm *Replica) loadTxnIntoMemory(txid []byte, batchReq *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	txn := db.NewIndexedBatch()

	// Ensure the txn is cleaned up if not prepared succesfully.
	loaded := false
	defer func() {
		if !loaded {
			txn.Close()
		}
	}()

	// Run all the propose commands against the txn batch.
	batchRsp := &rfpb.BatchCmdResponse{}
	for _, union := range batchReq.GetUnion() {
		rsp := sm.handlePropose(txn, union)
		if err := gstatus.FromProto(rsp.GetStatus()).Err(); err != nil {
			// An "normal" error could be returned here if a cas()
			// request finds a value it does not expect. In this
			// case we want the transaction to fail (in the prepare
			// step).
			return nil, err
		}
		batchRsp.Union = append(batchRsp.Union, rsp)
	}

	// Check if there are any locked keys conflicting.
	if err := sm.checkLocks(txn, txid); err != nil {
		return nil, err
	}

	// If not, acquire locks for all changed keys.
	sm.acquireLocks(txn, txid)

	// Save the txn batch in memory.
	sm.prepared[string(txid)] = txn
	loaded = true
	return batchRsp, nil
}

// PrepareTransaction processes all of the writes from `req` into a new batch
// and attempts to lock all keys modified in the batch. If any error is
// encountered, an error is returned; otherwise the batch is retained in memory
// so it can be applied or reverted via CommitTransaction or
// RollbackTransaction.
func (sm *Replica) PrepareTransaction(wb pebble.Batch, txid []byte, batchReq *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	// Save the txn batch in memory and acquire locks.
	batchRsp, err := sm.loadTxnIntoMemory(txid, batchReq)
	if err != nil {
		return nil, err
	}

	buf, err := proto.Marshal(batchReq)
	if err != nil {
		return nil, err
	}

	// Save the txn batch on-disk in case of a restart.
	txKey := keys.MakeKey(constants.LocalTransactionPrefix, txid)
	wb.Set(sm.replicaLocalKey(txKey), buf, nil /*ignored write options*/)

	return batchRsp, nil
}

func (sm *Replica) CommitTransaction(txid []byte) error {
	txn, ok := sm.prepared[string(txid)]
	if !ok {
		return status.NotFoundErrorf("%s: [%s] txid=%q", constants.TxnNotFoundMessage, sm.name(), txid)
	}
	defer txn.Close()
	delete(sm.prepared, string(txid))

	sm.releaseLocks(txn, txid)

	txKey := keys.MakeKey(constants.LocalTransactionPrefix, txid)
	txKey = sm.replicaLocalKey(txKey)

	// Lookup our request so that post-commit hooks can be applied, then
	// delete it from the batch, since the txn is being committed.
	batchReq := &rfpb.BatchCmdRequest{}
	iter, err := txn.NewIter(nil /*default iterOptions*/)
	if err != nil {
		return err
	}
	defer iter.Close()
	if err := pebble.LookupProto(iter, txKey, batchReq); err != nil {
		return err
	}
	txn.Delete(txKey, nil /*ignore write options*/)

	if err := txn.Commit(pebble.Sync); err != nil {
		return err
	}
	sm.updateInMemoryState(txn)

	// Run post commit hooks, if any are set.
	for _, hook := range batchReq.GetPostCommitHooks() {
		sm.handlePostCommit(hook)
	}
	return nil
}

func (sm *Replica) RollbackTransaction(txid []byte) error {
	txn, ok := sm.prepared[string(txid)]
	if !ok {
		return status.NotFoundErrorf("%s: [%s] txid=%q", constants.TxnNotFoundMessage, sm.name(), txid)
	}
	defer txn.Close()
	delete(sm.prepared, string(txid))

	sm.releaseLocks(txn, txid)

	txn.Reset()
	txKey := keys.MakeKey(constants.LocalTransactionPrefix, txid)
	txn.Delete(sm.replicaLocalKey(txKey), nil /*ignore write options*/)

	if err := txn.Commit(pebble.Sync); err != nil {
		return err
	}
	sm.updateInMemoryState(txn)
	return nil
}

func (sm *Replica) loadInflightTransactions(db ReplicaReader) error {
	iterOpts := &pebble.IterOptions{
		LowerBound: constants.LocalPrefix,
		UpperBound: constants.MetaRangePrefix,
	}
	iter, err := db.NewIter(iterOpts)
	if err != nil {
		return err
	}
	defer iter.Close()

	prefix := sm.replicaPrefix()
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			// Skip keys that are not ours.
			continue
		}
		key := iter.Key()[len(prefix):]
		if !bytes.HasPrefix(key, constants.LocalTransactionPrefix) {
			// Skip non-inflight-transactions
			continue
		}
		txid := key[len(constants.LocalTransactionPrefix):]

		batchReq := &rfpb.BatchCmdRequest{}
		if err := proto.Unmarshal(iter.Value(), batchReq); err != nil {
			return err
		}
		sm.log.Warningf("txid: %q, batchReq: %+v", txid, batchReq)
		if _, err := sm.loadTxnIntoMemory(txid, batchReq); err != nil {
			return err
		}
	}
	return nil
}

func (sm *Replica) loadPartitionMetadata(db ReplicaReader) error {
	pms, err := sm.getPartitionMetadatas(db)
	if err != nil {
		return err
	}
	sm.partitionMetadataMu.Lock()
	defer sm.partitionMetadataMu.Unlock()

	for _, pm := range pms.GetMetadata() {
		sm.partitionMetadata[pm.GetPartitionId()] = pm
	}
	return nil
}

func (sm *Replica) loadRangeDescriptor(db ReplicaReader) {
	buf, err := sm.lookup(db, constants.LocalRangeKey)
	if err != nil {
		sm.log.Debugf("Replica opened but range not yet set: %s", err)
		return
	}
	sm.setRange(constants.LocalRangeKey, buf)
}

func (sm *Replica) loadRangeLease(db ReplicaReader) {
	buf, err := sm.lookup(db, constants.LocalRangeLeaseKey)
	if err != nil {
		return
	}
	sm.setRangeLease(constants.LocalRangeLeaseKey, buf)
}

// clearInMemoryReplicaState clears in-memory replica state.
func (sm *Replica) clearInMemoryReplicaState() {
	sm.rangeMu.Lock()
	sm.rangeDescriptor = nil
	sm.mappedRange = nil
	sm.rangeMu.Unlock()
	sm.leaseMu.Lock()
	sm.rangeLease = nil
	sm.leaseMu.Unlock()

	sm.prepared = make(map[string]pebble.Batch)
	sm.lockedKeys = make(map[string][]byte)
	sm.lastAppliedIndex = 0

	sm.partitionMetadataMu.Lock()
	sm.partitionMetadata = make(map[string]*rfpb.PartitionMetadata)
	sm.partitionMetadataMu.Unlock()
}

// clearReplica clears in-memory replica state, and data (both in local range and
// in the range specified by range descriptor) on the disk.
func (sm *Replica) clearReplica(db ReplicaWriter) error {
	// Remove range from the store
	sm.rangeMu.Lock()
	rangeDescriptor := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	if sm.store != nil && rangeDescriptor != nil {
		sm.store.RemoveRange(rangeDescriptor, sm)
	}

	wb := db.NewIndexedBatch()

	start, end := keys.Range(sm.replicaPrefix())
	if err := wb.DeleteRange(start, end, nil /*ignored write options*/); err != nil {
		return err
	}
	if rangeDescriptor != nil && rangeDescriptor.GetStart() != nil && rangeDescriptor.GetEnd() != nil {

		if err := wb.DeleteRange(rangeDescriptor.GetStart(), rangeDescriptor.GetEnd(), nil /*ignored write options*/); err != nil {
			return err
		}
	}
	if err := wb.Commit(pebble.Sync); err != nil {
		return err
	}

	sm.clearInMemoryReplicaState()
	return nil
}

// loadReplicaState loads any in-memory replica state from the DB.
func (sm *Replica) loadReplicaState(db ReplicaReader) error {
	sm.loadRangeDescriptor(db)
	sm.loadRangeLease(db)
	if err := sm.loadPartitionMetadata(db); err != nil {
		return err
	}
	if err := sm.loadInflightTransactions(db); err != nil {
		return err
	}
	lastStoredIndex, err := sm.getLastAppliedIndex(db)
	if err != nil {
		return err
	}
	if sm.lastAppliedIndex > lastStoredIndex {
		return status.FailedPreconditionErrorf("[%s] last applied not moving forward: %d > %d", sm.name(), sm.lastAppliedIndex, lastStoredIndex)
	}
	sm.lastAppliedIndex = lastStoredIndex
	sm.log.Infof("[%s] loaded replica state, lastAppliedIndex=%d", sm.name(), sm.lastAppliedIndex)
	return nil
}

// Open opens the existing on disk state machine to be used or it creates a
// new state machine with empty state if it does not exist. Open returns the
// most recent index value of the Raft log that has been persisted, or it
// returns 0 when the state machine is a new one.
//
// The provided read only chan struct{} channel is used to notify the Open
// method that the node has been stopped and the Open method can choose to
// abort by returning an ErrOpenStopped error.
//
// Open is called shortly after the Raft node is started. The Update method
// and the Lookup method will not be called before the completion of the Open
// method.
func (sm *Replica) Open(stopc <-chan struct{}) (uint64, error) {
	db, err := sm.leaser.DB()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	sm.quitChan = make(chan struct{})
	if err := sm.loadReplicaState(db); err != nil {
		return 0, err
	}
	return sm.lastAppliedIndex, nil
}

func (sm *Replica) directWrite(wb pebble.Batch, req *rfpb.DirectWriteRequest) (*rfpb.DirectWriteResponse, error) {
	kv := req.GetKv()
	err := sm.rangeCheckedSet(wb, kv.Key, kv.Value)
	return &rfpb.DirectWriteResponse{}, err
}

func (sm *Replica) directDelete(wb pebble.Batch, req *rfpb.DirectDeleteRequest) (*rfpb.DirectDeleteResponse, error) {
	if !isLocalKey(req.GetKey()) {
		return nil, status.InvalidArgumentErrorf("[%s] cannot direct delete non-local key; use Delete instead", sm.name())
	}
	key := sm.replicaLocalKey(req.GetKey())
	err := wb.Delete(key, nil /* ignore write options*/)
	return &rfpb.DirectDeleteResponse{}, err
}

func (sm *Replica) directRead(db ReplicaReader, req *rfpb.DirectReadRequest) (*rfpb.DirectReadResponse, error) {
	buf, err := sm.lookup(db, req.GetKey())
	if err != nil {
		return nil, err
	}
	rsp := &rfpb.DirectReadResponse{
		Kv: &rfpb.KV{
			Key:   req.GetKey(),
			Value: buf,
		},
	}
	return rsp, nil
}

func (sm *Replica) increment(wb pebble.Batch, req *rfpb.IncrementRequest) (*rfpb.IncrementResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.InvalidArgumentError("Increment requires a valid key.")
	}
	buf, err := pebble.GetCopy(wb, req.GetKey())
	if err != nil {
		if !status.IsNotFoundError(err) {
			return nil, err
		}
	}
	var val uint64
	if status.IsNotFoundError(err) {
		val = 0
	} else {
		val = bytesToUint64(buf)
	}
	val += req.GetDelta()

	if err := wb.Set(req.GetKey(), uint64ToBytes(val), nil /*ignored write options*/); err != nil {
		return nil, err
	}
	return &rfpb.IncrementResponse{
		Key:   req.GetKey(),
		Value: val,
	}, nil
}

func (sm *Replica) cas(wb pebble.Batch, req *rfpb.CASRequest) (*rfpb.CASResponse, error) {
	kv := req.GetKv()
	var buf []byte
	var err error
	buf, err = sm.lookup(wb, kv.GetKey())
	if err != nil && !status.IsNotFoundError(err) {
		return nil, err
	}

	// Match: set value and return new value + no error.
	if bytes.Equal(buf, req.GetExpectedValue()) {
		err := sm.rangeCheckedSet(wb, kv.Key, kv.Value)
		if err == nil {
			return &rfpb.CASResponse{Kv: kv}, nil
		}
		return nil, err
	}

	// No match: return old value and error.
	return &rfpb.CASResponse{
		Kv: &rfpb.KV{
			Key:   kv.GetKey(),
			Value: buf,
		},
	}, status.FailedPreconditionError(constants.CASErrorMessage)
}

func absInt(i int64) int64 {
	if i < 0 {
		return -1 * i
	}
	return i
}

func (sm *Replica) findSplitPoint() (*rfpb.FindSplitPointResponse, error) {
	sm.rangeMu.Lock()
	rangeDescriptor := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	iterOpts := &pebble.IterOptions{
		LowerBound: rangeDescriptor.GetStart(),
		UpperBound: rangeDescriptor.GetEnd(),
	}

	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	iter, err := db.NewIter(iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	totalSize := int64(0)
	totalRows := 0
	for iter.First(); iter.Valid(); iter.Next() {
		sizeBytes, err := sizeOf(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		totalSize += sizeBytes
		totalRows += 1
	}

	optimalSplitSize := totalSize / 2
	minSplitSize := totalSize / 5
	maxSplitSize := totalSize - minSplitSize

	leftSize := int64(0)
	leftRows := 0
	var lastKey []byte

	splitSize := int64(0)
	splitRows := 0
	var splitKey []byte

	for iter.First(); iter.Valid(); iter.Next() {
		if canSplitKeys(lastKey, iter.Key()) {
			splitDistance := absInt(optimalSplitSize - leftSize)
			bestSplitDistance := absInt(optimalSplitSize - splitSize)
			if leftRows > 2 && leftSize > minSplitSize && leftSize < maxSplitSize && splitDistance < bestSplitDistance {
				if len(splitKey) != len(lastKey) {
					splitKey = make([]byte, len(lastKey))
				}
				copy(splitKey, lastKey)
				splitSize = leftSize
				splitRows = leftRows
			}
		}
		size, err := sizeOf(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		leftSize += size
		leftRows += 1
		if len(lastKey) != len(iter.Key()) {
			lastKey = make([]byte, len(iter.Key()))
		}
		copy(lastKey, iter.Key())
	}

	if splitKey == nil {
		sm.printRange(db, iterOpts, "unsplittable range")
		return nil, status.NotFoundErrorf("[%s] Could not find split point. (Total size: %d, start split size: %d", sm.name(), totalSize, leftSize)
	}
	sm.log.Debugf("Cluster %d found split @ %q start rows: %d, size: %d, end rows: %d, size: %d", sm.rangeID, splitKey, splitRows, splitSize, totalRows-splitRows, totalSize-splitSize)
	return &rfpb.FindSplitPointResponse{
		SplitKey: splitKey,
	}, nil
}

func canSplitKeys(startKey, endKey []byte) bool {
	if len(startKey) == 0 || len(endKey) == 0 {
		return false
	}
	// Disallow splitting the metarange, or any range before '\x04'.
	splitStart := []byte{constants.UnsplittableMaxByte}
	if bytes.Compare(endKey, splitStart) <= 0 {
		// start-end is before splitStart
		return false
	}
	if bytes.Compare(startKey, splitStart) <= 0 && bytes.Compare(endKey, splitStart) > 0 {
		// start-end crosses splitStart boundary
		return false
	}

	// Disallow splitting pebble file-metadata from stored-file-data.
	// File mdata will have a key like /foo/bar/baz
	// File data will have a key like /foo/bar/baz-{1..n}
	if bytes.HasPrefix(endKey, startKey) {
		log.Debugf("can't split between %q and %q, prefix match", startKey, endKey)
		return false
	}

	return true
}

func (sm *Replica) printRange(r pebble.Reader, iterOpts *pebble.IterOptions, tag string) {
	iter, err := r.NewIter(iterOpts)
	if err != nil {
		sm.log.Errorf("Error creating pebble iter: %s", err)
		return
	}
	defer iter.Close()

	totalSize := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		size, err := sizeOf(iter.Key(), iter.Value())
		if err != nil {
			sm.log.Errorf("Error computing size of %s: %s", iter.Key(), err)
			continue
		}
		totalSize += size
		sm.log.Infof("%q: key: %q (%d)", tag, iter.Key(), totalSize)
	}
}

func (sm *Replica) scan(db ReplicaReader, req *rfpb.ScanRequest) (*rfpb.ScanResponse, error) {
	if len(req.GetStart()) == 0 {
		return nil, status.InvalidArgumentError("Scan requires a valid key.")
	}

	start := sm.replicaLocalKey(req.GetStart())
	iterOpts := &pebble.IterOptions{}
	if req.GetEnd() != nil {
		iterOpts.UpperBound = sm.replicaLocalKey(req.GetEnd())
	} else {
		iterOpts.UpperBound = sm.replicaLocalKey(keys.Key(req.GetStart()).Next())
	}

	iter, err := db.NewIter(iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var t bool

	switch req.GetScanType() {
	case rfpb.ScanRequest_SEEKLT_SCAN_TYPE:
		t = iter.SeekLT(start)
	case rfpb.ScanRequest_SEEKGE_SCAN_TYPE:
		t = iter.SeekGE(start)
	case rfpb.ScanRequest_SEEKGT_SCAN_TYPE:
		t = iter.SeekGE(start)
		// If the iter's current key is *equal* to start, go to the next
		// key greater than this one.
		if t && bytes.Equal(iter.Key(), start) {
			t = iter.Next()
		}
	default:
		t = iter.SeekGE(start)
	}

	prefix := sm.replicaPrefix()
	rsp := &rfpb.ScanResponse{}
	for ; t; t = iter.Next() {
		key := bytes.TrimPrefix(iter.Key(), prefix)

		k := make([]byte, len(key))
		copy(k, key)
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())
		rsp.Kvs = append(rsp.Kvs, &rfpb.KV{
			Key:   k,
			Value: v,
		})
		if req.GetLimit() != 0 && len(rsp.GetKvs()) == int(req.GetLimit()) {
			break
		}
	}
	return rsp, nil
}

func (sm *Replica) get(db ReplicaReader, req *rfpb.GetRequest) (*rfpb.GetResponse, error) {
	// Check that key is a valid PebbleKey.
	var pk filestore.PebbleKey
	if _, err := pk.FromBytes(req.GetKey()); err != nil {
		return nil, err
	}

	iter, err := db.NewIter(nil /*default iterOptions*/)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	fileMetadata, err := lookupFileMetadata(iter, req.GetKey())
	if err != nil {
		return nil, err
	}
	return &rfpb.GetResponse{
		FileMetadata: fileMetadata,
	}, nil
}

func (sm *Replica) set(wb pebble.Batch, req *rfpb.SetRequest) (*rfpb.SetResponse, error) {
	// Check that key is a valid PebbleKey.
	var pk filestore.PebbleKey
	if _, err := pk.FromBytes(req.GetKey()); err != nil {
		return nil, err
	}
	// Check that value is non-nil.
	if req.GetFileMetadata() == nil {
		return nil, status.InvalidArgumentErrorf("Invalid (nil) FileMetadata for key %q", req.GetKey())
	}
	buf, err := proto.Marshal(req.GetFileMetadata())
	if err != nil {
		return nil, err
	}
	if err := sm.rangeCheckedSet(wb, req.GetKey(), buf); err != nil {
		return nil, err
	}
	return &rfpb.SetResponse{}, nil
}

func (sm *Replica) delete(wb pebble.Batch, req *rfpb.DeleteRequest) (*rfpb.DeleteResponse, error) {
	// Check that key is a valid PebbleKey.
	var pk filestore.PebbleKey
	if _, err := pk.FromBytes(req.GetKey()); err != nil {
		return nil, err
	}
	iter, err := wb.NewIter(nil /*default iter options*/)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	fileMetadata, err := lookupFileMetadata(iter, req.GetKey())
	if err != nil {
		if status.IsNotFoundError(err) {
			return &rfpb.DeleteResponse{}, nil
		}
		return nil, err
	}
	if req.GetMatchAtime() != 0 && req.GetMatchAtime() != fileMetadata.GetLastAccessUsec() {
		return nil, status.FailedPreconditionError("Atime mismatch")
	}
	if err := sm.fileStorer.DeleteStoredFile(context.TODO(), sm.fileDir, fileMetadata.GetStorageMetadata()); err != nil {
		return nil, err
	}
	if err := wb.Delete(req.GetKey(), nil /*ignored write options*/); err != nil {
		return nil, err
	}
	if err := sm.updateAndFlushPartitionMetadatas(wb, req.GetKey(), iter.Value(), fileMetadata, fileRecordDelete); err != nil {
		return nil, err
	}
	return &rfpb.DeleteResponse{}, nil
}

func (sm *Replica) find(db ReplicaReader, req *rfpb.FindRequest) (*rfpb.FindResponse, error) {
	// Check that key is a valid PebbleKey.
	var pk filestore.PebbleKey
	if _, err := pk.FromBytes(req.GetKey()); err != nil {
		return nil, err
	}

	iter, err := db.NewIter(nil /*default iterOptions*/)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	fileMetadata, err := lookupFileMetadata(iter, req.GetKey())
	present := (err == nil)

	return &rfpb.FindResponse{
		Present:        present,
		LastAccessUsec: fileMetadata.GetLastAccessUsec(),
	}, nil
}

func (sm *Replica) updateAtime(wb pebble.Batch, req *rfpb.UpdateAtimeRequest) (*rfpb.UpdateAtimeResponse, error) {
	// Check that key is a valid PebbleKey.
	var pk filestore.PebbleKey
	if _, err := pk.FromBytes(req.GetKey()); err != nil {
		return nil, err
	}

	buf, err := sm.lookup(wb, req.GetKey())
	if err != nil {
		return nil, err
	}
	fileMetadata := &rfpb.FileMetadata{}
	if err := proto.Unmarshal(buf, fileMetadata); err != nil {
		return nil, err
	}
	fileMetadata.LastAccessUsec = req.GetAccessTimeUsec()
	buf, err = proto.Marshal(fileMetadata)
	if err != nil {
		return nil, err
	}
	if err := sm.rangeCheckedSet(wb, req.GetKey(), buf); err != nil {
		return nil, err
	}
	return &rfpb.UpdateAtimeResponse{}, nil
}

func (sm *Replica) deleteSessions(wb pebble.Batch, req *rfpb.DeleteSessionsRequest) (*rfpb.DeleteSessionsResponse, error) {
	start, end := keys.Range(sm.replicaLocalKey(constants.LocalSessionPrefix))
	iterOpts := &pebble.IterOptions{
		UpperBound: end,
	}
	iter, err := wb.NewIter(iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	t := iter.SeekGE(start)
	for ; t; t = iter.Next() {
		session := &rfpb.Session{}
		if err := proto.Unmarshal(iter.Value(), session); err != nil {
			sm.log.Errorf("deleteSessions unable to parse value of key(%q): %s", string(iter.Key()), err)
			continue
		}
		if session.GetCreatedAtUsec() <= req.GetCreatedAtUsec() {
			wb.Delete(iter.Key(), nil /* ignore write options */)
		}
	}
	return &rfpb.DeleteSessionsResponse{}, nil
}

func statusProto(err error) *statuspb.Status {
	s, _ := gstatus.FromError(err)
	return s.Proto()
}

func (sm *Replica) handlePostCommit(hook *rfpb.PostCommitHook) {
	if snap := hook.GetSnapshotCluster(); snap != nil {
		go func() {
			if err := sm.store.SnapshotCluster(context.TODO(), sm.rangeID); err != nil {
				sm.log.Errorf("Error processing post-commit hook: %s", err)
			}
		}()
	}
}

func (sm *Replica) handlePropose(wb pebble.Batch, req *rfpb.RequestUnion) *rfpb.ResponseUnion {
	rsp := &rfpb.ResponseUnion{}

	switch value := req.Value.(type) {
	case *rfpb.RequestUnion_DirectWrite:
		r, err := sm.directWrite(wb, value.DirectWrite)
		rsp.Value = &rfpb.ResponseUnion_DirectWrite{
			DirectWrite: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_DirectDelete:
		r, err := sm.directDelete(wb, value.DirectDelete)
		rsp.Value = &rfpb.ResponseUnion_DirectDelete{
			DirectDelete: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Increment:
		r, err := sm.increment(wb, value.Increment)
		rsp.Value = &rfpb.ResponseUnion_Increment{
			Increment: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Cas:
		r, err := sm.cas(wb, value.Cas)
		rsp.Value = &rfpb.ResponseUnion_Cas{
			Cas: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Set:
		r, err := sm.set(wb, value.Set)
		rsp.Value = &rfpb.ResponseUnion_Set{
			Set: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Delete:
		r, err := sm.delete(wb, value.Delete)
		rsp.Value = &rfpb.ResponseUnion_Delete{
			Delete: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_UpdateAtime:
		r, err := sm.updateAtime(wb, value.UpdateAtime)
		rsp.Value = &rfpb.ResponseUnion_UpdateAtime{
			UpdateAtime: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_DeleteSessions:
		r, err := sm.deleteSessions(wb, value.DeleteSessions)
		rsp.Value = &rfpb.ResponseUnion_DeleteSessions{
			DeleteSessions: r,
		}
		rsp.Status = statusProto(err)
	default:
		rsp.Status = statusProto(status.UnimplementedErrorf("SyncPropose handling for %+v not implemented.", req))
	}

	if req.GetCas() == nil && rsp.GetStatus().GetCode() != 0 {
		// Log Update() errors (except Compare-And-Set) errors.
		sm.log.Errorf("error processing update %+v: %s", req, rsp.GetStatus())
	}

	return rsp
}

func (sm *Replica) handleRead(db ReplicaReader, req *rfpb.RequestUnion) *rfpb.ResponseUnion {
	sm.readQPS.Inc()
	rsp := &rfpb.ResponseUnion{}

	switch value := req.Value.(type) {
	case *rfpb.RequestUnion_DirectRead:
		r, err := sm.directRead(db, value.DirectRead)
		rsp.Value = &rfpb.ResponseUnion_DirectRead{
			DirectRead: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Scan:
		r, err := sm.scan(db, value.Scan)
		rsp.Value = &rfpb.ResponseUnion_Scan{
			Scan: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Get:
		r, err := sm.get(db, value.Get)
		rsp.Value = &rfpb.ResponseUnion_Get{
			Get: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_Find:
		r, err := sm.find(db, value.Find)
		rsp.Value = &rfpb.ResponseUnion_Find{
			Find: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_FindSplitPoint:
		r, err := sm.findSplitPoint()
		rsp.Value = &rfpb.ResponseUnion_FindSplitPoint{
			FindSplitPoint: r,
		}
		rsp.Status = statusProto(err)
	default:
		rsp.Status = statusProto(status.UnimplementedErrorf("Read handling for %+v not implemented.", req))
	}
	return rsp
}

func lookupFileMetadata(iter pebble.Iterator, fileMetadataKey []byte) (*rfpb.FileMetadata, error) {
	fileMetadata := &rfpb.FileMetadata{}
	if err := pebble.LookupProto(iter, fileMetadataKey, fileMetadata); err != nil {
		return nil, err
	}
	return fileMetadata, nil
}

func validateHeaderAgainstRange(rd *rfpb.RangeDescriptor, header *rfpb.Header) error {
	if rd == nil {
		return status.FailedPreconditionError("range descriptor is not set")
	}
	if rd.GetGeneration() != header.GetGeneration() {
		return status.OutOfRangeErrorf("%s: id %d generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetRangeId(), rd.GetGeneration(), header.GetGeneration())
	}
	return nil
}

var digestRunes = []rune("abcdef1234567890")

func (sm *Replica) updateInMemoryState(wb pebble.Batch) {
	// Update the local in-memory range descriptor iff this batch modified
	// it.
	if buf, ok := sm.batchContainsKey(wb, constants.LocalRangeKey); ok {
		sm.setRange(constants.LocalRangeKey, buf)
	}
	// Update the rangelease iff this batch sets it.
	if buf, ok := sm.batchContainsKey(wb, constants.LocalRangeLeaseKey); ok {
		sm.setRangeLease(constants.LocalRangeLeaseKey, buf)
	}

}

func errorEntry(err error) dbsm.Result {
	status := statusProto(err)
	rspBuf, _ := proto.Marshal(status)
	return dbsm.Result{
		Value: constants.EntryErrorValue,
		Data:  rspBuf,
	}
}

func (sm *Replica) getLastRespFromSession(db ReplicaReader, reqSession *rfpb.Session) ([]byte, error) {
	if reqSession == nil {
		return nil, nil
	}
	sessionKey := keys.MakeKey(constants.LocalSessionPrefix, reqSession.GetId())
	buf, err := sm.lookup(db, sessionKey)
	if err != nil {
		if status.IsNotFoundError(err) {
			// This is a new request
			return nil, nil
		}
		return nil, err
	}
	storedSession := &rfpb.Session{}
	if err := proto.Unmarshal(buf, storedSession); err != nil {
		return nil, err
	}
	if storedSession.GetIndex() == reqSession.GetIndex() {
		return storedSession.GetRspData(), nil
	}
	if storedSession.GetIndex() > reqSession.GetIndex() {
		return nil, status.InternalErrorf("%s getLastRespFromSession session (id=%q) index mismatch: storedSession (Index=%d, EntryIndex=%d) and reqSession(Index=%d, EntryIndex=%d) and last applied index=%d", sm.name(), storedSession.GetId(), storedSession.GetIndex(), storedSession.GetEntryIndex(), reqSession.GetIndex(), reqSession.GetEntryIndex(), sm.lastAppliedIndex)
	}
	// This is a new request.
	return nil, nil
}

func getEntryResult(cmd []byte, rspBuf []byte) dbsm.Result {
	return dbsm.Result{
		Value: uint64(len(cmd)),
		Data:  rspBuf,
	}
}

func (sm *Replica) commitIndexBatch(wb pebble.Batch, entryIndex uint64) error {
	appliedIndex := uint64ToBytes(entryIndex)
	wb.Set(sm.replicaLocalKey(constants.LastAppliedIndexKey), appliedIndex, nil)
	if err := wb.Commit(pebble.NoSync); err != nil {
		return status.InternalErrorf("[%s] failed to commit batch: %s", sm.name(), err)
	}
	// If the batch commit was successful, update the replica's in-
	// memory state.
	sm.updateInMemoryState(wb)

	if sm.lastAppliedIndex >= entryIndex {
		sm.log.Errorf("[%s] lastAppliedIndex not moving forward: current %d, new: %d", sm.lastAppliedIndex, entryIndex)
	}
	sm.lastAppliedIndex = entryIndex
	return nil
}

func (sm *Replica) updateSession(wb pebble.Batch, reqSession *rfpb.Session, rspBuf []byte) error {
	reqSession.RspData = rspBuf
	sessionBuf, err := proto.Marshal(reqSession)
	if err != nil {
		return status.InternalErrorf("[%s] failed to marshal session: %s", sm.name(), err)
	}
	sessionKey := keys.MakeKey(constants.LocalSessionPrefix, reqSession.GetId())
	wb.Set(sm.replicaLocalKey(sessionKey), sessionBuf, nil)
	return nil
}

func (sm *Replica) singleUpdate(db pebble.IPebbleDB, entry dbsm.Entry) (dbsm.Entry, error) {
	// This method should return errors if something truly fails in an
	// unrecoverable way (proto marshal/unmarshal, pebble batch commit) but
	// otherwise normal request handling errors are encoded in the response
	// and the statemachine keeps progressing.
	batchReq := &rfpb.BatchCmdRequest{}
	if err := proto.Unmarshal(entry.Cmd, batchReq); err != nil {
		return entry, status.InternalErrorf("[%s] failed to unmarshal entry.Cmd: %s", sm.name(), err)
	}

	// All of the data in a BatchCmdRequest is handled in a single pebble
	// write batch. That means that if any part of a batch update fails,
	// none of the batch will be applied.
	wb := db.NewIndexedBatch()
	defer wb.Close()

	reqSession := batchReq.GetSession()
	if reqSession != nil {
		reqSession.EntryIndex = proto.Uint64(entry.Index)
	}
	lastRspData, err := sm.getLastRespFromSession(db, reqSession)
	if err != nil {
		return entry, err
	}
	// We have executed this command in the past, return the stored response and
	// skip execution.
	if lastRspData != nil {
		entry.Result = getEntryResult(entry.Cmd, lastRspData)
		if err := sm.commitIndexBatch(wb, entry.Index); err != nil {
			return entry, err
		}
		return entry, nil
	}

	sm.rangeMu.RLock()
	rd := sm.rangeDescriptor
	sm.rangeMu.RUnlock()

	// Increment QPS counters.
	rangeID := rd.GetRangeId()
	metrics.RaftProposals.With(prometheus.Labels{
		metrics.RaftRangeIDLabel: strconv.Itoa(int(rangeID)),
	}).Inc()
	sm.raftProposeQPS.Inc()

	batchRsp := &rfpb.BatchCmdResponse{}
	if header := batchReq.GetHeader(); header != nil {
		if err := validateHeaderAgainstRange(rd, header); err != nil {
			entry.Result = errorEntry(err)
			return entry, nil
		}
	}
	if txid := batchReq.GetTransactionId(); len(txid) > 0 {
		// Check that request is not malformed.
		if len(batchReq.GetUnion()) > 0 && batchReq.GetFinalizeOperation() != rfpb.FinalizeOperation_UNKNOWN_OPERATION {
			batchRsp.Status = statusProto(status.InvalidArgumentErrorf("Batch must be empty when finalizing transaction"))
		}

		switch batchReq.GetFinalizeOperation() {
		case rfpb.FinalizeOperation_COMMIT:
			if err := sm.CommitTransaction(txid); err != nil {
				batchRsp.Status = statusProto(err)
			}
		case rfpb.FinalizeOperation_ROLLBACK:
			if err := sm.RollbackTransaction(txid); err != nil {
				batchRsp.Status = statusProto(err)
			}
		default:
			txnRsp, err := sm.PrepareTransaction(wb, txid, batchReq)
			if err != nil {
				batchRsp.Status = statusProto(err)
			} else {
				batchRsp = txnRsp
			}
		}
	} else {
		for _, union := range batchReq.GetUnion() {
			batchRsp.Union = append(batchRsp.Union, sm.handlePropose(wb, union))
		}
		if err := sm.checkLocks(wb, nil); err != nil {
			batchRsp.Status = statusProto(err)
			wb.Reset() // don't commit if conflict.
		}
	}

	rspBuf, err := proto.Marshal(batchRsp)
	if err != nil {
		return entry, status.InternalErrorf("[%s] failed to marshal batchRsp: %s", sm.name(), err)
	}
	entry.Result = getEntryResult(entry.Cmd, rspBuf)
	if reqSession != nil {
		sm.log.Debugf("[%s] save session %+v", sm.name(), reqSession)
		if err := sm.updateSession(wb, reqSession, rspBuf); err != nil {
			return entry, err
		}
	}

	if err := sm.commitIndexBatch(wb, entry.Index); err != nil {
		return entry, err
	}
	// Run post commit hooks, if any are set.
	for _, hook := range batchReq.GetPostCommitHooks() {
		sm.handlePostCommit(hook)
	}

	return entry, nil
}

// Update updates the IOnDiskStateMachine instance. The input Entry slice
// is a list of continuous proposed and committed commands from clients, they
// are provided together as a batch so the IOnDiskStateMachine implementation
// can choose to batch them and apply together to hide latency. Update returns
// the input entry slice with the Result field of all its members set.
//
// The read only Index field of each input Entry instance is the Raft log
// index of each entry, it is IOnDiskStateMachine's responsibility to
// atomically persist the Index value together with the corresponding state
// update.
//
// The Update method can choose to synchronize all of its in-core state with
// that on disk. This can minimize the number of committed Raft entries that
// need to be re-applied after reboot. Update can also choose to postpone such
// synchronization until the Sync method is invoked, this approach produces
// higher throughput during fault free running at the cost that some of the
// most recent Raft entries not synchronized onto disks will have to be
// re-applied after reboot.
//
// When the Update method does not synchronize its in-core state with that on
// disk, the implementation must ensure that after a reboot there is no
// applied entry in the State Machine more recent than any entry that was
// lost during reboot. For example, consider a state machine with 3 applied
// entries, let's assume their index values to be 1, 2 and 3. Once they have
// been applied into the state machine without synchronizing the in-core state
// with that on disk, it is okay to lose the data associated with the applied
// entry 3, but it is strictly forbidden to have the data associated with the
// applied entry 3 available in the state machine while the one with index
// value 2 got lost during reboot.
//
// The Update method must be deterministic, meaning given the same initial
// state of IOnDiskStateMachine and the same input sequence, it should reach
// to the same updated state and outputs the same results. The input entry
// slice should be the only input to this method. Reading from the system
// clock, random number generator or other similar external data sources will
// likely violate the deterministic requirement of the Update method.
//
// Concurrent calls to the Lookup method and the SaveSnapshot method are not
// blocked when the state machine is being updated by the Update method.
//
// The IOnDiskStateMachine implementation should not keep a reference to the
// input entry slice after return.
//
// Update returns an error when there is unrecoverable error when updating the
// on disk state machine.
func (sm *Replica) Update(entries []dbsm.Entry) ([]dbsm.Entry, error) {
	defer canary.Start("replica.Update", time.Second)()
	startTime := time.Now()
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, status.InternalErrorf("[%s] failed to get pebble DB from the leaser: %s", sm.name(), err)
	}
	defer db.Close()

	for i, entry := range entries {
		e, err := sm.singleUpdate(db, entry)
		if err != nil {
			return nil, status.InternalErrorf("[%s] failed to singleUpdate entry (index=%d): %s", sm.name(), entry.Index, err)
		}
		entries[i] = e
	}

	if sm.lastAppliedIndex-sm.lastUsageCheckIndex > uint64(*entriesBetweenUsageChecks) {
		usage, err := sm.Usage()
		if err != nil {
			sm.log.Warningf("Error computing usage: %s", err)
		} else {
			sm.rangeMu.RLock()
			rd := sm.rangeDescriptor
			sm.rangeMu.RUnlock()
			sm.notifyListenersOfUsage(rd, usage)
		}
		sm.lastUsageCheckIndex = sm.lastAppliedIndex
	}
	metrics.RaftReplicaUpdateDurationUs.With(prometheus.Labels{
		metrics.RaftRangeIDLabel: strconv.Itoa(int(sm.rangeID)),
	}).Observe(float64(time.Since(startTime).Microseconds()))
	return entries, nil
}

// Lookup queries the state of the IOnDiskStateMachine instance and returns
// the query result as an interface{}. The input interface{} specifies what to
// query, it is up to the IOnDiskStateMachine implementation to interpret such
// input. The returned interface{} contains the query result.
//
// When an error is returned by the Lookup method, the error will be passed
// to the caller to be handled. A typical scenario for returning an error is
// that the state machine has already been closed or aborted from a
// RecoverFromSnapshot procedure before Lookup is called.
//
// Concurrent calls to the Update and RecoverFromSnapshot method are not
// blocked when calls to the Lookup method are being processed.
//
// The IOnDiskStateMachine implementation should not keep any reference of
// the input interface{} after return.
//
// The Lookup method is a read only method, it should never change the state
// of IOnDiskStateMachine.
func (sm *Replica) Lookup(key interface{}) (interface{}, error) {
	defer canary.Start("replica.Lookup", time.Second)()
	reqBuf, ok := key.([]byte)
	if !ok {
		return nil, status.FailedPreconditionError("Cannot convert key to []byte")
	}

	batchReq := &rfpb.BatchCmdRequest{}
	if err := proto.Unmarshal(reqBuf, batchReq); err != nil {
		return nil, err
	}

	batchRsp, err := sm.BatchLookup(batchReq)
	if err != nil {
		return nil, err
	}

	rspBuf, err := proto.Marshal(batchRsp)
	if err != nil {
		return nil, err
	}
	return rspBuf, nil
}

func (sm *Replica) BatchLookup(batchReq *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	batchRsp := &rfpb.BatchCmdResponse{}

	if header := batchReq.GetHeader(); header != nil {
		sm.rangeMu.RLock()
		rd := sm.rangeDescriptor
		sm.rangeMu.RUnlock()

		if err := validateHeaderAgainstRange(rd, header); err != nil {
			return nil, err
		}
	}

	for _, req := range batchReq.GetUnion() {
		//sm.log.Debugf("Lookup: request union: %+v", req)
		rsp := sm.handleRead(db, req)
		// sm.log.Debugf("Lookup: response union: %+v", rsp)
		batchRsp.Union = append(batchRsp.Union, rsp)
	}

	return batchRsp, nil
}

// Sync synchronizes all in-core state of the state machine to persisted
// storage so the state machine can continue from its latest state after
// reboot.
//
// Sync is always invoked with mutual exclusion protection from the Update,
// PrepareSnapshot, RecoverFromSnapshot and Close methods.
//
// Sync returns an error when there is unrecoverable error for synchronizing
// the in-core state.
func (sm *Replica) Sync() error {
	db, err := sm.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()
	return db.LogData(nil, pebble.Sync)
}

// PrepareSnapshot prepares the snapshot to be concurrently captured and
// streamed. PrepareSnapshot is invoked before SaveSnapshot is called and it
// is always invoked with mutual exclusion protection from the Update, Sync,
// RecoverFromSnapshot and Close methods.
//
// PrepareSnapshot in general saves a state identifier of the current state,
// such state identifier can be a version number, a sequence number, a change
// ID or some other small in memory data structure used for describing the
// point in time state of the state machine. The state identifier is returned
// as an interface{} before being passed to the SaveSnapshot() method.
//
// PrepareSnapshot returns an error when there is unrecoverable error for
// preparing the snapshot.
func (sm *Replica) PrepareSnapshot() (interface{}, error) {
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	snap := db.NewSnapshot()
	return snap, nil
}

func encodeDataToWriter(w io.Writer, r io.Reader, msgLength int64) error {
	varintBuf := make([]byte, binary.MaxVarintLen64)
	varintSize := binary.PutVarint(varintBuf, msgLength)

	// Write a header-chunk to know how big the data coming is
	if _, err := w.Write(varintBuf[:varintSize]); err != nil {
		return err
	}
	if msgLength == 0 {
		return nil
	}

	n, err := io.Copy(w, r)
	if err != nil {
		return err
	}
	if int64(n) != msgLength {
		return status.FailedPreconditionError("wrote wrong number of bytes?")
	}
	return nil
}

func encodeKeyValue(w io.Writer, key, value []byte) error {
	kv := &rfpb.KV{
		Key:   key,
		Value: value,
	}
	protoBytes, err := proto.Marshal(kv)
	if err != nil {
		return err
	}
	protoLength := int64(len(protoBytes))
	return encodeDataToWriter(w, bytes.NewReader(protoBytes), protoLength)
}

func readDataFromReader(r *bufio.Reader) (io.Reader, int64, error) {
	count, err := binary.ReadVarint(r)
	if err != nil {
		return nil, 0, err
	}
	return io.LimitReader(r, count), count, nil
}

func (sm *Replica) saveRangeData(w io.Writer, snap *pebble.Snapshot) error {
	sm.rangeMu.RLock()
	rd := sm.rangeDescriptor
	sm.rangeMu.RUnlock()

	if rd == nil {
		sm.log.Warningf("No range descriptor set; not snapshotting range data")
		return nil
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: keys.Key(rd.GetStart()),
		UpperBound: keys.Key(rd.GetEnd()),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := encodeKeyValue(w, iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return nil
}

func (sm *Replica) saveRangeLocalData(w io.Writer, snap *pebble.Snapshot) error {
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: constants.LocalPrefix,
		UpperBound: constants.MetaRangePrefix,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	prefix := sm.replicaPrefix()
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			// Skip keys that are not ours.
			continue
		}
		// Trim the replica-specific suffix from keys that have it.
		// When this snapshot is loaded by another replica, it will
		// append its own replica suffix to local keys that need it.
		key := iter.Key()[len(prefix):]
		if err := encodeKeyValue(w, key, iter.Value()); err != nil {
			return err
		}
	}
	return nil
}

func flushBatch(wb pebble.Batch) error {
	if wb.Empty() {
		return nil
	}
	if err := wb.Commit(pebble.Sync); err != nil {
		return err
	}
	wb.Reset()
	return nil
}

func (sm *Replica) ApplySnapshotFromReader(r io.Reader, db ReplicaWriter) error {
	wb := db.NewBatch()
	defer wb.Close()

	readBuf := bufio.NewReader(r)
	for {
		r, count, err := readDataFromReader(readBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		protoBytes := make([]byte, count)
		n, err := io.ReadFull(r, protoBytes)
		if err != nil {
			return err
		}
		if int64(n) != count {
			return status.FailedPreconditionErrorf("[%s] Count %d != bytes read %d", sm.name(), count, n)
		}
		kv := &rfpb.KV{}
		if err := proto.Unmarshal(protoBytes, kv); err != nil {
			return err
		}
		if isLocalKey(kv.Key) {
			kv.Key = sm.replicaLocalKey(kv.Key)
		}
		if err := wb.Set(kv.Key, kv.Value, nil); err != nil {
			return err
		}
		if wb.Len() > 1*gb {
			// Pebble panics when the batch is greater than ~4GB (or 2GB on 32-bit systems)
			sm.log.Debugf("ApplySnapshotFromReader: flushed batch of size %s", units.BytesSize(float64(wb.Len())))
			if err = flushBatch(wb); err != nil {
				return err
			}
		}
	}
	sm.log.Debugf("ApplySnapshotFromReader: flushed batch of size %s", units.BytesSize(float64(wb.Len())))
	return flushBatch(wb)
}

// SaveSnapshot saves the point in time state of the IOnDiskStateMachine
// instance identified by the input state identifier, which is usually not
// the latest state of the IOnDiskStateMachine instance, to the provided
// io.Writer.
//
// It is application's responsibility to save the complete state to the
// provided io.Writer in a deterministic manner. That is for the same state
// machine state, when SaveSnapshot is invoked multiple times with the same
// input state identifier, the content written to the provided io.Writer
// should always be the same.
//
// When there is any connectivity error between the local node and the remote
// node, an ErrSnapshotStreaming will be returned by io.Writer's Write method.
// The SaveSnapshot method should return ErrSnapshotStreaming to abort its
// operation.
//
// It is SaveSnapshot's responsibility to free the resources owned by the
// input state identifier when it is done.
//
// The provided read-only chan struct{} is provided to notify the SaveSnapshot
// method that the associated Raft node is being closed so the
// IOnDiskStateMachine can choose to abort the SaveSnapshot procedure and
// return ErrSnapshotStopped immediately.
//
// SaveSnapshot is allowed to abort the snapshotting operation at any time by
// returning ErrSnapshotAborted.
//
// The SaveSnapshot method is allowed to be invoked when there is concurrent
// call to the Update method. SaveSnapshot is a read-only method, it should
// never change the state of the IOnDiskStateMachine.
//
// SaveSnapshot returns the encountered error when generating the snapshot.
// Other than the above mentioned ErrSnapshotStopped and ErrSnapshotAborted
// errors, the IOnDiskStateMachine implementation should only return a non-nil
// error when the system need to be immediately halted for critical errors,
// e.g. disk error preventing you from saving the snapshot.
func (sm *Replica) SaveSnapshot(preparedSnap interface{}, w io.Writer, quit <-chan struct{}) error {
	snap, ok := preparedSnap.(*pebble.Snapshot)
	if !ok {
		return status.FailedPreconditionError("unable to coerce snapshot to *pebble.Snapshot")
	}
	defer snap.Close()

	tstart := time.Now()
	defer func() {
		sm.log.Infof("Took %s to save snapshot to writer", time.Since(tstart))
	}()

	if err := sm.saveRangeLocalData(w, snap); err != nil {
		return err
	}
	if err := sm.saveRangeData(w, snap); err != nil {
		return err
	}
	return nil
}

// RecoverFromSnapshot recovers the state of the IOnDiskStateMachine instance
// from a snapshot captured by the SaveSnapshot() method on a remote node. The
// saved snapshot is provided as an io.Reader backed by a file stored on disk.
//
// Dragonboat ensures that the Update, Sync, PrepareSnapshot, SaveSnapshot and
// Close methods will not be invoked when RecoverFromSnapshot() is in
// progress.
//
// The provided read-only chan struct{} is provided to notify the
// RecoverFromSnapshot method that the associated Raft node has been closed.
// On receiving such notification, RecoverFromSnapshot() can choose to
// abort recovering from the snapshot and return an ErrSnapshotStopped error
// immediately. Other than ErrSnapshotStopped, IOnDiskStateMachine should
// only return a non-nil error when the system need to be immediately halted
// for non-recoverable error.
//
// RecoverFromSnapshot is not required to synchronize its recovered in-core
// state with that on disk.
func (sm *Replica) RecoverFromSnapshot(r io.Reader, quit <-chan struct{}) error {
	sm.log.Debugf("RecoverFromSnapshot for %s", sm.name())
	db, err := sm.leaser.DB()
	if err != nil {
		return err
	}

	sm.clearReplica(db)
	err = sm.ApplySnapshotFromReader(r, db)
	db.Close() // close the DB before handling errors or checking keys.
	if err != nil {
		return err
	}
	readDB, err := sm.leaser.DB()
	if err != nil {
		return err
	}
	defer readDB.Close()
	return sm.loadReplicaState(db)
}

func (sm *Replica) RangeDescriptor() *rfpb.RangeDescriptor {
	sm.rangeMu.RLock()
	rd := sm.rangeDescriptor
	sm.rangeMu.RUnlock()
	return rd.CloneVT()
}

func (sm *Replica) ReplicaID() uint64 {
	return sm.replicaID
}

func (sm *Replica) RangeID() uint64 {
	return sm.rangeID
}

func (sm *Replica) TestingDB() (pebble.IPebbleDB, error) {
	return sm.leaser.DB()
}

// Close closes the IOnDiskStateMachine instance. Close is invoked when the
// state machine is in a ready-to-exit state in which there will be no further
// call to the Update, Sync, PrepareSnapshot, SaveSnapshot and the
// RecoverFromSnapshot method. It is possible to have concurrent Lookup calls,
// Lookup can also be called after the return of Close.
//
// Close allows the application to finalize resources to a state easier to
// be re-opened and restarted in the future. It is important to understand
// that Close is not guaranteed to be always invoked, e.g. node can crash at
// any time without calling the Close method. IOnDiskStateMachine should be
// designed in a way that the safety and integrity of its on disk data
// doesn't rely on whether Close is eventually called or not.
//
// Other than setting up some internal flags to indicate that the
// IOnDiskStateMachine instance has been closed, the Close method is not
// allowed to update the state of IOnDiskStateMachine visible to the outside.
func (sm *Replica) Close() error {
	if sm.quitChan != nil {
		close(sm.quitChan)
	}

	sm.rangeMu.Lock()
	rangeDescriptor := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	if sm.store != nil && rangeDescriptor != nil {
		sm.store.RemoveRange(rangeDescriptor, sm)
	}

	sm.readQPS.Stop()
	sm.raftProposeQPS.Stop()

	return nil
}

// New creates a new Replica, an on-disk state machine.
func New(leaser pebble.Leaser, rangeID, replicaID uint64, store IStore, broadcast chan<- events.Event) *Replica {
	repl := &Replica{
		rangeID:             rangeID,
		replicaID:           replicaID,
		NHID:                store.NHID(),
		store:               store,
		leaser:              leaser,
		partitionMetadata:   make(map[string]*rfpb.PartitionMetadata),
		lastUsageCheckIndex: 0,
		fileStorer:          filestore.New(),
		readQPS:             qps.NewCounter(5 * time.Second),
		raftProposeQPS:      qps.NewCounter(5 * time.Second),
		broadcast:           broadcast,
		lockedKeys:          make(map[string][]byte),
		prepared:            make(map[string]pebble.Batch),
	}
	repl.log = log.NamedSubLogger(repl.name())
	return repl
}

func getName(rangeID, replicaID uint64) string {
	return fmt.Sprintf("c%04dn%04d", rangeID, replicaID)
}

func LocalKeyPrefix(rangeID, replicaID uint64) []byte {
	name := getName(rangeID, replicaID)
	prefixString := fmt.Sprintf("%s-", name)
	return append(constants.LocalPrefix, []byte(prefixString)...)
}
