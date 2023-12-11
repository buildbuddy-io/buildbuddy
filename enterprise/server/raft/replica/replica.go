package replica

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const (
	// atimeFlushPeriod is the time interval that we will wait before
	// flushing any atime updates in an incomplete batch (that have not
	// already been flushed due to throughput)
	atimeFlushPeriod = 10 * time.Second
)

var (
	atimeUpdateThreshold = flag.Duration("cache.raft.atime_update_threshold", 10*time.Minute, "Don't update atime if it was updated more recently than this")
	atimeWriteBatchSize  = flag.Int("cache.raft.atime_write_batch_size", 100, "Buffer this many writes before writing atime data")
	atimeBufferSize      = flag.Int("cache.raft.atime_buffer_size", 1_000, "Buffer up to this many atime updates in a channel before dropping atime updates")

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
	Sender() *sender.Sender
	AddPeer(ctx context.Context, sourceShardID, newShardID uint64) error
	SnapshotCluster(ctx context.Context, shardID uint64) error
}

// IOnDiskStateMachine is the interface to be implemented by application's
// state machine when the state machine state is always persisted on disks.
// IOnDiskStateMachine basically matches the state machine type described
// in the section 5.2 of the Raft thesis.
//
// For IOnDiskStateMachine types, concurrent access to the state machine is
// supported. An IOnDiskStateMachine type allows its Update method to be
// concurrently invoked when there are ongoing calls to the Lookup or the
// SaveSnapshot method. Lookup is also allowed when the RecoverFromSnapshot or
// the Close methods are being invoked. Invocations to the Update, Sync,
// PrepareSnapshot, RecoverFromSnapshot and Close methods are guarded by the
// system to ensure mutual exclusion.
//
// Once created, the Open method is immediately invoked to open and use the
// persisted state on disk. This makes IOnDiskStateMachine different from
// IStateMachine types which require the state machine state to be fully
// reconstructed from saved snapshots and Raft logs.
//
// Applications that implement IOnDiskStateMachine are recommended to setup
// periodic snapshotting with relatively short time intervals, that triggers
// the state machine's metadata, usually only a few KBytes each, to be
// periodically snapshotted and thus causes negligible overheads for the system.
// It also provides opportunities for the system to signal Raft Log compactions
// to free up disk spaces.
type Replica struct {
	leaser pebble.Leaser

	rootDir   string
	fileDir   string
	ShardID   uint64
	ReplicaID uint64

	store               IStore
	lastAppliedIndex    uint64
	lastUsageCheckIndex uint64

	partitionMetadataMu sync.Mutex
	partitionMetadata   map[string]*rfpb.PartitionMetadata

	log             log.Logger
	rangeMu         sync.RWMutex
	rangeDescriptor *rfpb.RangeDescriptor
	rangeLease      *rfpb.RangeLeaseRecord
	mappedRange     *rangemap.Range

	fileStorer filestore.Store

	quitChan  chan struct{}
	accesses  chan *accessTimeUpdate
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

func batchContainsKey(wb pebble.Batch, key []byte) ([]byte, bool) {
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, value, _ := batchReader.Next()
		if bytes.Equal(ukey, key) {
			return value, true
		}
	}
	return nil, false
}

func replicaSpecificSuffix(shardID, replicaID uint64) []byte {
	return []byte(fmt.Sprintf("-c%04dn%04d", shardID, replicaID))
}

func (sm *Replica) replicaSuffix() []byte {
	return replicaSpecificSuffix(sm.ShardID, sm.ReplicaID)
}

func replicaSpecificKey(key []byte, shardID, replicaID uint64) []byte {
	suffix := replicaSpecificSuffix(shardID, replicaID)
	if bytes.HasSuffix(key, suffix) {
		log.Debugf("Key %q already has replica suffix!", key)
		return key
	}
	return append(key, suffix...)
}

func (sm *Replica) replicaLocalKey(key []byte) []byte {
	return replicaSpecificKey(key, sm.ShardID, sm.ReplicaID)
}

func (sm *Replica) Usage() (*rfpb.ReplicaUsage, error) {
	sm.rangeMu.Lock()
	rd := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	ru := &rfpb.ReplicaUsage{
		Replica: &rfpb.ReplicaDescriptor{
			ShardId:   sm.ShardID,
			ReplicaId: sm.ReplicaID,
		},
		RangeId: rd.GetRangeId(),
	}

	var numFileRecords, sizeBytes int64
	sm.partitionMetadataMu.Lock()
	for _, pm := range sm.partitionMetadata {
		ru.Partitions = append(ru.Partitions, proto.Clone(pm).(*rfpb.PartitionMetadata))
		numFileRecords += pm.GetTotalCount()
		sizeBytes += pm.GetSizeBytes()
	}
	sm.partitionMetadataMu.Unlock()

	sort.Slice(ru.Partitions, func(i, j int) bool {
		return ru.Partitions[i].GetPartitionId() < ru.Partitions[j].GetPartitionId()
	})
	ru.EstimatedDiskBytesUsed = sizeBytes
	ru.ReadQps = int64(sm.readQPS.Get())
	ru.RaftProposeQps = int64(sm.raftProposeQPS.Get())

	return ru, nil
}

func (sm *Replica) String() string {
	sm.rangeMu.Lock()
	rd := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	return fmt.Sprintf("Replica c%dn%d %s", sm.ShardID, sm.ReplicaID, rdString(rd))
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
		return status.FailedPreconditionErrorf("setRangeLease called with non-range-lease key: %s", key)
	}
	lease := &rfpb.RangeLeaseRecord{}
	if err := proto.Unmarshal(val, lease); err != nil {
		return err
	}
	sm.rangeMu.Lock()
	sm.rangeLease = lease
	sm.rangeMu.Unlock()
	return nil
}

func (sm *Replica) GetRangeLease() *rfpb.RangeLeaseRecord {
	sm.rangeMu.RLock()
	defer sm.rangeMu.RUnlock()
	return sm.rangeLease
}

func (sm *Replica) setRange(key, val []byte) error {
	if !bytes.HasPrefix(key, constants.LocalRangeKey) {
		return status.FailedPreconditionErrorf("setRange called with non-range key: %s", key)
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
	sm.rangeMu.RLock()
	if !isLocalKey(key) {
		if sm.mappedRange != nil && sm.mappedRange.Contains(key) {
			sm.rangeMu.RUnlock()
			if isFileRecordKey(key) {
				if err := sm.updateAndFlushPartitionMetadatas(wb, key, val, nil /*=fileMetadata*/, fileRecordAdd); err != nil {
					return err
				}
			}
			return wb.Set(key, val, nil /*ignored write options*/)
		}
		sm.rangeMu.RUnlock()
		return status.OutOfRangeErrorf("range %s does not contain key %q", sm.mappedRange, string(key))
	}
	sm.rangeMu.RUnlock()

	// Still here? this is a local key, so treat it appropriately.
	key = sm.replicaLocalKey(key)
	return wb.Set(key, val, nil /*ignored write options*/)
}

func (sm *Replica) lookup(db ReplicaReader, key []byte) ([]byte, error) {
	if isLocalKey(key) {
		key = sm.replicaLocalKey(key)
	}
	buf, closer, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, status.NotFoundErrorf("Key not found: %s", err)
		}
		return nil, err
	}
	defer closer.Close()
	if len(buf) == 0 {
		return nil, status.NotFoundError("Key not found (empty)")
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
		if err != nil && err != pebble.ErrNotFound {
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

func (sm *Replica) getDBDir() string {
	return filepath.Join(sm.rootDir,
		fmt.Sprintf("cluster-%d", sm.ShardID),
		fmt.Sprintf("node-%d", sm.ReplicaID))
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
		_, ukey, _, _ := batchReader.Next()
		keyString := string(ukey)
		lockingTxid, ok := sm.lockedKeys[keyString]
		if ok && !bytes.Equal(txid, lockingTxid) {
			return status.UnavailableErrorf("Conflict on key %q", keyString)
		}
	}
	return nil
}

// acquireLocks locks all of the keys in batch `wb`.
func (sm *Replica) acquireLocks(wb pebble.Batch, txid []byte) {
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, _, _ := batchReader.Next()
		keyString := string(ukey)
		sm.lockedKeys[keyString] = txid
	}
}

// releaseLocks unlocks all the keys in the batch `wb` locked with `txid`. If a
// row in `wb` is locked by another transaction, an error is logged and the row
// lock is left unchanged.
func (sm *Replica) releaseLocks(wb pebble.Batch, txid []byte) {
	// Unlock all the keys in this batch.
	batchReader := wb.Reader()
	for len(batchReader) > 0 {
		_, ukey, _, _ := batchReader.Next()
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
		return status.NotFoundErrorf("Transaction %q not found", string(txid))
	}
	defer txn.Close()
	delete(sm.prepared, string(txid))

	sm.releaseLocks(txn, txid)

	txKey := keys.MakeKey(constants.LocalTransactionPrefix, txid)
	txKey = sm.replicaLocalKey(txKey)

	// Lookup our request so that post-commit hooks can be applied, then
	// delete it from the batch, since the txn is being committed.
	batchReq := &rfpb.BatchCmdRequest{}
	iter := txn.NewIter(nil /*default iterOptions*/)
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
		return status.NotFoundErrorf("Transaction %q not found", string(txid))
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
	iter := db.NewIter(iterOpts)
	defer iter.Close()

	suffix := sm.replicaSuffix()
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasSuffix(iter.Key(), suffix) {
			// Skip keys that are not ours.
			continue
		}
		if !bytes.HasPrefix(iter.Key(), constants.LocalTransactionPrefix) {
			// Skip non-inflight-transactions
			continue
		}

		key := iter.Key()[:len(iter.Key())-len(suffix)]
		txid := key[len(constants.LocalTransactionPrefix):]

		batchReq := &rfpb.BatchCmdRequest{}
		if err := proto.Unmarshal(iter.Value(), batchReq); err != nil {
			return err
		}
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
		return status.FailedPreconditionErrorf("last applied not moving forward: %d > %d", sm.lastAppliedIndex, lastStoredIndex)
	}
	sm.lastAppliedIndex = lastStoredIndex
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
	go sm.processAccessTimeUpdates()
	return sm.lastAppliedIndex, nil
}

func (sm *Replica) directWrite(wb pebble.Batch, req *rfpb.DirectWriteRequest) (*rfpb.DirectWriteResponse, error) {
	kv := req.GetKv()
	err := sm.rangeCheckedSet(wb, kv.Key, kv.Value)
	return &rfpb.DirectWriteResponse{}, err
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

	iter := db.NewIter(iterOpts)
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
		return nil, status.NotFoundErrorf("Could not find split point. (Total size: %d, start split size: %d", totalSize, leftSize)
	}
	sm.log.Debugf("Cluster %d found split @ %q start rows: %d, size: %d, end rows: %d, size: %d", sm.ShardID, splitKey, splitRows, splitSize, totalRows-splitRows, totalSize-splitSize)
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
	iter := r.NewIter(iterOpts)
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

	iterOpts := &pebble.IterOptions{}
	if req.GetEnd() != nil {
		iterOpts.UpperBound = req.GetEnd()
	} else {
		iterOpts.UpperBound = keys.Key(req.GetStart()).Next()
	}

	iter := db.NewIter(iterOpts)
	defer iter.Close()
	var t bool

	switch req.GetScanType() {
	case rfpb.ScanRequest_SEEKLT_SCAN_TYPE:
		t = iter.SeekLT(req.GetStart())
	case rfpb.ScanRequest_SEEKGE_SCAN_TYPE:
		t = iter.SeekGE(req.GetStart())
	case rfpb.ScanRequest_SEEKGT_SCAN_TYPE:
		t = iter.SeekGE(req.GetStart())
		// If the iter's current key is *equal* to start, go to the next
		// key greater than this one.
		if t && bytes.Equal(iter.Key(), req.GetStart()) {
			t = iter.Next()
		}
	default:
		t = iter.SeekGE(req.GetStart())
	}

	rsp := &rfpb.ScanResponse{}
	for ; t; t = iter.Next() {
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())
		rsp.Kvs = append(rsp.Kvs, &rfpb.KV{
			Key:   k,
			Value: v,
		})
	}
	return rsp, nil
}

func (sm *Replica) get(db ReplicaReader, req *rfpb.GetRequest) (*rfpb.GetResponse, error) {
	// Check that key is a valid PebbleKey.
	var pk filestore.PebbleKey
	if _, err := pk.FromBytes(req.GetKey()); err != nil {
		return nil, err
	}

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileMetadata, err := lookupFileMetadata(iter, req.GetKey())
	if err != nil {
		return nil, err
	}
	sm.sendAccessTimeUpdate(req.GetKey(), fileMetadata)
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
	iter := wb.NewIter(nil /*default iter options*/)
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

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	present := iter.SeekGE(req.GetKey()) && bytes.Equal(iter.Key(), req.GetKey())
	return &rfpb.FindResponse{
		Present: present,
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

func statusProto(err error) *statuspb.Status {
	s, _ := gstatus.FromError(err)
	return s.Proto()
}

func (sm *Replica) handlePostCommit(hook *rfpb.PostCommitHook) {
	if snap := hook.GetSnapshotCluster(); snap != nil {
		go func() {
			if err := sm.store.SnapshotCluster(context.TODO(), sm.ShardID); err != nil {
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
	case *rfpb.RequestUnion_FindSplitPoint:
		r, err := sm.findSplitPoint()
		rsp.Value = &rfpb.ResponseUnion_FindSplitPoint{
			FindSplitPoint: r,
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

func validateHeaderAgainstRange(rangeDescriptor *rfpb.RangeDescriptor, header *rfpb.Header) error {
	if rangeDescriptor == nil {
		return status.FailedPreconditionError("range descriptor is not set")
	}
	if rangeDescriptor.GetGeneration() != header.GetGeneration() {
		return status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, rangeDescriptor.GetGeneration(), header.GetGeneration())
	}
	return nil
}

type accessTimeUpdate struct {
	key []byte
}

func olderThanThreshold(t time.Time, threshold time.Duration) bool {
	return time.Since(t) >= threshold
}

func (sm *Replica) processAccessTimeUpdates() {
	batch := rbuilder.NewBatchBuilder()

	ctx := context.TODO()

	var lastWrite time.Time
	flush := func() {
		if batch.Size() == 0 {
			return
		}

		batchProto, err := batch.ToProto()
		if err != nil {
			sm.log.Warningf("could not generate atime update batch: %s", err)
			return
		}

		err = sm.store.Sender().Run(ctx, nil, func(c rfspb.ApiClient, h *rfpb.Header) error {
			_, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
				Header: h,
				Batch:  batchProto,
			})
			return err
		})
		if err != nil {
			sm.log.Warningf("could not update atimes: %s", err)
			return
		}

		batch = rbuilder.NewBatchBuilder()
		lastWrite = time.Now()
	}

	exitMu := sync.Mutex{}
	exiting := false
	go func() {
		<-sm.quitChan
		exitMu.Lock()
		exiting = true
		exitMu.Unlock()
	}()

	for {
		select {
		case accessTimeUpdate := <-sm.accesses:
			batch.Add(&rfpb.UpdateAtimeRequest{
				Key:            accessTimeUpdate.key,
				AccessTimeUsec: time.Now().UnixMicro(),
			})
			if batch.Size() >= *atimeWriteBatchSize {
				flush()
			}
		case <-time.After(time.Second):
			if time.Since(lastWrite) > atimeFlushPeriod {
				flush()
			}

			exitMu.Lock()
			done := exiting
			exitMu.Unlock()

			if done {
				flush()
				return
			}
		}
	}
}

func (sm *Replica) sendAccessTimeUpdate(key []byte, fileMetadata *rfpb.FileMetadata) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	if !olderThanThreshold(atime, *atimeUpdateThreshold) {
		return
	}

	up := &accessTimeUpdate{
		key: key,
	}

	// If the atimeBufferSize is 0, non-blocking writes do not make sense,
	// so in that case just do a regular channel send. Otherwise; use a non-
	// blocking channel send.
	if *atimeBufferSize == 0 {
		sm.accesses <- up
	} else {
		select {
		case sm.accesses <- up:
			return
		default:
			sm.log.Warningf("Dropping atime update for %+v", fileMetadata.GetFileRecord())
		}
	}
}

var digestRunes = []rune("abcdef1234567890")

func randomKey(partitionID string, n int) []byte {
	randKey := filestore.PartitionDirectoryPrefix + partitionID + "/"
	for i := 0; i < n; i++ {
		randKey += string(digestRunes[rand.Intn(len(digestRunes))])
	}
	return []byte(randKey)
}

type LRUSample struct {
	Bytes   []byte
	RangeID uint64
}

func (s *LRUSample) ID() string {
	return string(s.Bytes)
}

func (s *LRUSample) String() string {
	return fmt.Sprintf("Range-%d %q", s.RangeID, s.ID())
}

func (sm *Replica) Sample(ctx context.Context, partitionID string, n int) ([]*approxlru.Sample[*LRUSample], error) {
	sm.rangeMu.RLock()
	rangeID := sm.rangeDescriptor.GetRangeId()
	sm.rangeMu.RUnlock()

	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	start, end := keys.Range([]byte(filestore.PartitionDirectoryPrefix + partitionID + "/"))
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	samples := make([]*approxlru.Sample[*LRUSample], 0, n)
	var key filestore.PebbleKey

	// Generate k random samples. Attempt this up to k*2 times, to account
	// for the fact that some files sampled may be younger than
	// minEvictionAge. We try hard!
	for i := 0; i < n*2; i++ {
		if !iter.Valid() {
			// This should only happen once per call to sample(), or
			// occasionally more if we've exhausted the iter.
			randKey := randomKey(partitionID, 64)
			iter.SeekGE(randKey)
		}
		for ; iter.Valid(); iter.Next() {
			if _, err := key.FromBytes(iter.Key()); err != nil {
				return nil, err
			}

			fileMetadata := &rfpb.FileMetadata{}
			if err = proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
				return nil, err
			}

			keyBytes := make([]byte, len(iter.Key()))
			copy(keyBytes, iter.Key())
			sample := &approxlru.Sample[*LRUSample]{
				Key: &LRUSample{
					Bytes:   keyBytes,
					RangeID: rangeID,
				},
				SizeBytes: fileMetadata.GetStoredSizeBytes(),
				Timestamp: time.UnixMicro(fileMetadata.GetLastAccessUsec()),
			}
			samples = append(samples, sample)
			if len(samples) == n {
				return samples, nil
			}
		}
	}

	return samples, nil
}

func (sm *Replica) updateInMemoryState(wb pebble.Batch) {
	// Update the local in-memory range descriptor iff this batch modified
	// it.
	localRangeKey := sm.replicaLocalKey(constants.LocalRangeKey)
	if buf, ok := batchContainsKey(wb, localRangeKey); ok {
		sm.setRange(localRangeKey, buf)
	}
	// Update the rangelease iff this batch sets it.
	localRangeLeaseKey := sm.replicaLocalKey(constants.LocalRangeLeaseKey)
	if buf, ok := batchContainsKey(wb, localRangeLeaseKey); ok {
		sm.setRangeLease(localRangeLeaseKey, buf)
	}

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
func (sm *Replica) singleUpdate(db pebble.IPebbleDB, entry dbsm.Entry) (dbsm.Entry, error) {
	// This method should return errors if something truly fails in an
	// unrecoverable way (proto marshal/unmarshal, pebble batch commit) but
	// otherwise normal request handling errors are encoded in the response
	// and the statemachine keeps progressing.
	batchReq := &rfpb.BatchCmdRequest{}
	if err := proto.Unmarshal(entry.Cmd, batchReq); err != nil {
		return entry, err
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

	// All of the data in a BatchCmdRequest is handled in a single pebble
	// write batch. That means that if any part of a batch update fails,
	// none of the batch will be applied.
	wb := db.NewIndexedBatch()
	defer wb.Close()

	batchRsp := &rfpb.BatchCmdResponse{}
	var headerErr error
	if header := batchReq.GetHeader(); header != nil {
		headerErr = validateHeaderAgainstRange(rd, header)
		batchRsp.Status = statusProto(headerErr)
	}
	if headerErr == nil {
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
	}
	rspBuf, err := proto.Marshal(batchRsp)
	if err != nil {
		return entry, err
	}
	entry.Result = dbsm.Result{
		Value: uint64(len(entry.Cmd)),
		Data:  rspBuf,
	}
	appliedIndex := uint64ToBytes(entry.Index)
	wb.Set(sm.replicaLocalKey(constants.LastAppliedIndexKey), appliedIndex, nil)
	if err := wb.Commit(pebble.NoSync); err != nil {
		return entry, err
	} else {
		// If the batch commit was successful, update the replica's in-
		// memory state.
		sm.updateInMemoryState(wb)
		sm.lastAppliedIndex = entry.Index

		// Run post commit hooks, if any are set.
		for _, hook := range batchReq.GetPostCommitHooks() {
			sm.handlePostCommit(hook)
		}
	}

	return entry, nil
}

func (sm *Replica) Update(entries []dbsm.Entry) ([]dbsm.Entry, error) {
	defer canary.Start("replica.Update", time.Second)()
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	for i, entry := range entries {
		e, err := sm.singleUpdate(db, entry)
		if err != nil {
			return nil, err
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

	var headerErr error
	if header := batchReq.GetHeader(); header != nil {
		sm.rangeMu.RLock()
		rd := sm.rangeDescriptor
		sm.rangeMu.RUnlock()

		headerErr = validateHeaderAgainstRange(rd, header)
		batchRsp.Status = statusProto(headerErr)
	}

	if headerErr == nil {
		for _, req := range batchReq.GetUnion() {
			//sm.log.Debugf("Lookup: request union: %+v", req)
			rsp := sm.handleRead(db, req)
			// sm.log.Debugf("Lookup: response union: %+v", rsp)
			batchRsp.Union = append(batchRsp.Union, rsp)
		}
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
		return status.FailedPreconditionErrorf("wrote wrong number of bytes?")
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
	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: keys.Key(rd.GetStart()),
		UpperBound: keys.Key(rd.GetEnd()),
	})
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := encodeKeyValue(w, iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return nil
}

func (sm *Replica) saveRangeLocalData(w io.Writer, snap *pebble.Snapshot) error {
	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: constants.LocalPrefix,
		UpperBound: constants.MetaRangePrefix,
	})
	defer iter.Close()
	suffix := sm.replicaSuffix()
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasSuffix(iter.Key(), suffix) {
			// Skip keys that are not ours.
			continue
		}
		// Trim the replica-specific suffix from keys that have it.
		// When this snapshot is loaded by another replica, it will
		// append its own replica suffix to local keys that need it.
		key := iter.Key()[:len(iter.Key())-len(suffix)]
		if err := encodeKeyValue(w, key, iter.Value()); err != nil {
			return err
		}
	}
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
			return status.FailedPreconditionErrorf("Count %d != bytes read %d", count, n)
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
	}
	if err := db.Apply(wb, pebble.Sync); err != nil {
		return err
	}
	return nil
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
	db, err := sm.leaser.DB()
	if err != nil {
		return err
	}
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

// CreateReplica creates an ondisk statemachine.
func New(leaser pebble.Leaser, shardID, replicaID uint64, store IStore, broadcast chan<- events.Event) *Replica {
	return &Replica{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		store:               store,
		leaser:              leaser,
		partitionMetadata:   make(map[string]*rfpb.PartitionMetadata),
		lastUsageCheckIndex: 0,
		log:                 log.NamedSubLogger(fmt.Sprintf("c%dn%d", shardID, replicaID)),
		fileStorer:          filestore.New(),
		accesses:            make(chan *accessTimeUpdate, *atimeBufferSize),
		readQPS:             qps.NewCounter(1 * time.Minute),
		raftProposeQPS:      qps.NewCounter(1 * time.Minute),
		broadcast:           broadcast,
		lockedKeys:          make(map[string][]byte),
		prepared:            make(map[string]pebble.Batch),
	}
}
