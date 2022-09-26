package replica

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebbleutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
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
)

// Replicas need a reference back to the Store that holds them in order to
// add and remove themselves, read files from peers, etc. In order to make this
// more easily testable in a standalone fashion, IStore mocks out just the
// necessary methods that a Replica requires a Store to have.
type IStore interface {
	AddRange(rd *rfpb.RangeDescriptor, r *Replica)
	RemoveRange(rd *rfpb.RangeDescriptor, r *Replica)
	Sender() *sender.Sender
	ReadFileFromPeer(ctx context.Context, except *rfpb.ReplicaDescriptor, fileRecord *rfpb.FileRecord) (io.ReadCloser, error)
	GetReplica(rangeID uint64) (*Replica, error)
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
	db     *pebble.DB
	leaser pebbleutil.Leaser

	rootDir   string
	fileDir   string
	ClusterID uint64
	NodeID    uint64
	timer     *time.Timer
	timerMu   *sync.Mutex

	store            IStore
	lastAppliedIndex uint64

	log             log.Logger
	rangeMu         sync.RWMutex
	rangeDescriptor *rfpb.RangeDescriptor
	mappedRange     *rangemap.Range

	fileStorer filestore.Store

	quitChan chan struct{}
	accesses chan *accessTimeUpdate
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

	fileMetadata := &rfpb.FileMetadata{}
	if err := proto.Unmarshal(val, fileMetadata); err != nil {
		return 0, err
	}
	return fileMetadata.GetSizeBytes() + int64(len(val)), nil
}

func (sm *Replica) Usage() (*rfpb.ReplicaUsage, error) {
	ru := &rfpb.ReplicaUsage{
		Replica: &rfpb.ReplicaDescriptor{
			ClusterId: sm.ClusterID,
			NodeId:    sm.NodeID,
		},
	}
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	iterOpts := &pebble.IterOptions{
		LowerBound: keys.Key([]byte{constants.MinByte}),
		UpperBound: keys.Key([]byte{constants.MaxByte}),
	}

	iter := db.NewIter(iterOpts)
	defer iter.Close()

	estimatedBytesUsed := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		sizeBytes, err := sizeOf(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		estimatedBytesUsed += sizeBytes
	}
	ru.EstimatedDiskBytesUsed = estimatedBytesUsed
	return ru, nil
}

func (sm *Replica) String() string {
	sm.rangeMu.Lock()
	rd := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	return fmt.Sprintf("Replica c%dn%d %s", sm.ClusterID, sm.NodeID, rdString(rd))
}

func rdString(rd *rfpb.RangeDescriptor) string {
	if rd == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Range(%d) [%q, %q)", rd.GetRangeId(), rd.GetLeft(), rd.GetRight())
}

func (sm *Replica) setRange(key, val []byte) error {
	if bytes.Compare(key, constants.LocalRangeKey) != 0 {
		return status.FailedPreconditionErrorf("setRange called with non-range key: %s", key)
	}

	rangeDescriptor := &rfpb.RangeDescriptor{}
	if err := proto.Unmarshal(val, rangeDescriptor); err != nil {
		return err
	}

	sm.rangeMu.Lock()
	defer sm.rangeMu.Unlock()

	if sm.rangeDescriptor != nil {
		sm.store.RemoveRange(sm.rangeDescriptor, sm)
	}

	sm.log.Debugf("Range descriptor is changing from %s to %s", rdString(sm.rangeDescriptor), rdString(rangeDescriptor))
	sm.rangeDescriptor = rangeDescriptor
	sm.mappedRange = &rangemap.Range{
		Left:  rangeDescriptor.GetLeft(),
		Right: rangeDescriptor.GetRight(),
	}
	sm.store.AddRange(sm.rangeDescriptor, sm)
	return nil
}

func (sm *Replica) rangeCheckedSet(wb *pebble.Batch, key, val []byte) error {
	sm.rangeMu.RLock()

	if !keys.IsLocalKey(key) {
		if sm.mappedRange != nil && sm.mappedRange.Contains(key) {
			sm.rangeMu.RUnlock()
			return wb.Set(key, val, nil /*ignored write options*/)
		}
		sm.rangeMu.RUnlock()
		return status.OutOfRangeErrorf("range %s does not contain key %q", sm.mappedRange, string(key))
	}
	sm.rangeMu.RUnlock()

	if err := wb.Set(key, val, nil /*ignored write options*/); err != nil {
		return err
	}
	if bytes.Compare(key, constants.LocalRangeKey) == 0 {
		if err := sm.setRange(key, val); err != nil {
			log.Errorf("Error setting range: %s", err)
		}
	}
	return nil
}

func (sm *Replica) lookup(db ReplicaReader, query []byte) ([]byte, error) {
	buf, closer, err := db.Get(query)
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

func (sm *Replica) getLastAppliedIndex(db ReplicaReader) (uint64, error) {
	val, err := sm.lookup(db, []byte(constants.LastAppliedIndexKey))
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

func (sm *Replica) getDBDir() string {
	return filepath.Join(sm.rootDir,
		fmt.Sprintf("cluster-%d", sm.ClusterID),
		fmt.Sprintf("node-%d", sm.NodeID))
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
	NewBatch() *pebble.Batch
	NewIndexedBatch() *pebble.Batch
	NewSnapshot() *pebble.Snapshot
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
	db, err := pebble.Open(sm.getDBDir(), &pebble.Options{})
	if err != nil {
		return 0, err
	}
	sm.db = db
	sm.leaser = pebbleutil.NewDBLeaser(db)

	sm.checkAndSetRangeDescriptor(db)
	return sm.getLastAppliedIndex(db)
}

func (sm *Replica) checkAndSetRangeDescriptor(db ReplicaReader) {
	buf, err := sm.lookup(db, constants.LocalRangeKey)
	if err != nil {
		sm.log.Debugf("Replica opened but range not yet set")
		return
	}
	sm.setRange(constants.LocalRangeKey, buf)
}

func (sm *Replica) fileDelete(wb *pebble.Batch, req *rfpb.FileDeleteRequest) (*rfpb.FileDeleteResponse, error) {
	iter := wb.NewIter(nil /*default iter options*/)
	defer iter.Close()

	fileMetadataKey, err := sm.fileStorer.FileMetadataKey(req.GetFileRecord())
	if err != nil {
		return nil, err
	}

	found := iter.SeekGE(fileMetadataKey)
	if !found || bytes.Compare(fileMetadataKey, iter.Key()) != 0 {
		return nil, status.NotFoundErrorf("file data for %v was not found in replica", req.GetFileRecord())
	}
	fileMetadata := &rfpb.FileMetadata{}
	if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
		return nil, err
	}
	if err := sm.fileStorer.DeleteStoredFile(context.TODO(), sm.fileDir, fileMetadata.GetStorageMetadata()); err != nil {
		return nil, err
	}
	if err := wb.Delete(fileMetadataKey, nil /*ignored write options*/); err != nil {
		return nil, err
	}
	return &rfpb.FileDeleteResponse{}, nil
}

func (sm *Replica) deleteRange(wb *pebble.Batch, req *rfpb.DeleteRangeRequest) (*rfpb.DeleteRangeResponse, error) {
	iter := wb.NewIter(&pebble.IterOptions{
		LowerBound: keys.Key(req.GetStart()),
		UpperBound: keys.Key(req.GetEnd()),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := wb.Delete(iter.Key(), nil /*ignored write options*/); err != nil {
			return nil, err
		}
		fileMetadata := &rfpb.FileMetadata{}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err == nil {
			if fileMetadata.GetFileRecord() != nil {
				if err := sm.fileStorer.DeleteStoredFile(context.TODO(), sm.fileDir, fileMetadata.GetStorageMetadata()); err != nil {
					log.Errorf("Error deleting stored file: %s", err)
				}
			}
		}
	}

	return &rfpb.DeleteRangeResponse{}, nil
}

func (sm *Replica) fileUpdateMetadata(wb *pebble.Batch, req *rfpb.FileUpdateMetadataRequest) (*rfpb.FileUpdateMetadataResponse, error) {
	iter := wb.NewIter(nil /*default iter options*/)
	defer iter.Close()

	fileMetadataKey, err := sm.fileStorer.FileMetadataKey(req.GetFileRecord())
	if err != nil {
		return nil, err
	}

	fileMetadata, err := lookupFileMetadata(iter, fileMetadataKey)
	if err != nil {
		return nil, err
	}

	if req.GetLastAccessUsec() != 0 {
		fileMetadata.LastAccessUsec = req.GetLastAccessUsec()
	}

	d, err := proto.Marshal(fileMetadata)
	if err != nil {
		return nil, err
	}

	if err := wb.Set(fileMetadataKey, d, nil); err != nil {
		return nil, err
	}

	return &rfpb.FileUpdateMetadataResponse{}, nil
}

func (sm *Replica) fileWrite(wb *pebble.Batch, req *rfpb.FileWriteRequest) (*rfpb.FileWriteResponse, error) {
	// In the common case, this doesn't actually write any data, because
	// it's already been written it in a separate Write RPC. It simply
	// validates that the Metadata key exists. If it does not, the data is
	// read from a peer.
	iter := wb.NewIter(nil /*default iter options*/)
	defer iter.Close()

	ctx := context.TODO()
	fileMetadataKey, err := sm.fileStorer.FileMetadataKey(req.GetFileRecord())
	if err != nil {
		return nil, err
	}

	fileExists := false
	fileMetadata, err := lookupFileMetadata(iter, fileMetadataKey)
	if err == nil {
		fileExists = sm.fileStorer.FileExists(ctx, sm.fileDir, fileMetadata.GetStorageMetadata())
	}
	if fileExists {
		return &rfpb.FileWriteResponse{}, nil
	}

	storageMD, sizeBytes, err := sm.fetchFileToLocalStorage(ctx, req.GetFileRecord())
	if err != nil {
		return nil, err
	}
	d := req.GetFileRecord().GetDigest()
	sm.log.Debugf("fileWrite: fetched file %q/%d to local storage", d.GetHash(), d.GetSizeBytes())
	md := &rfpb.FileMetadata{
		FileRecord:      req.GetFileRecord(),
		StorageMetadata: storageMD,
		SizeBytes:       sizeBytes,
	}
	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return nil, err
	}
	if err := sm.rangeCheckedSet(wb, fileMetadataKey, protoBytes); err != nil {
		return nil, err
	}
	return &rfpb.FileWriteResponse{}, nil
}

func (sm *Replica) directWrite(wb *pebble.Batch, req *rfpb.DirectWriteRequest) (*rfpb.DirectWriteResponse, error) {
	kv := req.GetKv()
	return &rfpb.DirectWriteResponse{}, sm.rangeCheckedSet(wb, kv.Key, kv.Value)
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

func (sm *Replica) increment(wb *pebble.Batch, req *rfpb.IncrementRequest) (*rfpb.IncrementResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.InvalidArgumentError("Increment requires a valid key.")
	}
	buf, err := pebbleutil.GetCopy(wb, req.GetKey())
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

func (sm *Replica) cas(wb *pebble.Batch, req *rfpb.CASRequest) (*rfpb.CASResponse, error) {
	kv := req.GetKv()
	var buf []byte
	var err error
	buf, err = sm.lookup(wb, kv.GetKey())
	if err != nil && !status.IsNotFoundError(err) {
		return nil, err
	}

	// Match: set value and return new value + no error.
	if bytes.Compare(buf, req.GetExpectedValue()) == 0 {
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

func (sm *Replica) IsSplitting() bool {
	sm.timerMu.Lock()
	defer sm.timerMu.Unlock()
	return sm.timer != nil
}

func (sm *Replica) releaseAndClearTimer() {
	sm.timerMu.Lock()
	defer sm.timerMu.Unlock()
	if sm.timer == nil {
		log.Warningf("releaseAndClearTimer was called but no timer was active")
		return
	}

	sm.leaser.ReleaseSplitLock()
	sm.timer.Stop()
	sm.timer = nil
}

func (sm *Replica) oneshotCAS(cas *rfpb.CASRequest) error {
	if cas == nil {
		return nil
	}
	wb := sm.db.NewIndexedBatch()
	defer wb.Close()

	if _, err := sm.cas(wb, cas); err != nil {
		return err
	}

	return wb.Commit(&pebble.WriteOptions{Sync: true})
}

func (sm *Replica) splitLease(req *rfpb.SplitLeaseRequest) (*rfpb.SplitLeaseResponse, error) {
	sm.timerMu.Lock()
	defer sm.timerMu.Unlock()

	timeTilExpiry := time.Duration(req.GetDurationSeconds()) * time.Second
	startNewTimer := func() {
		sm.leaser.AcquireSplitLock()
		sm.timer = time.AfterFunc(timeTilExpiry, func() {
			log.Warning("Split lease expired!")
			sm.releaseAndClearTimer()
			if err := sm.oneshotCAS(req.GetCasOnExpiry()); err != nil {
				log.Errorf("Error reverting lease: %s", err)
			}
		})
	}

	if sm.timer == nil {
		startNewTimer()
	} else {
		if !sm.timer.Stop() {
			startNewTimer()
		}
		sm.timer.Reset(timeTilExpiry)
	}
	return &rfpb.SplitLeaseResponse{}, nil
}

func (sm *Replica) splitRelease(req *rfpb.SplitReleaseRequest) (*rfpb.SplitReleaseResponse, error) {
	wb := sm.db.NewIndexedBatch()
	defer wb.Close()

	defer func() {
		sm.releaseAndClearTimer()
	}()

	for _, union := range req.GetBatch().GetUnion() {
		switch value := union.Value.(type) {
		case *rfpb.RequestUnion_Cas:
			if _, err := sm.cas(wb, value.Cas); err != nil {
				return nil, err
			}
		default:
			return nil, status.InvalidArgumentError("only CAS reqs are allowed in split release batch")
		}
	}

	if wb.Count() > 0 {
		if err := wb.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
			return nil, err
		}
	}
	return &rfpb.SplitReleaseResponse{}, nil
}

type splitPoint struct {
	left      []byte
	right     []byte
	leftSize  int64
	rightSize int64
}

func (sm *Replica) findSplitPoint(req *rfpb.FindSplitPointRequest) (*rfpb.FindSplitPointResponse, error) {
	iterOpts := &pebble.IterOptions{
		LowerBound: keys.Key([]byte{constants.MinByte}),
		UpperBound: keys.Key([]byte{constants.MaxByte}),
	}

	iter := sm.db.NewIter(iterOpts)
	defer iter.Close()

	totalSize := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		sizeBytes, err := sizeOf(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		totalSize += sizeBytes
	}

	leftSplitSize := int64(0)
	var lastKey []byte
	for iter.First(); iter.Valid(); iter.Next() {
		if leftSplitSize >= totalSize/2 && canSplitKeys(lastKey, iter.Key()) {
			sp := &rfpb.FindSplitPointResponse{
				Split:          make([]byte, len(iter.Key())),
				LeftSizeBytes:  leftSplitSize,
				RightSizeBytes: totalSize - leftSplitSize,
			}
			copy(sp.Split, iter.Key())
			log.Debugf("Found split point: %+v", sp)
			return sp, nil
		}
		size, err := sizeOf(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		leftSplitSize += size
		if len(lastKey) != len(iter.Key()) {
			lastKey = make([]byte, len(iter.Key()))
		}
		copy(lastKey, iter.Key())
	}
	return nil, status.NotFoundErrorf("Could not find split point. (Total size: %d, left split size: %d", totalSize, leftSplitSize)
}

func canSplitKeys(leftKey, rightKey []byte) bool {
	splitStart := []byte{constants.UnsplittableMaxByte}
	// Disallow splitting the metarange, or any range before '\x04'.
	if bytes.Compare(rightKey, splitStart) <= 0 {
		log.Debugf("can't split between %q and %q, end in metarange", leftKey, rightKey)
		return false
	}

	if bytes.Compare(leftKey, splitStart) <= 0 && bytes.Compare(rightKey, splitStart) > 0 {
		log.Debugf("can't split between %q and %q, overlaps metarange", leftKey, rightKey)
		return false
	}

	// Disallow splitting pebble file-metadata from stored-file-data.
	// File mdata will have a key like /foo/bar/baz
	// File data will have a key like /foo/bar/baz-{1..n}
	if bytes.HasPrefix(rightKey, leftKey) {
		log.Debugf("can't split between %q and %q, prefix match", leftKey, rightKey)
		return false
	}

	return true
}

func printRange(wb *pebble.Batch, tag string) {
	iter := wb.NewIter(&pebble.IterOptions{})
	for iter.First(); iter.Valid(); iter.Next() {
		log.Printf("%q: key: %q", tag, iter.Key())
	}
	defer iter.Close()
}

func (sm *Replica) deleteStoredFiles(start, end []byte) error {
	ctx := context.Background()
	iter := sm.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if isLocalKey(iter.Key()) {
			continue
		}

		fileMetadata := &rfpb.FileMetadata{}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return err
		}
		if err := sm.fileStorer.DeleteStoredFile(ctx, sm.fileDir, fileMetadata.GetStorageMetadata()); err != nil {
			return err
		}
	}
	return nil
}

// copyStoredFiles copies stored file data from this replica to another replica
// which must be running locally on this nodehost and available in the store.
func (sm *Replica) copyStoredFiles(req *rfpb.CopyStoredFilesRequest) (*rfpb.CopyStoredFilesResponse, error) {
	start := time.Now()
	defer func() {
		sm.log.Infof("Took %s to copy stored files between ranges", time.Since(start))
	}()
	appliesToThisReplica := sm.splitAppliesToThisReplica(req.GetSourceRange())
	if !appliesToThisReplica {
		sm.log.Debugf("Received copyStoredFiles cmd but it doesn't apply to this replica. Not doing anything.")
		return &rfpb.CopyStoredFilesResponse{}, nil
	}

	targetReplica, err := sm.store.GetReplica(req.GetTargetRangeId())
	if err != nil {
		return nil, err
	}
	if targetReplica == sm {
		return nil, status.FailedPreconditionErrorf("cannot copy stored files from replica (range %d) to self", req.GetTargetRangeId())
	}

	sm.log.Debugf("Copying stored files from %+v to %+v", sm, targetReplica)
	ctx := context.Background()
	iter := sm.db.NewIter(&pebble.IterOptions{
		LowerBound: keys.Key(req.GetStart()),
		UpperBound: keys.Key(req.GetEnd()),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		fileMetadata := &rfpb.FileMetadata{}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			continue
		}
		if fileMetadata.GetStorageMetadata() == nil {
			continue
		}
		readCloser, err := sm.fileStorer.NewReader(ctx, sm.fileDir, fileMetadata.GetStorageMetadata(), 0, 0)
		if err != nil {
			return nil, err
		}
		writeCloserMetadata, err := targetReplica.storedFileWriter(ctx, fileMetadata.GetFileRecord())
		if err != nil {
			return nil, err
		}
		n, err := io.Copy(writeCloserMetadata, readCloser)
		readCloser.Close()
		if n != fileMetadata.GetSizeBytes() {
			return nil, status.FailedPreconditionErrorf("read %d bytes but expected %d", n, fileMetadata.GetSizeBytes())
		}
		if err := writeCloserMetadata.Close(); err != nil {
			return nil, err
		}
		if !proto.Equal(writeCloserMetadata.Metadata(), fileMetadata.GetStorageMetadata()) {
			sm.log.Errorf("Stored metadata differs after fetching file locally. Before %+v, after: %+v", fileMetadata.GetStorageMetadata(), writeCloserMetadata.Metadata())
			return nil, status.FailedPreconditionError("stored metadata changed when fetching file")
		}
	}
	return &rfpb.CopyStoredFilesResponse{}, nil
}

// splitAppliesToThisReplica returns true if this replica is one of the replicas
// found in the provided range descriptor.
func (sm *Replica) splitAppliesToThisReplica(rd *rfpb.RangeDescriptor) bool {
	for _, replica := range rd.GetReplicas() {
		if replica.GetClusterId() == sm.ClusterID &&
			replica.GetNodeId() == sm.NodeID {
			return true
		}
	}
	return false
}

func (sm *Replica) scan(db ReplicaReader, req *rfpb.ScanRequest) (*rfpb.ScanResponse, error) {
	if len(req.GetLeft()) == 0 {
		return nil, status.InvalidArgumentError("Scan requires a valid key.")
	}

	iterOpts := &pebble.IterOptions{}
	if req.GetRight() != nil {
		iterOpts.UpperBound = req.GetRight()
	} else {
		iterOpts.UpperBound = keys.Key(req.GetLeft()).Next()
	}

	iter := db.NewIter(iterOpts)
	defer iter.Close()
	var t bool

	switch req.GetScanType() {
	case rfpb.ScanRequest_SEEKLT_SCAN_TYPE:
		t = iter.SeekLT(req.GetLeft())
	case rfpb.ScanRequest_SEEKGE_SCAN_TYPE:
		t = iter.SeekGE(req.GetLeft())
	case rfpb.ScanRequest_SEEKGT_SCAN_TYPE:
		t = iter.SeekGE(req.GetLeft())
		// If the iter's current key is *equal* to left, go to the next
		// key greater than this one.
		if t && bytes.Compare(iter.Key(), req.GetLeft()) == 0 {
			t = iter.Next()
		}
	default:
		t = iter.SeekGE(req.GetLeft())
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

func statusProto(err error) *statuspb.Status {
	s, _ := gstatus.FromError(err)
	return s.Proto()
}

func (sm *Replica) handlePropose(wb *pebble.Batch, req *rfpb.RequestUnion, rsp *rfpb.ResponseUnion) {
	switch value := req.Value.(type) {
	case *rfpb.RequestUnion_FileWrite:
		r, err := sm.fileWrite(wb, value.FileWrite)
		rsp.Value = &rfpb.ResponseUnion_FileWrite{
			FileWrite: r,
		}
		rsp.Status = statusProto(err)
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
	case *rfpb.RequestUnion_FileDelete:
		r, err := sm.fileDelete(wb, value.FileDelete)
		rsp.Value = &rfpb.ResponseUnion_FileDelete{
			FileDelete: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_DeleteRange:
		r, err := sm.deleteRange(wb, value.DeleteRange)
		rsp.Value = &rfpb.ResponseUnion_DeleteRange{
			DeleteRange: r,
		}
		rsp.Status = statusProto(err)
	case *rfpb.RequestUnion_FileUpdateMetadata:
		r, err := sm.fileUpdateMetadata(wb, value.FileUpdateMetadata)
		rsp.Value = &rfpb.ResponseUnion_FileUpdateMetadata{
			FileUpdateMetadata: r,
		}
		rsp.Status = statusProto(err)
	default:
		rsp.Status = statusProto(status.UnimplementedErrorf("SyncPropose handling for %+v not implemented.", req))
	}
}

func (sm *Replica) handleRead(db ReplicaReader, req *rfpb.RequestUnion) *rfpb.ResponseUnion {
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
	default:
		rsp.Status = statusProto(status.UnimplementedErrorf("Read handling for %+v not implemented.", req))
	}
	return rsp
}

func lookupFileMetadata(iter *pebble.Iterator, fileMetadataKey []byte) (*rfpb.FileMetadata, error) {
	fileMetadata := &rfpb.FileMetadata{}
	if err := pebbleutil.LookupProto(iter, fileMetadataKey, fileMetadata); err != nil {
		return nil, err
	}
	return fileMetadata, nil
}

// validateRange checks that the requested range generation matches our range
// generation. We perform the generation check both in the store and the replica
// because of a race condition during splits. The replica may receive concurrent
// read/write & split requests that may pass the store generation check and
// enter the replica code. The split will hold the split lock and modify the
// internal state with the new range information. goroutines that are unblocked
// after the split lock is released need to verify that the generation has not
// changed under them.
func (sm *Replica) validateRange(header *rfpb.Header) error {
	sm.rangeMu.RLock()
	defer sm.rangeMu.RUnlock()
	if sm.rangeDescriptor == nil {
		return status.FailedPreconditionError("range descriptor is not set")
	}
	if sm.rangeDescriptor.GetGeneration() != header.GetGeneration() {
		return status.OutOfRangeErrorf("%s: generation: %d requested: %d (split)", constants.RangeNotCurrentMsg, sm.rangeDescriptor.GetGeneration(), header.GetGeneration())
	}
	return nil
}

func (sm *Replica) metadataForRecord(db pebble.Reader, fileRecord *rfpb.FileRecord) (*rfpb.FileMetadata, error) {
	fileMetadataKey, err := sm.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	iter := db.NewIter(nil /*default iterOptions*/)
	fileMetadata, err := lookupFileMetadata(iter, fileMetadataKey)
	iter.Close()
	if err != nil {
		return nil, err
	}
	return fileMetadata, nil
}

type accessTimeUpdate struct {
	record *rfpb.FileRecord
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
			log.Warningf("could not generate atime update batch: %s", err)
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
			log.Warningf("could not update atimes: %s", err)
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
			batch.Add(&rfpb.FileUpdateMetadataRequest{
				FileRecord:     accessTimeUpdate.record,
				LastAccessUsec: time.Now().UnixMicro(),
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

func (sm *Replica) sendAccessTimeUpdate(fileMetadata *rfpb.FileMetadata) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	if !olderThanThreshold(atime, *atimeUpdateThreshold) {
		return
	}

	up := &accessTimeUpdate{fileMetadata.GetFileRecord()}

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
			log.Warningf("Dropping atime update for %+v", fileMetadata.GetFileRecord())
		}
	}
}

func (sm *Replica) Reader(ctx context.Context, header *rfpb.Header, fileRecord *rfpb.FileRecord, offset, limit int64) (io.ReadCloser, error) {
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := sm.validateRange(header); err != nil {
		return nil, err
	}

	fileMetadata, err := sm.metadataForRecord(db, fileRecord)
	if err != nil {
		return nil, err
	}
	rc, err := sm.fileStorer.NewReader(ctx, sm.fileDir, fileMetadata.GetStorageMetadata(), offset, limit)
	if err != nil {
		return nil, err
	}
	sm.sendAccessTimeUpdate(fileMetadata)

	// Grab another lease and pass the Close function to the reader
	// so it will be closed when the reader is.
	db, err = sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	return pebbleutil.ReadCloserWithFunc(rc, db.Close), nil
}

func (sm *Replica) FindMissing(ctx context.Context, header *rfpb.Header, fileRecords []*rfpb.FileRecord) ([]*rfpb.FileRecord, error) {
	reader, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if err := sm.validateRange(header); err != nil {
		return nil, err
	}

	iter := reader.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	missing := make([]*rfpb.FileRecord, 0)
	for _, fileRecord := range fileRecords {
		fileMetadaKey, err := sm.fileStorer.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if !iter.SeekGE(fileMetadaKey) || bytes.Compare(iter.Key(), fileMetadaKey) != 0 {
			missing = append(missing, fileRecord)
		}
	}
	return missing, nil
}

func (sm *Replica) GetMulti(ctx context.Context, header *rfpb.Header, fileRecords []*rfpb.FileRecord) ([]*rfpb.GetMultiResponse_Data, error) {
	reader, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if err := sm.validateRange(header); err != nil {
		return nil, err
	}

	var rsp []*rfpb.GetMultiResponse_Data
	var buf bytes.Buffer
	for _, fileRecord := range fileRecords {
		rc, err := sm.Reader(ctx, header, fileRecord, 0, 0)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(&buf, rc)
		rc.Close()
		if err != nil {
			return nil, err
		}

		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		rsp = append(rsp, &rfpb.GetMultiResponse_Data{FileRecord: fileRecord, Data: data})
		buf.Reset()
	}
	return rsp, nil
}

func (sm *Replica) storedFileWriter(ctx context.Context, fileRecord *rfpb.FileRecord) (interfaces.MetadataWriteCloser, error) {
	return sm.fileStorer.NewWriter(ctx, sm.fileDir, fileRecord)
}

func (sm *Replica) Writer(ctx context.Context, header *rfpb.Header, fileRecord *rfpb.FileRecord) (interfaces.CommittedMetadataWriteCloser, error) {
	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := sm.validateRange(header); err != nil {
		return nil, err
	}

	fileMetadataKey, err := sm.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	writeCloserMetadata, err := sm.storedFileWriter(ctx, fileRecord)
	if err != nil {
		return nil, err
	}

	// Grab another lease and pass the Close function to the writer
	// (below) so it will be closed when the writer is.
	db, err = sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	commitFn := func(bytesWritten int64) error {
		batch := db.NewBatch()
		now := time.Now()
		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: writeCloserMetadata.Metadata(),
			SizeBytes:       bytesWritten,
			LastModifyUsec:  now.UnixMicro(),
			LastAccessUsec:  now.UnixMicro(),
		}
		protoBytes, err := proto.Marshal(md)
		if err != nil {
			return err
		}
		if err := batch.Set(fileMetadataKey, protoBytes, nil /*ignored write options*/); err != nil {
			return err
		}
		return batch.Commit(&pebble.WriteOptions{Sync: true})
	}
	return pebbleutil.CommittedWriterWithFunc(writeCloserMetadata, commitFn, db.Close), nil
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
	var wb *pebble.Batch

	// Insert all of the data in the batch.
	batchCmdReq := &rfpb.BatchCmdRequest{}
	for idx, entry := range entries {
		if err := proto.Unmarshal(entry.Cmd, batchCmdReq); err != nil {
			return nil, err
		}
		batchCmdRsp := &rfpb.BatchCmdResponse{}
		for _, union := range batchCmdReq.GetUnion() {
			rsp := &rfpb.ResponseUnion{}
			// Special Case: SplitLease and SplitRelease don't need
			// access to the db in the stame way, and once it's
			// locked, accessing it would block anyway. For that
			// reason, they are handled separately here.
			switch value := union.Value.(type) {
			case *rfpb.RequestUnion_SplitLease:
				r, err := sm.splitLease(value.SplitLease)
				rsp.Value = &rfpb.ResponseUnion_SplitLease{
					SplitLease: r,
				}
				rsp.Status = statusProto(err)
				batchCmdRsp.Union = append(batchCmdRsp.Union, rsp)
				continue
			case *rfpb.RequestUnion_SplitRelease:
				r, err := sm.splitRelease(value.SplitRelease)
				rsp.Value = &rfpb.ResponseUnion_SplitRelease{
					SplitRelease: r,
				}
				rsp.Status = statusProto(err)
				batchCmdRsp.Union = append(batchCmdRsp.Union, rsp)
				continue
			case *rfpb.RequestUnion_FindSplitPoint:
				r, err := sm.findSplitPoint(value.FindSplitPoint)
				rsp.Value = &rfpb.ResponseUnion_FindSplitPoint{
					FindSplitPoint: r,
				}
				rsp.Status = statusProto(err)
				batchCmdRsp.Union = append(batchCmdRsp.Union, rsp)
				continue
			case *rfpb.RequestUnion_CopyStoredFiles:
				r, err := sm.copyStoredFiles(value.CopyStoredFiles)
				rsp.Value = &rfpb.ResponseUnion_CopyStoredFiles{
					CopyStoredFiles: r,
				}
				rsp.Status = statusProto(err)
				batchCmdRsp.Union = append(batchCmdRsp.Union, rsp)
				continue
			}

			// All other raft commands are handled normally via
			// handlePropose below. If no write batch is currently
			// open, open one now.
			if wb == nil {
				db, err := sm.leaser.DB()
				if err != nil {
					// This should really be an error. The
					// reason it's not yet is because range
					// lease requests sometimes come in
					// during a split, and they are safe
					// to ignore.
					if bytes.Compare(union.GetCas().GetKv().GetKey(), constants.LocalRangeLeaseKey) == 0 {
						log.Warningf("mid-split, dropping rangelease cmd: %+v", union)
						continue
					}
					log.Errorf("Range locked but got request: %+v", union)
					return nil, err
				}
				defer db.Close()
				wb = db.NewIndexedBatch()
				defer wb.Close()
			}

			// sm.log.Debugf("Update: request union: %+v", union)
			sm.handlePropose(wb, union, rsp)
			if union.GetCas() == nil && rsp.GetStatus().GetCode() != 0 {
				log.Errorf("error processing update %+v: %s", union, rsp.GetStatus())
			}

			// sm.log.Debugf("Update: response union: %+v", rsp)
			batchCmdRsp.Union = append(batchCmdRsp.Union, rsp)
		}

		rspBuf, err := proto.Marshal(batchCmdRsp)
		if err != nil {
			return nil, err
		}
		entries[idx].Result = dbsm.Result{
			Value: uint64(len(entries[idx].Cmd)),
			Data:  rspBuf,
		}
	}

	lastEntry := entries[len(entries)-1]
	appliedIndex := uint64ToBytes(lastEntry.Index)
	syncWriteOptions := &pebble.WriteOptions{Sync: true}

	if sm.lastAppliedIndex >= lastEntry.Index {
		return nil, status.FailedPreconditionError("lastApplied not moving forward")
	}

	// If we have an active batch, bundle the LastAppliedIndex change into it
	// and commit it all at once. Otherwise just do a db set.
	if wb != nil {
		if err := wb.Set(constants.LastAppliedIndexKey, appliedIndex, nil /*ignored write options*/); err != nil {
			return nil, err
		}
		if err := wb.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
			return nil, err
		}
	} else {
		if err := sm.db.Set(constants.LastAppliedIndexKey, appliedIndex, syncWriteOptions); err != nil {
			return nil, err
		}
	}

	sm.lastAppliedIndex = lastEntry.Index
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
	reqBuf, ok := key.([]byte)
	if !ok {
		return nil, status.FailedPreconditionError("Cannot convert key to []byte")
	}

	db, err := sm.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	batchCmdReq := &rfpb.BatchCmdRequest{}
	if err := proto.Unmarshal(reqBuf, batchCmdReq); err != nil {
		return nil, err
	}
	batchCmdRsp := &rfpb.BatchCmdResponse{}
	for _, req := range batchCmdReq.GetUnion() {
		//sm.log.Debugf("Lookup: request union: %+v", req)
		rsp := sm.handleRead(db, req)
		//sm.log.Debugf("Lookup: response union: %+v", rsp)
		batchCmdRsp.Union = append(batchCmdRsp.Union, rsp)
	}

	rspBuf, err := proto.Marshal(batchCmdRsp)
	if err != nil {
		return nil, err
	}
	return rspBuf, nil
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
	return nil
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
	snap := sm.db.NewSnapshot()
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

func readDataFromReader(r *bufio.Reader) (io.Reader, int64, error) {
	count, err := binary.ReadVarint(r)
	if err != nil {
		return nil, 0, err
	}
	return io.LimitReader(r, count), count, nil
}

func (sm *Replica) SaveSnapshotToWriter(w io.Writer, snap *pebble.Snapshot, start, end []byte) error {
	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: keys.Key(start),
		UpperBound: keys.Key(end),
	})
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		kv := &rfpb.KV{
			Key:   iter.Key(),
			Value: iter.Value(),
		}
		protoBytes, err := proto.Marshal(kv)
		if err != nil {
			return err
		}

		protoLength := int64(len(protoBytes))
		if err := encodeDataToWriter(w, bytes.NewReader(protoBytes), protoLength); err != nil {
			return err
		}
	}
	return nil
}

func (sm *Replica) fetchFileToLocalStorage(ctx context.Context, fileRecord *rfpb.FileRecord) (*rfpb.StorageMetadata, int64, error) {
	rd := &rfpb.ReplicaDescriptor{
		ClusterId: sm.ClusterID,
		NodeId:    sm.NodeID,
	}
	writeCloserMetadata, err := sm.fileStorer.NewWriter(ctx, sm.fileDir, fileRecord)
	if err != nil {
		return nil, 0, err
	}
	readCloser, err := sm.store.ReadFileFromPeer(ctx, rd, fileRecord)
	if err != nil {
		return nil, 0, err
	}
	defer readCloser.Close()
	n, err := io.Copy(writeCloserMetadata, readCloser)
	if err != nil {
		return nil, 0, err
	}
	if err := writeCloserMetadata.Close(); err != nil {
		return nil, 0, err
	}
	return writeCloserMetadata.Metadata(), n, nil
}

func (sm *Replica) ApplySnapshotFromReader(r io.Reader, db ReplicaWriter) error {
	ctx := context.TODO()
	wb := db.NewBatch()
	defer wb.Close()

	// Delete everything in the current database first.
	if err := wb.DeleteRange(keys.Key{constants.MinByte}, keys.Key{constants.MaxByte}, nil /*ignored write options*/); err != nil {
		return err
	}

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
		if err := wb.Set(kv.Key, kv.Value, nil /*ignored write options*/); err != nil {
			return err
		}
		if !isLocalKey(kv.GetKey()) {
			fileMetadata := &rfpb.FileMetadata{}
			if err := proto.Unmarshal(kv.GetValue(), fileMetadata); err != nil {
				return err
			}
			newMetadata, _, err := sm.fetchFileToLocalStorage(ctx, fileMetadata.GetFileRecord())
			if err != nil {
				return err
			}
			if !proto.Equal(newMetadata, fileMetadata.GetStorageMetadata()) {
				sm.log.Errorf("Stored metadata differs after fetching file locally. Before %+v, after: %+v", fileMetadata.GetStorageMetadata(), newMetadata)
				return status.FailedPreconditionError("stored metadata changed when fetching file")
			}
		}
	}
	if err := db.Apply(wb, &pebble.WriteOptions{Sync: true}); err != nil {
		return err
	}
	return nil
}

type Record struct {
	PB    proto.Message
	Error error
}

func (sm *Replica) ParseSnapshot(ctx context.Context, r io.Reader) <-chan Record {
	ch := make(chan Record, 10)

	readBuf := bufio.NewReader(r)
	go func() {
		defer close(ch)
		for {
			r, count, err := readDataFromReader(readBuf)
			if err != nil {
				if err == io.EOF {
					break
				}
				ch <- Record{Error: err}
				break
			}
			protoBytes := make([]byte, count)
			n, err := io.ReadFull(r, protoBytes)
			if err != nil {
				ch <- Record{Error: err}
				break
			}
			if int64(n) != count {
				ch <- Record{
					Error: status.FailedPreconditionErrorf("Count %d != bytes read %d", count, n),
				}
				break
			}
			kv := &rfpb.KV{}
			if err := proto.Unmarshal(protoBytes, kv); err != nil {
				ch <- Record{Error: err}
				break
			}
			ch <- Record{PB: &rfpb.DirectWriteRequest{Kv: kv}}

			fileMetadata := &rfpb.FileMetadata{}
			if err := proto.Unmarshal(kv.GetValue(), fileMetadata); err == nil {
				if fileMetadata.GetStorageMetadata() != nil {
					ch <- Record{PB: &rfpb.FileWriteRequest{FileRecord: fileMetadata.GetFileRecord()}}
				}
			}
		}
	}()
	return ch
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
	return sm.SaveSnapshotToWriter(w, snap, []byte{constants.MinByte}, []byte{constants.MaxByte})
}

func (sm *Replica) SaveSnapshotRange(preparedSnap interface{}, w io.Writer, start, end []byte) error {
	snap, ok := preparedSnap.(*pebble.Snapshot)
	if !ok {
		return status.FailedPreconditionError("unable to coerce snapshot to *pebble.Snapshot")
	}
	defer snap.Close()
	return sm.SaveSnapshotToWriter(w, snap, start, end)
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
	newLastApplied, err := sm.getLastAppliedIndex(readDB)
	if err != nil {
		return err
	}
	if sm.lastAppliedIndex > newLastApplied {
		return status.FailedPreconditionErrorf("last applied not moving forward: %d > %d", sm.lastAppliedIndex, newLastApplied)
	}
	sm.lastAppliedIndex = newLastApplied
	sm.checkAndSetRangeDescriptor(readDB)
	return nil
}

func (sm *Replica) TestingDB() (pebbleutil.IPebbleDB, error) {
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
	if sm.leaser == nil {
		return nil
	}

	close(sm.quitChan)

	sm.leaser.Close()

	sm.rangeMu.Lock()
	rangeDescriptor := sm.rangeDescriptor
	sm.rangeMu.Unlock()

	if sm.store != nil && rangeDescriptor != nil {
		sm.store.RemoveRange(rangeDescriptor, sm)
	}
	return sm.db.Close()
}

// CreateReplica creates an ondisk statemachine.
func New(rootDir string, clusterID, nodeID uint64, store IStore) *Replica {
	fileDir := filepath.Join(rootDir, fmt.Sprintf("files-c%dn%d", clusterID, nodeID))
	if err := disk.EnsureDirectoryExists(fileDir); err != nil {
		log.Errorf("Error creating fileDir %q for replica: %s", fileDir, err)
	}
	r := &Replica{
		leaser:     nil,
		rootDir:    rootDir,
		fileDir:    fileDir,
		ClusterID:  clusterID,
		NodeID:     nodeID,
		timerMu:    &sync.Mutex{},
		store:      store,
		log:        log.NamedSubLogger(fmt.Sprintf("c%dn%d", clusterID, nodeID)),
		fileStorer: filestore.New(true /*=isolateByGroupIDs*/),
		quitChan:   make(chan struct{}),
		accesses:   make(chan *accessTimeUpdate, *atimeBufferSize),
	}
	go r.processAccessTimeUpdates()
	return r
}
