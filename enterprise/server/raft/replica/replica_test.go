package replica_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	defaultPartition = "default"
	anotherPartition = "another"

	partitions = []disk.Partition{
		{ID: defaultPartition, MaxSizeBytes: 10_000},
		{ID: anotherPartition, MaxSizeBytes: 10_000},
	}
)

type fileReadFn func(fileRecord *rfpb.FileRecord) (io.ReadCloser, error)

type fakeStore struct {
	fileReadFn fileReadFn
}

func (fs *fakeStore) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica)    {}
func (fs *fakeStore) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {}
func (fs *fakeStore) Sender() *sender.Sender {
	return nil
}
func (fs *fakeStore) AddPeer(ctx context.Context, sourceShardID, newShardID uint64) error {
	return nil
}
func (fs *fakeStore) SnapshotCluster(ctx context.Context, shardID uint64) error {
	return nil
}
func (fs *fakeStore) WithFileReadFn(fn fileReadFn) *fakeStore {
	fs.fileReadFn = fn
	return fs
}

func newTestReplica(t *testing.T, rootDir string, shardID, replicaID uint64, store replica.IStore) *replica.Replica {
	db, err := pebble.Open(rootDir, "test", &pebble.Options{})
	require.NoError(t, err)

	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})

	return replica.New(leaser, shardID, replicaID, store, nil /*=usageUpdates=*/)
}

func TestOpenCloseReplica(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)

	err = repl.Close()
	require.NoError(t, err)
}

type entryMaker struct {
	index uint64
	t     *testing.T
}

func newEntryMaker(t *testing.T) *entryMaker {
	return &entryMaker{
		t: t,
	}
}
func (em *entryMaker) makeEntry(batch *rbuilder.BatchBuilder) dbsm.Entry {
	em.index += 1
	buf, err := batch.ToBuf()
	if err != nil {
		em.t.Fatalf("Error making entry: %s", err)
	}
	return dbsm.Entry{Cmd: buf, Index: em.index}
}

func writeLocalRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica, rd *rfpb.RangeDescriptor) {
	rdBuf, err := proto.Marshal(rd)
	require.NoError(t, err)
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}))
	entries := []dbsm.Entry{entry}
	writeRsp, err := r.Update(entries)
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))
}

func reader(t *testing.T, r *replica.Replica, h *rfpb.Header, fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
	fs := filestore.New()

	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.GetRequest{
		Key: fileMetadataKey,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := r.Lookup(buf)
	require.NoError(t, err)
	readBatch := rbuilder.NewBatchResponse(readRsp)
	getRsp, err := readBatch.GetResponse(0)
	if err != nil {
		return nil, err
	}
	md := getRsp.GetFileMetadata()
	rc, err := fs.InlineReader(md.GetStorageMetadata().GetInlineMetadata(), 0, 0)
	require.NoError(t, err)
	return rc, nil
}

func writer(t *testing.T, em *entryMaker, r *replica.Replica, h *rfpb.Header, fileRecord *rfpb.FileRecord) interfaces.CommittedWriteCloser {
	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	writeCloserMetadata := fs.InlineWriter(context.TODO(), fileRecord.GetDigest().GetSizeBytes())

	wc := ioutil.NewCustomCommitWriteCloser(writeCloserMetadata)
	wc.CommitFn = func(bytesWritten int64) error {
		now := time.Now()
		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: writeCloserMetadata.Metadata(),
			StoredSizeBytes: bytesWritten,
			LastModifyUsec:  now.UnixMicro(),
			LastAccessUsec:  now.UnixMicro(),
		}
		protoBytes, err := proto.Marshal(md)
		if err != nil {
			return err
		}
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   fileMetadataKey,
				Value: protoBytes,
			},
		}))
		entries := []dbsm.Entry{entry}
		writeRsp, err := r.Update(entries)
		if err != nil {
			return err
		}
		require.Equal(t, 1, len(writeRsp))

		return rbuilder.NewBatchResponse(writeRsp[0].Result.Data).AnyError()
	}
	return wc
}

func writeDefaultRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica) {
	writeLocalRangeDescriptor(t, em, r, &rfpb.RangeDescriptor{
		Start:      keys.Key{constants.UnsplittableMaxByte},
		End:        keys.MaxByte,
		RangeId:    1,
		Generation: 1,
	})
}

func randomRecord(t *testing.T, partition string, sizeBytes int64) (*rfpb.FileRecord, []byte) {
	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	return &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   r.GetCacheType(),
			PartitionId: partition,
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}, buf
}

type replicaTester struct {
	t    *testing.T
	em   *entryMaker
	repl *replica.Replica
}

func newWriteTester(t *testing.T, em *entryMaker, repl *replica.Replica) *replicaTester {
	return &replicaTester{t, em, repl}
}

func (wt *replicaTester) writeRandom(header *rfpb.Header, partition string, sizeBytes int64) *rfpb.FileRecord {
	fr, buf := randomRecord(wt.t, partition, sizeBytes)
	wc := writer(wt.t, wt.em, wt.repl, header, fr)
	_, err := wc.Write(buf)
	require.NoError(wt.t, err)
	require.NoError(wt.t, wc.Commit())
	require.NoError(wt.t, wc.Close())
	return fr
}

func (wt *replicaTester) delete(fileRecord *rfpb.FileRecord) {
	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(wt.t, err)

	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(wt.t, err)

	entry := wt.em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DeleteRequest{
		Key: fileMetadataKey,
	}))
	entries := []dbsm.Entry{entry}
	deleteRsp, err := wt.repl.Update(entries)
	require.NoError(wt.t, err)
	require.Equal(wt.t, 1, len(deleteRsp))
}

func TestReplicaDirectReadWrite(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	md := &rfpb.FileMetadata{StoredSizeBytes: 123}
	val, err := proto.Marshal(md)
	require.NoError(t, err)

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("key-name"),
			Value: val,
		},
	}))
	entries := []dbsm.Entry{entry}
	writeRsp, err := repl.Update(entries)
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Do a DirectRead and verify the value is was written.
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: []byte("key-name"),
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	directRead, err := readBatch.DirectReadResponse(0)
	require.NoError(t, err)

	require.Equal(t, val, directRead.GetKv().GetValue())

	err = repl.Close()
	require.NoError(t, err)
}

func TestReplicaIncrement(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   []byte("incr-key"),
		Delta: 1,
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Make sure the response holds the new value.
	incrBatch := rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	incrRsp, err := incrBatch.IncrementResponse(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), incrRsp.GetValue())

	// Increment the same key again by a different value.
	entry = em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   []byte("incr-key"),
		Delta: 3,
	}))
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Make sure the response holds the new value.
	incrBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	incrRsp, err = incrBatch.IncrementResponse(0)
	require.NoError(t, err)
	require.Equal(t, uint64(4), incrRsp.GetValue())

	err = repl.Close()
	require.NoError(t, err)
}

func TestReplicaCAS(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Do a write.
	rt := newWriteTester(t, em, repl)
	header := &rfpb.Header{RangeId: 1, Generation: 1}
	fr := rt.writeRandom(header, defaultPartition, 100)

	fs := filestore.New()
	key, err := fs.PebbleKey(fr)
	require.NoError(t, err)

	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	// Do a DirectRead and verify the value was written.
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: fileMetadataKey,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)
	readBatch := rbuilder.NewBatchResponse(readRsp)
	directRead, err := readBatch.DirectReadResponse(0)
	require.NoError(t, err)

	mdBuf := directRead.GetKv().GetValue()

	// Do a CAS and verify:
	//   1) the value is not set
	//   2) the current value is returned.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: []byte{},
		},
		ExpectedValue: []byte("bogus-expected-value"),
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err := readBatch.CASResponse(0)
	require.True(t, status.IsFailedPreconditionError(err))
	require.Equal(t, mdBuf, casRsp.GetKv().GetValue())

	// Do a CAS with the correct expected value and ensure
	// the value was written.
	entry = em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: []byte{},
		},
		ExpectedValue: mdBuf,
	}))
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err = readBatch.CASResponse(0)
	require.NoError(t, err)
	require.Nil(t, casRsp.GetKv().GetValue())

	err = repl.Close()
	require.NoError(t, err)
}

func TestReplicaScan(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeLocalRangeDescriptor(t, em, repl, &rfpb.RangeDescriptor{
		Start:      keys.Key{constants.UnsplittableMaxByte},
		End:        keys.MaxByte,
		RangeId:    1,
		Generation: 1,
	})

	// Do a DirectWrite of some range descriptors.
	batch := rbuilder.NewBatchBuilder()
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("b"),
			Value: []byte("range-1"),
		},
	})
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("c"),
			Value: []byte("range-2"),
		},
	})
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("d"),
			Value: []byte("range-3"),
		},
	})
	entry := em.makeEntry(batch)
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Ensure that scan reads just the ranges we want.
	// Scan b-c.
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    []byte("b"),
		End:      []byte("c"),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	scanRsp, err := readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-1"), scanRsp.GetKvs()[0].GetValue())

	// Scan c-d.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    []byte("c"),
		End:      []byte("d"),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err = repl.Lookup(buf)
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-2"), scanRsp.GetKvs()[0].GetValue())

	// Scan d-*.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    []byte("d"),
		End:      []byte("z"),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err = repl.Lookup(buf)
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-3"), scanRsp.GetKvs()[0].GetValue())

	// Scan the full range.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    []byte("a"),
		End:      []byte("z"),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err = repl.Lookup(buf)
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-1"), scanRsp.GetKvs()[0].GetValue())
	require.Equal(t, []byte("range-2"), scanRsp.GetKvs()[1].GetValue())
	require.Equal(t, []byte("range-3"), scanRsp.GetKvs()[2].GetValue())

	err = repl.Close()
	require.NoError(t, err)
}

func TestReplicaFileWriteSnapshotRestore(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Write a file to the replica's data dir.
	r, buf := testdigest.RandomCASResourceBuf(t, 1000)

	store = store.WithFileReadFn(func(fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
		if fileRecord.GetDigest().GetHash() == r.GetDigest().GetHash() {
			return io.NopCloser(bytes.NewReader(buf)), nil
		}
		return nil, status.NotFoundError("File not found")
	})

	fileRecord := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}
	header := &rfpb.Header{RangeId: 1, Generation: 1}

	writeCommitter := writer(t, em, repl, header, fileRecord)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	readCloser, err := reader(t, repl, header, fileRecord)
	require.NoError(t, err)
	require.Equal(t, r.GetDigest().GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())

	// Create a snapshot of the replica.
	snapI, err := repl.PrepareSnapshot()
	require.NoError(t, err)

	baseDir := testfs.MakeTempDir(t)
	snapFile, err := os.CreateTemp(baseDir, "snapfile-*")
	require.NoError(t, err)
	snapFileName := snapFile.Name()
	defer os.Remove(snapFileName)

	err = repl.SaveSnapshot(snapI, snapFile, nil /*=quitChan*/)
	require.NoError(t, err)
	snapFile.Seek(0, 0)

	// Restore a new replica from the created snapshot.
	rootDir2 := testfs.MakeTempDir(t)
	repl2 := newTestReplica(t, rootDir2, 2, 2, store)
	require.NotNil(t, repl2)
	_, err = repl2.Open(stopc)
	require.NoError(t, err)

	err = repl2.RecoverFromSnapshot(snapFile, nil /*=quitChan*/)
	require.NoError(t, err)

	// Verify that the file is readable.
	readCloser, err = reader(t, repl2, header, fileRecord)
	require.NoError(t, err)
	require.Equal(t, r.GetDigest().GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())
}

func TestReplicaFileWriteDelete(t *testing.T) {
	fs := filestore.New()
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Write a file to the replica's data dir.
	r, buf := testdigest.RandomCASResourceBuf(t, 1000)
	fileRecord := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}

	header := &rfpb.Header{RangeId: 1, Generation: 1}
	writeCommitter := writer(t, em, repl, header, fileRecord)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	// Verify that the file is readable.
	{
		readCloser, err := reader(t, repl, header, fileRecord)
		require.NoError(t, err)
		require.Equal(t, r.GetDigest().GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())
	}
	// Delete the file.
	{
		key, err := fs.PebbleKey(fileRecord)
		require.NoError(t, err)
		fileMetadataKey, err := key.Bytes(filestore.Version5)
		require.NoError(t, err)

		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DeleteRequest{
			Key: fileMetadataKey,
		}))
		entries := []dbsm.Entry{entry}
		deleteRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(deleteRsp))
	}
	// Verify that the file is no longer readable and reading it returns a
	// NotFoundError.
	{
		_, err := reader(t, repl, header, fileRecord)
		require.NotNil(t, err)
		require.True(t, status.IsNotFoundError(err), err)
	}
}

func TestUsage(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	rt := newWriteTester(t, em, repl)

	header := &rfpb.Header{RangeId: 1, Generation: 1}
	frDefault := rt.writeRandom(header, defaultPartition, 1000)
	rt.writeRandom(header, defaultPartition, 500)
	rt.writeRandom(header, anotherPartition, 100)
	rt.writeRandom(header, anotherPartition, 200)
	rt.writeRandom(header, anotherPartition, 300)

	{
		ru, err := repl.Usage()
		require.NoError(t, err)
		require.EqualValues(t, 2100, ru.GetEstimatedDiskBytesUsed())
		require.Len(t, ru.GetPartitions(), 2)

		for _, usage := range ru.GetPartitions() {
			switch usage.GetPartitionId() {
			case defaultPartition:
				require.EqualValues(t, 1500, usage.GetSizeBytes())
				require.EqualValues(t, 2, usage.GetTotalCount())
			case anotherPartition:
				require.EqualValues(t, 600, usage.GetSizeBytes())
				require.EqualValues(t, 3, usage.GetTotalCount())
			}
		}
	}

	// Delete a single record and verify updated usage.
	rt.delete(frDefault)

	{
		ru, err := repl.Usage()
		require.NoError(t, err)

		require.EqualValues(t, 1100, ru.GetEstimatedDiskBytesUsed())
		require.Len(t, ru.GetPartitions(), 2)

		for _, usage := range ru.GetPartitions() {
			switch usage.GetPartitionId() {
			case defaultPartition:
				require.EqualValues(t, 500, usage.GetSizeBytes())
				require.EqualValues(t, 1, usage.GetTotalCount())
			case anotherPartition:
				require.EqualValues(t, 600, usage.GetSizeBytes())
				require.EqualValues(t, 3, usage.GetTotalCount())
			}
		}
	}
}
