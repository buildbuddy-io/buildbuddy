package replica_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type fileReadFn func(fileRecord *rfpb.FileRecord) (io.ReadCloser, error)

type fakeStore struct {
	fileReadFn fileReadFn
}

func (fs *fakeStore) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica)    {}
func (fs *fakeStore) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {}
func (fs *fakeStore) ReadFileFromPeer(ctx context.Context, except *rfpb.ReplicaDescriptor, fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
	if fs.fileReadFn != nil {
		return fs.fileReadFn(fileRecord)
	}
	return nil, status.NotFoundErrorf("file not found")
}
func (fs *fakeStore) GetReplica(rangeID uint64) (*replica.Replica, error) {
	return nil, nil
}
func (fs *fakeStore) Sender() *sender.Sender {
	return nil
}
func (fs *fakeStore) WithFileReadFn(fn fileReadFn) *fakeStore {
	fs.fileReadFn = fn
	return fs
}

func TestOpenCloseReplica(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
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

func writeDefaultRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica) {
	// Do a direct write of the range local range.
	rdBuf, err := proto.Marshal(&rfpb.RangeDescriptor{
		Left:       []byte{constants.MinByte},
		Right:      []byte("z"),
		RangeId:    1,
		Generation: 1,
	})
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

func TestReplicaDirectReadWrite(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("key-name"),
			Value: []byte("key-value"),
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
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	directRead, err := readBatch.DirectReadResponse(0)
	require.NoError(t, err)

	require.Equal(t, []byte("key-value"), directRead.GetKv().GetValue())

	err = repl.Close()
	require.NoError(t, err)
}

func TestReplicaIncrement(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
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
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("key-name"),
			Value: []byte("key-value"),
		},
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Do a CAS and verify:
	//   1) the value is not set
	//   2) the current value is returned.
	entry = em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   []byte("key-name"),
			Value: []byte("new-key-value"),
		},
		ExpectedValue: []byte("bogus-expected-value"),
	}))
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err := readBatch.CASResponse(0)
	require.True(t, status.IsFailedPreconditionError(err))
	require.Equal(t, []byte("key-value"), casRsp.GetKv().GetValue())

	// Do a CAS with the correct expected value and ensure
	// the value was written.
	entry = em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   []byte("key-name"),
			Value: []byte("new-key-value"),
		},
		ExpectedValue: []byte("key-value"),
	}))
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err = readBatch.CASResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("new-key-value"), casRsp.GetKv().GetValue())

	err = repl.Close()
	require.NoError(t, err)
}

func TestReplicaScan(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Do a DirectWrite of some range descriptors.
	batch := rbuilder.NewBatchBuilder()
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey([]byte("b")),
			Value: []byte("range-1"),
		},
	})
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey([]byte("c")),
			Value: []byte("range-2"),
		},
	})
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey([]byte("d")),
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
		Left:     keys.RangeMetaKey([]byte("b")),
		Right:    keys.RangeMetaKey([]byte("c")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	scanRsp, err := readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-1"), scanRsp.GetKvs()[0].GetValue())

	// Scan c-d.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("c")),
		Right:    keys.RangeMetaKey([]byte("d")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err = repl.Lookup(buf)
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-2"), scanRsp.GetKvs()[0].GetValue())

	// Scan d-*.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("d")),
		Right:    keys.RangeMetaKey([]byte("z")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err = repl.Lookup(buf)
	require.NoError(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.NoError(t, err)
	require.Equal(t, []byte("range-3"), scanRsp.GetKvs()[0].GetValue())

	// Scan the full range.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("a")),
		Right:    keys.RangeMetaKey([]byte("z")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
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

func TestReplicaFileWrite(t *testing.T) {
	ctx := context.Background()
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Write a file to the replica's data dir.
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	fileRecord := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest: d,
	}

	// Mismatched generation should fail with an OutOfRange error.
	_, err = repl.Writer(ctx, &rfpb.Header{RangeId: 1, Generation: 2}, fileRecord)
	require.ErrorContains(t, err, constants.RangeNotCurrentMsg)
	require.True(t, status.IsOutOfRangeError(err))

	header := &rfpb.Header{RangeId: 1, Generation: 1}

	writeCommitter, err := repl.Writer(ctx, header, fileRecord)
	require.NoError(t, err)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	// Do a FileWrite for the just written file: it should succeed.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.FileWriteRequest{
			FileRecord: fileRecord,
		}))
		entries := []dbsm.Entry{entry}
		writeRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(writeRsp))
	}

	// Do a FileWrite for a file that does not exist: it should fail.
	{
		d, _ := testdigest.NewRandomDigestBuf(t, 1000)
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.FileWriteRequest{
			FileRecord: &rfpb.FileRecord{
				Isolation: &rfpb.Isolation{
					CacheType:   rfpb.Isolation_CAS_CACHE,
					PartitionId: "default",
					GroupId:     interfaces.AuthAnonymousUser,
				},
				Digest: d,
			},
		}))
		entries := []dbsm.Entry{entry}
		writeRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(writeRsp))

		writeBatch := rbuilder.NewBatchResponse(writeRsp)
		_, err = writeBatch.FileWriteResponse(0)
		require.NotNil(t, err)
		require.True(t, status.IsFailedPreconditionError(err), err)
	}
}

func TestReplicaFileWriteSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Write a file to the replica's data dir.
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)

	store = store.WithFileReadFn(func(fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
		if fileRecord.GetDigest().GetHash() == d.GetHash() {
			return io.NopCloser(bytes.NewReader(buf)), nil
		}
		return nil, status.NotFoundError("File not found")
	})

	fileRecord := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest: d,
	}
	header := &rfpb.Header{RangeId: 1, Generation: 1}

	writeCommitter, err := repl.Writer(ctx, header, fileRecord)
	require.NoError(t, err)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	readCloser, err := repl.Reader(ctx, header, fileRecord, 0, 0)
	require.NoError(t, err)
	require.Equal(t, d.GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())

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
	repl2 := replica.New(rootDir2, 2, 2, store)
	require.NotNil(t, repl2)
	_, err = repl2.Open(stopc)
	require.NoError(t, err)

	err = repl2.RecoverFromSnapshot(snapFile, nil /*=quitChan*/)
	require.NoError(t, err)

	// Verify that the file is readable.
	readCloser, err = repl2.Reader(ctx, header, fileRecord, 0, 0)
	require.NoError(t, err)
	require.Equal(t, d.GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())
}

func TestReplicaFileWriteDelete(t *testing.T) {
	ctx := context.Background()
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Write a file to the replica's data dir.
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	fileRecord := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest: d,
	}

	header := &rfpb.Header{RangeId: 1, Generation: 1}
	writeCommitter, err := repl.Writer(ctx, header, fileRecord)
	require.NoError(t, err)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	// Do a FileWrite for the just written file: it should succeed.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.FileWriteRequest{
			FileRecord: fileRecord,
		}))
		entries := []dbsm.Entry{entry}
		writeRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(writeRsp))
	}
	// Verify that the file is readable.
	{
		readCloser, err := repl.Reader(ctx, header, fileRecord, 0, 0)
		require.NoError(t, err)
		require.Equal(t, d.GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())
	}
	// Delete the file.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.FileDeleteRequest{
			FileRecord: fileRecord,
		}))
		entries := []dbsm.Entry{entry}
		deleteRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(deleteRsp))
	}
	// Verify that the file is no longer readable and reading it returns a
	// NotFoundError.
	{
		_, err := repl.Reader(ctx, header, fileRecord, 0, 0)
		require.NotNil(t, err)
		require.True(t, status.IsNotFoundError(err), err)
	}
}

func TestReplicaSplitLease(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Acquire a split lease on a replica.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SplitLeaseRequest{
			DurationSeconds: 5,
		}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.NoError(t, err)
	}

	// Ensure that a direct write now fails.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("key-name"),
				Value: []byte("key-value"),
			},
		}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.Error(t, err)
	}

	// Reset the lease to 10 seconds from now.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SplitLeaseRequest{
			DurationSeconds: 10,
		}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.NoError(t, err)
	}

	// Release the split lease on a replica.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SplitReleaseRequest{}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.NoError(t, err)
	}

	// A direct write should now succeed again without error.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("key-name"),
				Value: []byte("key-value"),
			},
		}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.NoError(t, err)
	}

	// Acquire another split lease on the replica, but one that should
	// expire immediately.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SplitLeaseRequest{
			DurationSeconds: 0,
		}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.NoError(t, err)
	}

	// A direct write should succeed immediately without error.
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("key-name"),
				Value: []byte("key-value"),
			},
		}))
		entries := []dbsm.Entry{entry}
		_, err := repl.Update(entries)
		require.NoError(t, err)
	}

	err = repl.Close()
	require.NoError(t, err)
}
