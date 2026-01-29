package replica_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
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

func TestOpenCloseReplica(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
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

func writeRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica, key []byte, rd *rfpb.RangeDescriptor) {
	rdBuf, err := proto.Marshal(rd)
	require.NoError(t, err)
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   key,
			Value: rdBuf,
		},
	}))
	entries := []dbsm.Entry{entry}
	writeRsp, err := r.Update(entries)
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))
}

func writeLocalRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica, rd *rfpb.RangeDescriptor) {
	writeRangeDescriptor(t, em, r, constants.LocalRangeKey, rd)
}

func writeMetaRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica, rd *rfpb.RangeDescriptor) {
	writeRangeDescriptor(t, em, r, keys.RangeMetaKey(rd.GetEnd()), rd)
}

func reader(t *testing.T, r *replica.Replica, h *rfpb.Header, fileRecord *sgpb.FileRecord) (io.ReadCloser, error) {
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

func writer(t *testing.T, em *entryMaker, r *replica.Replica, h *rfpb.Header, fileRecord *sgpb.FileRecord) interfaces.CommittedWriteCloser {
	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	writeCloserMetadata := fs.InlineWriter(context.TODO(), fileRecord.GetDigest().GetSizeBytes())

	wc := ioutil.NewCustomCommitWriteCloser(writeCloserMetadata)
	wc.CommitFn = func(bytesWritten int64) error {
		now := time.Now()
		md := &sgpb.FileMetadata{
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

func writeDefaultRangeDescriptor(t *testing.T, em *entryMaker, r *replica.Replica) *rfpb.RangeDescriptor {
	rd := &rfpb.RangeDescriptor{
		Start:      keys.Key{constants.UnsplittableMaxByte},
		End:        keys.MaxByte,
		RangeId:    1,
		Generation: 1,
	}
	writeLocalRangeDescriptor(t, em, r, rd)
	return rd
}

func randomRecord(t *testing.T, partition string, sizeBytes int64) (*sgpb.FileRecord, []byte) {
	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	return &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   r.GetCacheType(),
			PartitionId: partition,
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}, buf
}

func directRead(t *testing.T, repl *testutil.TestingReplica, key []byte) (*rfpb.DirectReadResponse, error) {
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: key,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	return readBatch.DirectReadResponse(0)
}

func verifyReplicaHasLocalRange(t *testing.T, repl *testutil.TestingReplica, rd *rfpb.RangeDescriptor) {
	rsp, err := directRead(t, repl, constants.LocalRangeKey)
	require.NoError(t, err)
	gotRD := &rfpb.RangeDescriptor{}
	err = proto.Unmarshal(rsp.GetKv().GetValue(), gotRD)
	require.NoError(t, err)
	require.True(t, proto.Equal(rd, gotRD))
}

type replicaTester struct {
	t    *testing.T
	em   *entryMaker
	repl *replica.Replica
}

func newWriteTester(t *testing.T, em *entryMaker, repl *replica.Replica) *replicaTester {
	return &replicaTester{t, em, repl}
}

func (wt *replicaTester) writeRandom(header *rfpb.Header, partition string, sizeBytes int64) *sgpb.FileRecord {
	fr, buf := randomRecord(wt.t, partition, sizeBytes)
	wc := writer(wt.t, wt.em, wt.repl, header, fr)
	_, err := wc.Write(buf)
	require.NoError(wt.t, err)
	require.NoError(wt.t, wc.Commit())
	require.NoError(wt.t, wc.Close())
	return fr
}

func (wt *replicaTester) delete(fileRecord *sgpb.FileRecord) {
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
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	md := &sgpb.FileMetadata{StoredSizeBytes: 123}
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
	rsp, err := directRead(t, repl, []byte("key-name"))
	require.NoError(t, err)
	require.Equal(t, val, rsp.GetKv().GetValue())
}

func TestReplicaIncrementSnapshotRestore(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	session := &rfpb.Session{
		Id:    []byte(uuid.New()),
		Index: 1,
	}

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
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

	// Make sure the stored value is direct-readable.
	rsp, err := directRead(t, repl, constants.LastRangeIDKey)
	require.NoError(t, err)
	val := binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
	require.Equal(t, uint64(1), val)

	// Write the same request again.
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Make sure the response holds the same value
	incrBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	incrRsp, err = incrBatch.IncrementResponse(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), incrRsp.GetValue())

	rsp, err = directRead(t, repl, constants.LastRangeIDKey)
	require.NoError(t, err)
	val = binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
	require.Equal(t, uint64(1), val)

	session.Index = 2
	// Increment the same key again by a different value.
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
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

	rsp, err = directRead(t, repl, constants.LastRangeIDKey)
	require.NoError(t, err)
	val = binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
	require.Equal(t, uint64(4), val)

	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
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
	rsp, err = directRead(t, repl, constants.LastRangeIDKey)
	require.NoError(t, err)
	val = binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
	require.Equal(t, uint64(4), val)

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
	repl2 := testutil.NewTestingReplica(t, 1, 2)
	require.NotNil(t, repl2)
	t.Cleanup(func() {
		err := repl2.Close()
		require.NoError(t, err)
	})
	_, err = repl2.Open(stopc)
	require.NoError(t, err)

	// read from the key, it should hold the same value
	rsp, err = directRead(t, repl, constants.LastRangeIDKey)
	require.NoError(t, err)
	val = binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
	require.Equal(t, uint64(4), val)

	// Increment still work after restored from snapshot
	session.Index = 3
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: 2,
	}))
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Make sure the response holds the new value.
	incrBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	incrRsp, err = incrBatch.IncrementResponse(0)
	require.NoError(t, err)
	require.Equal(t, uint64(6), incrRsp.GetValue())
	rsp, err = directRead(t, repl, constants.LastRangeIDKey)
	require.NoError(t, err)
	val = binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
	require.Equal(t, uint64(6), val)
}

func TestSessionIndexMismatchError(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	session := &rfpb.Session{
		Id:    []byte(uuid.New()),
		Index: 1,
	}

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   []byte("incr-key"),
		Delta: 1,
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	session.Index = 0
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   []byte("incr-key"),
		Delta: 1,
	}))
	entries, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	result := entries[0].Result
	require.Equal(t, constants.EntryErrorValue, int(result.Value))

	status := &statuspb.Status{}
	err = proto.Unmarshal(result.Data, status)
	require.NoError(t, err)
	require.Contains(t, status.String(), fmt.Sprintf("session (id=\\\"%s\\\") index mismatch", session.Id))
}

func TestReplicaCAS(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	// Do a write.
	rt := newWriteTester(t, em, repl.Replica)
	header := &rfpb.Header{RangeId: 1, Generation: 1}
	fr := rt.writeRandom(header, defaultPartition, 100)

	fs := filestore.New()
	key, err := fs.PebbleKey(fr)
	require.NoError(t, err)

	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	// Do a DirectRead and verify the value was written.
	rsp, err := directRead(t, repl, fileMetadataKey)
	require.NoError(t, err)

	mdBuf := rsp.GetKv().GetValue()

	// Do a CAS and verify:
	//   1) the value is not set
	//   2) the current value is returned.
	session := &rfpb.Session{
		Id:    []byte(uuid.New()),
		Index: 1,
	}
	entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: []byte{},
		},
		ExpectedValue: []byte("bogus-expected-value"),
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err := readBatch.CASResponse(0)
	require.True(t, status.IsFailedPreconditionError(err))
	require.Equal(t, mdBuf, casRsp.GetKv().GetValue())

	// Do a CAS with the correct expected value and ensure
	// the value was written.
	session.Index++
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.CASRequest{
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

	// Do the same CAS again with same session
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.CASRequest{
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
}

func TestReplicaScan(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeLocalRangeDescriptor(t, em, repl.Replica, &rfpb.RangeDescriptor{
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
}

func TestReplicaFetchRanges(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	ranges := []*rfpb.RangeDescriptor{
		{
			Start:      constants.MetaRangePrefix,
			End:        keys.Key{constants.UnsplittableMaxByte},
			RangeId:    1,
			Generation: 1,
			Replicas: []*rfpb.ReplicaDescriptor{
				{RangeId: 1, ReplicaId: 1, Nhid: proto.String("nhid-1")},
			},
		},
		{
			Start:      keys.Key{constants.UnsplittableMaxByte},
			End:        keys.Key("a"),
			RangeId:    2,
			Generation: 1,
			Replicas: []*rfpb.ReplicaDescriptor{
				{RangeId: 2, ReplicaId: 1, Nhid: proto.String("nhid-1")},
				{RangeId: 2, ReplicaId: 2, Nhid: proto.String("nhid-2")},
			},
		},
		{
			Start:      keys.Key("a"),
			End:        keys.Key("b"),
			RangeId:    3,
			Generation: 1,
			Replicas: []*rfpb.ReplicaDescriptor{
				{RangeId: 3, ReplicaId: 1, Nhid: proto.String("nhid-2")},
			},
		},
		{
			Start:      keys.Key("b"),
			End:        keys.MaxByte,
			RangeId:    4,
			Generation: 1,
			Replicas: []*rfpb.ReplicaDescriptor{
				{RangeId: 4, ReplicaId: 1, Nhid: proto.String("nhid-3")},
			},
		},
	}

	for _, rd := range ranges {
		writeMetaRangeDescriptor(t, em, repl.Replica, rd)
	}

	testCases := []struct {
		name        string
		req         *rfpb.FetchRangesRequest
		expected    []*rfpb.RangeDescriptor
		expectError bool
	}{
		{
			name: "filter by range IDs",
			req: &rfpb.FetchRangesRequest{
				RangeIds: []uint64{3, 4},
			},
			expected: []*rfpb.RangeDescriptor{ranges[2], ranges[3]},
		},
		{
			name: "filter by nhid-1",
			req: &rfpb.FetchRangesRequest{
				Nhid: "nhid-1",
			},
			expected: []*rfpb.RangeDescriptor{ranges[0], ranges[1]},
		},
		{
			name: "filter by nhid-2",
			req: &rfpb.FetchRangesRequest{
				Nhid: "nhid-2",
			},
			expected: []*rfpb.RangeDescriptor{ranges[1], ranges[2]},
		},
		{
			name: "OR logic: range_ids and nhid",
			req: &rfpb.FetchRangesRequest{
				RangeIds: []uint64{1},
				Nhid:     "nhid-2",
			},
			expected: []*rfpb.RangeDescriptor{ranges[0], ranges[1], ranges[2]},
		},
		{
			name: "OR logic: non-overlapping sets",
			req: &rfpb.FetchRangesRequest{
				RangeIds: []uint64{4},
				Nhid:     "nhid-1",
			},
			expected: []*rfpb.RangeDescriptor{ranges[0], ranges[1], ranges[3]},
		},
		{
			name: "non-existent nhid",
			req: &rfpb.FetchRangesRequest{
				Nhid: "nhid-nonexistent",
			},
			expected: []*rfpb.RangeDescriptor{},
		},
		{
			name:        "neither range_ids nor nhid specified",
			req:         &rfpb.FetchRangesRequest{},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := rbuilder.NewBatchBuilder().Add(tc.req).ToBuf()
			require.NoError(t, err)
			readRsp, err := repl.Lookup(buf)
			require.NoError(t, err)

			readBatch := rbuilder.NewBatchResponse(readRsp)
			fetchRsp, err := readBatch.FetchRangesResponse(0)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.ElementsMatch(t, tc.expected, fetchRsp.GetRanges())
			}
		})
	}
}

func TestReplicaFileWriteSnapshotRestore(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	// Write a file to the replica's data dir.
	r, buf := testdigest.RandomCASResourceBuf(t, 1000)

	fileRecord := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}
	header := &rfpb.Header{RangeId: 1, Generation: 1}

	writeCommitter := writer(t, em, repl.Replica, header, fileRecord)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	readCloser, err := reader(t, repl.Replica, header, fileRecord)
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
	repl2 := testutil.NewTestingReplica(t, 2, 2)
	require.NotNil(t, repl2)
	t.Cleanup(func() {
		err := repl2.Close()
		require.NoError(t, err)
	})
	_, err = repl2.Open(stopc)
	require.NoError(t, err)

	err = repl2.RecoverFromSnapshot(snapFile, nil /*=quitChan*/)
	require.NoError(t, err)

	// Verify that the file is readable.
	readCloser, err = reader(t, repl2.Replica, header, fileRecord)
	require.NoError(t, err)
	require.Equal(t, r.GetDigest().GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())
}

func TestApplySnapshotEntriesDeleted(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	rt := newWriteTester(t, em, repl.Replica)
	header := &rfpb.Header{RangeId: 1, Generation: 1}
	fr1 := rt.writeRandom(header, defaultPartition, 1000)
	fr2 := rt.writeRandom(header, defaultPartition, 1000)

	localSessionKey := keys.MakeKey(constants.SessionPrefix, []byte("abcd"))

	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   localSessionKey,
			Value: []byte("12345"),
		},
	}))
	entries := []dbsm.Entry{entry}
	rsp, err := repl.Update(entries)
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())

	// Create a snapshot of the replica.
	snapI, err := repl.PrepareSnapshot()
	require.NoError(t, err)

	baseDir := testfs.MakeTempDir(t)
	snapFile1, err := os.CreateTemp(baseDir, "snapfile1-*")
	require.NoError(t, err)
	snapFileName1 := snapFile1.Name()
	defer os.Remove(snapFileName1)

	err = repl.SaveSnapshot(snapI, snapFile1, nil /*=quitChan*/)
	require.NoError(t, err)
	snapFile1.Seek(0, 0)

	{
		// delete some data
		entry1 := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectDeleteRequest{
			Key: localSessionKey,
		}))
		entries := []dbsm.Entry{entry1}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
	rt.delete(fr2)

	// Create a snapshot of the replica.
	snapI, err = repl.PrepareSnapshot()
	require.NoError(t, err)

	snapFile2, err := os.CreateTemp(baseDir, "snapfile2-*")
	require.NoError(t, err)
	snapFileName2 := snapFile2.Name()
	defer os.Remove(snapFileName2)

	err = repl.SaveSnapshot(snapI, snapFile2, nil /*=quitChan*/)
	require.NoError(t, err)
	snapFile2.Seek(0, 0)

	repl2 := testutil.NewTestingReplica(t, 1, 2)
	require.NotNil(t, repl2)
	_, err = repl2.Open(stopc)
	require.NoError(t, err)

	// Recover from snapshot 1.
	err = repl2.RecoverFromSnapshot(snapFile1, nil /*=quitChan*/)
	require.NoError(t, err)

	// Recover from snapshot 2
	err = repl2.RecoverFromSnapshot(snapFile2, nil /*=quitChan*/)
	require.NoError(t, err)

	// verify local session is deleted
	{
		_, err := directRead(t, repl2, localSessionKey)
		require.True(t, status.IsNotFoundError(err))
	}
	// verify that fr2 is deleted
	{
		_, err := reader(t, repl2.Replica, header, fr2)
		require.NotNil(t, err)
		require.True(t, status.IsNotFoundError(err), err)
	}
	{
		// verify fr1 is readable, since there is no delete operation
		readCloser, err := reader(t, repl2.Replica, header, fr1)
		require.NoError(t, err)
		require.Equal(t, fr1.GetDigest().GetHash(), testdigest.ReadDigestAndClose(t, readCloser).GetHash())
	}
}

func TestClearStateBeforeApplySnapshot(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("zoo"),
				Value: []byte("bar"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}

	// shrink the range descriptor.
	rd := &rfpb.RangeDescriptor{
		Start:      keys.Key("a"),
		End:        keys.Key("g"),
		RangeId:    1,
		Generation: 2,
	}
	writeLocalRangeDescriptor(t, em, repl.Replica, rd)

	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}

	wb := repl.DB().NewBatch()
	txid := []byte("TX1")
	cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo"),
		},
	}).ToProto()
	_, err = repl.PrepareTransaction(wb, txid, cmd)
	require.NoError(t, err)

	require.NoError(t, wb.Commit(pebble.Sync))
	require.NoError(t, wb.Close())

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
	repl2 := testutil.NewTestingReplica(t, 1, 2)
	require.NotNil(t, repl2)
	_, err = repl2.Open(stopc)
	require.NoError(t, err)

	em2 := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em2, repl2.Replica)

	// write an entry that is in the default range, but not in the updated range
	// in the snapshot
	{
		entry := em2.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("zoo"),
				Value: []byte("bar"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl2.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}

	// Prepare a transaction before recovering from snapshot
	wb2 := repl2.DB().NewBatch()
	txid2 := []byte("TX2")
	cmd2, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo2"),
		},
	}).ToProto()
	_, err = repl2.PrepareTransaction(wb2, txid2, cmd2)
	require.NoError(t, err)

	require.NoError(t, wb2.Commit(pebble.Sync))
	require.NoError(t, wb2.Close())

	// Recover from the snapshot
	err = repl2.RecoverFromSnapshot(snapFile, nil /*=quitChan*/)
	require.NoError(t, err)

	// Verify that local range key exists, and the value is the same as the local
	// range in the snapshot.
	verifyReplicaHasLocalRange(t, repl2, rd)

	// Verify that local last applied index key exists, and the value is not zero.
	{
		rsp, err := directRead(t, repl2, constants.LastAppliedIndexKey)
		require.NoError(t, err)
		gotIndex := binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
		require.Greater(t, gotIndex, uint64(0))
	}

	// Verify that "foo" should exist in repl2; this should be written from snapshot.
	{
		buf, closer, err := repl2.DB().Get([]byte("foo"))
		require.NoError(t, err)
		closer.Close()
		require.Equal(t, []byte("bar"), buf)
	}
	// Verify that "zoo" is not cleared.
	{
		buf, closer, err := repl2.DB().Get([]byte("zoo"))
		require.NoError(t, err)
		closer.Close()
		require.Equal(t, []byte("bar"), buf)
	}

	// Verify that we should not be able to commit the txn in the snapshot.
	err = repl2.CommitTransaction(txid)
	require.NoError(t, err)

	// Verify that we should not be able to commit the txn that was not in the
	// snapshot but created before recovering from the snapshot
	err = repl2.CommitTransaction(txid2)
	require.True(t, status.IsNotFoundError(err), "CommitTransaction should return NotFound error")
}

func TestReplicaFileWriteDelete(t *testing.T) {
	fs := filestore.New()
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	// Write a file to the replica's data dir.
	r, buf := testdigest.RandomCASResourceBuf(t, 1000)
	fileRecord := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}

	header := &rfpb.Header{RangeId: 1, Generation: 1}
	writeCommitter := writer(t, em, repl.Replica, header, fileRecord)

	_, err = writeCommitter.Write(buf)
	require.NoError(t, err)
	require.Nil(t, writeCommitter.Commit())
	require.Nil(t, writeCommitter.Close())

	// Verify that the file is readable.
	{
		readCloser, err := reader(t, repl.Replica, header, fileRecord)
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
		_, err := reader(t, repl.Replica, header, fileRecord)
		require.NotNil(t, err)
		require.True(t, status.IsNotFoundError(err), err)
	}
}

func TestFileWriteAndFind(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	now := time.Now().UnixMicro()
	// Write a file to the replica's data dir.
	r, _ := testdigest.RandomCASResourceBuf(t, 1000)
	fileRecord := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}

	md := &sgpb.FileMetadata{
		FileRecord: fileRecord,
		StorageMetadata: &sgpb.StorageMetadata{
			GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
				BlobName: "blob",
			},
		},
		LastAccessUsec: now,
	}

	fs := filestore.New()

	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)
	val, err := proto.Marshal(md)
	require.NoError(t, err)

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: val,
		},
	}))
	entries := []dbsm.Entry{entry}
	writeRsp, err := repl.Update(entries)
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Do a find
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.FindRequest{
		Key: fileMetadataKey,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	findRsp, err := readBatch.FindResponse(0)
	require.NoError(t, err)

	require.True(t, findRsp.GetPresent())
	require.Equal(t, now, findRsp.GetLastAccessUsec())
	require.Equal(t, "blob", findRsp.GetGcsMetadata().GetBlobName())
}

func TestUsage(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	rd := writeDefaultRangeDescriptor(t, em, repl.Replica)

	rt := newWriteTester(t, em, repl.Replica)

	header := &rfpb.Header{RangeId: 1, Generation: 1}
	frDefault := rt.writeRandom(header, defaultPartition, 1000)
	rt.writeRandom(header, defaultPartition, 500)
	rt.writeRandom(header, anotherPartition, 100)
	rt.writeRandom(header, anotherPartition, 200)
	rt.writeRandom(header, anotherPartition, 300)

	repl.DB().Flush()
	{
		ru, err := repl.Usage()
		require.NoError(t, err)
		require.InDelta(t, 2100, ru.GetEstimatedDiskBytesUsed(), 600.0)
	}

	// Delete a single record and verify updated usage.
	rt.delete(frDefault)
	repl.DB().Flush()
	repl.DB().Compact(rd.GetStart(), rd.GetEnd(), true)

	{
		ru, err := repl.Usage()
		require.NoError(t, err)
		require.InDelta(t, 1100, ru.GetEstimatedDiskBytesUsed(), 500.0)
	}
}

func TestTransactionPrepareAndCommit(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	wb := repl.DB().NewBatch()
	txid := []byte("TX1")
	cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("bar"),
		},
	}).ToProto()
	_, err = repl.PrepareTransaction(wb, txid, cmd)
	require.NoError(t, err)

	require.NoError(t, wb.Commit(pebble.Sync))
	require.NoError(t, wb.Close())

	wb = repl.DB().NewBatch()
	txid2 := []byte("TX2")
	badCmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("baz"),
		},
	}).ToProto()
	_, err = repl.PrepareTransaction(wb, txid2, badCmd)
	require.Error(t, err)
	require.NoError(t, wb.Close())

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("just-an-innocent-write"),
		},
	}))
	entries := []dbsm.Entry{entry}
	rsp, err := repl.Update(entries)
	require.NoError(t, err)
	require.Error(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())

	err = repl.CommitTransaction(txid)
	require.NoError(t, err)

	buf, closer, err := repl.DB().Get([]byte("foo"))
	require.NoError(t, err)
	defer closer.Close()
	require.Equal(t, []byte("bar"), buf)
}

func TestTransactionLockingMappedRange(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)

	rd := &rfpb.RangeDescriptor{
		Start:      keys.Key("a"),
		End:        keys.Key("c"),
		RangeId:    1,
		Generation: 1,
	}
	writeLocalRangeDescriptor(t, em, repl.Replica, rd)

	wb := repl.DB().NewBatch()
	txid := []byte("TX1")
	rd.End = keys.Key("b")
	rd.Generation = 2
	rdBuf, err := proto.Marshal(rd)
	require.NoError(t, err)
	cmd, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).SetLockMappedRange(true).ToProto()
	require.NoError(t, err)
	_, err = repl.PrepareTransaction(wb, txid, cmd)
	require.NoError(t, err)

	require.NoError(t, wb.Commit(pebble.Sync))
	require.NoError(t, wb.Close())

	// cannot write to [a, c)
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("azzz"),
				Value: []byte("just-an-innocent-write"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Error(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}

	// cannot write to [a, c) in a txn
	{
		wb = repl.DB().NewBatch()
		txid2 := []byte("TX2")
		badCmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("azzzz"),
				Value: []byte("baz"),
			},
		}).ToProto()
		_, err = repl.PrepareTransaction(wb, txid2, badCmd)
		require.Error(t, err)
		require.NoError(t, wb.Close())
	}

	err = repl.CommitTransaction(txid)
	require.NoError(t, err)

	verifyReplicaHasLocalRange(t, repl, rd)

	// should be able to write to [a, b)
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("azzz"),
				Value: []byte("just-an-innocent-write"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}

	// should be able to write to [a, c) in a txn
	{
		wb = repl.DB().NewBatch()
		txid2 := []byte("TX2")
		badCmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("azzzz"),
				Value: []byte("baz"),
			},
		}).ToProto()
		_, err = repl.PrepareTransaction(wb, txid2, badCmd)
		require.NoError(t, err)
		require.NoError(t, wb.Close())
	}
}

func TestTransactionPrepareAndRollback(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	wb := repl.DB().NewBatch()
	txid := []byte("TX1")
	cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("bar"),
		},
	}).ToProto()
	_, err = repl.PrepareTransaction(wb, txid, cmd)
	require.NoError(t, err)

	require.NoError(t, wb.Commit(pebble.Sync))
	require.NoError(t, wb.Close())

	err = repl.RollbackTransaction(txid)
	require.NoError(t, err)

	buf, _, err := repl.DB().Get([]byte("foo"))
	require.Error(t, err)
	require.Nil(t, buf)
}

func TestTransactionsSurviveRestart(t *testing.T) {
	em := newEntryMaker(t)

	txid := []byte("TX1")
	txid2 := []byte("TX2")

	var leaser pebble.Leaser

	{
		repl := testutil.NewTestingReplica(t, 1, 1)
		leaser = repl.Leaser()
		require.NotNil(t, repl)

		stopc := make(chan struct{})
		_, err := repl.Open(stopc)
		require.NoError(t, err)

		em := newEntryMaker(t)
		writeDefaultRangeDescriptor(t, em, repl.Replica)

		wb := repl.DB().NewBatch()
		cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}).ToProto()
		_, err = repl.PrepareTransaction(wb, txid, cmd)
		require.NoError(t, err)

		cmd2, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("baz"),
				Value: []byte("bap"),
			},
		}).ToProto()
		_, err = repl.PrepareTransaction(wb, txid2, cmd2)
		require.NoError(t, err)

		require.NoError(t, wb.Commit(pebble.Sync))
		require.NoError(t, wb.Close())

		err = repl.Close()
		require.NoError(t, err)
	}

	{
		repl := testutil.NewTestingReplicaWithLeaser(t, 1, 1, leaser)
		require.NotNil(t, repl)
		t.Cleanup(func() {
			err := repl.Close()
			require.NoError(t, err)
		})

		stopc := make(chan struct{})
		_, err := repl.Open(stopc)
		require.NoError(t, err)

		err = repl.CommitTransaction(txid)
		require.NoError(t, err)

		buf, closer, err := repl.DB().Get([]byte("foo"))
		require.NoError(t, err)
		defer closer.Close()
		require.Equal(t, []byte("bar"), buf)

		// Direct write should succeed; locks should be released.
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}

	{
		repl := testutil.NewTestingReplicaWithLeaser(t, 1, 1, leaser)
		require.NotNil(t, repl)
		t.Cleanup(func() {
			err := repl.Close()
			require.NoError(t, err)
		})

		stopc := make(chan struct{})
		_, err := repl.Open(stopc)
		require.NoError(t, err)
	}
}

func TestBatchTransaction(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)

	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	txid := []byte("test-txid")
	session := &rfpb.Session{
		Id:    []byte(uuid.New()),
		Index: 1,
	}

	{ // Do a DirectWrite.
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
	session.Index++
	{ // Prepare a transaction
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).SetTransactionID(txid).Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("transaction-succeeded"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
	session.Index++
	{ // Attempt another direct write (should fail b/c of pending txn).
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("boop"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Error(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
	{ // Do a DirectRead and verify the value is still `bar`.
		rsp, err := directRead(t, repl, []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("bar"), rsp.GetKv().GetValue())
	}
	session.Index++
	{ // Commit the transaction
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).SetTransactionID(txid).SetFinalizeOperation(rfpb.FinalizeOperation_COMMIT))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
	{ // retry the entry to commit transaction with the same session; should not return error.
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).SetTransactionID(txid).SetFinalizeOperation(rfpb.FinalizeOperation_COMMIT))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
	session.Index++
	{ // Commit transaction again; should return error.
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).SetTransactionID(txid).SetFinalizeOperation(rfpb.FinalizeOperation_COMMIT))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		err = rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError()
		require.True(t, status.IsNotFoundError(err), "CommitTransaction should return NotFound error")
	}
	{ // Do a DirectRead and verify the value was updated by the txn.
		rsp, err := directRead(t, repl, []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, []byte("transaction-succeeded"), rsp.GetKv().GetValue())
	}
	session.Index++
	{ // Value should be direct writable again (no more pending txns)
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}))
		entries := []dbsm.Entry{entry}
		rsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
	}
}

func TestScanSharedDB(t *testing.T) {
	{
		repl1 := testutil.NewTestingReplica(t, 1, 1)
		require.NotNil(t, repl1)
		t.Cleanup(func() {
			err := repl1.Close()
			require.NoError(t, err)
		})

		repl2 := testutil.NewTestingReplicaWithLeaser(t, 2, 1, repl1.Leaser())
		require.NotNil(t, repl2)

		stopc := make(chan struct{})
		_, err := repl1.Open(stopc)
		require.NoError(t, err)

		_, err = repl2.Open(stopc)
		require.NoError(t, err)

		em := newEntryMaker(t)

		metarangeDescriptor := &rfpb.RangeDescriptor{
			Start:      constants.MetaRangePrefix,
			End:        keys.Key{constants.UnsplittableMaxByte},
			RangeId:    1,
			Generation: 1,
		}
		writeLocalRangeDescriptor(t, em, repl1.Replica, metarangeDescriptor)
		writeMetaRangeDescriptor(t, em, repl1.Replica, metarangeDescriptor)

		secondRangeDescriptor := &rfpb.RangeDescriptor{
			Start:      keys.Key{constants.UnsplittableMaxByte},
			End:        keys.Key("z"),
			RangeId:    2,
			Generation: 1,
		}
		writeLocalRangeDescriptor(t, em, repl2.Replica, secondRangeDescriptor)
		writeMetaRangeDescriptor(t, em, repl1.Replica, secondRangeDescriptor)

		buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
			Start:    keys.RangeMetaKey(keys.Key("a")),
			End:      constants.SystemPrefix,
			ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
		}).ToBuf()
		require.NoError(t, err)
		readRsp, err := repl1.Lookup(buf)
		require.NoError(t, err)

		readBatch := rbuilder.NewBatchResponse(readRsp)
		scanRsp, err := readBatch.ScanResponse(0)
		require.NoError(t, err)
		require.Equal(t, 1, len(scanRsp.GetKvs()))

		gotRD := &rfpb.RangeDescriptor{}
		require.NoError(t, proto.Unmarshal(scanRsp.GetKvs()[0].GetValue(), gotRD))
		require.Equal(t, keys.Key("z"), keys.Key(gotRD.GetEnd()))
	}
}

func TestDeleteSessions(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	now := time.Now()

	session1 := &rfpb.Session{
		Id:            []byte(uuid.New()),
		Index:         1,
		CreatedAtUsec: now.Add(-2 * time.Hour).UnixMicro(),
	}

	{
		// Write session 1
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session1).Add(&rfpb.IncrementRequest{
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
	}

	session2 := &rfpb.Session{
		Id:            []byte(uuid.New()),
		Index:         1,
		CreatedAtUsec: now.Add(-1 * time.Hour).UnixMicro(),
	}

	{
		// Write session 2
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session2).Add(&rfpb.IncrementRequest{
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
		require.Equal(t, uint64(2), incrRsp.GetValue())
	}

	session3 := &rfpb.Session{
		Id:            []byte(uuid.New()),
		Index:         1,
		CreatedAtUsec: now.UnixMicro(),
	}
	{
		// Delete sessions created 90 minutes ago
		entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session3).Add(&rfpb.DeleteSessionsRequest{
			CreatedAtUsec: now.Add(-90 * time.Minute).UnixMicro(),
		}))
		writeRsp, err := repl.Update([]dbsm.Entry{entry})
		require.NoError(t, err)
		require.Equal(t, 1, len(writeRsp))

		// Make sure the response holds the new value.
		incrBatch := rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
		_, err = incrBatch.DeleteSessionsResponse(0)
		require.NoError(t, err)
	}
	// Verify that session 1 is deleted and session 2 is not
	start, end := keys.Range(constants.SessionPrefix)
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    start,
		End:      end,
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	scanRsp, err := readBatch.ScanResponse(0)
	require.NoError(t, err)
	got := []*rfpb.Session{}
	for _, kv := range scanRsp.GetKvs() {
		session := &rfpb.Session{}
		err := proto.Unmarshal(kv.GetValue(), session)
		require.NoError(t, err)
		// We are not comparing RspData
		session.RspData = nil
		got = append(got, session)
	}
	session2.EntryIndex = proto.Uint64(3)
	session3.EntryIndex = proto.Uint64(4)
	require.ElementsMatch(t, got, []*rfpb.Session{session2, session3})
}

func TestUpdateATime(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		err := repl.Close()
		require.NoError(t, err)
	})

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	// Create a file record and write initial metadata with an access time
	r, _ := testdigest.RandomCASResourceBuf(t, 1000)
	fileRecord := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}

	initialATime := int64(1_000_000)
	md := &sgpb.FileMetadata{
		FileRecord: fileRecord,
		StorageMetadata: &sgpb.StorageMetadata{
			InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
				Data: []byte("test-data"),
			},
		},
		LastAccessUsec: initialATime,
	}

	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)
	val, err := proto.Marshal(md)
	require.NoError(t, err)

	// Write the initial file metadata
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: val,
		},
	}))
	entries := []dbsm.Entry{entry}
	writeRsp, err := repl.Update(entries)
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Test case 1: New atime comes before old atime - should not update
	olderATime := int64(500_000)
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.UpdateAtimeRequest{
			Key:            fileMetadataKey,
			AccessTimeUsec: olderATime,
		}))
		entries := []dbsm.Entry{entry}
		updateRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(updateRsp))
		require.NoError(t, rbuilder.NewBatchResponse(updateRsp[0].Result.Data).AnyError())

		// Verify that atime was NOT updated (should still be initialATime)
		rsp, err := directRead(t, repl, fileMetadataKey)
		require.NoError(t, err)

		gotMd := &sgpb.FileMetadata{}
		err = proto.Unmarshal(rsp.GetKv().GetValue(), gotMd)
		require.NoError(t, err)
		require.Equal(t, initialATime, gotMd.GetLastAccessUsec(), "atime should not be updated when new atime is older")
	}

	// Test case 2: New atime comes after old atime - should update
	newerATime := int64(2_000_000)
	{
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.UpdateAtimeRequest{
			Key:            fileMetadataKey,
			AccessTimeUsec: newerATime,
		}))
		entries := []dbsm.Entry{entry}
		updateRsp, err := repl.Update(entries)
		require.NoError(t, err)
		require.Equal(t, 1, len(updateRsp))
		require.NoError(t, rbuilder.NewBatchResponse(updateRsp[0].Result.Data).AnyError())

		// Verify that atime was updated to newerATime
		rsp, err := directRead(t, repl, fileMetadataKey)
		require.NoError(t, err)

		gotMd := &sgpb.FileMetadata{}
		err = proto.Unmarshal(rsp.GetKv().GetValue(), gotMd)
		require.NoError(t, err)
		require.Equal(t, newerATime, gotMd.GetLastAccessUsec(), "atime should be updated when new atime is newer")
	}
}
