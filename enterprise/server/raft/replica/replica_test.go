package replica_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/require"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	dbconfig "github.com/lni/dragonboat/v4/config"
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
	t     testing.TB
}

func newEntryMaker(t testing.TB) *entryMaker {
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

func writeRangeDescriptor(t testing.TB, em *entryMaker, r *replica.Replica, key []byte, rd *rfpb.RangeDescriptor) {
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

func writeLocalRangeDescriptor(t testing.TB, em *entryMaker, r *replica.Replica, rd *rfpb.RangeDescriptor) {
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
	wc.SetCommitFn(func(bytesWritten int64) error {
		now := time.Now()
		md := &sgpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: writeCloserMetadata.Metadata(),
			StoredSizeBytes: bytesWritten,
			LastModifyUsec:  now.UnixMicro(),
			LastAccessUsec:  now.UnixMicro(),
		}
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
			Key:          fileMetadataKey,
			FileMetadata: md,
		}))
		entries := []dbsm.Entry{entry}
		writeRsp, err := r.Update(entries)
		if err != nil {
			return err
		}
		require.Equal(t, 1, len(writeRsp))

		return rbuilder.NewBatchResponse(writeRsp[0].Result.Data).AnyError()
	})
	return wc
}

func writeDefaultRangeDescriptor(t testing.TB, em *entryMaker, r *replica.Replica) *rfpb.RangeDescriptor {
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

// The generic KV writers must refuse file-record keys: records must be written
// through SetRequest (and mutated via UpdateAtime/Delete) so validation and any
// state derived from record writes can rely on those paths being exhaustive.
func TestDirectWriteRefusesFileRecordKeys(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		require.NoError(t, repl.Close())
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	r, _ := testdigest.RandomCASResourceBuf(t, 100)
	fileRecord := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "default",
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         r.GetDigest(),
		DigestFunction: repb.DigestFunction_SHA256,
	}
	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: fileMetadataKey, Value: []byte("unindexed")},
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	err = rbuilder.NewBatchResponse(writeRsp[0].Result.Data).AnyError()
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)

	// CAS is the same generic writer and must refuse too. rbuilder already
	// rejects CAS on splittable keys client-side, so hand-roll the batch to
	// prove the apply-path guard holds for callers that bypass rbuilder.
	casBatch, err := proto.Marshal(&rfpb.BatchCmdRequest{
		Union: []*rfpb.RequestUnion{{Value: &rfpb.RequestUnion_Cas{
			Cas: &rfpb.CASRequest{
				Kv: &rfpb.KV{Key: fileMetadataKey, Value: []byte("unindexed")},
			},
		}}},
	})
	require.NoError(t, err)
	casRsp, err := repl.Update([]dbsm.Entry{{Cmd: casBatch, Index: 3}})
	require.NoError(t, err)
	err = rbuilder.NewBatchResponse(casRsp[0].Result.Data).AnyError()
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)

	// Neither write may have landed.
	_, err = directRead(t, repl, fileMetadataKey)
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got: %v", err)
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
		Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key")),
		Delta: 1,
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	session.Index = 0
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.IncrementRequest{
		Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key")),
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

// TestSessionRangeIDNamespace verifies that sessions are namespaced by
// (id, range_id) on the replica: a write with the same id+lower-index but
// a different range_id does not collide with a prior write. This is what
// makes cross-range retries (e.g. after a split) safe — they will miss
// dedup on the destination range rather than getting stuck behind a
// stored entry that happens to share the same id.
func TestSessionRangeIDNamespace(t *testing.T) {
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

	id := []byte(uuid.New())

	// errorMessage returns the encoded gRPC status from a state-machine
	// entry result, or empty string if the result is not an error.
	errorMessage := func(r dbsm.Result) string {
		if int(r.Value) != constants.EntryErrorValue {
			return ""
		}
		st := &statuspb.Status{}
		require.NoError(t, proto.Unmarshal(r.Data, st))
		return st.String()
	}

	// Write with (id, idx=5, range_id=2). Stores dedup record under
	// `session-<id>-2`.
	sessionA := &rfpb.Session{Id: id, Index: 5, RangeId: 2}
	entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(sessionA).Add(&rfpb.IncrementRequest{
		Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key-a")),
		Delta: 1,
	}))
	rsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Empty(t, errorMessage(rsp[0].Result))

	// Write with the same id, lower index, but a different range_id.
	// Stores under `session-<id>-9` — separate namespace, so this is not
	// a replay and not stale. Must succeed.
	sessionB := &rfpb.Session{Id: id, Index: 3, RangeId: 9}
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(sessionB).Add(&rfpb.IncrementRequest{
		Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key-b")),
		Delta: 1,
	}))
	rsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Empty(t, errorMessage(rsp[0].Result))

	// Within range_id=2's namespace, a lower index is still stale.
	sessionStale := &rfpb.Session{Id: id, Index: 4, RangeId: 2}
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(sessionStale).Add(&rfpb.IncrementRequest{
		Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key-a")),
		Delta: 1,
	}))
	rsp, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Contains(t, errorMessage(rsp[0].Result), "index mismatch")
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

	// CAS is only permitted on non-splittable keys, so seed a
	// system-prefix key with an initial value via DirectWrite.
	casKey := keys.MakeKey(constants.SystemPrefix, []byte("cas-key"))
	initialValue := []byte("initial")
	seed := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: casKey, Value: initialValue},
	}))
	_, err = repl.Update([]dbsm.Entry{seed})
	require.NoError(t, err)

	// Do a CAS and verify:
	//   1) the value is not set
	//   2) the current value is returned.
	session := &rfpb.Session{
		Id:    []byte(uuid.New()),
		Index: 1,
	}
	entry := em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   casKey,
			Value: []byte{},
		},
		ExpectedValue: []byte("bogus-expected-value"),
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	readBatch := rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err := readBatch.CASResponse(0)
	require.True(t, status.IsFailedPreconditionError(err))
	require.Equal(t, initialValue, casRsp.GetKv().GetValue())

	// Do a CAS with the correct expected value and ensure
	// the value was written.
	session.Index++
	entry = em.makeEntry(rbuilder.NewBatchBuilder().SetSession(session).Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   casKey,
			Value: []byte{},
		},
		ExpectedValue: initialValue,
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
			Key:   casKey,
			Value: []byte{},
		},
		ExpectedValue: initialValue,
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

// crashingReader fails after reading a fixed number of bytes.
type crashingReader struct {
	r         io.Reader
	remaining int
}

var errSimulatedCrash = errors.New("simulated crash")

func (c *crashingReader) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		return 0, errSimulatedCrash
	}
	if len(p) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.r.Read(p)
	c.remaining -= n
	return n, err
}

// writeDirectKVs writes direct KV entries and returns their keys.
func writeDirectKVs(t *testing.T, em *entryMaker, repl *replica.Replica, count, valueSize int) [][]byte {
	keys := make([][]byte, 0, count)
	for i := range count {
		key := []byte(fmt.Sprintf("key-%04d", i))
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   key,
				Value: bytes.Repeat([]byte{byte(i)}, valueSize),
			},
		}))
		rsp, err := repl.Update([]dbsm.Entry{entry})
		require.NoError(t, err)
		require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
		keys = append(keys, key)
	}
	return keys
}

func countPresentKeys(t *testing.T, repl *testutil.TestingReplica, keys [][]byte) int {
	present := 0
	for _, key := range keys {
		rsp, err := directRead(t, repl, key)
		if status.IsNotFoundError(err) {
			continue
		}
		require.NoError(t, err)
		require.NotEmpty(t, rsp.GetKv().GetValue())
		present++
	}
	return present
}

// A partial restore must not persist the snapshot's applied index, so
// Dragonboat reapplies the snapshot after restart.
func TestRecoverFromSnapshotCrashMidApply(t *testing.T) {
	prev := replica.TestingSetSnapshotBatchSizeBytes(4 * 1024)
	t.Cleanup(func() { replica.TestingSetSnapshotBatchSizeBytes(prev) })

	stopc := make(chan struct{})
	em := newEntryMaker(t)

	// Create a snapshot spanning many batches.
	src := testutil.NewTestingReplica(t, 1, 1)
	t.Cleanup(func() { require.NoError(t, src.Close()) })
	_, err := src.Open(stopc)
	require.NoError(t, err)
	writeDefaultRangeDescriptor(t, em, src.Replica)
	keys := writeDirectKVs(t, em, src.Replica, 100, 1024)
	snapshotIndex, err := src.LastAppliedIndex()
	require.NoError(t, err)
	require.Greater(t, snapshotIndex, uint64(0))

	snapI, err := src.PrepareSnapshot()
	require.NoError(t, err)
	snapBuf := &bytes.Buffer{}
	require.NoError(t, src.SaveSnapshot(snapI, snapBuf, nil /*=quitChan*/))
	snapBytes := snapBuf.Bytes()

	// Fail halfway through restoring the snapshot.
	dst := testutil.NewTestingReplica(t, 1, 2)
	leaser := dst.Leaser()
	_, err = dst.Open(stopc)
	require.NoError(t, err)
	err = dst.RecoverFromSnapshot(&crashingReader{
		r:         bytes.NewReader(snapBytes),
		remaining: len(snapBytes) / 2,
	}, nil /*=quitChan*/)
	require.ErrorIs(t, err, errSimulatedCrash)
	require.NoError(t, dst.Close())

	// Restart on the partially restored DB.
	restarted := testutil.NewTestingReplicaWithLeaser(t, 1, 2, leaser)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	openIndex, err := restarted.Open(stopc)
	require.NoError(t, err)

	present := countPresentKeys(t, restarted, keys)
	require.Less(t, present, len(keys), "crash should leave range data incomplete")
	require.Less(t, openIndex, snapshotIndex, "Open must not report the snapshot index while range data is incomplete")

	// Reapply the snapshot, as Dragonboat would.
	err = restarted.RecoverFromSnapshot(bytes.NewReader(snapBytes), nil /*=quitChan*/)
	require.NoError(t, err)
	idx, err := restarted.LastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, snapshotIndex, idx)
	require.Equal(t, len(keys), countPresentKeys(t, restarted, keys))
}

// Rejected entries advance only the stored index, leaving data and sessions alone.
func TestRejectedEntryAdvancesLastAppliedIndex(t *testing.T) {
	incrKey := keys.MakeKey(constants.SystemPrefix, []byte("incr-key"))
	increment := func() *rbuilder.BatchBuilder {
		return rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{Key: incrKey, Delta: 1})
	}
	staleHeader := &rfpb.Header{RangeId: 1, Generation: 0}
	currentHeader := &rfpb.Header{RangeId: 1, Generation: 1}

	for _, tc := range []struct {
		name string
		// makeEntry may apply setup entries before returning a rejected entry.
		makeEntry func(t *testing.T, em *entryMaker, repl *replica.Replica) dbsm.Entry
		// Counter value after rejection.
		wantCounter uint64
		// Optional retry that must succeed.
		retry *rbuilder.BatchBuilder
	}{
		{
			name: "malformed payload",
			makeEntry: func(t *testing.T, em *entryMaker, repl *replica.Replica) dbsm.Entry {
				em.index++
				return dbsm.Entry{Index: em.index, Cmd: []byte{0xff, 0xff, 0xff}}
			},
		},
		{
			name: "stale header",
			makeEntry: func(t *testing.T, em *entryMaker, repl *replica.Replica) dbsm.Entry {
				session := &rfpb.Session{Id: []byte("stale-header-session"), Index: 1}
				return em.makeEntry(increment().SetHeader(staleHeader).SetSession(session))
			},
			// Verify rejection did not cache a session response.
			retry: increment().SetHeader(currentHeader).SetSession(&rfpb.Session{Id: []byte("stale-header-session"), Index: 1}),
		},
		{
			name: "stale session",
			makeEntry: func(t *testing.T, em *entryMaker, repl *replica.Replica) dbsm.Entry {
				session := &rfpb.Session{Id: []byte("stale-session"), Index: 2}
				rsp, err := repl.Update([]dbsm.Entry{em.makeEntry(increment().SetSession(session))})
				require.NoError(t, err)
				require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
				session.Index = 1
				return em.makeEntry(increment().SetSession(session))
			},
			wantCounter: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repl := testutil.NewTestingReplica(t, 1, 1)
			_, err := repl.Open(make(chan struct{}))
			require.NoError(t, err)
			em := newEntryMaker(t)
			writeDefaultRangeDescriptor(t, em, repl.Replica)

			readCounter := func(r *testutil.TestingReplica) uint64 {
				rsp, err := directRead(t, r, incrKey)
				if status.IsNotFoundError(err) {
					return 0
				}
				require.NoError(t, err)
				return binary.LittleEndian.Uint64(rsp.GetKv().GetValue())
			}

			entry := tc.makeEntry(t, em, repl.Replica)
			rsp, err := repl.Update([]dbsm.Entry{entry})
			require.NoError(t, err)
			require.Equal(t, constants.EntryErrorValue, int(rsp[0].Result.Value))

			idx, err := repl.LastAppliedIndex()
			require.NoError(t, err)
			require.Equal(t, entry.Index, idx)
			require.Equal(t, tc.wantCounter, readCounter(repl))

			if tc.retry != nil {
				rsp, err := repl.Update([]dbsm.Entry{em.makeEntry(tc.retry)})
				require.NoError(t, err)
				require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
				require.Equal(t, tc.wantCounter+1, readCounter(repl))
			}
			wantIndex, err := repl.LastAppliedIndex()
			require.NoError(t, err)

			// The index survives a restart.
			require.NoError(t, repl.Close())
			restarted := testutil.NewTestingReplicaWithLeaser(t, 1, 1, repl.Leaser())
			t.Cleanup(func() { require.NoError(t, restarted.Close()) })
			openIndex, err := restarted.Open(make(chan struct{}))
			require.NoError(t, err)
			require.Equal(t, wantIndex, openIndex)
		})
	}
}

// recordingSM records Open's index and the last index from successful Updates.
// The latter tracks Dragonboat's onDiskIndex as entries are applied.
type recordingSM struct {
	*replica.Replica

	mu              sync.Mutex
	openIndex       uint64
	lastUpdateIndex uint64
}

func (r *recordingSM) Open(stopc <-chan struct{}) (uint64, error) {
	idx, err := r.Replica.Open(stopc)
	r.mu.Lock()
	r.openIndex = idx
	r.mu.Unlock()
	return idx, err
}

func (r *recordingSM) Update(entries []dbsm.Entry) ([]dbsm.Entry, error) {
	rsp, err := r.Replica.Update(entries)
	if err == nil && len(entries) > 0 {
		r.mu.Lock()
		r.lastUpdateIndex = entries[len(entries)-1].Index
		r.mu.Unlock()
	}
	return rsp, err
}

func (r *recordingSM) indexes() (openIndex, lastUpdateIndex uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.openIndex, r.lastUpdateIndex
}

func syncProposeWithRetry(t *testing.T, nh *dragonboat.NodeHost, rangeID uint64, batch *rbuilder.BatchBuilder) dbsm.Result {
	buf, err := batch.ToBuf()
	require.NoError(t, err)
	var lastErr error
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(rangeID), buf)
		cancel()
		if err == nil {
			return res
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	require.FailNowf(t, "propose timed out", "last error: %s", lastErr)
	return dbsm.Result{}
}

// Open must cover the snapshot's OnDiskIndex, including rejected entries:
// local on-disk snapshots contain no application data to restore.
func TestRejectedEntryAdvancesOpenIndex(t *testing.T) {
	const rangeID, replicaID = 1, 1

	rootDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(filepath.Join(rootDir, "pebble"), "test", &pebble.Options{})
	require.NoError(t, err)
	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})

	raftAddr := fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
	logDBConfig := dbconfig.GetSmallMemLogDBConfig()
	logDBConfig.Shards = 2
	nhc := dbconfig.NodeHostConfig{
		WALDir:         filepath.Join(rootDir, "wal"),
		NodeHostDir:    filepath.Join(rootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    raftAddr,
		Expert: dbconfig.ExpertConfig{
			LogDB: logDBConfig,
		},
	}
	rc := raftConfig.GetRaftConfig(rangeID, replicaID)

	var sm *recordingSM
	factory := func(rangeID, replicaID uint64) dbsm.IOnDiskStateMachine {
		sm = &recordingSM{
			Replica: replica.New(leaser, rangeID, replicaID, &testutil.FakeStore{}, nil /*=usageUpdates*/),
		}
		return sm
	}

	// Close before DB cleanup, even on failure; avoid double-close panics.
	closeOnce := func(nh *dragonboat.NodeHost) func() {
		var once sync.Once
		return func() { once.Do(nh.Close) }
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	require.NoError(t, err)
	closeNH := closeOnce(nh)
	t.Cleanup(closeNH)
	err = nh.StartOnDiskReplica(map[uint64]string{replicaID: raftAddr}, false /*=join*/, factory, rc)
	require.NoError(t, err)

	// Write the range descriptor.
	rd := &rfpb.RangeDescriptor{
		Start:      keys.Key{constants.UnsplittableMaxByte},
		End:        keys.MaxByte,
		RangeId:    rangeID,
		Generation: 1,
	}
	rdBuf, err := proto.Marshal(rd)
	require.NoError(t, err)
	res := syncProposeWithRetry(t, nh, rangeID, rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: constants.LocalRangeKey, Value: rdBuf},
	}))
	require.NotEqual(t, uint64(constants.EntryErrorValue), res.Value)

	// Reject a stale header.
	res = syncProposeWithRetry(t, nh, rangeID, rbuilder.NewBatchBuilder().
		SetHeader(&rfpb.Header{RangeId: rangeID, Generation: 0}).
		Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{Key: []byte("key-rejected"), Value: []byte("value")},
		}))
	require.Equal(t, uint64(constants.EntryErrorValue), res.Value)

	// Rejection must advance the stored index to match Dragonboat's.
	_, onDiskIndex := sm.indexes()
	storedIndex, err := sm.LastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, onDiskIndex, storedIndex)

	// Snapshot with the rejected entry last.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = nh.SyncRequestSnapshot(ctx, rangeID, dragonboat.SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         0,
	})
	require.NoError(t, err)

	// Restart using the same storage.
	closeNH()
	nh, err = dragonboat.NewNodeHost(nhc)
	require.NoError(t, err)
	t.Cleanup(closeOnce(nh))
	err = nh.StartOnDiskReplica(nil, false /*=join*/, factory, rc)
	require.NoError(t, err)

	// Propose to wait for startup recovery.
	res = syncProposeWithRetry(t, nh, rangeID, rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{Key: []byte("key-after-restart"), Value: []byte("value")},
	}))
	require.NotEqual(t, uint64(constants.EntryErrorValue), res.Value)

	openIndex, _ := sm.indexes()
	require.Equal(t, onDiskIndex, openIndex, "Open must return the snapshot's on-disk index")
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

	txid := []byte("TX1")
	cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo"),
		},
	}).ToProto()
	err = applyTransaction(t, em, repl.Replica, txid, cmd)
	require.NoError(t, err)

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

	txid2 := []byte("TX2")
	cmd2, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("zoo2"),
		},
	}).ToProto()
	err = applyTransaction(t, em2, repl2.Replica, txid2, cmd2)
	require.NoError(t, err)

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

	// Verify that we can commit the txn in the snapshot.
	err = applyTransaction(t, em2, repl2.Replica, txid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_COMMIT.Enum()})
	require.NoError(t, err)

	// Verify that we should not be able to commit the txn that was not in the
	// snapshot but created before recovering from the snapshot
	err = applyTransaction(t, em2, repl2.Replica, txid2, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_COMMIT.Enum()})
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
				BlobName:           "blob",
				LastCustomTimeUsec: now,
			},
		},
		StoredSizeBytes: 1000,
		LastAccessUsec:  now,
	}

	fs := filestore.New()

	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	// Write the record.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
		Key:          fileMetadataKey,
		FileMetadata: md,
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
	require.Equal(t, now, findRsp.GetGcsMetadata().GetLastCustomTimeUsec())
}

// A zero-length record is an anomaly the read path rejects, so Find must report
// it absent.
func TestFileFindZeroLengthReportsAbsent(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		require.NoError(t, repl.Close())
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

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
		FileRecord:      fileRecord,
		StoredSizeBytes: 0, // zero-length anomaly
		LastAccessUsec:  time.Now().UnixMicro(),
	}

	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
		Key:          fileMetadataKey,
		FileMetadata: md,
	}))
	_, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.FindRequest{
		Key: fileMetadataKey,
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(buf)
	require.NoError(t, err)

	findRsp, err := rbuilder.NewBatchResponse(readRsp).FindResponse(0)
	require.NoError(t, err)
	require.False(t, findRsp.GetPresent(), "zero-length record must report absent")
}

// Find must report a missing record absent, and must not report GCS metadata
// for a record that isn't stored in GCS.
func TestFileFindMissingAndInline(t *testing.T) {
	repl := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, repl)
	t.Cleanup(func() {
		require.NoError(t, repl.Close())
	})

	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(t, err)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)

	fs := filestore.New()
	fileMetadataKey := func(fileRecord *sgpb.FileRecord) []byte {
		key, err := fs.PebbleKey(fileRecord)
		require.NoError(t, err)
		keyBytes, err := key.Bytes(filestore.Version5)
		require.NoError(t, err)
		return keyBytes
	}

	now := time.Now().UnixMicro()
	inlineRecord, buf := randomRecord(t, defaultPartition, 1000)
	inlineKey := fileMetadataKey(inlineRecord)
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
		Key: inlineKey,
		FileMetadata: &sgpb.FileMetadata{
			FileRecord: inlineRecord,
			StorageMetadata: &sgpb.StorageMetadata{
				InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
					Data: buf,
				},
			},
			StoredSizeBytes: 1000,
			LastAccessUsec:  now,
		},
	}))
	_, err = repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)

	missingRecord, _ := randomRecord(t, defaultPartition, 1000)
	reqBuf, err := rbuilder.NewBatchBuilder().Add(&rfpb.FindRequest{
		Key: inlineKey,
	}).Add(&rfpb.FindRequest{
		Key: fileMetadataKey(missingRecord),
	}).ToBuf()
	require.NoError(t, err)
	readRsp, err := repl.Lookup(reqBuf)
	require.NoError(t, err)
	batchRsp := rbuilder.NewBatchResponse(readRsp)

	inlineRsp, err := batchRsp.FindResponse(0)
	require.NoError(t, err)
	require.True(t, inlineRsp.GetPresent())
	require.Equal(t, now, inlineRsp.GetLastAccessUsec())
	require.Nil(t, inlineRsp.GetGcsMetadata())

	missingRsp, err := batchRsp.FindResponse(1)
	require.NoError(t, err)
	require.False(t, missingRsp.GetPresent())
	require.Zero(t, missingRsp.GetLastAccessUsec())
	require.Nil(t, missingRsp.GetGcsMetadata())
}

func BenchmarkFind(b *testing.B) {
	for _, storage := range []struct {
		name            string
		digestSizeBytes int64
		gcs             bool
	}{
		{"512B-inline", 512, false},
		{"32KiB-inline", 32 * 1024, false},
		{"1KiB-gcs", 1024, true},
	} {
		for _, present := range []bool{true, false} {
			name := fmt.Sprintf("storage=%s/present=%v", storage.name, present)
			b.Run(name, func(b *testing.B) {
				benchmarkFind(b, storage.digestSizeBytes, storage.gcs, present)
			})
		}
	}
}

// benchmarkFind times a Lookup of one batch of FindRequests, the shape of a
// metadata server SyncRead, against a replica holding (or not holding) every
// requested record.
func benchmarkFind(b *testing.B, digestSizeBytes int64, gcs bool, present bool) {
	repl := testutil.NewTestingReplica(b, 1, 1)
	b.Cleanup(func() {
		require.NoError(b, repl.Close())
	})
	_, err := repl.Open(make(chan struct{}))
	require.NoError(b, err)
	em := newEntryMaker(b)
	writeDefaultRangeDescriptor(b, em, repl.Replica)

	fs := filestore.New()
	now := time.Now().UnixMicro()
	batch := rbuilder.NewBatchBuilder()
	numRecords := 100
	for range numRecords {
		r, buf := testdigest.RandomCASResourceBuf(b, digestSizeBytes)
		fileRecord := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   rspb.CacheType_CAS,
				PartitionId: defaultPartition,
				GroupId:     interfaces.AuthAnonymousUser,
			},
			Digest:         r.GetDigest(),
			DigestFunction: repb.DigestFunction_SHA256,
		}
		key, err := fs.PebbleKey(fileRecord)
		require.NoError(b, err)
		fileMetadataKey, err := key.Bytes(filestore.Version5)
		require.NoError(b, err)
		batch.Add(&rfpb.FindRequest{Key: fileMetadataKey})
		if !present {
			continue
		}

		storageMetadata := &sgpb.StorageMetadata{
			InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
				Data:          buf,
				CreatedAtNsec: time.Now().UnixNano(),
			},
		}
		if gcs {
			storageMetadata = &sgpb.StorageMetadata{
				GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
					BlobName:           r.GetDigest().GetHash(),
					LastCustomTimeUsec: now,
				},
			}
		}
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
			Key: fileMetadataKey,
			FileMetadata: &sgpb.FileMetadata{
				FileRecord:      fileRecord,
				StorageMetadata: storageMetadata,
				StoredSizeBytes: digestSizeBytes,
				LastAccessUsec:  now,
				LastModifyUsec:  now,
			},
		}))
		_, err = repl.Update([]dbsm.Entry{entry})
		require.NoError(b, err)
	}
	reqBuf, err := batch.ToBuf()
	require.NoError(b, err)

	// Check the results once, outside the timed loop.
	rspBuf, err := repl.Lookup(reqBuf)
	require.NoError(b, err)
	batchRsp := rbuilder.NewBatchResponse(rspBuf)
	for i := range numRecords {
		findRsp, err := batchRsp.FindResponse(i)
		require.NoError(b, err)
		require.Equal(b, present, findRsp.GetPresent())
		require.Equal(b, present && gcs, findRsp.GetGcsMetadata() != nil)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := repl.Lookup(reqBuf); err != nil {
			b.Fatal(err)
		}
	}
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

// applyTransaction exercises the same persistence and memory publication path
// as Raft, rather than committing transaction helper batches directly.
func applyTransaction(t *testing.T, em *entryMaker, repl *replica.Replica, txid []byte, req *rfpb.BatchCmdRequest) error {
	t.Helper()
	index, err := repl.LastAppliedIndex()
	require.NoError(t, err)
	em.index = index + 1
	req = req.CloneVT()
	req.TransactionId = txid
	buf, err := proto.Marshal(req)
	require.NoError(t, err)
	entries, err := repl.Update([]dbsm.Entry{{Index: em.index, Cmd: buf}})
	if err != nil {
		return err
	}
	require.NotEqualValues(t, constants.EntryErrorValue, entries[0].Result.Value)
	return rbuilder.NewBatchResponse(entries[0].Result.Data).AnyError()
}

type txnFaultDB struct {
	pebble.IPebbleDB
	beforeCommit func() error
	afterCommit  func()
}

func (db *txnFaultDB) NewBatch() pebble.Batch {
	return &txnFaultBatch{Batch: db.IPebbleDB.NewBatch(), db: db}
}

func (db *txnFaultDB) NewIndexedBatch() pebble.Batch {
	return &txnFaultBatch{Batch: db.IPebbleDB.NewIndexedBatch(), db: db}
}

type txnFaultBatch struct {
	pebble.Batch
	db *txnFaultDB
}

func (b *txnFaultBatch) Apply(other pebble.Batch, opts *pebble.WriteOptions) error {
	if wrapped, ok := other.(*txnFaultBatch); ok {
		other = wrapped.Batch
	}
	return b.Batch.Apply(other, opts)
}

func (b *txnFaultBatch) Commit(opts *pebble.WriteOptions) error {
	if b.db.beforeCommit != nil {
		if err := b.db.beforeCommit(); err != nil {
			return err
		}
	}
	if err := b.Batch.Commit(opts); err != nil {
		return err
	}
	if b.db.afterCommit != nil {
		b.db.afterCommit()
	}
	return nil
}

func openTxnFaultReplica(t *testing.T, dir string) (*testutil.TestingReplica, *txnFaultDB, func()) {
	t.Helper()
	db, err := pebble.Open(dir, "txn-atomicity-test", &pebble.Options{})
	require.NoError(t, err)
	faultDB := &txnFaultDB{IPebbleDB: db}
	leaser := pebble.NewDBLeaser(faultDB)
	repl := testutil.NewTestingReplicaWithLeaser(t, 1, 1, leaser)
	_, err = repl.Open(make(chan struct{}))
	require.NoError(t, err)
	closed := false
	closeReplica := func() {
		if closed {
			return
		}
		closed = true
		require.NoError(t, repl.Close())
		leaser.Close()
		require.NoError(t, db.Close())
	}
	t.Cleanup(closeReplica)
	return repl, faultDB, closeReplica
}

// Interrupt immediately after the first database commit of a COMMIT entry.
// The writes, session response, and applied index must survive together.
func TestTransactionCommitCrashRecovery(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	repl, db, closeReplica := openTxnFaultReplica(t, dir)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl.Replica)
	txid := []byte("atomic-commit")
	key := keys.MakeKey(constants.SystemPrefix, []byte("atomic-counter"))
	prepare := em.makeEntry(rbuilder.NewBatchBuilder().
		SetTransactionID(txid).
		Add(&rfpb.IncrementRequest{Key: key, Delta: 1}))
	rsp, err := repl.Update([]dbsm.Entry{prepare})
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())

	commit := em.makeEntry(rbuilder.NewBatchBuilder().
		SetTransactionID(txid).
		SetSession(&rfpb.Session{Id: []byte("commit-session"), Index: 1}).
		SetFinalizeOperation(rfpb.FinalizeOperation_COMMIT))
	db.afterCommit = func() { panic("crash after database commit") }
	require.PanicsWithValue(t, "crash after database commit", func() {
		repl.Update([]dbsm.Entry{commit})
	})
	db.afterCommit = nil
	closeReplica()

	restarted, _, _ := openTxnFaultReplica(t, dir)
	appliedIndex, err := restarted.LastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, commit.Index, appliedIndex)
	// Dragonboat skips the applied entry. Exercise a client retry with the
	// same session to verify its response survived with the data.
	commit.Index++
	rsp, err = restarted.Update([]dbsm.Entry{commit})
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError(),
		"committed transaction must replay or deduplicate successfully")
	read, err := directRead(t, restarted, key)
	require.NoError(t, err)
	require.Equal(t, uint64(1), binary.LittleEndian.Uint64(read.GetKv().GetValue()))
	_, err = directRead(t, restarted, keys.MakeKey(constants.LocalTransactionPrefix, txid))
	require.True(t, status.IsNotFoundError(err))
}

func TestTransactionPersistenceFailure(t *testing.T) {
	for _, op := range []rfpb.FinalizeOperation{
		rfpb.FinalizeOperation_UNKNOWN_OPERATION,
		rfpb.FinalizeOperation_COMMIT,
		rfpb.FinalizeOperation_ROLLBACK,
	} {
		t.Run(op.String(), func(t *testing.T) {
			dir := testfs.MakeTempDir(t)
			repl, db, closeReplica := openTxnFaultReplica(t, dir)
			em := newEntryMaker(t)
			writeDefaultRangeDescriptor(t, em, repl.Replica)
			txid := []byte("failed-transaction")
			key := []byte("transaction-value")
			prepare := rbuilder.NewBatchBuilder().SetTransactionID(txid).
				Add(&rfpb.DirectWriteRequest{Kv: &rfpb.KV{Key: key, Value: []byte("committed")}})
			if op != rfpb.FinalizeOperation_UNKNOWN_OPERATION {
				rsp, err := repl.Update([]dbsm.Entry{em.makeEntry(prepare)})
				require.NoError(t, err)
				require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
			}
			batch := prepare
			if op != rfpb.FinalizeOperation_UNKNOWN_OPERATION {
				batch = rbuilder.NewBatchBuilder().SetTransactionID(txid).SetFinalizeOperation(op)
			}
			entry := em.makeEntry(batch.SetSession(&rfpb.Session{Id: []byte("failed-session"), Index: 1}))
			injected := errors.New("injected transaction commit failure")
			db.beforeCommit = func() error { return injected }
			failed, err := repl.Update([]dbsm.Entry{entry})
			require.NoError(t, err)
			// Commit failures are reported in the entry result.
			// Update returns no error.
			require.EqualValues(t, constants.EntryErrorValue, failed[0].Result.Value)
			failure := &statuspb.Status{}
			require.NoError(t, proto.Unmarshal(failed[0].Result.Data, failure))
			require.Contains(t, failure.GetMessage(), injected.Error())
			db.beforeCommit = nil
			index, err := repl.LastAppliedIndex()
			require.NoError(t, err)
			require.Equal(t, entry.Index-1, index)
			_, err = directRead(t, repl, key)
			require.True(t, status.IsNotFoundError(err))
			_, err = directRead(t, repl, keys.MakeKey(constants.SessionPrefix, []byte("failed-session")))
			require.True(t, status.IsNotFoundError(err))
			closeReplica()

			restarted, _, _ := openTxnFaultReplica(t, dir)
			rsp, err := restarted.Update([]dbsm.Entry{entry})
			require.NoError(t, err)
			require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
			if op == rfpb.FinalizeOperation_UNKNOWN_OPERATION {
				entry = em.makeEntry(rbuilder.NewBatchBuilder().
					SetTransactionID(txid).SetFinalizeOperation(rfpb.FinalizeOperation_COMMIT))
				rsp, err = restarted.Update([]dbsm.Entry{entry})
				require.NoError(t, err)
				require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
			}
			read, err := directRead(t, restarted, key)
			if op == rfpb.FinalizeOperation_ROLLBACK {
				require.True(t, status.IsNotFoundError(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, []byte("committed"), read.GetKv().GetValue())
			}
		})
	}
}

func TestTransactionMalformedFinalization(t *testing.T) {
	for _, op := range []rfpb.FinalizeOperation{rfpb.FinalizeOperation_COMMIT, rfpb.FinalizeOperation_ROLLBACK} {
		t.Run(op.String(), func(t *testing.T) {
			repl, _, _ := openTxnFaultReplica(t, testfs.MakeTempDir(t))
			em := newEntryMaker(t)
			writeDefaultRangeDescriptor(t, em, repl.Replica)
			txid := []byte("malformed-finalize")
			write := &rfpb.DirectWriteRequest{Kv: &rfpb.KV{Key: []byte("foo"), Value: []byte("bar")}}
			rsp, err := repl.Update([]dbsm.Entry{em.makeEntry(rbuilder.NewBatchBuilder().
				SetTransactionID(txid).SetLockMappedRange(true).Add(write))})
			require.NoError(t, err)
			require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
			rsp, err = repl.Update([]dbsm.Entry{em.makeEntry(rbuilder.NewBatchBuilder().
				SetTransactionID(txid).SetFinalizeOperation(op).Add(write))})
			require.NoError(t, err)
			if rsp[0].Result.Value != constants.EntryErrorValue {
				require.Error(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
			}
			_, err = directRead(t, repl, []byte("foo"))
			require.True(t, status.IsNotFoundError(err), "rejected finalization must not apply writes")
			_, err = directRead(t, repl, keys.MakeKey(constants.LocalTransactionPrefix, txid))
			require.NoError(t, err, "rejected finalization must preserve the prepared record")

			// A different key in the mapped range must remain locked too.
			rsp, err = repl.Update([]dbsm.Entry{em.makeEntry(rbuilder.NewBatchBuilder().
				Add(&rfpb.DirectWriteRequest{Kv: &rfpb.KV{Key: []byte("other"), Value: []byte("blocked")}}))})
			require.NoError(t, err)
			require.Error(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
			rsp, err = repl.Update([]dbsm.Entry{em.makeEntry(rbuilder.NewBatchBuilder().
				SetTransactionID(txid).SetFinalizeOperation(rfpb.FinalizeOperation_COMMIT))})
			require.NoError(t, err)
			require.NoError(t, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
		})
	}
}

func TestTransactionFinalizePreservesOtherMappedRangeLock(t *testing.T) {
	for _, op := range []rfpb.FinalizeOperation{rfpb.FinalizeOperation_COMMIT, rfpb.FinalizeOperation_ROLLBACK} {
		t.Run(op.String(), func(t *testing.T) {
			repl := testutil.NewTestingReplica(t, 1, 1)
			t.Cleanup(func() { require.NoError(t, repl.Close()) })
			_, err := repl.Open(make(chan struct{}))
			require.NoError(t, err)
			em := newEntryMaker(t)
			writeDefaultRangeDescriptor(t, em, repl.Replica)
			apply := func(batch *rbuilder.BatchBuilder) error {
				t.Helper()
				rsp, err := repl.Update([]dbsm.Entry{em.makeEntry(batch)})
				require.NoError(t, err)
				require.NotEqualValues(t, constants.EntryErrorValue, rsp[0].Result.Value)
				return rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError()
			}

			txA := []byte("range-lock-owner")
			txB := []byte("other-transaction")
			require.NoError(t, apply(rbuilder.NewBatchBuilder().
				SetTransactionID(txA).SetLockMappedRange(true).
				Add(&rfpb.DirectWriteRequest{Kv: &rfpb.KV{Key: []byte("a"), Value: []byte("A")}})))
			// B writes outside the mapped range, so it can prepare while A
			// holds the range lock.
			localKey := keys.MakeKey(constants.SystemPrefix, []byte("other-transaction"))
			require.NoError(t, apply(rbuilder.NewBatchBuilder().
				SetTransactionID(txB).
				Add(&rfpb.DirectWriteRequest{Kv: &rfpb.KV{Key: localKey, Value: []byte("B")}})))
			require.NoError(t, apply(rbuilder.NewBatchBuilder().
				SetTransactionID(txB).SetFinalizeOperation(op)))

			// Use a key A did not write, so only its range lock can block it.
			probe := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
				Kv: &rfpb.KV{Key: []byte("b"), Value: []byte("probe")},
			})
			err = apply(probe)
			require.True(t, status.IsUnavailableError(err), "another transaction must not release A's range lock")
			require.Contains(t, err.Error(), constants.ConflictKeyMsg)
			_, err = directRead(t, repl, []byte("b"))
			require.True(t, status.IsNotFoundError(err))

			require.NoError(t, apply(rbuilder.NewBatchBuilder().
				SetTransactionID(txA).SetFinalizeOperation(op)))
			require.NoError(t, apply(probe), "finalizing the owner must release its range lock")
		})
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

	txid := []byte("TX1")
	cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("bar"),
		},
	}).ToProto()
	err = applyTransaction(t, em, repl.Replica, txid, cmd)
	require.NoError(t, err)

	txid2 := []byte("TX2")
	badCmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("baz"),
		},
	}).ToProto()
	err = applyTransaction(t, em, repl.Replica, txid2, badCmd)
	require.Error(t, err)

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

	err = applyTransaction(t, em, repl.Replica, txid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_COMMIT.Enum()})
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
	err = applyTransaction(t, em, repl.Replica, txid, cmd)
	require.NoError(t, err)

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
		txid2 := []byte("TX2")
		badCmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("azzzz"),
				Value: []byte("baz"),
			},
		}).ToProto()
		err = applyTransaction(t, em, repl.Replica, txid2, badCmd)
		require.Error(t, err)
	}

	err = applyTransaction(t, em, repl.Replica, txid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_COMMIT.Enum()})
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
		txid2 := []byte("TX2")
		badCmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("azzzz"),
				Value: []byte("baz"),
			},
		}).ToProto()
		err = applyTransaction(t, em, repl.Replica, txid2, badCmd)
		require.NoError(t, err)
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

	txid := []byte("TX1")
	cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   []byte("foo"),
			Value: []byte("bar"),
		},
	}).ToProto()
	err = applyTransaction(t, em, repl.Replica, txid, cmd)
	require.NoError(t, err)

	err = applyTransaction(t, em, repl.Replica, txid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_ROLLBACK.Enum(), TxnFinalizedAtUsec: time.Now().UnixMicro()})
	require.NoError(t, err)

	buf, _, err := repl.DB().Get([]byte("foo"))
	require.Error(t, err)
	require.Nil(t, buf)
}

func TestRollbackMarkerSurvivesRestartAndRejectsPrepare(t *testing.T) {
	txid := []byte("TX1")
	em := newEntryMaker(t)
	var leaser pebble.Leaser

	{
		repl := testutil.NewTestingReplica(t, 1, 1)
		leaser = repl.Leaser()
		require.NotNil(t, repl)

		stopc := make(chan struct{})
		_, err := repl.Open(stopc)
		require.NoError(t, err)

		writeDefaultRangeDescriptor(t, em, repl.Replica)

		err = applyTransaction(t, em, repl.Replica, txid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_ROLLBACK.Enum(), TxnFinalizedAtUsec: time.Now().UnixMicro()})
		require.NoError(t, err)

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

		cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}).ToProto()
		err = applyTransaction(t, em, repl.Replica, txid, cmd)
		require.Error(t, err)
		require.Contains(t, err.Error(), constants.TxnRolledBackMessage)
	}
}

func TestRollbackMarkerGCFiltersByTimestamp(t *testing.T) {
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

	now := time.Now()
	oldTxid := []byte("old-tx")
	newTxid := []byte("new-tx")

	require.NoError(t, applyTransaction(t, em, repl.Replica, oldTxid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_ROLLBACK.Enum(), TxnFinalizedAtUsec: now.Add(-4 * 24 * time.Hour).UnixMicro()}))
	require.NoError(t, applyTransaction(t, em, repl.Replica, newTxid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_ROLLBACK.Enum(), TxnFinalizedAtUsec: now.UnixMicro()}))

	hasMarkers, err := repl.HasTxnRollbackMarkersBeforeForTest(now.Add(-3 * 24 * time.Hour).UnixMicro())
	require.NoError(t, err)
	require.True(t, hasMarkers)

	batch := rbuilder.NewBatchBuilder().Add(&rfpb.DeleteTxnRollbackMarkersBeforeRequest{
		CutoffUsec: now.Add(-3 * 24 * time.Hour).UnixMicro(),
	})
	entry := em.makeEntry(batch)
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.NoError(t, rbuilder.NewBatchResponse(writeRsp[0].Result.Data).AnyError())

	_, err = directRead(t, repl, keys.MakeKey(constants.LocalTxnRollbackMarkerPrefix, oldTxid))
	require.Error(t, err)

	_, err = directRead(t, repl, keys.MakeKey(constants.LocalTxnRollbackMarkerPrefix, newTxid))
	require.NoError(t, err)
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

		cmd, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		}).ToProto()
		err = applyTransaction(t, em, repl.Replica, txid, cmd)
		require.NoError(t, err)

		cmd2, _ := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   []byte("baz"),
				Value: []byte("bap"),
			},
		}).ToProto()
		err = applyTransaction(t, em, repl.Replica, txid2, cmd2)
		require.NoError(t, err)

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

		err = applyTransaction(t, em, repl.Replica, txid, &rfpb.BatchCmdRequest{FinalizeOperation: rfpb.FinalizeOperation_COMMIT.Enum()})
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
			Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key")),
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
			Key:   keys.MakeKey(constants.SystemPrefix, []byte("incr-key")),
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

	// Write the initial file metadata
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
		Key:          fileMetadataKey,
		FileMetadata: md,
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

func TestUpdateATimeGCSCustomTime(t *testing.T) {
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

	initialCustomTime := int64(1_000_000)
	md := &sgpb.FileMetadata{
		FileRecord: fileRecord,
		StorageMetadata: &sgpb.StorageMetadata{
			GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
				BlobName:           "test-blob",
				LastCustomTimeUsec: initialCustomTime,
			},
		},
		LastAccessUsec: int64(1_000_000),
	}

	fs := filestore.New()
	key, err := fs.PebbleKey(fileRecord)
	require.NoError(t, err)
	fileMetadataKey, err := key.Bytes(filestore.Version5)
	require.NoError(t, err)

	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
		Key:          fileMetadataKey,
		FileMetadata: md,
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, len(writeRsp))

	updateATime := func(t *testing.T, req *rfpb.UpdateAtimeRequest) *sgpb.FileMetadata {
		t.Helper()
		entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(req))
		updateRsp, err := repl.Update([]dbsm.Entry{entry})
		require.NoError(t, err)
		require.Equal(t, 1, len(updateRsp))
		require.NoError(t, rbuilder.NewBatchResponse(updateRsp[0].Result.Data).AnyError())

		rsp, err := directRead(t, repl, fileMetadataKey)
		require.NoError(t, err)
		gotMd := &sgpb.FileMetadata{}
		require.NoError(t, proto.Unmarshal(rsp.GetKv().GetValue(), gotMd))
		return gotMd
	}

	// An update that did not refresh the object's custom time leaves the
	// recorded custom time alone.
	{
		gotMd := updateATime(t, &rfpb.UpdateAtimeRequest{
			Key:            fileMetadataKey,
			AccessTimeUsec: int64(2_000_000),
		})
		require.Equal(t, int64(2_000_000), gotMd.GetLastAccessUsec())
		require.Equal(t, initialCustomTime, gotMd.GetStorageMetadata().GetGcsMetadata().GetLastCustomTimeUsec(), "custom time should be untouched when the request does not set one")
	}

	// A refreshed custom time is recorded.
	newCustomTime := int64(3_000_000)
	{
		gotMd := updateATime(t, &rfpb.UpdateAtimeRequest{
			Key:                fileMetadataKey,
			AccessTimeUsec:     int64(3_000_000),
			LastCustomTimeUsec: newCustomTime,
		})
		require.Equal(t, newCustomTime, gotMd.GetStorageMetadata().GetGcsMetadata().GetLastCustomTimeUsec(), "refreshed custom time should be recorded")
	}

	// The custom time only moves forward, so a replayed or reordered update
	// cannot make a live object look older than it is.
	{
		gotMd := updateATime(t, &rfpb.UpdateAtimeRequest{
			Key:                fileMetadataKey,
			AccessTimeUsec:     int64(4_000_000),
			LastCustomTimeUsec: int64(2_000_000),
		})
		require.Equal(t, newCustomTime, gotMd.GetStorageMetadata().GetGcsMetadata().GetLastCustomTimeUsec(), "custom time should not move backwards")
	}

	// A stale atime does not block recording a newer custom time.
	{
		gotMd := updateATime(t, &rfpb.UpdateAtimeRequest{
			Key:                fileMetadataKey,
			AccessTimeUsec:     int64(1),
			LastCustomTimeUsec: int64(5_000_000),
		})
		require.Equal(t, int64(4_000_000), gotMd.GetLastAccessUsec(), "atime should not move backwards")
		require.Equal(t, int64(5_000_000), gotMd.GetStorageMetadata().GetGcsMetadata().GetLastCustomTimeUsec(), "custom time should be recorded even when the atime is stale")
	}
}
