package replica_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type fakeStore struct{}

func (fs *fakeStore) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica)    {}
func (fs *fakeStore) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {}
func (fs *fakeStore) ReadFileFromPeer(ctx context.Context, except *rfpb.ReplicaDescriptor, fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
	return nil, nil
}
func (fs *fakeStore) GetReplica(rangeID uint64) (*replica.Replica, error) {
	return nil, nil
}
func (fs *fakeStore) Sender() *sender.Sender {
	return nil
}

func TestSnapshotAndRestore(t *testing.T) {
	baseDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(baseDir, &pebble.Options{})
	require.Nil(t, err)

	snapFile, err := os.CreateTemp(baseDir, "snapfile-*")
	require.Nil(t, err)
	snapFileName := snapFile.Name()
	defer os.Remove(snapFileName)

	hashes := make(map[string]struct{}, 0)
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		hashes[d.GetHash()] = struct{}{}
		err := db.Set([]byte(d.GetHash()), buf, nil)
		require.Nil(t, err)
	}

	err = replica.SaveSnapshotToWriter(snapFile, db.NewSnapshot())
	require.Nil(t, err)
	snapFile.Close()
	db.Close()

	snapFile, err = os.Open(snapFileName)
	require.Nil(t, err)

	newDir := testfs.MakeTempDir(t)
	db, err = pebble.Open(newDir, &pebble.Options{})
	require.Nil(t, err)
	err = replica.ApplySnapshotFromReader(snapFile, db)
	require.Nil(t, err)

	iter := db.NewIter(&pebble.IterOptions{})
	for iter.First(); iter.Valid(); iter.Next() {
		hash := string(iter.Key())
		if _, ok := hashes[hash]; !ok {
			t.Fatalf("Read hash from snapshot created DB that was not written.")
		}
		delete(hashes, hash)
	}
	require.Equal(t, 0, len(hashes))
	db.Close()
}

func TestOpenCloseReplica(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	fileDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, fileDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.Nil(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)

	err = repl.Close()
	require.Nil(t, err)
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
		Left:    []byte{constants.MinByte},
		Right:   []byte("z"),
		RangeId: 1,
	})
	require.Nil(t, err)
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}))
	entries := []dbsm.Entry{entry}
	writeRsp, err := r.Update(entries)
	require.Nil(t, err)
	require.Equal(t, 1, len(writeRsp))
}

func TestReplicaDirectReadWrite(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	fileDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, fileDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.Nil(t, err)
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
	require.Nil(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Do a DirectRead and verify the value is was written.
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: []byte("key-name"),
	}).ToBuf()
	readRsp, err := repl.Lookup(buf)
	require.Nil(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	directRead, err := readBatch.DirectReadResponse(0)
	require.Nil(t, err)

	require.Equal(t, []byte("key-value"), directRead.GetKv().GetValue())

	err = repl.Close()
	require.Nil(t, err)
}

func TestReplicaIncrement(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	fileDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, fileDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.Nil(t, err)
	require.Equal(t, uint64(0), lastAppliedIndex)
	em := newEntryMaker(t)
	writeDefaultRangeDescriptor(t, em, repl)

	// Do a DirectWrite.
	entry := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   []byte("incr-key"),
		Delta: 1,
	}))
	writeRsp, err := repl.Update([]dbsm.Entry{entry})
	require.Nil(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Make sure the response holds the new value.
	incrBatch := rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	incrRsp, err := incrBatch.IncrementResponse(0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), incrRsp.GetValue())

	// Increment the same key again by a different value.
	entry = em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   []byte("incr-key"),
		Delta: 3,
	}))
	writeRsp, err = repl.Update([]dbsm.Entry{entry})
	require.Nil(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Make sure the response holds the new value.
	incrBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	incrRsp, err = incrBatch.IncrementResponse(0)
	require.Nil(t, err)
	require.Equal(t, uint64(4), incrRsp.GetValue())

	err = repl.Close()
	require.Nil(t, err)
}

func TestReplicaCAS(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	fileDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, fileDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.Nil(t, err)
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
	require.Nil(t, err)
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
	require.Nil(t, err)

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
	require.Nil(t, err)

	readBatch = rbuilder.NewBatchResponse(writeRsp[0].Result.Data)
	casRsp, err = readBatch.CASResponse(0)
	require.Nil(t, err)
	require.Equal(t, []byte("new-key-value"), casRsp.GetKv().GetValue())

	err = repl.Close()
	require.Nil(t, err)
}

func TestReplicaScan(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	fileDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	repl := replica.New(rootDir, fileDir, 1, 1, store)
	require.NotNil(t, repl)

	stopc := make(chan struct{})
	lastAppliedIndex, err := repl.Open(stopc)
	require.Nil(t, err)
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
	require.Nil(t, err)
	require.Equal(t, 1, len(writeRsp))

	// Ensure that scan reads just the ranges we want.
	// Scan b-c.
	buf, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("b")),
		Right:    keys.RangeMetaKey([]byte("c")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err := repl.Lookup(buf)
	require.Nil(t, err)

	readBatch := rbuilder.NewBatchResponse(readRsp)
	scanRsp, err := readBatch.ScanResponse(0)
	require.Nil(t, err)
	require.Equal(t, []byte("range-1"), scanRsp.GetKvs()[0].GetValue())

	// Scan c-d.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("c")),
		Right:    keys.RangeMetaKey([]byte("d")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err = repl.Lookup(buf)
	require.Nil(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.Nil(t, err)
	require.Equal(t, []byte("range-2"), scanRsp.GetKvs()[0].GetValue())

	// Scan d-*.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("d")),
		Right:    keys.RangeMetaKey([]byte("z")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err = repl.Lookup(buf)
	require.Nil(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.Nil(t, err)
	require.Equal(t, []byte("range-3"), scanRsp.GetKvs()[0].GetValue())

	// Scan the full range.
	buf, err = rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey([]byte("a")),
		Right:    keys.RangeMetaKey([]byte("z")),
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToBuf()
	readRsp, err = repl.Lookup(buf)
	require.Nil(t, err)

	readBatch = rbuilder.NewBatchResponse(readRsp)
	scanRsp, err = readBatch.ScanResponse(0)
	require.Nil(t, err)
	require.Equal(t, []byte("range-1"), scanRsp.GetKvs()[0].GetValue())
	require.Equal(t, []byte("range-2"), scanRsp.GetKvs()[1].GetValue())
	require.Equal(t, []byte("range-3"), scanRsp.GetKvs()[2].GetValue())

	err = repl.Close()
	require.Nil(t, err)
}
