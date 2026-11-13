package replica_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

// This file benchmarks the state-machine apply path for the two mutations
// that dominate cache write traffic: SetRequest (record writes) and
// UpdateAtimeRequest (the write side effect of FindMissing/Get hits older
// than atime_update_threshold). It reports pebble write-amplification
// metrics alongside latency.

// benchPayloadSize approximates a typical metadata record's inline payload.
const benchPayloadSize = 100

var benchPayload = make([]byte, benchPayloadSize)

// benchReplica opens a replica whose range covers the whole data keyspace.
func benchReplica(b *testing.B) (*testutil.TestingReplica, *entryMaker) {
	repl := testutil.NewTestingReplica(b, 1, 1)
	b.Cleanup(func() {
		require.NoError(b, repl.Close())
	})
	stopc := make(chan struct{})
	_, err := repl.Open(stopc)
	require.NoError(b, err)

	em := newEntryMaker(b)
	writeDefaultRangeDescriptor(b, em, repl.Replica)
	return repl, em
}

// benchRecords returns n records with deterministic, collision-free digests
// plus their file-metadata keys.
func benchRecords(b *testing.B, n int) ([]*sgpb.FileRecord, [][]byte) {
	fs := filestore.New()
	records := make([]*sgpb.FileRecord, 0, n)
	recordKeys := make([][]byte, 0, n)
	for i := range n {
		fr := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   rspb.CacheType_CAS,
				PartitionId: defaultPartition,
				GroupId:     interfaces.AuthAnonymousUser,
			},
			Digest:         &repb.Digest{Hash: fmt.Sprintf("%064x", i+1), SizeBytes: benchPayloadSize},
			DigestFunction: repb.DigestFunction_SHA256,
		}
		pk, err := fs.PebbleKey(fr)
		require.NoError(b, err)
		kb, err := pk.Bytes(filestore.Version5)
		require.NoError(b, err)
		records = append(records, fr)
		recordKeys = append(recordKeys, kb)
	}
	return records, recordKeys
}

func benchSetEntry(em *entryMaker, fr *sgpb.FileRecord, key []byte, atimeUsec int64) dbsm.Entry {
	md := &sgpb.FileMetadata{
		FileRecord: fr,
		StorageMetadata: &sgpb.StorageMetadata{
			InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{Data: benchPayload},
		},
		StoredSizeBytes: benchPayloadSize,
		LastAccessUsec:  atimeUsec,
		LastModifyUsec:  atimeUsec,
	}
	return em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
		Key:          key,
		FileMetadata: md,
	}))
}

// pebbleWriteStats snapshots the write-side pebble metrics we care about.
type pebbleWriteStats struct {
	walBytes       uint64
	flushedBytes   uint64
	compactedBytes uint64
}

func writeStats(db pebble.IPebbleDB) pebbleWriteStats {
	m := db.Metrics()
	s := pebbleWriteStats{walBytes: m.WAL.BytesWritten}
	for _, lm := range m.Levels {
		s.flushedBytes += lm.BytesFlushed
		s.compactedBytes += lm.BytesCompacted
	}
	return s
}

// reportWriteAmp flushes the memtable and reports per-op pebble write bytes
// accumulated since `before`: WAL bytes (direct batch-commit cost), flushed
// bytes, and compaction write bytes (background write amplification).
func reportWriteAmp(b *testing.B, db pebble.IPebbleDB, before pebbleWriteStats, ops int) {
	require.NoError(b, db.Flush())
	after := writeStats(db)
	b.ReportMetric(float64(after.walBytes-before.walBytes)/float64(ops), "wal-B/op")
	b.ReportMetric(float64(after.flushedBytes-before.flushedBytes)/float64(ops), "flush-B/op")
	b.ReportMetric(float64(after.compactedBytes-before.compactedBytes)/float64(ops), "compact-B/op")
}

// BenchmarkSetApply measures the apply cost of SetRequest record writes. It
// cycles through a fixed pool of records, so most applies are overwrites.
// Request construction happens in-loop, so it is included in the time.
func BenchmarkSetApply(b *testing.B) {
	repl, em := benchReplica(b)
	const n = 1024
	records, recordKeys := benchRecords(b, n)
	db := repl.DB()
	before := writeStats(db)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		e := benchSetEntry(em, records[i%n], recordKeys[i%n], int64(1_000_000+i))
		if _, err := repl.Update([]dbsm.Entry{e}); err != nil {
			b.Fatal(err)
		}
		i++
	}
	b.StopTimer()

	// Union errors are carried in responses, not Update's error; verify the
	// path end-to-end once rather than paying response decoding per op.
	e := benchSetEntry(em, records[0], recordKeys[0], int64(1_000_000+i))
	rsp, err := repl.Update([]dbsm.Entry{e})
	require.NoError(b, err)
	require.NoError(b, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())

	reportWriteAmp(b, db, before, i)
}

// BenchmarkUpdateAtimeApply measures the apply cost of UpdateAtimeRequest --
// the write FindMissing traffic generates once records age past
// atime_update_threshold.
func BenchmarkUpdateAtimeApply(b *testing.B) {
	repl, em := benchReplica(b)
	const n = 1024
	records, recordKeys := benchRecords(b, n)
	for i := range n {
		e := benchSetEntry(em, records[i], recordKeys[i], 1_000_000)
		if _, err := repl.Update([]dbsm.Entry{e}); err != nil {
			b.Fatal(err)
		}
	}
	db := repl.DB()
	before := writeStats(db)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		// The atime strictly increases, so every apply takes the write path
		// rather than the atime-moved-backwards early return.
		e := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.UpdateAtimeRequest{
			Key:            recordKeys[i%n],
			AccessTimeUsec: int64(2_000_000 + i),
		}))
		if _, err := repl.Update([]dbsm.Entry{e}); err != nil {
			b.Fatal(err)
		}
		i++
	}
	b.StopTimer()

	e := em.makeEntry(rbuilder.NewBatchBuilder().Add(&rfpb.UpdateAtimeRequest{
		Key:            recordKeys[0],
		AccessTimeUsec: int64(2_000_000 + i),
	}))
	rsp, err := repl.Update([]dbsm.Entry{e})
	require.NoError(b, err)
	require.NoError(b, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())

	reportWriteAmp(b, db, before, i)
}
