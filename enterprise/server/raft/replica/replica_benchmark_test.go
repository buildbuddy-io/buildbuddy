package replica_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

// Benchmarks for the apply path of the two mutations that dominate cache write
// traffic, SetRequest and UpdateAtimeRequest. They report the pebble bytes
// written per op alongside latency.

func benchSetEntry(em *entryMaker, fr *sgpb.FileRecord, key []byte, atimeUsec int64) dbsm.Entry {
	return em.makeEntry(rbuilder.NewBatchBuilder().Add(setRequest(fr, key, atimeUsec)))
}

// pebbleWriteStats is a snapshot of pebble's write-side metrics.
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

// reportWriteAmp flushes the memtable and reports the pebble bytes written
// per op since `before`: WAL, flush and compaction.
func reportWriteAmp(b *testing.B, db pebble.IPebbleDB, before pebbleWriteStats, ops int) {
	require.NoError(b, db.Flush())
	after := writeStats(db)
	b.ReportMetric(float64(after.walBytes-before.walBytes)/float64(ops), "wal-B/op")
	b.ReportMetric(float64(after.flushedBytes-before.flushedBytes)/float64(ops), "flush-B/op")
	b.ReportMetric(float64(after.compactedBytes-before.compactedBytes)/float64(ops), "compact-B/op")
}

// reportIndexEntries reports the number of atime-index entries, and how many
// of them are in excess of one per record.
func reportIndexEntries(b *testing.B, db pebble.IPebbleDB, nRecords int) {
	start, end := keys.AtimeIndexPartitionRange(defaultPartition)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	require.NoError(b, err)
	defer iter.Close()
	entries := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		entries++
	}
	b.ReportMetric(float64(entries), "idx-entries")
	b.ReportMetric(float64(max(entries-nRecords, 0)), "idx-orphans")
}

// BenchmarkSetApply measures applying SetRequests. It cycles through a fixed
// pool of records, so most applies are overwrites.
func BenchmarkSetApply(b *testing.B) {
	repl, em := openTestReplica(b)
	const n = 1024
	records, recordKeys := testRecords(b, n)
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

	// Errors are carried in the responses, so check one apply end to end.
	e := benchSetEntry(em, records[0], recordKeys[0], int64(1_000_000+i))
	rsp, err := repl.Update([]dbsm.Entry{e})
	require.NoError(b, err)
	require.NoError(b, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())

	reportWriteAmp(b, db, before, i)
	reportIndexEntries(b, db, n)
}

// BenchmarkUpdateAtimeApply measures applying UpdateAtimeRequests.
func BenchmarkUpdateAtimeApply(b *testing.B) {
	repl, em := openTestReplica(b)
	const n = 1024
	records, recordKeys := testRecords(b, n)
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
		// The atime always increases, so every apply writes.
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
	reportIndexEntries(b, db, n)
}
