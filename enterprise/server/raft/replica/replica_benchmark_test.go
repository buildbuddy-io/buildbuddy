package replica_test

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	pebble "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebblev2"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

var pebbleBenchFormats = flag.String("pebble_bench_formats", "16,19", "Comma-separated pebble format major versions that BenchmarkPebbleFormat compares.")

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
		// Blob bytes are non-zero only with value separation.
		s.flushedBytes += lm.TableBytesFlushed + lm.BlobBytesFlushed
		s.compactedBytes += lm.TableBytesCompacted + lm.BlobBytesCompacted
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

// BenchmarkPebbleFormat compares replica operations across pebble format
// major versions, which decide the SSTable layout: 16 (FormatVirtualSSTables)
// writes row blocks, 19 (FormatColumnarBlocks) and up write columnar blocks.
// Read datasets are flushed and compacted, so reads hit SSTables, not the
// memtable. A format alone does not enable value separation; that also needs
// Options.ValueSeparationPolicy.
//
// Ops:
//
//	find-present, find-missing: a Lookup of 100 FindRequests, as in a
//	             SyncRead.
//	scan:        iterate every record in the partition, as eviction sampling
//	             does.
//	split-point: FindSplitPoint, two passes over the whole range.
//	write:       apply SetRequests to an empty DB, flushing about 4 times,
//	             then compact.
//	             Covers block encoding CPU and the size of what is written.
//
// Cache sizes for read ops:
//
//	hot:     2x the uncompressed data, warmed; isolates in-block work.
//	reduced: 10% of the uncompressed data, about the dev metadata server's
//	         block cache to data ratio; most data block reads miss.
//
// ns/record is time per record looked up, scanned or written. Run locally,
// not remotely, and compare formats with benchstat. The output is too large
// for bazel to print, so read test.log:
//
//	bb test //enterprise/server/raft/replica:replica_test --test_timeout=3600 \
//	  --test_arg=-test.run='^$' --test_arg=-test.bench=BenchmarkPebbleFormat \
//	  --test_arg=-pebble_bench_formats=16,19 --test_arg=-test.count=10
//	benchstat -col /format \
//	  bazel-testlogs/enterprise/server/raft/replica/replica_test/test.log
func BenchmarkPebbleFormat(b *testing.B) {
	formats := parseBenchFormats(b)
	storages := []formatBenchStorage{
		{name: "512B-inline", digestSizeBytes: 512, numRecords: 200_000},
		{name: "1KiB-gcs", digestSizeBytes: 1024, gcs: true, numRecords: 200_000},
		// Values are ~32 KiB, so fewer records keep the dataset near 300 MB.
		{name: "32KiB-inline", digestSizeBytes: 32 * 1024, numRecords: 10_000},
	}
	for _, storage := range storages {
		for _, format := range formats {
			b.Run(fmt.Sprintf("storage=%s/format=%d", storage.name, format), func(b *testing.B) {
				benchmarkFormat(b, storage, format)
			})
		}
	}
}

// benchmarkFormat runs every op for one storage and format. The read ops
// share a dataset, built on first use and deleted when b finishes.
func benchmarkFormat(b *testing.B, storage formatBenchStorage, format pebble.FormatMajorVersion) {
	dir := testfs.MakeTempDir(b)
	var ds *formatBenchDataset
	builtBy := ""
	dataset := func(b *testing.B) *formatBenchDataset {
		if builtBy == "" {
			// Set first: a failed build leaves a partial DB in dir, which
			// must not be built on again.
			builtBy = b.Name()
			ds = buildFormatBenchDataset(b, dir, storage, format)
		}
		if ds == nil {
			// That op already failed with the cause; don't repeat it here.
			b.Skipf("dataset build failed; see the error under %s", builtBy)
		}
		return ds
	}
	for _, op := range []string{"find-present", "find-missing", "scan", "split-point"} {
		for _, cache := range []string{"hot", "reduced"} {
			b.Run(fmt.Sprintf("op=%s/cache=%s", op, cache), func(b *testing.B) {
				benchmarkFormatRead(b, dataset(b), op, cache)
			})
		}
	}
	b.Run("op=write", func(b *testing.B) {
		benchmarkFormatWrite(b, storage, format)
	})
}

func parseBenchFormats(b *testing.B) []pebble.FormatMajorVersion {
	var formats []pebble.FormatMajorVersion
	for s := range strings.SplitSeq(*pebbleBenchFormats, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
		require.NoError(b, err, "-pebble_bench_formats")
		f := pebble.FormatMajorVersion(v)
		if f < pebble.FormatMinSupported || f > pebble.FormatNewest {
			b.Fatalf("-pebble_bench_formats: %d is outside [%d, %d]", v, pebble.FormatMinSupported, pebble.FormatNewest)
		}
		formats = append(formats, f)
	}
	return formats
}

type formatBenchStorage struct {
	name            string
	digestSizeBytes int64
	gcs             bool
	numRecords      int
}

// formatBenchDataset is a compacted DB on disk plus the lookup batches to run
// against it. It is built once per (storage, format) and reused across ops,
// cache sizes, and -test.count runs.
type formatBenchDataset struct {
	dir            string
	format         pebble.FormatMajorVersion
	numRecords     int
	rawBytes       int64 // uncompressed bytes written, including atime-index keys
	sstBytes       int64 // on-disk SSTable bytes after compaction
	presentBatches [][]byte
	missingBatches [][]byte
}

const (
	formatBenchFindBatchSize  = 100 // FindRequests per Lookup, as in BenchmarkFind
	formatBenchNumFindBatches = 1000
	formatBenchWriteBatchSize = 100 // SetRequests per raft entry
	formatBenchWriteFlushes   = 4
)

// formatBenchNowUsec is the fixed "now" that record atimes are relative to,
// so keys are identical across formats and runs.
var formatBenchNowUsec = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()

// formatBenchDB is a pebble DB in a given format, with a replica on top.
type formatBenchDB struct {
	db     pebble.IPebbleDB
	leaser pebble.Leaser
	repl   *testutil.TestingReplica
	em     *entryMaker
	unref  func()
}

// openFormatBenchDB opens the DB in dir with options that mirror the raft
// store's (store.getPebbleOptions), with the format and block cache varied.
func openFormatBenchDB(b *testing.B, dir string, format pebble.FormatMajorVersion, cacheBytes int64) *formatBenchDB {
	c := pebble.NewCache(cacheBytes)
	db, err := pebble.Open(dir, "bench", &pebble.Options{
		FormatMajorVersion: format,
		MemTableSize:       64 << 20,
		MaxOpenFiles:       2000,
		Cache:              c,
	})
	require.NoError(b, err)
	require.Equal(b, format, db.FormatMajorVersion())
	leaser := pebble.NewDBLeaser(db)
	repl := testutil.NewTestingReplicaWithLeaser(b, 1, 1, leaser)
	_, err = repl.Open(make(chan struct{}))
	require.NoError(b, err)
	return &formatBenchDB{db: db, leaser: leaser, repl: repl, em: newEntryMaker(b), unref: c.Unref}
}

func (d *formatBenchDB) close(b *testing.B) {
	require.NoError(b, d.repl.Close())
	d.leaser.Close()
	require.NoError(b, d.db.Close())
	d.unref()
}

// write applies marshaled SetRequest batches, flushing every flushEvery
// batches if positive, then flushes and compacts everything into SSTables
// written in this DB's format.
func (d *formatBenchDB) write(b *testing.B, batches [][]byte, flushEvery int) {
	for i, buf := range batches {
		d.em.index++
		rsp, err := d.repl.Update([]dbsm.Entry{{Cmd: buf, Index: d.em.index}})
		require.NoError(b, err)
		require.NoError(b, rbuilder.NewBatchResponse(rsp[0].Result.Data).AnyError())
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			require.NoError(b, d.db.Flush())
		}
	}
	require.NoError(b, d.db.Flush())
	require.NoError(b, d.db.Compact([]byte{0x00}, []byte{0xff, 0xff}, true /*parallelize*/))
}

func (d *formatBenchDB) sstBytes() int64 {
	var n int64
	for _, l := range d.db.Metrics().Levels {
		n += l.TablesSize
	}
	return n
}

// formatBenchRand is the seeded randomness for generating records and
// lookups. The same seed gives the same data, so every format is benchmarked
// on identical keys and values.
type formatBenchRand struct {
	rng     *rand.Rand
	digests *digest.Generator
}

func newFormatBenchRand(seed uint64) *formatBenchRand {
	return &formatBenchRand{
		rng:     rand.New(rand.NewPCG(seed, seed)),
		digests: digest.RandomGenerator(int64(seed)),
	}
}

// formatBenchRecords are random CAS records, marshaled as SetRequest batches.
type formatBenchRecords struct {
	keys     [][]byte
	batches  [][]byte
	rawBytes int64
}

func makeFormatBenchRecords(b *testing.B, r *formatBenchRand, storage formatBenchStorage, n int) *formatBenchRecords {
	fs := filestore.New()
	atimeSpreadUsec := (6 * time.Hour).Microseconds()
	recs := &formatBenchRecords{}
	batch := rbuilder.NewBatchBuilder()
	appendBatch := func() {
		buf, err := batch.ToBuf()
		require.NoError(b, err)
		recs.batches = append(recs.batches, buf)
		batch = rbuilder.NewBatchBuilder()
	}
	for i := range n {
		// Spread atimes over the last 6 hours, as in production, so
		// atime-index keys don't all share the same atime prefix.
		atimeUsec := formatBenchNowUsec - r.rng.Int64N(atimeSpreadUsec)
		key, md := formatBenchFileMetadata(b, r.digests, fs, storage, atimeUsec)
		recs.keys = append(recs.keys, key)
		// Each SetRequest writes the record and an atime-index key with an
		// empty value.
		indexKey := keys.AtimeIndexKey(defaultPartition, atimeUsec, key)
		recs.rawBytes += int64(len(key) + proto.Size(md) + len(indexKey))
		batch.Add(&rfpb.SetRequest{Key: key, FileMetadata: md})
		if (i+1)%formatBenchWriteBatchSize == 0 {
			appendBatch()
		}
	}
	if n%formatBenchWriteBatchSize != 0 {
		appendBatch()
	}
	return recs
}

// buildFormatBenchDataset writes the dataset into dir, which the caller owns:
// it outlives the sub-benchmark that builds it.
func buildFormatBenchDataset(b *testing.B, dir string, storage formatBenchStorage, format pebble.FormatMajorVersion) *formatBenchDataset {
	start := time.Now()
	r := newFormatBenchRand(1)
	recs := makeFormatBenchRecords(b, r, storage, storage.numRecords)
	d := openFormatBenchDB(b, dir, format, 64<<20)
	writeDefaultRangeDescriptor(b, d.em, d.repl.Replica)
	d.write(b, recs.batches, 0 /*flushEvery*/)
	ds := &formatBenchDataset{
		dir:        dir,
		format:     format,
		numRecords: storage.numRecords,
		rawBytes:   recs.rawBytes,
		sstBytes:   d.sstBytes(),
	}
	d.close(b)

	fs := filestore.New()
	for range formatBenchNumFindBatches {
		present := rbuilder.NewBatchBuilder()
		missing := rbuilder.NewBatchBuilder()
		for range formatBenchFindBatchSize {
			present.Add(&rfpb.FindRequest{Key: recs.keys[r.rng.IntN(len(recs.keys))]})
			// 64 random bytes make each missing key unique; a tiny digest
			// would repeat a few keys whose blocks always stay cached.
			missingKey, _ := formatBenchFileMetadata(b, r.digests, fs, formatBenchStorage{digestSizeBytes: 64}, 0)
			missing.Add(&rfpb.FindRequest{Key: missingKey})
		}
		buf, err := present.ToBuf()
		require.NoError(b, err)
		ds.presentBatches = append(ds.presentBatches, buf)
		buf, err = missing.ToBuf()
		require.NoError(b, err)
		ds.missingBatches = append(ds.missingBatches, buf)
	}
	b.Logf("built %s format=%d: %d records, %d MB raw, %d MB sst, in %s",
		storage.name, format, storage.numRecords, ds.rawBytes>>20, ds.sstBytes>>20, time.Since(start).Round(time.Second))
	return ds
}

// formatBenchFileMetadata returns a random CAS record's v5 key and metadata,
// shaped like BenchmarkFind's.
func formatBenchFileMetadata(b *testing.B, gen *digest.Generator, fs filestore.Store, storage formatBenchStorage, atimeUsec int64) ([]byte, *sgpb.FileMetadata) {
	d, buf, err := gen.RandomDigestBuf(storage.digestSizeBytes)
	require.NoError(b, err)
	fileRecord := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: defaultPartition,
			GroupId:     interfaces.AuthAnonymousUser,
		},
		Digest:         d,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	pebbleKey, err := fs.PebbleKey(fileRecord)
	require.NoError(b, err)
	key, err := pebbleKey.Bytes(filestore.Version5)
	require.NoError(b, err)

	storageMetadata := &sgpb.StorageMetadata{
		InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
			Data:          buf,
			CreatedAtNsec: atimeUsec * 1000,
		},
	}
	if storage.gcs {
		storageMetadata = &sgpb.StorageMetadata{
			GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
				BlobName:           d.GetHash(),
				LastCustomTimeUsec: atimeUsec,
			},
		}
	}
	return key, &sgpb.FileMetadata{
		FileRecord:      fileRecord,
		StorageMetadata: storageMetadata,
		StoredSizeBytes: storage.digestSizeBytes,
		LastAccessUsec:  atimeUsec,
		LastModifyUsec:  atimeUsec,
	}
}

func benchmarkFormatRead(b *testing.B, ds *formatBenchDataset, op string, cache string) {
	cacheBytes := ds.rawBytes * 2
	if cache == "reduced" {
		cacheBytes = ds.rawBytes / 10
	}
	d := openFormatBenchDB(b, ds.dir, ds.format, cacheBytes)
	defer d.close(b)

	// check verifies results and warms the cache; run is one timed op.
	var check, run func()
	var recordsPerOp int
	switch op {
	case "find-present", "find-missing":
		present := op == "find-present"
		batches := ds.presentBatches
		if !present {
			batches = ds.missingBatches
		}
		recordsPerOp = formatBenchFindBatchSize
		check = func() {
			for _, buf := range batches {
				rspBuf, err := d.repl.Lookup(buf)
				require.NoError(b, err)
				rsp := rbuilder.NewBatchResponse(rspBuf)
				for i := range formatBenchFindBatchSize {
					findRsp, err := rsp.FindResponse(i)
					require.NoError(b, err)
					require.Equal(b, present, findRsp.GetPresent())
				}
			}
		}
		i := 0
		run = func() {
			if _, err := d.repl.Lookup(batches[i%len(batches)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	case "scan":
		start, end := keys.Range([]byte(filestore.PartitionDirectoryPrefix + defaultPartition + "/"))
		scan := func() int {
			iter, err := d.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
			if err != nil {
				b.Fatal(err)
			}
			defer iter.Close()
			n := 0
			for iter.First(); iter.Valid(); iter.Next() {
				// Read the value too, as eviction sampling does.
				_ = iter.Value()
				n++
			}
			if err := iter.Error(); err != nil {
				b.Fatal(err)
			}
			return n
		}
		recordsPerOp = ds.numRecords
		check = func() { require.Equal(b, ds.numRecords, scan()) }
		run = func() { scan() }
	case "split-point":
		req, err := rbuilder.NewBatchBuilder().Add(&rfpb.FindSplitPointRequest{}).ToBuf()
		require.NoError(b, err)
		recordsPerOp = ds.numRecords
		check = func() {
			rspBuf, err := d.repl.Lookup(req)
			require.NoError(b, err)
			rsp, err := rbuilder.NewBatchResponse(rspBuf).FindSplitPointResponse(0)
			require.NoError(b, err)
			require.NotEmpty(b, rsp.GetSplitKey())
		}
		run = func() {
			if _, err := d.repl.Lookup(req); err != nil {
				b.Fatal(err)
			}
		}
	default:
		b.Fatalf("unknown op %q", op)
	}

	check()
	before := d.db.Metrics().BlockCache
	b.ReportAllocs()
	ops := 0
	for b.Loop() {
		run()
		ops++
	}
	after := d.db.Metrics().BlockCache
	hits := after.Hits - before.Hits
	misses := after.Misses - before.Misses
	if hits+misses > 0 {
		b.ReportMetric(100*float64(hits)/float64(hits+misses), "cache-hit-%")
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(ops*recordsPerOp), "ns/record")
	b.ReportMetric(float64(ds.sstBytes)/(1<<20), "sst-MB")
}

// benchmarkFormatWrite times writing a tenth of the storage's records into an
// empty DB: apply, flush about formatBenchWriteFlushes times so the L0 tables
// overlap, then compact them into one level. Opening and closing the DB are
// not timed.
func benchmarkFormatWrite(b *testing.B, storage formatBenchStorage, format pebble.FormatMajorVersion) {
	n := storage.numRecords / 10
	recs := makeFormatBenchRecords(b, newFormatBenchRand(2), storage, n)
	flushEvery := max(len(recs.batches)/formatBenchWriteFlushes, 1)
	root := testfs.MakeTempDir(b)

	var flushedBytes, compactedBytes uint64
	var sstBytes int64
	b.ReportAllocs()
	ops := 0
	for b.Loop() {
		b.StopTimer()
		// Under root, so a failed iteration's directory is still deleted.
		dir, err := os.MkdirTemp(root, "write-")
		require.NoError(b, err)
		d := openFormatBenchDB(b, dir, format, 64<<20)
		writeDefaultRangeDescriptor(b, d.em, d.repl.Replica)
		before := writeStats(d.db)
		b.StartTimer()

		d.write(b, recs.batches, flushEvery)

		b.StopTimer()
		after := writeStats(d.db)
		flushedBytes += after.flushedBytes - before.flushedBytes
		compactedBytes += after.compactedBytes - before.compactedBytes
		sstBytes = d.sstBytes()
		d.close(b)
		require.NoError(b, os.RemoveAll(dir))
		ops++
		b.StartTimer()
	}
	records := float64(ops * n)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/records, "ns/record")
	b.ReportMetric(float64(flushedBytes)/records, "flush-B/record")
	b.ReportMetric(float64(compactedBytes)/records, "compact-B/record")
	b.ReportMetric(float64(recs.rawBytes)/float64(n), "raw-B/record")
	b.ReportMetric(float64(sstBytes)/(1<<20), "sst-MB")
}
