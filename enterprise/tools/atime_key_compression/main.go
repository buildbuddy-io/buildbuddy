// Tool to measure how v7-style raft-cache keys behave on disk under the four
// combinations of {hex, raw} digest encoding x {snappy, zstd} block
// compression, using realistic FileMetadata values.
//
// It builds four pebble DBs, one per combination, and writes the SAME dataset
// into each (only the key digest encoding differs; values always keep the
// REAPI hex digest), then prints logical sizes, WAL bytes, live sstable
// sizes, and per-block breakdowns from sstable properties.
//
// Rows have a uniformly random payload size in [0, 1500]:
//   - payload <= 1024: stored inline in FileMetadata (InlineMetadata.Data)
//   - payload >  1024: stored as a GCS pointer (GcsMetadata.BlobName in the
//     production blobKey format, which embeds the hex digest); no GCS I/O.
//
// Run (from the repo root):
//
//	bazel run --config=remote //enterprise/tools/atime_key_compression -- -n=10000
//
// zstd goes through pebble's cgo path (DataDog/zstd), which needs the go.mod
// replace pinning DataDog to v1.4.5 -- pebble v1.1.4's expected version; the
// repo's default v1.5.5 crashes pebble on zstd table reads (see BUILD).
//
// Note: 10k rows is only ~5MB per DB (a handful of data blocks after
// compaction); increase -n for stabler numbers.
//
// This tool is intentionally uncommitted; delete the directory when done.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/cockroachdb/pebble"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	n             = flag.Int("n", 10_000, "Number of rows to write per DB.")
	seed          = flag.Int64("seed", 42, "RNG seed (dataset is identical across variants).")
	rootDir       = flag.String("dir", "", "Directory to create the DBs in (kept for inspection). Default: a temp dir, removed on exit.")
	cacheMB       = flag.Int("cache_mb", 1024, "Block cache size per DB, in MiB. Large enough that nothing is evicted, so end-of-run residency reflects the full working set.")
	digestInValue = flag.Bool("digest_in_value", true, "Store the hex digest inside the value (Digest.hash + full GCS blob name), as production does today. false models a value format that derives both from the key: no digest hash, and only the 5-char salt of the blob name.")
)

const (
	numGroups    = 16
	inlineCutoff = 1024
	maxPayload   = 1500
	// Inline payloads larger than this are stored zstd-compressed (via the
	// production compression util), matching cache behavior. Random payloads
	// model already-high-entropy content, so compression here mostly adds the
	// zstd framing; the point is to store the same bytes production would.
	compressCutoff = 100
	partitionID    = "default"
	appName        = "buildbuddy-app-0"
	saltAlphabet   = "abcdefghijklmnopqrstuvwxyz0123456789"
)

var atimeWindowUs = int64(72 * time.Hour / time.Microsecond)

type row struct {
	digest     []byte // 32 raw bytes
	group      int
	gcs        bool
	compressed bool
	value      []byte // marshaled FileMetadata; identical across variants
}

func makeRows(rng *rand.Rand) []row {
	now := time.Now().UnixMicro()
	rows := make([]row, 0, *n)
	for i := 0; i < *n; i++ {
		digest := make([]byte, 32)
		rng.Read(digest)
		hexDigest := hex.EncodeToString(digest)
		group := i % numGroups
		payloadLen := rng.Intn(maxPayload + 1)

		valueDigest := &repb.Digest{Hash: hexDigest, SizeBytes: int64(payloadLen)}
		if !*digestInValue {
			// Digest hash is derivable from the key; keep only the size.
			valueDigest = &repb.Digest{SizeBytes: int64(payloadLen)}
		}
		md := &sgpb.FileMetadata{
			FileRecord: &sgpb.FileRecord{
				Isolation: &sgpb.Isolation{
					CacheType:   rspb.CacheType_CAS,
					PartitionId: partitionID,
					// The record keeps the unpadded group ID; only the pebble
					// key uses the fixed-width form (matching production).
					GroupId: fmt.Sprintf("GR%d", group),
				},
				Digest:         valueDigest,
				DigestFunction: repb.DigestFunction_SHA256,
			},
			StoredSizeBytes: int64(payloadLen),
			LastAccessUsec:  now - rng.Int63n(atimeWindowUs),
			LastModifyUsec:  now - rng.Int63n(atimeWindowUs),
		}
		isGCS := payloadLen > inlineCutoff
		isCompressed := false
		if isGCS {
			salt := make([]byte, 5)
			for j := range salt {
				salt[j] = saltAlphabet[rng.Intn(len(saltAlphabet))]
			}
			// Production blobKey format:
			// {appName}/{partitionID}/{groupID}/{ac|cas}/{hashPrefix:4}/{hash}-{salt}
			blobName := fmt.Sprintf("%s/PT%s/GR%d/cas/%s/%s-%s",
				appName, partitionID, group, hexDigest[:4], hexDigest, string(salt))
			// if !*digestInValue {
			// Everything but the salt is derivable from the key.
			// blobName = string(salt)
			// }
			md.StorageMetadata = &sgpb.StorageMetadata{
				GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
					BlobName:           blobName,
					LastCustomTimeUsec: now - rng.Int63n(atimeWindowUs),
				},
			}
		} else {
			payload := make([]byte, payloadLen)
			rng.Read(payload)
			if payloadLen > compressCutoff {
				payload = compression.CompressZstd(nil, payload)
				md.FileRecord.Compressor = repb.Compressor_ZSTD
				md.StoredSizeBytes = int64(len(payload))
				isCompressed = true
			}
			md.StorageMetadata = &sgpb.StorageMetadata{
				InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
					Data:          payload,
					CreatedAtNsec: (now - rng.Int63n(atimeWindowUs)) * 1000,
				},
			}
		}
		val, err := proto.Marshal(md)
		if err != nil {
			panic(err)
		}
		rows = append(rows, row{digest: digest, group: group, gcs: isGCS, compressed: isCompressed, value: val})
	}
	return rows
}

// v7Key builds a v7-like key: PT<part>/GR<20-digit group>/<digest>/1/cas/v7,
// with the digest hex-encoded (64 bytes, today's format) or raw (32 bytes,
// the proposal).
func v7Key(r row, rawDigest bool) []byte {
	digest := r.digest
	if !rawDigest {
		digest = []byte(hex.EncodeToString(r.digest))
	}
	k := make([]byte, 0, 128)
	k = append(k, "PT"...)
	k = append(k, partitionID...)
	k = append(k, '/')
	k = append(k, fmt.Sprintf("GR%020d", r.group)...)
	k = append(k, '/')
	k = append(k, digest...)
	k = append(k, "/1/cas/v7"...)
	return k
}

type result struct {
	name                string
	logicalKey          int64
	logicalVal          int64
	walBytes            uint64
	liveSSTBytes        int64
	dataBlockBytes      uint64
	indexBlockBytes     uint64
	rawKeySize          uint64 // includes pebble's 8B internal-key trailer
	rawValueSize        uint64
	numDataBlocks       uint64
	compactWriteBytes   int64
	writeFlushCompactMs int64

	// Read phase: every row point-Get once, in shuffled order, against a cold
	// (fresh) block cache large enough to hold everything.
	readMs          int64
	cacheSizeBytes  int64
	cacheBlockCount int64
	cacheHits       int64
	cacheMisses     int64
}

func runVariant(name, dir string, rows []row, rawDigest bool, upper, bottom pebble.Compression) result {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	// upper applies to L0..L5, bottom to L6. The standard "zstd on the last
	// level only" production pattern is upper=snappy, bottom=zstd: flushes and
	// hot compactions stay cheap, and bytes pay the stronger compression once,
	// at their final resting place. Note that after this tool's full
	// compaction everything lives in L6, so the *measured sstables* of a
	// snappy+zstdL6 variant match all-zstd; the difference is in the write
	// path (flush/compaction bytes and timing).
	for i := range opts.Levels {
		opts.Levels[i].Compression = upper
	}
	opts.Levels[len(opts.Levels)-1].Compression = bottom
	// A dedicated cache per variant so residency measurements don't mix.
	c := pebble.NewCache(int64(*cacheMB) << 20)
	defer c.Unref()
	opts.Cache = c
	db, err := pebble.Open(dir, opts)
	if err != nil {
		panic(err)
	}

	res := result{name: name}
	start := time.Now()
	b := db.NewBatch()
	for _, r := range rows {
		k := v7Key(r, rawDigest)
		res.logicalKey += int64(len(k))
		res.logicalVal += int64(len(r.value))
		if err := b.Set(k, r.value, nil); err != nil {
			panic(err)
		}
		if b.Len() > 4<<20 {
			if err := b.Commit(pebble.Sync); err != nil {
				panic(err)
			}
			b = db.NewBatch()
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		panic(err)
	}
	if err := db.Flush(); err != nil {
		panic(err)
	}
	if err := db.Compact([]byte{0}, []byte{0xff}, false); err != nil {
		panic(err)
	}
	res.writeFlushCompactMs = time.Since(start).Milliseconds()

	// Read phase: point-Get every row in shuffled order. Compaction reads
	// bypass the block cache, so what's resident afterwards is (approximately)
	// what these reads pulled in: data blocks + index blocks, stored
	// DECOMPRESSED -- which is why cache residency should track the hex/raw
	// axis but not the snappy/zstd axis.
	perm := rand.New(rand.NewSource(*seed + 1)).Perm(len(rows))
	readStart := time.Now()
	for _, idx := range perm {
		r := rows[idx]
		v, closer, err := db.Get(v7Key(r, rawDigest))
		if err != nil {
			panic(fmt.Sprintf("%s: get: %v", name, err))
		}
		if len(v) != len(r.value) {
			panic(fmt.Sprintf("%s: value length mismatch", name))
		}
		closer.Close()
	}
	res.readMs = time.Since(readStart).Milliseconds()

	m := db.Metrics()
	res.walBytes = m.WAL.BytesWritten
	for _, lm := range m.Levels {
		res.compactWriteBytes += int64(lm.BytesCompacted)
	}
	res.cacheSizeBytes = m.BlockCache.Size
	res.cacheBlockCount = m.BlockCache.Count
	res.cacheHits = m.BlockCache.Hits
	res.cacheMisses = m.BlockCache.Misses

	tables, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		panic(err)
	}
	for _, level := range tables {
		for _, t := range level {
			res.liveSSTBytes += int64(t.Size)
			p := t.Properties
			res.dataBlockBytes += p.DataSize
			res.indexBlockBytes += p.IndexSize
			res.rawKeySize += p.RawKeySize
			res.rawValueSize += p.RawValueSize
			res.numDataBlocks += p.NumDataBlocks
		}
	}
	if err := db.Close(); err != nil {
		panic(err)
	}
	return res
}

func main() {
	flag.Parse()

	dir := *rootDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "atime-key-compression-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
	}

	rows := makeRows(rand.New(rand.NewSource(*seed)))
	inline, inlineCompressed, gcs := 0, 0, 0
	valBytes := int64(0)
	for _, r := range rows {
		valBytes += int64(len(r.value))
		switch {
		case r.gcs:
			gcs++
		case r.compressed:
			inline++
			inlineCompressed++
		default:
			inline++
		}
	}
	fmt.Printf("rows: %d  (inline<=%dB: %d, of which zstd-compressed(>%dB): %d; gcs-pointer: %d)   avg value: %.1f B   digest_in_value=%v\n",
		len(rows), inlineCutoff, inline, compressCutoff, inlineCompressed, gcs, float64(valBytes)/float64(len(rows)), *digestInValue)
	fmt.Printf("dbs under: %s\n\n", dir)

	variants := []struct {
		name          string
		rawDigest     bool
		upper, bottom pebble.Compression
	}{
		{"hex-snappy", false, pebble.SnappyCompression, pebble.SnappyCompression},
		{"raw-snappy", true, pebble.SnappyCompression, pebble.SnappyCompression},
		{"hex-zstd", false, pebble.ZstdCompression, pebble.ZstdCompression},
		{"raw-zstd", true, pebble.ZstdCompression, pebble.ZstdCompression},
		{"hex-zstdL6", false, pebble.SnappyCompression, pebble.ZstdCompression},
		{"raw-zstdL6", true, pebble.SnappyCompression, pebble.ZstdCompression},
	}

	results := make([]result, 0, len(variants))
	for _, v := range variants {
		results = append(results, runVariant(v.name, filepath.Join(dir, v.name), rows, v.rawDigest, v.upper, v.bottom))
	}

	nf := float64(len(rows))
	fmt.Printf("%-11s %9s %9s %9s %9s %10s %9s %8s %8s %8s\n",
		"variant", "key B/e", "val B/e", "wal B/e", "sst B/e", "data B/e", "idx B/e", "blocks", "cmp MB", "time ms")
	for _, r := range results {
		fmt.Printf("%-11s %9.1f %9.1f %9.1f %9.1f %10.1f %9.1f %8d %8.1f %8d\n",
			r.name,
			float64(r.logicalKey)/nf,
			float64(r.logicalVal)/nf,
			float64(r.walBytes)/nf,
			float64(r.liveSSTBytes)/nf,
			float64(r.dataBlockBytes)/nf,
			float64(r.indexBlockBytes)/nf,
			r.numDataBlocks,
			float64(r.compactWriteBytes)/(1<<20),
			r.writeFlushCompactMs,
		)
	}

	fmt.Println()
	fmt.Printf("%-11s %9s %11s %12s %14s %8s\n",
		"variant", "read ms", "cache MB", "cache B/e", "cached blocks", "hit%")
	for _, r := range results {
		hitPct := 0.0
		if r.cacheHits+r.cacheMisses > 0 {
			hitPct = 100 * float64(r.cacheHits) / float64(r.cacheHits+r.cacheMisses)
		}
		fmt.Printf("%-11s %9d %11.1f %12.1f %14d %8.1f\n",
			r.name,
			r.readMs,
			float64(r.cacheSizeBytes)/(1<<20),
			float64(r.cacheSizeBytes)/nf,
			r.cacheBlockCount,
			hitPct,
		)
	}

	fmt.Println()
	base := results[0]
	for _, r := range results[1:] {
		sstDelta := float64(r.liveSSTBytes-base.liveSSTBytes) / nf
		walDelta := (float64(r.walBytes) - float64(base.walBytes)) / nf
		cacheDelta := float64(r.cacheSizeBytes-base.cacheSizeBytes) / nf
		fmt.Printf("%-11s vs %s:  sst %+7.1f B/e (%+5.1f%%)   wal %+7.1f B/e   cache %+7.1f B/e\n",
			r.name, base.name,
			sstDelta, 100*float64(r.liveSSTBytes-base.liveSSTBytes)/float64(base.liveSSTBytes),
			walDelta, cacheDelta)
	}
}
