package store

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	pebblev1 "github.com/cockroachdb/pebble"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

// writeIndexedRecord writes a file record with deterministic digest i plus its
// atime-index entry, mirroring what the replica apply path maintains.
func writeIndexedRecord(t *testing.T, db pebble.IPebbleDB, i int, atimeUsec int64) (fileKey, indexKey []byte) {
	fs := filestore.New()
	fr := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "FOO",
		},
		Digest:         &repb.Digest{Hash: fmt.Sprintf("%064x", i+1), SizeBytes: 1},
		DigestFunction: repb.DigestFunction_SHA256,
	}
	pk, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	fileKey, err = pk.Bytes(filestore.Version5)
	require.NoError(t, err)

	md := &sgpb.FileMetadata{FileRecord: fr, LastAccessUsec: atimeUsec}
	val, err := proto.Marshal(md)
	require.NoError(t, err)
	require.NoError(t, db.Set(fileKey, val, pebble.NoSync))

	indexKey = keys.AtimeIndexKey("FOO", atimeUsec, fileKey)
	require.NoError(t, db.Set(indexKey, nil, pebble.NoSync))
	return fileKey, indexKey
}

func exists(t *testing.T, db pebble.IPebbleDB, key []byte) bool {
	_, closer, err := db.Get(key)
	if err == pebblev1.ErrNotFound {
		return false
	}
	require.NoError(t, err)
	closer.Close()
	return true
}

// RemoveData's cleanup must delete exactly the records and index entries in
// the removed span, and tolerate non-record keys (e.g. the meta range).
func TestDeleteRangeDataAndAtimeIndexEntries(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	db, err := pebble.Open(dir, "test", &pebblev1.Options{})
	require.NoError(t, err)
	defer db.Close()

	// Force the cleanup to commit in multiple chunks.
	prev := atimeIndexCleanupCommitSizeBytes
	atimeIndexCleanupCommitSizeBytes = 64
	defer func() { atimeIndexCleanupCommitSizeBytes = prev }()

	// Records 1..8; digests are 0-padded so they sort by i.
	type rec struct{ fileKey, indexKey []byte }
	var recs []rec
	for i := 0; i < 8; i++ {
		fk, ik := writeIndexedRecord(t, db, i, int64(1_000_000+i))
		recs = append(recs, rec{fk, ik})
	}

	// Remove the span covering records 2..5 (bounds via their file keys).
	require.NoError(t, deleteRangeDataAndAtimeIndexEntries(db, recs[2].fileKey, recs[6].fileKey))

	for i, r := range recs {
		inSpan := i >= 2 && i < 6
		require.Equal(t, !inSpan, exists(t, db, r.indexKey), "index entry %d", i)
		require.Equal(t, !inSpan, exists(t, db, r.fileKey), "record %d", i)
	}

	// A span with no file records (e.g. the meta range) works and touches
	// nothing outside it.
	require.NoError(t, deleteRangeDataAndAtimeIndexEntries(db, []byte{'\x02'}, []byte{'\x04'}))
	for i, r := range recs {
		inSpan := i >= 2 && i < 6
		require.Equal(t, !inSpan, exists(t, db, r.fileKey), "record %d after meta-span delete", i)
	}
}
