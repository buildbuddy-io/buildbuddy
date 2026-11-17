package replica

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

// writeIndexedRecord writes record i and its atime-index entry. Record keys
// sort by i.
func writeIndexedRecord(t *testing.T, db pebble.IPebbleDB, i int, atimeUsec int64) (fileKey, indexKey []byte) {
	fr := &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: "FOO",
		},
		Digest:         &repb.Digest{Hash: fmt.Sprintf("%064x", i+1), SizeBytes: 1},
		DigestFunction: repb.DigestFunction_SHA256,
	}
	pk, err := filestore.New().PebbleKey(fr)
	require.NoError(t, err)
	fileKey, err = pk.Bytes(filestore.Version5)
	require.NoError(t, err)

	val, err := proto.Marshal(&sgpb.FileMetadata{FileRecord: fr, LastAccessUsec: atimeUsec})
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

func TestDeleteRangeDataAndAtimeIndexEntries(t *testing.T) {
	db, err := pebble.Open(testfs.MakeTempDir(t), "test", &pebblev1.Options{})
	require.NoError(t, err)
	defer db.Close()

	// Force the cleanup to commit in several chunks.
	prev := atimeIndexCleanupCommitSizeBytes
	atimeIndexCleanupCommitSizeBytes = 64
	defer func() { atimeIndexCleanupCommitSizeBytes = prev }()

	type rec struct{ fileKey, indexKey []byte }
	var recs []rec
	for i := range 8 {
		fk, ik := writeIndexedRecord(t, db, i, int64(1_000_000+i))
		recs = append(recs, rec{fk, ik})
	}

	// Delete the span holding records 2 to 5.
	require.NoError(t, DeleteRangeDataAndAtimeIndexEntries(db, recs[2].fileKey, recs[6].fileKey, pebble.NoSync))
	for i, r := range recs {
		inSpan := i >= 2 && i < 6
		require.Equal(t, !inSpan, exists(t, db, r.fileKey), "record %d", i)
		require.Equal(t, !inSpan, exists(t, db, r.indexKey), "index entry %d", i)
	}

	// A span with no file records, like the meta range, deletes nothing else.
	require.NoError(t, DeleteRangeDataAndAtimeIndexEntries(db, []byte{'\x02'}, []byte{'\x04'}, pebble.NoSync))
	for i, r := range recs {
		inSpan := i >= 2 && i < 6
		require.Equal(t, !inSpan, exists(t, db, r.fileKey), "record %d", i)
		require.Equal(t, !inSpan, exists(t, db, r.indexKey), "index entry %d", i)
	}
}
