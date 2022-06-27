package filestore_test

import (
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestPebbleWriteCloser(t *testing.T) {
	db, err := pebble.Open(testfs.MakeTempDir(t), &pebble.Options{})
	if err != nil {
		t.Fatalf("Error opening pebble db: %s", err)
	}
	defer db.Close()
	wb := db.NewIndexedBatch()

	wc, err := filestore.PebbleWriter(wb, &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: "major",
		},
		Digest: &repb.Digest{
			Hash:      "key/alert",
			SizeBytes: 3,
		},
	})
	require.Nil(t, err, err)
	buf := make([]byte, 10)

	// Write a buf smaller than the flush size of 3M.
	for i := 0; i < len(buf); i++ {
		buf[i] = byte('a')
	}
	n, err := wc.Write(buf)
	require.Nil(t, err)
	require.Equal(t, len(buf), n)

	// Now write a buf larger than the flush size of 3M.
	buf = make([]byte, 4000000)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte('b')
	}
	n, err = wc.Write(buf)
	require.Nil(t, err)
	require.Equal(t, 4000000, n)

	// Close the write closer, commit the batch, and close it.
	require.Nil(t, wc.Close())
	require.Nil(t, wb.Commit(&pebble.WriteOptions{Sync: true}))

	first, closer, err := db.Get([]byte("major/cas/key/alert-1"))
	require.Nil(t, err)
	require.Equal(t, 3000000, len(first))
	require.Equal(t, byte('a'), first[0])
	require.Equal(t, byte('b'), first[len(first)-1])
	require.Nil(t, closer.Close())

	second, closer, err := db.Get([]byte("major/cas/key/alert-2"))
	require.Nil(t, err)
	require.Equal(t, 1000010, len(second))
	require.Equal(t, byte('b'), second[0])
	require.Nil(t, closer.Close())
}

func TestPebbleReadCloser(t *testing.T) {
	db, err := pebble.Open(testfs.MakeTempDir(t), &pebble.Options{})
	if err != nil {
		t.Fatalf("Error opening pebble db: %s", err)
	}
	defer db.Close()
	wb := db.NewIndexedBatch()
	buf := make([]byte, 3000000)
	for i := 0; i < 3000000; i++ {
		c := byte('b')
		if i < 10 {
			c = byte('a')
		}
		buf[i] = c
	}
	require.Nil(t, wb.Set([]byte("major/key/alert-1"), buf, nil /*ignored write options*/))

	buf = make([]byte, 1000010)
	for i := 0; i < 1000010; i++ {
		buf[i] = byte('b')
	}
	require.Nil(t, wb.Set([]byte("major/key/alert-2"), buf, nil /*ignored write options*/))

	// Commit the batch, and close it.
	require.Nil(t, wb.Commit(&pebble.WriteOptions{Sync: true}))
	require.Nil(t, wb.Close())

	iter := db.NewIter(nil)
	defer iter.Close()
	rc := filestore.PebbleReader(iter, &rfpb.StorageMetadata_PebbleMetadata{
		Key:    []byte("major/key/alert-"),
		Chunks: 2,
	})

	rbuf := make([]byte, 1000010)
	n, err := rc.Read(rbuf)
	require.Nil(t, err)
	require.Equal(t, 1000010, n)
	require.Equal(t, byte('a'), rbuf[0])
	require.Equal(t, byte('b'), rbuf[len(rbuf)-1])

	rbuf = make([]byte, 3000000)
	n, err = rc.Read(rbuf)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 3000000, n)
}

func TestMissingChunks(t *testing.T) {
	db, err := pebble.Open(testfs.MakeTempDir(t), &pebble.Options{})
	if err != nil {
		t.Fatalf("Error opening pebble db: %s", err)
	}
	defer db.Close()
	wb := db.NewIndexedBatch()
	buf := make([]byte, 3000000)
	for i := 0; i < 3000000; i++ {
		buf[i] = byte('a')
	}
	require.Nil(t, wb.Set([]byte("major/key/alert-1"), buf, nil /*ignored write options*/))
	// Skip chunk 2 (UH OH :{)
	require.Nil(t, wb.Set([]byte("major/key/alert-3"), buf, nil /*ignored write options*/))

	// Commit the batch, and close it.
	require.Nil(t, wb.Commit(&pebble.WriteOptions{Sync: true}))
	require.Nil(t, wb.Close())

	md := &rfpb.StorageMetadata_PebbleMetadata{
		Key:    []byte("major/key/alert-"),
		Chunks: 3,
	}

	// Ensure that HasChunks returns false.
	iter := db.NewIter(nil)
	defer iter.Close()
	require.False(t, filestore.PebbleHasChunks(iter, md))

	// Ensure that reading fails.
	rc := filestore.PebbleReader(iter, md)
	rbuf := make([]byte, 10000000)
	_, err = rc.Read(rbuf)
	require.True(t, status.IsOutOfRangeError(err))
}

func TestPebbleReadWrite(t *testing.T) {
	db, err := pebble.Open(testfs.MakeTempDir(t), &pebble.Options{})
	if err != nil {
		t.Fatalf("Error opening pebble db: %s", err)
	}
	defer db.Close()

	wb := db.NewIndexedBatch()
	defer wb.Close()

	fr := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: "major",
		},
		Digest: &repb.Digest{
			Hash:      "key/alert",
			SizeBytes: 100,
		},
	}

	wc, err := filestore.PebbleWriter(wb, fr)
	require.Nil(t, err, err)
	buf := make([]byte, 1000)

	for i := 0; i < len(buf); i++ {
		buf[i] = byte('a')
	}
	n, err := wc.Write(buf)
	require.Nil(t, err)
	require.Equal(t, len(buf), n)
	require.Nil(t, wc.Close())

	iter := wb.NewIter(nil)
	defer iter.Close()

	rc := filestore.PebbleReader(iter, wc.Metadata().GetPebbleMetadata())
	rbuf := make([]byte, len(buf))
	n, err = rc.Read(rbuf)

	require.Nil(t, err, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, len(buf), len(rbuf))
}
