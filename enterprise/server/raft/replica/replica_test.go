package replica_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func getTmpDir(t *testing.T) string {
	dir, err := ioutil.TempDir("/tmp", "buildbuddy_diskcache_*")
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.EnsureDirectoryExists(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	})
	return dir
}

func TestSnapshotAndRestore(t *testing.T) {
	baseDir := getTmpDir(t)
	db, err := pebble.Open(baseDir, &pebble.Options{})
	require.Nil(t, err)

	snapFile, err := os.CreateTemp(os.Getenv("TEST_TMPDIR"), "buildbuddy-test-*")
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

	newDir := getTmpDir(t)
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
