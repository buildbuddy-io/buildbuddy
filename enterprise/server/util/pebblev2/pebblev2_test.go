package pebblev2_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebblev2"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	pebblev1 "github.com/cockroachdb/pebble"
)

func TestCloseLeasedDB(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	db, err := pebblev2.Open(rootDir, "test", &pebblev2.Options{})
	require.NoError(t, err)
	defer db.Close()

	leaser := pebblev2.NewDBLeaser(db)
	require.NotNil(t, leaser)

	var leaserCloseTime time.Time
	var dbReturnTime time.Time

	leasedDB, err := leaser.DB()
	require.NoError(t, err)
	require.NotNil(t, leasedDB)

	eg := errgroup.Group{}
	eg.Go(func() error {
		leaser.Close()
		leaserCloseTime = time.Now()
		return nil
	})
	time.Sleep(10 * time.Millisecond)
	eg.Go(func() error {
		leasedDB.Close()
		dbReturnTime = time.Now()
		return nil
	})

	require.NoError(t, eg.Wait())

	// DB is now closed; check that leasing fails.
	_, err = leaser.DB()
	require.True(t, status.IsUnavailableError(err), "expected Unavailable, got %v", err)

	// Check that the db lease was returned before the leaser closed
	// (even though we did these in opposite order). This is to check
	// that the serialization is working properly.
	require.Less(t, dbReturnTime, leaserCloseTime)
}

func TestConcurrentCloseLeasedDB(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	db, err := pebblev2.Open(rootDir, "test", &pebblev2.Options{})
	require.NoError(t, err)
	defer db.Close()

	leaser := pebblev2.NewDBLeaser(db)
	handle1, err := leaser.DB()
	require.NoError(t, err)
	handle2, err := leaser.DB()
	require.NoError(t, err)

	// Close the same handle from many goroutines; only one lease should be
	// returned.
	eg := errgroup.Group{}
	for range 100 {
		eg.Go(handle1.Close)
	}
	require.NoError(t, eg.Wait())

	// handle2 is still leased, so leaser.Close must keep waiting.
	leaserClosed := make(chan struct{})
	go func() {
		leaser.Close()
		close(leaserClosed)
	}()
	select {
	case <-leaserClosed:
		require.FailNow(t, "leaser closed while a lease was still held")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, handle2.Close())
	select {
	case <-leaserClosed:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "leaser did not close after all leases were returned")
	}
}

func TestOpenV1DB(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)

	// Create a DB with pebble v1 at its newest format.
	v1DB, err := pebblev1.Open(rootDir, &pebblev1.Options{FormatMajorVersion: pebblev1.FormatNewest})
	require.NoError(t, err)
	require.NoError(t, v1DB.Set([]byte("key"), []byte("value"), pebblev1.Sync))
	require.NoError(t, v1DB.Close())

	// Open it with v2 without ratcheting; the format must stay at v1's
	// newest so a v1 binary can still open it.
	v2DB, err := pebblev2.Open(rootDir, "test", &pebblev2.Options{})
	require.NoError(t, err)
	require.Equal(t, pebblev2.FormatMajorVersion(pebblev1.FormatNewest), v2DB.FormatMajorVersion())
	requireValue(t, v2DB, "key", "value")
	require.NoError(t, v2DB.Set([]byte("key2"), []byte("value2"), pebblev2.Sync))
	require.NoError(t, v2DB.Close())

	// Roll back to v1 and read data written by both versions.
	v1DB, err = pebblev1.Open(rootDir, &pebblev1.Options{})
	require.NoError(t, err)
	defer v1DB.Close()
	for key, want := range map[string]string{"key": "value", "key2": "value2"} {
		val, closer, err := v1DB.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, want, string(val))
		require.NoError(t, closer.Close())
	}
}

func TestOpenV1DBBelowMinSupportedFormat(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)

	// An unset format in v1 defaults to FormatMostCompatible, which is below
	// the oldest format v2 can open.
	v1DB, err := pebblev1.Open(rootDir, &pebblev1.Options{})
	require.NoError(t, err)
	require.Equal(t, pebblev1.FormatMostCompatible, v1DB.FormatMajorVersion())
	require.NoError(t, v1DB.Close())

	_, err = pebblev2.Open(rootDir, "test", &pebblev2.Options{})
	require.ErrorContains(t, err, "no longer supported")
}

func requireValue(t *testing.T, db pebblev2.IPebbleDB, key, want string) {
	t.Helper()
	val, closer, err := db.Get([]byte(key))
	require.NoError(t, err)
	defer closer.Close()
	require.Equal(t, want, string(val))
}
