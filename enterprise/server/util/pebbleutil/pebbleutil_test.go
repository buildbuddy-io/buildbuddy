package pebbleutil_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebbleutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCloseLeasedDB(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(rootDir, &pebble.Options{})
	require.NoError(t, err)

	leaser := pebbleutil.NewDBLeaser(db)
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
	require.Error(t, err)

	// Check that the db lease was returned before the leaser closed
	// (even though we did these in opposite order). This is to check
	// that the serialization is working properly.
	require.Less(t, dbReturnTime, leaserCloseTime)
}
