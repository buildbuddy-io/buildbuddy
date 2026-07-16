// Internal test, unlike metadata_test.go: calls maybeUpdateGCSAtime directly
// rather than standing up a cluster through New().

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockgcs"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const testBlobName = "test-blob"

// gcsAtimeFixture wires a Server up to a mock GCS holding a single blob.
type gcsAtimeFixture struct {
	server *Server
	clock  *clockwork.FakeClock
	// callCount reports how many UpdateCustomTime calls GCS has seen.
	callCount func() int
}

func newGCSAtimeFixture(t *testing.T, ttlDays int64, threshold time.Duration) *gcsAtimeFixture {
	t.Helper()
	ctx := context.Background()
	clock := clockwork.NewFakeClockAt(time.Now())

	gcs := mockgcs.New(clock)
	require.NoError(t, gcs.SetBucketCustomTimeTTL(ctx, ttlDays))

	wc, err := gcs.ConditionalWriter(ctx, testBlobName, true /*=overwriteExisting*/, clock.Now(), 4)
	require.NoError(t, err)
	_, err = wc.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, wc.Commit())
	require.NoError(t, wc.Close())

	return &gcsAtimeFixture{
		server: &Server{
			clock:                   clock,
			fileStorer:              filestore.New(filestore.WithGCSBlobstore(gcs, "app-name"), filestore.WithClock(clock)),
			gcsTTLDays:              ttlDays,
			gcsAtimeUpdateThreshold: threshold,
		},
		clock:     clock,
		callCount: gcs.UpdateCustomTimeCallCount,
	}
}

// metadataWithCustomTime returns metadata for the fixture's blob, recording a
// custom time offsetFromNow from the current fake clock time.
func (f *gcsAtimeFixture) metadataWithCustomTime(offsetFromNow time.Duration) *sgpb.StorageMetadata_GCSMetadata {
	return &sgpb.StorageMetadata_GCSMetadata{
		BlobName:           testBlobName,
		LastCustomTimeUsec: f.clock.Now().Add(offsetFromNow).UnixMicro(),
	}
}

func TestMaybeUpdateGCSAtime_NilMetadataIsNoop(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 0 /*=threshold*/)

	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), nil)
	require.NoError(t, err)
	require.Zero(t, customTime, "inline records have no GCS object to refresh")
	require.Equal(t, 0, f.callCount())
}

func TestMaybeUpdateGCSAtime_ZeroThresholdRefreshesEveryTime(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 0 /*=threshold*/)
	md := f.metadataWithCustomTime(0)

	// The default threshold of 0 must preserve refresh-on-every-update.
	for i := 1; i <= 3; i++ {
		f.clock.Advance(time.Minute)
		customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
		require.NoError(t, err)
		require.Equal(t, f.clock.Now().UnixMicro(), customTime)
		require.Equal(t, i, f.callCount())
	}
}

func TestMaybeUpdateGCSAtime_WithinThresholdSkipsRefresh(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 24*time.Hour /*=threshold*/)
	md := f.metadataWithCustomTime(-1 * time.Hour)

	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
	require.NoError(t, err)
	require.Zero(t, customTime, "a refresh 1h ago is inside the 24h threshold")
	require.Equal(t, 0, f.callCount(), "no GCS call should be made")
}

func TestMaybeUpdateGCSAtime_PastThresholdRefreshes(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 24*time.Hour /*=threshold*/)
	md := f.metadataWithCustomTime(-25 * time.Hour)

	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
	require.NoError(t, err)
	require.Equal(t, f.clock.Now().UnixMicro(), customTime)
	require.Equal(t, 1, f.callCount())
}

func TestMaybeUpdateGCSAtime_PastTTLSkipsRefresh(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 0 /*=threshold*/)
	md := f.metadataWithCustomTime(-8 * 24 * time.Hour)

	// A recorded custom time past the TTL means GCS lifecycle has likely
	// deleted the object already, so don't refresh it.
	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
	require.NoError(t, err)
	require.Zero(t, customTime)
	require.Equal(t, 0, f.callCount())
}

func TestMaybeUpdateGCSAtime_TTLDisabledSkipsRefresh(t *testing.T) {
	f := newGCSAtimeFixture(t, 0 /*=ttlDays*/, 0 /*=threshold*/)
	md := f.metadataWithCustomTime(0)

	// With no bucket TTL there is no lifecycle deletion to defend against.
	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
	require.NoError(t, err)
	require.Zero(t, customTime)
	require.Equal(t, 0, f.callCount())
}

func TestMaybeUpdateGCSAtime_FutureCustomTimeSkipsRefresh(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 24*time.Hour /*=threshold*/)
	// A future-dated value means another node just refreshed the object (the
	// recorded value is only ever one we really wrote), so treat it as recent.
	md := f.metadataWithCustomTime(5 * time.Minute)

	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
	require.NoError(t, err)
	require.Zero(t, customTime)
	require.Equal(t, 0, f.callCount())
}

func TestMaybeUpdateGCSAtime_FailedRefreshRecordsNoCustomTime(t *testing.T) {
	f := newGCSAtimeFixture(t, 7 /*=ttlDays*/, 0 /*=threshold*/)
	md := f.metadataWithCustomTime(0)
	md.BlobName = "does-not-exist"

	// Never report a custom time we didn't persist: recording one would
	// throttle refreshes of an object whose real custom time never moved.
	customTime, err := f.server.maybeUpdateGCSAtime(context.Background(), md)
	require.Error(t, err)
	require.Zero(t, customTime)
}
