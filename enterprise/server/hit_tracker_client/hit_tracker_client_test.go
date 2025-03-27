package hit_tracker_client

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	digestA = &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 123,
	}
	digestB = &repb.Digest{
		Hash:      strings.Repeat("b", 64),
		SizeBytes: 234,
	}
	digestC = &repb.Digest{
		Hash:      strings.Repeat("c", 64),
		SizeBytes: 345,
	}
)

type testHitTracker struct {
	t               *testing.T
	requests        atomic.Int64
	emptyHits       atomic.Int64
	downloads       atomic.Int64
	bytesDownloaded atomic.Int64
}

func newTestHitTracker(t *testing.T) *testHitTracker {
	return &testHitTracker{
		t:               t,
		requests:        atomic.Int64{},
		emptyHits:       atomic.Int64{},
		downloads:       atomic.Int64{},
		bytesDownloaded: atomic.Int64{},
	}
}

func (ht *testHitTracker) Track(ctx context.Context, req *hitpb.TrackRequest) (*hitpb.TrackResponse, error) {
	for _, cacheHit := range req.GetHits() {
		if cacheHit.GetInvocationId() == "" {
			log.Warning("Skipping hit-tracking for empty invocation ID")
			continue
		}

		ht.emptyHits.Add(cacheHit.GetEmptyHits())
		ht.downloads.Add(int64(len(cacheHit.GetDownloads())))
		for _, download := range cacheHit.GetDownloads() {
			ht.bytesDownloaded.Add(download.GetSizeBytes())
		}
	}

	// Increment this at the end so that we know when requests are finished.
	ht.requests.Add(1)
	return &hitpb.TrackResponse{}, nil
}

func setup(t *testing.T) (*HitTrackerFactory, *testHitTracker) {
	te := testenv.GetTestEnv(t)
	hitTrackerService := newTestHitTracker(t)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	hitpb.RegisterHitTrackerServiceServer(grpcServer, hitTrackerService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(context.Background(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return newHitTrackerClient(conn), hitTrackerService
}

func TestACHitTracker(t *testing.T) {
	hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewACHitTracker(ctx, "")
	hitTracker.TrackMiss(digestA)
	hitTracker.TrackEmptyHit()
	hitTracker.TrackDownload(digestB).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(digestC).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(0), hitTrackerService.requests.Load())
}

func TestCASHitTracker(t *testing.T) {
	hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, "iid")
	hitTracker.TrackMiss(digestA)
	hitTracker.TrackEmptyHit()
	hitTracker.TrackDownload(digestB).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(digestC).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectToEqual(t, 2, &hitTrackerService.requests, "Expected 2 HitTracker.Track requests")
	require.Equal(t, int64(1), hitTrackerService.emptyHits.Load())
	require.Equal(t, int64(1), hitTrackerService.downloads.Load())
	require.Equal(t, int64(2000), hitTrackerService.bytesDownloaded.Load())
}

func expectToEqual(t *testing.T, expected int64, actual *atomic.Int64, message string) {
	backoff := time.Millisecond
	for i := 0; i < 10; i++ {
		if expected == actual.Load() {
			return
		}
		time.Sleep(backoff)
		backoff = backoff * 2
	}
	require.Fail(t, message)
}
