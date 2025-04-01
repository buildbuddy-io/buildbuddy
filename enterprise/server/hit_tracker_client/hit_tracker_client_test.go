package hit_tracker_client

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	user1  = "USER1"
	group1 = "GROUP1"
)

var (
	aDigest = digestProto(strings.Repeat("a", 64), 3)
	bDigest = digestProto(strings.Repeat("b", 64), 3)
	cDigest = digestProto(strings.Repeat("c", 64), 3)
	dDigest = digestProto(strings.Repeat("d", 64), 3)
	eDigest = digestProto(strings.Repeat("e", 64), 3)
	fDigest = digestProto(strings.Repeat("f", 64), 3)
	gDigest = digestProto(strings.Repeat("g", 64), 3)
	hDigest = digestProto(strings.Repeat("h", 64), 3)
	iDigest = digestProto(strings.Repeat("i", 64), 3)
	jDigest = digestProto(strings.Repeat("j", 64), 3)
	kDigest = digestProto(strings.Repeat("k", 64), 3)
)

func digestProto(hash string, sizeBytes int64) *repb.Digest {
	return &repb.Digest{Hash: hash, SizeBytes: sizeBytes}
}

type testHitTracker struct {
	t               *testing.T
	authenticator   interfaces.Authenticator
	downloads       map[string]*atomic.Int64
	bytesDownloaded map[string]*atomic.Int64
}

func newTestHitTracker(t *testing.T, authenticator interfaces.Authenticator) *testHitTracker {
	return &testHitTracker{
		t:             t,
		authenticator: authenticator,
		downloads: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		bytesDownloaded: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
	}
}

func (ht *testHitTracker) Track(ctx context.Context, req *hitpb.TrackRequest) (*hitpb.TrackResponse, error) {
	groupID := interfaces.AuthAnonymousUser
	user, err := ht.authenticator.AuthenticatedUser(ctx)
	if err == nil {
		groupID = user.GetGroupID()
	}

	for _, hit := range req.GetHits() {
		if _, ok := ht.bytesDownloaded[groupID]; !ok {
			ht.bytesDownloaded[groupID] = &atomic.Int64{}
		}
		ht.bytesDownloaded[groupID].Add(hit.GetSizeBytes())
	}
	if _, ok := ht.downloads[groupID]; !ok {
		ht.downloads[groupID] = &atomic.Int64{}
	}
	ht.downloads[groupID].Add(int64(len(req.GetHits())))
	return &hitpb.TrackResponse{}, nil
}

func setup(t *testing.T) (interfaces.Authenticator, *HitTrackerFactory, *testHitTracker, clockwork.FakeClock) {
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1))
	te.SetAuthenticator(authenticator)
	hitTrackerService := newTestHitTracker(t, authenticator)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	hitpb.RegisterHitTrackerServiceServer(grpcServer, hitTrackerService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(context.Background(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	flags.Set(t, "cache.remote_hit_tracker.update_interval", 995*time.Millisecond)
	flags.Set(t, "cache.remote_hit_tracker.max_pending_hits_per_group", 10)
	fakeClock := clockwork.NewFakeClock()
	te.SetClock(fakeClock)
	return authenticator, newHitTrackerClient(te, conn), hitTrackerService, fakeClock
}

func TestACHitTracker(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService, fakeClock := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewACHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	for i := 0; i < 10; i++ {
		fakeClock.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(0), hitTrackerService.downloads[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService, fakeClock := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	for i := 0; i < 10; i++ {
		fakeClock.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}

	expectToEqual(t, 1, hitTrackerService.downloads[interfaces.AuthAnonymousUser], "Expected 1 HitTracker.Track download")
	require.Equal(t, int64(2000), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker_NoDeduplication(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService, fakeClock := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")

	for i := 0; i < 10; i++ {
		fakeClock.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}

	expectToEqual(t, int64(2), hitTrackerService.downloads[interfaces.AuthAnonymousUser], "Expected 2 cache hits")
	require.Equal(t, int64(4000), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker_DropsUpdates(t *testing.T) {
	authenticator, hitTrackerFactory, hitTrackerService, fakeClock := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	for i := 0; i < 100; i++ {
		hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(10, 20, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(cDigest).CloseWithBytesTransferred(100, 200, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(dDigest).CloseWithBytesTransferred(1_000, 2_000, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(eDigest).CloseWithBytesTransferred(10_000, 20_000, repb.Compressor_IDENTITY, "test")
	}

	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	hitTracker = hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")

	for i := 0; i < 10; i++ {
		fakeClock.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}

	// These updates should come in in-order, so they should be:
	// ANON: a, b, c, d, e, a, b, c, d, e
	expectToEqual(t, int64(10), hitTrackerService.downloads[interfaces.AuthAnonymousUser], "Expected 10 cache hits for ANON")
	require.Equal(t, int64(44_444), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())

	// Even though group 1's update came at the end, it should still be sent.
	expectToEqual(t, int64(1), hitTrackerService.downloads[group1], "Expected 1 cache hits for group 1")
	require.Equal(t, int64(2_000_000), hitTrackerService.bytesDownloaded[group1].Load())
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
