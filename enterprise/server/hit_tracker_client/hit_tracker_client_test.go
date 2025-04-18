package hit_tracker_client

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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
	t               testing.TB
	authenticator   interfaces.Authenticator
	wg              sync.WaitGroup
	downloads       map[string]*atomic.Int64
	bytesDownloaded map[string]*atomic.Int64
}

func newTestHitTracker(t testing.TB, authenticator interfaces.Authenticator) *testHitTracker {
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
	ht.wg.Wait()

	groupID := interfaces.AuthAnonymousUser
	user, err := ht.authenticator.AuthenticatedUser(ctx)
	if err == nil {
		groupID = user.GetGroupID()
	}

	for _, hit := range req.GetHits() {
		ht.bytesDownloaded[groupID].Add(hit.GetSizeBytes())
	}
	ht.downloads[groupID].Add(int64(len(req.GetHits())))
	return &hitpb.TrackResponse{}, nil
}

func setup(t testing.TB) (interfaces.Authenticator, *HitTrackerFactory, *testHitTracker) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1))
	te.SetAuthenticator(authenticator)
	hitTrackerService := newTestHitTracker(t, authenticator)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	hitpb.RegisterHitTrackerServiceServer(grpcServer, hitTrackerService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return authenticator, newHitTrackerClient(ctx, te, conn), hitTrackerService
}

func TestACHitTracker(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewACHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(0), hitTrackerService.downloads[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := func() bool { return hitTrackerService.downloads[interfaces.AuthAnonymousUser].Load() == 1 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker_NoDeduplication(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")

	expectation := func() bool { return hitTrackerService.downloads[interfaces.AuthAnonymousUser].Load() == 2 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(4000), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker_SplitsUpdates(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_hit_tracker.max_hits_per_update", 10)
	authenticator, hitTrackerFactory, hitTrackerService := setup(t)

	// Pause the hit-tracker RPC service and send an RPC that'll block the
	// hit-tracker-client worker so updates are queued.
	hitTrackerService.wg.Add(1)
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	hitTracker := hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")

	anonCtx := context.Background()
	hitTracker = hitTrackerFactory.NewCASHitTracker(anonCtx, &repb.RequestMetadata{})

	for i := 0; i < 10; i++ {
		hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(10, 20, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(cDigest).CloseWithBytesTransferred(100, 200, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(dDigest).CloseWithBytesTransferred(1_000, 2_000, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(eDigest).CloseWithBytesTransferred(10_000, 20_000, repb.Compressor_IDENTITY, "test")
	}

	hitTracker = hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")
	hitTrackerService.wg.Done()

	// Expect 10x [A, B, C, D, E] for ANON.
	expectation := func() bool { return hitTrackerService.downloads[interfaces.AuthAnonymousUser].Load() == 50 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 10 updates for group ANON")
	require.Equal(t, int64(222_220), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())

	// Group 1's update shouldn't be affected by ANON's batching
	expectation = func() bool { return hitTrackerService.downloads[group1].Load() == 2 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 1 cache hits for group 1")
	require.Equal(t, int64(4_000_000), hitTrackerService.bytesDownloaded[group1].Load())
}

func TestCASHitTracker_DropsUpdates(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_hit_tracker.max_hits_per_update", 10)
	flags.Set(t, "cache_proxy.remote_hit_tracker.max_pending_hits_per_group", 10)
	authenticator, hitTrackerFactory, hitTrackerService := setup(t)

	// Pause the hit-tracker RPC service and send an RPC that'll block the
	// hit-tracker-client worker so updates are queued.
	hitTrackerService.wg.Add(1)
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	hitTracker := hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")

	anonCtx := context.Background()
	hitTracker = hitTrackerFactory.NewCASHitTracker(anonCtx, &repb.RequestMetadata{})

	for i := 0; i < 10; i++ {
		hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(10, 20, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(cDigest).CloseWithBytesTransferred(100, 200, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(dDigest).CloseWithBytesTransferred(1_000, 2_000, repb.Compressor_IDENTITY, "test")
		hitTracker.TrackDownload(eDigest).CloseWithBytesTransferred(10_000, 20_000, repb.Compressor_IDENTITY, "test")
	}

	hitTracker = hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")
	hitTrackerService.wg.Done()

	// Expect A, B, C, D, E, A, B, C, D, E to be sent for ANON.
	expectation := func() bool { return hitTrackerService.downloads[interfaces.AuthAnonymousUser].Load() == 10 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 10 updates for group ANON")
	require.Equal(t, int64(44_444), hitTrackerService.bytesDownloaded[interfaces.AuthAnonymousUser].Load())

	// Even though group 1's second update came at the end, it should still be sent.
	expectation = func() bool { return hitTrackerService.downloads[group1].Load() == 2 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 2 cache hits for group 1")
	require.Equal(t, int64(4_000_000), hitTrackerService.bytesDownloaded[group1].Load())
}

func BenchmarkEnqueue(b *testing.B) {
	numToEnqueue := 1_000_000
	flags.Set(b, "cache_proxy.remote_hit_tracker.max_hits_per_update", b.N*numToEnqueue)
	flags.Set(b, "cache_proxy.remote_hit_tracker.max_pending_hits_per_group", b.N*numToEnqueue)
	_, hitTrackerFactory, _ := setup(b)

	b.ReportAllocs()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		hitTracker := hitTrackerFactory.NewCASHitTracker(b.Context(), &repb.RequestMetadata{})
		b.StartTimer()
		wg := sync.WaitGroup{}
		for i := 0; i < numToEnqueue; i++ {
			wg.Add(1)
			go func() {
				hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
				wg.Done()
			}()
		}
		wg.Wait()
		b.StopTimer()
	}
}
