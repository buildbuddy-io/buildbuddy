package hit_tracker_client

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
	t                  testing.TB
	authenticator      interfaces.Authenticator
	wg                 sync.WaitGroup
	casDownloads       map[string]*atomic.Int64
	casBytesDownloaded map[string]*atomic.Int64
	casUploads         map[string]*atomic.Int64
	casBytesUploaded   map[string]*atomic.Int64
	acDownloads        map[string]*atomic.Int64
	acBytesDownloaded  map[string]*atomic.Int64
	acUploads          map[string]*atomic.Int64
	acBytesUploaded    map[string]*atomic.Int64
}

func newTestHitTracker(t testing.TB, authenticator interfaces.Authenticator) *testHitTracker {
	return &testHitTracker{
		t:             t,
		authenticator: authenticator,
		casDownloads: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		casBytesDownloaded: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		casUploads: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		casBytesUploaded: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		acDownloads: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		acBytesDownloaded: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		acUploads: map[string]*atomic.Int64{
			interfaces.AuthAnonymousUser: &atomic.Int64{},
			group1:                       &atomic.Int64{},
		},
		acBytesUploaded: map[string]*atomic.Int64{
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
		if hit.GetCacheRequestType() == capb.RequestType_READ {
			if hit.GetResource().GetCacheType() == rspb.CacheType_AC {
				ht.acDownloads[groupID].Add(1)
				ht.acBytesDownloaded[groupID].Add(hit.GetSizeBytes())
			} else {
				ht.casDownloads[groupID].Add(1)
				ht.casBytesDownloaded[groupID].Add(hit.GetSizeBytes())
			}
		} else if hit.GetCacheRequestType() == capb.RequestType_WRITE {
			if hit.GetResource().GetCacheType() == rspb.CacheType_AC {
				ht.acUploads[groupID].Add(1)
				ht.acBytesUploaded[groupID].Add(hit.GetSizeBytes())
			} else {
				ht.casUploads[groupID].Add(1)
				ht.casBytesUploaded[groupID].Add(hit.GetSizeBytes())
			}
		}
	}
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

	// Hit tracking should be disabled by default
	require.Equal(t, int64(0), hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(0), hitTrackerService.casUploads[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(0), hitTrackerService.acDownloads[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(0), hitTrackerService.acUploads[interfaces.AuthAnonymousUser].Load())
}

func TestACHitTracker_SkipRemote(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(proxy_util.SkipRemoteKey, "true"))
	hitTracker := hitTrackerFactory.NewACHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := func() bool { return hitTrackerService.acDownloads[interfaces.AuthAnonymousUser].Load() == 1 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.acBytesDownloaded[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(1), hitTrackerService.acUploads[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(4000), hitTrackerService.acBytesUploaded[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(0), hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(0), hitTrackerService.casUploads[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := func() bool { return hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load() == 1 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.casBytesDownloaded[interfaces.AuthAnonymousUser].Load())
	// By default, we don't expect CAS upload tracking
	require.Equal(t, int64(0), hitTrackerService.casUploads[interfaces.AuthAnonymousUser].Load(), "Expected 1 HitTracker.Track upload")
}

func TestCASHitTracker_SkipRemote(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(proxy_util.SkipRemoteKey, "true"))
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := func() bool { return hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load() == 1 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.casBytesDownloaded[interfaces.AuthAnonymousUser].Load())
	require.Equal(t, int64(1), hitTrackerService.casUploads[interfaces.AuthAnonymousUser].Load(), "Expected 1 HitTracker.Track upload")
	require.Equal(t, int64(4000), hitTrackerService.casBytesUploaded[interfaces.AuthAnonymousUser].Load())
}

func TestCASHitTracker_NoDeduplication(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")

	expectation := func() bool { return hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load() == 2 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(4000), hitTrackerService.casBytesDownloaded[interfaces.AuthAnonymousUser].Load())
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
	expectation := func() bool { return hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load() == 50 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 10 updates for group ANON")
	require.Equal(t, int64(222_220), hitTrackerService.casBytesDownloaded[interfaces.AuthAnonymousUser].Load())

	// Group 1's update shouldn't be affected by ANON's batching
	expectation = func() bool { return hitTrackerService.casDownloads[group1].Load() == 2 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 1 cache hits for group 1")
	require.Equal(t, int64(4_000_000), hitTrackerService.casBytesDownloaded[group1].Load())
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
	expectation := func() bool { return hitTrackerService.casDownloads[interfaces.AuthAnonymousUser].Load() == 10 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 10 updates for group ANON")
	require.Equal(t, int64(44_444), hitTrackerService.casBytesDownloaded[interfaces.AuthAnonymousUser].Load())

	// Even though group 1's second update came at the end, it should still be sent.
	expectation = func() bool { return hitTrackerService.casDownloads[group1].Load() == 2 }
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 2 cache hits for group 1")
	require.Equal(t, int64(4_000_000), hitTrackerService.casBytesDownloaded[group1].Load())
}

func BenchmarkEnqueue(b *testing.B) {
	numToEnqueue := 1_000_000
	flags.Set(b, "cache_proxy.remote_hit_tracker.max_hits_per_update", b.N*numToEnqueue)
	flags.Set(b, "cache_proxy.remote_hit_tracker.max_pending_hits_per_group", b.N*numToEnqueue)
	_, hitTrackerFactory, _ := setup(b)

	b.ReportAllocs()
	for b.Loop() {
		hitTracker := hitTrackerFactory.NewCASHitTracker(b.Context(), &repb.RequestMetadata{})
		wg := sync.WaitGroup{}
		for i := 0; i < numToEnqueue; i++ {
			wg.Add(1)
			go func() {
				hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
