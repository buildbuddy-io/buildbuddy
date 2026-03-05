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
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
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

	anonKey = usageutil.EncodeCollection(&usageutil.Collection{
		GroupID: interfaces.AuthAnonymousUser,
	})
	group1Key = usageutil.EncodeCollection(&usageutil.Collection{
		GroupID: group1,
	})
	group1InternalKey = usageutil.EncodeCollection(&usageutil.Collection{
		GroupID: group1,
		Origin:  "internal",
	})
	group1InternalBazelKey = usageutil.EncodeCollection(&usageutil.Collection{
		GroupID: group1,
		Client:  "bazel",
		Origin:  "internal",
	})
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
	out := &testHitTracker{
		t:                  t,
		authenticator:      authenticator,
		casDownloads:       map[string]*atomic.Int64{},
		casBytesDownloaded: map[string]*atomic.Int64{},
		casUploads:         map[string]*atomic.Int64{},
		casBytesUploaded:   map[string]*atomic.Int64{},
		acDownloads:        map[string]*atomic.Int64{},
		acBytesDownloaded:  map[string]*atomic.Int64{},
		acUploads:          map[string]*atomic.Int64{},
		acBytesUploaded:    map[string]*atomic.Int64{},
	}
	for _, k := range []string{anonKey, group1Key, group1InternalKey, group1InternalBazelKey} {
		out.casDownloads[k] = &atomic.Int64{}
		out.casBytesDownloaded[k] = &atomic.Int64{}
		out.casUploads[k] = &atomic.Int64{}
		out.casBytesUploaded[k] = &atomic.Int64{}
		out.acDownloads[k] = &atomic.Int64{}
		out.acBytesDownloaded[k] = &atomic.Int64{}
		out.acUploads[k] = &atomic.Int64{}
		out.acBytesUploaded[k] = &atomic.Int64{}
	}
	return out
}

func (ht *testHitTracker) Track(ctx context.Context, req *hitpb.TrackRequest) (*hitpb.TrackResponse, error) {
	ht.wg.Wait()

	key := usageutil.EncodeCollection(usageutil.CollectionFromRPCContext(ctx))

	for _, hit := range req.GetHits() {
		if hit.GetCacheRequestType() == capb.RequestType_READ {
			if hit.GetResource().GetCacheType() == rspb.CacheType_AC {
				ht.acDownloads[key].Add(1)
				ht.acBytesDownloaded[key].Add(hit.GetSizeBytes())
			} else {
				ht.casDownloads[key].Add(1)
				ht.casBytesDownloaded[key].Add(hit.GetSizeBytes())
			}
		} else if hit.GetCacheRequestType() == capb.RequestType_WRITE {
			if hit.GetResource().GetCacheType() == rspb.CacheType_AC {
				ht.acUploads[key].Add(1)
				ht.acBytesUploaded[key].Add(hit.GetSizeBytes())
			} else {
				ht.casUploads[key].Add(1)
				ht.casBytesUploaded[key].Add(hit.GetSizeBytes())
			}
		}
	}
	return &hitpb.TrackResponse{}, nil
}

func (ht *testHitTracker) acDownloadExpectation(key string, expectation int64) func() bool {
	return func() bool {
		return ht.acDownloads[key] != nil && ht.acDownloads[key].Load() == expectation
	}
}

func (ht *testHitTracker) casDownloadExpectation(key string, expectation int64) func() bool {
	return func() bool {
		return ht.casDownloads[key] != nil && ht.casDownloads[key].Load() == expectation
	}
}

func setup(t testing.TB) (interfaces.Authenticator, *HitTrackerFactory, *testHitTracker) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	te := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers(user1, group1))
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

func authenticatedContext(user string, authenticator interfaces.Authenticator) context.Context {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, "fakeheader"))
	if user != "" {
		ctx = authenticator.AuthContextFromAPIKey(ctx, user)
	}
	return authutil.ContextWithCachedAuthHeaders(ctx, authenticator)
}

func authenticatedContextWithInternalBazelClient(user string, authenticator interfaces.Authenticator) context.Context {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, "fakeheader", usageutil.OriginHeaderName, "internal"))
	if user != "" {
		ctx = authenticator.AuthContextFromAPIKey(ctx, user)
	}
	return bazel_request.OverrideRequestMetadata(authutil.ContextWithCachedAuthHeaders(ctx, authenticator), &repb.RequestMetadata{
		ToolDetails: &repb.ToolDetails{
			ToolName: "bazel",
		},
	})
}

func authenticatedContextWithOrigin(user string, authenticator interfaces.Authenticator, origin string) context.Context {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, "fakeheader", usageutil.OriginHeaderName, origin))
	if user != "" {
		ctx = authenticator.AuthContextFromAPIKey(ctx, user)
	}
	return authutil.ContextWithCachedAuthHeaders(ctx, authenticator)
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
	require.Empty(t, int64(0), hitTrackerService.casDownloads)
	require.Empty(t, int64(0), hitTrackerService.casUploads)
	require.Empty(t, int64(0), hitTrackerService.acDownloads)
	require.Empty(t, int64(0), hitTrackerService.acUploads)
}

func TestACHitTracker_SkipRemote(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(proxy_util.SkipRemoteKey, "true"))
	hitTracker := hitTrackerFactory.NewACHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := hitTrackerService.acDownloadExpectation(anonKey, 1)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.acBytesDownloaded[anonKey].Load())
	require.Equal(t, int64(1), hitTrackerService.acUploads[anonKey].Load())
	require.Equal(t, int64(4000), hitTrackerService.acBytesUploaded[anonKey].Load())
	require.Equal(t, int64(0), hitTrackerService.casDownloads[anonKey].Load())
	require.Equal(t, int64(0), hitTrackerService.casUploads[anonKey].Load())
}

func TestCASHitTracker(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := hitTrackerService.casDownloadExpectation(anonKey, 1)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.casBytesDownloaded[anonKey].Load())
	// By default, we don't expect CAS upload tracking
	require.Equal(t, int64(0), hitTrackerService.casUploads[anonKey].Load())
}

func TestCASHitTracker_SkipRemote(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(proxy_util.SkipRemoteKey, "true"))
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackMiss(aDigest)
	hitTracker.TrackDownload(bDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackUpload(cDigest).CloseWithBytesTransferred(3000, 4000, repb.Compressor_IDENTITY, "test")

	expectation := hitTrackerService.casDownloadExpectation(anonKey, 1)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(2000), hitTrackerService.casBytesDownloaded[anonKey].Load())
	require.Equal(t, int64(1), hitTrackerService.casUploads[anonKey].Load(), "Expected 1 HitTracker.Track upload")
	require.Equal(t, int64(4000), hitTrackerService.casBytesUploaded[anonKey].Load())
}

func TestCASHitTracker_NoDeduplication(t *testing.T) {
	_, hitTrackerFactory, hitTrackerService := setup(t)
	ctx := context.Background()
	hitTracker := hitTrackerFactory.NewCASHitTracker(ctx, &repb.RequestMetadata{})
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")
	hitTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1000, 2000, repb.Compressor_IDENTITY, "test")

	expectation := hitTrackerService.casDownloadExpectation(anonKey, 2)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, int64(4000), hitTrackerService.casBytesDownloaded[anonKey].Load())
}

func TestCASHitTracker_SplitsUpdates(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_hit_tracker.max_hits_per_update", 10)
	authenticator, hitTrackerFactory, hitTrackerService := setup(t)

	// Pause the hit-tracker RPC service and send an RPC that'll block the
	// hit-tracker-client worker so updates are queued.
	hitTrackerService.wg.Add(1)
	group1Ctx := authenticatedContext(user1, authenticator)
	group1Tracker := hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	group1Tracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")

	group1InternalCtx := authenticatedContextWithOrigin(user1, authenticator, "internal")
	internalHitTracker := hitTrackerFactory.NewCASHitTracker(group1InternalCtx, &repb.RequestMetadata{})
	internalHitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
	internalHitTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")

	anonCtx := context.Background()
	anonTracker := hitTrackerFactory.NewCASHitTracker(anonCtx, &repb.RequestMetadata{})

	for i := 0; i < 10; i++ {
		anonTracker.TrackDownload(aDigest).CloseWithBytesTransferred(1, 2, repb.Compressor_IDENTITY, "test")
		anonTracker.TrackDownload(bDigest).CloseWithBytesTransferred(10, 20, repb.Compressor_IDENTITY, "test")
		anonTracker.TrackDownload(cDigest).CloseWithBytesTransferred(100, 200, repb.Compressor_IDENTITY, "test")
		anonTracker.TrackDownload(dDigest).CloseWithBytesTransferred(1_000, 2_000, repb.Compressor_IDENTITY, "test")
		anonTracker.TrackDownload(eDigest).CloseWithBytesTransferred(10_000, 20_000, repb.Compressor_IDENTITY, "test")
	}

	anotherGroup1Tracker := hitTrackerFactory.NewCASHitTracker(group1Ctx, &repb.RequestMetadata{})
	anotherGroup1Tracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_000_000, repb.Compressor_IDENTITY, "test")

	group1BazelCtx := authenticatedContextWithInternalBazelClient(user1, authenticator)
	group1WithBazelClientTracker := hitTrackerFactory.NewCASHitTracker(group1BazelCtx, &repb.RequestMetadata{})
	group1WithBazelClientTracker.TrackDownload(fDigest).CloseWithBytesTransferred(1_000_000, 2_030_000, repb.Compressor_IDENTITY, "test")
	hitTrackerService.wg.Done()

	// Expect 10x [A, B, C, D, E] for ANON.
	expectation := hitTrackerService.casDownloadExpectation(anonKey, 50)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 50 updates for group ANON")
	require.Equal(t, int64(222_220), hitTrackerService.casBytesDownloaded[anonKey].Load())

	// Group 1's update shouldn't be affected by ANON's batching
	expectation = hitTrackerService.casDownloadExpectation(group1Key, 2)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 2 external cache hits for group 1")
	require.Equal(t, int64(4_000_000), hitTrackerService.casBytesDownloaded[group1Key].Load())

	// Group 1's update that claims to be 'internal' should be separate from its non-internal update.
	expectation = hitTrackerService.casDownloadExpectation(group1InternalKey, 2)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 2 internal cache hits for group 1")
	require.Equal(t, int64(2_000_002), hitTrackerService.casBytesDownloaded[group1InternalKey].Load())

	// Same for Group 1's update that claims to be 'bazel' and 'internal'.
	expectation = hitTrackerService.casDownloadExpectation(group1InternalBazelKey, 1)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 1 internal bazel cache hit for group 1")
	require.Equal(t, int64(2_030_000), hitTrackerService.casBytesDownloaded[group1InternalBazelKey].Load())
}

func TestCASHitTracker_DropsUpdates(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_hit_tracker.max_hits_per_update", 10)
	flags.Set(t, "cache_proxy.remote_hit_tracker.max_pending_hits_per_key", 10)
	authenticator, hitTrackerFactory, hitTrackerService := setup(t)

	// Pause the hit-tracker RPC service and send an RPC that'll block the
	// hit-tracker-client worker so updates are queued.
	hitTrackerService.wg.Add(1)
	group1Ctx := authenticatedContext(user1, authenticator)
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
	expectation := hitTrackerService.casDownloadExpectation(anonKey, 10)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 10 updates for group ANON")
	require.Equal(t, int64(44_444), hitTrackerService.casBytesDownloaded[anonKey].Load())

	// Even though group 1's second update came at the end, it should still be sent.
	expectation = hitTrackerService.casDownloadExpectation(group1Key, 2)
	require.Eventually(t, expectation, 10*time.Second, 100*time.Millisecond, "Expected 2 cache hits for group 1")
	require.Equal(t, int64(4_000_000), hitTrackerService.casBytesDownloaded[group1Key].Load())
}

func BenchmarkEnqueue(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	numToEnqueue := 1_000_000
	flags.Set(b, "cache_proxy.remote_hit_tracker.max_hits_per_update", b.N*numToEnqueue)
	flags.Set(b, "cache_proxy.remote_hit_tracker.max_pending_hits_per_key", b.N*numToEnqueue)

	b.ReportAllocs()
	for b.Loop() {
		// The client discards hits and logs stuff if too many hits are
		// enqueued. Recreate for each benchmark to prevent this.
		b.StopTimer()
		_, hitTrackerFactory, _ := setup(b)
		b.StartTimer()
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
