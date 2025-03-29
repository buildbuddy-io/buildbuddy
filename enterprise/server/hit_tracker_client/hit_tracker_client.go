package hit_tracker_client

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	remoteHitTrackerTarget       = flag.String("cache.remote_hit_tracker.target", "", "The gRPC target of the remote cache-hit-tracking service.")
	remoteHitTrackerTimeout      = flag.Duration("cache.remote_hit_tracker.rpc_timeout", 5*time.Second, "The timout to use for gRPC requests to the remote cache-hit-tracking service.")
	remoteHitTrackerPollInterval = flag.Duration("cache.remote_hit_tracker.update_interval", 250*time.Millisecond, "The time interval to wait between sending remote cache-hit-tracking RPCs.")
	remoteHitTrackerWorkers      = flag.Int("cache.remote_hit_tracker.workers", 3, "The number of workers to use to send asynchronous remote-cache-hit-tracking RPCs.")
)

func Register(env *real_environment.RealEnv) error {
	conn, err := grpc_client.DialInternal(env, *remoteHitTrackerTarget)
	if err != nil {
		return err
	}
	env.SetHitTrackerFactory(newHitTrackerClient(env, conn))
	return nil
}

func newHitTrackerClient(env *real_environment.RealEnv, conn grpc.ClientConnInterface) *HitTrackerFactory {
	factory := HitTrackerFactory{
		authenticator: env.GetAuthenticator(),
		ticker:        env.GetClock().NewTicker(*remoteHitTrackerPollInterval).Chan(),
		quit:          make(chan struct{}, 1),
		hits:          map[groupID]cacheHitsByInvocationID{},
		rpcTimeout:    *remoteHitTrackerTimeout,
		client:        hitpb.NewHitTrackerServiceClient(conn),
	}
	for i := 0; i < *remoteHitTrackerWorkers; i++ {
		go factory.start()
	}
	env.GetHealthChecker().RegisterShutdownFunction(factory.shutdown)
	return &factory
}

type cacheHits struct {
	// Gotta keep that jwt around so the app knows what group.
	emptyHits int64
	downloads []*hitpb.Download
}

// typedefs to make triple-nested map less gross :-(
type groupID string
type cacheHitsByRequestMetadata map[string]cacheHits
type cacheHitsByInvocationID map[string]cacheHitsByRequestMetadata

type HitTrackerFactory struct {
	authenticator interfaces.Authenticator

	ticker <-chan time.Time
	quit   chan struct{}

	mu   sync.Mutex
	hits map[groupID]cacheHitsByInvocationID

	rpcTimeout time.Duration

	client hitpb.HitTrackerServiceClient
}

func (h *HitTrackerFactory) NewACHitTracker(ctx context.Context, invocationID string, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	return &NoOpHitTracker{}
}

func (h *HitTrackerFactory) NewCASHitTracker(ctx context.Context, invocationID string, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	return &HitTrackerClient{ctx: ctx, enqueueFn: h.enqueue, client: h.client, invocationID: invocationID}
}

// For Action Cache hit-tracking, explicitly ignore everything. These cache
// artifacts should always be served from the authoritative cache which will
// take care of hit-tracking.
type NoOpHitTracker struct {
}

func (h *NoOpHitTracker) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
}

func (h *NoOpHitTracker) TrackMiss(d *repb.Digest) error {
	return nil
}

func (h *NoOpHitTracker) TrackEmptyHit() error {
	return nil
}

func (h *NoOpHitTracker) TrackDownload(d *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}

func (h *NoOpHitTracker) TrackUpload(d *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}

type NoOpTransferTimer struct {
}

func (t *NoOpTransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	return nil
}

func (t *NoOpTransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return nil
}

// For CAS hit-tracking, use a hit-tracker that sends information about local
// cache hits to the RPC service at the configured backend.
type HitTrackerClient struct {
	ctx             context.Context
	enqueueFn       func(context.Context, string, string, int64, *hitpb.Download)
	client          hitpb.HitTrackerServiceClient
	invocationID    string
	requestMetadata string
}

// This should only be used for Action Cache hit tracking.
func (h *HitTrackerClient) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
	log.Warning("Unexpected call to SetExecutedActionMetadata")
}

// Misses hit the backing cache, so no need to report on hit-tracking.
func (h *HitTrackerClient) TrackMiss(d *repb.Digest) error {
	return nil
}

func (h *HitTrackerFactory) groupID(ctx context.Context) groupID {
	user, err := h.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return groupID(user.GetGroupID())
}

func (h *HitTrackerFactory) enqueue(ctx context.Context, invocationID, requestMetadata string, emptyHits int64, download *hitpb.Download) {
	h.mu.Lock()
	defer h.mu.Unlock()
	groupID := h.groupID(ctx)
	hitsByGroup, ok := h.hits[groupID]
	if !ok {
		hitsByGroup = cacheHitsByInvocationID{}
		h.hits[groupID] = hitsByGroup
	}

	hitsByInvocation, ok := hitsByGroup[invocationID]
	if !ok {
		hitsByInvocation = cacheHitsByRequestMetadata{}
		hitsByGroup[invocationID] = hitsByInvocation
	}

	hitsByRMD, ok := hitsByInvocation[requestMetadata]
	if !ok {
		hitsByRMD = cacheHits{}
		hitsByInvocation[requestMetadata] = hitsByRMD
	}

	hitsByRMD.emptyHits += emptyHits
	if download != nil {
		hitsByRMD.downloads = append(hitsByRMD.downloads, download)
	}
}

func (h *HitTrackerClient) TrackEmptyHit() error {
	h.enqueueFn(h.ctx, h.invocationID, h.requestMetadata, 1, nil)
	return nil
}

type TransferTimer struct {
	ctx             context.Context
	enqueueFn       func(context.Context, string, string, int64, *hitpb.Download)
	invocationID    string
	requestMetadata string
	digest          *repb.Digest
	start           time.Time
	client          hitpb.HitTrackerServiceClient
}

func (t *TransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	download := &hitpb.Download{
		Resource: &rspb.ResourceName{
			Digest:    t.digest,
			CacheType: rspb.CacheType_CAS,
		},
		SizeBytes: bytesTransferredClient,
		Duration:  durationpb.New(time.Since(t.start)),
	}
	t.enqueueFn(t.ctx, t.invocationID, t.requestMetadata, 0, download)
	return nil
}

func (t *TransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return status.InternalError("Unxpected call to hit_tracker_client.Record()")
}

func (h *HitTrackerClient) TrackDownload(digest *repb.Digest) interfaces.TransferTimer {
	return &TransferTimer{
		ctx:          h.ctx,
		enqueueFn:    h.enqueueFn,
		invocationID: h.invocationID,
		digest:       digest,
		start:        time.Now(),
		client:       h.client,
	}
}

// Writes hit the backing cache, so no need to report on hit-tracking.
func (h *HitTrackerClient) TrackUpload(digest *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}

func (h *HitTrackerFactory) start() {
	for {
		select {
		case <-h.quit:
			return
		case <-h.ticker:
			h.sendTrackRequests(context.Background())
		}
	}
}

func (h *HitTrackerFactory) sendTrackRequests(ctx context.Context) int {
	i := 0
	groups := make([]groupID, len(h.hits))
	h.mu.Lock()
	for group := range h.hits {
		groups[i] = group
		i++
	}
	h.mu.Unlock()

	for _, group := range groups {
		trackRequestProto := hitpb.TrackRequest{}
		h.mu.Lock()
		// TODO(iain): limit the size of these, maybe?
		for iid, hitsByIID := range h.hits[group] {
			cacheHitsProto := hitpb.CacheHits{InvocationId: iid}
			trackRequestProto.Hits = append(trackRequestProto.Hits, &cacheHitsProto)
			for rmd, hits := range hitsByIID {
				cacheHitsProto.BazelRequestMetadata = rmd
				cacheHitsProto.EmptyHits = hits.emptyHits
				cacheHitsProto.Downloads = hits.downloads
			}
		}
		h.hits[group] = cacheHitsByInvocationID{}
		h.mu.Unlock()

		if len(trackRequestProto.Hits) > 0 {
			h.client.Track(context.Background(), &trackRequestProto)
		}
	}
	return 0
}

func (h *HitTrackerFactory) shutdown(ctx context.Context) error {
	close(h.quit)

	// Make a best-effort attempt to flush pending updates.
	// TODO(iain): we could do something fancier here if necessary, like
	// fire-and-forget these RPCs with a rate-limiter. Let's try this for now.
	for h.sendTrackRequests(ctx) > 0 {
	}

	return nil
}
