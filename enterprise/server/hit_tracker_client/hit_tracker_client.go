package hit_tracker_client

import (
	"context"
	"flag"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

const (
	initialSliceCapacity = 4_096
)

var (
	remoteHitTrackerTarget       = flag.String("cache.remote_hit_tracker.target", "", "The gRPC target of the remote cache-hit-tracking service.")
	remoteHitTrackerTimeout      = flag.Duration("cache.remote_hit_tracker.rpc_timeout", 5*time.Second, "The timout to use for gRPC requests to the remote cache-hit-tracking service.")
	remoteHitTrackerPollInterval = flag.Duration("cache.remote_hit_tracker.update_interval", 250*time.Millisecond, "The time interval to wait between sending remote cache-hit-tracking RPCs.")
	maxPendingHitsPerGroup       = flag.Int("cache.remote_hit_tracker.max_pending_hits_per_group", 100_000, "The maximum number of pending cache-hit updates to store in memory for a given group.")
	remoteHitTrackerWorkers      = flag.Int("cache.remote_hit_tracker.workers", 1, "The number of workers to use to send asynchronous remote cache-hit-tracking RPCs.")
)

func Register(env *real_environment.RealEnv) error {
	if *remoteHitTrackerTarget == "" {
		return nil
	}

	conn, err := grpc_client.DialInternal(env, *remoteHitTrackerTarget)
	if err != nil {
		return err
	}
	env.SetHitTrackerFactory(newHitTrackerClient(env, conn))
	return nil
}

func newHitTrackerClient(env *real_environment.RealEnv, conn grpc.ClientConnInterface) *HitTrackerFactory {
	factory := HitTrackerFactory{
		authenticator:          env.GetAuthenticator(),
		ticker:                 env.GetClock().NewTicker(*remoteHitTrackerPollInterval).Chan(),
		quit:                   make(chan struct{}, 1),
		stopped:                atomic.Bool{},
		maxPendingHitsPerGroup: *maxPendingHitsPerGroup,
		hitsByGroup:            map[groupID]*cacheHits{},
		rpcTimeout:             *remoteHitTrackerTimeout,
		client:                 hitpb.NewHitTrackerServiceClient(conn),
	}
	for i := 0; i < *remoteHitTrackerWorkers; i++ {
		go factory.runWorker()
	}
	env.GetHealthChecker().RegisterShutdownFunction(factory.shutdown)
	return &factory
}

type groupID string
type cacheHits struct {
	authHeaders map[string][]string
	hits        []*hitpb.CacheHit
}

type HitTrackerFactory struct {
	authenticator interfaces.Authenticator

	ticker  <-chan time.Time
	quit    chan struct{}
	stopped atomic.Bool

	mu                     sync.Mutex
	maxPendingHitsPerGroup int
	hitsByGroup            map[groupID]*cacheHits

	rpcTimeout time.Duration
	client     hitpb.HitTrackerServiceClient
}

func (h *HitTrackerFactory) NewACHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	// For Action Cache hit-tracking, explicitly ignore everything. These cache
	// artifacts should always be served from the authoritative cache which
	// will take care of hit-tracking.
	return &NoOpHitTracker{}
}

func (h *HitTrackerFactory) NewCASHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	// For CAS hit-tracking, use a hit-tracker that sends information about
	// local cache hits to the RPC service at the configured backend.
	return &HitTrackerClient{ctx: ctx, enqueueFn: h.enqueue, client: h.client, requestMetadata: requestMetadata}
}

type NoOpHitTracker struct {
}

func (h *NoOpHitTracker) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
}

func (h *NoOpHitTracker) TrackMiss(d *repb.Digest) error {
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

type HitTrackerClient struct {
	ctx             context.Context
	enqueueFn       func(context.Context, *repb.RequestMetadata, *hitpb.CacheHit)
	client          hitpb.HitTrackerServiceClient
	requestMetadata *repb.RequestMetadata
}

// This should only be used for Action Cache hit tracking.
func (h *HitTrackerClient) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
	alert.UnexpectedEvent("Unexpected call to SetExecutedActionMetadata")
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

func (h *HitTrackerFactory) enqueue(ctx context.Context, requestMetadata *repb.RequestMetadata, hit *hitpb.CacheHit) {
	if h.stopped.Load() {
		log.Warning("hit_tracker_client.enqueue after worker shutdown, sending RPC synchronously")
		hit.RequestMetadata = requestMetadata
		if _, err := h.client.Track(ctx, &hitpb.TrackRequest{Hits: []*hitpb.CacheHit{hit}}); err != nil {
			log.Infof("Error sending HitTrackerService.Track RPC: %v", err)
		}
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	groupID := h.groupID(ctx)
	if _, ok := h.hitsByGroup[groupID]; !ok {
		h.hitsByGroup[groupID] = &cacheHits{
			hits: make([]*hitpb.CacheHit, 0, initialSliceCapacity),
		}
	}

	groupHits := h.hitsByGroup[groupID]
	if len(groupHits.hits) >= h.maxPendingHitsPerGroup {
		log.CtxWarning(ctx, "Exceeded maximum number of pending cache hits, dropping some.")
		metrics.RemoteHitTrackerUpdates.With(
			prometheus.Labels{
				metrics.GroupID:              string(groupID),
				metrics.EnqueueUpdateOutcome: "dropped_too_many_updates",
			}).Add(float64(1))
		return
	}
	metrics.RemoteHitTrackerUpdates.With(
		prometheus.Labels{
			metrics.GroupID:              string(groupID),
			metrics.EnqueueUpdateOutcome: "enqueued",
		}).Add(float64(1))

	authHeaders := authutil.GetAuthHeaders(ctx, h.authenticator)
	groupHits.authHeaders = authHeaders
	groupHits.hits = append(groupHits.hits, hit)
}

type TransferTimer struct {
	ctx             context.Context
	enqueueFn       func(context.Context, *repb.RequestMetadata, *hitpb.CacheHit)
	invocationID    string
	requestMetadata *repb.RequestMetadata
	digest          *repb.Digest
	start           time.Time
	client          hitpb.HitTrackerServiceClient
}

func (t *TransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	hit := &hitpb.CacheHit{
		RequestMetadata: t.requestMetadata,
		Resource: &rspb.ResourceName{
			Digest:    t.digest,
			CacheType: rspb.CacheType_CAS,
		},
		SizeBytes: bytesTransferredClient,
		Duration:  durationpb.New(time.Since(t.start)),
	}
	t.enqueueFn(t.ctx, t.requestMetadata, hit)
	return nil
}

func (t *TransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return status.InternalError("Unxpected call to hit_tracker_client.Record()")
}

func (h *HitTrackerClient) TrackDownload(digest *repb.Digest) interfaces.TransferTimer {
	return &TransferTimer{
		ctx:             h.ctx,
		enqueueFn:       h.enqueueFn,
		requestMetadata: h.requestMetadata,
		digest:          digest,
		start:           time.Now(),
		client:          h.client,
	}
}

// Writes hit the backing cache, so no need to report on hit-tracking.
func (h *HitTrackerClient) TrackUpload(digest *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}

func (h *HitTrackerFactory) runWorker() {
	for {
		select {
		case <-h.quit:
			h.stopped.Store(false)
			return
		case <-h.ticker:
			h.sendTrackRequests(context.Background())
		}
	}
}

func (h *HitTrackerFactory) sendTrackRequests(ctx context.Context) int {
	i := 0
	groups := make([]groupID, len(h.hitsByGroup))
	h.mu.Lock()
	for group := range h.hitsByGroup {
		groups[i] = group
		i++
	}
	h.mu.Unlock()

	for _, groupID := range groups {
		h.mu.Lock()
		headers := h.hitsByGroup[groupID].authHeaders
		trackRequest := hitpb.TrackRequest{Hits: h.hitsByGroup[groupID].hits}
		h.hitsByGroup[groupID] = &cacheHits{}
		h.mu.Unlock()

		if len(trackRequest.Hits) > 0 {
			ctx = authutil.AddAuthHeadersToContext(ctx, headers, h.authenticator)
			_, err := h.client.Track(ctx, &trackRequest)
			metrics.RemoteAtimeUpdatesSent.With(
				prometheus.Labels{
					metrics.GroupID:     string(groupID),
					metrics.StatusLabel: gstatus.Code(err).String(),
				}).Inc()
			if err != nil {
				log.CtxWarningf(ctx, "Error sending Track request to record cache hit-tracking state group %s: %v", groupID, err)
			}
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
