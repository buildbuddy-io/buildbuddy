package hit_tracker_client

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

var (
	enqueueChanSize = flag.Int("cache_proxy.remote_hit_tracker.remote_hit_queue_size", 3_000_000, "The size of the channel to use for buffering enqueued hits.")

	remoteHitTrackerTarget       = flag.String("cache_proxy.remote_hit_tracker.target", "", "The gRPC target of the remote cache-hit-tracking service.")
	remoteHitTrackerPollInterval = flag.Duration("cache_proxy.remote_hit_tracker.update_interval", 250*time.Millisecond, "The time interval to wait between sending remote cache-hit-tracking RPCs.")
	maxPendingHitsPerKey         = flag.Int("cache_proxy.remote_hit_tracker.max_pending_hits_per_key", 3_000_000, "The maximum number of pending cache-hit updates to store in memory for a given (group, usagelabels) tuple.")
	maxHitsPerUpdate             = flag.Int("cache_proxy.remote_hit_tracker.max_hits_per_update", 250_000, "The maximum number of cache-hit updates to send in one request to the hit-tracking backend.")
	remoteHitTrackerWorkers      = flag.Int("cache_proxy.remote_hit_tracker.workers", 1, "The number of workers to use to send asynchronous remote cache-hit-tracking RPCs.")
)

func Register(env *real_environment.RealEnv) error {
	if *remoteHitTrackerTarget == "" || *remoteHitTrackerWorkers < 1 {
		env.SetHitTrackerFactory(&NoOpHitTrackerFactory{})
		return nil
	}

	conn, err := grpc_client.DialInternal(env, *remoteHitTrackerTarget)
	if err != nil {
		return err
	}
	env.SetHitTrackerFactory(newHitTrackerClient(env.GetServerContext(), env, conn))
	return nil
}

type NoOpHitTrackerFactory struct{}

func (h *NoOpHitTrackerFactory) NewACHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	return &NoOpHitTracker{}
}

func (h *NoOpHitTrackerFactory) NewCASHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	return &NoOpHitTracker{}
}

func (h *NoOpHitTrackerFactory) NewRemoteACHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata, server string) interfaces.HitTracker {
	alert.CtxUnexpectedEvent(ctx, "Tried to do remote AC hit tracking from a server that is itself remote.")
	return &NoOpHitTracker{}
}

func (h *NoOpHitTrackerFactory) NewRemoteCASHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata, server string) interfaces.HitTracker {
	alert.CtxUnexpectedEvent(ctx, "Tried to do remote CAS hit tracking from a server that is itself remote.")
	return &NoOpHitTracker{}
}

func newHitTrackerClient(ctx context.Context, env *real_environment.RealEnv, conn grpc.ClientConnInterface) *HitTrackerFactory {
	factory := HitTrackerFactory{
		authenticator:        env.GetAuthenticator(),
		quit:                 make(chan struct{}, 1),
		enqueueChan:          make(chan *enqueuedCacheHit, *enqueueChanSize),
		maxPendingHitsPerKey: *maxPendingHitsPerKey,
		maxHitsPerUpdate:     *maxHitsPerUpdate,
		hitsByCollection:     map[string]*cacheHits{},
		client:               hitpb.NewHitTrackerServiceClient(conn),
	}
	go factory.batcher()
	for i := 0; i < *remoteHitTrackerWorkers; i++ {
		factory.wg.Add(1)
		go func() {
			ticker := env.GetClock().NewTicker(*remoteHitTrackerPollInterval).Chan()
			factory.sender(ctx, ticker)
			factory.wg.Done()
		}()
	}
	env.GetHealthChecker().RegisterShutdownFunction(factory.shutdown)
	return &factory
}

type groupID string
type cacheHits struct {
	maxPendingHits    int
	encodedCollection string
	mu                sync.Mutex
	authHeaders       map[string][]string
	hits              []*hitpb.CacheHit
}

func (c *cacheHits) enqueue(hit *hitpb.CacheHit, authHeaders map[string][]string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.hits) >= c.maxPendingHits {
		return false
	}

	// Store the latest headers for this group for use in the async RPC.
	c.authHeaders = authHeaders
	c.hits = append(c.hits, hit)
	return true
}

// An enqueued cache hit
type enqueuedCacheHit struct {
	collection  *usageutil.Collection
	hit         *hitpb.CacheHit
	authHeaders map[string][]string
}

type HitTrackerFactory struct {
	authenticator interfaces.Authenticator

	quit chan struct{}
	wg   sync.WaitGroup

	enqueueChan chan *enqueuedCacheHit

	mu                   sync.Mutex
	maxPendingHitsPerKey int
	maxHitsPerUpdate     int
	hitsByCollection     map[string]*cacheHits
	hitsQueue            []*cacheHits // Used to round-robin send collection keys

	client hitpb.HitTrackerServiceClient
}

func (h *HitTrackerFactory) NewACHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	// XXX: TBD
	/*if !proxy_util.SkipRemote(ctx) {
		// For Action Cache hit-tracking hitting the remote cache, the
		// authoritative cache should always take care of hit-tracking.
		alert.UnexpectedEvent("Unexpected call to NewACHitTracker in the proxy")
	}*/

	// Use a hit-tracker that sends information
	// about local cache hits to the RPC service at the configured backend.
	return &HitTrackerClient{ctx: ctx, enqueueFn: h.enqueue, client: h.client, requestMetadata: requestMetadata, cacheType: rspb.CacheType_AC}
}

func (h *HitTrackerFactory) NewCASHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	// For CAS hit-tracking, use a hit-tracker that sends information about
	// local cache hits to the RPC service at the configured backend.
	return &HitTrackerClient{ctx: ctx, enqueueFn: h.enqueue, client: h.client, requestMetadata: requestMetadata, cacheType: rspb.CacheType_CAS}
}

func (h *HitTrackerFactory) NewRemoteACHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata, server string) interfaces.HitTracker {
	alert.CtxUnexpectedEvent(ctx, "Tried to do remote AC hit tracking from a server that is itself remote.")
	return h.NewACHitTracker(ctx, requestMetadata)
}

func (h *HitTrackerFactory) NewRemoteCASHitTracker(ctx context.Context, requestMetadata *repb.RequestMetadata, server string) interfaces.HitTracker {
	alert.CtxUnexpectedEvent(ctx, "Tried to do remote CAS hit tracking from a server that is itself remote.")
	return h.NewCASHitTracker(ctx, requestMetadata)
}

type NoOpHitTracker struct{}

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
	enqueueFn       func(context.Context, *hitpb.CacheHit)
	client          hitpb.HitTrackerServiceClient
	requestMetadata *repb.RequestMetadata
	cacheType       rspb.CacheType
}

// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/4875) Implement
func (h *HitTrackerClient) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
	// This is used to track action durations and is not used for non-RBE executions.
	// Currently skip-remote behavior is not used for RBE, so do nothing in this case.
	if proxy_util.SkipRemote(h.ctx) {
		return
	}
	// By default, AC hit tracking should be handled by the remote cache.
	// XXX: TBD
	// alert.UnexpectedEvent("Unexpected call to SetExecutedActionMetadata")
}

// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/4875) Implement
func (h *HitTrackerClient) TrackMiss(d *repb.Digest) error {
	// For requests that hit the backing cache: local cache misses hit the backing
	// cache, which will take care of hit-tracking for this request.
	//
	// For requests that skip the backing cache: tracking misses is only used for
	// populating the cache scorecard for Bazel builds with remote caching.
	// Currently skip-remote behavior is only used for workflows + Remote Bazel,
	// and not typical Bazel builds, so don't worry about tracking misses.
	return nil
}

func (h *HitTrackerFactory) groupID(ctx context.Context) string {
	claims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return claims.GetGroupID()
}

func (h *HitTrackerFactory) enqueue(ctx context.Context, hit *hitpb.CacheHit) {
	if h.shouldFlushSynchronously() {
		log.CtxInfof(ctx, "hit_tracker_client.enqueue after worker shutdown, sending RPC synchronously")
		// Note: no need to mess with client/origin headers here, they're forwarded along from the incoming ctx.
		if _, err := h.client.Track(ctx, &hitpb.TrackRequest{Hits: []*hitpb.CacheHit{hit}, Server: usageutil.ServerName()}); err != nil {
			log.CtxWarningf(ctx, "Error sending HitTrackerService.Track RPC: %v", err)
		}
		return
	}

	enqueuedHit := enqueuedCacheHit{
		collection:  usageutil.CollectionFromRPCContext(ctx),
		hit:         hit,
		authHeaders: authutil.GetAuthHeaders(ctx),
	}

	select {
	case h.enqueueChan <- &enqueuedHit:
	default:
		metrics.RemoteHitTrackerUpdates.WithLabelValues(
			enqueuedHit.collection.GroupID,
			"dropped_channel_full",
		).Inc()
	}
}
func (h *HitTrackerFactory) batch(enqueuedHit *enqueuedCacheHit) {
	c := enqueuedHit.collection
	k := usageutil.EncodeCollection(c)

	h.mu.Lock()
	usageKeyHits, ok := h.hitsByCollection[k]
	if !ok {
		usageKeyHits = &cacheHits{
			maxPendingHits:    h.maxPendingHitsPerKey,
			encodedCollection: k,
			hits:              []*hitpb.CacheHit{},
		}
		h.hitsByCollection[k] = usageKeyHits
		h.hitsQueue = append(h.hitsQueue, usageKeyHits)
	}
	h.mu.Unlock()

	if usageKeyHits.enqueue(enqueuedHit.hit, enqueuedHit.authHeaders) {
		metrics.RemoteHitTrackerUpdates.WithLabelValues(
			string(c.GroupID),
			"enqueued",
		).Add(1)
		return
	}

	metrics.RemoteHitTrackerUpdates.WithLabelValues(
		c.GroupID,
		"dropped_too_many_updates",
	).Add(1)
}

type TransferTimer struct {
	ctx              context.Context
	enqueueFn        func(context.Context, *hitpb.CacheHit)
	invocationID     string
	requestMetadata  *repb.RequestMetadata
	digest           *repb.Digest
	start            time.Time
	client           hitpb.HitTrackerServiceClient
	cacheType        rspb.CacheType
	cacheRequestType capb.RequestType
}

func (t *TransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	hit := &hitpb.CacheHit{
		RequestMetadata: t.requestMetadata,
		Resource: &rspb.ResourceName{
			Digest:    t.digest,
			CacheType: t.cacheType,
		},
		SizeBytes:        bytesTransferredClient,
		Duration:         durationpb.New(time.Since(t.start)),
		CacheRequestType: t.cacheRequestType,
	}
	t.enqueueFn(t.ctx, hit)
	return nil
}

func (t *TransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return status.InternalError("Unxpected call to hit_tracker_client.Record()")
}

func (h *HitTrackerClient) TrackDownload(digest *repb.Digest) interfaces.TransferTimer {
	return &TransferTimer{
		ctx:              h.ctx,
		enqueueFn:        h.enqueueFn,
		requestMetadata:  h.requestMetadata,
		digest:           digest,
		start:            time.Now(),
		client:           h.client,
		cacheType:        h.cacheType,
		cacheRequestType: capb.RequestType_READ,
	}
}

func (h *HitTrackerClient) TrackUpload(digest *repb.Digest) interfaces.TransferTimer {
	if proxy_util.SkipRemote(h.ctx) {
		return &TransferTimer{
			ctx:              h.ctx,
			enqueueFn:        h.enqueueFn,
			requestMetadata:  h.requestMetadata,
			digest:           digest,
			start:            time.Now(),
			client:           h.client,
			cacheType:        h.cacheType,
			cacheRequestType: capb.RequestType_WRITE,
		}
	}
	// If writes hit the backing cache, it will handle hit tracking.
	return &NoOpTransferTimer{}
}

func (h *HitTrackerFactory) sender(ctx context.Context, ticker <-chan time.Time) {
	for {
		select {
		case <-h.quit:
			return
		case <-ticker:
		}
		// Keep flushing until there is nothing to flush.
		for h.sendTrackRequest(ctx) > 0 {
		}
	}
}

func (h *HitTrackerFactory) batcher() {
	for {
		select {
		case <-h.quit:
			return
		case update := <-h.enqueueChan:
			h.batch(update)
		}
	}
}

func (h *HitTrackerFactory) shouldFlushSynchronously() bool {
	select {
	case <-h.quit:
		return true
	default:
		return false
	}
}

// Sends the oldest pending batch of hits from the queue. This function sends
// one RPC and returns the number of updates sent.
func (h *HitTrackerFactory) sendTrackRequest(ctx context.Context) int {
	h.mu.Lock()
	if len(h.hitsQueue) == 0 {
		h.mu.Unlock()
		return 0
	}
	hitsToSend := h.hitsQueue[0]
	h.hitsQueue = h.hitsQueue[1:]
	hitsToSend.mu.Lock()
	if len(hitsToSend.hits) <= h.maxHitsPerUpdate {
		delete(h.hitsByCollection, hitsToSend.encodedCollection)
	} else {
		hitsToEnqueue := cacheHits{
			encodedCollection: hitsToSend.encodedCollection,
			authHeaders:       hitsToSend.authHeaders,
			hits:              hitsToSend.hits[h.maxHitsPerUpdate:],
		}
		hitsToSend.hits = hitsToSend.hits[:h.maxHitsPerUpdate]
		h.hitsQueue = append(h.hitsQueue, &hitsToEnqueue)
		h.hitsByCollection[hitsToEnqueue.encodedCollection] = &hitsToEnqueue
	}
	h.mu.Unlock()

	ctx = authutil.AddAuthHeadersToContext(ctx, hitsToSend.authHeaders, h.authenticator)

	c, _, err := usageutil.DecodeCollection(hitsToSend.encodedCollection)
	if err != nil {
		log.CtxWarningf(ctx, "Error decoding collection for remote usage tracking key %s: %v", hitsToSend.encodedCollection, err)
	}
	ctx = usageutil.AddUsageHeadersToContext(ctx, c.Client, c.Origin)
	trackRequest := hitpb.TrackRequest{Hits: hitsToSend.hits, Server: usageutil.ServerName()}
	groupID := c.GroupID
	hitCount := len(hitsToSend.hits)
	hitsToSend.mu.Unlock()

	_, err = h.client.Track(ctx, &trackRequest)
	metrics.RemoteHitTrackerRequests.WithLabelValues(
		string(groupID),
		gstatus.Code(err).String(),
	).Observe(float64(hitCount))
	if err != nil {
		log.CtxWarningf(ctx, "Error sending Track request to record cache hit-tracking state group %s: %v", groupID, err)
	}
	return hitCount
}

func (h *HitTrackerFactory) shutdown(ctx context.Context) error {
	close(h.quit)
	h.wg.Wait()

	// Make a best-effort attempt to flush pending updates.
	// TODO(iain): we could do something fancier here if necessary, like
	// fire-and-forget these RPCs with a rate-limiter. Let's try this for now.
	for h.sendTrackRequest(ctx) > 0 {
	}

	return nil
}
