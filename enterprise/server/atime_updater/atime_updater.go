package atime_updater

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

var (
	// The maximum gRPC message size is 4MB. Each digest proto is
	// 64 bytes (hash) + 8 bytes (size) + 2 bytes (tags) = 74 bytes. The
	// remaining fields in FindMissingBlobsRequest should be small (1kb?), so
	// we can conceivably fit 4MB / 74bytes = 14.1k of these. Cap it at 10k for
	// now and we can increase it (or increase the update frequency) if we drop
	// a lot of updates. Note this also affects memory usage of the proxy!
	atimeUpdaterMaxDigestsPerUpdate = flag.Int("cache_proxy.remote_atime_max_digests_per_update", 10*1000, "The maximum number of blob digests to send in a single request for updating blob access times in the remote cache.")
	atimeUpdaterMaxUpdatesPerGroup  = flag.Int("cache_proxy.remote_atime_max_updates_per_group", 50, "The maximum number of FindMissingBlobRequests to accumulate per group for updating blob access times in the remote cache.")
	atimeUpdaterMaxDigestsPerGroup  = flag.Int("cache_proxy.remote_atime_max_digests_per_group", 200*1000, "The maximum number of blob digests to enqueue atime updates for, per group, across all instance names / hash fucntions.")
	atimeUpdaterBatchUpdateInterval = flag.Duration("cache_proxy.remote_atime_update_interval", 10*time.Second, "The time interval to wait between sending access time updates to the remote cache.")
	atimeUpdaterRpcTimeout          = flag.Duration("cache_proxy.remote_atime_rpc_timeout", 2*time.Second, "RPC timeout to use when updating blob access times in the remote cache.")
)

// A pending batch of atime updates (basically a FindMissingBlobsRequest).
// Stored as a struct so digest keys can be stored in a set for de-duping.
type atimeUpdate struct {
	instanceName   string
	digests        map[digest.Key]struct{} // stored as a set for de-duping.
	digestFunction repb.DigestFunction_Value
}

func (u *atimeUpdate) toProto() *repb.FindMissingBlobsRequest {
	req := repb.FindMissingBlobsRequest{
		InstanceName:   u.instanceName,
		BlobDigests:    make([]*repb.Digest, len(u.digests)),
		DigestFunction: u.digestFunction,
	}
	i := 0
	for d := range u.digests {
		req.BlobDigests[i] = d.ToDigest()
		i++
	}
	return &req
}

// The set of pending access time updates to send per group. There should be at
// most one of these per group, but each contains many atimeUpdate{}s, as
// there is one per (groupID, instance-name, digest-function) tuple. However,
// the update will only keep one such atimeUpdate{} and if it gets too big will
// discard additional digests until it's flushed.
type atimeUpdates struct {
	mu          sync.Mutex // protects jwt, updates, and atimeUpdate.digests.
	authHeaders map[string][]string
	updates     []*atimeUpdate
	numDigests  int
}

type atimeUpdater struct {
	authenticator interfaces.Authenticator

	ticker <-chan time.Time
	quit   chan struct{}

	mu      sync.Mutex               // protects updates
	updates map[string]*atimeUpdates // pending updates keyed by groupID.

	maxDigestsPerUpdate int
	maxUpdatesPerGroup  int
	maxDigestsPerGroup  int
	rpcTimeout          time.Duration

	remote repb.ContentAddressableStorageClient
}

func Register(env *real_environment.RealEnv) error {
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return fmt.Errorf("An Authenticator is required to enable the AtimeUpdater.")
	}
	remote := env.GetContentAddressableStorageClient()
	if remote == nil {
		return fmt.Errorf("A remote CAS client is required to enable the AtimeUpdater.")
	}
	updater := atimeUpdater{
		authenticator:       authenticator,
		ticker:              env.GetClock().NewTicker(*atimeUpdaterBatchUpdateInterval).Chan(),
		quit:                make(chan struct{}, 1),
		updates:             make(map[string]*atimeUpdates),
		maxDigestsPerUpdate: *atimeUpdaterMaxDigestsPerUpdate,
		maxUpdatesPerGroup:  *atimeUpdaterMaxUpdatesPerGroup,
		maxDigestsPerGroup:  *atimeUpdaterMaxDigestsPerGroup,
		rpcTimeout:          *atimeUpdaterRpcTimeout,
		remote:              remote,
	}
	// TODO(iain): make an effort to drain queue on server shutdown.
	go updater.start()
	env.SetAtimeUpdater(&updater)
	env.GetHealthChecker().RegisterShutdownFunction(updater.shutdown)
	return nil
}

func (u *atimeUpdater) groupID(ctx context.Context) string {
	user, err := u.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (u *atimeUpdater) Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) {
	if len(digests) == 0 {
		return
	}

	groupID := u.groupID(ctx)
	authHeaders := authutil.GetAuthHeaders(ctx, u.authenticator)
	if len(authHeaders) == 0 {
		log.Infof("Dropping remote atime update due to missing auth headers in context")
		return
	}

	u.mu.Lock()
	updates, ok := u.updates[groupID]
	if !ok {
		updates = &atimeUpdates{}
		u.updates[groupID] = updates
	}
	u.mu.Unlock()

	// Uniqueify the incoming digests. Note this wrecks the order of the
	// requested updates. Too bad. Keep track of the counts for metrics below.
	keys := map[digest.Key]int{}
	for _, digestProto := range digests {
		keys[digest.NewKey(digestProto)]++
	}

	// TODO(iain): this locking may be a bit overzealous. Hopefully not though.
	updates.mu.Lock()
	defer updates.mu.Unlock()

	// Always use the most recent auth headers for a group for remote atime updates.
	updates.authHeaders = authHeaders

	// First, find the update that the new digests can be merged into, or create
	// a new one if one doesn't exist.
	var pendingUpdate *atimeUpdate
	for _, update := range updates.updates {
		if update.instanceName == instanceName && update.digestFunction == digestFunction {
			pendingUpdate = update
			break
		}
	}
	if pendingUpdate == nil {
		if len(updates.updates) >= u.maxUpdatesPerGroup {
			log.CtxInfof(ctx, "Too many pending FindMissingBlobsRequests for updating remote atime for group %s, dropping %d pending atime updates", groupID, len(keys))
			metrics.RemoteAtimeUpdates.With(
				prometheus.Labels{
					metrics.GroupID:              groupID,
					metrics.EnqueueUpdateOutcome: "dropped_too_many_batches",
				}).Add(float64(len(digests)))
			return
		}
		pendingUpdate = &atimeUpdate{
			instanceName:   instanceName,
			digests:        map[digest.Key]struct{}{},
			digestFunction: digestFunction,
		}
		updates.updates = append(updates.updates, pendingUpdate)
	}

	// Now merge the new digests into the update.
	enqueued := 0
	duplicate := 0
	dropped := 0
	for key, count := range keys {
		if _, ok := pendingUpdate.digests[key]; ok {
			duplicate += count
			continue
		}
		if updates.numDigests+1 > u.maxDigestsPerGroup {
			dropped = len(digests) - enqueued - duplicate
			log.CtxWarningf(ctx, "maxDigestsPerGroup exceeded for group %s, dropping %d digests", groupID, dropped)
			break
		}
		pendingUpdate.digests[key] = struct{}{}
		updates.numDigests++
		enqueued++
		duplicate += count - 1
	}

	if enqueued+duplicate+dropped != len(digests) {
		log.Debugf("atime-updater metrics don't add up. incoming digests: %d, added: %d, duplicates: %d, dropped: %d", len(digests), enqueued, duplicate, dropped)
	}

	metrics.RemoteAtimeUpdates.With(
		prometheus.Labels{
			metrics.GroupID:              groupID,
			metrics.EnqueueUpdateOutcome: "enqueued",
		}).Add(float64(enqueued))
	metrics.RemoteAtimeUpdates.With(
		prometheus.Labels{
			metrics.GroupID:              groupID,
			metrics.EnqueueUpdateOutcome: "duplicate",
		}).Add(float64(duplicate))
	metrics.RemoteAtimeUpdates.With(
		prometheus.Labels{
			metrics.GroupID:              groupID,
			metrics.EnqueueUpdateOutcome: "dropped_too_many_updates",
		}).Add(float64(dropped))
}

func (u *atimeUpdater) EnqueueByResourceName(ctx context.Context, downloadString string) {
	if digest.IsActionCacheResourceName(downloadString) {
		// ActionCache entries are always read authoritatively, so no need to
		// enqueue an atime update.
		return
	}
	rn, err := digest.ParseDownloadResourceName(downloadString)
	if err != nil {
		log.Warningf("Skipping remote atime update for malformed download resource name: %s [%s]", downloadString, err)
		return
	}
	u.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
}

func (u *atimeUpdater) EnqueueByFindMissingRequest(ctx context.Context, req *repb.FindMissingBlobsRequest) {
	u.Enqueue(ctx, req.InstanceName, req.BlobDigests, req.DigestFunction)
}

func (u *atimeUpdater) start() {
	for {
		select {
		case <-u.quit:
			return
		case <-u.ticker:
			u.sendUpdates(context.Background())
		}
	}
}

// Sends one update per group. Because the updates are stored in a per-group
// array, the updater will round-robin across instance-names and digest
// functions for each group. We could change that approach to send the biggest
// update per group each time or something, but that could starve lesser used
// instance-names.
func (u *atimeUpdater) sendUpdates(ctx context.Context) int {
	// Remove updates to send and release the mutex before sending RPCs.
	updatesToSend := map[string]*repb.FindMissingBlobsRequest{}
	updatesSent := 0
	authHeaders := map[string]map[string][]string{}
	u.mu.Lock()
	for groupID, updates := range u.updates {
		updates.mu.Lock()
		update := u.getUpdate(updates)
		if update == nil {
			updates.mu.Unlock()
			continue
		}
		updatesToSend[groupID] = update
		authHeaders[groupID] = updates.authHeaders
		updates.numDigests -= len(update.BlobDigests)
		updates.mu.Unlock()
	}
	u.mu.Unlock()

	for groupID, update := range updatesToSend {
		updatesSent++
		ctx, cancel := context.WithTimeout(ctx, u.rpcTimeout)
		u.update(ctx, groupID, authHeaders[groupID], update)
		cancel()
	}
	return updatesSent
}

// Returns a FindMissingBlobsRequest representing the first atimeUpdate in the
// provided list of atimeUpdates, potentially splitting it if the first update
// in the queue is too large. Also reorders the atimeUpdates for inter-group
// fairness.
func (u *atimeUpdater) getUpdate(updates *atimeUpdates) *repb.FindMissingBlobsRequest {
	if len(updates.updates) == 0 {
		return nil
	}

	update := updates.updates[0]

	// If this update is small enough to send in its entirety, remove it from
	// the queue of updates.
	if len(update.digests) <= u.maxDigestsPerUpdate {
		updates.updates = updates.updates[1:]
		return update.toProto()
	}

	// Otherwise, remove maxDigestsPerUpdate digests from the update, send
	// those, and move this update to the back of the queue for fairness
	// with other updates in the group.
	updates.updates = updates.updates[1:]
	updates.updates = append(updates.updates, update)
	req := repb.FindMissingBlobsRequest{
		InstanceName:   update.instanceName,
		BlobDigests:    make([]*repb.Digest, u.maxDigestsPerUpdate),
		DigestFunction: update.digestFunction,
	}
	i := 0
	for digest := range update.digests {
		if i >= u.maxDigestsPerUpdate {
			break
		}
		req.BlobDigests[i] = digest.ToDigest()
		delete(update.digests, digest)
		i++
	}
	return &req
}

func (u *atimeUpdater) update(ctx context.Context, groupID string, authHeaders map[string][]string, req *repb.FindMissingBlobsRequest) {
	log.CtxDebugf(ctx, "Asynchronously processing %d atime updates for group %s", len(req.BlobDigests), groupID)

	ctx = authutil.AddAuthHeadersToContext(ctx, authHeaders, u.authenticator)

	_, err := u.remote.FindMissingBlobs(ctx, req)
	metrics.RemoteAtimeUpdatesSent.With(
		prometheus.Labels{
			metrics.GroupID:     groupID,
			metrics.StatusLabel: gstatus.Code(err).String(),
		}).Inc()
	if err != nil {
		log.CtxWarningf(ctx, "Error sending FindMissingBlobs request to update remote atimes for group %s: %s", groupID, err)
	}
}

func (u *atimeUpdater) shutdown(ctx context.Context) error {
	close(u.quit)

	// Make a best-effort attempt to flush pending updates.
	// TODO(iain): we could do something fancier here if necessary, like
	// fire-and-forget these RPCs with a rate-limiter. Let's try this for now.
	for u.sendUpdates(ctx) > 0 {
	}

	return nil
}
