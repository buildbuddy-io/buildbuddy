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
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

var (
	atimeUpdaterEnqueueChanSize = flag.Int("cache_proxy.remote_atime_queue_size", 1000*1000, "The size of the channel to use for buffering enqueued atime updates.")

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
)

// A pending batch of atime updates (basically a FindMissingBlobsRequest).
// Stored as a struct so digest keys can be stored in a set for de-duping.
type atimeUpdate struct {
	mu             sync.Mutex
	instanceName   string
	digests        map[digest.Key]struct{} // stored as a set for de-duping.
	digestFunction repb.DigestFunction_Value
}

func (u *atimeUpdate) set(key digest.Key) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.digests[key] = struct{}{}
}

func (u *atimeUpdate) contains(key digest.Key) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, ok := u.digests[key]
	return ok
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

	maxUpdatesPerGroup int
}

func (u *atimeUpdates) findOrCreatePendingUpdate(instanceName string, digestFunction repb.DigestFunction_Value) *atimeUpdate {
	for _, update := range u.updates {
		if update.instanceName == instanceName && update.digestFunction == digestFunction {
			return update
		}
	}

	if len(u.updates) >= u.maxUpdatesPerGroup {
		return nil
	}
	pendingUpdate := &atimeUpdate{
		instanceName:   instanceName,
		digests:        map[digest.Key]struct{}{},
		digestFunction: digestFunction,
	}
	u.updates = append(u.updates, pendingUpdate)
	return pendingUpdate
}

// An enqueued atime update
type enqueuedAtimeUpdates struct {
	groupID        string
	authHeaders    map[string][]string
	instanceName   string
	digests        []*repb.Digest
	digestFunction repb.DigestFunction_Value
}
type atimeUpdater struct {
	authenticator interfaces.Authenticator

	enqueueChan chan *enqueuedAtimeUpdates

	ticker <-chan time.Time
	quit   chan struct{}

	updates sync.Map // pending updates keyed by groupID (string -> *atimeUpdates)

	maxDigestsPerUpdate int
	maxUpdatesPerGroup  int
	maxDigestsPerGroup  int

	remote repb.ContentAddressableStorageClient
}

func Register(env *real_environment.RealEnv) error {
	updater, err := new(env)
	if err != nil {
		return err
	}
	// TODO(iain): make an effort to drain queue on server shutdown.
	go updater.batcher()
	go updater.sender()
	env.SetAtimeUpdater(updater)
	env.GetHealthChecker().RegisterShutdownFunction(updater.shutdown)
	return nil
}

func new(env *real_environment.RealEnv) (*atimeUpdater, error) {
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, fmt.Errorf("An Authenticator is required to enable the AtimeUpdater.")
	}
	remote := env.GetContentAddressableStorageClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote CAS client is required to enable the AtimeUpdater.")
	}
	updater := atimeUpdater{
		authenticator:       authenticator,
		enqueueChan:         make(chan *enqueuedAtimeUpdates, *atimeUpdaterEnqueueChanSize),
		ticker:              env.GetClock().NewTicker(*atimeUpdaterBatchUpdateInterval).Chan(),
		quit:                make(chan struct{}, 1),
		maxDigestsPerUpdate: *atimeUpdaterMaxDigestsPerUpdate,
		maxUpdatesPerGroup:  *atimeUpdaterMaxUpdatesPerGroup,
		maxDigestsPerGroup:  *atimeUpdaterMaxDigestsPerGroup,
		remote:              remote,
	}
	return &updater, nil
}

func (u *atimeUpdater) groupID(ctx context.Context) string {
	user, err := u.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (u *atimeUpdater) Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) bool {
	if len(digests) == 0 {
		return true
	}

	groupID := u.groupID(ctx)
	authHeaders := authutil.GetAuthHeaders(ctx)
	if len(authHeaders) == 0 {
		log.Infof("Dropping remote atime update due to missing auth headers in context")
		return false
	}
	update := enqueuedAtimeUpdates{
		groupID:        groupID,
		authHeaders:    authHeaders,
		instanceName:   instanceName,
		digests:        digests,
		digestFunction: digestFunction,
	}

	// Try to send the update to the enqueueChan, where it will be processed by
	// the batcher, but only if this will not block.
	select {
	case u.enqueueChan <- &update:
		return true
	default:
		metrics.RemoteAtimeUpdates.WithLabelValues(
			groupID,
			"dropped_channel_full",
		).Add(float64(len(digests)))
		return false
	}
}

func (u *atimeUpdater) EnqueueByResourceName(ctx context.Context, rn *digest.CASResourceName) bool {
	return u.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
}

// Runs a loop that consumes updates from enqueueChan and adds them to the
// batches of pending atime updates to be sent to the backend.
func (u *atimeUpdater) batcher() {
	for {
		select {
		case <-u.quit:
			return
		case update := <-u.enqueueChan:
			u.batch(update)
		}
	}
}

func (u *atimeUpdater) batch(update *enqueuedAtimeUpdates) {
	groupID := update.groupID
	rawUpdates, _ := u.updates.LoadOrStore(groupID, &atimeUpdates{maxUpdatesPerGroup: u.maxUpdatesPerGroup})
	updates, ok := rawUpdates.(*atimeUpdates)
	if !ok {
		alert.UnexpectedEvent("atimeUpdater.updates contains value with invalid type", "actual type: %T", rawUpdates)
		return
	}

	// Uniqueify the incoming digests. Note this wrecks the order of the
	// requested updates. Too bad. Keep track of the counts for metrics below.
	keys := map[digest.Key]int{}
	for _, digestProto := range update.digests {
		keys[digest.NewKey(digestProto)]++
	}

	// Always use the most recent auth headers for a group for remote atime updates.
	updates.mu.Lock()
	updates.authHeaders = update.authHeaders

	// First, find the update that the new digests can be merged into, or create
	// a new one if one doesn't exist.
	pendingUpdate := updates.findOrCreatePendingUpdate(update.instanceName, update.digestFunction)
	updates.mu.Unlock()
	if pendingUpdate == nil {
		log.Infof("Too many pending FindMissingBlobsRequests for updating remote atime for group %s, dropping %d pending atime updates", groupID, len(keys))
		metrics.RemoteAtimeUpdates.WithLabelValues(
			groupID,
			"dropped_too_many_batches",
		).Add(float64(len(update.digests)))
		return
	}

	// Now merge the new digests into the update.
	enqueued := 0
	duplicate := 0
	dropped := 0
	for key, count := range keys {
		if pendingUpdate.contains(key) {
			duplicate += count
			continue
		}
		updates.mu.Lock()
		if updates.numDigests+1 > u.maxDigestsPerGroup {
			updates.mu.Unlock()
			dropped = len(update.digests) - enqueued - duplicate
			log.Warningf("maxDigestsPerGroup exceeded for group %s, dropping %d digests", groupID, dropped)
			break
		}
		updates.numDigests++
		updates.mu.Unlock()
		pendingUpdate.set(key)
		enqueued++
		duplicate += count - 1
	}

	if enqueued+duplicate+dropped != len(update.digests) {
		log.Debugf("atime-updater metrics don't add up. incoming digests: %d, added: %d, duplicates: %d, dropped: %d", len(update.digests), enqueued, duplicate, dropped)
	}

	metrics.RemoteAtimeUpdates.WithLabelValues(
		groupID,
		"enqueued",
	).Add(float64(enqueued))
	metrics.RemoteAtimeUpdates.WithLabelValues(
		groupID,
		"duplicate",
	).Add(float64(duplicate))
	metrics.RemoteAtimeUpdates.WithLabelValues(
		groupID,
		"dropped_too_many_updates",
	).Add(float64(dropped))
}

// Starts the loop that sends atime updates to the backend.
func (u *atimeUpdater) sender() {
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
	u.updates.Range(func(key, value interface{}) bool {
		groupID, ok := key.(string)
		if !ok {
			alert.UnexpectedEvent("atimeUpdater.updates contains key with unexpected type", "actual type: %T", key)
			return true
		}
		updates, ok := value.(*atimeUpdates)
		if !ok {
			alert.UnexpectedEvent("atimeUpdater.updates contains value with unexpected type", "actual type: %T", value)
			return true
		}
		updates.mu.Lock()
		update := u.getUpdate(updates)
		if update == nil {
			updates.mu.Unlock()
			return true
		}
		updatesToSend[groupID] = update
		authHeaders[groupID] = updates.authHeaders
		updates.numDigests -= len(update.BlobDigests)
		updates.mu.Unlock()
		return true
	})

	for groupID, update := range updatesToSend {
		updatesSent++
		u.update(ctx, groupID, authHeaders[groupID], update)
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
	update.mu.Lock()
	defer update.mu.Unlock()

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
	metrics.RemoteAtimeUpdatesSent.WithLabelValues(
		groupID,
		gstatus.Code(err).String(),
	).Inc()
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
