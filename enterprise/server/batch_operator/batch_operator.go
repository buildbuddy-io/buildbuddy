package batch_operator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultQueueSize           = 1_000_000
	defaultBatchInterval       = 10 * time.Second
	defaultMaxDigestsPerUpdate = 10_000
	defaultMaxUpdatesPerGroup  = 50
	defaultMaxDigestsPerGroup  = 200_000
)

// A pending batch of atime updates (basically a FindMissingBlobsRequest).
// Stored as a struct so digest keys can be stored in a set for de-duping.
type DigestBatch struct {
	mu             sync.Mutex
	InstanceName   string
	Digests        map[digest.Key]struct{} // stored as a set for de-duping.
	DigestFunction repb.DigestFunction_Value
}

func (u *DigestBatch) set(key digest.Key) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Digests[key] = struct{}{}
}

func (u *DigestBatch) contains(key digest.Key) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, ok := u.Digests[key]
	return ok
}

func (u *DigestBatch) copy() *DigestBatch {
	db2 := DigestBatch{
		InstanceName:   u.InstanceName,
		DigestFunction: u.DigestFunction,
		Digests:        make(map[digest.Key]struct{}, len(u.Digests)),
	}
	for d := range u.Digests {
		db2.Digests[d] = struct{}{}
	}
	return &db2
}

// The set of pending access time updates to send per group. There should be at
// most one of these per group, but each contains many atimeUpdate{}s, as
// there is one per (groupID, instance-name, digest-function) tuple. However,
// the update will only keep one such atimeUpdate{} and if it gets too big will
// discard additional digests until it's flushed.
type atimeUpdates struct {
	mu          sync.Mutex // protects jwt, updates, and atimeUpdate.digests.
	authHeaders map[string][]string
	updates     []*DigestBatch
	numDigests  int

	maxUpdatesPerGroup int
}

func (u *atimeUpdates) findOrCreatePendingUpdate(instanceName string, digestFunction repb.DigestFunction_Value) *DigestBatch {
	for _, update := range u.updates {
		if update.InstanceName == instanceName && update.DigestFunction == digestFunction {
			return update
		}
	}

	if len(u.updates) >= u.maxUpdatesPerGroup {
		return nil
	}
	pendingUpdate := &DigestBatch{
		InstanceName:   instanceName,
		Digests:        map[digest.Key]struct{}{},
		DigestFunction: digestFunction,
	}
	u.updates = append(u.updates, pendingUpdate)
	return pendingUpdate
}

// An enqueued atime update
type enqueuedOps struct {
	groupID        string
	authHeaders    map[string][]string
	instanceName   string
	digests        []*repb.Digest
	digestFunction repb.DigestFunction_Value
}

type BatchDigestOperator interface {
	// Enqueues atime updates for the provided instanceName, digestFunction,
	// and set of digests provided. Returns true if the updates were
	// successfully enqueued, false if not.
	Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) bool

	// Enqueues atime updates for the provided resource name. Returns true if
	// the update was successfully enqueued, false if not.
	EnqueueByResourceName(ctx context.Context, rn *digest.CASResourceName) bool

	Start(hc interfaces.HealthChecker)

	ForceBatchingForTesting()
	ForceSendUpdatesForTesting(ctx context.Context)
	ForceShutdownForTesting()
}

type BatchDigestOperatorConfig struct {
	QueueSize           int
	BatchInterval       time.Duration
	MaxDigestsPerGroup  int
	MaxDigestsPerUpdate int
	MaxUpdatesPerGroup  int
}

type batchOperator struct {
	authenticator interfaces.Authenticator

	enqueueChan chan *enqueuedOps

	ticker <-chan time.Time
	quit   chan struct{}

	updates sync.Map // pending updates keyed by groupID (string -> *atimeUpdates)

	maxDigestsPerGroup  int
	maxDigestsPerUpdate int
	maxUpdatesPerGroup  int

	remote repb.ContentAddressableStorageClient

	op func(ctx context.Context, u *DigestBatch) error
}

func (u *batchOperator) ForceBatchingForTesting() {
	update := <-u.enqueueChan
	u.batch(update)
}

func (u *batchOperator) ForceSendUpdatesForTesting(ctx context.Context) {
	u.sendUpdates(ctx)
}

func (u *batchOperator) ForceShutdownForTesting() {
	u.quit <- struct{}{}
}

// Start implements BatchDigestOperator.
func (u *batchOperator) Start(hc interfaces.HealthChecker) {
	go u.batcher()
	go u.sender()
	hc.RegisterShutdownFunction(u.shutdown)
}

func New(env environment.Env, f func(ctx context.Context, u *DigestBatch) error, c BatchDigestOperatorConfig) (BatchDigestOperator, error) {
	config := &BatchDigestOperatorConfig{
		QueueSize:           defaultQueueSize,
		BatchInterval:       defaultBatchInterval,
		MaxDigestsPerGroup:  defaultMaxDigestsPerGroup,
		MaxDigestsPerUpdate: defaultMaxDigestsPerUpdate,
		MaxUpdatesPerGroup:  defaultMaxUpdatesPerGroup,
	}
	if c.QueueSize > 0 {
		config.QueueSize = c.QueueSize
	}
	if c.BatchInterval > 0 {
		config.BatchInterval = c.BatchInterval
	}
	if c.MaxDigestsPerGroup > 0 {
		config.MaxDigestsPerGroup = c.MaxDigestsPerGroup
	}
	if c.MaxDigestsPerUpdate > 0 {
		config.MaxDigestsPerUpdate = c.MaxDigestsPerUpdate
	}
	if c.MaxUpdatesPerGroup > 0 {
		config.MaxUpdatesPerGroup = c.MaxUpdatesPerGroup
	}

	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, fmt.Errorf("An Authenticator is required to enable the AtimeUpdater.")
	}
	remote := env.GetContentAddressableStorageClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote CAS client is required to enable the AtimeUpdater.")
	}
	updater := batchOperator{
		authenticator:       authenticator,
		enqueueChan:         make(chan *enqueuedOps, c.QueueSize),
		ticker:              env.GetClock().NewTicker(c.BatchInterval).Chan(),
		quit:                make(chan struct{}, 1),
		maxDigestsPerGroup:  c.MaxDigestsPerGroup,
		maxDigestsPerUpdate: c.MaxDigestsPerUpdate,
		maxUpdatesPerGroup:  c.MaxUpdatesPerGroup,
		remote:              remote,
		op:                  f,
	}
	return &updater, nil
}

func (u *batchOperator) groupID(ctx context.Context) string {
	user, err := u.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (u *batchOperator) Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) bool {
	if len(digests) == 0 {
		return true
	}

	groupID := u.groupID(ctx)
	authHeaders := authutil.GetAuthHeaders(ctx)
	if len(authHeaders) == 0 {
		log.Infof("Dropping batch op due to missing auth headers in context")
		return false
	}
	update := enqueuedOps{
		groupID:        groupID,
		authHeaders:    authHeaders,
		instanceName:   instanceName,
		digests:        digests,
		digestFunction: digestFunction,
	}

	// Try to send the batch to the enqueueChan, where it will be processed by
	// the batcher, but only if this will not block.
	select {
	case u.enqueueChan <- &update:
		return true
	default:
		// XXX
		metrics.RemoteAtimeUpdates.WithLabelValues(
			groupID,
			"dropped_channel_full",
		).Add(float64(len(digests)))
		return false
	}
}

func (u *batchOperator) EnqueueByResourceName(ctx context.Context, rn *digest.CASResourceName) bool {
	return u.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
}

// Runs a loop that consumes updates from enqueueChan and adds them to the
// batches of pending atime updates to be sent to the backend.
func (u *batchOperator) batcher() {
	for {
		select {
		case <-u.quit:
			return
		case update := <-u.enqueueChan:
			u.batch(update)
		}
	}
}

func (u *batchOperator) batch(update *enqueuedOps) {
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
func (u *batchOperator) sender() {
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
func (u *batchOperator) sendUpdates(ctx context.Context) int {
	// Remove updates to send and release the mutex before sending RPCs.
	updatesToSend := map[string]*DigestBatch{}
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
		updates.numDigests -= len(update.Digests)
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
func (u *batchOperator) getUpdate(updates *atimeUpdates) *DigestBatch {
	if len(updates.updates) == 0 {
		return nil
	}

	update := updates.updates[0]
	update.mu.Lock()
	defer update.mu.Unlock()

	// If this update is small enough to send in its entirety, remove it from
	// the queue of updates.
	if len(update.Digests) <= u.maxDigestsPerUpdate {
		updates.updates = updates.updates[1:]
		return update.copy()
	}

	// Otherwise, remove maxDigestsPerUpdate digests from the update, send
	// those, and move this update to the back of the queue for fairness
	// with other updates in the group.
	updates.updates = updates.updates[1:]
	updates.updates = append(updates.updates, update)
	req := DigestBatch{
		InstanceName:   update.InstanceName,
		DigestFunction: update.DigestFunction,
		Digests:        make(map[digest.Key]struct{}, u.maxDigestsPerUpdate),
	}
	i := 0
	for digest := range update.Digests {
		if i >= u.maxDigestsPerUpdate {
			break
		}
		req.Digests[digest] = struct{}{}
		delete(update.Digests, digest)
		i++
	}
	return &req
}

func (u *batchOperator) update(ctx context.Context, groupID string, authHeaders map[string][]string, b *DigestBatch) {

	// Here we are ! Time to do some magic..
	log.CtxDebugf(ctx, "Asynchronously processing %d atime updates for group %s", len(b.Digests), groupID)

	ctx = authutil.AddAuthHeadersToContext(ctx, authHeaders, u.authenticator)
	err := u.op(ctx, b)

	metrics.RemoteAtimeUpdatesSent.WithLabelValues(
		groupID,
		gstatus.Code(err).String(),
	).Inc()
	if err != nil {
		log.CtxWarningf(ctx, "Error sending FindMissingBlobs request to update remote atimes for group %s: %s", groupID, err)
	}
}

func (u *batchOperator) shutdown(ctx context.Context) error {
	close(u.quit)

	// Make a best-effort attempt to flush pending updates.
	// TODO(iain): we could do something fancier here if necessary, like
	// fire-and-forget these RPCs with a rate-limiter. Let's try this for now.
	for u.sendUpdates(ctx) > 0 {
	}

	return nil
}
