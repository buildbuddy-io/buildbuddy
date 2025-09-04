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
)

const (
	defaultQueueSize          = 1_000_000
	defaultBatchInterval      = 10 * time.Second
	defaultMaxDigestsPerBatch = 10_000
	defaultMaxBatchesPerGroup = 50
	defaultMaxDigestsPerGroup = 200_000
)

type DigestBatch struct {
	InstanceName   string
	Digests        []*repb.Digest
	DigestFunction repb.DigestFunction_Value
}

// A pending batch of digests to be operated on.
// Stored as a struct so digest keys can be stored in a set for de-duping.
type pendingDigestBatch struct {
	mu             sync.Mutex
	instanceName   string
	digests        map[digest.Key]struct{} // stored as a set for de-duping.
	digestFunction repb.DigestFunction_Value
}

func (u *pendingDigestBatch) set(key digest.Key) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.digests[key] = struct{}{}
}

func (u *pendingDigestBatch) contains(key digest.Key) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, ok := u.digests[key]
	return ok
}

func (u *pendingDigestBatch) finalize() *DigestBatch {
	db2 := DigestBatch{
		InstanceName:   u.instanceName,
		DigestFunction: u.digestFunction,
		Digests:        make([]*repb.Digest, len(u.digests)),
	}
	i := 0
	for d := range u.digests {
		db2.Digests[i] = d.ToDigest()
		i++
	}
	return &db2
}

// The set of pending digests batches for a single group. There should be at
// most one of these per group, but each contains many pendingDigestBatches, as
// there is one per (groupID, instance-name, digest-function) tuple. However,
// each pendingDigestBatch will only hold MaxDigestsPerBatch: if it gets too big,
// we will discard additional digests until it's flushed.
type groupDigestBatches struct {
	mu          sync.Mutex // protects jwt, batches, and pendingDigestBatch.digests.
	authHeaders map[string][]string
	batches     []*pendingDigestBatch
	numDigests  int

	MaxBatchesPerGroup int
}

func (u *groupDigestBatches) findOrCreatePendingBatch(instanceName string, digestFunction repb.DigestFunction_Value) *pendingDigestBatch {
	for _, batch := range u.batches {
		if batch.instanceName == instanceName && batch.digestFunction == digestFunction {
			return batch
		}
	}

	if len(u.batches) >= u.MaxBatchesPerGroup {
		return nil
	}
	newPendingBatch := &pendingDigestBatch{
		instanceName:   instanceName,
		digests:        map[digest.Key]struct{}{},
		digestFunction: digestFunction,
	}
	u.batches = append(u.batches, newPendingBatch)
	return newPendingBatch
}

// An enqueued set of digest that haven't been
// added to a pending batch yet.
type enqueuedDigests struct {
	groupID        string
	authHeaders    map[string][]string
	instanceName   string
	digests        []*repb.Digest
	digestFunction repb.DigestFunction_Value
}

type BatchDigestOperator interface {
	// Enqueues digests for the provided instanceName, digestFunction,
	// and set of digests provided. Returns true if the digests were
	// successfully enqueued, false if not.
	Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) bool

	// Enqueues the digest for the provided resource name. Returns true if
	// the digest was successfully enqueued, false if not.
	EnqueueByResourceName(ctx context.Context, rn *digest.CASResourceName) bool

	Start(hc interfaces.HealthChecker)

	ForceBatchingForTesting()
	ForceFlushBatchesForTesting(ctx context.Context)
	ForceShutdownForTesting()
}

type BatchDigestOperatorConfig struct {
	QueueSize          int
	BatchInterval      time.Duration
	MaxDigestsPerGroup int
	MaxDigestsPerBatch int
	MaxBatchesPerGroup int
}

type batchOperator struct {
	authenticator interfaces.Authenticator

	enqueueChan chan *enqueuedDigests

	ticker <-chan time.Time
	quit   chan struct{}

	batchesByGroupID sync.Map // (string -> *groupDigestBatches)

	name               string
	maxDigestsPerGroup int
	maxDigestsPerBatch int
	maxBatchesPerGroup int

	op func(ctx context.Context, groupID string, u *DigestBatch) error
}

func (u *batchOperator) ForceBatchingForTesting() {
	digests := <-u.enqueueChan
	u.batch(digests)
}

func (u *batchOperator) ForceFlushBatchesForTesting(ctx context.Context) {
	u.flushBatches(ctx)
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

func New(env environment.Env, name string, f func(ctx context.Context, groupID string, u *DigestBatch) error, c BatchDigestOperatorConfig) (BatchDigestOperator, error) {
	config := &BatchDigestOperatorConfig{
		QueueSize:          defaultQueueSize,
		BatchInterval:      defaultBatchInterval,
		MaxDigestsPerGroup: defaultMaxDigestsPerGroup,
		MaxDigestsPerBatch: defaultMaxDigestsPerBatch,
		MaxBatchesPerGroup: defaultMaxBatchesPerGroup,
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
	if c.MaxDigestsPerBatch > 0 {
		config.MaxDigestsPerBatch = c.MaxDigestsPerBatch
	}
	if c.MaxBatchesPerGroup > 0 {
		config.MaxBatchesPerGroup = c.MaxBatchesPerGroup
	}

	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, fmt.Errorf("An Authenticator is required to create a BatchDigestOperator.")
	}
	operator := batchOperator{
		name:               name,
		authenticator:      authenticator,
		enqueueChan:        make(chan *enqueuedDigests, c.QueueSize),
		ticker:             env.GetClock().NewTicker(c.BatchInterval).Chan(),
		quit:               make(chan struct{}, 1),
		maxDigestsPerGroup: c.MaxDigestsPerGroup,
		maxDigestsPerBatch: c.MaxDigestsPerBatch,
		maxBatchesPerGroup: c.MaxBatchesPerGroup,
		op:                 f,
	}
	return &operator, nil
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
		log.Infof("[%s] Dropping batch op due to missing auth headers in context", u.name)
		return false
	}
	toEnqueue := enqueuedDigests{
		groupID:        groupID,
		authHeaders:    authHeaders,
		instanceName:   instanceName,
		digests:        digests,
		digestFunction: digestFunction,
	}

	// Try to send the digests to the enqueueChan, where it will be processed by
	// the batcher, but only if this will not block.
	select {
	case u.enqueueChan <- &toEnqueue:
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

// Runs a loop that consumes digests from enqueueChan and adds them to the
// corresponding pending batches, creating new batches as necessary.
func (u *batchOperator) batcher() {
	for {
		select {
		case <-u.quit:
			return
		case digests := <-u.enqueueChan:
			u.batch(digests)
		}
	}
}

func (u *batchOperator) batch(eqd *enqueuedDigests) {
	groupID := eqd.groupID
	rawBatchesForGroup, _ := u.batchesByGroupID.LoadOrStore(groupID, &groupDigestBatches{MaxBatchesPerGroup: u.maxBatchesPerGroup})
	batchesForGroup, ok := rawBatchesForGroup.(*groupDigestBatches)
	if !ok {
		alert.UnexpectedEvent("batch-operator-unexpected-key-type", "[%s] groupDigestBatches contains value with invalid type %T", u.name, rawBatchesForGroup)
		return
	}

	// Uniqueify the incoming digests. Note this wrecks the order of the
	// input digests. Too bad. Keep track of the counts for metrics below.
	keys := map[digest.Key]int{}
	for _, digestProto := range eqd.digests {
		keys[digest.NewKey(digestProto)]++
	}

	// Always save the most recent auth headers for a group.
	batchesForGroup.mu.Lock()
	batchesForGroup.authHeaders = eqd.authHeaders

	// First, find the batch that the new digests can be merged into, or create
	// a new one if one doesn't exist.
	pendingBatch := batchesForGroup.findOrCreatePendingBatch(eqd.instanceName, eqd.digestFunction)
	batchesForGroup.mu.Unlock()
	if pendingBatch == nil {
		log.Infof("[%s] Too many pending batches for group %s, dropping %d pending digests", u.name, groupID, len(keys))
		metrics.RemoteAtimeUpdates.WithLabelValues(
			groupID,
			"dropped_too_many_batches",
		).Add(float64(len(eqd.digests)))
		return
	}

	// Now merge the new digests into the batch.
	enqueued := 0
	duplicate := 0
	dropped := 0
	for key, count := range keys {
		if pendingBatch.contains(key) {
			duplicate += count
			continue
		}
		batchesForGroup.mu.Lock()
		if batchesForGroup.numDigests+1 > u.maxDigestsPerGroup {
			batchesForGroup.mu.Unlock()
			dropped = len(eqd.digests) - enqueued - duplicate
			log.Warningf("[%s] maxDigestsPerGroup exceeded for group %s, dropping %d digests", u.name, groupID, dropped)
			break
		}
		batchesForGroup.numDigests++
		batchesForGroup.mu.Unlock()
		pendingBatch.set(key)
		enqueued++
		duplicate += count - 1
	}

	if enqueued+duplicate+dropped != len(eqd.digests) {
		log.Debugf("[%s] Metrics don't add up. incoming digests: %d, added: %d, duplicates: %d, dropped: %d", u.name, len(eqd.digests), enqueued, duplicate, dropped)
	}

	// XXX: rename / modify.
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

// Starts the loop that flushes batches to the caaller-provided operation.
func (u *batchOperator) sender() {
	for {
		select {
		case <-u.quit:
			return
		case <-u.ticker:
			u.flushBatches(context.Background())
		}
	}
}

// Sends one batch per group. Because the batches are stored in a per-group
// array, the batchOperator will round-robin across instance-names and digest
// functions for each group. We could change that approach to send the biggest
// batch per group each time or something, but that could starve lesser used
// instance-names.
func (u *batchOperator) flushBatches(ctx context.Context) int {
	// Remove batches to flush and release the mutex before sending RPCs.
	batchesToFlush := map[string]*DigestBatch{}
	batchesFlushed := 0
	authHeaders := map[string]map[string][]string{}
	u.batchesByGroupID.Range(func(key, value interface{}) bool {
		groupID, ok := key.(string)
		if !ok {
			alert.UnexpectedEvent("batch-operator-unexpected-key-type", "[%s] batchesByGroupID contains key with unexpected type. actual type: %T", u.name, key)
			return true
		}
		batches, ok := value.(*groupDigestBatches)
		if !ok {
			alert.UnexpectedEvent("batch-operator-unexpected-value-type", "[%s] batchesByGroupID contains value with unexpected type. actual type: %T", u.name, value)
			return true
		}
		batches.mu.Lock()

		batch := u.getBatch(batches)
		if batch == nil {
			batches.mu.Unlock()
			return true
		}
		batchesToFlush[groupID] = batch
		authHeaders[groupID] = batches.authHeaders
		batches.numDigests -= len(batch.Digests)
		batches.mu.Unlock()
		return true
	})

	for groupID, batch := range batchesToFlush {
		batchesFlushed++
		u.dispatch(ctx, groupID, authHeaders[groupID], batch)
	}
	return batchesFlushed
}

// Returns a DigestBatch representing the first batch in the
// provided list of pending batches, potentially splitting it if the first batch
// in the queue is too large. Also reorders the pending batches for inter-group
// fairness.
func (u *batchOperator) getBatch(groupBatches *groupDigestBatches) *DigestBatch {
	if len(groupBatches.batches) == 0 {
		return nil
	}

	pendingBatch := groupBatches.batches[0]
	pendingBatch.mu.Lock()
	defer pendingBatch.mu.Unlock()

	// If this batch is small enough to send in its entirety, remove it from
	// the queue of batches.
	if len(pendingBatch.digests) <= u.maxDigestsPerBatch {
		groupBatches.batches = groupBatches.batches[1:]
		return pendingBatch.finalize()
	}

	// Otherwise, remove MaxDigestsPerBatch digests from the batch, send
	// those, and move this batch to the back of the queue for fairness
	// with other batches in the group.
	groupBatches.batches = groupBatches.batches[1:]
	groupBatches.batches = append(groupBatches.batches, pendingBatch)
	batch := DigestBatch{
		InstanceName:   pendingBatch.instanceName,
		DigestFunction: pendingBatch.digestFunction,
		Digests:        make([]*repb.Digest, u.maxDigestsPerBatch),
	}
	i := 0
	for digest := range pendingBatch.digests {
		if i >= u.maxDigestsPerBatch {
			break
		}
		batch.Digests[i] = digest.ToDigest()
		delete(pendingBatch.digests, digest)
		i++
	}
	return &batch
}

func (u *batchOperator) dispatch(ctx context.Context, groupID string, authHeaders map[string][]string, b *DigestBatch) {
	log.CtxDebugf(ctx, "Asynchronously processing %d batches for group %s", len(b.Digests), groupID)

	ctx = authutil.AddAuthHeadersToContext(ctx, authHeaders, u.authenticator)
	err := u.op(ctx, groupID, b)

	// XXX: Change metric (already copied over to atime updater).
	// metrics.RemoteAtimeUpdatesSent.WithLabelValues(
	//	groupID,
	//	gstatus.Code(err).String(),
	//).Inc()
	if err != nil {
		log.CtxWarningf(ctx, "[%s] Error processing batch for group %s: %s", u.name, groupID, err)
	}
}

func (u *batchOperator) shutdown(ctx context.Context) error {
	close(u.quit)

	// Make a best-effort attempt to flush pending batches.
	// TODO(iain): we could do something fancier here if necessary, like
	// fire-and-forget these RPCs with a rate-limiter. Let's try this for now.
	for u.flushBatches(ctx) > 0 {
	}

	return nil
}
