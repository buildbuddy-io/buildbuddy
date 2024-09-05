package atime_updater

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// The maximum gRPC message size is 4MB. Each digest proto is
	// 64 bytes (hash) + 8 bytes (size) + 2 bytes (tags) = 74 bytes. The
	// remaining fields in FindMissingBlobsRequest should be small (1kb?), so
	// we can conceivably fit 4MB / 74bytes = 14.1k of these. Cap it at 10k for
	// now and we can increase it (or increase the update frequency) if we drop
	// a lot of updates. Note this also affects memory usage of the proxy!
	atimeUpdaterMaxDigestsPerUpdate = flag.Int("cache_proxy.remote_atime_max_digests_per_update", 10*1000, "The maximum number of blob digests to send in a single request for updating blob access times in the remote cache.")
	atimeUpdaterMaxUpdatesPerGroup  = flag.Int("cache_proxy.remote_atime_max_updates_per_group", 25, "The maximum number of FindMissingBlobRequests to accumulate per group for updating blob access times in the remote cache.")
	atimeUpdaterBatchUpdateInterval = flag.Duration("cache_proxy.remote_atime_update_interval", 30*time.Second, "The time interval to wait between sending access time updates to the remote cache.")
	atimeUpdaterRpcTimeout          = flag.Duration("cache_proxy.remote_atime_rpc_timeout", 1*time.Second, "RPC timeout to use when updating blob access times in the remote cache.")
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
	mu      sync.Mutex // protects jwt, updates, and atimeUpdate.digests.
	jwt     string
	updates []*atimeUpdate
}

type atimeUpdater struct {
	authenticator interfaces.Authenticator

	mu      sync.Mutex               // protects updates
	updates map[string]*atimeUpdates // pending updates keyed by groupID.

	maxDigestsPerUpdate int
	maxUpdatesPerGroup  int
	updateInterval      time.Duration
	rpcTimeout          time.Duration

	remote repb.ContentAddressableStorageClient
}

type Opts struct {
	TickerForTesting <-chan time.Time
}

func Register(env *real_environment.RealEnv, opts Opts) error {
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
		updates:             make(map[string]*atimeUpdates),
		maxDigestsPerUpdate: *atimeUpdaterMaxDigestsPerUpdate,
		maxUpdatesPerGroup:  *atimeUpdaterMaxUpdatesPerGroup,
		updateInterval:      *atimeUpdaterBatchUpdateInterval,
		rpcTimeout:          *atimeUpdaterRpcTimeout,
		remote:              remote,
	}
	go updater.start(opts)
	env.SetAtimeUpdater(&updater)
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
	jwt := u.authenticator.TrustedJWTFromAuthContext(ctx)
	u.mu.Lock()
	updates, ok := u.updates[groupID]
	if !ok {
		updates = &atimeUpdates{}
		u.updates[groupID] = updates
	}
	u.mu.Unlock()

	// Uniqueify the incoming digests. Note this wrecks the order of the
	// requested updates. Too bad.
	keys := map[digest.Key]struct{}{}
	for _, digestProto := range digests {
		keys[digest.NewKey(digestProto)] = struct{}{}
	}

	// TODO(iain): this locking may be a bit overzealous. Hopefully not though.
	updates.mu.Lock()
	defer updates.mu.Unlock()

	// Always use the most recent JWT for a group for remote atime updates.
	updates.jwt = jwt

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
			log.CtxInfof(ctx, "Too many pending FindMissingBlobsRequests for updating remote atime for group %s, dropping some pending atime updates", groupID)
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
	for key := range keys {
		if len(pendingUpdate.digests) >= u.maxDigestsPerUpdate {
			log.CtxInfof(ctx, "FindMissingBlobsRequest for updating remote atime for group %s too large, dropping some pending atime updates", groupID)
			break
		}
		pendingUpdate.digests[key] = struct{}{}
	}
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

func (u *atimeUpdater) start(opts Opts) {
	tickerChan := opts.TickerForTesting
	if tickerChan == nil {
		ticker := time.NewTicker(u.updateInterval)
		tickerChan = ticker.C
		defer ticker.Stop()
	}

	for {
		// Send one update per group every tick. Because the updates are stored
		// in a per-group array, they will round-robin across instance-names
		// and digest functions for each group. We could change that approach
		// to send the biggest update per group each time or something, but
		// that could starve lesser used instance-names.
		<-tickerChan

		// Remove updates to send and release the mutex before sending RPCs.
		updatesToSend := map[string]*atimeUpdate{}
		jwts := map[string]string{}
		u.mu.Lock()
		for groupID, updates := range u.updates {
			updates.mu.Lock()
			if len(updates.updates) == 0 {
				updates.mu.Unlock()
				continue
			}
			updatesToSend[groupID] = updates.updates[0]
			jwts[groupID] = updates.jwt
			updates.updates = updates.updates[1:]
			updates.mu.Unlock()
		}
		u.mu.Unlock()

		for groupID, update := range updatesToSend {
			ctx, cancel := context.WithTimeout(context.Background(), u.rpcTimeout)
			u.update(ctx, groupID, jwts[groupID], update)
			cancel()
		}
	}
}

func (u *atimeUpdater) update(ctx context.Context, groupID string, jwt string, update *atimeUpdate) {
	log.CtxDebugf(ctx, "Asynchronously processing %d atime updates for group %s", len(update.digests), groupID)
	ctx = u.authenticator.AuthContextFromTrustedJWT(ctx, jwt)
	if _, err := u.remote.FindMissingBlobs(ctx, update.toProto()); err != nil {
		log.CtxWarningf(ctx, "Error sending FindMissingBlobs request to update remote atimes for group %s: %s", groupID, err)
	}
}
