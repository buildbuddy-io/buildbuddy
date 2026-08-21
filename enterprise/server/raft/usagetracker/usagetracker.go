package usagetracker

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"

	"github.com/docker/go-units"
	"github.com/hashicorp/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	partitionUsageDeltaGossipThreshold = flag.Int("cache.raft.partition_usage_delta_bytes_threshold", 100e6, "Gossip partition usage information if it has changed by more than this amount since the last gossip.")
	localSizeUpdatePeriod              = flag.Duration("cache.raft.local_size_update_period", 10*time.Second, "How often we update local size updates.")
	evictionRateLimit                  = flag.Int("cache.raft.eviction_rate_limit", 300, "Maximum number of entries to evict per second (per partition).")
	deleteBufferSize                   = flag.Int("cache.raft.delete_buffer_size", 20, "Buffer up to this many eviction candidates between the index sweep and the delete batcher")
	minEvictionAge                     = flag.Duration("cache.raft.min_eviction_age", 6*time.Hour, "Don't evict anything unless it's been idle for at least this long")
	// The flag name predates the removal of the sampling-based evictor; it is
	// kept so existing configs don't break.
	idleSleepDuration         = flag.Duration("cache.raft.sampler_sleep_duration", 1*time.Second, "How long the eviction loop sleeps when the partition is below its eviction threshold or nothing is old enough to evict. Set to 0 to disable sleeping (intended for tests).")
	evictionBatchSize         = flag.Int("cache.raft.eviction_batch_size", 100, "Buffer this many writes before delete")
	numDeleteWorkers          = flag.Int("cache.raft.num_delete_worker", 4, "Number of deletes in parallel")
	numGCSDeleteWorkers       = flag.Int("cache.raft.num_gcs_delete_worker", 32, "Number of parallel GCS blob deletion workers (per partition).")
	gcsDeleteBufferSize       = flag.Int("cache.raft.gcs_delete_buffer_size", 10000, "Buffer up to this many GCS deletion requests")
	gcsDeleteDrainTimeout     = flag.Duration("cache.raft.gcs_delete_drain_timeout", 10*time.Second, "Max time to spend draining buffered GCS deletes on shutdown.")
	atimeIndexVerifyRateLimit = flag.Int("cache.raft.atime_index_verify_rate_limit", 1000, "Maximum records per second the background atime-index verifier reads (per partition). Non-positive disables the verifier.")
	atimeIndexVerifyInterval  = flag.Duration("cache.raft.atime_index_verify_interval", 1*time.Hour, "How long the atime-index verifier sleeps between full passes over a partition's records.")
)

const (
	// evictionCutoffThreshold is the point above which the cache will be
	// considered to be full and eviction will kick in.
	EvictionCutoffThreshold = .90

	// How often stores will check whether to gossip usage data if it is
	// sufficiently different from the last broadcast.
	storePartitionUsageCheckInterval = 15 * time.Second

	// How often stores can go without broadcasting usage information.
	// Usage data will be gossiped after this time if no updated were triggered
	// based on data changes.
	storePartitionUsageMaxAge = 5 * time.Minute

	evictFlushPeriod     = 10 * time.Second
	metricsRefreshPeriod = 30 * time.Second

	// maxCursorAge bounds how long the eviction sweep can keep running from
	// its resume cursor before a front-to-back sweep is forced. Cold entries
	// can land behind the cursor (snapshot recovery imports, verifier
	// repairs), and a partition that stays over budget would otherwise never
	// revisit them. Must comfortably exceed evictFlushPeriod so a forced
	// front sweep doesn't re-enqueue candidates still buffered in the delete
	// pipeline.
	maxCursorAge = 1 * time.Minute
)

// backfillCommitSizeBytes is how large the atime-index backfill lets its write
// batch grow before committing it: a partition can hold millions of records
// (one small index write each), and pebble panics on batches over ~4GB. Var so
// tests can exercise the chunking cheaply.
var backfillCommitSizeBytes = 4 * 1024 * 1024

// verifyChunkRecords is how many records a verify pass examines per iterator
// before reopening it. The pass is rate-limited and can run for hours on a
// large partition, and an open pebble iterator pins the sstables from when it
// was opened -- retained disk that EstimateDiskUsage (and so the partition
// usage gauge) does not count. At the default verify rate, this keeps each
// iterator around half a minute old. Var so tests can exercise chunk
// boundaries cheaply.
var verifyChunkRecords = 30_000

// atimeIndexVersion identifies the atime-index key encoding. It is stored in
// the backfill marker; incrementing it makes every store wipe and rebuild its
// index on the next startup. The index is derived state, so a rebuild is
// always safe -- bump this whenever the entry encoding changes.
//
// Rollout caution: the marker only records that a backfill completed, not
// that the index has stayed complete since. Rolling back to a pre-index
// binary and re-upgrading leaves records written during the rollback window
// unindexed (and so invisible to eviction), because the marker still
// matches. The background verifier (startAtimeIndexVerifier) detects and
// repairs such records -- watch the repair metric spike -- but only at its
// rate limit, taking up to a full verify interval plus a pass. If the
// rollback window was large, delete the marker keys (or bump this version)
// instead: the full-speed backfill rebuild runs before eviction starts.
const atimeIndexVersion = byte(1)

type Tracker struct {
	gossipManager interfaces.GossipService
	node          *rfpb.NodeDescriptor
	partitions    []disk.Partition
	sender        *sender.Sender
	clock         clockwork.Clock
	dbGetter      pebble.Leaser

	mu            sync.Mutex
	byPartition   map[string]*partitionUsage
	lastBroadcast map[string]*sgpb.PartitionMetadata

	eg                                 *errgroup.Group
	egCancel                           context.CancelFunc
	partitionUsageDeltaGossipThreshold int
}

type nodePartitionUsage struct {
	sizeBytes  int64
	lastUpdate time.Time
}

// evictionCandidate is a coldest-first eviction candidate produced by the
// atime-index sweep.
type evictionCandidate struct {
	keyBytes        []byte
	storageMetadata *sgpb.StorageMetadata
	sizeBytes       int64
	atime           time.Time
}

type metricSet struct {
	cachePartitionSizeBytes     prometheus.Gauge
	cachePartitionCapacityBytes prometheus.Gauge

	gcsDeleteDropped         prometheus.Counter
	cacheEvictionAgeMsec     prometheus.Observer
	cacheLastEvictionAgeUsec prometheus.Gauge
	cacheNumEvictions        prometheus.Counter
	cacheBytesEvicted        prometheus.Counter
	atimeIndexRepairs        prometheus.Counter
	atimeIndexOrphansDropped prometheus.Counter
	atimeIndexSweepSeek      prometheus.Observer

	evictionGCSChanSize prometheus.Gauge
}

type partitionUsage struct {
	part disk.Partition

	dbGetter pebble.Leaser
	sender   *sender.Sender
	clock    clockwork.Clock

	mu sync.RWMutex
	// Global view of usage, keyed by Node Host ID.
	nodes map[string]*nodePartitionUsage

	deletes    chan *evictionCandidate
	gcsDeletes chan *sgpb.StorageMetadata_GCSMetadata

	eg       *errgroup.Group
	egCancel context.CancelFunc

	// gcsDeleteEg runs the GCS-delete worker pool under its own lifecycle so
	// the producers (above) can be stopped first while the workers stay alive
	// to drain buffered deletes on shutdown. See drainGCSDeletes.
	gcsDeleteEg     *errgroup.Group
	gcsDeleteCancel context.CancelFunc

	sizeBytes int64

	evictionRateLimit     int
	idleSleepDuration     time.Duration
	minEvictionAge        time.Duration
	localSizeUpdatePeriod time.Duration
	evictionBatchSize     int
	numDeleteWorkers      int
	numGCSDeleteWorkers   int
	indexVerifyRateLimit  int
	indexVerifyInterval   time.Duration
	fileStorer            filestore.Store

	// Closed once the one-shot atime-index backfill has run (successfully or
	// not); gates the background index verifier.
	backfillDone chan struct{}

	metrics metricSet
}

func (pu *partitionUsage) localSizeBytes() int64 {
	db, err := pu.dbGetter.DB()
	if err != nil {
		log.Warningf("unable to get local size bytes for partition %q: %s", pu.part.ID, err)
		return 0
	}
	defer db.Close()
	start, end := keys.Range([]byte(pu.partitionKeyPrefix() + "/"))
	sizeBytes, err := db.EstimateDiskUsage(start, end)
	if err != nil {
		log.Warningf("unable to get local size bytes for partition %q: %s", pu.part.ID, err)
		return 0
	}
	// The atime index consumes the partition's disk budget too; count it so
	// usage (and therefore eviction pressure) reflects the true footprint.
	idxStart, idxEnd := keys.AtimeIndexPartitionRange(pu.part.ID)
	idxSizeBytes, err := db.EstimateDiskUsage(idxStart, idxEnd)
	if err != nil {
		log.Warningf("unable to get atime index size bytes for partition %q: %s", pu.part.ID, err)
		return 0
	}
	return int64(sizeBytes + idxSizeBytes)
}

func (pu *partitionUsage) updateLocalSizeBytes(ctx context.Context) {
	ticker := pu.clock.NewTicker(pu.localSizeUpdatePeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			sizeBytes := pu.localSizeBytes()
			pu.mu.RLock()
			pu.sizeBytes = sizeBytes
			pu.mu.RUnlock()
			pu.metrics.cachePartitionSizeBytes.Set(float64(sizeBytes))
			pu.metrics.cachePartitionCapacityBytes.Set(float64(pu.part.MaxSizeBytes))
		}
	}
}

func (pu *partitionUsage) GlobalSizeBytes() int64 {
	pu.mu.RLock()
	defer pu.mu.RUnlock()
	sizeBytes := int64(0)
	for _, nu := range pu.nodes {
		sizeBytes += nu.sizeBytes
	}
	return sizeBytes
}

func (pu *partitionUsage) RemoteUpdate(nhid string, update *sgpb.PartitionMetadata) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	n, ok := pu.nodes[nhid]
	if !ok {
		n = &nodePartitionUsage{}
		pu.nodes[nhid] = n
	}
	n.lastUpdate = time.Now()
	n.sizeBytes = update.GetSizeBytes()
}

func (pu *partitionUsage) partitionKeyPrefix() string {
	return filestore.PartitionDirectoryPrefix + pu.part.ID
}

func (pu *partitionUsage) sendDeleteRequests(ctx context.Context, keys []*sender.KeyMeta) {
	if len(keys) == 0 {
		return
	}
	start := pu.clock.Now()
	defer metrics.RaftBatchDeleteDurationUsec.Observe(float64(pu.clock.Since(start).Microseconds()))

	// Eviction delete is replay-safe: a duplicate retry after the entry is gone
	// still returns success, so this path does not need sender-owned sessions.
	//
	// TODO(vanja): duplicate deletes are indistinguishable from real
	// evictions here, double-counting eviction metrics, the speculative
	// usage decrement and GCS deletes. A not_found field on DeleteResponse
	// (default false, for rollout safety) would let us tell them apart.
	rsps, err := pu.sender.RunMultiKey(ctx, keys, func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (any, error) {
		batch := rbuilder.NewBatchBuilder()
		for _, k := range keys {
			candidate, ok := k.Meta.(*evictionCandidate)
			if !ok {
				return nil, errors.New("meta not type of *evictionCandidate")
			}
			batch.Add(&rfpb.DeleteRequest{
				Key:        k.Key,
				MatchAtime: candidate.atime.UnixMicro(),
			})
		}
		batchCmd, err := batch.ToProto()
		if err != nil {
			return nil, fmt.Errorf("could not construct delete req proto: %s", err)
		}
		rsp, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchCmd,
		})
		if err != nil {
			return nil, err
		}
		parsed := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		res := make([]*evictionCandidate, 0)
		errCount := 0
		var lastErr error
		for i, k := range keys {
			_, lastErr = parsed.DeleteResponse(i)
			if lastErr == nil {
				res = append(res, k.Meta.(*evictionCandidate))
			} else {
				errCount++
			}
		}
		if errCount > 0 {
			return res, fmt.Errorf("failed to evict %d keys in partition %s, last error: %s", errCount, pu.part.ID, lastErr)
		}
		return res, nil
	})
	if err != nil {
		metrics.RaftEvictionErrorCount.Inc()
		log.Warning(err.Error())
	}
	for _, rsp := range rsps {
		res, ok := rsp.([]*evictionCandidate)
		if !ok {
			alert.UnexpectedEvent("raft_unexpected_delete_rsp", "response not type of *evictionCandidate")
			continue
		}

		pu.updateEvictionMetrics(res)

		for _, s := range res {
			if gcsMD := s.storageMetadata.GetGcsMetadata(); gcsMD != nil {
				select {
				case pu.gcsDeletes <- gcsMD:
				default:
					pu.metrics.gcsDeleteDropped.Inc()
					log.Warningf("GCS deletion queue full, dropping delete request for blob %s", gcsMD.GetBlobName())
				}
			}
		}
	}
}

// TODO(vanja): remove the deletes channel and this batcher goroutine
// (approxlru leftovers): sweepIndex could batch directly, dispatch batches
// via a size-limited errgroup, and wait for them at sweep exits. Same
// throughput, but no candidates in flight across sweeps, making the other
// two eviction TODOs moot.
func (pu *partitionUsage) processEviction(ctx context.Context) {
	batches := make(chan []*sender.KeyMeta, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(batches)
		// sendBatch hands a batch to the dispatcher, but bails out on ctx.Done
		// so the batcher stops promptly on shutdown instead of blocking until
		// pu.deletes drains. Returns false if ctx was cancelled (return then).
		sendBatch := func(b []*sender.KeyMeta) bool {
			select {
			case batches <- b:
				return true
			case <-ctx.Done():
				return false
			}
		}
		var batch []*sender.KeyMeta
		timer := time.NewTimer(evictFlushPeriod)
		for {
			select {
			case <-ctx.Done():
				return
			case candidate := <-pu.deletes:
				batch = append(batch, &sender.KeyMeta{
					Key:  candidate.keyBytes,
					Meta: candidate,
				})
				if len(batch) >= pu.evictionBatchSize {
					if !sendBatch(batch) {
						return
					}
					batch = nil
					timer.Reset(evictFlushPeriod)
				}
			case <-timer.C:
				if !sendBatch(batch) {
					return
				}
				batch = nil
			}
		}
	})
	wg.Go(func() {
		sem := semaphore.NewWeighted(int64(pu.numDeleteWorkers))
		// inner tracks the in-flight sendDeleteRequests goroutines — the only
		// senders to pu.gcsDeletes. Wait for them, then close gcsDeletes so the
		// GCS workers drain the remainder and exit.
		var inner sync.WaitGroup
		defer func() {
			inner.Wait()
			close(pu.gcsDeletes)
		}()
		for batch := range batches {
			if err := sem.Acquire(ctx, 1); err != nil {
				// Context cancelled; the batcher has already (or will) close
				// batches, so stop launching new deletes and return.
				return
			}
			inner.Go(func() {
				defer sem.Release(1)
				pu.sendDeleteRequests(ctx, batch)
			})
		}
	})
	// Block until the batcher, the dispatcher, and every in-flight
	// sendDeleteRequests goroutine have finished. By then the dispatcher has
	// closed gcsDeletes (above), so the GCS workers can drain the remainder and
	// exit; Stop just waits for that (see drainGCSDeletes).
	wg.Wait()
}

func (pu *partitionUsage) processGCSDeletions(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case gcsMD, ok := <-pu.gcsDeletes:
			if !ok {
				// Channel closed and drained on shutdown.
				return
			}
			err := pu.fileStorer.DeleteStoredBlob(ctx, gcsMD)
			metrics.RaftGCSEvictionCount.With(prometheus.Labels{
				metrics.PartitionID:              pu.part.ID,
				metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
			}).Inc()
			if err != nil {
				log.Warningf("failed to delete blob %q: %s", gcsMD.GetBlobName(), err)
			}
		}
	}
}

// drainGCSDeletes waits for the GCS-delete workers to flush the buffered
// deletes, bounded by shutdownCtx (the shared drain budget). gcsDeletes is
// closed by processEviction once the producers stop, so the workers drain
// what's buffered and exit; here we wait for that, then cancel — releasing
// their context on success, or force-stopping them (abandoning whatever is
// left) when the budget runs out first.
func (pu *partitionUsage) drainGCSDeletes(shutdownCtx context.Context) {
	eg := pu.gcsDeleteEg
	if eg == nil {
		return
	}
	pu.gcsDeleteEg = nil // idempotent: a second Stop() is a no-op
	start := pu.clock.Now()
	if waitErrgroup(shutdownCtx, eg) {
		log.Infof("partition %q: drained GCS deletes in %s", pu.part.ID, pu.clock.Since(start))
	} else {
		log.Warningf("partition %q: GCS delete drain hit shutdown deadline after %s, %d deletes abandoned", pu.part.ID, pu.clock.Since(start), len(pu.gcsDeletes))
	}
	pu.gcsDeleteCancel()
}

func (pu *partitionUsage) startEviction(ctx context.Context) {
	if err := pu.backfillAtimeIndex(ctx); err != nil && ctx.Err() == nil {
		// Run eviction anyway: already-indexed records still get evicted, and
		// the missing marker means the backfill is retried on next startup.
		// Alert because until then, unindexed records are invisible to
		// eviction (the verifier heals them only slowly, at its rate limit).
		alert.UnexpectedEvent("raft_atime_index_backfill_failed", "partition %q: atime index backfill failed: %s", pu.part.ID, err)
	}
	if pu.backfillDone != nil {
		close(pu.backfillDone)
	}
	// evictionLoop returns non-nil only on storage errors (db acquisition or
	// iterator failures), which may be transient. A dead loop means the
	// partition grows without bound, so alert and retry with backoff rather
	// than giving up for the process lifetime.
	for ctx.Err() == nil {
		err := pu.evictionLoop(ctx)
		if err == nil {
			return // context done
		}
		alert.UnexpectedEvent("raft_eviction_loop_failed", "partition %q: eviction loop failed (will retry): %s", pu.part.ID, err)
		select {
		case <-ctx.Done():
			return
		case <-pu.clock.After(10 * time.Second):
		}
	}
}

// backfillAtimeIndex builds the partition's atime index from already-stored
// records the first time a store starts with indexing enabled; from then on
// the replica apply path maintains the index and the completion marker skips
// this scan. The backfill is idempotent and doesn't coordinate with concurrent
// applies: an entry that goes stale mid-scan is an orphan the eviction sweep
// cleans up.
func (pu *partitionUsage) backfillAtimeIndex(ctx context.Context) error {
	db, err := pu.dbGetter.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	markerKey := keys.AtimeIndexBackfillMarkerKey(pu.part.ID)
	if val, closer, err := db.Get(markerKey); err == nil {
		upToDate := len(val) == 1 && val[0] == atimeIndexVersion
		closer.Close()
		if upToDate {
			return nil
		}
		log.Infof("partition %q: atime index version changed; rebuilding", pu.part.ID)
	} else if err != pebble.ErrNotFound {
		return err
	}

	// Drop any existing entries first: on a version bump they may use an old
	// encoding the sweep can't reliably interpret, and on a crashed prior
	// backfill this keeps the rebuild exact rather than additive.
	idxStart, idxEnd := keys.AtimeIndexPartitionRange(pu.part.ID)
	if err := db.DeleteRange(idxStart, idxEnd, pebble.NoSync); err != nil {
		return err
	}

	start, end := keys.Range([]byte(pu.partitionKeyPrefix() + "/"))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return err
	}
	defer iter.Close()

	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()

	wb := db.NewBatch()
	defer wb.Close()
	count := 0
	startTime := time.Now()
	lastLog := startTime
	for iter.First(); iter.Valid(); iter.Next() {
		// Don't write the marker on cancellation: rescanning on the next
		// startup is cheap and idempotent.
		if err := ctx.Err(); err != nil {
			return err
		}
		fileMetadata.ResetVT()
		if err := fileMetadata.UnmarshalVT(iter.Value()); err != nil {
			log.Warningf("atime index backfill: skipping non-FileMetadata key %q: %s", iter.Key(), err)
			continue
		}
		if err := wb.Set(keys.AtimeIndexKey(pu.part.ID, fileMetadata.GetLastAccessUsec(), iter.Key()), nil, nil); err != nil {
			return err
		}
		count++
		if wb.Len() >= backfillCommitSizeBytes {
			if err := wb.Commit(pebble.NoSync); err != nil {
				return err
			}
			wb.Reset()
		}
		if time.Since(lastLog) > 10*time.Second {
			log.Infof("partition %q: atime index backfill: %d entries so far", pu.part.ID, count)
			lastLog = time.Now()
		}
	}
	// A mid-scan iterator error (I/O, block checksum) makes Valid() return
	// false just like normal exhaustion. Writing the marker after a truncated
	// scan would permanently strand the unscanned records outside the index,
	// so fail -- without the marker, the next startup rescans.
	if err := iter.Error(); err != nil {
		return err
	}
	if err := wb.Commit(pebble.NoSync); err != nil {
		return err
	}
	if err := db.Set(markerKey, []byte{atimeIndexVersion}, pebble.Sync); err != nil {
		return err
	}
	log.Infof("partition %q: atime index backfill complete: %d entries in %s", pu.part.ID, count, time.Since(startTime))
	return nil
}

// startAtimeIndexVerifier runs a slow, continuous reverse check of the atime
// index: it scans every record in the partition and verifies that the record's
// index entry exists, repairing (and counting) any that are missing.
//
// "Every record has an index entry" is the invariant eviction rests on: a
// record without one is invisible to the sweep and can never be evicted. The
// apply path maintains it batch-atomically in steady state, so the repair
// metric staying at zero is continuous evidence that it holds -- a sustained
// nonzero rate is the alert. The verifier also heals the known narrow gaps
// (a crash inside the sweep's orphan-drop/restore window, records written by
// a pre-index binary during a rollback) and any bug class not yet imagined.
//
// It needs no synchronization with concurrent applies: every race it can lose
// only produces a stale extra entry (an orphan the sweep drops lazily), never
// a missing one. See verifyAtimeIndexPass for the re-check sequence that keeps
// the repair count meaningful.
func (pu *partitionUsage) startAtimeIndexVerifier(ctx context.Context) {
	if pu.indexVerifyRateLimit <= 0 {
		return
	}
	// Wait for the one-shot backfill so a first pass over a fresh store doesn't
	// redo the backfill's work one rate-limited record at a time. (If the
	// backfill failed, the verifier doubles as a slow retry; the repairs it
	// then reports are genuinely missing entries.)
	select {
	case <-ctx.Done():
		return
	case <-pu.backfillDone:
	}
	limiter := rate.NewLimiter(rate.Limit(pu.indexVerifyRateLimit), 1)
	for ctx.Err() == nil {
		// The pass logs its own summary (and each repair individually).
		if _, err := pu.verifyAtimeIndexPass(ctx, limiter); err != nil {
			log.Errorf("partition %q: atime index verify pass failed (will retry): %s", pu.part.ID, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-pu.clock.After(pu.indexVerifyInterval):
		}
	}
}

// verifyAtimeIndexPass makes one full pass over the partition's records,
// point-reading each record's exact index key. A miss goes through two more
// checks before it counts as a violation, because the pass runs unsynchronized
// with applies:
//
//  1. Re-read the record live. The scan iterator sees a snapshot, so the
//     record's atime may have legitimately moved on (atomically deleting the
//     entry we just looked for), or the record may be gone; either way the
//     apply that changed it maintained the index.
//  2. Re-check the entry. An apply that writes historical atimes (snapshot
//     recovery) may have written the record and entry between the first
//     entry check and the record re-read.
//
// Only then is the entry genuinely missing: repair, count, log. The repair
// write is safe against any concurrent apply -- if the record moved again in
// the meantime, the write creates an orphan, which the sweep drops.
//
// The pass iterates in chunks of verifyChunkRecords, reopening its iterator
// between them so no iterator lives for hours (see verifyChunkRecords). The
// verification logic is chunk-boundary-agnostic: it never trusted a single
// snapshot anyway, since every miss is re-checked against live reads.
func (pu *partitionUsage) verifyAtimeIndexPass(ctx context.Context, limiter *rate.Limiter) (int, error) {
	db, err := pu.dbGetter.DB()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()

	lower, upper := keys.Range([]byte(pu.partitionKeyPrefix() + "/"))
	repaired := 0
	scanned := 0
	startTime := pu.clock.Now()
	for lower != nil && ctx.Err() == nil {
		nextLower, n, r, err := pu.verifyAtimeIndexChunk(ctx, db, limiter, lower, upper, fileMetadata)
		scanned += n
		repaired += r
		if err != nil {
			return repaired, err
		}
		lower = nextLower
	}
	log.Infof("partition %q: atime index verify pass: %d records scanned, %d entries repaired in %s", pu.part.ID, scanned, repaired, pu.clock.Since(startTime))
	return repaired, nil
}

// verifyAtimeIndexChunk verifies up to verifyChunkRecords records in
// [lower, upper), returning the lower bound to continue from (nil when the
// span is exhausted or the context was cancelled) and the scanned/repaired
// counts.
func (pu *partitionUsage) verifyAtimeIndexChunk(ctx context.Context, db pebble.IPebbleDB, limiter *rate.Limiter, lower, upper []byte, fileMetadata *sgpb.FileMetadata) (nextLower []byte, scanned, repaired int, err error) {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, 0, 0, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if scanned >= verifyChunkRecords {
			// Resume at this (unexamined) record with a fresh iterator.
			return bytes.Clone(iter.Key()), scanned, repaired, nil
		}
		scanned++
		if err := limiter.Wait(ctx); err != nil {
			return nil, scanned, repaired, nil // context cancelled
		}
		fileMetadata.ResetVT()
		if err := fileMetadata.UnmarshalVT(iter.Value()); err != nil {
			log.Warningf("atime index verify: skipping non-FileMetadata key %q: %s", iter.Key(), err)
			continue
		}
		fileKey := iter.Key()
		atimeUsec := fileMetadata.GetLastAccessUsec()
		entryKey := keys.AtimeIndexKey(pu.part.ID, atimeUsec, fileKey)
		if ok, err := hasKey(db, entryKey); err != nil {
			log.Warningf("atime index verify: cannot check entry %q: %s", entryKey, err)
			continue
		} else if ok {
			continue
		}
		// Check 1: the record, live.
		fileMetadata.ResetVT()
		err := pebble.GetProto(db, fileKey, fileMetadata)
		if status.IsNotFoundError(err) || (err == nil && fileMetadata.GetLastAccessUsec() != atimeUsec) {
			continue
		}
		if err != nil {
			log.Warningf("atime index verify: cannot re-read record %q: %s", fileKey, err)
			continue
		}
		// Check 2: the entry, again.
		if ok, err := hasKey(db, entryKey); err != nil {
			log.Warningf("atime index verify: cannot re-check entry %q: %s", entryKey, err)
			continue
		} else if ok {
			continue
		}
		if err := db.Set(entryKey, nil, pebble.NoSync); err != nil {
			log.Warningf("atime index verify: failed to repair entry %q: %s", entryKey, err)
			continue
		}
		repaired++
		pu.metrics.atimeIndexRepairs.Inc()
		log.Warningf("partition %q: repaired missing atime index entry for record %q (atime %d)", pu.part.ID, fileKey, atimeUsec)
	}
	if err := iter.Error(); err != nil {
		return nil, scanned, repaired, err
	}
	return nil, scanned, repaired, nil
}

// hasKey reports whether key exists in db.
func hasKey(db pebble.IPebbleDB, key []byte) (bool, error) {
	_, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// idleSleep pauses the eviction loop for the configured sleep duration to
// avoid busy-looping when there is nothing to evict. It returns false if the
// context was cancelled.
func (pu *partitionUsage) idleSleep(ctx context.Context) bool {
	if pu.idleSleepDuration <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-pu.clock.After(pu.idleSleepDuration):
		return true
	}
}

// maxAllowedSizeBytes is the size above which the partition is considered
// full and eviction kicks in.
func (pu *partitionUsage) maxAllowedSizeBytes() int64 {
	return int64(EvictionCutoffThreshold * float64(pu.part.MaxSizeBytes))
}

// evictionLoop keeps the partition below its eviction threshold. While the
// partition is over budget it sweeps the atime index in ascending atime
// order, enqueueing delete candidates; otherwise it sleeps. Sweeps continue
// from a resume cursor so candidates whose deletes are still in flight aren't
// re-enqueued; the cursor resets to the index front once the partition stays
// below the threshold across a sleep (by then the delete pipeline has had
// time to drain), or after maxCursorAge without a front sweep.
func (pu *partitionUsage) evictionLoop(ctx context.Context) error {
	db, err := pu.dbGetter.DB()
	if err != nil {
		log.Warningf("partition %q: eviction loop failed to get db: %s", pu.part.ID, err)
		return err
	}
	defer db.Close()

	evictionRate := rate.Limit(pu.evictionRateLimit)
	if pu.evictionRateLimit <= 0 {
		// A zero-rate limiter blocks Wait forever, silently disabling
		// eviction; treat non-positive as unlimited instead.
		evictionRate = rate.Inf
	}
	limiter := rate.NewLimiter(evictionRate, 1)

	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()

	var resumeKey []byte
	lastFrontSweep := pu.clock.Now()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if pu.GlobalSizeBytes() <= pu.maxAllowedSizeBytes() {
			if !pu.idleSleep(ctx) {
				return nil
			}
			// Discard the cursor only if the partition is still below the
			// threshold after sleeping: flapping back over within one sleep
			// usually means the last sweep's deletes are still buffered (the
			// batcher can hold a partial batch for up to evictFlushPeriod),
			// and resuming instead of restarting at the front avoids
			// re-enqueueing them. TODO(vanja): removing the batcher (see
			// processEviction) makes this heuristic unnecessary.
			if pu.GlobalSizeBytes() <= pu.maxAllowedSizeBytes() {
				resumeKey = nil
			}
			continue
		}
		if resumeKey != nil && pu.clock.Since(lastFrontSweep) > maxCursorAge {
			resumeKey = nil
		}
		if resumeKey == nil {
			lastFrontSweep = pu.clock.Now()
		}
		nextResume, exhausted, err := pu.sweepIndex(ctx, db, limiter, resumeKey, fileMetadata)
		if err != nil {
			return err
		}
		resumeKey = nextResume
		if exhausted {
			// Nothing actionable ahead of the cursor: everything currently
			// eligible has been enqueued and the entries ahead (if any) are
			// younger than min_eviction_age. Sleep while they age in; keeping
			// the cursor prevents the next sweep from re-enqueueing
			// candidates whose deletes are still in flight.
			if !pu.idleSleep(ctx) {
				return nil
			}
		}
	}
}

// sweepIndex walks the partition's atime index from resumeKey (or the coldest
// entry), enqueueing eviction candidates until the partition drops below its
// eviction threshold, entries become younger than min_eviction_age, or the
// index is exhausted. Orphaned entries -- the record is gone or its atime
// moved on (crash windows, cleared or removed ranges) -- are deleted in place;
// the index is node-local derived state, so those deletes don't go through
// raft.
//
// Returns the key to resume the next sweep from -- the sweep's stop position
// on every exit path, so consecutive sweeps never re-visit (and re-enqueue)
// candidates already handed to the delete pipeline -- and whether the sweep
// exhausted the actionable entries (hit the age boundary or the index end),
// in which case the caller should sleep before sweeping again.
func (pu *partitionUsage) sweepIndex(ctx context.Context, db pebble.IPebbleDB, limiter *rate.Limiter, resumeKey []byte, fileMetadata *sgpb.FileMetadata) ([]byte, bool, error) {
	start, end := keys.AtimeIndexPartitionRange(pu.part.ID)
	if resumeKey != nil && bytes.Compare(resumeKey, start) > 0 && bytes.Compare(resumeKey, end) < 0 {
		start = resumeKey
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()

	// Time the initial seek: evictions leave point tombstones at the index's
	// cold front, and until compaction drops them this seek pays to skip them
	// all. A growing tail on this metric is the signal that tombstone cleanup
	// is falling behind the eviction rate.
	seekStart := pu.clock.Now()
	valid := iter.First()
	pu.metrics.atimeIndexSweepSeek.Observe(float64(pu.clock.Since(seekStart).Microseconds()))

	var lastKey []byte
	for ; valid; valid = iter.Next() {
		lastKey = append(lastKey[:0], iter.Key()...)
		if ctx.Err() != nil {
			return nil, false, nil
		}
		if pu.GlobalSizeBytes() <= pu.maxAllowedSizeBytes() {
			// Below budget; resume at the current (unprocessed) entry if the
			// budget check turns out to have been optimistic.
			return bytes.Clone(iter.Key()), false, nil
		}
		_, atimeUsec, fileKey, err := keys.ParseAtimeIndexKey(iter.Key())
		if err != nil {
			log.Warningf("dropping unparseable atime index entry %q: %s", iter.Key(), err)
			if err := db.Delete(iter.Key(), pebble.NoSync); err != nil {
				log.Warningf("failed to drop atime index entry %q: %s", iter.Key(), err)
			}
			continue
		}
		atime := time.UnixMicro(atimeUsec)
		if pu.clock.Since(atime) < pu.minEvictionAge {
			// Entries are atime-ordered: everything from here on is younger.
			// Resume at this entry -- it becomes eligible as time advances.
			return bytes.Clone(iter.Key()), true, nil
		}
		// Verify the entry against the stored record; drop orphans in place.
		fileMetadata.ResetVT()
		err = pebble.GetProto(db, fileKey, fileMetadata)
		if err != nil && !status.IsNotFoundError(err) {
			log.Warningf("cannot check eviction candidate, skipping %q: %s", fileKey, err)
			continue
		}
		if status.IsNotFoundError(err) || fileMetadata.GetLastAccessUsec() != atimeUsec {
			entryKey := bytes.Clone(iter.Key())
			if err := db.Delete(entryKey, pebble.NoSync); err != nil {
				log.Warningf("failed to drop orphaned atime index entry %q: %s", entryKey, err)
				continue
			}
			// This delete is unsynchronized with the apply path, so it can
			// race a concurrent re-write of the record at this exact atime
			// and remove the entry that write just created. Fresh writes
			// can't collide (they stamp a new atime, landing at a different
			// index key); the writer that recreates entries at historical
			// atimes is snapshot recovery, replaying records into a span
			// clearRangeData just wiped -- which is also why the sweep sees
			// orphans there. Re-check and restore: the reverse mistake --
			// restoring an entry for a record deleted inside this window --
			// only creates an orphan, which is safe. If we fail to restore the
			// index, verifyAtimeIndexPass will fix it.
			restored := false
			fileMetadata.ResetVT()
			if err := pebble.GetProto(db, fileKey, fileMetadata); err == nil && fileMetadata.GetLastAccessUsec() == atimeUsec {
				if err := db.Set(entryKey, nil, pebble.NoSync); err != nil {
					log.Warningf("failed to restore atime index entry %q: %s", entryKey, err)
				} else {
					restored = true
				}
			}
			if !restored {
				pu.metrics.atimeIndexOrphansDropped.Inc()
			}
			continue
		}
		if err := limiter.Wait(ctx); err != nil {
			return nil, false, nil // context cancelled
		}
		candidate := &evictionCandidate{
			keyBytes: bytes.Clone(fileKey),
			// Clone: fileMetadata is a pooled message reset on the next loop
			// iteration, but the candidate outlives the sweep in the delete
			// pipeline (and losing GcsMetadata there would orphan the blob).
			storageMetadata: fileMetadata.GetStorageMetadata().CloneVT(),
			// Include the index entry's bytes: LocalSizeBytes counts the
			// index, so the speculative post-eviction decrement should too.
			sizeBytes: int64(proto.Size(fileMetadata)) + int64(len(fileKey)) + int64(len(iter.Key())),
			atime:     atime,
		}
		select {
		case pu.deletes <- candidate:
		case <-ctx.Done():
			return nil, false, nil
		}
	}
	if err := iter.Error(); err != nil {
		return nil, false, err
	}
	if lastKey == nil {
		// The swept span was empty.
		return nil, true, nil
	}
	// Index exhausted: resume just past the last entry, where entries with
	// newer atimes will appear.
	return append(lastKey, 0), true, nil
}

func (pu *partitionUsage) updateEvictionMetrics(candidates []*evictionCandidate) error {
	sizeBytes := float64(0)
	for _, c := range candidates {
		age := time.Since(c.atime)
		sizeBytes += float64(c.sizeBytes)
		pu.metrics.cacheEvictionAgeMsec.Observe(float64(age.Milliseconds()))
		pu.metrics.cacheLastEvictionAgeUsec.Set(float64(age.Microseconds()))
	}
	pu.metrics.cacheNumEvictions.Add(float64(len(candidates)))
	pu.metrics.cacheBytesEvicted.Add(sizeBytes)

	pu.mu.Lock()
	defer pu.mu.Unlock()
	localSizeBytes := float64(pu.sizeBytes)

	// Assume eviction on all stores is happening at a similar rate as on the
	// current store and update the usage information speculatively since we
	// don't know when we'll receive the next usage update from remote stores.
	// When we do receive updates from other stores they will overwrite our
	// speculative numbers.
	for _, npu := range pu.nodes {
		npu.sizeBytes -= int64(sizeBytes * float64(npu.sizeBytes) / localSizeBytes)
		if npu.sizeBytes < 0 {
			npu.sizeBytes = 0
		}
	}

	return nil
}

func (pu *partitionUsage) updateMetrics() {
	pu.mu.Lock()
	defer pu.mu.Unlock()

	pu.metrics.evictionGCSChanSize.Set(float64(len(pu.gcsDeletes)))
}

func New(sender *sender.Sender, dbGetter pebble.Leaser, gossipManager interfaces.GossipService, node *rfpb.NodeDescriptor, partitions []disk.Partition, clock clockwork.Clock, fileStorer filestore.Store) (*Tracker, error) {
	ut := &Tracker{
		gossipManager: gossipManager,
		node:          node,
		partitions:    partitions,
		byPartition:   make(map[string]*partitionUsage),
		clock:         clock,
		dbGetter:      dbGetter,
		lastBroadcast: make(map[string]*sgpb.PartitionMetadata),

		partitionUsageDeltaGossipThreshold: *partitionUsageDeltaGossipThreshold,
	}

	for _, p := range partitions {
		if p.SoftDeleted {
			continue
		}
		lbls := prometheus.Labels{metrics.PartitionID: p.ID, metrics.CacheNameLabel: constants.CacheName}
		partitionLabel := prometheus.Labels{metrics.PartitionID: p.ID}
		metricSet := metricSet{
			cachePartitionSizeBytes:     metrics.DiskCachePartitionSizeBytes.With(lbls),
			cachePartitionCapacityBytes: metrics.DiskCachePartitionCapacityBytes.With(lbls),
			gcsDeleteDropped:            metrics.RaftGCSDeleteDropped.With(partitionLabel),
			cacheEvictionAgeMsec:        metrics.DiskCacheEvictionAgeMsec.With(lbls),
			cacheLastEvictionAgeUsec:    metrics.DiskCacheLastEvictionAgeUsec.With(lbls),
			cacheNumEvictions:           metrics.DiskCacheNumEvictions.With(lbls),
			cacheBytesEvicted:           metrics.DiskCacheBytesEvicted.With(lbls),
			evictionGCSChanSize:         metrics.RaftEvictionGCSChanSize.With(partitionLabel),
			atimeIndexRepairs:           metrics.RaftAtimeIndexMissingEntriesRepaired.With(partitionLabel),
			atimeIndexOrphansDropped:    metrics.RaftAtimeIndexOrphansDropped.With(partitionLabel),
			atimeIndexSweepSeek:         metrics.RaftAtimeIndexSweepSeekDurationUsec.With(partitionLabel),
		}
		u := &partitionUsage{
			part:                  p,
			sender:                sender,
			clock:                 clock,
			nodes:                 make(map[string]*nodePartitionUsage),
			dbGetter:              dbGetter,
			deletes:               make(chan *evictionCandidate, *deleteBufferSize),
			gcsDeletes:            make(chan *sgpb.StorageMetadata_GCSMetadata, *gcsDeleteBufferSize),
			evictionRateLimit:     *evictionRateLimit,
			idleSleepDuration:     *idleSleepDuration,
			minEvictionAge:        *minEvictionAge,
			localSizeUpdatePeriod: *localSizeUpdatePeriod,
			evictionBatchSize:     *evictionBatchSize,
			numDeleteWorkers:      *numDeleteWorkers,
			numGCSDeleteWorkers:   *numGCSDeleteWorkers,
			indexVerifyRateLimit:  *atimeIndexVerifyRateLimit,
			indexVerifyInterval:   *atimeIndexVerifyInterval,
			fileStorer:            fileStorer,
			backfillDone:          make(chan struct{}),
			metrics:               metricSet,
		}
		ut.byPartition[p.ID] = u
	}

	gossipManager.AddListener(ut)
	return ut, nil
}

// cleanupStaleAtimeIndexState removes the atime-index region and backfill
// marker of partitions that are no longer configured. The partition-removal
// flow (soft delete, then remove from the config) deletes records and their
// index entries via RemoveData, but that derives entry keys from the records
// it reads: orphaned entries -- which accumulate unswept while a partition is
// soft-deleted -- and the backfill marker survive it, and no sweep ever runs
// for the partition again. The index is node-local derived state, so wiping
// it directly is safe and needs no coordination.
func cleanupStaleAtimeIndexState(db pebble.IPebbleDB, configured set.Set[string]) error {
	stale := make(set.Set[string])

	// Enumerate distinct partition IDs in the index region, seeking from each
	// partition's range end to the next (one seek per partition, no scan).
	start, end := keys.Range(keys.AtimeIndexPrefix)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return err
	}
	for valid := iter.First(); valid; {
		part, _, _, err := keys.ParseAtimeIndexKey(iter.Key())
		if err != nil {
			log.Warningf("atime index cleanup: skipping unparseable key %q: %s", iter.Key(), err)
			valid = iter.Next()
			continue
		}
		if !configured.Contains(part) {
			stale.Add(part)
		}
		_, partEnd := keys.AtimeIndexPartitionRange(part)
		valid = iter.SeekGE(partEnd)
	}
	err = iter.Error()
	iter.Close()
	if err != nil {
		return err
	}

	// Backfill markers can exist without entries (empty partitions), so
	// enumerate them separately.
	markerPrefix := keys.AtimeIndexBackfillMarkerKey("")
	mStart, mEnd := keys.Range(markerPrefix)
	iter, err = db.NewIter(&pebble.IterOptions{LowerBound: mStart, UpperBound: mEnd})
	if err != nil {
		return err
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		part := string(bytes.TrimPrefix(iter.Key(), markerPrefix))
		if !configured.Contains(part) {
			stale.Add(part)
		}
	}
	err = iter.Error()
	iter.Close()
	if err != nil {
		return err
	}

	for part := range stale {
		idxStart, idxEnd := keys.AtimeIndexPartitionRange(part)
		if err := db.DeleteRange(idxStart, idxEnd, pebble.NoSync); err != nil {
			return err
		}
		if err := db.Delete(keys.AtimeIndexBackfillMarkerKey(part), pebble.NoSync); err != nil {
			return err
		}
		log.Infof("removed atime index state of unconfigured partition %q", part)
	}
	return nil
}

func (ut *Tracker) Start() {
	if db, err := ut.dbGetter.DB(); err != nil {
		log.Errorf("atime index cleanup: failed to get db: %s", err)
	} else {
		configured := make(set.Set[string], len(ut.partitions))
		for _, p := range ut.partitions {
			// Soft-deleted partitions count as configured: their index state
			// is removed by the hard-delete flow (RemoveData) and, for the
			// residue, by this cleanup once they leave the config entirely.
			configured.Add(p.ID)
		}
		if err := cleanupStaleAtimeIndexState(db, configured); err != nil {
			// Non-fatal: the leftovers are inert and the cleanup reruns on
			// the next startup.
			log.Errorf("failed to clean up stale atime index state: %s", err)
		}
		db.Close()
	}

	for _, pu := range ut.byPartition {
		ctx, cancelFunc := context.WithCancel(context.Background())
		pu.egCancel = cancelFunc
		eg, gctx := errgroup.WithContext(ctx)
		pu.eg = eg
		pu.eg.Go(func() error {
			pu.startEviction(gctx)
			return nil
		})
		pu.eg.Go(func() error {
			pu.startAtimeIndexVerifier(gctx)
			return nil
		})
		pu.eg.Go(func() error {
			pu.processEviction(gctx)
			return nil
		})
		// Run the GCS-delete workers under a separate errgroup/context so that
		// on shutdown we can stop the producers first and keep the workers
		// alive to drain the buffer (see drainGCSDeletes).
		gcsCtx, gcsCancel := context.WithCancel(context.Background())
		pu.gcsDeleteCancel = gcsCancel
		gcsEg, gcsGctx := errgroup.WithContext(gcsCtx)
		pu.gcsDeleteEg = gcsEg
		numGCSWorkers := pu.numGCSDeleteWorkers
		if numGCSWorkers < 1 {
			numGCSWorkers = 1
		}
		for i := 0; i < numGCSWorkers; i++ {
			pu.gcsDeleteEg.Go(func() error {
				pu.processGCSDeletions(gcsGctx)
				return nil
			})
		}
		pu.eg.Go(func() error {
			pu.updateLocalSizeBytes(gctx)
			return nil
		})
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	ut.egCancel = cancelFunc

	eg, gctx := errgroup.WithContext(ctx)
	ut.eg = eg

	eg.Go(func() error {
		ut.broadcastLoop(gctx)
		return nil
	})
	eg.Go(func() error {
		ut.refreshMetrics(gctx)
		return nil
	})
}

// Stop shuts the tracker down. It honors ctx (the server's bounded shutdown
// grace) so it always returns within that budget: it drains buffered GCS
// deletes when there is time, and degrades to a clean abandon when there isn't.
func (ut *Tracker) Stop(ctx context.Context) {
	// A single drain budget for all partitions, measured from Stop and capped
	// by the shutdown grace.
	drainCtx, cancel := context.WithTimeout(ctx, *gcsDeleteDrainTimeout)
	defer cancel()

	if ut.egCancel != nil {
		ut.egCancel()
		waitErrgroup(ctx, ut.eg)
	}
	// Shut partitions down concurrently. Per partition, one goroutine stops the
	// producers (which closes gcsDeletes) while another drains the GCS workers;
	// both are bounded by drainCtx so Stop always returns within budget.
	var wg sync.WaitGroup
	for _, p := range ut.byPartition {
		wg.Go(func() {
			if p.egCancel != nil {
				p.egCancel()
				waitErrgroup(drainCtx, p.eg)
			}
		})
		wg.Go(func() {
			p.drainGCSDeletes(drainCtx)
		})
	}
	wg.Wait()
}

// waitErrgroup blocks until eg's goroutines finish or ctx is done. It returns
// true only if the errgroup finished first. The detached waiter goroutine
// leaks only if ctx wins and the group never finishes, which is fine during
// shutdown.
func waitErrgroup(ctx context.Context, eg *errgroup.Group) bool {
	done := make(chan struct{})
	go func() {
		eg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (ut *Tracker) Statusz(ctx context.Context) string {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	var buf strings.Builder
	buf.WriteString("Partitions:\n")
	for _, p := range ut.partitions {
		buf.WriteString(fmt.Sprintf("\t%s\n", p.ID))
		u, ok := ut.byPartition[p.ID]
		if !ok {
			buf.WriteString("\t\tno data\n")
			continue
		}

		globalSizeBytes := u.GlobalSizeBytes()
		percentFull := (float64(globalSizeBytes) / float64(p.MaxSizeBytes)) * 100

		buf.WriteString(fmt.Sprintf("\t\tCapacity: %s / %s (%2.2f%% full)\n", units.BytesSize(float64(globalSizeBytes)), units.BytesSize(float64(p.MaxSizeBytes)), percentFull))

		// Show nodes in a consistent order so that they don't jump around when
		// refreshing the statusz page.
		var nhids []string
		for nhid := range u.nodes {
			nhids = append(nhids, nhid)
		}
		sort.Strings(nhids)
		buf.WriteString("\t\tGlobal Usage:\n")
		for _, nhid := range nhids {
			nu, ok := u.nodes[nhid]
			if !ok {
				continue
			}
			buf.WriteString(fmt.Sprintf("\t\t\t%s: %s (last updated: %s)\n", nhid, units.BytesSize(float64(nu.sizeBytes)), nu.lastUpdate))
		}
	}
	return buf.String()
}

func (ut *Tracker) OnEvent(updateType serf.EventType, event serf.Event) {
	if updateType != serf.EventUser {
		return
	}
	userEvent, ok := event.(serf.UserEvent)
	if !ok {
		return
	}
	if userEvent.Name != constants.NodePartitionUsageEvent {
		return
	}

	nu := &rfpb.NodePartitionUsage{}
	if err := proto.Unmarshal(userEvent.Payload, nu); err != nil {
		return
	}

	ut.RemoteUpdate(nu)
}

// RemoteUpdate processes a usage update broadcast by Raft stores.
// Note that this also includes data broadcast by the local store.
func (ut *Tracker) RemoteUpdate(usage *rfpb.NodePartitionUsage) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	nhid := usage.GetNode().GetNhid()
	for _, pu := range usage.GetPartitionUsage() {
		lpu, ok := ut.byPartition[pu.GetPartitionId()]
		if !ok {
			log.Warningf("unknown partition %q", pu.GetPartitionId())
			continue
		}
		lpu.RemoteUpdate(nhid, pu)
	}
}

func (ut *Tracker) refreshMetrics(ctx context.Context) {
	partitionUsages := make([]*partitionUsage, 0, len(ut.byPartition))
	ut.mu.Lock()
	for _, pu := range ut.byPartition {
		partitionUsages = append(partitionUsages, pu)
	}

	ut.mu.Unlock()

	ticker := ut.clock.NewTicker(metricsRefreshPeriod)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			for _, pu := range partitionUsages {
				pu.updateMetrics()
			}
		}
	}
}

func (ut *Tracker) computeUsage() *rfpb.NodePartitionUsage {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	nu := &rfpb.NodePartitionUsage{
		Node: ut.node,
	}

	for _, p := range ut.partitions {
		up := &sgpb.PartitionMetadata{
			PartitionId: p.ID,
		}
		if u, ok := ut.byPartition[p.ID]; ok {
			u.mu.Lock()
			up.SizeBytes = u.sizeBytes
			u.mu.Unlock()
		}
		nu.PartitionUsage = append(nu.PartitionUsage, up)
	}
	return nu
}

func (ut *Tracker) broadcastLoop(ctx context.Context) {
	idleTimer := ut.clock.NewTimer(storePartitionUsageMaxAge)
	ticker := ut.clock.NewTicker(storePartitionUsageCheckInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			broadcasted, err := ut.broadcast(false /*=force*/)
			if err != nil {
				log.Warningf("could not gossip node partition usage info: %s", err)
			}
			if broadcasted {
				idleTimer.Reset(storePartitionUsageMaxAge)
			}
		case <-idleTimer.Chan():
			if _, err := ut.broadcast(true /*=force*/); err != nil {
				log.Warningf("could not gossip node partition usage info: %s", err)
			}
			idleTimer.Reset(storePartitionUsageMaxAge)
		}
	}
}

func (ut *Tracker) broadcast(force bool) (bool, error) {
	usage := ut.computeUsage()

	// If not forced, check whether there's enough changes to force a broadcast.
	if !force {
		significantChange := false
		ut.mu.Lock()
		for _, u := range usage.GetPartitionUsage() {
			lb, ok := ut.lastBroadcast[u.GetPartitionId()]
			if !ok || math.Abs(float64(u.GetSizeBytes()-lb.GetSizeBytes())) > float64(ut.partitionUsageDeltaGossipThreshold) {
				significantChange = true
				break
			}
		}
		ut.mu.Unlock()
		if !significantChange {
			return false, nil
		}
	}

	buf, err := proto.Marshal(usage)
	if err != nil {
		return false, err
	}

	if err := ut.gossipManager.SendUserEvent(constants.NodePartitionUsageEvent, buf, false /*coalesce*/); err != nil {
		return false, err
	}
	log.Debugf("usagetracker sent node partition usage event (force=%t) %+v", force, usage)

	ut.mu.Lock()
	defer ut.mu.Unlock()
	for _, u := range usage.GetPartitionUsage() {
		ut.lastBroadcast[u.GetPartitionId()] = u
	}

	return true, nil
}

type watermark struct {
	timestamp time.Time
	sizeBytes int64
}

func (ut *Tracker) TestingWaitForGC(ctx context.Context) error {
	lastSize := make(map[string]watermark)
	for {
		ut.mu.Lock()
		partitionUsage := ut.byPartition
		ut.mu.Unlock()

		done := 0
		for _, pu := range partitionUsage {
			db, err := pu.dbGetter.DB()
			if err != nil {
				log.Warningf("failed to get db: %s", db)
				break
			}
			db.Flush()
			start, end := keys.Range([]byte(pu.partitionKeyPrefix() + "/"))
			db.Compact(start, end, false /*parallelize*/)
			// LocalSizeBytes counts the atime index too; compact it so the
			// estimate reflects settled sizes rather than tombstones.
			idxStart, idxEnd := keys.AtimeIndexPartitionRange(pu.part.ID)
			db.Compact(idxStart, idxEnd, false /*parallelize*/)
			db.Close()
			totalSizeBytes := pu.localSizeBytes()
			// Tests run a single node with a possibly-frozen fake clock, so
			// gossip may never refresh the global usage view; inject the local
			// size directly so the eviction loop sees it.
			pu.RemoteUpdate(ut.node.GetNhid(), &sgpb.PartitionMetadata{
				PartitionId: pu.part.ID,
				SizeBytes:   totalSizeBytes,
			})
			maxAllowedSize := pu.maxAllowedSizeBytes()
			if lastSize[pu.part.ID].sizeBytes != totalSizeBytes {
				lastSize[pu.part.ID] = watermark{
					timestamp: time.Now(),
					sizeBytes: totalSizeBytes,
				}
			} else {
				if size := lastSize[pu.part.ID].sizeBytes; size > 0 && time.Since(lastSize[pu.part.ID].timestamp) > 3*time.Second {
					log.Warningf("LRU not making progress: size is %s, maxAllowedSize is %s", units.HumanSize(float64(size)), units.HumanSize(float64(maxAllowedSize)))
				}
			}
			if totalSizeBytes <= maxAllowedSize {
				done += 1
			}
		}
		if done == len(partitionUsage) {
			break
		}
		select {
		case <-ctx.Done():
			return status.CanceledError("context canceled waiting for GC")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}
