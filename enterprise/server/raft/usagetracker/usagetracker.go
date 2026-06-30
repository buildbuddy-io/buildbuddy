package usagetracker

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"

	"github.com/docker/go-units"
	"github.com/hashicorp/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	partitionUsageDeltaGossipThreshold = flag.Int("cache.raft.partition_usage_delta_bytes_threshold", 100e6, "Gossip partition usage information if it has changed by more than this amount since the last gossip.")
	localSizeUpdatePeriod              = flag.Duration("cache.raft.local_size_update_period", 10*time.Second, "How often we update local size updates.")
	samplesPerEviction                 = flag.Int("cache.raft.samples_per_eviction", 20, "How many records to sample on each eviction")
	samplesPerBatch                    = flag.Int("cache.raft.samples_per_batch", 10000, "How many keys we read forward every time we get a random key.")
	samplePoolSize                     = flag.Int("cache.raft.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")
	sampleBufferSize                   = flag.Int("cache.raft.sample_buffer_size", 100, "Buffer up to this many samples for eviction sampling")
	deletesPerEviction                 = flag.Int("cache.raft.deletes_per_eviction", 5, "Maximum number keys to delete in one eviction attempt before resampling.")
	evictionRateLimit                  = flag.Int("cache.raft.eviction_rate_limit", 300, "Maximum number of entries to evict per second (per partition).")
	deleteBufferSize                   = flag.Int("cache.raft.delete_buffer_size", 20, "Buffer up to this many samples for eviction eviction")
	minEvictionAge                     = flag.Duration("cache.raft.min_eviction_age", 6*time.Hour, "Don't evict anything unless it's been idle for at least this long")
	samplerIterRefreshPeriod           = flag.Duration("cache.raft.sampler_iter_refresh_peroid", 5*time.Minute, "How often we refresh iterator in sampler")
	samplerSleepDuration               = flag.Duration("cache.raft.sampler_sleep_duration", 1*time.Second, "How long the eviction sampler sleeps when it cannot find eligible entries to evict. Set to 0 to disable sleeping (intended for tests).")
	evictionBatchSize                  = flag.Int("cache.raft.eviction_batch_size", 100, "Buffer this many writes before delete")
	numDeleteWorkers                   = flag.Int("cache.raft.num_delete_worker", 4, "Number of deletes in parallel")
	numGCSDeleteWorkers                = flag.Int("cache.raft.num_gcs_delete_worker", 32, "Number of parallel GCS blob deletion workers (per partition).")
	gcsDeleteBufferSize                = flag.Int("cache.raft.gcs_delete_buffer_size", 10000, "Buffer up to this many GCS deletion requests")
	gcsDeleteDrainTimeout              = flag.Duration("cache.raft.gcs_delete_drain_timeout", 10*time.Second, "Max time to spend draining buffered GCS deletes on shutdown.")
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

	samplerSleepThreshold = float64(0.2)
	evictFlushPeriod      = 10 * time.Second
	metricsRefreshPeriod  = 30 * time.Second
)

type Tracker struct {
	gossipManager interfaces.GossipService
	node          *rfpb.NodeDescriptor
	partitions    []disk.Partition
	sender        *sender.Sender
	clock         clockwork.Clock

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

type evictionKey struct {
	bytes           []byte
	storageMetadata *sgpb.StorageMetadata
}

func (k *evictionKey) ID() string {
	return string(k.bytes)
}

func (k *evictionKey) String() string {
	return string(k.bytes)
}

type metricSet struct {
	cachePartitionSizeBytes     prometheus.Gauge
	cachePartitionCapacityBytes prometheus.Gauge

	gcsDeleteDropped         prometheus.Counter
	cacheEvictionAgeMsec     prometheus.Observer
	cacheLastEvictionAgeUsec prometheus.Gauge
	cacheNumEvictions        prometheus.Counter
	cacheBytesEvicted        prometheus.Counter

	evictionSamplesChanSize prometheus.Gauge
	evictionGCSChanSize     prometheus.Gauge
}

type partitionUsage struct {
	part disk.Partition

	dbGetter pebble.Leaser
	sender   *sender.Sender
	clock    clockwork.Clock

	mu  sync.RWMutex
	lru *approxlru.LRU[*evictionKey]
	// Global view of usage, keyed by Node Host ID.
	nodes map[string]*nodePartitionUsage

	samples    chan *approxlru.Sample[*evictionKey]
	deletes    chan *approxlru.Sample[*evictionKey]
	gcsDeletes chan *sgpb.StorageMetadata_GCSMetadata
	rng        *rand.Rand

	eg       *errgroup.Group
	egCancel context.CancelFunc

	// gcsDeleteEg runs the GCS-delete worker pool under its own lifecycle so
	// the producers (above) can be stopped first while the workers stay alive
	// to drain buffered deletes on shutdown. See drainGCSDeletes.
	gcsDeleteEg     *errgroup.Group
	gcsDeleteCancel context.CancelFunc

	sizeBytes int64

	samplesPerBatch          int
	samplerIterRefreshPeriod time.Duration
	samplerSleepDuration     time.Duration
	minEvictionAge           time.Duration
	localSizeUpdatePeriod    time.Duration
	evictionBatchSize        int
	numDeleteWorkers         int
	numGCSDeleteWorkers      int
	fileStorer               filestore.Store

	metrics metricSet
}

func (pu *partitionUsage) LocalSizeBytes() int64 {
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
	return int64(sizeBytes)
}

func (pu *partitionUsage) updateLocalSizeBytes(ctx context.Context) {
	ticker := pu.clock.NewTicker(pu.localSizeUpdatePeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			sizeBytes := pu.LocalSizeBytes()
			pu.mu.RLock()
			pu.sizeBytes = sizeBytes
			pu.mu.RUnlock()
			pu.lru.UpdateLocalSizeBytes(sizeBytes)
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
	rsps, err := pu.sender.RunMultiKey(ctx, keys, func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (any, error) {
		batch := rbuilder.NewBatchBuilder()
		for _, k := range keys {
			sample, ok := k.Meta.(*approxlru.Sample[*evictionKey])
			if !ok {
				return nil, errors.New("meta not type of approxlru.Sample[*evictionKey]")
			}
			batch.Add(&rfpb.DeleteRequest{
				Key:        k.Key,
				MatchAtime: sample.Timestamp.UnixMicro(),
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
		res := make([]*approxlru.Sample[*evictionKey], 0)
		errCount := 0
		var lastErr error
		for i, k := range keys {
			_, lastErr = parsed.DeleteResponse(i)
			if lastErr == nil {
				res = append(res, k.Meta.(*approxlru.Sample[*evictionKey]))
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
		res, ok := rsp.([]*approxlru.Sample[*evictionKey])
		if !ok {
			alert.UnexpectedEvent("raft_unexpected_delete_rsp", "response not type of approxlru.Sample[*evictionKey]")
			continue
		}

		pu.updateEvictionMetrics(res)

		for _, s := range res {
			if gcsMD := s.Key.storageMetadata.GetGcsMetadata(); gcsMD != nil {
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

func (pu *partitionUsage) processEviction(ctx context.Context) {
	batches := make(chan []*sender.KeyMeta, 1)
	// sendBatch hands a batch to the dispatcher, but bails out on ctx.Done so the
	// batcher stops promptly on shutdown instead of blocking until pu.deletes
	// drains. Returns false if ctx was cancelled (caller should return).
	sendBatch := func(b []*sender.KeyMeta) bool {
		select {
		case batches <- b:
			return true
		case <-ctx.Done():
			return false
		}
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(batches)
		var batch []*sender.KeyMeta
		timer := time.NewTimer(evictFlushPeriod)
		for {
			select {
			case <-ctx.Done():
				return
			case sampleToDelete := <-pu.deletes:
				batch = append(batch, &sender.KeyMeta{
					Key:  sampleToDelete.Key.bytes,
					Meta: sampleToDelete,
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
	sem := semaphore.NewWeighted(int64(pu.numDeleteWorkers))
	wg.Go(func() {
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

func (pu *partitionUsage) startSampleGenerator(ctx context.Context) {
	pu.generateSamplesForEviction(ctx)
	close(pu.samples)
}

var digestRunes = []rune("abcdef1234567890")

func (pu *partitionUsage) randomKey(n int) []byte {
	var randKey strings.Builder
	randKey.WriteString(pu.partitionKeyPrefix() + "/")
	for i := 0; i < n; i++ {
		randKey.WriteString(string(digestRunes[rand.Intn(len(digestRunes))]))
	}
	return []byte(randKey.String())
}

// samplerSleep pauses the sampler for the configured sleep duration to avoid
// busy-looping when there is nothing useful to sample. It returns false if the
// context was cancelled.
func (pu *partitionUsage) samplerSleep(ctx context.Context) bool {
	if pu.samplerSleepDuration <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-pu.clock.After(pu.samplerSleepDuration):
		return true
	}
}

func (pu *partitionUsage) generateSamplesForEviction(ctx context.Context) error {
	db, err := pu.dbGetter.DB()
	if err != nil {
		log.Warningf("cannot generate samples for eviction: failed to get db: %s", err)
		return err
	}
	defer db.Close()
	start, end := keys.Range([]byte(pu.partitionKeyPrefix() + "/"))
	iterCreatedAt := time.Now()
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return err
	}
	// We update the iter variable later on, so we need to wrap the Close call
	// in a func to operate on the correct iterator instance.
	defer func() {
		iter.Close()
	}()

	leftInBatch := pu.samplesPerBatch
	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()

	timer := pu.clock.NewTimer(0)
	defer timer.Stop()

	// Files are kept in random order (because they are keyed by digest), so
	// instead of doing a new seek for every random sample we will seek once
	// and just read forward, yielding digests until we've found enough.
	for {
		if ctx.Err() != nil {
			return nil
		}

		// When we started to populate a cache, we cannot find any eligible
		// entries to evict. We will sleep for some time to prevent from
		// constantly generating samples in vain.
		globalSize := pu.GlobalSizeBytes()
		shouldSleep := globalSize <= int64(samplerSleepThreshold*float64(pu.part.MaxSizeBytes))
		if shouldSleep {
			if !pu.samplerSleep(ctx) {
				return nil
			}
		}

		// Refresh the iterator once a while
		if leftInBatch <= 0 || time.Since(iterCreatedAt) > pu.samplerIterRefreshPeriod {
			leftInBatch = pu.samplesPerBatch
			iterCreatedAt = time.Now()
			// This iterator won't be positioned (Valid() will return false),
			// so we will position it below.
			newIter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
			if err != nil {
				return err
			}
			iter.Close()
			iter = newIter
		}
		leftInBatch--
		if !iter.Valid() {
			// This happens when we create a new iterator or exhaust the
			// existing one.
			randomKey := pu.randomKey(64)
			if valid := iter.SeekGE(randomKey); !valid {
				// This is a probabilistic sleep. A partition with no rows on
				// this node will always sleep. A partition with many rows is
				// very unlikely to sleep. This ensures that we don't waste CPU
				// cycles trying to find samples for a partition with no (or
				// few) rows.
				if !pu.samplerSleep(ctx) {
					return nil
				}
				leftInBatch = 0 // Force creating a new iterator
				continue
			}
		}
		var key filestore.PebbleKey
		if _, err := key.FromBytes(iter.Key()); err != nil {
			log.Warningf("cannot generate sample for eviction, skipping: failed to read key: %s", err)
			continue
		}
		fileMetadata.ResetVT() // UnmarshalVT doesn't reset, unlike proto.Unmarshal.
		err = fileMetadata.UnmarshalVT(iter.Value())
		if err != nil {
			log.Warningf("cannot generate sample for eviction, skipping: failed to read proto: %s", err)
			continue
		}

		pu.maybeAddToSampleChan(ctx, iter, fileMetadata, timer)
		iter.Next()
	}
}

func (pu *partitionUsage) maybeAddToSampleChan(ctx context.Context, iter pebble.Iterator, fileMetadata *sgpb.FileMetadata, timer clockwork.Timer) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	age := pu.clock.Since(atime)
	if age < pu.minEvictionAge {
		return
	}
	sizeBytes := int64(proto.Size(fileMetadata)) + int64(len(iter.Key()))

	keyBytes := make([]byte, len(iter.Key()))
	copy(keyBytes, iter.Key())
	sample := &approxlru.Sample[*evictionKey]{
		Key: &evictionKey{
			bytes:           keyBytes,
			storageMetadata: fileMetadata.GetStorageMetadata(),
		},
		SizeBytes: sizeBytes,
		Timestamp: atime,
	}
	timer.Reset(pu.samplerSleepDuration)
	select {
	case pu.samples <- sample:
	case <-ctx.Done():
		return
	case <-timer.Chan():
		// e.samples is full.
	}
}

func (e *partitionUsage) evict(ctx context.Context, sample *approxlru.Sample[*evictionKey]) error {
	e.deletes <- sample
	return nil
}

func (pu *partitionUsage) updateEvictionMetrics(samples []*approxlru.Sample[*evictionKey]) error {
	sizeBytes := float64(0)
	for _, sample := range samples {
		age := time.Since(sample.Timestamp)
		sizeBytes += float64(sample.SizeBytes)
		pu.metrics.cacheEvictionAgeMsec.Observe(float64(age.Milliseconds()))
		pu.metrics.cacheLastEvictionAgeUsec.Set(float64(age.Microseconds()))
	}
	pu.metrics.cacheNumEvictions.Add(float64(len(samples)))
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

func (pu *partitionUsage) sample(ctx context.Context, k int) ([]*approxlru.Sample[*evictionKey], error) {
	samples := make([]*approxlru.Sample[*evictionKey], 0, k)
	for i := 0; i < k; i++ {
		s, ok := <-pu.samples
		if ok {
			samples = append(samples, s)
		}
	}

	return samples, nil
}

func (pu *partitionUsage) updateMetrics() {
	pu.mu.Lock()
	defer pu.mu.Unlock()

	pu.metrics.evictionSamplesChanSize.Set(float64(len(pu.samples)))
	pu.metrics.evictionGCSChanSize.Set(float64(len(pu.gcsDeletes)))
}

func New(sender *sender.Sender, dbGetter pebble.Leaser, gossipManager interfaces.GossipService, node *rfpb.NodeDescriptor, partitions []disk.Partition, clock clockwork.Clock, fileStorer filestore.Store) (*Tracker, error) {
	ut := &Tracker{
		gossipManager: gossipManager,
		node:          node,
		partitions:    partitions,
		byPartition:   make(map[string]*partitionUsage),
		clock:         clock,
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
			evictionSamplesChanSize:     metrics.RaftEvictionSamplesChanSize.With(partitionLabel),
			evictionGCSChanSize:         metrics.RaftEvictionGCSChanSize.With(partitionLabel),
		}
		u := &partitionUsage{
			part:                     p,
			sender:                   sender,
			clock:                    clock,
			nodes:                    make(map[string]*nodePartitionUsage),
			dbGetter:                 dbGetter,
			samples:                  make(chan *approxlru.Sample[*evictionKey], *sampleBufferSize),
			deletes:                  make(chan *approxlru.Sample[*evictionKey], *deleteBufferSize),
			gcsDeletes:               make(chan *sgpb.StorageMetadata_GCSMetadata, *gcsDeleteBufferSize),
			samplesPerBatch:          *samplesPerBatch,
			samplerIterRefreshPeriod: *samplerIterRefreshPeriod,
			samplerSleepDuration:     *samplerSleepDuration,
			minEvictionAge:           *minEvictionAge,
			localSizeUpdatePeriod:    *localSizeUpdatePeriod,
			evictionBatchSize:        *evictionBatchSize,
			numDeleteWorkers:         *numDeleteWorkers,
			numGCSDeleteWorkers:      *numGCSDeleteWorkers,
			fileStorer:               fileStorer,
			metrics:                  metricSet,
		}
		ut.byPartition[p.ID] = u
		maxSizeBytes := int64(EvictionCutoffThreshold * float64(p.MaxSizeBytes))
		l, err := approxlru.New(&approxlru.Opts[*evictionKey]{
			SamplePoolSize:              *samplePoolSize,
			SamplesPerEviction:          *samplesPerEviction,
			MaxSizeBytes:                maxSizeBytes,
			DeletesPerEviction:          *deletesPerEviction,
			RateLimit:                   float64(*evictionRateLimit),
			EvictionResampleLatencyUsec: metrics.PebbleCacheEvictionResampleLatencyUsec.With(lbls),
			EvictionEvictLatencyUsec:    metrics.PebbleCacheEvictionEvictLatencyUsec.With(lbls),
			Clock:                       clock,
			OnEvict: func(ctx context.Context, sample *approxlru.Sample[*evictionKey]) error {
				return u.evict(ctx, sample)
			},
			OnSample: func(ctx context.Context, n int) ([]*approxlru.Sample[*evictionKey], error) {
				return u.sample(ctx, n)
			},
		})
		if err != nil {
			return nil, err
		}
		u.lru = l
	}

	gossipManager.AddListener(ut)
	return ut, nil
}

func (ut *Tracker) Start() {
	for _, pu := range ut.byPartition {
		ctx, cancelFunc := context.WithCancel(context.Background())
		pu.egCancel = cancelFunc
		eg, gctx := errgroup.WithContext(ctx)
		pu.eg = eg
		pu.eg.Go(func() error {
			pu.startSampleGenerator(gctx)
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
		pu.lru.Start()
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
			p.lru.Stop()
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

	// Propagate the updated usage to the LRU.
	for _, u := range ut.byPartition {
		sizeBytes := u.GlobalSizeBytes()
		u.lru.UpdateGlobalSizeBytes(sizeBytes)
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
			db.Close()
			totalSizeBytes := pu.LocalSizeBytes()
			pu.lru.UpdateSizeBytes(totalSizeBytes)
			maxAllowedSize := int64(EvictionCutoffThreshold * float64(pu.part.MaxSizeBytes))
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
