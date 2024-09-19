package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/elastic/gosigar"
	"github.com/jonboulle/clockwork"

	"github.com/docker/go-units"
	"github.com/hashicorp/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
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
	evictionBatchSize                  = flag.Int("cache.raft.eviction_batch_size", 100, "Buffer this many writes before delete")

	metricsRefreshPeriod = 30 * time.Second
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
	storePartitionUsageMaxAge = 15 * time.Minute

	// How old store partition usage data can be before we consider it invalid.
	storePartitionStalenessLimit = storePartitionUsageMaxAge * 2

	SamplerSleepThreshold = float64(0.2)
	SamplerSleepDuration  = 1 * time.Second

	SamplerIterRefreshPeriod = 5 * time.Minute
	evictFlushPeriod         = 10 * time.Second
)

type Tracker struct {
	rootDir       string
	gossipManager interfaces.GossipService
	node          *rfpb.NodeDescriptor
	partitions    []disk.Partition
	sender        *sender.Sender
	clock         clockwork.Clock

	mu            sync.Mutex
	byPartition   map[string]*partitionUsage
	lastBroadcast map[string]*rfpb.PartitionMetadata

	eg       *errgroup.Group
	egCancel context.CancelFunc
}

type nodePartitionUsage struct {
	sizeBytes  int64
	lastUpdate time.Time
}

type evictionKey struct {
	bytes []byte
}

func (k *evictionKey) ID() string {
	return string(k.bytes)
}

func (k *evictionKey) String() string {
	return string(k.bytes)
}

type partitionUsage struct {
	part disk.Partition

	dbGetter pebble.Leaser
	sender   *sender.Sender
	clock    clockwork.Clock

	mu  sync.Mutex
	lru *approxlru.LRU[*evictionKey]
	// Global view of usage, keyed by Node Host ID.
	nodes map[string]*nodePartitionUsage

	samples chan *approxlru.Sample[*evictionKey]
	deletes chan *approxlru.Sample[*evictionKey]
	rng     *rand.Rand

	eg       *errgroup.Group
	egCancel context.CancelFunc

	sizeBytes int64
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
	ticker := pu.clock.NewTicker(*localSizeUpdatePeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			sizeBytes := pu.LocalSizeBytes()
			pu.mu.Lock()
			pu.sizeBytes = sizeBytes
			pu.mu.Unlock()
			pu.lru.UpdateLocalSizeBytes(sizeBytes)
		}
	}

}

func (pu *partitionUsage) GlobalSizeBytes() int64 {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	sizeBytes := int64(0)
	for _, nu := range pu.nodes {
		sizeBytes += nu.sizeBytes
	}
	return sizeBytes
}

func (pu *partitionUsage) RemoteUpdate(nhid string, update *rfpb.PartitionMetadata) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	n, ok := pu.nodes[nhid]
	if !ok {
		n = &nodePartitionUsage{}
		pu.nodes[nhid] = n
	}
	n.lastUpdate = time.Now()
	n.sizeBytes = update.GetSizeBytes()

	// Prune stale data.
	for id, n := range pu.nodes {
		if time.Since(n.lastUpdate) > storePartitionStalenessLimit {
			delete(pu.nodes, id)
		}
	}
}

func (pu *partitionUsage) partitionKeyPrefix() string {
	return filestore.PartitionDirectoryPrefix + pu.part.ID
}

func (pu *partitionUsage) processEviction(ctx context.Context) {
	var keys []*sender.KeyMeta
	timer := time.NewTimer(evictFlushPeriod)
	defer timer.Stop()

	flush := func() {
		if len(keys) == 0 {
			return
		}
		rsps, err := pu.sender.RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
			batch := rbuilder.NewBatchBuilder()
			for _, k := range keys {
				sample, ok := k.Meta.(*approxlru.Sample[*evictionKey])
				if !ok {
					return nil, status.InternalError("meta not type of approxlru.Sample[*evictionKey]")
				}
				batch.Add(&rfpb.DeleteRequest{
					Key:        k.Key,
					MatchAtime: sample.Timestamp.UnixMicro(),
				})
			}
			batchCmd, err := batch.ToProto()
			if err != nil {
				return nil, status.InternalErrorf("could not construct delete req proto: %s", err)
			}
			rsp, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
				Header: h,
				Batch:  batchCmd,
			})
			if err != nil {
				return nil, status.InternalErrorf("could not propose eviction: %s", err)
			}
			res := make([]*approxlru.Sample[*evictionKey], 0)
			batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
			for i, k := range keys {
				_, err := batchRsp.DeleteResponse(i)
				if err != nil {
					return nil, err
				}
				res = append(res, k.Meta.(*approxlru.Sample[*evictionKey]))
			}
			return res, nil
		})
		if err != nil {
			log.Warningf("failed to evict %d keys: %s", len(keys), err)
		}

		for _, rsp := range rsps {
			res, ok := rsp.([]*approxlru.Sample[*evictionKey])
			if !ok {
				alert.UnexpectedEvent("raft_unexpected_delete_rsp", "response not type of approxlru.Sample[*evictionKey]")
			}
			pu.updateEvictionMetrics(res)
		}

		keys = nil
		timer.Reset(evictFlushPeriod)
	}

	for {
		select {
		case <-ctx.Done():
			for len(pu.deletes) > 0 {
				<-pu.deletes
			}
			return
		case sampleToDelete := <-pu.deletes:
			keys = append(keys, &sender.KeyMeta{
				Key:  sampleToDelete.Key.bytes,
				Meta: sampleToDelete,
			})
			if len(keys) >= *evictionBatchSize {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

func (pu *partitionUsage) startSampleGenerator(ctx context.Context) {
	eg := &errgroup.Group{}
	eg.Go(func() error {
		return pu.generateSamplesForEviction(ctx)
	})
	eg.Wait()
	// Drain samples chan before exiting
	for len(pu.samples) > 0 {
		<-pu.samples
	}
	close(pu.samples)
}

var digestRunes = []rune("abcdef1234567890")

func (pu *partitionUsage) randomKey(n int) []byte {
	randKey := pu.partitionKeyPrefix() + "/"
	for i := 0; i < n; i++ {
		randKey += string(digestRunes[rand.Intn(len(digestRunes))])
	}
	return []byte(randKey)
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
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return err
	}
	// We update the iter variable later on, so we need to wrap the Close call
	// in a func to operate on the correct iterator instance.
	defer func() {
		iter.Close()
	}()

	totalCount := 0
	shouldCreateNewIter := false
	fileMetadata := rfpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()

	timer := pu.clock.NewTimer(SamplerSleepDuration)
	defer timeutil.StopAndDrainClockworkTimer(timer)

	// Files are kept in random order (because they are keyed by digest), so
	// instead of doing a new seek for every random sample we will seek once
	// and just read forward, yielding digests until we've found enough.
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// When we started to populate a cache, we cannot find any eligible
		// entries to evict. We will sleep for some time to prevent from
		// constantly generating samples in vain.
		pu.mu.Lock()
		shouldSleep := pu.sizeBytes <= int64(SamplerSleepThreshold*float64(pu.part.MaxSizeBytes))
		pu.mu.Unlock()
		if shouldSleep {
			select {
			case <-ctx.Done():
				return nil
			case <-pu.clock.After(SamplerSleepDuration):
			}
		}

		if totalCount > *samplesPerBatch || time.Since(iterCreatedAt) > *samplerIterRefreshPeriod {
			// Going to refresh the iterator in the next iteration.
			shouldCreateNewIter = true
		}

		// Refresh the iterator once a while
		if shouldCreateNewIter {
			shouldCreateNewIter = false
			totalCount = 0
			iterCreatedAt = time.Now()
			newIter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: start,
				UpperBound: end,
			})
			if err != nil {
				return err
			}
			iter.Close()
			iter = newIter
		}
		totalCount += 1
		if !iter.Valid() {
			// This should happen once every totalCount times or when
			// we exausted the iter.
			randomKey := pu.randomKey(64)
			valid := iter.SeekGE(randomKey)
			if !valid {
				shouldCreateNewIter = true
				continue
			}
		}
		var key filestore.PebbleKey
		if _, err := key.FromBytes(iter.Key()); err != nil {
			log.Warningf("cannot generate sample for eviction, skipping: failed to read key: %s", err)
			continue
		}

		err = proto.Unmarshal(iter.Value(), fileMetadata)
		if err != nil {
			log.Warningf("cannot generate sample for eviction, skipping: failed to read proto: %s", err)
			continue
		}

		pu.maybeAddToSampleChan(ctx, iter, fileMetadata, timer)

		iter.Next()
		fileMetadata.ResetVT()
	}
}

func (pu *partitionUsage) maybeAddToSampleChan(ctx context.Context, iter pebble.Iterator, fileMetadata *rfpb.FileMetadata, timer clockwork.Timer) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	age := pu.clock.Since(atime)
	if age < *minEvictionAge {
		return
	}
	sizeBytes := int64(proto.Size(fileMetadata)) + int64(len(iter.Key()))

	keyBytes := make([]byte, len(iter.Key()))
	copy(keyBytes, iter.Key())
	sample := &approxlru.Sample[*evictionKey]{
		Key: &evictionKey{
			bytes: keyBytes,
		},
		SizeBytes: sizeBytes,
		Timestamp: atime,
	}
	timeutil.StopAndDrainClockworkTimer(timer)
	timer.Reset(SamplerSleepDuration)
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
	sizeBytes := int64(0)
	lbls := prometheus.Labels{metrics.PartitionID: pu.part.ID, metrics.CacheNameLabel: constants.CacheName}
	for _, sample := range samples {
		age := time.Since(sample.Timestamp)
		sizeBytes += sample.SizeBytes
		metrics.DiskCacheEvictionAgeMsec.With(lbls).Observe(float64(age.Milliseconds()))
		metrics.DiskCacheLastEvictionAgeUsec.With(lbls).Set(float64(age.Microseconds()))
	}
	metrics.DiskCacheNumEvictions.With(lbls).Add(float64(len(samples)))
	metrics.DiskCacheBytesEvicted.With(lbls).Add(float64(sizeBytes))

	globalSizeBytes := pu.GlobalSizeBytes()

	pu.mu.Lock()
	defer pu.mu.Unlock()

	// Assume eviction on all stores is happening at a similar rate as on the
	// current store and update the usage information speculatively since we
	// don't know when we'll receive the next usage update from remote stores.
	// When we do receive updates from other stores they will overwrite our
	// speculative numbers.
	for _, npu := range pu.nodes {
		npu.sizeBytes -= int64(float64(sizeBytes) * float64(globalSizeBytes) / float64(npu.sizeBytes))
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
	globalSizeBytes := pu.GlobalSizeBytes()

	lbls := prometheus.Labels{metrics.PartitionID: pu.part.ID, metrics.CacheNameLabel: constants.CacheName}

	metrics.DiskCachePartitionSizeBytes.With(lbls).Set(float64(globalSizeBytes))
	metrics.DiskCachePartitionCapacityBytes.With(lbls).Set(float64(pu.part.MaxSizeBytes))
}

func New(rootDir string, sender *sender.Sender, dbGetter pebble.Leaser, gossipManager interfaces.GossipService, node *rfpb.NodeDescriptor, partitions []disk.Partition, clock clockwork.Clock) (*Tracker, error) {
	ut := &Tracker{
		rootDir:       rootDir,
		gossipManager: gossipManager,
		node:          node,
		partitions:    partitions,
		byPartition:   make(map[string]*partitionUsage),
		clock:         clock,
		lastBroadcast: make(map[string]*rfpb.PartitionMetadata),
	}

	for _, p := range partitions {
		u := &partitionUsage{
			part:     p,
			sender:   sender,
			clock:    clock,
			nodes:    make(map[string]*nodePartitionUsage),
			dbGetter: dbGetter,
			samples:  make(chan *approxlru.Sample[*evictionKey], *sampleBufferSize),
			deletes:  make(chan *approxlru.Sample[*evictionKey], *deleteBufferSize),
		}
		ut.byPartition[p.ID] = u
		metricLbls := prometheus.Labels{
			metrics.PartitionID:    p.ID,
			metrics.CacheNameLabel: constants.CacheName,
		}
		maxSizeBytes := int64(EvictionCutoffThreshold * float64(p.MaxSizeBytes))
		l, err := approxlru.New(&approxlru.Opts[*evictionKey]{
			SamplePoolSize:              *samplePoolSize,
			SamplesPerEviction:          *samplesPerEviction,
			MaxSizeBytes:                maxSizeBytes,
			DeletesPerEviction:          *deletesPerEviction,
			RateLimit:                   float64(*evictionRateLimit),
			EvictionResampleLatencyUsec: metrics.PebbleCacheEvictionResampleLatencyUsec.With(metricLbls),
			EvictionEvictLatencyUsec:    metrics.PebbleCacheEvictionEvictLatencyUsec.With(metricLbls),
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
		ctx, cancelFunc := context.WithCancel(context.Background())
		u.egCancel = cancelFunc

		eg, gctx := errgroup.WithContext(ctx)
		u.eg = eg
		u.eg.Go(func() error {
			u.startSampleGenerator(gctx)
			return nil
		})
		u.eg.Go(func() error {
			u.processEviction(gctx)
			return nil
		})
		u.eg.Go(func() error {
			u.updateLocalSizeBytes(gctx)
			return nil
		})
		l.Start()
		u.lru = l
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

	gossipManager.AddListener(ut)
	return ut, nil
}

func (ut *Tracker) Stop() {
	ut.egCancel()
	ut.eg.Wait()
	for _, p := range ut.byPartition {
		p.lru.Stop()
		p.egCancel()
		p.eg.Wait()
	}
}

func (ut *Tracker) Statusz(ctx context.Context) string {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	buf := "Partitions:\n"
	for _, p := range ut.partitions {
		buf += fmt.Sprintf("\t%s\n", p.ID)
		u, ok := ut.byPartition[p.ID]
		if !ok {
			buf += "\t\tno data\n"
			continue
		}

		globalSizeBytes := u.GlobalSizeBytes()
		percentFull := (float64(globalSizeBytes) / float64(p.MaxSizeBytes)) * 100

		buf += fmt.Sprintf("\t\tCapacity: %s / %s (%2.2f%% full)\n", units.BytesSize(float64(globalSizeBytes)), units.BytesSize(float64(p.MaxSizeBytes)), percentFull)

		// Show nodes in a consistent order so that they don't jump around when
		// refreshing the statusz page.
		var nhids []string
		for nhid := range u.nodes {
			nhids = append(nhids, nhid)
		}
		sort.Strings(nhids)
		buf += "\t\tGlobal Usage:\n"
		for _, nhid := range nhids {
			nu, ok := u.nodes[nhid]
			if !ok {
				continue
			}
			buf += fmt.Sprintf("\t\t\t%s: %s\n", nhid, units.BytesSize(float64(nu.sizeBytes)))
		}
	}
	return buf
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

func (ut *Tracker) computeUsage() *rfpb.NodePartitionUsage {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	nu := &rfpb.NodePartitionUsage{
		Node: ut.node,
	}

	for _, p := range ut.partitions {
		up := &rfpb.PartitionMetadata{
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-ut.clock.After(storePartitionUsageCheckInterval):
			if !idleTimer.Stop() {
				<-idleTimer.Chan()
			}
			idleTimer.Reset(storePartitionUsageMaxAge)
			if err := ut.broadcast(false /*=force*/); err != nil {
				log.Warningf("could not gossip node partition usage info: %s", err)
			}
		case <-idleTimer.Chan():
			if err := ut.broadcast(true /*=force*/); err != nil {
				log.Warningf("could not gossip node partition usage info: %s", err)
			}
		}
	}
}

func (ut *Tracker) broadcast(force bool) error {
	usage := ut.computeUsage()

	// If not forced, check whether there's enough changes to force a broadcast.
	if !force {
		significantChange := false
		ut.mu.Lock()
		for _, u := range usage.GetPartitionUsage() {
			lb, ok := ut.lastBroadcast[u.GetPartitionId()]
			if !ok || math.Abs(float64(u.GetSizeBytes()-lb.GetSizeBytes())) > float64(*partitionUsageDeltaGossipThreshold) {
				significantChange = true
				break
			}
		}
		ut.mu.Unlock()
		if !significantChange {
			return nil
		}
	}

	buf, err := proto.Marshal(usage)
	if err != nil {
		return err
	}

	if err := ut.gossipManager.SendUserEvent(constants.NodePartitionUsageEvent, buf, false /*coalesce*/); err != nil {
		return err
	}

	ut.mu.Lock()
	defer ut.mu.Unlock()
	for _, u := range usage.GetPartitionUsage() {
		ut.lastBroadcast[u.GetPartitionId()] = u
	}

	return nil
}

func (ut *Tracker) refreshMetrics(ctx context.Context) {
	ticker := time.NewTicker(metricsRefreshPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fsu := gosigar.FileSystemUsage{}
			if err := fsu.Get(ut.rootDir); err != nil {
				log.Warningf("could not retrieve filesystem stats: %s", err)
			} else {
				metrics.DiskCacheFilesystemTotalBytes.With(prometheus.Labels{metrics.CacheNameLabel: constants.CacheName}).Set(float64(fsu.Total))
				metrics.DiskCacheFilesystemAvailBytes.With(prometheus.Labels{metrics.CacheNameLabel: constants.CacheName}).Set(float64(fsu.Avail))
			}

			ut.mu.Lock()
			for _, pu := range ut.byPartition {
				pu.updateMetrics()
			}
			ut.mu.Unlock()
		}
	}
}

func (ut *Tracker) TestingWaitForGC() {
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
			db.Close()
			totalSizeBytes := pu.LocalSizeBytes()
			pu.lru.UpdateSizeBytes(totalSizeBytes)
			maxAllowedSize := int64(EvictionCutoffThreshold * float64(pu.part.MaxSizeBytes))
			if totalSizeBytes <= maxAllowedSize {
				done += 1
			}
		}
		if done == len(partitionUsage) {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}
