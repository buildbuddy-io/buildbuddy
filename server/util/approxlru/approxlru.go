package approxlru

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

const (
	// evictCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	evictCheckPeriod = 1 * time.Second

	defaultDeletesPerEviction = 5
)

type Key interface {
	// String() is used for information logging.
	fmt.Stringer
	// The ID is used to filter out duplicate samples and is stored within the
	// eviction pool.
	ID() string
}

type Sample[T Key] struct {
	Key       T
	SizeBytes int64
	Timestamp time.Time
}

func (s *Sample[T]) String() string {
	return fmt.Sprintf("{key: %s, sizeBytes: %d, timestamp: %s}", s.Key, s.SizeBytes, s.Timestamp)
}

// OnEvict requests that the given key be evicted.
type OnEvict[T Key] func(ctx context.Context, sample *Sample[T]) error

// OnSample requests that n random samples be provided from the underlying data.
type OnSample[T Key] func(ctx context.Context, n int) ([]*Sample[T], error)

// The LRU does not store information about the full set of keys or values that
// is subject to eviction, instead relying on user provided callbacks to perform
// sampling of the full universe of data.
//
// An internal eviction pool is maintained consisting of random keys sampled
// by the user-provided callback. When LRU capacity is exceeded, the LRU evicts
// the oldest entries in the pool and resamples to maintain the pool size.
//
// For more details, see:
//
//	https://github.com/redis/redis/blob/unstable/src/evict.c#L118 and
//	http://antirez.com/news/109
type LRU[T Key] struct {
	samplesPerEviction          int
	deletesPerEviction          int
	samplePoolSize              int
	maxSizeBytes                int64
	onEvict                     OnEvict[T]
	onSample                    OnSample[T]
	evictionResampleLatencyUsec prometheus.Observer
	evictionEvictLatencyUsec    prometheus.Observer
	limiter                     *rate.Limiter

	samplePool []*Sample[T]

	mu              sync.Mutex
	ctx             context.Context
	cancelCtx       context.CancelFunc
	globalSizeBytes int64
	localSizeBytes  int64
	lastRun         time.Time
	lastEvicted     *Sample[T]
}

type Opts[T Key] struct {
	MaxSizeBytes int64
	// SamplesPerEviction is the number of random keys to sample when adding a
	// new deletion candidate to the sample pool. Increasing this number
	// makes eviction slower but improves sampled-LRU accuracy.
	SamplesPerEviction int
	// DeletesPerEviction is the number of keys to evict from the sample pool
	// in one eviction cycle before refilling the sample pool.
	DeletesPerEviction int
	// SamplePoolSize is the number of deletion candidates to maintain in
	// memory at a time. Increasing this number uses more memory but
	// improves sampled-LRU accuracy.
	SamplePoolSize int
	// EvictionResampleLatencyUsec is an optional metric to update with latency
	// for resampling during a single eviction iteration.
	EvictionResampleLatencyUsec prometheus.Observer
	// EvictionEvictLatencyUsec is an optional metric to update with latency
	// for eviction of a single key.
	EvictionEvictLatencyUsec prometheus.Observer
	// RateLimit is the maximum number of evictions to perform per second.
	// If not set, no limit is enforced.
	RateLimit          float64
	NumEvictionWorkers int

	OnEvict  OnEvict[T]
	OnSample OnSample[T]
}

func New[T Key](opts *Opts[T]) (*LRU[T], error) {
	if opts.SamplePoolSize == 0 {
		return nil, status.FailedPreconditionError("sample pool size is required")
	}
	if opts.SamplesPerEviction == 0 {
		return nil, status.FailedPreconditionError("samples per eviction is required")
	}
	deletesPerEviction := opts.DeletesPerEviction
	if opts.DeletesPerEviction == 0 {
		deletesPerEviction = defaultDeletesPerEviction
	}
	if opts.MaxSizeBytes == 0 {
		return nil, status.FailedPreconditionError("max size is required")
	}
	if opts.OnEvict == nil {
		return nil, status.FailedPreconditionError("eviction callback is required")
	}
	if opts.OnSample == nil {
		return nil, status.FailedPreconditionError("sample callback is required")
	}
	rateLimit := rate.Limit(opts.RateLimit)
	if rateLimit == 0 {
		rateLimit = rate.Inf
	}
	l := &LRU[T]{
		samplePoolSize:              opts.SamplePoolSize,
		samplesPerEviction:          opts.SamplesPerEviction,
		deletesPerEviction:          deletesPerEviction,
		maxSizeBytes:                opts.MaxSizeBytes,
		onEvict:                     opts.OnEvict,
		onSample:                    opts.OnSample,
		evictionResampleLatencyUsec: opts.EvictionResampleLatencyUsec,
		evictionEvictLatencyUsec:    opts.EvictionEvictLatencyUsec,
		limiter:                     rate.NewLimiter(rateLimit, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.ctx = ctx
	l.cancelCtx = cancel
	return l, nil
}

func (l *LRU[T]) Start() {
	go l.evictor()
}

func (l *LRU[T]) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancelCtx != nil {
		l.cancelCtx()
		l.cancelCtx = nil
	}
}

func (l *LRU[T]) LocalSizeBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.localSizeBytes
}

func (l *LRU[T]) MaxSizeBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxSizeBytes
}

func (l *LRU[T]) UpdateLocalSizeBytes(localSizeBytes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.localSizeBytes = localSizeBytes
}

func (l *LRU[T]) UpdateGlobalSizeBytes(globalSizeBytes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.globalSizeBytes = globalSizeBytes
}

func (l *LRU[T]) UpdateSizeBytes(sizeBytes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.globalSizeBytes = sizeBytes
	l.localSizeBytes = sizeBytes
}

func (l *LRU[T]) resampleK(k int) error {
	seen := make(map[string]struct{}, len(l.samplePool))
	for _, entry := range l.samplePool {
		seen[entry.Key.ID()] = struct{}{}
	}

	// read new entries to put in the pool.
	additions := make([]*Sample[T], 0, k*l.samplesPerEviction)
	for i := 0; i < k; i++ {
		entries, err := l.onSample(l.ctx, l.samplesPerEviction)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if _, ok := seen[e.Key.ID()]; ok {
				continue
			}
			seen[e.Key.ID()] = struct{}{}
			additions = append(additions, e)
		}
	}

	l.samplePool = append(l.samplePool, additions...)

	if len(l.samplePool) > 0 {
		sort.Slice(l.samplePool, func(i, j int) bool {
			return l.samplePool[i].Timestamp.UnixNano() > l.samplePool[j].Timestamp.UnixNano()
		})
	}

	if len(l.samplePool) > l.samplePoolSize {
		l.samplePool = l.samplePool[len(l.samplePool)-l.samplePoolSize:]
	}

	return nil
}

func (l *LRU[T]) evictSingleKey() (*Sample[T], error) {
	if err := l.limiter.Wait(l.ctx); err != nil {
		return nil, err
	}
	for i := len(l.samplePool) - 1; i >= 0; i-- {
		sample := l.samplePool[i]

		l.mu.Lock()
		oldLocalSizeBytes := l.localSizeBytes
		oldGlobalSizeBytes := l.globalSizeBytes
		l.mu.Unlock()

		log.Infof("Evictor attempting to evict %q (last accessed %s)", sample.Key, time.Since(sample.Timestamp))
		err := l.onEvict(l.ctx, sample)
		if err != nil {
			log.Warningf("Could not evict %q: %s", sample.Key, err)
			continue
		}

		l.mu.Lock()
		// The user (e.g. pebble cache) is the source of truth of the size
		// data, but the LRU also needs to do its own accounting in between
		// the times that the user provides a size update to the LRU.
		// We skip our own accounting here if we detect that the size has
		// changed since it means the user provided their own size update
		// which takes priority.
		if l.localSizeBytes == oldLocalSizeBytes {
			l.localSizeBytes -= sample.SizeBytes
		}
		if l.globalSizeBytes == oldGlobalSizeBytes {
			// Assume eviction on remote servers is happening at the same
			// rate as local eviction. It's fine to be wrong as we expect the
			// actual sizes to be periodically to be reset to the true numbers
			// using UpdateSizeBytes from data received from other servers.
			l.globalSizeBytes -= int64(float64(sample.SizeBytes) * float64(l.globalSizeBytes) / float64(l.localSizeBytes))
		}
		l.mu.Unlock()

		l.samplePool = append(l.samplePool[:i], l.samplePool[i+1:]...)

		return sample, nil
	}

	return nil, status.NotFoundErrorf("could not find sample to evict")
}

func (l *LRU[T]) evict() (*Sample[T], error) {
	// Resample every time we evict keys.
	// N.B. We might end up evicting less than deletesPerEviction keys below
	// but sampling extra keys is harmless.
	numToSample := l.deletesPerEviction
	// Fill the pool if it's empty.
	if len(l.samplePool) == 0 {
		numToSample = l.samplePoolSize
	}
	start := time.Now()
	if err := l.resampleK(numToSample); err != nil {
		return nil, err
	}
	if l.evictionResampleLatencyUsec != nil {
		l.evictionResampleLatencyUsec.Observe(float64(time.Since(start).Microseconds()))
	}

	var evicted []*Sample[T]
	for {
		start = time.Now()
		evictedKey, err := l.evictSingleKey()
		if status.IsNotFoundError(err) {
			// If no candidates were evictable in the whole pool, resample
			// the pool.
			l.samplePool = l.samplePool[:0]
			if err := l.resampleK(l.samplePoolSize); err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}
		if l.evictionEvictLatencyUsec != nil {
			l.evictionEvictLatencyUsec.Observe(float64(time.Since(start).Microseconds()))
		}

		evicted = append(evicted, evictedKey)

		if len(evicted) == l.deletesPerEviction {
			break
		}

		l.mu.Lock()
		globalSizeBytes := l.globalSizeBytes
		localSizeBytes := l.localSizeBytes
		l.mu.Unlock()

		// Cache is under the max, so we can stop early.
		if globalSizeBytes <= l.maxSizeBytes || localSizeBytes == 0 {
			break
		}
	}

	return evicted[len(evicted)-1], nil
}

func (l *LRU[T]) ttl() error {
	for {
		l.mu.Lock()
		globalSizeBytes := l.globalSizeBytes
		localSizeBytes := l.localSizeBytes
		l.mu.Unlock()

		if globalSizeBytes <= l.maxSizeBytes || localSizeBytes == 0 {
			break
		}

		select {
		case <-l.ctx.Done():
			return nil
		default:
			break
		}

		lastEvicted, err := l.evict()
		if err != nil {
			return err
		}

		l.mu.Lock()
		l.lastRun = time.Now()
		l.lastEvicted = lastEvicted
		l.mu.Unlock()
	}
	return nil
}

func (l *LRU[T]) evictor() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(evictCheckPeriod):
			if err := l.ttl(); err != nil {
				log.Warningf("could not evict: %s", err)
			}
		}
	}
}

func (l *LRU[T]) LastRun() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastRun
}

func (l *LRU[T]) LastEvicted() *Sample[T] {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastEvicted
}
