package approxlru

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// evictCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	evictCheckPeriod = 1 * time.Second
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

// OnEvict requests that the given key be evicted. The callback may return
// skip=true to indicate that the given sample is no longer valid and should be
// removed from the eviction pool.
type OnEvict[T Key] func(ctx context.Context, sample *Sample[T]) (skip bool, err error)

// OnSample requests that n random samples be provided from the underlying data.
type OnSample[T Key] func(ctx context.Context, n int) ([]*Sample[T], error)

// OnRefresh requests the latest access timestamp for the given key. The
// callback may return skip=true to indicate that the given sample is no longer
// valid and should be removed from the eviction pool.
type OnRefresh[T Key] func(ctx context.Context, key T) (skip bool, timestamp time.Time, error error)

// LRU implements a thread safe fixed size LRU cache using sampled eviction.
//
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
	samplesPerEviction int
	samplePoolSize     int
	maxSizeBytes       int64
	onEvict            OnEvict[T]
	onSample           OnSample[T]
	onRefresh          OnRefresh[T]

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
	// SamplePoolSize is the number of deletion candidates to maintain in
	// memory at a time. Increasing this number uses more memory but
	// improves sampled-LRU accuracy.
	SamplePoolSize int
	OnEvict        OnEvict[T]
	OnSample       OnSample[T]
	OnRefresh      OnRefresh[T]
}

func New[T Key](opts *Opts[T]) (*LRU[T], error) {
	if opts.SamplePoolSize == 0 {
		return nil, status.FailedPreconditionError("sample pool size is required")
	}
	if opts.SamplesPerEviction == 0 {
		return nil, status.FailedPreconditionError("samples per eviction is required")
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
	if opts.OnRefresh == nil {
		return nil, status.FailedPreconditionError("refresh callback is required")
	}
	l := &LRU[T]{
		samplePoolSize:     opts.SamplePoolSize,
		samplesPerEviction: opts.SamplesPerEviction,
		maxSizeBytes:       opts.MaxSizeBytes,
		onEvict:            opts.OnEvict,
		onSample:           opts.OnSample,
		onRefresh:          opts.OnRefresh,
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.ctx = ctx
	l.cancelCtx = cancel
	go l.evictor()
	return l, nil
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

	filtered := make([]*Sample[T], 0, len(l.samplePool))

	// refresh all the entries already in the pool.
	for _, sample := range l.samplePool {
		skip, timestamp, err := l.onRefresh(l.ctx, sample.Key)
		if err != nil {
			log.Warningf("Could not refresh timestamp for %q: %s", sample.Key, err)
		}
		if skip {
			continue
		}
		sample.Timestamp = timestamp
		filtered = append(filtered, sample)
	}

	l.samplePool = append(filtered, additions...)

	if len(l.samplePool) > 0 {
		sort.Slice(l.samplePool, func(i, j int) bool {
			return l.samplePool[i].Timestamp.UnixNano() < l.samplePool[j].Timestamp.UnixNano()
		})
	}

	if len(l.samplePool) > l.samplePoolSize {
		l.samplePool = l.samplePool[:l.samplePoolSize]
	}

	return nil
}

func (l *LRU[T]) evict() (*Sample[T], error) {
	// Resample every time we evict a key
	if err := l.resampleK(1); err != nil {
		return nil, err
	}

	for {
		for i, sample := range l.samplePool {
			log.Infof("Evictor attempting to evict %q", sample.Key)
			skip, err := l.onEvict(l.ctx, sample)
			if err != nil {
				log.Warningf("Could not evict %q: %s", sample.Key, err)
				continue
			}

			l.mu.Lock()
			l.localSizeBytes -= sample.SizeBytes
			// Assume eviction on remote servers is happening at the same
			// rate as local eviction. It's fine to be wrong as we expect the
			// actual sizes to be periodically to be reset to the true numbers
			// using UpdateSizeBytes from data received from other servers.
			l.globalSizeBytes -= int64(float64(sample.SizeBytes) * float64(l.globalSizeBytes) / float64(l.localSizeBytes))
			l.mu.Unlock()

			l.samplePool = append(l.samplePool[:i], l.samplePool[i+1:]...)

			if skip {
				continue
			}

			return sample, nil
		}

		// If no candidates were evictable in the whole pool, resample
		// the pool.
		l.samplePool = l.samplePool[:0]
		if err := l.resampleK(l.samplePoolSize); err != nil {
			return nil, err
		}
	}
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
