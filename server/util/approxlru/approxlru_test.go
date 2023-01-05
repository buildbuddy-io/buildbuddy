package approxlru_test

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

type entry struct {
	id        string
	sizeBytes int64
	atime     time.Time
}

func (l *entry) String() string {
	return l.id
}

func (l *entry) ID() string {
	return l.id
}

type testCache struct {
	mu        sync.Mutex
	data      []*entry
	evictions []*entry
}

func (tc *testCache) Add(e *entry) {
	tc.data = append(tc.data, e)
}

func (tc *testCache) evict(ctx context.Context, sample *approxlru.Sample[*entry]) (skip bool, err error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	key := sample.Key
	for i, v := range tc.data {
		if v.ID() == key.ID() {
			tc.evictions = append(tc.evictions, key)
			tc.data[i] = tc.data[len(tc.data)-1]
			tc.data = tc.data[:len(tc.data)-1]
			return false, nil
		}
	}

	return false, status.InvalidArgumentErrorf("key %q not in cache", key.ID())
}

func (tc *testCache) sample(ctx context.Context, n int) ([]*approxlru.Sample[*entry], error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	var samples []*approxlru.Sample[*entry]
	for i := 0; i < n; i++ {
		s := tc.data[rand.Intn(len(tc.data))]
		samples = append(samples, &approxlru.Sample[*entry]{
			Key:       s,
			SizeBytes: s.sizeBytes,
			Timestamp: s.atime,
		})
	}
	return samples, nil
}

func (tc *testCache) refresh(ctx context.Context, key *entry) (skip bool, timestamp time.Time, err error) {
	return false, key.atime, nil
}

func waitForEviction(t *testing.T, l *approxlru.LRU[*entry]) {
	start := time.Now()
	for l.LocalSizeBytes() > l.MaxSizeBytes() {
		time.Sleep(1 * time.Second)
		if time.Since(start) > 10*time.Second {
			require.FailNow(t, "eviction did not finish")
		}
	}
}

func newCache(t *testing.T, maxSizeBytes int64) (*testCache, *approxlru.LRU[*entry]) {
	c := &testCache{}
	l, err := approxlru.New(&approxlru.Opts[*entry]{
		SamplePoolSize:     100,
		SamplesPerEviction: 10,
		MaxSizeBytes:       maxSizeBytes,
		OnEvict: func(ctx context.Context, sample *approxlru.Sample[*entry]) (skip bool, err error) {
			return c.evict(ctx, sample)
		},
		OnSample: func(ctx context.Context, n int) ([]*approxlru.Sample[*entry], error) {
			return c.sample(ctx, n)
		},
		OnRefresh: func(ctx context.Context, key *entry) (skip bool, timestamp time.Time, err error) {
			return c.refresh(ctx, key)
		},
	})
	require.NoError(t, err)
	return c, l
}

func fillCache(t *testing.T, c *testCache, n int, sizeBytes int64) {
	atime, err := time.Parse(time.RFC3339, "2022-01-01T00:00:00-07:00")
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		c.Add(&entry{
			id:        strconv.Itoa(i),
			sizeBytes: sizeBytes,
			atime:     atime,
		})
		atime = atime.Add(1 * time.Hour)
	}
}

func TestPartialEviction(t *testing.T) {
	c, l := newCache(t, 9_000 /*=maxSizeBytes*/)

	fillCache(t, c, 1000, 10 /*=sizeBytes*/)

	// Inform the LRU about the new size of the underlying cache.
	// Eviction should kick in and evict until the cache is down to 9_000.
	l.UpdateSizeBytes(10_000)
	waitForEviction(t, l)

	oldest := c.data[0].atime

	for _, e := range c.evictions {
		if !e.atime.Before(oldest) {
			require.FailNowf(t, "early eviction", "evicted item %+v is not older than entries still in the cache", e)
		}
	}
}

func TestRefresh(t *testing.T) {
	c, l := newCache(t, 9_000 /*=maxSizeBytes*/)

	fillCache(t, c, 1000, 10 /*=sizeBytes*/)

	// Inform the LRU about the new size of the underlying cache.
	// Eviction should kick in and evict until the cache is down to 9_000.
	l.UpdateSizeBytes(10_000)
	waitForEviction(t, l)

	// Update atimes on the oldest atimes to avoid evicting them.
	var refreshedIDs []string
	c.mu.Lock()
	for i := 0; i < 10; i++ {
		c.data[i].atime = time.Now()
		refreshedIDs = append(refreshedIDs, c.data[i].id)
	}
	c.mu.Unlock()

	// Tell the LRU that the cache is back to 10,000 so it evicts 1_000 more
	// bytes.
	l.UpdateSizeBytes(10_000)
	waitForEviction(t, l)

	c.mu.Lock()
	for i := 0; i < 10; i++ {
		if c.data[i].id != refreshedIDs[i] {
			require.FailNowf(t, "early eviction", "entry %q should not have been evicted yet", refreshedIDs[i])
		}
	}
	c.mu.Unlock()
}
