package cache_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagetracker"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	userMap = testauth.TestUsers("user1", "group1", "user2", "group2")
)

func getTestEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(userMap)
	te.SetAuthenticator(ta)
	return te
}

type testConfig struct {
	env    *testenv.TestEnv
	config *raft_cache.Config
}

func getTestConfigs(t *testing.T, n int) []testConfig {
	res := make([]testConfig, 0, n)
	for i := 0; i < n; i++ {
		c := testConfig{
			env:    getTestEnv(t),
			config: getCacheConfig(t),
		}
		res = append(res, c)
	}
	return res
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *rspb.ResourceName) {
	reader, err := c.Reader(ctx, d, 0, 0)
	require.NoError(t, err, "cache: %+v", c)
	d1 := testdigest.ReadDigestAndClose(t, reader)
	require.Equal(t, d.GetDigest().GetHash(), d1.GetHash())
}

func writeDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *rspb.ResourceName, buf []byte) {
	writeCloser, err := c.Writer(ctx, d)
	require.NoError(t, err, "cache: %+v", c)
	n, err := writeCloser.Write(buf)
	require.NoError(t, err)
	require.Equal(t, n, len(buf))

	err = writeCloser.Commit()
	require.NoError(t, err)

	err = writeCloser.Close()
	require.NoError(t, err)
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func getCacheConfig(t *testing.T) *raft_cache.Config {
	return &raft_cache.Config{
		RootDir:  testfs.MakeTempDir(t),
		HTTPAddr: localAddr(t),
		GRPCAddr: localAddr(t),
	}
}

func allHealthy(caches ...*raft_cache.RaftCache) bool {
	eg := errgroup.Group{}
	for _, cache := range caches {
		cache := cache
		eg.Go(func() error {
			return cache.Check(context.Background())
		})
	}
	err := eg.Wait()
	return err == nil
}

func parallelShutdown(caches ...*raft_cache.RaftCache) {
	eg := errgroup.Group{}
	ctx := context.Background()
	for _, cache := range caches {
		cache := cache
		eg.Go(func() error {
			cache.Stop(ctx)
			return nil
		})
	}
	eg.Wait()
}

func waitForHealthy(t *testing.T, caches ...*raft_cache.RaftCache) {
	start := time.Now()
	timeout := 30 * time.Second
	done := make(chan struct{})
	go func() {
		for {
			if allHealthy(caches...) {
				close(done)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		log.Printf("%d caches became healthy in %s", len(caches), time.Since(start))
	case <-time.After(timeout):
		t.Fatalf("Caches [%d] did not become healthy after %s, %+v", len(caches), timeout, caches[0])
	}
}

func waitForShutdown(t *testing.T, caches ...*raft_cache.RaftCache) {
	timeout := 10 * time.Second
	done := make(chan struct{})
	go func() {
		parallelShutdown(caches...)
		close(done)
	}()

	select {
	case <-done:
		break
	case <-time.After(timeout):
		t.Fatalf("Caches [%d] did not shutdown after %s", len(caches), timeout)
	}
}

func startNNodes(t *testing.T, configs []testConfig) []*raft_cache.RaftCache {
	eg := errgroup.Group{}
	n := len(configs)
	caches := make([]*raft_cache.RaftCache, n)

	joinList := make([]string, 0, n)
	for i := 0; i < n; i++ {
		joinList = append(joinList, localAddr(t))
	}

	for i, config := range configs {
		i := i
		lN := joinList[i]
		joinList := joinList
		gs, err := gossip.New("name-"+lN, lN, joinList)
		require.NoError(t, err)
		config.env.SetGossipService(gs)
		eg.Go(func() error {
			n, err := raft_cache.NewRaftCache(config.env, config.config)
			if err != nil {
				return err
			}
			caches[i] = n
			return nil
		})
	}
	require.Nil(t, eg.Wait())

	// wait for them all to become healthy
	waitForHealthy(t, caches...)
	return caches
}

func TestAutoBringup(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNNodes(t, configs)
	waitForShutdown(t, caches...)
}

func TestReaderAndWriter(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNNodes(t, configs)
	rc1 := caches[0]

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		r, buf := testdigest.RandomCASResourceBuf(t, 100)
		writeDigest(t, ctx, rc1, r, buf)
		readAndCompareDigest(t, ctx, rc1, r)
	}
	waitForShutdown(t, caches...)
}

func TestCacheShutdown(t *testing.T) {
	t.Skip()

	configs := getTestConfigs(t, 3)
	caches := startNNodes(t, configs)
	rc1 := caches[0]
	rc2 := caches[1]

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env)
	require.NoError(t, err)

	cacheRPCTimeout := 5 * time.Second
	digestsWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		rn, buf := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_CAS, "remote/instance/name")
		writeDigest(t, ctx, rc1, rn, buf)
		digestsWritten = append(digestsWritten, rn)
	}

	// shutdown one node
	waitForShutdown(t, caches[len(caches)-1])

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		rn, buf := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_CAS, "remote/instance/name")
		writeDigest(t, ctx, rc1, rn, buf)
		digestsWritten = append(digestsWritten, rn)
	}

	for _, d := range digestsWritten {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		readAndCompareDigest(t, ctx, rc2, d)
	}

	waitForShutdown(t, caches...)
}

func TestDistributedRanges(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNNodes(t, configs)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env)
	require.NoError(t, err)

	digests := make([]*rspb.ResourceName, 0)
	for i := 0; i < 10; i++ {
		rc := caches[rand.Intn(len(caches))]

		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		r, buf := testdigest.RandomCASResourceBuf(t, 100)
		writeDigest(t, ctx, rc, r, buf)
	}

	victim := caches[0]
	caches = caches[1:]
	waitForShutdown(t, victim)

	for _, d := range digests {
		rc := caches[rand.Intn(len(caches))]
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		readAndCompareDigest(t, ctx, rc, d)
	}

	waitForShutdown(t, caches...)
}

func TestFindMissingBlobs(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNNodes(t, configs)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env)
	require.NoError(t, err)

	rc1 := caches[0]

	digestsWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		r, buf := testdigest.RandomCASResourceBuf(t, 100)
		digestsWritten = append(digestsWritten, r)
		writeDigest(t, ctx, rc1, r, buf)
		readAndCompareDigest(t, ctx, rc1, r)
	}

	missingDigests := make([]*rspb.ResourceName, 0)
	expectedMissingHashes := make([]string, 0)
	for i := 0; i < 10; i++ {
		r, _ := testdigest.RandomCASResourceBuf(t, 100)
		missingDigests = append(missingDigests, r)
		expectedMissingHashes = append(expectedMissingHashes, r.GetDigest().GetHash())
	}

	rns := append(digestsWritten, missingDigests...)
	missing, err := rc1.FindMissing(ctx, rns)
	require.NoError(t, err)

	missingHashes := make([]string, 0)
	for _, d := range missing {
		missingHashes = append(missingHashes, d.GetHash())
	}
	require.ElementsMatch(t, expectedMissingHashes, missingHashes)
	waitForShutdown(t, caches...)
}

func TestLRU(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.atime_update_threshold", 10*time.Second)
	flags.Set(t, "cache.raft.atime_write_batch_size", 1)
	flags.Set(t, "cache.raft.min_eviction_age", 0)
	flags.Set(t, "cache.raft.samples_per_batch", 50)
	flags.Set(t, "cache.raft.local_size_update_period", 100*time.Millisecond)
	flags.Set(t, "cache.raft.partition_usage_delta_bytes_threshold", 100)

	digestSize := int64(1000)
	numDigests := 25
	maxSizeBytes := int64(math.Ceil(14022 * (1 / usagetracker.EvictionCutoffThreshold))) // account for .9 evictor cutoff

	configs := getTestConfigs(t, 1)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env)
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	for _, c := range configs {
		c.env.SetClock(clock)
		c.config.Partitions = []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: maxSizeBytes,
			},
		}
	}

	caches := startNNodes(t, configs)
	rc1 := caches[0]
	quartile := numDigests / 4
	lastUsed := make(map[*rspb.ResourceName]time.Time, numDigests)
	resourceKeys := make([]*rspb.ResourceName, 0)
	for i := 0; i < numDigests; i++ {
		r, buf := testdigest.RandomCASResourceBuf(t, digestSize)
		writeDigest(t, ctx, rc1, r, buf)
		lastUsed[r] = clock.Now()
		resourceKeys = append(resourceKeys, r)
	}

	rc1.TestingFlush()

	clock.Advance(5 * time.Minute)
	// Use the digests in the following way:
	// 1) first 3 quartiles
	// 2) first 2 quartiles
	// 3) first quartile
	// This sets us up so we add an additional quartile of data
	// and then expect data from the 3rd quartile (least recently used)
	// to be the most evicted.
	for i := 3; i > 0; i-- {
		log.Printf("Using data from 0:%d", quartile*i)
		for j := 0; j < quartile*i; j++ {
			r := resourceKeys[j]
			_, err = rc1.Get(ctx, r)
			require.NoError(t, err)
			lastUsed[r] = clock.Now()
		}
		clock.Advance(5 * time.Minute)
	}

	// Write more data
	for i := 0; i < quartile; i++ {
		r, buf := testdigest.RandomCASResourceBuf(t, digestSize)
		writeDigest(t, ctx, rc1, r, buf)
		lastUsed[r] = clock.Now()
		resourceKeys = append(resourceKeys, r)
	}

	rc1.TestingWaitForGC()
	waitForShutdown(t, caches...)

	caches = startNNodes(t, configs)
	rc1 = caches[0]

	perfectLRUEvictees := make(map[*rspb.ResourceName]struct{})
	sort.Slice(resourceKeys, func(i, j int) bool {
		return lastUsed[resourceKeys[i]].Before(lastUsed[resourceKeys[j]])
	})
	for _, r := range resourceKeys[:quartile] {
		perfectLRUEvictees[r] = struct{}{}
	}
	// We expect no more than x keys to have been evicted
	// We expect *most* of the keys evicted to be older
	evictedCount := 0
	perfectEvictionCount := 0
	evictedAgeTotal := time.Duration(0)

	keptCount := 0
	keptAgeTotal := time.Duration(0)

	now := clock.Now()
	for r, usedAt := range lastUsed {
		ok, err := rc1.Contains(ctx, r)
		evicted := err != nil || !ok
		age := now.Sub(usedAt)
		if evicted {
			evictedCount++
			evictedAgeTotal += age
			if _, ok := perfectLRUEvictees[r]; ok {
				perfectEvictionCount++
			}
		} else {
			keptCount++
			keptAgeTotal += age
		}
	}

	avgEvictedAgeSeconds := evictedAgeTotal.Seconds() / float64(evictedCount)
	avgKeptAgeSeconds := keptAgeTotal.Seconds() / float64(keptCount)

	log.Printf("evictedCount: %d [%d perfect], keptCount: %d, quartile: %d", evictedCount, perfectEvictionCount, keptCount, quartile)
	log.Printf("evictedAgeTotal: %s, keptAgeTotal: %s", evictedAgeTotal, keptAgeTotal)
	log.Printf("avg evictedAge: %f, avg keptAge: %f", avgEvictedAgeSeconds, avgKeptAgeSeconds)

	// Check that mostly (80%) of evictions were perfect
	require.GreaterOrEqual(t, perfectEvictionCount, int(.80*float64(evictedCount)))
	// Check that total number of evictions was < quartile*2, so not too much
	// good stuff was evicted.
	require.LessOrEqual(t, evictedCount, quartile*2)
	// Check that the avg age of evicted items is older than avg age of kept items.
	require.Greater(t, avgEvictedAgeSeconds, avgKeptAgeSeconds)

	waitForShutdown(t, caches...)
}
