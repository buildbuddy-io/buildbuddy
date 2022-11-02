package cache_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
)

var (
	userMap = testauth.TestUsers("user1", "group1", "user2", "group2")
)

func getEnvAuthAndCtx(t *testing.T) (*testenv.TestEnv, *testauth.TestAuthenticator, context.Context) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "auth.enable_anonymous_usage", true)
	ta := testauth.NewTestAuthenticator(userMap)
	te.SetAuthenticator(ta)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return te, ta, ctx
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *resource.ResourceName) {
	reader, err := c.Reader(ctx, d, 0, 0)
	if err != nil {
		require.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	require.Equal(t, d.GetDigest().GetHash(), d1.GetHash())
}

func writeDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *resource.ResourceName, buf []byte) {
	writeCloser, err := c.Writer(ctx, d)
	if err != nil {
		require.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	n, err := writeCloser.Write(buf)
	require.NoError(t, err)
	require.Equal(t, n, len(buf))
	err = writeCloser.Close()
	require.NoError(t, err)
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func getCacheConfig(t *testing.T, listenAddr string, join []string) *raft_cache.Config {
	return &raft_cache.Config{
		RootDir:       testfs.MakeTempDir(t),
		ListenAddress: listenAddr,
		Join:          join,
		HTTPPort:      testport.FindFree(t),
		GRPCPort:      testport.FindFree(t),
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
	for _, cache := range caches {
		cache := cache
		eg.Go(func() error {
			cache.Stop()
			return nil
		})
	}
	eg.Wait()
}

func waitForHealthy(t *testing.T, caches ...*raft_cache.RaftCache) {
	start := time.Now()
	timeout := 10 * time.Second
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

func TestAutoBringup(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env := testenv.GetTestEnv(t)

	var rc1, rc2, rc3 *raft_cache.RaftCache
	eg := errgroup.Group{}

	// startup 3 cache nodes
	eg.Go(func() error {
		var err error
		rc1, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc2, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc3, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l3, join))
		return err
	})
	err := eg.Wait()
	require.NoError(t, err)

	// wait for them all to become healthy
	waitForHealthy(t, rc1, rc2, rc3)
	waitForShutdown(t, rc1, rc2, rc3)
}

func TestReaderAndWriter(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env, _, ctx := getEnvAuthAndCtx(t)

	var rc1, rc2, rc3 *raft_cache.RaftCache
	eg := errgroup.Group{}

	// startup 3 cache nodes
	eg.Go(func() error {
		var err error
		rc1, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc2, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc3, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l3, join))
		return err
	})
	require.Nil(t, eg.Wait())

	// wait for them all to become healthy
	waitForHealthy(t, rc1, rc2, rc3)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: "remote/instance/name",
		}
		writeDigest(t, ctx, rc1, r, buf)
		readAndCompareDigest(t, ctx, rc1, r)
	}
	waitForShutdown(t, rc1, rc2, rc3)
}

func TestCacheShutdown(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env, _, ctx := getEnvAuthAndCtx(t)

	var rc1, rc2, rc3 *raft_cache.RaftCache
	eg := errgroup.Group{}

	// startup 3 cache nodes
	eg.Go(func() error {
		var err error
		rc1, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc2, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
		return err
	})

	rc3Config := getCacheConfig(t, l3, join)
	eg.Go(func() error {
		var err error
		rc3, err = raft_cache.NewRaftCache(env, rc3Config)
		return err
	})
	err := eg.Wait()
	require.NoError(t, err)

	// wait for them all to become healthy
	waitForHealthy(t, rc1, rc2, rc3)

	cacheRPCTimeout := 5 * time.Second
	digestsWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := digest.NewCASResourceName(d, "remote/instance/name").ToProto()
		writeDigest(t, ctx, rc1, rn, buf)
		digestsWritten = append(digestsWritten, rn)
	}

	// shutdown one node
	waitForShutdown(t, rc3)

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := digest.NewCASResourceName(d, "remote/instance/name").ToProto()
		writeDigest(t, ctx, rc1, rn, buf)
		digestsWritten = append(digestsWritten, rn)
	}

	rc3, err = raft_cache.NewRaftCache(env, rc3Config)
	require.NoError(t, err)
	waitForHealthy(t, rc3)

	for _, d := range digestsWritten {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		readAndCompareDigest(t, ctx, rc3, d)
	}

	waitForShutdown(t, rc1, rc2, rc3)
}

func TestDistributedRanges(t *testing.T) {
	t.Skip()
	numNodes := 5
	addrs := make([]string, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		addrs = append(addrs, localAddr(t))
	}
	join := addrs[:3]
	env, _, ctx := getEnvAuthAndCtx(t)

	mu := sync.Mutex{}
	raftCaches := make([]*raft_cache.RaftCache, 0)
	eg := errgroup.Group{}
	for i := 0; i < numNodes; i++ {
		localAddr := addrs[i]
		eg.Go(func() error {
			c, err := raft_cache.NewRaftCache(env, getCacheConfig(t, localAddr, join))
			if err != nil {
				return err
			}
			mu.Lock()
			raftCaches = append(raftCaches, c)
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("Err starting caches: %s", err)
	}
	waitForHealthy(t, raftCaches...)

	digests := make([]*resource.ResourceName, 0)
	for i := 0; i < 10; i++ {
		rc := raftCaches[rand.Intn(len(raftCaches))]

		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: "remote/instance/name",
		}
		writeDigest(t, ctx, rc, r, buf)
	}

	for _, d := range digests {
		rc := raftCaches[rand.Intn(len(raftCaches))]
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		readAndCompareDigest(t, ctx, rc, d)
	}

	waitForShutdown(t, raftCaches...)
}

func TestFindMissingBlobs(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env, _, ctx := getEnvAuthAndCtx(t)

	var rc1, rc2, rc3 *raft_cache.RaftCache
	eg := errgroup.Group{}

	// startup 3 cache nodes
	eg.Go(func() error {
		var err error
		rc1, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc2, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
		return err
	})
	eg.Go(func() error {
		var err error
		rc3, err = raft_cache.NewRaftCache(env, getCacheConfig(t, l3, join))
		return err
	})
	require.Nil(t, eg.Wait())

	// wait for them all to become healthy
	waitForHealthy(t, rc1, rc2, rc3)

	digestsWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: "remote/instance/name",
		}
		digestsWritten = append(digestsWritten, r)
		writeDigest(t, ctx, rc1, r, buf)
	}

	missingDigests := make([]*resource.ResourceName, 0)
	expectedMissingHashes := make([]string, 0)
	for i := 0; i < 10; i++ {
		d, _ := testdigest.NewRandomDigestBuf(t, 100)
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: "remote/instance/name",
		}

		missingDigests = append(missingDigests, r)
		expectedMissingHashes = append(expectedMissingHashes, d.GetHash())
	}

	rns := append(digestsWritten, missingDigests...)
	missing, err := rc1.FindMissing(ctx, rns)
	require.NoError(t, err)

	missingHashes := make([]string, 0)
	for _, d := range missing {
		missingHashes = append(missingHashes, d.GetHash())
	}
	require.ElementsMatch(t, expectedMissingHashes, missingHashes)
	waitForShutdown(t, rc1, rc2, rc3)
}
