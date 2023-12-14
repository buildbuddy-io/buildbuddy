package cache_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
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

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *rspb.ResourceName) {
	reader, err := c.Reader(ctx, d, 0, 0)
	if err != nil {
		require.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	require.Equal(t, d.GetDigest().GetHash(), d1.GetHash())
}

func writeDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *rspb.ResourceName, buf []byte) {
	writeCloser, err := c.Writer(ctx, d)
	if err != nil {
		require.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
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

func startNNodes(t *testing.T, n int) ([]*raft_cache.RaftCache, []*testenv.TestEnv) {
	eg := errgroup.Group{}
	caches := make([]*raft_cache.RaftCache, n)
	envs := make([]*testenv.TestEnv, n)

	joinList := make([]string, 0, n)
	for i := 0; i < n; i++ {
		joinList = append(joinList, localAddr(t))
	}

	for i := 0; i < n; i++ {
		i := i
		lN := joinList[i]
		joinList := joinList
		env := getTestEnv(t)
		gs, err := gossip.New("name-"+lN, lN, joinList)
		require.NoError(t, err)
		env.SetGossipService(gs)
		envs[i] = env
		eg.Go(func() error {
			n, err := raft_cache.NewRaftCache(env, getCacheConfig(t))
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
	return caches, envs
}

func TestAutoBringup(t *testing.T) {
	caches, _ := startNNodes(t, 3)
	waitForShutdown(t, caches...)
}

func TestReaderAndWriter(t *testing.T) {
	caches, envs := startNNodes(t, 3)
	rc1 := caches[0]

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), envs[0])
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

	caches, envs := startNNodes(t, 3)
	rc1 := caches[0]
	rc2 := caches[1]

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), envs[0])
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
	caches, envs := startNNodes(t, 5)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), envs[0])
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
	caches, envs := startNNodes(t, 3)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), envs[0])
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
