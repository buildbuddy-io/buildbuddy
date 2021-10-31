package cache_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	userMap = testauth.TestUsers("user1", "group1", "user2", "group2")
)

func getEnvAuthAndCtx(t *testing.T) (*testenv.TestEnv, *testauth.TestAuthenticator, context.Context) {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(userMap)
	te.SetAuthenticator(ta)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return te, ta, ctx
}

func getTmpDir(t *testing.T) string {
	dir, err := ioutil.TempDir("/tmp", "buildbuddy_diskcache_*")
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.EnsureDirectoryExists(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	})
	return dir
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *repb.Digest) {
	reader, err := c.Reader(ctx, d, 0)
	if err != nil {
		require.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	require.Equal(t, d.GetHash(), d1.GetHash())
}

func writeDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *repb.Digest, buf []byte) {
	writeCloser, err := c.Writer(ctx, d)
	if err != nil {
		require.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	n, err := writeCloser.Write(buf)
	require.Nil(t, err)
	require.Equal(t, n, len(buf))
	err = writeCloser.Close()
	require.Nil(t, err, err)
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", app.FreePort(t))
}

func getCacheConfig(t *testing.T, listenAddr string, join []string) *raft_cache.Config {
	return &raft_cache.Config{
		RootDir:       getTmpDir(t),
		ListenAddress: listenAddr,
		Join:          join,
		HTTPPort:      app.FreePort(t),
		GRPCPort:      app.FreePort(t),
	}
}

func allHealthy(caches ...*raft_cache.RaftCache) bool {
	for _, c := range caches {
		if err := c.Check(context.Background()); err != nil {
			return false
		}
		log.Printf("%+v is healthy!", c)
	}
	return true
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

func waitForHealthy(t *testing.T, timeout time.Duration, caches ...*raft_cache.RaftCache) {
	done := make(chan struct{})
	go func() {
		for {
			if allHealthy(caches...) {
				close(done)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		break
	case <-time.After(timeout):
		t.Fatalf("Caches did not become healthy after %s", timeout)
	}
}

func waitForShutdown(t *testing.T, timeout time.Duration, caches ...*raft_cache.RaftCache) {
	done := make(chan struct{})
	go func() {
		parallelShutdown(caches...)
		close(done)
	}()

	select {
	case <-done:
		break
	case <-time.After(timeout):
		t.Fatalf("Caches did not shutdown after %s", timeout)
	}
}

func TestAutoBringup(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env := testenv.GetTestEnv(t)

	// startup 3 cache nodes
	rc1, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
	require.Nil(t, err)
	rc2, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
	require.Nil(t, err)
	rc3, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l3, join))
	require.Nil(t, err)

	// wait for them all to become healthy
	waitForHealthy(t, 3*time.Second, rc1, rc2, rc3)
	waitForShutdown(t, 1*time.Second, rc1, rc2, rc3)
}

func TestReaderAndWriter(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env, _, ctx := getEnvAuthAndCtx(t)

	// startup 3 cache nodes
	rc1, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
	require.Nil(t, err)
	rc2, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
	require.Nil(t, err)
	rc3, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l3, join))
	require.Nil(t, err)

	// wait for them all to become healthy
	waitForHealthy(t, 3*time.Second, rc1, rc2, rc3)

	cache, err := rc1.WithIsolation(ctx, interfaces.CASCacheType, "remote/instance/name")
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		writeDigest(t, ctx, cache, d, buf)
		readAndCompareDigest(t, ctx, cache, d)
	}
	waitForShutdown(t, 1*time.Second, rc1, rc2, rc3)
}

func TestCacheShutdown(t *testing.T) {
	l1 := localAddr(t)
	l2 := localAddr(t)
	l3 := localAddr(t)
	join := []string{l1, l2, l3}

	env, _, ctx := getEnvAuthAndCtx(t)

	// startup 3 cache nodes
	rc1, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l1, join))
	require.Nil(t, err)
	rc2, err := raft_cache.NewRaftCache(env, getCacheConfig(t, l2, join))
	require.Nil(t, err)

	rc3Config := getCacheConfig(t, l3, join)
	rc3, err := raft_cache.NewRaftCache(env, rc3Config)
	require.Nil(t, err)

	// wait for them all to become healthy
	waitForHealthy(t, 3*time.Second, rc1, rc2, rc3)

	cache, err := rc1.WithIsolation(ctx, interfaces.CASCacheType, "remote/instance/name")
	require.Nil(t, err)

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		writeDigest(t, ctx, cache, d, buf)
		digestsWritten = append(digestsWritten, d)
	}

	// shutdown one node
	waitForShutdown(t, 1*time.Second, rc3)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		writeDigest(t, ctx, cache, d, buf)
		digestsWritten = append(digestsWritten, d)
	}

	rc3, err = raft_cache.NewRaftCache(env, rc3Config)
	require.Nil(t, err)
	waitForHealthy(t, 3*time.Second, rc3)

	cache, err = rc1.WithIsolation(ctx, interfaces.CASCacheType, "remote/instance/name")
	require.Nil(t, err)

	for _, d := range digestsWritten {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		readAndCompareDigest(t, ctx, cache, d)
	}
	waitForShutdown(t, 1*time.Second, rc1, rc2, rc3)
}
