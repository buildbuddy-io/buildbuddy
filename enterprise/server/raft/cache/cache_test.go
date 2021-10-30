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

	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
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
	done := make(chan struct{})
	go func() {
		for {
			if allHealthy(rc1, rc2, rc3) {
				close(done)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		break
	case <-time.After(3 * time.Second):
		t.Fatal("Caches did not become healthy after 3s")
	}
}

func TestWriter(t *testing.T) {
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
	done := make(chan struct{})
	go func() {
		for {
			if allHealthy(rc1, rc2, rc3) {
				close(done)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		break
	case <-time.After(3 * time.Second):
		t.Fatal("Caches did not become healthy after 3s")
	}

	cache, err := rc1.WithIsolation(ctx, interfaces.CASCacheType, "remote/instance/name")
	require.Nil(t, err)

	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		writeCloser, err := cache.Writer(ctx, d)
		require.Nil(t, err, err)
		n, err := writeCloser.Write(buf)
		require.Nil(t, err)
		require.Equal(t, n, len(buf))
		err = writeCloser.Close()
		require.Nil(t, err, err)
	}
}

// Next Steps:
//  - implement reader
//  - write a test that writes some data, kills a node, writes more data, brings
//    it back up and ensures that all data is correctly replicated after some time
//  - write a test that writes data, nukes a nude, brings up a new one, and
//    ensures that all data is correctly replicated
