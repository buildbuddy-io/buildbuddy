package cache_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
	
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"	

	raft_cache "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache"
)

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
		RootDir: getTmpDir(t),
		ListenAddress: listenAddr,
		Join: join,
		HTTPPort: app.FreePort(t),
		GRPCPort: app.FreePort(t),
	}
}

func allHealthy(caches ...*raft_cache.RaftCache) bool {
	for _, c := range caches {
		if err := c.Check(context.Background()); err != nil {
			return false
		}
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

