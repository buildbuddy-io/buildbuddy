package distributed

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func getAnonContext(t *testing.T) context.Context {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func newMemoryCache(t *testing.T, maxSizeBytes int64) interfaces.Cache {
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return mc
}

func waitForReady(t *testing.T, addr string, maxWait time.Duration) {
	log.Printf("Waiting for peer %q to become ready!", addr)
	for {
		select {
		case <-time.After(maxWait):
			t.Fatalf("%q did not become ready in %s", addr, maxWait)
			return
		default:
			conn, err := net.Dial("tcp", addr)
			if conn != nil {
				conn.Close()
				return
			}
			log.Printf("Got err: %s", err)
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func startNewDCache(t *testing.T, te environment.Env, config CacheConfig, baseCache interfaces.Cache) *Cache {
	c, err := NewDistributedCache(te, baseCache, config, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	c.StartListening()
	waitForReady(t, config.ListenAddr, 100*time.Millisecond)
	//	t.Cleanup(func() {
	//		shutdownCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	//		c.Shutdown(shutdownCtx)
	//		cancel()
	//	})
	return c
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *repb.Digest) {
	reader, err := c.Reader(ctx, d, 0)
	if err != nil {
		assert.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	assert.Equal(t, d.GetHash(), d1.GetHash())
}

func TestBasicReadWrite(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t)
	singleCacheSizeBytes := int64(1024)
	peer1 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer2 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer3 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, te, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, te, config2, memoryCache2)
	_ = dc2

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, te, config3, memoryCache3)
	_ = dc3

	baseCaches := []interfaces.Cache{
		memoryCache1,
		memoryCache2,
		memoryCache3,
	}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}
}

func TestReadWriteWithFailedNode(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t)
	singleCacheSizeBytes := int64(1024)
	peer1 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer2 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer3 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, te, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, te, config2, memoryCache2)
	_ = dc2

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, te, config3, memoryCache3)
	_ = dc3

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2}
	distributedCaches := []interfaces.Cache{dc1, dc2}

	// "Fail" a a node by shutting it down.
	// The basecache and distributed cache are not in baseCaches
	// or distributedCaches so they should not be referenced
	// below when reading / writing, although the running nodes
	// still have reference to them via the Nodes list.
	shutdownCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err := dc3.Shutdown(shutdownCtx)
	cancel()
	assert.Nil(t, err)

	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		if err := distributedCaches[i%2].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}
}

func TestReadWriteWithFailedAndRestoredNode(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t)
	singleCacheSizeBytes := int64(100000)
	peer1 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer2 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer3 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, te, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, te, config2, memoryCache2)
	_ = dc2

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, te, config3, memoryCache3)
	_ = dc3

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2}
	distributedCaches := []*Cache{dc1, dc2}

	// "Fail" a a node by shutting it down.
	// The basecache and distributed cache are not in baseCaches
	// or distributedCaches so they should not be referenced
	// below when reading / writing, although the running nodes
	// still have reference to them via the Nodes list.
	shutdownCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err := dc3.Shutdown(shutdownCtx)
	cancel()
	assert.Nil(t, err)

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		log.Printf("About to set value in cache!")
		if err := distributedCaches[i%2].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		log.Printf("set value in cache!")
		digestsWritten = append(digestsWritten, d)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}

	baseCaches = append(baseCaches, memoryCache3)
	distributedCaches = append(distributedCaches, dc3)
	dc3.StartListening()

	for _, d := range digestsWritten {
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, d)
		}
	}
}

func TestBackfill(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx := getAnonContext(t)
	singleCacheSizeBytes := int64(100000)
	peer1 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer2 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	peer3 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, te, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, te, config2, memoryCache2)
	_ = dc2

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, te, config3, memoryCache3)
	_ = dc3

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2}
	distributedCaches := []*Cache{dc1, dc2}

	// "Fail" a a node by shutting it down.
	// The basecache and distributed cache are not in baseCaches
	// or distributedCaches so they should not be referenced
	// below when reading / writing, although the running nodes
	// still have reference to them via the Nodes list.
	shutdownCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err := dc3.Shutdown(shutdownCtx)
	cancel()
	assert.Nil(t, err)

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		log.Printf("About to set value in cache!")
		if err := distributedCaches[i%2].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		log.Printf("set value in cache!")
		digestsWritten = append(digestsWritten, d)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}

	// Restore the node.
	baseCaches = append(baseCaches, memoryCache3)
	distributedCaches = append(distributedCaches, dc3)
	dc3.StartListening()
	waitForReady(t, peer3, 100*time.Millisecond)

	// Read our digests, and ensure that after each read, the digest
	// is *also* present in the base cache of the shutdown node,
	// because it has been backfilled.
	for _, d := range digestsWritten {
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, d)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}
}
