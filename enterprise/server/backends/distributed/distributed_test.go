package distributed

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	userMap = testauth.TestUsers("user1", "group1", "user2", "group2")

	// The maximum duration to wait for a server to become ready.
	maxWaitForReadyDuration = 3 * time.Second

	// The maximum duration to wait for a server to shut down.
	maxShutdownDuration = 3 * time.Second
)

func getEnvAuthAndCtx(t *testing.T) (*testenv.TestEnv, *testauth.TestAuthenticator, context.Context) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(userMap)
	te.SetAuthenticator(ta)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return te, ta, ctx
}

func newMemoryCache(t *testing.T, maxSizeBytes int64) interfaces.Cache {
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return mc
}

func waitForReady(t *testing.T, addr string) {
	conn, err := grpc_client.DialTargetWithOptions("grpc://"+addr, false, grpc.WithBlock(), grpc.WithTimeout(maxWaitForReadyDuration))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}
func startNewDCache(t *testing.T, te environment.Env, config CacheConfig, baseCache interfaces.Cache) *Cache {
	c, err := NewDistributedCache(te, baseCache, config, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	ic, err := c.WithIsolation(context.Background(), resource.CacheType_CAS, "" /* =remoteInstanceName */)
	require.NoError(t, err)
	c = ic.(*Cache)
	c.StartListening()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), maxShutdownDuration)
		c.Shutdown(shutdownCtx)
		cancel()
	})
	return c
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, d *repb.Digest) {
	reader, err := c.Reader(ctx, d, 0, 0)
	if err != nil {
		assert.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	assert.Equal(t, d.GetHash(), d1.GetHash())
}

func TestBasicReadWrite(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

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
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}
}

func TestReadMaxOffset(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	// Do a write, and ensure it was written to all nodes.
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	if err := distributedCaches[0].Set(ctx, d, buf); err != nil {
		t.Fatal(err)
	}

	reader, err := distributedCaches[1].Reader(ctx, d, d.GetSizeBytes(), 0)
	if err != nil {
		assert.FailNow(t, fmt.Sprintf("cache: %+v", distributedCaches[1]), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", d1.GetHash())
}

func TestReadOffsetLimit(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	// Do a write, and ensure it was written to all nodes.
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	if err := distributedCaches[0].Set(ctx, d, buf); err != nil {
		t.Fatal(err)
	}

	offset := int64(2)
	limit := int64(3)
	reader, err := distributedCaches[1].Reader(ctx, d, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, d.GetSizeBytes())
	n, err := reader.Read(readBuf)
	require.EqualValues(t, limit, n)
	require.Equal(t, buf[offset:offset+limit], readBuf[:limit])
}

func TestReadWriteWithFailedNode(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer4 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3, peer4},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 4 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	memoryCache4 := newMemoryCache(t, singleCacheSizeBytes)
	config4 := baseConfig
	config4.ListenAddr = peer4
	dc4 := startNewDCache(t, env, config4, memoryCache4)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)
	waitForReady(t, config4.ListenAddr)

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2, memoryCache4}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc4}

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
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}
}

func TestReadWriteWithFailedAndRestoredNode(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer4 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3, peer4},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 4 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	memoryCache4 := newMemoryCache(t, singleCacheSizeBytes)
	config4 := baseConfig
	config4.ListenAddr = peer4
	dc4 := startNewDCache(t, env, config4, memoryCache4)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)
	waitForReady(t, config4.ListenAddr)

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2, memoryCache4}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc4}

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
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, d)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}

	baseCaches = append(baseCaches, memoryCache3)
	distributedCaches = append(distributedCaches, dc3)
	dc3.StartListening()
	waitForReady(t, config3.ListenAddr)
	for _, d := range digestsWritten {
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, d)
		}
	}
}

func TestBackfill(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:    3,
		Nodes:                []string{peer1, peer2, peer3},
		DisableLocalLookup:   true,
		RPCHeartbeatInterval: 100 * time.Millisecond,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2, memoryCache3}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, d)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}

	// Now zero out one of the base caches.
	for _, d := range digestsWritten {
		if err := memoryCache3.Delete(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	// Read our digests, and ensure that after each read, the digest
	// is *also* present in the base cache of the zeroed-out node,
	// because it has been backfilled.
	for _, d := range digestsWritten {
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, d)
		}
		for i, baseCache := range baseCaches {
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err, fmt.Sprintf("basecache %dmissing digest", i))
			assert.True(t, exists, fmt.Sprintf("basecache %dmissing digest", i))
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}
}

func TestContainsMulti(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	baseCaches := []interfaces.Cache{
		memoryCache1,
		memoryCache2,
		memoryCache3,
	}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, d)
	}

	for _, baseCache := range baseCaches {
		missingMap, err := baseCache.FindMissing(ctx, digestsWritten)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(missingMap))
	}

	for _, distributedCache := range distributedCaches {
		missingMap, err := distributedCache.FindMissing(ctx, digestsWritten)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(missingMap))
	}
}

func TestMetadata(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000,
	}
	for i, testSize := range testSizes {
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		// Set() the bytes in the cache.
		err := distributedCaches[i%3].Set(ctx, d, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}

		for _, dc := range distributedCaches {
			// Metadata should return true size of the blob, regardless of queried size.
			md, err := dc.Metadata(ctx, &repb.Digest{Hash: d.GetHash(), SizeBytes: 1})
			if err != nil {
				t.Fatalf("Error getting %q metadata from cache: %s", d.GetHash(), err.Error())
			}
			require.Equal(t, testSize, md.SizeBytes)
		}
	}
}

func TestFindMissing(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	baseCaches := []interfaces.Cache{
		memoryCache1,
		memoryCache2,
		memoryCache3,
	}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, d)
	}

	// Generate some more digests, but don't write them to the cache.
	digestsNotWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		d, _ := testdigest.NewRandomDigestBuf(t, 100)
		digestsNotWritten = append(digestsNotWritten, d)
	}

	allDigests := append(digestsWritten, digestsNotWritten...)

	for _, baseCache := range baseCaches {
		missing, err := baseCache.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.ElementsMatch(t, missing, digestsNotWritten)
	}

	for _, distributedCache := range distributedCaches {
		missing, err := distributedCache.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.ElementsMatch(t, missing, digestsNotWritten)
	}
}

func TestGetMulti(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	baseCaches := []interfaces.Cache{
		memoryCache1,
		memoryCache2,
		memoryCache3,
	}
	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, d)
	}

	for _, baseCache := range baseCaches {
		gotMap, err := baseCache.GetMulti(ctx, digestsWritten)
		assert.Nil(t, err)
		for _, d := range digestsWritten {
			buf, ok := gotMap[d]
			assert.True(t, ok)
			assert.Equal(t, d.GetSizeBytes(), int64(len(buf)))
		}
	}

	for _, distributedCache := range distributedCaches {
		gotMap, err := distributedCache.GetMulti(ctx, digestsWritten)
		assert.Nil(t, err)
		for _, d := range digestsWritten {
			buf, ok := gotMap[d]
			assert.True(t, ok)
			assert.Equal(t, d.GetSizeBytes(), int64(len(buf)))
		}
	}
}

func TestHintedHandoff(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)

	// Authenticate as user1.
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer4 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3, peer4},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 4 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	memoryCache4 := newMemoryCache(t, singleCacheSizeBytes)
	config4 := baseConfig
	config4.ListenAddr = peer4
	dc4 := startNewDCache(t, env, config4, memoryCache4)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)
	waitForReady(t, config4.ListenAddr)

	baseCaches := []interfaces.Cache{memoryCache1, memoryCache2, memoryCache4}
	distributedCaches := []*Cache{dc1, dc2, dc4}

	// "Fail" a a node by shutting it down.
	// The basecache and distributed cache are not in baseCaches
	// or distributedCaches so they should not be referenced
	// below when reading / writing, although the running nodes
	// still have reference to them via the Nodes list.
	shutdownCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err = dc3.Shutdown(shutdownCtx)
	cancel()
	assert.Nil(t, err)

	digestsWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, d, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, d)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, d)
		}
	}

	// Restart the downed node -- as soon as it's back up, it should
	// receive hinted handoffs from the other peers.
	baseCaches = append(baseCaches, memoryCache3)
	distributedCaches = append(distributedCaches, dc3)
	dc3.StartListening()
	waitForReady(t, config3.ListenAddr)

	// Wait for all peers to finish their backfill requests.
	for _, distributedCache := range distributedCaches {
		for _, backfillChannel := range distributedCache.hintedHandoffsByPeer {
			if backfillChannel == nil {
				continue
			}
			for len(backfillChannel) > 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// Figure out the set of digests that were hinted-handoffs. We'll verify
	// that these were successfully handed off below.
	hintedHandoffs := make([]*repb.Digest, 0)
	for _, d := range digestsWritten {
		ps := dc3.readPeers(d)
		for _, p := range ps.PreferredPeers {
			if p == peer3 {
				hintedHandoffs = append(hintedHandoffs, d)
				break
			}
		}
	}

	// Ensure that dc3 successfully received all the hinted handoffs.
	for _, d := range hintedHandoffs {
		exists, err := memoryCache3.ContainsDeprecated(ctx, d)
		assert.Nil(t, err)
		assert.True(t, exists)
		readAndCompareDigest(t, ctx, dc3, d)
	}
}

func TestDelete(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

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
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.NoError(t, err)
			assert.True(t, exists)
		}

		// Do a delete, and verify no nodes still have the data.
		if err := distributedCaches[i%3].Delete(ctx, d); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.ContainsDeprecated(ctx, d)
			assert.NoError(t, err)
			assert.False(t, exists)
		}
	}
}

func TestDelete_NonExistentFile(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := newMemoryCache(t, singleCacheSizeBytes)
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	for i := 0; i < 100; i++ {
		d, _ := testdigest.NewRandomDigestBuf(t, 100)

		// Do a delete on a file that does not exist.
		err := distributedCaches[i%3].Delete(ctx, d)
		assert.NoError(t, err)
	}
}
