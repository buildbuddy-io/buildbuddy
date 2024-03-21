package distributed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
	conn, err := grpc_client.DialSimple("grpc://"+addr, grpc.WithBlock(), grpc.WithTimeout(maxWaitForReadyDuration))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func waitForShutdown(c *Cache) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), maxShutdownDuration)
	c.Shutdown(shutdownCtx)
	cancel()
}

func startNewDCache(t *testing.T, te environment.Env, config CacheConfig, baseCache interfaces.Cache) *Cache {
	c, err := NewDistributedCache(te, baseCache, config, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	c.StartListening()
	t.Cleanup(func() {
		waitForShutdown(c)
	})
	return c
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, r *rspb.ResourceName) {
	reader, err := c.Reader(ctx, r, 0, 0)
	if err != nil {
		assert.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	assert.Equal(t, r.GetDigest().GetHash(), d1.GetHash())
}

func TestBasicReadWrite(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	metrics.DistributedCachePeerLookups.Reset()
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
		// Do a write, and ensure we can read it back from each node,
		// both via the base cache and distributed cache for each node.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, rn)
		}
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, rn)
		}
	}

	peerLookupMetrics := testmetrics.HistogramVecValues(t, metrics.DistributedCachePeerLookups)
	assert.Equal(t, []testmetrics.HistogramValues{
		{
			Labels: map[string]string{
				"op":     "Reader",
				"status": "hit",
			},
			// For each of the 100 objects, we did a Read on all 3 distributed
			// caches, which should have done only 1 lookup each (from the local
			// cache).
			Buckets: map[float64]uint64{1: 300},
		},
	}, peerLookupMetrics)
}

func TestReadWrite_Compression(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)

	testCases := []struct {
		name             string
		writeCompression repb.Compressor_Value
		readCompression  repb.Compressor_Value
	}{
		{
			name:             "Write compressed, read compressed",
			writeCompression: repb.Compressor_ZSTD,
			readCompression:  repb.Compressor_ZSTD,
		},
		{
			name:             "Write compressed, read decompressed",
			writeCompression: repb.Compressor_ZSTD,
			readCompression:  repb.Compressor_IDENTITY,
		},
		{
			name:             "Write decompressed, read decompressed",
			writeCompression: repb.Compressor_IDENTITY,
			readCompression:  repb.Compressor_IDENTITY,
		},
		{
			name:             "Write decompressed, read compressed",
			writeCompression: repb.Compressor_IDENTITY,
			readCompression:  repb.Compressor_ZSTD,
		},
	}

	for _, tc := range testCases {
		{
			// Setup distributed cache
			peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
			peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
			peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
			baseConfig := CacheConfig{
				ReplicationFactor:  3,
				Nodes:              []string{peer1, peer2, peer3},
				DisableLocalLookup: true,
			}

			config1 := baseConfig
			config1.ListenAddr = peer1
			cache1 := &testcompression.CompressionCache{Cache: env.GetCache()}
			dc1 := startNewDCache(t, env, config1, cache1)

			config2 := baseConfig
			config2.ListenAddr = peer2
			cache2 := &testcompression.CompressionCache{Cache: env.GetCache()}
			dc2 := startNewDCache(t, env, config2, cache2)

			config3 := baseConfig
			config3.ListenAddr = peer3
			cache3 := &testcompression.CompressionCache{Cache: env.GetCache()}
			dc3 := startNewDCache(t, env, config3, cache3)

			waitForReady(t, config1.ListenAddr)
			waitForReady(t, config2.ListenAddr)
			waitForReady(t, config3.ListenAddr)

			baseCaches := []interfaces.Cache{
				cache1,
				cache2,
				cache3,
			}
			distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

			for i := 0; i < 10; i++ {
				// Do a write, and ensure it was written to all nodes.
				rn, buf := testdigest.RandomCASResourceBuf(t, 100)
				writeRN := rn.CloneVT()
				writeRN.Compressor = tc.writeCompression
				compressedBuf := compression.CompressZstd(nil, buf)

				bufToWrite := buf
				if tc.writeCompression == repb.Compressor_ZSTD {
					bufToWrite = compressedBuf
				}
				err := distributedCaches[i%3].Set(ctx, writeRN, bufToWrite)
				require.NoError(t, err)

				readRN := rn.CloneVT()
				readRN.Compressor = tc.readCompression
				bufToRead := buf
				if tc.readCompression == repb.Compressor_ZSTD {
					bufToRead = compressedBuf
				}
				for _, baseCache := range baseCaches {
					actual, err := baseCache.Get(ctx, readRN)
					assert.NoError(t, err)
					require.True(t, bytes.Equal(bufToRead, actual))
				}
			}
		}
	}
}

func TestContains(t *testing.T) {
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

	// Do a write - should be written to all nodes
	rn1, buf := testdigest.RandomACResourceBuf(t, 100)
	if err := dc1.Set(ctx, rn1, buf); err != nil {
		require.NoError(t, err)
	}

	c, err := dc1.Contains(ctx, rn1)
	require.NoError(t, err)
	require.True(t, c)

	c, err = dc2.Contains(ctx, rn1)
	require.NoError(t, err)
	require.True(t, c)

	c, err = dc3.Contains(ctx, rn1)
	require.NoError(t, err)
	require.True(t, c)
}

func TestContains_NotWritten(t *testing.T) {
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

	// Data is not written - no nodes should contain it
	r, _ := testdigest.RandomCASResourceBuf(t, 100)

	c, err := dc1.Contains(ctx, r)
	require.NoError(t, err)
	require.False(t, c)

	c, err = dc2.Contains(ctx, r)
	require.NoError(t, err)
	require.False(t, c)

	c, err = dc3.Contains(ctx, r)
	require.NoError(t, err)
	require.False(t, c)
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
	rn, buf := testdigest.RandomCASResourceBuf(t, 100)

	if err := distributedCaches[0].Set(ctx, rn, buf); err != nil {
		t.Fatal(err)
	}

	reader, err := distributedCaches[1].Reader(ctx, rn, rn.GetDigest().GetSizeBytes(), 0)
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
	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	if err := distributedCaches[0].Set(ctx, rn, buf); err != nil {
		t.Fatal(err)
	}

	offset := int64(2)
	limit := int64(3)
	reader, err := distributedCaches[1].Reader(ctx, rn, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, rn.GetDigest().GetSizeBytes())
	n, err := io.ReadFull(reader, readBuf)
	require.Error(t, err)
	require.Equal(t, "unexpected EOF", err.Error())
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
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, rn)
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

	resourcesWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, rn)
		}
	}

	distributedCaches = append(distributedCaches, dc3)
	dc3.StartListening()
	waitForReady(t, config3.ListenAddr)
	for _, r := range resourcesWritten {
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, r)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, r)
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

	resourcesWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, rn)
		}
	}

	// Now zero out one of the base caches.
	for _, r := range resourcesWritten {
		if err := memoryCache3.Delete(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	// Read our digests, and ensure that after each read, the digest
	// is *also* present in the base cache of the zeroed-out node,
	// because it has been backfilled.
	for _, r := range resourcesWritten {
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, r)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, r)
		}
		for i, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, r)
			assert.Nil(t, err, fmt.Sprintf("basecache %dmissing digest", i))
			assert.True(t, exists, fmt.Sprintf("basecache %dmissing digest", i))
			readAndCompareDigest(t, ctx, baseCache, r)
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

	resourcesWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
	}

	for _, baseCache := range baseCaches {
		missingMap, err := baseCache.FindMissing(ctx, resourcesWritten)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(missingMap))
	}

	for _, distributedCache := range distributedCaches {
		missingMap, err := distributedCache.FindMissing(ctx, resourcesWritten)
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
		rn, buf := testdigest.NewRandomResourceAndBuf(t, testSize, rspb.CacheType_CAS, "blah")
		err := distributedCaches[i%3].Set(ctx, rn, buf)
		require.NoError(t, err)

		for _, dc := range distributedCaches {
			// Metadata should return true size of the blob, regardless of queried size.
			rn2 := proto.Clone(rn).(*rspb.ResourceName)
			rn2.Digest.SizeBytes = 1
			md, err := dc.Metadata(ctx, rn2)
			require.NoError(t, err)
			require.Equal(t, testSize, md.StoredSizeBytes)
		}
	}
}

func TestFindMissing(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	metrics.DistributedCachePeerLookups.Reset()
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

	resourcesWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
	}

	// Generate some more digests, but don't write them to the cache.
	resourcesNotWritten := make([]*rspb.ResourceName, 0)
	digestsNotWritten := make([]*repb.Digest, 0)
	for i := 0; i < 70; i++ {
		rn, _ := testdigest.RandomCASResourceBuf(t, 100)
		resourcesNotWritten = append(resourcesNotWritten, rn)
		digestsNotWritten = append(digestsNotWritten, rn.GetDigest())
	}

	allResources := append(resourcesWritten, resourcesNotWritten...)
	for _, baseCache := range baseCaches {
		missing, err := baseCache.FindMissing(ctx, allResources)
		require.NoError(t, err)
		require.ElementsMatch(t, missing, digestsNotWritten)
	}

	for _, distributedCache := range distributedCaches {
		missing, err := distributedCache.FindMissing(ctx, allResources)
		require.NoError(t, err)
		require.ElementsMatch(t, missing, digestsNotWritten)
	}

	peerLookupMetrics := testmetrics.HistogramVecValues(t, metrics.DistributedCachePeerLookups)
	assert.Equal(t, []testmetrics.HistogramValues{
		{
			Labels: map[string]string{
				"op":     "FindMissing",
				"status": "hit",
			},
			// Each hit only requires 1 peer lookup, and we did 3 FindMissing
			// calls (loop count above) * 100 hits for each call (number of
			// digests written).
			Buckets: map[float64]uint64{1: 300},
		},
		{
			Labels: map[string]string{
				"op":     "FindMissing",
				"status": "miss",
			},
			// Each miss should result in 3 peer lookups (replication factor),
			// and we did 3 FindMissing calls (loop count above) * 70 misses
			// each (number of digests not written).
			Buckets: map[float64]uint64{3: 210},
		},
	}, peerLookupMetrics)
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

	resourcesWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
	}

	for _, baseCache := range baseCaches {
		gotMap, err := baseCache.GetMulti(ctx, resourcesWritten)
		assert.Nil(t, err)
		for _, r := range resourcesWritten {
			d := r.GetDigest()
			buf, ok := gotMap[d]
			assert.True(t, ok)
			assert.Equal(t, d.GetSizeBytes(), int64(len(buf)))
		}
	}

	for _, distributedCache := range distributedCaches {
		gotMap, err := distributedCache.GetMulti(ctx, resourcesWritten)
		assert.Nil(t, err)
		for _, r := range resourcesWritten {
			d := r.GetDigest()
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

	digestsWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		j := i % len(distributedCaches)
		if err := distributedCaches[j].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		digestsWritten = append(digestsWritten, rn)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, rn)
		}
	}

	// Restart the downed node -- as soon as it's back up, it should
	// receive hinted handoffs from the other peers.
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
	hintedHandoffs := make([]*rspb.ResourceName, 0)
	for _, d := range digestsWritten {
		ps := dc3.readPeers(d.Digest)
		for _, p := range ps.PreferredPeers {
			if p == peer3 {
				hintedHandoffs = append(hintedHandoffs, d)
				break
			}
		}
	}

	// Ensure that dc3 successfully received all the hinted handoffs.
	for _, r := range hintedHandoffs {
		exists, err := memoryCache3.Contains(ctx, r)
		assert.Nil(t, err)
		assert.True(t, exists)
		readAndCompareDigest(t, ctx, dc3, r)
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
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.NoError(t, err)
			assert.True(t, exists)
		}

		// Do a delete, and verify no nodes still have the data.
		if err := distributedCaches[i%3].Delete(ctx, rn); err != nil {
			t.Fatal(err)
		}
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
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
		rn, _ := testdigest.RandomCASResourceBuf(t, 100)

		// Do a delete on a file that does not exist.
		err := distributedCaches[i%3].Delete(ctx, rn)
		assert.NoError(t, err)
	}
}

func TestSupportsCompressor(t *testing.T) {
	singleCacheSizeBytes := int64(1000000)
	env := testenv.GetTestEnv(t)

	testCases := []struct {
		name                     string
		compressionLookupEnabled bool
		cache1                   interfaces.Cache
		cache2                   interfaces.Cache
		expected                 bool
	}{
		{
			name:                     "supports zstd",
			compressionLookupEnabled: true,
			cache1:                   &testcompression.CompressionCache{Cache: env.GetCache()},
			cache2:                   &testcompression.CompressionCache{Cache: env.GetCache()},
			expected:                 true,
		},
		{
			name:                     "does not support zstd",
			compressionLookupEnabled: true,
			cache1:                   newMemoryCache(t, singleCacheSizeBytes),
			cache2:                   newMemoryCache(t, singleCacheSizeBytes),
			expected:                 false,
		},
		{
			name:                     "compression lookup disabled",
			compressionLookupEnabled: false,
			cache1:                   &testcompression.CompressionCache{Cache: env.GetCache()},
			cache2:                   &testcompression.CompressionCache{Cache: env.GetCache()},
			expected:                 false,
		},
	}

	for _, tc := range testCases {
		{
			// Setup distributed cache
			peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
			peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
			baseConfig := CacheConfig{
				ReplicationFactor:            2,
				Nodes:                        []string{peer1, peer2},
				DisableLocalLookup:           true,
				EnableLocalCompressionLookup: tc.compressionLookupEnabled,
			}

			config1 := baseConfig
			config1.ListenAddr = peer1
			dc1 := startNewDCache(t, env, config1, tc.cache1)

			config2 := baseConfig
			config2.ListenAddr = peer2
			dc2 := startNewDCache(t, env, config2, tc.cache2)

			waitForReady(t, config1.ListenAddr)
			waitForReady(t, config2.ListenAddr)

			sc := dc1.SupportsCompressor(repb.Compressor_ZSTD)
			require.Equal(t, tc.expected, sc)

			sc = dc2.SupportsCompressor(repb.Compressor_ZSTD)
			require.Equal(t, tc.expected, sc)
		}
	}
}

func TestExtraNodes(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	numDigestsToWrite := 200

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
	dc1, err := NewDistributedCache(env, memoryCache1, config1, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc1.StartListening()

	memoryCache2 := newMemoryCache(t, singleCacheSizeBytes)
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2, err := NewDistributedCache(env, memoryCache2, config2, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc2.StartListening()

	memoryCache3 := newMemoryCache(t, singleCacheSizeBytes)
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3, err := NewDistributedCache(env, memoryCache3, config3, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc3.StartListening()

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	written := make([]*rspb.ResourceName, 0)
	for i := 0; i < numDigestsToWrite; i++ {
		// Do a write - should be visible from all nodes
		rn, buf := testdigest.RandomACResourceBuf(t, 100)
		if err := dc1.Set(ctx, rn, buf); err != nil {
			require.NoError(t, err)
		}
		written = append(written, rn)

		c, err := dc3.Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}

	waitForShutdown(dc1)
	waitForShutdown(dc2)
	waitForShutdown(dc3)

	// These nodes will be added in as "extra nodes", then the test
	// will check that old data is still readable and new data is writeable.
	peer4 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer5 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer6 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer7 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer8 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer9 := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	baseConfig = CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		NewNodes:           []string{peer1, peer2, peer3, peer4, peer5, peer6, peer7, peer8, peer9},
		DisableLocalLookup: true,
	}

	config1 = baseConfig
	config1.ListenAddr = peer1
	dc1, err = NewDistributedCache(env, memoryCache1, config1, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc1.StartListening()

	config2 = baseConfig
	config2.ListenAddr = peer2
	dc2, err = NewDistributedCache(env, memoryCache2, config2, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc2.StartListening()

	config3 = baseConfig
	config3.ListenAddr = peer3
	dc3, err = NewDistributedCache(env, memoryCache3, config3, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc3.StartListening()

	// Now bring up the new nodes
	memoryCache4 := newMemoryCache(t, singleCacheSizeBytes)
	config4 := baseConfig
	config4.ListenAddr = peer4
	dc4, err := NewDistributedCache(env, memoryCache4, config4, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc4.StartListening()

	memoryCache5 := newMemoryCache(t, singleCacheSizeBytes)
	config5 := baseConfig
	config5.ListenAddr = peer5
	dc5, err := NewDistributedCache(env, memoryCache5, config5, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc5.StartListening()

	memoryCache6 := newMemoryCache(t, singleCacheSizeBytes)
	config6 := baseConfig
	config6.ListenAddr = peer6
	dc6, err := NewDistributedCache(env, memoryCache6, config6, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc6.StartListening()

	memoryCache7 := newMemoryCache(t, singleCacheSizeBytes)
	config7 := baseConfig
	config7.ListenAddr = peer7
	dc7, err := NewDistributedCache(env, memoryCache7, config7, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc7.StartListening()

	memoryCache8 := newMemoryCache(t, singleCacheSizeBytes)
	config8 := baseConfig
	config8.ListenAddr = peer8
	dc8, err := NewDistributedCache(env, memoryCache8, config8, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc8.StartListening()

	memoryCache9 := newMemoryCache(t, singleCacheSizeBytes)
	config9 := baseConfig
	config9.ListenAddr = peer9
	dc9, err := NewDistributedCache(env, memoryCache9, config9, env.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc9.StartListening()

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)
	waitForReady(t, config4.ListenAddr)
	waitForReady(t, config5.ListenAddr)
	waitForReady(t, config6.ListenAddr)
	waitForReady(t, config7.ListenAddr)
	waitForReady(t, config8.ListenAddr)
	waitForReady(t, config9.ListenAddr)

	for i := 0; i < numDigestsToWrite; i++ {
		// Do a write - should be written to new nodes
		rn, buf := testdigest.RandomACResourceBuf(t, 100)
		if err := dc6.Set(ctx, rn, buf); err != nil {
			require.NoError(t, err)
		}
		written = append(written, rn)
	}

	for _, rn := range written {
		c, err := dc1.Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)

		c, err = dc6.Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}

	waitForShutdown(dc1)
	waitForShutdown(dc2)
	waitForShutdown(dc3)
	waitForShutdown(dc4)
	waitForShutdown(dc5)
	waitForShutdown(dc6)
	waitForShutdown(dc7)
	waitForShutdown(dc8)
	waitForShutdown(dc9)

}

func TestExtraNodesReadOnly(t *testing.T) {
	// Use new nodes for reads ONLY. No content should be written.
	flags.Set(t, "cache.distributed_cache.new_nodes_read_only", true)
	flags.Set(t, "cache.distributed_cache.new_consistent_hash_function", "SHA256")
	flags.Set(t, "cache.distributed_cache.new_consistent_hash_vnodes", 10000)

	// Disable backfills so digests are not copied to the new peerset.
	flags.Set(t, "cache.distributed_cache.enable_backfill", false)

	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	numDigestsToWrite := 200

	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	// Write some data.
	written := make([]*rspb.ResourceName, 0)
	for i := 0; i < numDigestsToWrite; i++ {
		rn, buf := testdigest.RandomACResourceBuf(t, 100)
		if err := dc1.Set(ctx, rn, buf); err != nil {
			require.NoError(t, err)
		}
		written = append(written, rn)
	}
	// Reset log of caches 1,2,3 so we don't see the initial write ops.
	memoryCache1.clearLog()
	memoryCache2.clearLog()
	memoryCache3.clearLog()

	caches := []interfaces.Cache{dc1, dc2, dc3}

	// Read the data we just wrote.
	for i, rn := range written {
		c, err := caches[i%len(caches)].Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}

	// Capture the original read operations (to compare against later).
	originalReadOpsByPeer := make(map[string][]cacheOp, 0)
	originalReadOpsByPeer[peer1] = append([]cacheOp{}, memoryCache1.ops...)
	originalReadOpsByPeer[peer2] = append([]cacheOp{}, memoryCache2.ops...)
	originalReadOpsByPeer[peer3] = append([]cacheOp{}, memoryCache3.ops...)

	// Reset 1,2,3 again so all node op lists are empty.
	memoryCache1.clearLog()
	memoryCache2.clearLog()
	memoryCache3.clearLog()

	waitForShutdown(dc1)
	waitForShutdown(dc2)
	waitForShutdown(dc3)

	// These nodes will be added in as "extra nodes", then the test
	// will check that old data is still readable and new data is writeable.
	peer4 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer5 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer6 := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	baseConfig = CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		NewNodes:           []string{peer1, peer2, peer3, peer4, peer5, peer6},
		DisableLocalLookup: true,
	}

	config1 = baseConfig
	config1.ListenAddr = peer1
	dc1 = startNewDCache(t, env, config1, memoryCache1)

	config2 = baseConfig
	config2.ListenAddr = peer2
	dc2 = startNewDCache(t, env, config2, memoryCache2)

	config3 = baseConfig
	config3.ListenAddr = peer3
	dc3 = startNewDCache(t, env, config3, memoryCache3)

	// Now bring up the new nodes
	memoryCache4 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config4 := baseConfig
	config4.ListenAddr = peer4
	dc4 := startNewDCache(t, env, config4, memoryCache4)

	memoryCache5 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config5 := baseConfig
	config5.ListenAddr = peer5
	dc5 := startNewDCache(t, env, config5, memoryCache5)

	memoryCache6 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config6 := baseConfig
	config6.ListenAddr = peer6
	dc6 := startNewDCache(t, env, config6, memoryCache6)

	caches = []interfaces.Cache{dc1, dc2, dc3, dc4, dc5, dc6}

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)
	waitForReady(t, config4.ListenAddr)
	waitForReady(t, config5.ListenAddr)
	waitForReady(t, config6.ListenAddr)

	// Read the data again from the same nodes we did originally.
	for i, rn := range written {
		c, err := caches[i%3].Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}

	// Capture the new read operations. They should be exactly the same as
	// the old read operations, even in the presence of the new nodes.
	newReadOpsByPeer := make(map[string][]cacheOp, 0)
	newReadOpsByPeer[peer1] = append([]cacheOp{}, memoryCache1.ops...)
	newReadOpsByPeer[peer2] = append([]cacheOp{}, memoryCache2.ops...)
	newReadOpsByPeer[peer3] = append([]cacheOp{}, memoryCache3.ops...)

	for _, peer := range []string{peer1, peer2, peer3} {
		require.Equal(t, newReadOpsByPeer[peer], originalReadOpsByPeer[peer])
		for i, oldOp := range originalReadOpsByPeer[peer] {
			log.Printf("%q old: %s, new: %s", peer, oldOp, newReadOpsByPeer[peer][i])
		}
	}

	// Now write some new data. It should only be written to the old nodes
	// because the newly added nodes are read-only.
	for i := 0; i < numDigestsToWrite; i++ {
		rn, buf := testdigest.RandomACResourceBuf(t, 100)
		if err := caches[i%len(caches)].Set(ctx, rn, buf); err != nil {
			require.NoError(t, err)
		}
		written = append(written, rn)
	}

	// Ensure that no write operations happened on the internal memory
	// caches of the newly added nodes, which should be read only.
	require.Empty(t, memoryCache4.ops)
	require.Empty(t, memoryCache5.ops)
	require.Empty(t, memoryCache6.ops)

	// Ensure that all digests (including newly written ones) are still
	// found.
	for i, rn := range written {
		c, err := caches[i%len(caches)].Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}
}

func TestExtraNodesReadWrite(t *testing.T) {
	flags.Set(t, "cache.distributed_cache.new_nodes_read_only", false)
	flags.Set(t, "cache.distributed_cache.new_consistent_hash_function", "SHA256")
	flags.Set(t, "cache.distributed_cache.new_consistent_hash_vnodes", 10000)

	// Disable backfills so digests are not copied to the new peerset.
	flags.Set(t, "cache.distributed_cache.enable_backfill", false)

	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	numDigestsToWrite := 200

	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	baseConfig := CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		DisableLocalLookup: true,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	// Write some data.
	written := make([]*rspb.ResourceName, 0)
	for i := 0; i < numDigestsToWrite; i++ {
		rn, buf := testdigest.RandomACResourceBuf(t, 100)
		if err := dc1.Set(ctx, rn, buf); err != nil {
			require.NoError(t, err)
		}
		written = append(written, rn)
	}
	// Reset log of caches 1,2,3 so we don't see the initial write ops.
	memoryCache1.clearLog()
	memoryCache2.clearLog()
	memoryCache3.clearLog()

	caches := []interfaces.Cache{dc1, dc2, dc3}

	// Read the data we just wrote.
	for i, rn := range written {
		c, err := caches[i%len(caches)].Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}

	// Capture the original read operations (to compare against later).
	originalReadOpsByPeer := make(map[string][]cacheOp, 0)
	originalReadOpsByPeer[peer1] = append([]cacheOp{}, memoryCache1.ops...)
	originalReadOpsByPeer[peer2] = append([]cacheOp{}, memoryCache2.ops...)
	originalReadOpsByPeer[peer3] = append([]cacheOp{}, memoryCache3.ops...)

	// Reset 1,2,3 again so all node op lists are empty.
	memoryCache1.clearLog()
	memoryCache2.clearLog()
	memoryCache3.clearLog()

	waitForShutdown(dc1)
	waitForShutdown(dc2)
	waitForShutdown(dc3)

	// These nodes will be added in as "extra nodes", then the test
	// will check that old data is still readable and new data is writeable.
	peer4 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer5 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer6 := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	baseConfig = CacheConfig{
		ReplicationFactor:  3,
		Nodes:              []string{peer1, peer2, peer3},
		NewNodes:           []string{peer1, peer2, peer3, peer4, peer5, peer6},
		DisableLocalLookup: true,
	}

	config1 = baseConfig
	config1.ListenAddr = peer1
	dc1 = startNewDCache(t, env, config1, memoryCache1)

	config2 = baseConfig
	config2.ListenAddr = peer2
	dc2 = startNewDCache(t, env, config2, memoryCache2)

	config3 = baseConfig
	config3.ListenAddr = peer3
	dc3 = startNewDCache(t, env, config3, memoryCache3)

	// Now bring up the new nodes
	memoryCache4 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config4 := baseConfig
	config4.ListenAddr = peer4
	dc4 := startNewDCache(t, env, config4, memoryCache4)

	memoryCache5 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config5 := baseConfig
	config5.ListenAddr = peer5
	dc5 := startNewDCache(t, env, config5, memoryCache5)

	memoryCache6 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config6 := baseConfig
	config6.ListenAddr = peer6
	dc6 := startNewDCache(t, env, config6, memoryCache6)

	caches = []interfaces.Cache{dc1, dc2, dc3, dc4, dc5, dc6}

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)
	waitForReady(t, config4.ListenAddr)
	waitForReady(t, config5.ListenAddr)
	waitForReady(t, config6.ListenAddr)

	// Read the data again from the same nodes we did originally.
	for i, rn := range written {
		c, err := caches[i%3].Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}

	// Capture the new read operations. They should be exactly the same as
	// the old read operations, even in the presence of the new nodes.
	newReadOpsByPeer := make(map[string][]cacheOp, 0)
	newReadOpsByPeer[peer1] = append([]cacheOp{}, memoryCache1.ops...)
	newReadOpsByPeer[peer2] = append([]cacheOp{}, memoryCache2.ops...)
	newReadOpsByPeer[peer3] = append([]cacheOp{}, memoryCache3.ops...)

	for _, peer := range []string{peer1, peer2, peer3} {
		require.Equal(t, newReadOpsByPeer[peer], originalReadOpsByPeer[peer])
		for i, oldOp := range originalReadOpsByPeer[peer] {
			log.Printf("%q old: %s, new: %s", peer, oldOp, newReadOpsByPeer[peer][i])
		}
	}

	for i := 0; i < numDigestsToWrite; i++ {
		rn, buf := testdigest.RandomACResourceBuf(t, 100)
		if err := caches[i%len(caches)].Set(ctx, rn, buf); err != nil {
			require.NoError(t, err)
		}
		written = append(written, rn)
	}

	// Ensure that all digests (including newly written ones) are still
	// found, no matter the node queried.
	for i, rn := range written {
		c, err := caches[i%len(caches)].Contains(ctx, rn)
		require.NoError(t, err)
		require.True(t, c)
	}
}

func TestReadThroughLookaside(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:       3,
		Nodes:                   []string{peer1, peer2, peer3},
		DisableLocalLookup:      true,
		LookasideCacheSizeBytes: 100_000,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
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
	allResources := make([]*rspb.ResourceName, 0)

	for i := 0; i < 100; i++ {
		// Do a write, and ensure we can read it back from each node,
		// both via the base cache and distributed cache for each node.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		allResources = append(allResources, rn)
		for _, baseCache := range baseCaches {
			exists, err := baseCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, baseCache, rn)
		}
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, rn)
		}
	}

	opCountBefore := map[string]int{
		peer1: len(memoryCache1.ops),
		peer2: len(memoryCache2.ops),
		peer3: len(memoryCache3.ops),
	}

	// Now read all of the digests again -- they should all
	// be served from the lookaside cache.
	for _, rn := range allResources {
		for _, distributedCache := range distributedCaches {
			readAndCompareDigest(t, ctx, distributedCache, rn)
		}
	}

	assert.Equal(t, opCountBefore[peer1], len(memoryCache1.ops))
	assert.Equal(t, opCountBefore[peer2], len(memoryCache2.ops))
	assert.Equal(t, opCountBefore[peer3], len(memoryCache3.ops))
}

func TestGetMultiLookaside(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(1000000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:       3,
		Nodes:                   []string{peer1, peer2, peer3},
		DisableLocalLookup:      true,
		LookasideCacheSizeBytes: 100_000,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}

	resourcesWritten := make([]*rspb.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
	}

	for _, distributedCache := range distributedCaches {
		gotMap, err := distributedCache.GetMulti(ctx, resourcesWritten)
		assert.Nil(t, err)
		for _, r := range resourcesWritten {
			d := r.GetDigest()
			buf, ok := gotMap[d]
			assert.True(t, ok)
			assert.Equal(t, d.GetSizeBytes(), int64(len(buf)))
		}
	}

	opCountBefore := map[string]int{
		peer1: len(memoryCache1.ops),
		peer2: len(memoryCache2.ops),
		peer3: len(memoryCache3.ops),
	}

	// Now read all of the digests again -- they should all
	// be served from the lookaside cache.
	for _, distributedCache := range distributedCaches {
		gotMap, err := distributedCache.GetMulti(ctx, resourcesWritten)
		assert.Nil(t, err)
		for _, r := range resourcesWritten {
			d := r.GetDigest()
			buf, ok := gotMap[d]
			assert.True(t, ok)
			assert.Equal(t, d.GetSizeBytes(), int64(len(buf)))
		}
	}

	assert.Equal(t, opCountBefore[peer1], len(memoryCache1.ops))
	assert.Equal(t, opCountBefore[peer2], len(memoryCache2.ops))
	assert.Equal(t, opCountBefore[peer3], len(memoryCache3.ops))
}

func TestLookasideLimits(t *testing.T) {
	env, _, ctx := getEnvAuthAndCtx(t)
	singleCacheSizeBytes := int64(10_000_000)
	peer1 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer2 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	peer3 := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	baseConfig := CacheConfig{
		ReplicationFactor:       3,
		Nodes:                   []string{peer1, peer2, peer3},
		DisableLocalLookup:      true,
		LookasideCacheSizeBytes: 2_000_000,
	}

	// Setup a distributed cache, 3 nodes, R = 3.
	memoryCache1 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config1 := baseConfig
	config1.ListenAddr = peer1
	dc1 := startNewDCache(t, env, config1, memoryCache1)

	memoryCache2 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config2 := baseConfig
	config2.ListenAddr = peer2
	dc2 := startNewDCache(t, env, config2, memoryCache2)

	memoryCache3 := traceCache(newMemoryCache(t, singleCacheSizeBytes))
	config3 := baseConfig
	config3.ListenAddr = peer3
	dc3 := startNewDCache(t, env, config3, memoryCache3)

	waitForReady(t, config1.ListenAddr)
	waitForReady(t, config2.ListenAddr)
	waitForReady(t, config3.ListenAddr)

	distributedCaches := []interfaces.Cache{dc1, dc2, dc3}
	allResources := make([]*rspb.ResourceName, 0)

	for i := 0; i < 3; i++ {
		// Do a write, and ensure we can read it back from each node,
		// both via the base cache and distributed cache for each node.
		rn, buf := testdigest.RandomCASResourceBuf(t, 1_000_000)
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		allResources = append(allResources, rn)
		for _, distributedCache := range distributedCaches {
			exists, err := distributedCache.Contains(ctx, rn)
			assert.Nil(t, err)
			assert.True(t, exists)
			readAndCompareDigest(t, ctx, distributedCache, rn)
		}
	}

	opCountBefore := map[string]int{
		peer1: len(memoryCache1.ops),
		peer2: len(memoryCache2.ops),
		peer3: len(memoryCache3.ops),
	}

	// Now read all of the digests again -- none should be served
	// from the lookaside cache because they were all large files.
	for _, rn := range allResources {
		for _, distributedCache := range distributedCaches {
			readAndCompareDigest(t, ctx, distributedCache, rn)
		}
	}

	assert.NotEqual(t, opCountBefore[peer1], len(memoryCache1.ops))
	assert.NotEqual(t, opCountBefore[peer2], len(memoryCache2.ops))
	assert.NotEqual(t, opCountBefore[peer3], len(memoryCache3.ops))
}

type Op int

const (
	Read Op = iota
	Write
	Delete
	Contains
	Metadata
)

func (o Op) String() string {
	switch o {
	case Read:
		return "READ"
	case Write:
		return "WRITE"
	case Delete:
		return "DELETE"
	case Contains:
		return "CONTAINS"
	case Metadata:
		return "METADATA"
	default:
		return "UNKNOWN"
	}
}

type cacheOp struct {
	op Op
	rn *rspb.ResourceName
}

func (o cacheOp) String() string {
	return fmt.Sprintf("%s %s", o.op, o.rn.GetDigest().GetHash())
}

func traceCache(c interfaces.Cache) *tracedCache {
	return &tracedCache{
		Cache: c,
		ops:   make([]cacheOp, 0),
		mu:    &sync.Mutex{},
	}
}

type tracedCache struct {
	interfaces.Cache
	ops []cacheOp
	mu  *sync.Mutex
}

func (t *tracedCache) clearLog() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ops = t.ops[:0]
}

func (t *tracedCache) addOps(o Op, resourceNames ...*rspb.ResourceName) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, rn := range resourceNames {
		t.ops = append(t.ops, cacheOp{o, rn})
	}
}
func (t *tracedCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	t.addOps(Contains, r)
	return t.Cache.Contains(ctx, r)
}
func (t *tracedCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	t.addOps(Metadata, r)
	return t.Cache.Metadata(ctx, r)
}
func (t *tracedCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	t.addOps(Contains, resources...)
	return t.Cache.FindMissing(ctx, resources)
}
func (t *tracedCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	t.addOps(Read, r)
	return t.Cache.Get(ctx, r)
}
func (t *tracedCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	t.addOps(Read, resources...)
	return t.Cache.GetMulti(ctx, resources)
}
func (t *tracedCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	t.addOps(Write, r)
	return t.Cache.Set(ctx, r, data)
}
func (t *tracedCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	resources := make([]*rspb.ResourceName, 0, len(kvs))
	for rn := range kvs {
		resources = append(resources, rn)
	}
	t.addOps(Write, resources...)
	return t.Cache.SetMulti(ctx, kvs)
}
func (t *tracedCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	t.addOps(Delete, r)
	return t.Cache.Delete(ctx, r)
}
func (t *tracedCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	t.addOps(Read, r)
	return t.Cache.Reader(ctx, r, uncompressedOffset, limit)
}
func (t *tracedCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	t.addOps(Write, r)
	return t.Cache.Writer(ctx, r)
}
