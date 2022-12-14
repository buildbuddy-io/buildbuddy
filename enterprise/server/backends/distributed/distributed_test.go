package distributed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
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
	c.StartListening()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), maxShutdownDuration)
		c.Shutdown(shutdownCtx)
		cancel()
	})
	return c
}

func readAndCompareDigest(t *testing.T, ctx context.Context, c interfaces.Cache, r *resource.ResourceName) {
	reader, err := c.Reader(ctx, r, 0, 0)
	if err != nil {
		assert.FailNow(t, fmt.Sprintf("cache: %+v", c), err)
	}
	d1 := testdigest.ReadDigestAndClose(t, reader)
	assert.Equal(t, r.GetDigest().GetHash(), d1.GetHash())
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
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
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
				d, buf := testdigest.NewRandomDigestBuf(t, 100)
				compressedBuf := compression.CompressZstd(nil, buf)
				writeRN := &resource.ResourceName{
					Digest:     d,
					CacheType:  resource.CacheType_CAS,
					Compressor: tc.writeCompression,
				}

				bufToWrite := buf
				if tc.writeCompression == repb.Compressor_ZSTD {
					bufToWrite = compressedBuf
				}
				err := distributedCaches[i%3].Set(ctx, writeRN, bufToWrite)
				require.NoError(t, err)

				readRN := &resource.ResourceName{
					Digest:     d,
					CacheType:  resource.CacheType_CAS,
					Compressor: tc.readCompression,
				}
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
	remoteInstanceName := "remote"

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
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	rn1 := &resource.ResourceName{
		Digest:       d,
		InstanceName: remoteInstanceName,
		CacheType:    resource.CacheType_AC,
	}
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
	d, _ := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

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
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	rn := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

	if err := distributedCaches[0].Set(ctx, rn, buf); err != nil {
		t.Fatal(err)
	}

	reader, err := distributedCaches[1].Reader(ctx, rn, d.GetSizeBytes(), 0)
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
	rn := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	if err := distributedCaches[0].Set(ctx, rn, buf); err != nil {
		t.Fatal(err)
	}

	offset := int64(2)
	limit := int64(3)
	reader, err := distributedCaches[1].Reader(ctx, rn, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, d.GetSizeBytes())
	n, err := io.ReadFull(reader, readBuf)
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
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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

	resourcesWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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

	baseCaches = append(baseCaches, memoryCache3)
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

	resourcesWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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

	resourcesWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		rn := &resource.ResourceName{
			InstanceName: "blah",
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
		}
		// Set() the bytes in the cache.
		err := distributedCaches[i%3].Set(ctx, rn, buf)
		require.NoError(t, err)

		for _, dc := range distributedCaches {
			// Metadata should return true size of the blob, regardless of queried size.
			md, err := dc.Metadata(ctx, &resource.ResourceName{
				Digest:       &repb.Digest{Hash: d.GetHash(), SizeBytes: 1},
				InstanceName: "blah",
				CacheType:    resource.CacheType_CAS,
			})
			require.NoError(t, err)
			require.Equal(t, testSize, md.StoredSizeBytes)
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

	resourcesWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, rn)
	}

	// Generate some more digests, but don't write them to the cache.
	resourcesNotWritten := make([]*resource.ResourceName, 0)
	digestsNotWritten := make([]*repb.Digest, 0)
	for i := 0; i < 100; i++ {
		d, _ := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		resourcesNotWritten = append(resourcesNotWritten, rn)
		digestsNotWritten = append(digestsNotWritten, d)
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

	resourcesWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
		if err := distributedCaches[i%3].Set(ctx, rn, buf); err != nil {
			t.Fatal(err)
		}
		resourcesWritten = append(resourcesWritten, &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		})
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

	digestsWritten := make([]*resource.ResourceName, 0)
	for i := 0; i < 100; i++ {
		// Do a write, and ensure it was written to all nodes.
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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
	hintedHandoffs := make([]*resource.ResourceName, 0)
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
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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
		d, _ := testdigest.NewRandomDigestBuf(t, 100)
		rn := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}

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
