package cache_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	maxSizeBytes = int64(1e9) // 100MB
	numDigests   = 50
)

var (
	emptyUserMap = testauth.TestUsers()
)

func init() {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()
}

func getTestEnv(t testing.TB, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func getAnonContext(t testing.TB, te *testenv.TestEnv) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}
	return ctx
}

type digestBuf struct {
	d   *rspb.ResourceName
	buf []byte
}

func makeCompressibleDigests(t testing.TB, numDigests int, digestSizeBytes int64) []*digestBuf {
	digestBufs := make([]*digestBuf, 0, numDigests)
	for i := 0; i < numDigests; i++ {
		r, buf := testdigest.RandomCompressibleCASResourceBuf(t, digestSizeBytes, "")
		r.Compressor = repb.Compressor_ZSTD
		compressedBuf := compression.CompressZstd(nil, buf)
		digestBufs = append(digestBufs, &digestBuf{
			d:   r,
			buf: compressedBuf,
		})
	}
	return digestBufs
}

func makeDigests(t testing.TB, numDigests int, digestSizeBytes int64) []*digestBuf {
	digestBufs := make([]*digestBuf, 0, numDigests)
	for i := 0; i < numDigests; i++ {
		r, buf := testdigest.RandomCASResourceBuf(t, digestSizeBytes)
		digestBufs = append(digestBufs, &digestBuf{
			d:   r,
			buf: buf,
		})
	}
	return digestBufs
}

func setDigestsInCache(t testing.TB, ctx context.Context, c interfaces.Cache, dbufs []*digestBuf) {
	for _, dbuf := range dbufs {
		if err := c.Set(ctx, dbuf.d, dbuf.buf); err != nil {
			t.Fatal(err)
		}
	}
}

func getMemoryCache(t testing.TB) interfaces.Cache {
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return mc
}

func getDiskCache(t testing.TB, env environment.Env) interfaces.Cache {
	testRootDir := testfs.MakeTempDir(t)
	dc, err := disk_cache.NewDiskCache(env, &disk_cache.Options{RootDirectory: testRootDir}, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return dc
}

func getDistributedCache(t testing.TB, te *testenv.TestEnv, c interfaces.Cache) interfaces.Cache {
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	conf := distributed.CacheConfig{
		ListenAddr:         listenAddr,
		GroupName:          "default",
		ReplicationFactor:  1,
		Nodes:              []string{listenAddr},
		DisableLocalLookup: true,
	}
	dc, err := distributed.NewDistributedCache(te, c, conf, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	dc.StartListening()
	return dc
}

func getPebbleCacheOptions(t testing.TB, blockSize int64) *pebble_cache.Options {
	testRootDir := testfs.MakeTempDir(t)
	activeKeyVersion := int64(5)
	return &pebble_cache.Options{
		RootDirectory:         testRootDir,
		BlockCacheSizeBytes:   blockSize,
		MaxSizeBytes:          maxSizeBytes,
		ActiveKeyVersion:      &activeKeyVersion,
		AverageChunkSizeBytes: 524288,
		IncludeMetadataSize:   true,
	}
}

func getPebbleCache(t testing.TB, te *testenv.TestEnv, blockSize int64) interfaces.Cache {
	opts := getPebbleCacheOptions(t, blockSize)
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	t.Cleanup(func() {
		pc.Stop()
	})
	return pc
}

func getPebbleCacheWithDigests(ctx context.Context, t testing.TB, te *testenv.TestEnv, opts *pebble_cache.Options, dbufs []*digestBuf) interfaces.Cache {
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	if err != nil {
		t.Fatalf("failed to create the first pebble cache: %s", err)
	}
	err = pc.Start()
	if err != nil {
		t.Fatalf("failed to start the first pebble cache: %s", err)
	}
	setDigestsInCache(t, ctx, pc, dbufs)
	err = pc.Stop()
	if err != nil {
		t.Fatalf("failed to stop the first pebble cache: %s", err)
	}

	// start a second cache with same options
	pc2, err := pebble_cache.NewPebbleCache(te, opts)
	if err != nil {
		t.Fatalf("failed to create the second pebble cache: %s", err)
	}
	err = pc2.Start()
	if err != nil {
		t.Fatalf("failed to start the second pebble cache: %s", err)
	}
	t.Cleanup(func() {
		pc2.Stop()
	})
	return pc2
}

func benchmarkSet(ctx context.Context, c interfaces.Cache, digestBufs []*digestBuf, b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dbuf := digestBufs[rand.Intn(len(digestBufs))]
		b.SetBytes(dbuf.d.GetDigest().GetSizeBytes())
		err := c.Set(ctx, dbuf.d, dbuf.buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(ctx context.Context, c interfaces.Cache, digestBufs []*digestBuf, b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dbuf := digestBufs[rand.Intn(len(digestBufs))]
		b.SetBytes(int64(len(dbuf.buf)))
		_, err := c.Get(ctx, dbuf.d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGetMulti(ctx context.Context, c interfaces.Cache, digestBufs []*digestBuf, b *testing.B) {
	digests := make([]*rspb.ResourceName, 0, len(digestBufs))
	var sumBytes int64
	for _, dbuf := range digestBufs {
		digests = append(digests, dbuf.d)
		sumBytes += int64(len(dbuf.buf))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(sumBytes)

	for i := 0; i < b.N; i++ {
		_, err := c.GetMulti(ctx, digests)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkFindMissing(ctx context.Context, c interfaces.Cache, digestBufs []*digestBuf, b *testing.B) {
	digests := make([]*rspb.ResourceName, 0, len(digestBufs))
	for _, dbuf := range digestBufs {
		digests = append(digests, dbuf.d)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := c.FindMissing(ctx, digests)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type namedCache struct {
	interfaces.Cache
	Name string
}

func getAllCaches(b *testing.B, te *testenv.TestEnv) []*namedCache {
	//dc := getDiskCache(b, te)
	//ddc := getDistributedCache(b, te, dc)
	//pc := getPebbleCache(b, te)
	//dpc := getDistributedCache(b, te, pc)

	time.Sleep(100 * time.Millisecond)
	caches := []*namedCache{
		//{getMemoryCache(b), "Memory"},
		//{getDiskCache(b, te), "Disk"},
		//{ddc, "DDisk"},
		{getPebbleCache(b, te, 0), "Pebble"},
		//{dpc, "DPebble"},
	}
	return caches
}

type namedPebbleOptions struct {
	opts *pebble_cache.Options
	Name string
}

func getAllPebbleCacheOptions(b *testing.B, te *testenv.TestEnv) []*namedPebbleOptions {
	return []*namedPebbleOptions{
		{getPebbleCacheOptions(b, 1e9), "blockSize=1e9"},
		{getPebbleCacheOptions(b, 5e9), "blockSize=5e9"},
		{getPebbleCacheOptions(b, 1e10), "blockSize=1e10"},
		{getPebbleCacheOptions(b, 5e10), "blockSize=5e10"},
	}
}

func BenchmarkSet(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%ssize=%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				benchmarkSet(ctx, cache, digestBufs, b)
			})
		}
	}
}

func BenchmarkGet(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/size=%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				setDigestsInCache(b, ctx, cache, digestBufs)
				benchmarkGet(ctx, cache, digestBufs, b)
			})
		}
	}
}

func BenchmarkPebbleGet(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)
	for _, namedOpts := range getAllPebbleCacheOptions(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/size=%d", namedOpts.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				pc := getPebbleCacheWithDigests(ctx, b, te, namedOpts.opts, digestBufs)
				benchmarkGet(ctx, pc, digestBufs, b)
			})
		}
	}
}

func BenchmarkGetMulti(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/size=%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				setDigestsInCache(b, ctx, cache, digestBufs)
				benchmarkGetMulti(ctx, cache, digestBufs, b)
			})
		}
	}
}

func BenchmarkPebbleGetMulti(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, namedOpts := range getAllPebbleCacheOptions(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/size=%d", namedOpts.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				pc := getPebbleCacheWithDigests(ctx, b, te, namedOpts.opts, digestBufs)
				benchmarkGetMulti(ctx, pc, digestBufs, b)
			})
		}
	}
}

func BenchmarkFindMissing(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/size=%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				setDigestsInCache(b, ctx, cache, digestBufs)
				benchmarkFindMissing(ctx, cache, digestBufs, b)
			})
		}
	}
}

func BenchmarkPebbleFindMissing(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1e5, 1e6, 1e7, 5e7, 1e8}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, namedOpts := range getAllPebbleCacheOptions(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/size=%d", namedOpts.Name, size)
			b.Run(name, func(b *testing.B) {
				digestBufs := makeCompressibleDigests(b, numDigests, size)
				pc := getPebbleCacheWithDigests(ctx, b, te, namedOpts.opts, digestBufs)
				benchmarkFindMissing(ctx, pc, digestBufs, b)
			})
		}
	}
}
