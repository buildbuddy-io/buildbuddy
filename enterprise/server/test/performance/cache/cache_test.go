package cache_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/migration_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// 10GB should be enough to prevent any eviction
	maxSizeBytes = int64(10_000_000_000)
	numDigests   = 100
)

func init() {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()
}

func getAnonContext(t testing.TB, te *testenv.TestEnv) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}
	return ctx
}

type digestBuf struct {
	d   *rspb.ResourceName
	buf []byte
}

func makeDigests(t testing.TB, numDigests int, digestSizeBytes int64, cacheType rspb.CacheType) []*digestBuf {
	digestBufs := make([]*digestBuf, 0, numDigests)
	for i := 0; i < numDigests; i++ {
		r, buf := testdigest.NewRandomResourceAndBuf(t, digestSizeBytes, cacheType, "")
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

func getMigrationCache(t testing.TB, env environment.Env, src, dest interfaces.Cache) interfaces.Cache {
	config := &migration_cache.MigrationConfig{
		CopyChanBufferSize:             200,
		MaxCopiesPerSec:                100,
		NumCopyWorkers:                 1,
		CopyChanFullWarningIntervalMin: 60,
		AsyncDestWrites:                false,
		DoubleReadPercentage:           1,
	}
	return migration_cache.NewMigrationCache(env, config, src, dest)
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
	t.Cleanup(func() { dc.Shutdown(context.Background()) })
	return dc
}

func getPebbleCache(t testing.TB, te *testenv.TestEnv) interfaces.Cache {
	testRootDir := testfs.MakeTempDir(t)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		Name:          testRootDir,
		RootDirectory: testRootDir,
		MaxSizeBytes:  maxSizeBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	t.Cleanup(func() {
		pc.Stop()
	})
	return pc
}

func benchmarkSet(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B, cacheType rspb.CacheType) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes, cacheType)
	b.ReportAllocs()
	b.SetBytes(digestSizeBytes)

	i := 0
	for b.Loop() {
		dbuf := digestBufs[i%len(digestBufs)]
		i++
		err := c.Set(ctx, dbuf.d, dbuf.buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkRead(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes, rspb.CacheType_CAS)
	setDigestsInCache(b, ctx, c, digestBufs)
	b.ReportAllocs()
	b.SetBytes(digestSizeBytes)

	// Using a bytes.Buffer here because it is used in the distributed.Cache.Get
	// path, which calls Cache.Reader, and this results in Read calls of various
	// sizes.
	readBuf := bytes.NewBuffer(make([]byte, 1))
	i := 1
	for b.Loop() {
		dbuf := digestBufs[i%len(digestBufs)]
		i++
		r, err := c.Reader(ctx, dbuf.d, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
		n, err := readBuf.ReadFrom(r)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
		if n != digestSizeBytes {
			b.Fatalf("Wanted %v bytes, got %v", digestSizeBytes, n)
		}
	}
}

func benchmarkGet(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes, rspb.CacheType_CAS)
	setDigestsInCache(b, ctx, c, digestBufs)
	b.ReportAllocs()
	b.SetBytes(digestSizeBytes)

	i := 0
	for b.Loop() {
		dbuf := digestBufs[i%len(digestBufs)]
		i++
		_, err := c.Get(ctx, dbuf.d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGetMulti(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes, rspb.CacheType_CAS)
	setDigestsInCache(b, ctx, c, digestBufs)
	digests := make([]*rspb.ResourceName, 0, len(digestBufs))
	var sumBytes int64
	for _, dbuf := range digestBufs {
		digests = append(digests, dbuf.d)
		sumBytes += dbuf.d.GetDigest().GetSizeBytes()
	}
	b.ReportAllocs()
	b.SetBytes(sumBytes)

	for b.Loop() {
		_, err := c.GetMulti(ctx, digests)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkFindMissing(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes, rspb.CacheType_CAS)
	setDigestsInCache(b, ctx, c, digestBufs)
	digests := make([]*rspb.ResourceName, 0, len(digestBufs))
	for _, dbuf := range digestBufs {
		digests = append(digests, dbuf.d)
	}
	b.ReportAllocs()

	for b.Loop() {
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
	flags.Set(b, "cache.distributed_cache.consistent_hash_function", "SHA256")
	dc := getDiskCache(b, te)
	ddc := getDistributedCache(b, te, dc)
	pc := getPebbleCache(b, te)
	dpc := getDistributedCache(b, te, pc)

	time.Sleep(100 * time.Millisecond)
	caches := []*namedCache{
		{getMemoryCache(b), "LocalMemory"},
		{getDiskCache(b, te), "LocalDisk"},
		{ddc, "DistDisk"},
		{getPebbleCache(b, te), "LocalPebble"},
		{dpc, "DistPebble"},
		{getMigrationCache(b, te, getPebbleCache(b, te), getPebbleCache(b, te)), "LocalMigration"},
		{getDistributedCache(b, te, getMigrationCache(b, te, getPebbleCache(b, te), getPebbleCache(b, te))), "DistMigration"},
	}
	return caches
}

func BenchmarkSet(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000, 1_000_000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			// AC entries are sometimes treated differently. For example, the
			// distributed cache doesn't check if they exist before writing
			// them.
			for _, cacheType := range []rspb.CacheType{rspb.CacheType_AC, rspb.CacheType_CAS} {
				name := fmt.Sprintf("%s%d/%v", cache.Name, size, cacheType)
				b.Run(name, func(b *testing.B) {
					benchmarkSet(ctx, cache, size, b, cacheType)
				})
			}
		}
	}
}

func BenchmarkRead(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				benchmarkRead(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkGetSingle(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				benchmarkGet(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkGetMulti(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				benchmarkGetMulti(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkFindMissing(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				benchmarkFindMissing(ctx, cache, size, b)
			})
		}
	}
}
