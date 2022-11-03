package cache_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
)

const (
	maxSizeBytes = int64(100000000) // 100MB
	numDigests   = 100
)

var (
	emptyUserMap = testauth.TestUsers()
)

func init() {
	*log.LogLevel = "warn"
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
	d   *resource.ResourceName
	buf []byte
}

func makeDigests(t testing.TB, numDigests int, digestSizeBytes int64) []*digestBuf {
	digestBufs := make([]*digestBuf, 0, numDigests)
	for i := 0; i < numDigests; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, digestSizeBytes)
		r := &resource.ResourceName{
			Digest:    d,
			CacheType: resource.CacheType_CAS,
		}
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

func getDistributedDiskCache(t testing.TB, te *testenv.TestEnv) interfaces.Cache {
	dc := getDiskCache(t, te)
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	conf := distributed.CacheConfig{
		ListenAddr:         listenAddr,
		GroupName:          "default",
		ReplicationFactor:  1,
		Nodes:              []string{listenAddr},
		DisableLocalLookup: true,
	}
	c, err := distributed.NewDistributedCache(te, dc, conf, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	c.StartListening()
	return c
}

func benchmarkSet(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)

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

func benchmarkGet(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, digestBufs)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dbuf := digestBufs[rand.Intn(len(digestBufs))]
		b.SetBytes(dbuf.d.GetDigest().GetSizeBytes())
		_, err := c.Get(ctx, dbuf.d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGetMulti(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, digestBufs)
	digests := make([]*resource.ResourceName, 0, len(digestBufs))
	var sumBytes int64
	for _, dbuf := range digestBufs {
		digests = append(digests, dbuf.d)
		sumBytes += dbuf.d.GetDigest().GetSizeBytes()
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

func benchmarkFindMissing(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, digestBufs)
	digests := make([]*resource.ResourceName, 0, len(digestBufs))
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
	dc := getDistributedDiskCache(b, te)
	time.Sleep(100 * time.Millisecond)
	caches := []*namedCache{
		{getMemoryCache(b), "Memory"},
		{getDiskCache(b, te), "Disk"},
		{dc, "DDisk"},
	}
	return caches
}

func BenchmarkSet(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for _, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cache.Name, size)
			b.Run(name, func(b *testing.B) {
				benchmarkSet(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkGet(b *testing.B) {
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
