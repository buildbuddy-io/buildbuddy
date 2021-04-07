package cache_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	maxSizeBytes = int64(100000000) // 100MB
	numDigests   = 100
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t testing.TB, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func getAnonContext(t testing.TB, te *testenv.TestEnv) context.Context {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}
	return ctx
}

type digestBuf struct {
	d   *repb.Digest
	buf []byte
}

func makeDigests(t testing.TB, numDigests int, digestSizeBytes int64) []*digestBuf {
	digestBufs := make([]*digestBuf, 0, numDigests)
	for i := 0; i < numDigests; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, digestSizeBytes)
		digestBufs = append(digestBufs, &digestBuf{
			d:   d,
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

func getDiskCache(t testing.TB) interfaces.Cache {
	testRootDir, err := ioutil.TempDir("/tmp", "diskcache_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(testRootDir)
		if err != nil {
			t.Fatal(err)
		}
	})
	dc, err := disk_cache.NewDiskCache(testRootDir, maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return dc
}

func getDistributedDiskCache(t testing.TB, te *testenv.TestEnv) interfaces.Cache {
	dc := getDiskCache(t)
	listenAddr := fmt.Sprintf("localhost:%d", app.FreePort(t))
	conf := distributed.CacheConfig{
		ListenAddr:       listenAddr,
		GroupName:        "default",
		ReplicationFactor: 1,
		Nodes:             []string{listenAddr},
	}
	c, err := distributed.NewDistributedCache(te, dc, conf, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func benchmarkSetSingleThread(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dbuf := digestBufs[rand.Intn(len(digestBufs))]
		err := c.Set(ctx, dbuf.d, dbuf.buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSetMultiThread(ctx context.Context,c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			dbuf := digestBufs[rand.Intn(len(digestBufs))]
			err := c.Set(ctx, dbuf.d, dbuf.buf)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkGetSingleThread(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, digestBufs)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dbuf := digestBufs[rand.Intn(len(digestBufs))]
		_, err := c.Get(ctx, dbuf.d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGetMultiThread(ctx context.Context, c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	digestBufs := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, digestBufs)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			dbuf := digestBufs[rand.Intn(len(digestBufs))]
			_, err := c.Get(ctx, dbuf.d)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}


func getAllCaches(b *testing.B, te *testenv.TestEnv) map[string]interfaces.Cache {
	return map[string]interfaces.Cache{
		"Memory": getMemoryCache(b),
		"Disk": getDiskCache(b),
		"DistributedDisk": getDistributedDiskCache(b, te),
	}
}

func BenchmarkSetSingleThread(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for cacheName, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cacheName, size)
			b.Run(name, func(b *testing.B) {
				benchmarkSetSingleThread(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkSetMultiThread(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for cacheName, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cacheName, size)
			b.Run(name, func(b *testing.B) {
				benchmarkSetMultiThread(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkGetSingleThread(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for cacheName, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cacheName, size)
			b.Run(name, func(b *testing.B) {
				benchmarkGetSingleThread(ctx, cache, size, b)
			})
		}
	}
}

func BenchmarkGetMultiThread(b *testing.B) {
	sizes := []int64{10, 100, 1000, 10000}
	te := testenv.GetTestEnv(b)
	ctx := getAnonContext(b, te)

	for cacheName, cache := range getAllCaches(b, te) {
		for _, size := range sizes {
			name := fmt.Sprintf("%s%d", cacheName, size)
			b.Run(name, func(b *testing.B) {
				benchmarkGetMultiThread(ctx, cache, size, b)
			})
		}
	}
}
