package cache_test

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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

func getAnonContext(t testing.TB) context.Context {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func makeDigests(t testing.TB, numDigests int, digestSizeBytes int64) ([]*repb.Digest, map[*repb.Digest][]byte) {
	digests := make([]*repb.Digest, 0, numDigests)
	data := make(map[*repb.Digest][]byte, 0)
	for i := 0; i < numDigests; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, digestSizeBytes)
		data[d] = buf
		digests = append(digests, d)
	}
	return digests, data
}

func setDigestsInCache(t testing.TB, ctx context.Context, c interfaces.Cache, data map[*repb.Digest][]byte) {
	for d, buf := range data {
		if err := c.Set(ctx, d, buf); err != nil {
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

func benchmarkSetSingleThread(c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	ctx := getAnonContext(b)
	digests, data := makeDigests(b, numDigests, digestSizeBytes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := digests[rand.Intn(len(digests))]
		err := c.Set(ctx, d, data[d])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetSingleThreadMemory10(b *testing.B) {
	benchmarkSetSingleThread(getMemoryCache(b), 10, b)
}
func BenchmarkSetSingleThreadMemory100(b *testing.B) {
	benchmarkSetSingleThread(getMemoryCache(b), 100, b)
}
func BenchmarkSetSingleThreadMemory1000(b *testing.B) {
	benchmarkSetSingleThread(getMemoryCache(b), 1000, b)
}
func BenchmarkSetSingleThreadMemory10000(b *testing.B) {
	benchmarkSetSingleThread(getMemoryCache(b), 10000, b)
}

func BenchmarkSetSingleThreadDisk10(b *testing.B) {
	benchmarkSetSingleThread(getDiskCache(b), 10, b)
}
func BenchmarkSetSingleThreadDisk100(b *testing.B) {
	benchmarkSetSingleThread(getDiskCache(b), 100, b)
}
func BenchmarkSetSingleThreadDisk1000(b *testing.B) {
	benchmarkSetSingleThread(getDiskCache(b), 1000, b)
}
func BenchmarkSetSingleThreadDisk10000(b *testing.B) {
	benchmarkSetSingleThread(getDiskCache(b), 10000, b)
}

func benchmarkSetMultiThread(c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	ctx := getAnonContext(b)

	digests, data := makeDigests(b, numDigests, digestSizeBytes)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d := digests[rand.Intn(len(digests))]
			err := c.Set(ctx, d, data[d])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetMultiThreadMemory10(b *testing.B) {
	benchmarkSetMultiThread(getMemoryCache(b), 10, b)
}
func BenchmarkSetMultiThreadMemory100(b *testing.B) {
	benchmarkSetMultiThread(getMemoryCache(b), 100, b)
}
func BenchmarkSetMultiThreadMemory1000(b *testing.B) {
	benchmarkSetMultiThread(getMemoryCache(b), 1000, b)
}
func BenchmarkSetMultiThreadMemory10000(b *testing.B) {
	benchmarkSetMultiThread(getMemoryCache(b), 10000, b)
}

func BenchmarkSetMultiThreadDisk10(b *testing.B) {
	benchmarkSetMultiThread(getDiskCache(b), 10, b)
}
func BenchmarkSetMultiThreadDisk100(b *testing.B) {
	benchmarkSetMultiThread(getDiskCache(b), 100, b)
}
func BenchmarkSetMultiThreadDisk1000(b *testing.B) {
	benchmarkSetMultiThread(getDiskCache(b), 1000, b)
}
func BenchmarkSetMultiThreadDisk10000(b *testing.B) {
	benchmarkSetMultiThread(getDiskCache(b), 10000, b)
}

func benchmarkGetSingleThread(c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	ctx := getAnonContext(b)

	digests, data := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, data)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d := digests[rand.Intn(len(digests))]
		_, err := c.Get(ctx, d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSingleThreadMemory10(b *testing.B) {
	benchmarkGetSingleThread(getMemoryCache(b), 10, b)
}
func BenchmarkGetSingleThreadMemory100(b *testing.B) {
	benchmarkGetSingleThread(getMemoryCache(b), 100, b)
}
func BenchmarkGetSingleThreadMemory1000(b *testing.B) {
	benchmarkGetSingleThread(getMemoryCache(b), 1000, b)
}
func BenchmarkGetSingleThreadMemory10000(b *testing.B) {
	benchmarkGetSingleThread(getMemoryCache(b), 10000, b)
}

func BenchmarkGetSingleThreadDisk10(b *testing.B) {
	benchmarkGetSingleThread(getDiskCache(b), 10, b)
}
func BenchmarkGetSingleThreadDisk100(b *testing.B) {
	benchmarkGetSingleThread(getDiskCache(b), 100, b)
}
func BenchmarkGetSingleThreadDisk1000(b *testing.B) {
	benchmarkGetSingleThread(getDiskCache(b), 1000, b)
}
func BenchmarkGetSingleThreadDisk10000(b *testing.B) {
	benchmarkGetSingleThread(getDiskCache(b), 10000, b)
}

func benchmarkGetMultiThread(c interfaces.Cache, digestSizeBytes int64, b *testing.B) {
	ctx := getAnonContext(b)

	digests, data := makeDigests(b, numDigests, digestSizeBytes)
	setDigestsInCache(b, ctx, c, data)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d := digests[rand.Intn(len(digests))]
			_, err := c.Get(ctx, d)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetMultiThreadMemory10(b *testing.B) {
	benchmarkGetMultiThread(getMemoryCache(b), 10, b)
}
func BenchmarkGetMultiThreadMemory100(b *testing.B) {
	benchmarkGetMultiThread(getMemoryCache(b), 100, b)
}
func BenchmarkGetMultiThreadMemory1000(b *testing.B) {
	benchmarkGetMultiThread(getMemoryCache(b), 1000, b)
}
func BenchmarkGetMultiThreadMemory10000(b *testing.B) {
	benchmarkGetMultiThread(getMemoryCache(b), 10000, b)
}

func BenchmarkGetMultiThreadDisk10(b *testing.B) {
	benchmarkGetMultiThread(getDiskCache(b), 10, b)
}
func BenchmarkGetMultiThreadDisk100(b *testing.B) {
	benchmarkGetMultiThread(getDiskCache(b), 100, b)
}
func BenchmarkGetMultiThreadDisk1000(b *testing.B) {
	benchmarkGetMultiThread(getDiskCache(b), 1000, b)
}
func BenchmarkGetMultiThreadDisk10000(b *testing.B) {
	benchmarkGetMultiThread(getDiskCache(b), 10000, b)
}
