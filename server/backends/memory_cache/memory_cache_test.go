package memory_cache_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"
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
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func TestIsolation(t *testing.T) {
	maxSizeBytes := int64(1000000000) // 1GB
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t)

	type test struct {
		cache1         interfaces.Cache
		cache2         interfaces.Cache
		shouldBeShared bool
	}
	mustIsolate := func(cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) interfaces.Cache {
		c, err := mc.WithIsolation(ctx, cacheType, remoteInstanceName)
		if err != nil {
			t.Fatalf("Error isolating cache: %s", err)
		}
		return c
	}

	tests := []test{
		{ // caches with the same isolation are shared.
			cache1:         mustIsolate(interfaces.CASCacheType, "remoteInstanceName"),
			cache2:         mustIsolate(interfaces.CASCacheType, "remoteInstanceName"),
			shouldBeShared: true,
		},
		{ // action caches with the same isolation are shared.
			cache1:         mustIsolate(interfaces.ActionCacheType, "remoteInstanceName"),
			cache2:         mustIsolate(interfaces.ActionCacheType, "remoteInstanceName"),
			shouldBeShared: true,
		},
		{ // CAS caches with different remote instance names are shared.
			cache1:         mustIsolate(interfaces.CASCacheType, "remoteInstanceName"),
			cache2:         mustIsolate(interfaces.CASCacheType, "otherInstanceName"),
			shouldBeShared: true,
		},
		{ // Action caches with different remote instance names are not shared.
			cache1:         mustIsolate(interfaces.ActionCacheType, "remoteInstanceName"),
			cache2:         mustIsolate(interfaces.ActionCacheType, "otherInstanceName"),
			shouldBeShared: false,
		},
		{ // CAS and Action caches are not shared.
			cache1:         mustIsolate(interfaces.CASCacheType, "remoteInstanceName"),
			cache2:         mustIsolate(interfaces.ActionCacheType, "remoteInstanceName"),
			shouldBeShared: false,
		},
	}

	for _, test := range tests {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		// Set() the bytes in cache1.
		err := test.cache1.Set(ctx, d, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		// Get() the bytes from cache2.
		rbuf, err := test.cache2.Get(ctx, d)
		if test.shouldBeShared {
			// if the caches should be shared but there was an error
			// getting the digest: fail.
			if err != nil {
				t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
			}

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf))
			if err != nil {
				t.Fatalf("Error computing digest: %s", err.Error())
			}

			if d.GetHash() != d2.GetHash() {
				t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
			}
		} else {
			// if the caches should *not* be shared but there was
			// no error getting the digest: fail.
			if err == nil {
				t.Fatalf("Got %q from cache, but should have been isolated.", d.GetHash())
			}
		}
	}
}

func TestGetSet(t *testing.T) {
	maxSizeBytes := int64(1000000000) // 1GB
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		ctx := getAnonContext(t)
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		// Set() the bytes in the cache.
		err := mc.Set(ctx, d, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		// Get() the bytes from the cache.
		rbuf, err := mc.Get(ctx, d)
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func randomDigests(t *testing.T, sizes ...int64) map[*repb.Digest][]byte {
	m := make(map[*repb.Digest][]byte)
	for _, size := range sizes {
		d, buf := testdigest.NewRandomDigestBuf(t, size)
		m[d] = buf
	}
	return m
}

func TestMultiGetSet(t *testing.T) {
	maxSizeBytes := int64(1000000000) // 1GB
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t)
	digests := randomDigests(t, 10, 20, 11, 30, 40)
	if err := mc.SetMulti(ctx, digests); err != nil {
		t.Fatalf("Error multi-setting digests: %s", err.Error())
	}
	digestKeys := make([]*repb.Digest, 0, len(digests))
	for d := range digests {
		digestKeys = append(digestKeys, d)
	}
	m, err := mc.GetMulti(ctx, digestKeys)
	if err != nil {
		t.Fatalf("Error multi-getting digests: %s", err.Error())
	}
	for d := range digests {
		rbuf, ok := m[d]
		if !ok {
			t.Fatalf("Multi-get failed to return expected digest: %q", d.GetHash())
		}
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if err != nil {
			t.Fatal(err)
		}
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match multi-set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestReadWrite(t *testing.T) {
	maxSizeBytes := int64(1000000000) // 1GB
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		ctx := getAnonContext(t)
		d, r := testdigest.NewRandomDigestReader(t, testSize)
		// Use Writer() to set the bytes in the cache.
		wc, err := mc.Writer(ctx, d)
		if err != nil {
			t.Fatalf("Error getting %q writer: %s", d.GetHash(), err.Error())
		}
		if _, err := io.Copy(wc, r); err != nil {
			t.Fatalf("Error copying bytes to cache: %s", err.Error())
		}
		if err := wc.Close(); err != nil {
			t.Fatalf("Error closing writer: %s", err.Error())
		}
		// Use Reader() to get the bytes from the cache.
		reader, err := mc.Reader(ctx, d, 0, 0)
		if err != nil {
			t.Fatalf("Error getting %q reader: %s", d.GetHash(), err.Error())
		}
		d2 := testdigest.ReadDigestAndClose(t, reader)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestReadOffsetLimit(t *testing.T) {
	mc, err := memory_cache.NewMemoryCache(1000)
	require.NoError(t, err)

	ctx := getAnonContext(t)
	size := int64(10)
	d, buf := testdigest.NewRandomDigestBuf(t, size)
	err = mc.Set(ctx, d, buf)
	require.NoError(t, err)

	offset := int64(2)
	limit := int64(3)
	reader, err := mc.Reader(ctx, d, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, size)
	n, err := reader.Read(readBuf)
	require.EqualValues(t, limit, n)
	require.Equal(t, buf[offset:offset+limit], readBuf[:limit])
}

func TestSizeLimit(t *testing.T) {
	maxSizeBytes := int64(1000) // 1000 bytes
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t)
	digestBufs := randomDigests(t, 400, 400, 400)
	digestKeys := make([]*repb.Digest, 0, len(digestBufs))
	for d, buf := range digestBufs {
		if err := mc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		digestKeys = append(digestKeys, d)
	}
	// Expect the last *2* digests to be present.
	// The first digest should have been evicted.
	for i, d := range digestKeys {
		rbuf, err := mc.Get(ctx, d)
		if i == 0 {
			if err == nil {
				t.Fatalf("%q should have been evicted from cache", d.GetHash())
			}
			continue
		}
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}
		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestLRU(t *testing.T) {
	maxSizeBytes := int64(1000) // 1000 bytes
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx := getAnonContext(t)
	digestBufs := randomDigests(t, 400, 400)
	digestKeys := make([]*repb.Digest, 0, len(digestBufs))
	for d, buf := range digestBufs {
		if err := mc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		digestKeys = append(digestKeys, d)
	}
	// Now "use" the first digest written so it is most recently used.
	ok, err := mc.ContainsDeprecated(ctx, digestKeys[0])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Key %q was not present in cache, it should have been.", digestKeys[0].GetHash())
	}
	// Now write one more digest, which should evict the oldest digest,
	// (the second one we wrote).
	d, buf := testdigest.NewRandomDigestBuf(t, 400)
	if err := mc.Set(ctx, d, buf); err != nil {
		t.Fatal(err)
	}
	digestKeys = append(digestKeys, d)

	// Expect the first and third digests to be present.
	// The second digest should have been evicted.
	for i, d := range digestKeys {
		rbuf, err := mc.Get(ctx, d)
		if i == 1 {
			if err == nil {
				t.Fatalf("%q should have been evicted from cache", d.GetHash())
			}
			continue
		}
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}
		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}
