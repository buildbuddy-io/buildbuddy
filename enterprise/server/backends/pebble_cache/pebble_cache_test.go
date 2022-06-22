package pebble_cache_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getAnonContext(t *testing.T, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func TestACIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	c1, err := pc.WithIsolation(ctx, interfaces.ActionCacheType, "foo")
	require.Nil(t, err)
	c2, err := pc.WithIsolation(ctx, interfaces.ActionCacheType, "bar")
	require.Nil(t, err)

	d1, buf1 := testdigest.NewRandomDigestBuf(t, 100)

	require.Nil(t, c1.Set(ctx, d1, buf1))
	require.Nil(t, c2.Set(ctx, d1, []byte("evilbuf")))

	got1, err := c1.Get(ctx, d1)
	require.Nil(t, err, err)
	require.Equal(t, buf1, got1)
}

func TestIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	type test struct {
		cache1         interfaces.Cache
		cache2         interfaces.Cache
		shouldBeShared bool
	}
	mustIsolate := func(cacheType interfaces.CacheType, remoteInstanceName string) interfaces.Cache {
		c, err := pc.WithIsolation(ctx, cacheType, remoteInstanceName)
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
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		// Set() the bytes in the cache.
		err := pc.Set(ctx, d, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		// Get() the bytes from the cache.
		rbuf, err := pc.Get(ctx, d)
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
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	digests := randomDigests(t, 10, 20, 11, 30, 40)
	if err := pc.SetMulti(ctx, digests); err != nil {
		t.Fatalf("Error multi-setting digests: %s", err.Error())
	}
	digestKeys := make([]*repb.Digest, 0, len(digests))
	for d := range digests {
		digestKeys = append(digestKeys, d)
	}
	m, err := pc.GetMulti(ctx, digestKeys)
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
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, testSize := range testSizes {
		d, r := testdigest.NewRandomDigestReader(t, testSize)
		// Use Writer() to set the bytes in the cache.
		wc, err := pc.Writer(ctx, d)
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
		reader, err := pc.Reader(ctx, d, 0, 0)
		if err != nil {
			t.Fatalf("Error getting %q reader: %s", d.GetHash(), err.Error())
		}
		d2 := testdigest.ReadDigestAndClose(t, reader)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestSizeLimit(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(100_000_000)
	rootDir := testfs.MakeTempDir(t)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	digestKeys := make([]*repb.Digest, 0, 150000)
	for i := 0; i < 150; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		digestKeys = append(digestKeys, d)
		if err := pc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
	}

	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()

	// Expect the sum of all contained digests be less than or equal to max
	// size bytes.
	containedDigestsSize := int64(0)
	for _, d := range digestKeys {
		if ok, err := pc.Contains(ctx, d); err == nil && ok {
			containedDigestsSize += d.GetSizeBytes()
		}
	}
	require.LessOrEqual(t, containedDigestsSize, maxSizeBytes)

	// Expect the on disk directory size be less than or equal to max size
	// bytes.
	dirSize, err := disk.DirSize(rootDir)
	require.Nil(t, err)
	require.LessOrEqual(t, dirSize, maxSizeBytes)
}

func TestLRU(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	numDigests := 1000
	digestSize := 100
	maxSizeBytes := int64(numDigests * digestSize)
	rootDir := testfs.MakeTempDir(t)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	quartile := numDigests / 4

	digestKeys := make([]*repb.Digest, numDigests)
	for i := range digestKeys {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		digestKeys[i] = d
		if err := pc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err)
		}
	}

	// Use the digests in the following way:
	// 1) first 3 quartiles
	// 2) first 2 quartiles
	// 3) first quartile
	// This sets us up so we add an additional quartile of data
	// and then expect data from the 3rd quartile (least recently used)
	// to be the most evicted.
	for i := 3; i > 0; i-- {
		log.Printf("Using data from 0:%d", quartile*i)
		for j := 0; j < quartile*i; j++ {
			d := digestKeys[j]
			if _, err := pc.Contains(ctx, d); err != nil {
				t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err)
			}
		}
	}

	// Write more data.
	for i := 0; i < quartile; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		digestKeys = append(digestKeys, d)
		if err := pc.Set(ctx, d, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err)
		}
	}

	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()

	evictionsByQuartile := make([][]*repb.Digest, 5)
	for i, d := range digestKeys {
		ok, err := pc.Contains(ctx, d)
		evicted := err != nil || !ok
		q := i / quartile
		if evicted {
			evictionsByQuartile[q] = append(evictionsByQuartile[q], d)
		}
	}

	for quartile, evictions := range evictionsByQuartile {
		count := len(evictions)
		sample := ""
		for i, d := range evictions {
			if i > 3 {
				break
			}
			sample += d.GetHash()
			sample += ", "
		}
		log.Printf("Evicted %d keys in quartile: %d (%s)", count, quartile, sample)
	}

	// None of the files "used" just before adding more should have been
	// evicted.
	require.Equal(t, 0, len(evictionsByQuartile[0]))

	// None of the most recently added files should have been evicted.
	require.Equal(t, 0, len(evictionsByQuartile[4]))

	require.LessOrEqual(t, len(evictionsByQuartile[1]), len(evictionsByQuartile[2]))
	require.LessOrEqual(t, len(evictionsByQuartile[2]), len(evictionsByQuartile[3]))
}

func TestMigrationFromDiskV1(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1e9)
	diskDir := testfs.MakeTempDir(t)
	dc, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: diskDir}, maxSizeBytes)
	require.Nil(t, err)

	setKeys := make(map[*digest.ResourceName][]byte, 0)
	for i := 0; i < 1000; i++ {
		remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
		c, err := dc.WithIsolation(ctx, interfaces.ActionCacheType, remoteInstanceName)
		require.Nil(t, err)

		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		err = c.Set(ctx, d, buf)
		require.Nil(t, err)
		setKeys[digest.NewResourceName(d, remoteInstanceName)] = buf
	}

	pebbleDir := testfs.MakeTempDir(t)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: pebbleDir, MaxSizeBytes: maxSizeBytes})
	require.Nil(t, err)
	err = pc.MigrateFromDiskDir(diskDir)
	require.Nil(t, err)

	pc.Start()
	defer pc.Stop()

	for rn, buf := range setKeys {
		c, err := pc.WithIsolation(ctx, interfaces.ActionCacheType, rn.GetInstanceName())
		require.Nil(t, err)

		gotBuf, err := c.Get(ctx, rn.GetDigest())
		require.Nil(t, err)

		require.Equal(t, buf, gotBuf)
	}
}

func TestMigrationFromDiskV2(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te.SetAuthenticator(testauth.NewTestAuthenticator(testUsers))

	maxSizeBytes := int64(1e9)
	diskDir := testfs.MakeTempDir(t)

	diskConfig := &disk_cache.Options{
		RootDirectory: diskDir,
		UseV2Layout:   true,
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: maxSizeBytes,
			},
			{
				ID:           "FOO",
				MaxSizeBytes: maxSizeBytes,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: "FOO",
			},
		},
	}

	dc, err := disk_cache.NewDiskCache(te, diskConfig, maxSizeBytes)
	require.Nil(t, err)

	ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
	ctx, err = prefix.AttachUserPrefixToContext(ctx, te)

	setKeys := make(map[*digest.ResourceName][]byte, 0)
	for i := 0; i < 1000; i++ {
		remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
		c, err := dc.WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
		require.Nil(t, err)

		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		err = c.Set(ctx, d, buf)
		require.Nil(t, err)
		setKeys[digest.NewResourceName(d, remoteInstanceName)] = buf
	}

	pebbleDir := testfs.MakeTempDir(t)
	pebbleConfig := &pebble_cache.Options{
		RootDirectory:     pebbleDir,
		MaxSizeBytes:      maxSizeBytes,
		Partitions:        diskConfig.Partitions,
		PartitionMappings: diskConfig.PartitionMappings,
	}
	pc, err := pebble_cache.NewPebbleCache(te, pebbleConfig)
	require.Nil(t, err)
	err = pc.MigrateFromDiskDir(diskDir)
	require.Nil(t, err)

	pc.Start()
	defer pc.Stop()

	for rn, buf := range setKeys {
		c, err := pc.WithIsolation(ctx, interfaces.CASCacheType, rn.GetInstanceName())
		require.Nil(t, err)

		gotBuf, err := c.Get(ctx, rn.GetDigest())
		require.Nil(t, err, err)

		require.Equal(t, buf, gotBuf)
	}
}
