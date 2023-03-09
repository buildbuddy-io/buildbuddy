package pebble_cache_test

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getAnonContext(t testing.TB, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func TestSetOptionDefaults(t *testing.T) {
	// Test sets all fields for empty Options
	opts := &pebble_cache.Options{}
	pebble_cache.SetOptionDefaults(opts)
	require.Equal(t, pebble_cache.DefaultMaxSizeBytes, opts.MaxSizeBytes)
	require.Equal(t, pebble_cache.DefaultBlockCacheSizeBytes, opts.BlockCacheSizeBytes)
	require.Equal(t, pebble_cache.DefaultMaxInlineFileSizeBytes, opts.MaxInlineFileSizeBytes)
	require.Equal(t, &pebble_cache.DefaultAtimeUpdateThreshold, opts.AtimeUpdateThreshold)
	require.Equal(t, &pebble_cache.DefaultAtimeBufferSize, opts.AtimeBufferSize)
	require.Equal(t, &pebble_cache.DefaultMinEvictionAge, opts.MinEvictionAge)

	// Test does not overwrite fields that are explicitly set
	atimeUpdateThreshold := time.Duration(10)
	atimeBufferSize := 20
	minEvictionAge := time.Duration(30)
	opts = &pebble_cache.Options{
		MaxSizeBytes:           1,
		BlockCacheSizeBytes:    2,
		MaxInlineFileSizeBytes: 3,
		AtimeUpdateThreshold:   &atimeUpdateThreshold,
		AtimeBufferSize:        &atimeBufferSize,
		MinEvictionAge:         &minEvictionAge,
	}
	pebble_cache.SetOptionDefaults(opts)
	require.Equal(t, int64(1), opts.MaxSizeBytes)
	require.Equal(t, int64(2), opts.BlockCacheSizeBytes)
	require.Equal(t, int64(3), opts.MaxInlineFileSizeBytes)
	require.Equal(t, &atimeUpdateThreshold, opts.AtimeUpdateThreshold)
	require.Equal(t, &atimeBufferSize, opts.AtimeBufferSize)
	require.Equal(t, &minEvictionAge, opts.MinEvictionAge)

	// Test mix of set and unset fields
	opts = &pebble_cache.Options{
		MaxSizeBytes:         1,
		AtimeUpdateThreshold: &atimeUpdateThreshold,
	}
	pebble_cache.SetOptionDefaults(opts)
	require.Equal(t, int64(1), opts.MaxSizeBytes)
	require.Equal(t, &atimeUpdateThreshold, opts.AtimeUpdateThreshold)
	require.Equal(t, pebble_cache.DefaultBlockCacheSizeBytes, opts.BlockCacheSizeBytes)
	require.Equal(t, pebble_cache.DefaultMaxInlineFileSizeBytes, opts.MaxInlineFileSizeBytes)
	require.Equal(t, &pebble_cache.DefaultAtimeBufferSize, opts.AtimeBufferSize)
	require.Equal(t, &pebble_cache.DefaultMinEvictionAge, opts.MinEvictionAge)

	// Test does not overwrite fields that are explicitly and validly set to 0
	atimeUpdateThreshold = time.Duration(0)
	atimeBufferSize = 0
	minEvictionAge = time.Duration(0)
	opts = &pebble_cache.Options{
		AtimeUpdateThreshold: &atimeUpdateThreshold,
		AtimeBufferSize:      &atimeBufferSize,
		MinEvictionAge:       &minEvictionAge,
	}
	pebble_cache.SetOptionDefaults(opts)
	require.Equal(t, &atimeUpdateThreshold, opts.AtimeUpdateThreshold)
	require.Equal(t, &atimeBufferSize, opts.AtimeBufferSize)
	require.Equal(t, &minEvictionAge, opts.MinEvictionAge)
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

	d1, buf1 := testdigest.NewRandomDigestBuf(t, 100)
	r1 := &rspb.ResourceName{
		Digest:       d1,
		CacheType:    rspb.CacheType_AC,
		InstanceName: "foo",
	}
	r2 := &rspb.ResourceName{
		Digest:       d1,
		CacheType:    rspb.CacheType_AC,
		InstanceName: "bar",
	}

	require.Nil(t, pc.Set(ctx, r1, buf1))
	require.Nil(t, pc.Set(ctx, r2, []byte("evilbuf")))

	got1, err := pc.Get(ctx, r1)
	require.NoError(t, err)
	require.Equal(t, buf1, got1)

	contains, err := pc.Contains(ctx, r1)
	require.NoError(t, err)
	require.True(t, contains)
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
		cacheType1     rspb.CacheType
		cacheType2     rspb.CacheType
		instanceName1  string
		instanceName2  string
		shouldBeShared bool
	}
	tests := []test{
		{ // caches with the same isolation are shared.
			cacheType1:     rspb.CacheType_CAS,
			instanceName1:  "remoteInstanceName",
			cacheType2:     rspb.CacheType_CAS,
			instanceName2:  "remoteInstanceName",
			shouldBeShared: true,
		},
		{ // action caches with the same isolation are shared.
			cacheType1:     rspb.CacheType_AC,
			instanceName1:  "remoteInstanceName",
			cacheType2:     rspb.CacheType_AC,
			instanceName2:  "remoteInstanceName",
			shouldBeShared: true,
		},
		{ // CAS caches with different remote instance names are shared.
			cacheType1:     rspb.CacheType_CAS,
			instanceName1:  "remoteInstanceName",
			cacheType2:     rspb.CacheType_CAS,
			instanceName2:  "otherInstanceName",
			shouldBeShared: true,
		},
		{ // Action caches with different remote instance names are not shared.
			cacheType1:     rspb.CacheType_AC,
			instanceName1:  "remoteInstanceName",
			cacheType2:     rspb.CacheType_AC,
			instanceName2:  "otherInstanceName",
			shouldBeShared: false,
		},
		{ // CAS and Action caches are not shared.
			cacheType1:     rspb.CacheType_CAS,
			instanceName1:  "remoteInstanceName",
			cacheType2:     rspb.CacheType_AC,
			instanceName2:  "remoteInstanceName",
			shouldBeShared: false,
		},
	}

	for _, test := range tests {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r1 := &rspb.ResourceName{
			Digest:       d,
			CacheType:    test.cacheType1,
			InstanceName: test.instanceName1,
		}
		// Set() the bytes in cache1.
		err = pc.Set(ctx, r1, buf)
		require.NoError(t, err)

		// Get() the bytes from cache2.
		rbuf, err := pc.Get(ctx, &rspb.ResourceName{
			Digest:       d,
			InstanceName: test.instanceName2,
			CacheType:    test.cacheType2,
		})
		if test.shouldBeShared {
			// if the caches should be shared but there was an error
			// getting the digest: fail.
			if err != nil {
				t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
			}

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
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
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		// Set() the bytes in the cache.
		err := pc.Set(ctx, r, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
		// Get() the bytes from the cache.
		rbuf, err := pc.Get(ctx, r)
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

func TestDupeWrites(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		RootDirectory:          testfs.MakeTempDir(t),
		MaxSizeBytes:           maxSizeBytes,
		MaxInlineFileSizeBytes: 100,
	})
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)
	defer pc.Stop()

	var tests = []struct {
		name      string
		size      int64
		cacheType rspb.CacheType
	}{
		{"cas_inline", 1, rspb.CacheType_CAS},
		{"cas_extern", 1000, rspb.CacheType_CAS},
		{"ac_inline", 1, rspb.CacheType_AC},
		{"ac_extern", 1000, rspb.CacheType_AC},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, buf := testdigest.NewRandomDigestBuf(t, test.size)
			r := &rspb.ResourceName{Digest: d, CacheType: test.cacheType}

			w1, err := pc.Writer(ctx, r)
			require.NoError(t, err)
			_, err = w1.Write(buf)
			require.NoError(t, err)

			w2, err := pc.Writer(ctx, r)
			require.NoError(t, err)
			_, err = w2.Write(buf)
			require.NoError(t, err)

			err = w1.Commit()
			require.NoError(t, err)
			err = w1.Close()
			require.NoError(t, err)

			err = w2.Commit()
			require.NoError(t, err)
			err = w2.Close()
			require.NoError(t, err)

			// Verify we can read the data back after dupe writes are done.
			rbuf, err := pc.Get(ctx, r)
			require.NoError(t, err)

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
			if d.GetHash() != d2.GetHash() {
				t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
			}
		})
	}
}

func TestIsolateByGroupIds(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te.SetAuthenticator(testauth.NewTestAuthenticator(testUsers))

	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	partitionID := "FOO"
	instanceName := "cloud"
	opts := &pebble_cache.Options{
		RootDirectory:          rootDir,
		MaxSizeBytes:           maxSizeBytes,
		MaxInlineFileSizeBytes: 1, // Ensure file is written to disk
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: maxSizeBytes,
			},
			{
				ID:           partitionID,
				MaxSizeBytes: maxSizeBytes,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: partitionID,
			},
		},
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	pc.Start()
	defer pc.Stop()

	// Matching authenticated user should use non-default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		// CAS records should not have group ID or remote instance name in their file path
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
			InstanceName: instanceName,
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := d.GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, partitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)

		// AC records should have group ID and remote instance hash in their file path
		d, buf = testdigest.NewRandomDigestBuf(t, 1000)
		r = &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_AC,
			InstanceName: instanceName,
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err = pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		instanceNameHash := strconv.Itoa(int(crc32.ChecksumIEEE([]byte(instanceName))))
		hash = d.GetHash()
		expectedFilename = fmt.Sprintf("%s/blobs/PT%s/%s/ac/%s/%v/%v", rootDir, partitionID, testGroup, instanceNameHash, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}

	// Anon user should use the default partition.
	{
		ctx := getAnonContext(t, te)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
			InstanceName: instanceName,
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := d.GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, pebble_cache.DefaultPartitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}
}

func TestCopyPartitionData(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te.SetAuthenticator(testauth.NewTestAuthenticator(testUsers))

	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	partitionID := "FOO"
	instanceName := "cloud"
	opts := &pebble_cache.Options{
		RootDirectory:          rootDir,
		MaxSizeBytes:           maxSizeBytes,
		MaxInlineFileSizeBytes: 100,
		Partitions: []disk.Partition{
			{
				ID:           pebble_cache.DefaultPartitionID,
				MaxSizeBytes: maxSizeBytes,
			},
			{
				ID:           partitionID,
				MaxSizeBytes: maxSizeBytes,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: partitionID,
			},
		},
	}

	// Create a cache and write some data in default and custom partitions.

	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)

	var defaultResources []*rspb.ResourceName
	var customResources []*rspb.ResourceName

	// Write some data to the default partition.
	{
		ctx := getAnonContext(t, te)
		for i := 0; i < 10; i++ {
			size := int64(10)
			// Mix of inline and extern data.
			if i > 5 {
				size = 1000
			}
			d, buf := testdigest.NewRandomDigestBuf(t, size)
			r := digest.NewResourceName(d, instanceName, rspb.CacheType_CAS).ToProto()
			defaultResources = append(defaultResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			r := digest.NewResourceName(d, instanceName, rspb.CacheType_AC).ToProto()
			defaultResources = append(defaultResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
	}

	// Write some data to the custom partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		for i := 0; i < 10; i++ {
			d, buf := testdigest.NewRandomDigestBuf(t, 1000)
			r := digest.NewResourceName(d, instanceName, rspb.CacheType_CAS).ToProto()
			customResources = append(customResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			d, buf := testdigest.NewRandomDigestBuf(t, 100)
			r := digest.NewResourceName(d, instanceName, rspb.CacheType_AC).ToProto()
			customResources = append(customResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
	}

	err = pc.Stop()
	require.NoError(t, err)

	// copy data from default to anon partition
	flags.Set(t, "cache.pebble.copy_partition_data", pebble_cache.DefaultPartitionID+":"+interfaces.AuthAnonymousUser)

	// Now add the anon partition and remove the custom partition.
	opts = &pebble_cache.Options{
		RootDirectory:          rootDir,
		MaxSizeBytes:           maxSizeBytes,
		MaxInlineFileSizeBytes: 100,
		Partitions: []disk.Partition{
			{
				ID:           pebble_cache.DefaultPartitionID,
				MaxSizeBytes: maxSizeBytes,
			},
			{
				ID:           interfaces.AuthAnonymousUser,
				MaxSizeBytes: maxSizeBytes,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     interfaces.AuthAnonymousUser,
				Prefix:      "",
				PartitionID: interfaces.AuthAnonymousUser,
			},
			{
				GroupID:     testGroup,
				Prefix:      "",
				PartitionID: interfaces.AuthAnonymousUser,
			},
		},
	}
	pc, err = pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)

	// Data that was in the original partition should be readable.
	{
		ctx := getAnonContext(t, te)
		for _, r := range defaultResources {
			_, err := pc.Get(ctx, r)
			require.NoError(t, err)
		}
	}

	// Data that was in the custom partition should not have been copied.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		for _, r := range customResources {
			_, err := pc.Get(ctx, r)
			require.Error(t, err)
		}
	}

	pc.Stop()
	pc, err = pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
}

func TestIsolateAnonUsers(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testAPIKey := "AK2222"
	testGroup := "GR7890"
	testUsers := testauth.TestUsers(testAPIKey, testGroup)
	te.SetAuthenticator(testauth.NewTestAuthenticator(testUsers))

	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	partitionID := "FOO"
	instanceName := "cloud"
	opts := &pebble_cache.Options{
		RootDirectory:          rootDir,
		MaxSizeBytes:           maxSizeBytes,
		MaxInlineFileSizeBytes: 1, // Ensure file is written to disk
		Partitions: []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: maxSizeBytes,
			},
			{
				ID:           partitionID,
				MaxSizeBytes: maxSizeBytes,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     interfaces.AuthAnonymousUser,
				Prefix:      "",
				PartitionID: partitionID,
			},
		},
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	pc.Start()
	defer pc.Stop()

	// Anon user should use matching anon partition.
	{
		ctx := getAnonContext(t, te)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
			InstanceName: instanceName,
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := d.GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, partitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}

	// Authenticated user should use default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		// CAS records should not have group ID or remote instance name in their file path
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
			InstanceName: instanceName,
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := d.GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, pebble_cache.DefaultPartitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}
}

func TestMetadata(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	options := &pebble_cache.Options{
		RootDirectory:               testfs.MakeTempDir(t),
		MaxSizeBytes:                maxSizeBytes,
		MinBytesAutoZstdCompression: math.MaxInt64, // Turn off automatic compression
	}
	pc, err := pebble_cache.NewPebbleCache(te, options)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	testCases := []struct {
		name       string
		cacheType  rspb.CacheType
		compressor repb.Compressor_Value
	}{
		{
			name:       "CAS uncompressed",
			cacheType:  rspb.CacheType_CAS,
			compressor: repb.Compressor_IDENTITY,
		},
		{
			name:       "CAS compressed",
			cacheType:  rspb.CacheType_CAS,
			compressor: repb.Compressor_ZSTD,
		},
		{
			name:       "AC uncompressed",
			cacheType:  rspb.CacheType_AC,
			compressor: repb.Compressor_IDENTITY,
		},
		{
			name:       "AC compressed",
			cacheType:  rspb.CacheType_AC,
			compressor: repb.Compressor_ZSTD,
		},
	}

	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}
	for _, tc := range testCases {
		for _, testSize := range testSizes {
			d, buf := testdigest.NewRandomDigestBuf(t, testSize)

			dataToWrite := buf
			if tc.compressor == repb.Compressor_ZSTD {
				dataToWrite = compression.CompressZstd(nil, buf)
			}
			r := &rspb.ResourceName{
				Digest:     d, // Digest contains uncompressed size
				CacheType:  tc.cacheType,
				Compressor: tc.compressor,
			}

			// Set data in the cache.
			err := pc.Set(ctx, r, dataToWrite)
			require.NoError(t, err, tc.name)

			// Metadata should return correct size, regardless of queried size.
			digestWrongSize := &repb.Digest{Hash: d.GetHash(), SizeBytes: 1}
			rWrongSize := &rspb.ResourceName{
				Digest:    digestWrongSize,
				CacheType: tc.cacheType,
			}

			md, err := pc.Metadata(ctx, rWrongSize)
			require.NoError(t, err, tc.name)
			require.Equal(t, int64(len(dataToWrite)), md.StoredSizeBytes, tc.name)
			require.Equal(t, testSize, md.DigestSizeBytes, tc.name)
			lastAccessTime1 := md.LastAccessTimeUsec
			lastModifyTime1 := md.LastModifyTimeUsec
			require.NotZero(t, lastAccessTime1)
			require.NotZero(t, lastModifyTime1)

			// Last access time should not update since last call to Metadata()
			md, err = pc.Metadata(ctx, rWrongSize)
			require.NoError(t, err, tc.name)
			require.Equal(t, int64(len(dataToWrite)), md.StoredSizeBytes, tc.name)
			require.Equal(t, testSize, md.DigestSizeBytes, tc.name)
			lastAccessTime2 := md.LastAccessTimeUsec
			lastModifyTime2 := md.LastModifyTimeUsec
			require.Equal(t, lastAccessTime1, lastAccessTime2)
			require.Equal(t, lastModifyTime1, lastModifyTime2)

			// After updating data, last access and modify time should update
			err = pc.Set(ctx, r, dataToWrite)
			md, err = pc.Metadata(ctx, rWrongSize)
			require.NoError(t, err, tc.name)
			require.Equal(t, int64(len(dataToWrite)), md.StoredSizeBytes, tc.name)
			require.Equal(t, testSize, md.DigestSizeBytes, tc.name)
			lastAccessTime3 := md.LastAccessTimeUsec
			lastModifyTime3 := md.LastModifyTimeUsec
			require.Greater(t, lastAccessTime3, lastAccessTime1)
			require.Greater(t, lastModifyTime3, lastModifyTime2)
		}
	}
}

func randomDigests(t *testing.T, sizes ...int64) map[*rspb.ResourceName][]byte {
	m := make(map[*rspb.ResourceName][]byte)
	for _, size := range sizes {
		d, buf := testdigest.NewRandomDigestBuf(t, size)
		rn := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		m[rn] = buf
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
	resourceNames := make([]*rspb.ResourceName, 0, len(digests))
	for d := range digests {
		resourceNames = append(resourceNames, d)
	}
	m, err := pc.GetMulti(ctx, resourceNames)
	if err != nil {
		t.Fatalf("Error multi-getting digests: %s", err.Error())
	}
	for rn := range digests {
		d := rn.GetDigest()
		rbuf, ok := m[d]
		if !ok {
			t.Fatalf("Multi-get failed to return expected digest: %q", d.GetHash())
		}
		d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
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
		d, buf := testdigest.NewRandomDigestBuf(t, testSize)
		rn := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		// Use Writer() to set the bytes in the cache.
		wc, err := pc.Writer(ctx, rn)
		if err != nil {
			t.Fatalf("Error getting %q writer: %s", d.GetHash(), err.Error())
		}
		_, err = wc.Write(buf)
		require.NoError(t, err)
		if err := wc.Commit(); err != nil {
			t.Fatalf("Error closing writer: %s", err.Error())
		}
		if err := wc.Close(); err != nil {
			t.Fatalf("Error closing writer: %s", err.Error())
		}
		// Use Reader() to get the bytes from the cache.
		reader, err := pc.Reader(ctx, rn, 0, 0)
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

	resourceKeys := make([]*rspb.ResourceName, 0, 150000)
	for i := 0; i < 150; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		resourceKeys = append(resourceKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
	}

	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()

	// Expect the sum of all contained digests be less than or equal to max
	// size bytes.
	containedDigestsSize := int64(0)
	for _, r := range resourceKeys {
		if ok, err := pc.Contains(ctx, r); err == nil && ok {
			containedDigestsSize += r.Digest.GetSizeBytes()
		}
	}
	require.LessOrEqual(t, containedDigestsSize, maxSizeBytes)

	// Expect the on disk directory size be less than or equal to max size
	// bytes.
	dirSize, err := disk.DirSize(rootDir)
	require.NoError(t, err)
	require.LessOrEqual(t, dirSize, maxSizeBytes)
}

func TestCompression(t *testing.T) {
	// Make blob big enough to require multiple chunks to compress
	blob := compressibleBlobOfSize(pebble_cache.CompressorBufSizeBytes + 1)
	compressedBuf := compression.CompressZstd(nil, blob)

	inlineBlob := compressibleBlobOfSize(int(pebble_cache.DefaultMaxInlineFileSizeBytes - 100))
	compressedInlineBuf := compression.CompressZstd(nil, inlineBlob)

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	inlineD, err := digest.Compute(bytes.NewReader(inlineBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	compressedRN := &rspb.ResourceName{
		Digest:     d,
		CacheType:  rspb.CacheType_CAS,
		Compressor: repb.Compressor_ZSTD,
	}
	decompressedRN := &rspb.ResourceName{
		Digest:     d,
		CacheType:  rspb.CacheType_CAS,
		Compressor: repb.Compressor_IDENTITY,
	}

	compressedInlineRN := &rspb.ResourceName{
		Digest:     inlineD,
		CacheType:  rspb.CacheType_CAS,
		Compressor: repb.Compressor_ZSTD,
	}
	decompressedInlineRN := &rspb.ResourceName{
		Digest:     inlineD,
		CacheType:  rspb.CacheType_CAS,
		Compressor: repb.Compressor_IDENTITY,
	}

	testCases := []struct {
		name             string
		rnToWrite        *rspb.ResourceName
		dataToWrite      []byte
		rnToRead         *rspb.ResourceName
		isReadCompressed bool
		// You cannot directly compare the compressed bytes because there may be differences with the compression headers
		// due to the chunking compression algorithm used
		expectedUncompressedReadData []byte
	}{
		{
			name:                         "Write compressed data, read compressed data",
			rnToWrite:                    compressedRN,
			dataToWrite:                  compressedBuf,
			rnToRead:                     compressedRN,
			isReadCompressed:             true,
			expectedUncompressedReadData: blob,
		},
		{
			name:                         "Write compressed data, read decompressed data",
			rnToWrite:                    compressedRN,
			dataToWrite:                  compressedBuf,
			rnToRead:                     decompressedRN,
			expectedUncompressedReadData: blob,
		},
		{
			name:                         "Write decompressed data, read compressed data",
			rnToWrite:                    decompressedRN,
			dataToWrite:                  blob,
			rnToRead:                     compressedRN,
			isReadCompressed:             true,
			expectedUncompressedReadData: blob,
		},
		{
			name:                         "Write decompressed data, read decompressed data",
			rnToWrite:                    decompressedRN,
			dataToWrite:                  blob,
			rnToRead:                     decompressedRN,
			expectedUncompressedReadData: blob,
		},
		{
			name:                         "Write compressed inline data, read compressed data",
			rnToWrite:                    compressedInlineRN,
			dataToWrite:                  compressedInlineBuf,
			rnToRead:                     compressedInlineRN,
			isReadCompressed:             true,
			expectedUncompressedReadData: inlineBlob,
		},
		{
			name:                         "Write compressed inline data, read decompressed data",
			rnToWrite:                    compressedInlineRN,
			dataToWrite:                  compressedInlineBuf,
			rnToRead:                     decompressedInlineRN,
			expectedUncompressedReadData: inlineBlob,
		},
		{
			name:                         "Write decompressed inline data, read compressed data",
			rnToWrite:                    decompressedInlineRN,
			dataToWrite:                  inlineBlob,
			rnToRead:                     compressedInlineRN,
			isReadCompressed:             true,
			expectedUncompressedReadData: inlineBlob,
		},
		{
			name:                         "Write decompressed inline data, read decompressed data",
			rnToWrite:                    decompressedInlineRN,
			dataToWrite:                  inlineBlob,
			rnToRead:                     decompressedInlineRN,
			expectedUncompressedReadData: inlineBlob,
		},
	}

	for _, tc := range testCases {
		{
			te := testenv.GetTestEnv(t)
			te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
			ctx := getAnonContext(t, te)

			maxSizeBytes := int64(1_000_000_000) // 1GB
			opts := &pebble_cache.Options{
				RootDirectory: testfs.MakeTempDir(t),
				MaxSizeBytes:  maxSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			if err != nil {
				t.Fatal(err)
			}
			pc.Start()
			defer pc.Stop()

			// Write data to cache
			wc, err := pc.Writer(ctx, tc.rnToWrite)
			require.NoError(t, err, tc.name)
			n, err := wc.Write(tc.dataToWrite)
			require.NoError(t, err, tc.name)
			require.Equal(t, len(tc.dataToWrite), n)
			err = wc.Commit()
			require.NoError(t, err, tc.name)
			err = wc.Close()
			require.NoError(t, err, tc.name)

			// Read data
			reader, err := pc.Reader(ctx, tc.rnToRead, 0, 0)
			require.NoError(t, err, tc.name)
			defer reader.Close()
			data, err := io.ReadAll(reader)
			require.NoError(t, err, tc.name)
			if tc.isReadCompressed {
				data, err = compression.DecompressZstd(nil, data)
				require.NoError(t, err, tc.name)
			}
			require.Equal(t, tc.expectedUncompressedReadData, data, tc.name)
		}
	}
}

func TestCompression_BufferPoolReuse(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1000)
	opts := &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  maxSizeBytes,
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	// Do multiple reads to reuse buffers in bufferpool
	for i := 0; i < 5; i++ {
		blob := compressibleBlobOfSize(100)

		// Note: Digest is of uncompressed contents
		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)
		decompressedRN := &rspb.ResourceName{
			Digest:     d,
			CacheType:  rspb.CacheType_CAS,
			Compressor: repb.Compressor_IDENTITY,
		}

		// Write non-compressed data to cache
		wc, err := pc.Writer(ctx, decompressedRN)
		require.NoError(t, err)
		n, err := wc.Write(blob)
		require.NoError(t, err)
		require.Equal(t, len(blob), n)
		err = wc.Commit()
		require.NoError(t, err)
		err = wc.Close()
		require.NoError(t, err)

		// Read data in compressed form
		compressedRN := &rspb.ResourceName{
			Digest:     d,
			CacheType:  rspb.CacheType_CAS,
			Compressor: repb.Compressor_ZSTD,
		}
		reader, err := pc.Reader(ctx, compressedRN, 0, 0)
		require.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		decompressed, err := compression.DecompressZstd(nil, data)
		require.Equal(t, blob, decompressed)
	}
}

func TestCompression_ParallelRequests(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1000)
	opts := &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  maxSizeBytes,
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	eg := errgroup.Group{}
	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			blob := compressibleBlobOfSize(10000)

			// Note: Digest is of uncompressed contents
			d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
			require.NoError(t, err)
			decompressedRN := &rspb.ResourceName{
				Digest:     d,
				CacheType:  rspb.CacheType_CAS,
				Compressor: repb.Compressor_IDENTITY,
			}

			// Write non-compressed data to cache
			wc, err := pc.Writer(ctx, decompressedRN)
			require.NoError(t, err)
			n, err := wc.Write(blob)
			require.NoError(t, err)
			require.Equal(t, len(blob), n)
			err = wc.Commit()
			require.NoError(t, err)
			err = wc.Close()
			require.NoError(t, err)

			// Read data in compressed form
			compressedRN := &rspb.ResourceName{
				Digest:     d,
				CacheType:  rspb.CacheType_CAS,
				Compressor: repb.Compressor_ZSTD,
			}
			reader, err := pc.Reader(ctx, compressedRN, 0, 0)
			require.NoError(t, err)
			defer reader.Close()
			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			decompressed, err := compression.DecompressZstd(nil, data)
			require.Equal(t, blob, decompressed)
			return nil
		})
	}
	eg.Wait()
}

func TestCompression_NoEarlyEviction(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	numDigests := 10
	totalSizeCompresedData := 0
	digestBlobs := make(map[*repb.Digest][]byte, numDigests)
	for i := 0; i < numDigests; i++ {
		blob := compressibleBlobOfSize(2000)
		compressed := compression.CompressZstd(nil, blob)
		require.Less(t, len(compressed), len(blob))
		totalSizeCompresedData += len(compressed)

		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)
		digestBlobs[d] = blob
	}

	minEvictionAge := time.Duration(0)
	// Base max size off compressed data. Cache should fit all compressed data, but evict decompressed data
	maxSizeBytes := int64(
		math.Ceil( // account for integer rounding
			float64(totalSizeCompresedData) *
				(1 / pebble_cache.JanitorCutoffThreshold))) // account for .9 evictor cutoff
	opts := &pebble_cache.Options{
		RootDirectory:  testfs.MakeTempDir(t),
		MaxSizeBytes:   maxSizeBytes,
		MinEvictionAge: &minEvictionAge,
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	pc.Start()
	defer pc.Stop()

	// Write decompressed bytes to the cache. Because blob compression is enabled, pebble should compress before writing
	for d, blob := range digestBlobs {
		rn := &rspb.ResourceName{
			Digest:     d,
			Compressor: repb.Compressor_IDENTITY,
			CacheType:  rspb.CacheType_CAS,
		}
		err = pc.Set(ctx, rn, blob)
		require.NoError(t, err)
	}
	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()

	// All reads should succeed. Nothing should've been evicted
	for d, blob := range digestBlobs {
		rn := &rspb.ResourceName{
			Digest:     d,
			Compressor: repb.Compressor_IDENTITY,
			CacheType:  rspb.CacheType_CAS,
		}
		data, err := pc.Get(ctx, rn)
		require.NoError(t, err)
		require.True(t, bytes.Equal(blob, data))
	}
}

func TestCompressionOffset(t *testing.T) {
	// Make blob big enough to require multiple chunks to compress
	blob := compressibleBlobOfSize(pebble_cache.CompressorBufSizeBytes + 1)
	compressedBuf := compression.CompressZstd(nil, blob)

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	compressedRN := &rspb.ResourceName{
		Digest:     d,
		CacheType:  rspb.CacheType_CAS,
		Compressor: repb.Compressor_ZSTD,
	}
	decompressedRN := &rspb.ResourceName{
		Digest:     d,
		CacheType:  rspb.CacheType_CAS,
		Compressor: repb.Compressor_IDENTITY,
	}

	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB
	opts := &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  maxSizeBytes,
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	pc.Start()
	defer pc.Stop()

	// Write data to cache
	wc, err := pc.Writer(ctx, compressedRN)
	require.NoError(t, err)
	_, err = wc.Write(compressedBuf)
	require.NoError(t, err)
	err = wc.Commit()
	require.NoError(t, err)
	err = wc.Close()
	require.NoError(t, err)

	// Read data
	offset := int64(1024)
	limit := int64(10)
	reader, err := pc.Reader(ctx, decompressedRN, offset, limit)
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, blob[offset:offset+limit], data)
}

func compressibleBlobOfSize(sizeBytes int) []byte {
	out := make([]byte, 0, sizeBytes)
	for len(out) < sizeBytes {
		runEnd := len(out) + 100 + rand.Intn(100)
		if runEnd > sizeBytes {
			runEnd = sizeBytes
		}

		runChar := byte(rand.Intn('Z'-'A'+1)) + 'A'
		for len(out) < runEnd {
			out = append(out, runChar)
		}
	}
	return out
}

func TestFindMissing(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1000)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: testfs.MakeTempDir(t), MaxSizeBytes: maxSizeBytes})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	notSetD1, _ := testdigest.NewRandomDigestBuf(t, 100)
	notSetD2, _ := testdigest.NewRandomDigestBuf(t, 100)

	err = pc.Set(ctx, &rspb.ResourceName{
		Digest:       d,
		CacheType:    rspb.CacheType_AC,
		InstanceName: "remote",
	}, buf)
	require.NoError(t, err)

	digests := []*repb.Digest{d, notSetD1, notSetD2}
	rns := digest.ResourceNames(rspb.CacheType_AC, "remote", digests)
	missing, err := pc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.ElementsMatch(t, []*repb.Digest{notSetD1, notSetD2}, missing)

	digests = []*repb.Digest{d}
	rns = digest.ResourceNames(rspb.CacheType_AC, "remote", digests)
	missing, err = pc.FindMissing(ctx, rns)
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestNoEarlyEviction(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	numDigests := 10
	digestSize := int64(100)
	maxSizeBytes := int64(
		math.Ceil( // account for integer rounding
			float64(numDigests) *
				float64(digestSize) *
				(1 / pebble_cache.JanitorCutoffThreshold))) // account for .9 evictor cutoff

	rootDir := testfs.MakeTempDir(t)
	atimeUpdateThreshold := time.Duration(0) // update atime on every access
	atimeBufferSize := 0                     // blocking channel of atime updates
	minEvictionAge := time.Duration(0)       // no min eviction age
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		RootDirectory:        rootDir,
		MaxSizeBytes:         maxSizeBytes,
		AtimeUpdateThreshold: &atimeUpdateThreshold,
		AtimeBufferSize:      &atimeBufferSize,
		MinEvictionAge:       &minEvictionAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	// Should be able to add 10 things without anything getting evicted
	resourceKeys := make([]*rspb.ResourceName, numDigests)
	for i := 0; i < numDigests; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, digestSize)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		resourceKeys[i] = r
		if err := pc.Set(ctx, r, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err)
		}
	}

	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()

	// Verify that nothing was evicted
	for _, r := range resourceKeys {
		_, err = pc.Get(ctx, r)
		require.NoError(t, err)
	}
}

func TestLRU(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	numDigests := 100
	digestSize := 100
	maxSizeBytes := int64(math.Ceil( // account for integer rounding
		float64(numDigests) * float64(digestSize) * (1 / pebble_cache.JanitorCutoffThreshold))) // account for .9 evictor cutoff
	rootDir := testfs.MakeTempDir(t)
	atimeUpdateThreshold := time.Duration(0) // update atime on every access
	atimeBufferSize := 0                     // blocking channel of atime updates
	minEvictionAge := time.Duration(0)       // no min eviction age
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		RootDirectory:               rootDir,
		MaxSizeBytes:                maxSizeBytes,
		AtimeUpdateThreshold:        &atimeUpdateThreshold,
		AtimeBufferSize:             &atimeBufferSize,
		MinEvictionAge:              &minEvictionAge,
		MinBytesAutoZstdCompression: maxSizeBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	quartile := numDigests / 4

	resourceKeys := make([]*rspb.ResourceName, numDigests)
	for i := range resourceKeys {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		resourceKeys[i] = r
		if err := pc.Set(ctx, r, buf); err != nil {
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
			r := resourceKeys[j]
			_, err = pc.Get(ctx, r)
			require.NoError(t, err)
		}
	}

	// Write more data.
	for i := 0; i < quartile; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		resourceKeys = append(resourceKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err)
		}
	}

	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()

	evictionsByQuartile := make([][]*repb.Digest, 5)
	for i, r := range resourceKeys {
		ok, err := pc.Contains(ctx, r)
		evicted := err != nil || !ok
		q := i / quartile
		if evicted {
			evictionsByQuartile[q] = append(evictionsByQuartile[q], r.GetDigest())
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
	require.Greater(t, len(evictionsByQuartile[3]), 0)
}

func TestStartupScan(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)
	rootDir := testfs.MakeTempDir(t)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	options := &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes}
	pc, err := pebble_cache.NewPebbleCache(te, options)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	digests := make([]*repb.Digest, 0)
	for i := 0; i < 1000; i++ {
		remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
		d, buf := testdigest.NewRandomDigestBuf(t, 1000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_AC,
			InstanceName: remoteInstanceName,
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		digests = append(digests, d)
	}
	log.Printf("Wrote %d digests", len(digests))

	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()
	pc.Stop()

	pc2, err := pebble_cache.NewPebbleCache(te, options)
	require.NoError(t, err)

	pc2.Start()
	defer pc2.Stop()
	for i, d := range digests {
		remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
		rbuf, err := pc2.Get(ctx, &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_AC,
			InstanceName: remoteInstanceName,
		})
		require.NoError(t, err)

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

type digestAndType struct {
	cacheType rspb.CacheType
	digest    *repb.Digest
}

func TestDeleteOrphans(t *testing.T) {
	flags.Set(t, "cache.pebble.scan_for_orphaned_files", true)
	flags.Set(t, "cache.pebble.orphan_delete_dry_run", false)
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)
	rootDir := testfs.MakeTempDir(t)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	options := &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes}
	pc, err := pebble_cache.NewPebbleCache(te, options)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	for !pc.DoneScanning() {
		time.Sleep(10 * time.Millisecond)
	}
	digests := make(map[string]*digestAndType, 0)
	for i := 0; i < 1000; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
			InstanceName: "remoteInstanceName",
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		digests[d.GetHash()] = &digestAndType{rspb.CacheType_CAS, d}
	}
	for i := 0; i < 1000; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_AC,
			InstanceName: "remoteInstanceName",
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		digests[d.GetHash()] = &digestAndType{rspb.CacheType_AC, d}
	}

	log.Printf("Wrote %d digests", len(digests))
	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()
	pc.Stop()

	// Now open the pebble database directly and delete some entries.
	// When we next start up the pebble cache, we'll expect it to
	// delete the files for the records we manually deleted from the db.
	db, err := pebble.Open(rootDir, &pebble.Options{})
	require.NoError(t, err)
	deletedDigests := make(map[string]*digestAndType, 0)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: keys.MinByte,
		UpperBound: keys.MaxByte,
	})
	iter.SeekLT(keys.MinByte)

	for iter.Next() {
		if rand.Intn(2) == 0 {
			continue
		}
		if bytes.HasPrefix(iter.Key(), pebble_cache.SystemKeyPrefix) {
			continue
		}
		err := db.Delete(iter.Key(), &pebble.WriteOptions{Sync: false})
		require.NoError(t, err)

		fileMetadata := &rfpb.FileMetadata{}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			require.NoError(t, err)
		}
		require.NoError(t, err)
		fr := fileMetadata.GetFileRecord()
		ct := rspb.CacheType_CAS
		if fr.GetIsolation().GetCacheType() == rspb.CacheType_AC {
			ct = rspb.CacheType_AC
		}
		deletedDigests[fr.GetDigest().GetHash()] = &digestAndType{ct, fr.GetDigest()}
		delete(digests, fileMetadata.GetFileRecord().GetDigest().GetHash())
	}

	err = iter.Close()
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)

	pc2, err := pebble_cache.NewPebbleCache(te, options)
	require.NoError(t, err)
	pc2.Start()
	for !pc2.DoneScanning() {
		time.Sleep(10 * time.Millisecond)
	}

	// Check that all of the deleted digests are not in the cache.
	for _, dt := range deletedDigests {
		_, err := pc2.Get(ctx, &rspb.ResourceName{
			Digest:       dt.digest,
			CacheType:    dt.cacheType,
			InstanceName: "remoteInstanceName",
		})
		require.True(t, status.IsNotFoundError(err), "digest %q should not be in the cache", dt.digest.GetHash())
	}

	// Check that the underlying files have been deleted.
	err = filepath.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if _, ok := deletedDigests[info.Name()]; ok {
				t.Fatalf("%q file should have been deleted but was found on disk", filepath.Join(path, info.Name()))
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Check that all of the non-deleted items are still fetchable.
	for _, dt := range digests {
		_, err = pc2.Get(ctx, &rspb.ResourceName{
			Digest:       dt.digest,
			CacheType:    dt.cacheType,
			InstanceName: "remoteInstanceName",
		})
		require.NoError(t, err)
	}

	pc2.Stop()
}

func TestDeleteEmptyDirs(t *testing.T) {
	flags.Set(t, "cache.pebble.dir_deletion_delay", time.Nanosecond)
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)
	rootDir := testfs.MakeTempDir(t)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	options := &pebble_cache.Options{
		RootDirectory:          rootDir,
		MaxSizeBytes:           maxSizeBytes,
		MaxInlineFileSizeBytes: 0,
	}
	pc, err := pebble_cache.NewPebbleCache(te, options)
	if err != nil {
		t.Fatal(err)
	}
	pc.Start()
	resources := make([]*rspb.ResourceName, 0)
	for i := 0; i < 1000; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 10000)
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
			InstanceName: "remoteInstanceName",
		}
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		resources = append(resources, r)
	}
	for _, r := range resources {
		err = pc.Delete(ctx, r)
		require.NoError(t, err)
	}

	log.Printf("Wrote and deleted %d resources", len(resources))
	time.Sleep(pebble_cache.JanitorCheckPeriod)
	pc.TestingWaitForGC()
	pc.Stop()

	err = filepath.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && regexp.MustCompile(`^([a-f0-9]{4})$`).MatchString(info.Name()) {
			t.Fatalf("Subdir %q should have been deleted", path)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestMigrateVersions(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)
	rootDir := testfs.MakeTempDir(t)
	maxSizeBytes := int64(1_000_000_000) // 1GB
	options := &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes}

	digests := make([]*repb.Digest, 0)
	{
		// Set the active key version to 0 and write some data at this
		// version.
		flags.Set(t, "cache.pebble.active_key_version", 0)
		pc, err := pebble_cache.NewPebbleCache(te, options)
		if err != nil {
			t.Fatal(err)
		}
		pc.Start()
		for i := 0; i < 1000; i++ {
			remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
			d, buf := testdigest.NewRandomDigestBuf(t, 1000)
			r := &rspb.ResourceName{
				Digest:       d,
				CacheType:    rspb.CacheType_CAS,
				InstanceName: remoteInstanceName,
			}
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
			digests = append(digests, d)
		}
		log.Printf("Wrote %d digests", len(digests))
		time.Sleep(pebble_cache.JanitorCheckPeriod)
		pc.TestingWaitForGC()
		pc.Stop()
	}

	{
		// Now set the active key version to 1 (to trigger a migration)
		// and set the migration QPS low enough to ensure that
		// migrations are happening during our reads.
		flags.Set(t, "cache.pebble.active_key_version", 1)
		flags.Set(t, "cache.pebble.migration_qps_limit", 1000)
		pc2, err := pebble_cache.NewPebbleCache(te, options)
		require.NoError(t, err)

		pc2.Start()
		defer pc2.Stop()
		startTime := time.Now()
		j := 0
		for {
			if time.Since(startTime) > 2*time.Second {
				break
			}

			i := j % len(digests)
			d := digests[i]
			j += 1

			remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
			resourceName := &rspb.ResourceName{
				Digest:       d,
				CacheType:    rspb.CacheType_CAS,
				InstanceName: remoteInstanceName,
			}

			exists, err := pc2.Contains(ctx, resourceName)
			require.NoError(t, err)
			require.True(t, exists)

			rbuf, err := pc2.Get(ctx, resourceName)
			require.NoError(t, err)

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
			if d.GetHash() != d2.GetHash() {
				t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
			}
		}
	}
}

func BenchmarkGetMulti(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	te := testenv.GetTestEnv(b)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(b, te)

	maxSizeBytes := int64(100_000_000)
	rootDir := testfs.MakeTempDir(b)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes})
	if err != nil {
		b.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	digestKeys := make([]*rspb.ResourceName, 0, 100000)
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(b, 1000)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		digestKeys = append(digestKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			b.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
	}

	randomDigests := func(n int) []*rspb.ResourceName {
		r := make([]*rspb.ResourceName, 0, n)
		offset := rand.Intn(len(digestKeys))
		for i := 0; i < n; i++ {
			r = append(r, digestKeys[(i+offset)%len(digestKeys)])
		}
		return r
	}

	b.ReportAllocs()
	b.StopTimer()
	for n := 0; n < b.N; n++ {
		keys := randomDigests(100)

		b.StartTimer()
		m, err := pc.GetMulti(ctx, keys)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if len(m) != len(keys) {
			b.Fatalf("Response was incomplete, asked for %d, got %d", len(keys), len(m))
		}
	}
}

func BenchmarkFindMissing(b *testing.B) {
	te := testenv.GetTestEnv(b)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(b, te)

	maxSizeBytes := int64(100_000_000)
	rootDir := testfs.MakeTempDir(b)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes})
	if err != nil {
		b.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	digestKeys := make([]*rspb.ResourceName, 0, 100000)
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(b, 1000)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		digestKeys = append(digestKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			b.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
	}

	randomDigests := func(n int) []*rspb.ResourceName {
		r := make([]*rspb.ResourceName, 0, n)
		offset := rand.Intn(len(digestKeys))
		for i := 0; i < n; i++ {
			r = append(r, digestKeys[(i+offset)%len(digestKeys)])
		}
		return r
	}

	b.ReportAllocs()
	b.StopTimer()
	for n := 0; n < b.N; n++ {
		keys := randomDigests(100)

		b.StartTimer()
		missing, err := pc.FindMissing(ctx, keys)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if len(missing) != 0 {
			b.Fatalf("Missing: %+v, but all digests should be present", missing)
		}
	}
}

func BenchmarkContains1(b *testing.B) {
	te := testenv.GetTestEnv(b)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(b, te)

	maxSizeBytes := int64(100_000_000)
	rootDir := testfs.MakeTempDir(b)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes})
	if err != nil {
		b.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	digestKeys := make([]*rspb.ResourceName, 0, 100000)
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(b, 1000)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}
		digestKeys = append(digestKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			b.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
	}

	b.ReportAllocs()
	b.StopTimer()
	for n := 0; n < b.N; n++ {
		b.StartTimer()
		found, err := pc.Contains(ctx, digestKeys[rand.Intn(len(digestKeys))])
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if !found {
			b.Fatalf("All digests should be present")
		}
	}
}

func BenchmarkSet(b *testing.B) {
	te := testenv.GetTestEnv(b)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(b, te)

	maxSizeBytes := int64(100_000_000)
	rootDir := testfs.MakeTempDir(b)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{RootDirectory: rootDir, MaxSizeBytes: maxSizeBytes})
	if err != nil {
		b.Fatal(err)
	}
	pc.Start()
	defer pc.Stop()

	b.ReportAllocs()
	b.StopTimer()
	for n := 0; n < b.N; n++ {
		d, buf := testdigest.NewRandomDigestBuf(b, 1000)
		r := &rspb.ResourceName{
			Digest:    d,
			CacheType: rspb.CacheType_CAS,
		}

		b.StartTimer()
		err := pc.Set(ctx, r, buf)
		b.StopTimer()
		if err != nil {
			b.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}
	}
}
