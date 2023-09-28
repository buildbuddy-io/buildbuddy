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
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
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
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	emptyUserMap   = testauth.TestUsers()
	randomSeedOnce sync.Once
	randomGen      *digest.Generator
)

const (
	averageChunkSizeBytes = 64 * 4
)

func getAnonContext(t testing.TB, env environment.Env) context.Context {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env)
	require.NoError(t, err)
	return ctx
}

func newResourceAndBuf(t testing.TB, sizeBytes int64, cacheType rspb.CacheType, instanceName string) (*rspb.ResourceName, []byte) {
	randomSeedOnce.Do(func() {
		// use a fixed seed to avoid flakiness in tests.
		randomGen = digest.RandomGenerator(0)
	})
	d, rs, err := randomGen.RandomDigestReader(sizeBytes)
	require.NoError(t, err)
	buf, err := io.ReadAll(rs)
	require.NoError(t, err)
	return digest.NewResourceName(d, instanceName, cacheType, repb.DigestFunction_SHA256).ToProto(), buf
}

func newCASResourceBuf(t testing.TB, sizeBytes int64) (*rspb.ResourceName, []byte) {
	return newResourceAndBuf(t, sizeBytes, rspb.CacheType_CAS, "" /*instancename*/)
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

func TestIsolation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB

	type params struct {
		desc                   string
		testDataSizeBytes      int64
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
	}

	testParams := []params{
		{
			desc:                   "file is written inline, chunking turned off",
			testDataSizeBytes:      100,
			maxInlineFileSizeBytes: 1,
		},
		{
			desc:                   "file is written on disk, chunking turned off",
			testDataSizeBytes:      1000,
			maxInlineFileSizeBytes: 100,
		},
		{
			desc:                  "file is chunked",
			testDataSizeBytes:     2 * 1024, // Ensure that the file is chunked
			averageChunkSizeBytes: 64 * 4,
		},
		{
			desc:                  "file is not chunked, chunking turned on",
			testDataSizeBytes:     60, // Ensure that the file is not chunked
			averageChunkSizeBytes: 64 * 4,
		},
	}
	type test struct {
		desc           string
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

	for _, tp := range testParams {
		options := &pebble_cache.Options{
			RootDirectory:          testfs.MakeTempDir(t),
			MaxSizeBytes:           maxSizeBytes,
			AverageChunkSizeBytes:  tp.averageChunkSizeBytes,
			MaxInlineFileSizeBytes: tp.maxInlineFileSizeBytes,
		}
		pc, err := pebble_cache.NewPebbleCache(te, options)
		require.NoError(t, err)
		err = pc.Start()
		require.NoError(t, err)
		defer pc.Stop()
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s(%s)", test.desc, tp.desc), func(t *testing.T) {
				r1, buf := newResourceAndBuf(t, 100, test.cacheType1, test.instanceName1)
				// Set() the bytes in cache1.
				err = pc.Set(ctx, r1, buf)
				require.NoError(t, err)

				r2 := proto.Clone(r1).(*rspb.ResourceName)
				r2.InstanceName = test.instanceName2
				r2.CacheType = test.cacheType2

				contains, err := pc.Contains(ctx, r2)
				require.NoError(t, err)
				require.Equal(t, test.shouldBeShared, contains)

				// Get() the bytes from cache2.
				rbuf, err := pc.Get(ctx, r2)
				if test.shouldBeShared {
					// if the caches should be shared but there was an error
					// getting the digest: fail.
					require.NoError(t, err, "Error getting %q from cache: %s", r2.GetDigest().GetHash(), err)

					// Compute a digest for the bytes returned.
					d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
					require.NoError(t, err, "Error computing digest: %s", err)

					require.Equal(t, r1.GetDigest().GetHash(), d2.GetHash())

				} else {
					// if the caches should *not* be shared but there was
					// no error getting the digest: fail.
					require.ErrorContains(t, err, "not found", "Got %q from cache, but should have been isolated.", r2.GetDigest().GetHash())
				}
			})
		}
	}
}

func TestGetSet(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB

	type testCase struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
	}

	testCases := []testCase{
		{
			desc:                   "chunking turned off",
			maxInlineFileSizeBytes: 100,
		},
		{
			desc:                  "chunking turned on",
			averageChunkSizeBytes: 64 * 4,
		},
	}
	testSizes := []int64{
		1, 10, 100, 256, 512, 1000, 1024, 2 * 1024, 10000, 1000000, 10000000,
	}
	for _, tc := range testCases {
		options := &pebble_cache.Options{
			RootDirectory:          testfs.MakeTempDir(t),
			MaxSizeBytes:           maxSizeBytes,
			AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
			MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
		}
		pc, err := pebble_cache.NewPebbleCache(te, options)
		require.NoError(t, err)
		err = pc.Start()
		require.NoError(t, err)
		defer pc.Stop()
		for _, testSize := range testSizes {
			desc := fmt.Sprintf("test size: %d (%s)", testSize, tc.desc)
			t.Run(desc, func(t *testing.T) {
				r, buf := newCASResourceBuf(t, testSize)
				// Set() the bytes in the cache.
				err := pc.Set(ctx, r, buf)
				require.NoError(t, err, "Error setting %q in cache: %s", r.GetDigest().GetHash(), err)

				// Get() the bytes from the cache.
				rbuf, err := pc.Get(ctx, r)
				require.NoError(t, err, "Error getting %q from cache: %s", r.GetDigest().GetHash(), err)

				// Compute a digest for the bytes returned.
				d2, err := digest.Compute(bytes.NewReader(rbuf), r.GetDigestFunction())
				require.NoError(t, err)
				require.Equal(t, r.GetDigest().GetHash(), d2.GetHash())
			})
		}
	}
}

func TestDupeWrites(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB

	var testParams = []struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
	}{
		{
			desc:                   "chunking off",
			maxInlineFileSizeBytes: 100,
		},
		{
			desc:                  "chunking on",
			averageChunkSizeBytes: 64 * 4,
		},
	}

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

	for _, tp := range testParams {
		options := &pebble_cache.Options{
			RootDirectory:          testfs.MakeTempDir(t),
			MaxSizeBytes:           maxSizeBytes,
			AverageChunkSizeBytes:  tp.averageChunkSizeBytes,
			MaxInlineFileSizeBytes: tp.maxInlineFileSizeBytes,
		}
		pc, err := pebble_cache.NewPebbleCache(te, options)
		require.NoError(t, err)
		err = pc.Start()
		require.NoError(t, err)
		for _, test := range tests {
			desc := fmt.Sprintf("%s(%s)", test.name, tp.desc)
			t.Run(desc, func(t *testing.T) {
				r, buf := newResourceAndBuf(t, test.size, test.cacheType, "" /*instanceName*/)

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
				require.NoError(t, err)
				require.Equal(t, r.GetDigest().GetHash(), d2.GetHash())
			})
		}
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
		r, buf := newResourceAndBuf(t, 1000, rspb.CacheType_CAS, instanceName)
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := r.GetDigest().GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, partitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)

		// AC records should have group ID and remote instance hash in their file path
		r, buf = newResourceAndBuf(t, 1000, rspb.CacheType_AC, instanceName)
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err = pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		instanceNameHash := strconv.Itoa(int(crc32.ChecksumIEEE([]byte(instanceName))))
		hash = r.GetDigest().GetHash()
		expectedFilename = fmt.Sprintf("%s/blobs/PT%s/%s/ac/%s/%v/%v", rootDir, partitionID, testGroup, instanceNameHash, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}

	// Anon user should use the default partition.
	{
		ctx := getAnonContext(t, te)
		r, buf := newResourceAndBuf(t, 1000, rspb.CacheType_CAS, instanceName)
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := r.GetDigest().GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, pebble_cache.DefaultPartitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}
}

func TestCopyPartitionData(t *testing.T) {
	chunkingOn := []bool{true, false}
	for _, tc := range chunkingOn {
		t.Run(fmt.Sprintf("chunkingOn=%t", tc), func(t *testing.T) {
			testCopyPartitionData(t, tc)
		})
	}
}

func testCopyPartitionData(t *testing.T, chunkingOn bool) {
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

	if chunkingOn {
		opts.AverageChunkSizeBytes = 64 * 4
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
				size = 2 * 1024
			}
			r, buf := newResourceAndBuf(t, size, rspb.CacheType_CAS, instanceName)
			defaultResources = append(defaultResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			r, buf := newResourceAndBuf(t, 100, rspb.CacheType_AC, instanceName)
			defaultResources = append(defaultResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
	}

	// Write some data to the custom partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		for i := 0; i < 10; i++ {
			size := int64(10)
			// Mix of inline and extern data.
			if i > 5 {
				size = 2 * 1024
			}
			r, buf := newResourceAndBuf(t, size, rspb.CacheType_CAS, instanceName)
			customResources = append(customResources, r)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			r, buf := newResourceAndBuf(t, 1000, rspb.CacheType_AC, instanceName)
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
	_, err = pebble_cache.NewPebbleCache(te, opts)
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
		r, buf := newResourceAndBuf(t, 1000, rspb.CacheType_CAS, instanceName)
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := r.GetDigest().GetHash()
		expectedFilename := fmt.Sprintf("%s/blobs/PT%s/cas/%v/%v", rootDir, partitionID, hash[:4], hash)
		_, err = os.Stat(expectedFilename)
		require.NoError(t, err)
	}

	// Authenticated user should use default partition.
	{
		ctx := te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), testAPIKey)
		// CAS records should not have group ID or remote instance name in their file path
		r, buf := newResourceAndBuf(t, 1000, rspb.CacheType_CAS, instanceName)
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		rbuf, err := pc.Get(ctx, r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(buf, rbuf))
		hash := r.GetDigest().GetHash()
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

	averageChunkSizeBytesParam := []int{0, 64 * 4}

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
	for _, averageChunkSizeBytes := range averageChunkSizeBytesParam {
		options := &pebble_cache.Options{
			RootDirectory:               testfs.MakeTempDir(t),
			MaxSizeBytes:                maxSizeBytes,
			MaxInlineFileSizeBytes:      100,
			MinBytesAutoZstdCompression: math.MaxInt64, // Turn off automatic compression
			AverageChunkSizeBytes:       averageChunkSizeBytes,
		}
		pc, err := pebble_cache.NewPebbleCache(te, options)
		require.NoError(t, err)
		err = pc.Start()
		require.NoError(t, err)
		defer pc.Stop()
		for _, tc := range testCases {
			for _, testSize := range testSizes {
				desc := fmt.Sprintf("testSize: %d %s (averageChunkSizeBytes=%d)", testSize, tc.name, averageChunkSizeBytes)
				t.Run(desc, func(t *testing.T) {
					r, buf := newCASResourceBuf(t, testSize)
					r.CacheType = tc.cacheType
					r.Compressor = tc.compressor

					dataToWrite := buf
					if tc.compressor == repb.Compressor_ZSTD {
						dataToWrite = compression.CompressZstd(nil, buf)
					}

					// Set data in the cache.
					err := pc.Set(ctx, r, dataToWrite)
					require.NoError(t, err, tc.name)

					// Metadata should return correct size, regardless of queried size.
					rWrongSize := proto.Clone(r).(*rspb.ResourceName)
					rWrongSize.Digest.SizeBytes = 1

					md, err := pc.Metadata(ctx, rWrongSize)
					require.NoError(t, err, tc.name)
					chunkingOn := averageChunkSizeBytes > 0 && len(dataToWrite) > averageChunkSizeBytes
					if chunkingOn {
						require.Equal(t, int64(0), md.StoredSizeBytes, tc.name)
					} else {
						require.Equal(t, int64(len(dataToWrite)), md.StoredSizeBytes, tc.name)
					}
					require.Equal(t, testSize, md.DigestSizeBytes, tc.name)
					lastAccessTime1 := md.LastAccessTimeUsec
					lastModifyTime1 := md.LastModifyTimeUsec
					require.NotZero(t, lastAccessTime1)
					require.NotZero(t, lastModifyTime1)

					// Last access time should not update since last call to Metadata()
					md, err = pc.Metadata(ctx, rWrongSize)
					require.NoError(t, err, tc.name)
					if chunkingOn {
						require.Equal(t, int64(0), md.StoredSizeBytes, tc.name)
					} else {
						require.Equal(t, int64(len(dataToWrite)), md.StoredSizeBytes, tc.name)
					}

					require.Equal(t, testSize, md.DigestSizeBytes, tc.name)
					lastAccessTime2 := md.LastAccessTimeUsec
					lastModifyTime2 := md.LastModifyTimeUsec
					require.Equal(t, lastAccessTime1, lastAccessTime2)
					require.Equal(t, lastModifyTime1, lastModifyTime2)

					// After updating data, last access and modify time should update
					err = pc.Set(ctx, r, dataToWrite)
					require.NoError(t, err)
					md, err = pc.Metadata(ctx, rWrongSize)
					require.NoError(t, err, tc.name)
					if chunkingOn {
						require.Equal(t, int64(0), md.StoredSizeBytes, tc.name)
					} else {
						require.Equal(t, int64(len(dataToWrite)), md.StoredSizeBytes, tc.name)
					}

					require.Equal(t, testSize, md.DigestSizeBytes, tc.name)
					lastAccessTime3 := md.LastAccessTimeUsec
					lastModifyTime3 := md.LastModifyTimeUsec
					require.Greater(t, lastAccessTime3, lastAccessTime1)
					require.Greater(t, lastModifyTime3, lastModifyTime2)
				})
			}
		}
	}
}

func randomDigests(t *testing.T, sizes ...int64) map[*rspb.ResourceName][]byte {
	m := make(map[*rspb.ResourceName][]byte)
	for _, size := range sizes {
		rn, buf := newCASResourceBuf(t, size)
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

	var testCases = []struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
	}{
		{
			desc:                   "chunking off",
			maxInlineFileSizeBytes: 100,
		},
		{
			desc:                  "chunking on",
			averageChunkSizeBytes: 64 * 4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			options := &pebble_cache.Options{
				RootDirectory:          testfs.MakeTempDir(t),
				MaxSizeBytes:           maxSizeBytes,
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, options)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			digests := randomDigests(t, 10, 20, 11, 30, 1024, 40)
			err = pc.SetMulti(ctx, digests)
			require.NoError(t, err)
			resourceNames := make([]*rspb.ResourceName, 0, len(digests))
			for d := range digests {
				resourceNames = append(resourceNames, d)
			}
			m, err := pc.GetMulti(ctx, resourceNames)
			require.NoError(t, err)
			for rn := range digests {
				d := rn.GetDigest()
				rbuf, ok := m[d]
				require.True(t, ok, "Multi-get failed to return expected digest: %q", d.GetHash())
				d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
				require.NoError(t, err)
				require.Equal(t, d.GetHash(), d2.GetHash())
			}
		})
	}
}

func TestReadWrite(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1_000_000_000) // 1GB

	type testCase struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
	}
	testCases := []testCase{
		{
			desc:                   "chunking turned off",
			maxInlineFileSizeBytes: 100,
		},
		{
			desc:                  "chunking turned on",
			averageChunkSizeBytes: 64 * 4,
		},
	}
	testSizes := []int64{
		1, 10, 100, 256, 512, 1000, 1024, 2 * 1024, 10000, 1000000, 10000000,
	}
	for _, tc := range testCases {
		options := &pebble_cache.Options{
			RootDirectory:          testfs.MakeTempDir(t),
			MaxSizeBytes:           maxSizeBytes,
			AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
			MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
		}
		pc, err := pebble_cache.NewPebbleCache(te, options)
		require.NoError(t, err)
		err = pc.Start()
		require.NoError(t, err)
		defer pc.Stop()

		for _, testSize := range testSizes {
			desc := fmt.Sprintf("test size: %d (%s)", testSize, tc.desc)
			t.Run(desc, func(t *testing.T) {
				rn, buf := newCASResourceBuf(t, testSize)
				// Use Writer() to set the bytes in the cache.
				wc, err := pc.Writer(ctx, rn)
				require.NoError(t, err, "Error getting %q writer", rn.GetDigest().GetHash())
				_, err = wc.Write(buf)
				require.NoError(t, err)
				err = wc.Commit()
				require.NoError(t, err)
				err = wc.Close()
				require.NoError(t, err)

				// Use Reader() to get the bytes from the cache.
				reader, err := pc.Reader(ctx, rn, 0, 0)
				require.NoError(t, err, "Error getting %q reader", rn.GetDigest().GetHash())
				d2 := testdigest.ReadDigestAndClose(t, reader)
				require.Equal(t, rn.GetDigest().GetHash(), d2.GetHash())
			})
		}
	}
}

func TestWriteCancelBeforeCommit(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))

	maxSizeBytes := int64(1_000_000_000) // 1GB

	type testCase struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
		testSize               int64
	}
	testCases := []testCase{
		{
			desc:                  "chunking turned on",
			averageChunkSizeBytes: 64 * 4,
			testSize:              256,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			options := &pebble_cache.Options{
				RootDirectory:          testfs.MakeTempDir(t),
				MaxSizeBytes:           maxSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, options)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)

			ctx := getAnonContext(t, te)
			ctx, cancel := context.WithCancel(ctx)
			rn, buf := newCASResourceBuf(t, tc.testSize)
			// Use Writer() to set the bytes in the cache.
			wc, err := pc.Writer(ctx, rn)
			require.NoError(t, err, "Error getting %q writer", rn.GetDigest().GetHash())
			_, err = wc.Write(buf)
			require.NoError(t, err)
			cancel()
			err = wc.Commit()
			require.ErrorContains(t, err, "context canceled")
			err = wc.Close()
			require.NoError(t, err)

			err = pc.Stop()
			require.NoError(t, err)
		})
	}
}

func TestCancelBeforeWrite(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))

	maxSizeBytes := int64(1_000_000_000) // 1GB

	type testCase struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
		testSize               int64
	}
	testCases := []testCase{
		{
			desc:                  "chunking turned on",
			averageChunkSizeBytes: 64 * 4,
			testSize:              256,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			options := &pebble_cache.Options{
				RootDirectory:          testfs.MakeTempDir(t),
				MaxSizeBytes:           maxSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, options)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)

			ctx := getAnonContext(t, te)
			ctx, cancel := context.WithCancel(ctx)
			rn, buf := newCASResourceBuf(t, tc.testSize)
			// Use Writer() to set the bytes in the cache.
			wc, err := pc.Writer(ctx, rn)
			require.NoError(t, err, "Error getting %q writer", rn.GetDigest().GetHash())
			cancel()
			// Wait for cancel to take effect.
			time.Sleep(100 * time.Millisecond)
			_, err = wc.Write(buf)
			require.ErrorContains(t, err, "context canceled")
			err = wc.Commit()
			require.ErrorContains(t, err, "context canceled")
			err = wc.Close()
			require.NoError(t, err)

			err = pc.Stop()
			require.NoError(t, err)
		})
	}
}

func TestSizeLimit(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(100_000_000)

	testCases := []struct {
		desc                   string
		maxInlineFileSizeBytes int64
		averageChunkSizeBytes  int
	}{
		{
			desc:                   "inline_chunking_off",
			maxInlineFileSizeBytes: 100,
		},
		{
			desc:                   "disk_chunking_off",
			maxInlineFileSizeBytes: 2000,
		},
		{
			desc:                   "chunking_on",
			maxInlineFileSizeBytes: 64 * 4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rootDir := testfs.MakeTempDir(t)
			options := &pebble_cache.Options{
				RootDirectory:          rootDir,
				MaxSizeBytes:           maxSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, options)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			resourceKeys := make([]*rspb.ResourceName, 0, 150000)
			for i := 0; i < 150; i++ {
				r, buf := newCASResourceBuf(t, 1000)
				resourceKeys = append(resourceKeys, r)
				if err := pc.Set(ctx, r, buf); err != nil {
					t.Fatalf("Error setting %q in cache: %s", r.GetDigest().GetHash(), err.Error())
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
		})
	}
}

func writeResource(t *testing.T, ctx context.Context, pc *pebble_cache.PebbleCache, r *rspb.ResourceName, data []byte) {
	// Write data to cache
	wc, err := pc.Writer(ctx, r)
	require.NoError(t, err)
	n, err := wc.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	err = wc.Commit()
	require.NoError(t, err)
	err = wc.Close()
	require.NoError(t, err)
}

func readResource(t *testing.T, ctx context.Context, pc *pebble_cache.PebbleCache, r *rspb.ResourceName, offset, limit int64) []byte {
	reader, err := pc.Reader(ctx, r, offset, limit)
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	return data
}

func TestCompression(t *testing.T) {
	maxInlineFileSizeBytes := int64(1024)
	averageChunkSizeBytes := 64 * 4
	maxSizeBytes := int64(1_000_000_000) // 1GB

	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	testParams := []struct {
		desc                  string
		blobSize              int
		averageChunkSizeBytes int
	}{
		{
			desc:     "disk_multiple_compression_chunk",
			blobSize: pebble_cache.CompressorBufSizeBytes + 1,
		},
		{
			desc:     "inline",
			blobSize: int(maxInlineFileSizeBytes) - 100,
		},
		{
			desc:                  "chunking_on_multiple_cdc_chunks",
			blobSize:              pebble_cache.CompressorBufSizeBytes + 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
		},
		{
			desc:                  "chunking_on_multiple_cdc_chunks_single_compression_chunk",
			blobSize:              pebble_cache.CompressorBufSizeBytes - 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
		},
		{
			desc:                  "chunking_on_single_chunk",
			blobSize:              averageChunkSizeBytes/4 - 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
		},
	}

	testCases := []struct {
		name              string
		isWriteCompressed bool
		isReadCompressed  bool
	}{
		{
			name:              "write_compressed_read_compressed",
			isWriteCompressed: true,
			isReadCompressed:  true,
		},
		{
			name:              "write_compressed_read_decompressed",
			isWriteCompressed: true,
			isReadCompressed:  false,
		},
		{
			name:              "write_uncompressed_read_compressed",
			isWriteCompressed: false,
			isReadCompressed:  true,
		},
		{
			name:              "write_uncompressed_read_decompressed",
			isWriteCompressed: false,
			isReadCompressed:  false,
		},
	}

	for _, tp := range testParams {
		blob := compressibleBlobOfSize(tp.blobSize)
		compressedBuf := compression.CompressZstd(nil, blob)
		// Note: Digest is of uncompressed contents
		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)
		decompressedRN := digest.NewResourceName(d, "" /*instanceName*/, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()
		compressedRN := proto.Clone(decompressedRN).(*rspb.ResourceName)
		compressedRN.Compressor = repb.Compressor_ZSTD

		for _, tc := range testCases {
			desc := fmt.Sprintf("%s_%s", tp.desc, tc.name)
			t.Run(desc, func(t *testing.T) {
				opts := &pebble_cache.Options{
					RootDirectory:          testfs.MakeTempDir(t),
					MaxSizeBytes:           maxSizeBytes,
					MaxInlineFileSizeBytes: maxInlineFileSizeBytes,
					AverageChunkSizeBytes:  tp.averageChunkSizeBytes,
				}
				pc, err := pebble_cache.NewPebbleCache(te, opts)
				require.NoError(t, err)
				err = pc.Start()
				require.NoError(t, err)
				defer pc.Stop()
				var dataToWrite []byte
				var rnToRead, rnToWrite *rspb.ResourceName
				if tc.isWriteCompressed {
					dataToWrite = compressedBuf
					rnToWrite = compressedRN
				} else {
					dataToWrite = blob
					rnToWrite = decompressedRN
				}
				if tc.isReadCompressed {
					rnToRead = compressedRN
				} else {
					rnToRead = decompressedRN
				}

				writeResource(t, ctx, pc, rnToWrite, dataToWrite)

				// Read data
				data := readResource(t, ctx, pc, rnToRead, 0, 0)
				if tc.isReadCompressed {
					data, err = compression.DecompressZstd(nil, data)
					require.NoError(t, err)
				}
				require.Equal(t, blob, data)
			})
		}
	}
}

func TestCompression_BufferPoolReuse(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	maxSizeBytes := int64(1000)

	testCases := []struct {
		desc                   string
		maxInlineFileSizeBytes int
		averageChunkSizeBytes  int
		blobSize               int
	}{
		{
			desc:                  "chunking on single chunk",
			averageChunkSizeBytes: 64 * 4,
			blobSize:              60,
		},
		{
			desc:                  "chunking on multiple chunks",
			averageChunkSizeBytes: 64 * 4,
			blobSize:              2 * 1024,
		},
		{
			desc:                   "chunking off inline",
			maxInlineFileSizeBytes: 1024,
			blobSize:               100,
		},
		{
			desc:                   "chunking off inline",
			maxInlineFileSizeBytes: 1,
			blobSize:               100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			opts := &pebble_cache.Options{
				RootDirectory:          testfs.MakeTempDir(t),
				MaxSizeBytes:           maxSizeBytes,
				MaxInlineFileSizeBytes: int64(tc.maxInlineFileSizeBytes),
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			// Do multiple reads to reuse buffers in bufferpool
			for i := 0; i < 5; i++ {
				blob := compressibleBlobOfSize(tc.blobSize)

				// Note: Digest is of uncompressed contents
				d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
				require.NoError(t, err, "i=%d", i)
				decompressedRN := digest.NewResourceName(d, "" /*instanceName*/, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()

				// Write non-compressed data to cache
				writeResource(t, ctx, pc, decompressedRN, blob)

				compressedRN := proto.Clone(decompressedRN).(*rspb.ResourceName)
				compressedRN.Compressor = repb.Compressor_ZSTD

				data := readResource(t, ctx, pc, compressedRN, 0, 0)
				decompressed, err := compression.DecompressZstd(nil, data)
				require.NoError(t, err, "i=%d", i)
				require.Equal(t, blob, decompressed, "i=%d", i)
			}
		})
	}
}

func TestCompression_ParallelRequests(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	maxSizeBytes := int64(1000)

	testCases := []struct {
		desc                   string
		maxInlineFileSizeBytes int
		averageChunkSizeBytes  int
		blobSize               int
	}{
		{
			desc:                  "chunking on single chunk",
			averageChunkSizeBytes: 64 * 4,
			blobSize:              60,
		},
		{
			desc:                  "chunking on multiple chunks",
			averageChunkSizeBytes: 64 * 4,
			blobSize:              2 * 1024,
		},
		{
			desc:                   "chunking off inline",
			maxInlineFileSizeBytes: 1024,
			blobSize:               100,
		},
		{
			desc:                   "chunking off inline",
			maxInlineFileSizeBytes: 1,
			blobSize:               100,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := getAnonContext(t, te)
			opts := &pebble_cache.Options{
				RootDirectory:          testfs.MakeTempDir(t),
				MaxSizeBytes:           maxSizeBytes,
				MaxInlineFileSizeBytes: int64(tc.maxInlineFileSizeBytes),
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			eg := errgroup.Group{}
			for i := 0; i < 10; i++ {
				eg.Go(func() error {
					blob := compressibleBlobOfSize(tc.blobSize)

					// Note: Digest is of uncompressed contents
					d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
					require.NoError(t, err)
					decompressedRN := digest.NewResourceName(d, "" /*instanceName*/, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()

					// Write non-compressed data to cache
					writeResource(t, ctx, pc, decompressedRN, blob)

					// Read data in compressed form
					compressedRN := proto.Clone(decompressedRN).(*rspb.ResourceName)
					compressedRN.Compressor = repb.Compressor_ZSTD

					data := readResource(t, ctx, pc, compressedRN, 0, 0)
					decompressed, err := compression.DecompressZstd(nil, data)
					require.NoError(t, err)
					require.Equal(t, blob, decompressed)
					return nil
				})
			}
			eg.Wait()
		})
	}
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

	testCases := []struct {
		desc                  string
		averageChunkSizeBytes int
	}{
		{
			desc:                  "cdc chunking on",
			averageChunkSizeBytes: 64 * 4,
		},
		{
			desc:                  "cdc chunking off",
			averageChunkSizeBytes: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			opts := &pebble_cache.Options{
				RootDirectory:  testfs.MakeTempDir(t),
				MaxSizeBytes:   maxSizeBytes,
				MinEvictionAge: &minEvictionAge,
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			// Write decompressed bytes to the cache. Because blob compression is enabled, pebble should compress before writing
			for d, blob := range digestBlobs {
				rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()
				err = pc.Set(ctx, rn, blob)
				require.NoError(t, err)
			}
			time.Sleep(pebble_cache.JanitorCheckPeriod)
			pc.TestingWaitForGC()

			// All reads should succeed. Nothing should've been evicted
			for d, blob := range digestBlobs {
				rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()
				data, err := pc.Get(ctx, rn)
				require.NoError(t, err)
				require.True(t, bytes.Equal(blob, data))
			}
		})
	}
}

func TestCompressionOffset(t *testing.T) {
	maxInlineFileSizeBytes := 1024
	maxSizeBytes := int64(1_000_000_000) // 1GB
	averageChunkSizeBytes := 64 * 4

	testCases := []struct {
		desc                  string
		blobSize              int
		averageChunkSizeBytes int
		readOffset            int64
		readLimit             int64
	}{
		{
			desc:       "disk_multiple_compression_chunk",
			blobSize:   pebble_cache.CompressorBufSizeBytes + 1,
			readOffset: 2 * 1024,
			readLimit:  10,
		},
		{
			desc:       "inline",
			blobSize:   maxInlineFileSizeBytes - 100,
			readOffset: 512,
			readLimit:  10,
		},
		{
			desc:                  "chunking_on_multiple_cdc_chunks",
			blobSize:              pebble_cache.CompressorBufSizeBytes + 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
			readOffset:            2 * 1024,
			readLimit:             10,
		},
		{
			desc:                  "chunking_on_multiple_cdc_chunks_single_compression_chunk",
			blobSize:              pebble_cache.CompressorBufSizeBytes - 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
			readOffset:            2 * 1024,
			readLimit:             10,
		},
		{
			desc:                  "chunking_on_single_chunk",
			blobSize:              averageChunkSizeBytes/4 - 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
			readOffset:            20,
			readLimit:             10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Make blob big enough to require multiple chunks to compress
			blob := compressibleBlobOfSize(tc.blobSize)
			compressedBuf := compression.CompressZstd(nil, blob)

			// Note: Digest is of uncompressed contents
			d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
			require.NoError(t, err)

			decompressedRN := digest.NewResourceName(d, "" /*instanceName*/, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()
			compressedRN := proto.Clone(decompressedRN).(*rspb.ResourceName)
			compressedRN.Compressor = repb.Compressor_ZSTD

			te := testenv.GetTestEnv(t)
			te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
			ctx := getAnonContext(t, te)

			opts := &pebble_cache.Options{
				RootDirectory:          testfs.MakeTempDir(t),
				MaxSizeBytes:           maxSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
				MaxInlineFileSizeBytes: int64(maxInlineFileSizeBytes),
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			// Write data to cache
			writeResource(t, ctx, pc, compressedRN, compressedBuf)

			// Read data
			data := readResource(t, ctx, pc, decompressedRN, tc.readOffset, tc.readLimit)
			require.NoError(t, err)
			require.Equal(t, blob[tc.readOffset:tc.readOffset+tc.readLimit], data)
		})
	}
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
	maxSizeBytes := int64(1_000_000_000) // 1GB
	tests := []struct {
		desc                   string
		averageChunkSizeBytes  int
		maxInlineFileSizeBytes int64
	}{
		{
			desc:                  "chunking_on",
			averageChunkSizeBytes: 64 * 4,
		},
		{
			desc:                   "chunking_off",
			maxInlineFileSizeBytes: 100,
		},
	}
	testSizes := []int64{50, 100, 1000, 1500, 10000}
	for _, tc := range tests {
		for _, testSize := range testSizes {
			desc := fmt.Sprintf("%s_test_size_%d", tc.desc, testSize)
			t.Run(desc, func(t *testing.T) {
				te := testenv.GetTestEnv(t)
				te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
				ctx := getAnonContext(t, te)

				options := &pebble_cache.Options{
					RootDirectory:          testfs.MakeTempDir(t),
					MaxSizeBytes:           maxSizeBytes,
					AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
					MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
				}
				pc, err := pebble_cache.NewPebbleCache(te, options)
				require.NoError(t, err)
				err = pc.Start()
				require.NoError(t, err)
				defer pc.Stop()

				r, buf := newCASResourceBuf(t, testSize)
				notSetR1, _ := newCASResourceBuf(t, testSize)
				notSetR2, _ := newCASResourceBuf(t, testSize)

				err = pc.Set(ctx, r, buf)
				require.NoError(t, err)

				rns := []*rspb.ResourceName{r, notSetR1, notSetR2}
				missing, err := pc.FindMissing(ctx, rns)
				require.NoError(t, err)
				require.ElementsMatch(t, []*repb.Digest{notSetR1.GetDigest(), notSetR2.GetDigest()}, missing)

				rns = []*rspb.ResourceName{r}
				missing, err = pc.FindMissing(ctx, rns)
				require.NoError(t, err)
				require.Empty(t, missing)
			})
		}
	}
}

func TestNoEarlyEviction(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)

	testCases := []struct {
		desc                   string
		averageChunkSizeBytes  int
		maxInlineFileSizeBytes int64
		digestSize             int64
	}{
		{
			desc:                   "chunking_off_inline_file",
			maxInlineFileSizeBytes: 200,
			digestSize:             100,
		},
		{
			desc:                   "chunking_off_disk",
			maxInlineFileSizeBytes: 1,
			digestSize:             100,
		},
		{
			desc:                  "chunking_on_single_chunk",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            63,
		},
		{
			desc:                  "chunking_on_multiple_chunk",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            2 * 1064,
		},
	}

	numDigests := 10
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			maxSizeBytes := int64(
				math.Ceil( // account for integer rounding
					float64(numDigests) *
						float64(tc.digestSize) *
						(1 / pebble_cache.JanitorCutoffThreshold))) // account for .9 evictor cutoff

			rootDir := testfs.MakeTempDir(t)
			atimeUpdateThreshold := time.Duration(0) // update atime on every access
			atimeBufferSize := 0                     // blocking channel of atime updates
			minEvictionAge := time.Duration(0)       // no min eviction age
			pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
				RootDirectory:          rootDir,
				MaxSizeBytes:           maxSizeBytes,
				AtimeUpdateThreshold:   &atimeUpdateThreshold,
				AtimeBufferSize:        &atimeBufferSize,
				MinEvictionAge:         &minEvictionAge,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
			})
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			defer pc.Stop()

			// Should be able to add 10 things without anything getting evicted
			resourceKeys := make([]*rspb.ResourceName, numDigests)
			for i := 0; i < numDigests; i++ {
				r, buf := newCASResourceBuf(t, tc.digestSize)
				resourceKeys[i] = r

				err := pc.Set(ctx, r, buf)
				require.NoError(t, err)
			}

			time.Sleep(pebble_cache.JanitorCheckPeriod)
			pc.TestingWaitForGC()

			// Verify that nothing was evicted
			for _, r := range resourceKeys {
				_, err = pc.Get(ctx, r)
				require.NoError(t, err)
			}
		})
	}
}

func testLRU(t *testing.T, testACEviction bool) {
}

func TestLRU(t *testing.T) {
	testCases := []struct {
		desc                   string
		averageChunkSizeBytes  int
		maxInlineFileSizeBytes int64
		digestSize             int64
	}{
		{
			desc:                  "chunking_on_multiple_chunks",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            2 * 1024,
		},
		{
			desc:                  "chunking_on_single_chunk",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            2 * 1024,
		},
		{
			desc:                   "chunking_off_inline",
			maxInlineFileSizeBytes: 1024,
			digestSize:             100,
		},
		{
			desc:                   "chunking_off_disk",
			maxInlineFileSizeBytes: 1,
			digestSize:             100,
		},
	}
	withACEviction := []bool{true, false}

	for _, testACEviction := range withACEviction {
		for _, tc := range testCases {
			desc := fmt.Sprintf("%s_ac_eviction_%t", tc.desc, testACEviction)
			t.Run(desc, func(t *testing.T) {
				if testACEviction {
					flags.Set(t, "cache.pebble.active_key_version", 3)
					flags.Set(t, "cache.pebble.ac_eviction_enabled", true)
				}
				te := testenv.GetTestEnv(t)
				te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
				ctx := getAnonContext(t, te)

				numDigests := 100
				maxSizeBytes := int64(math.Ceil( // account for integer rounding
					float64(numDigests) * float64(tc.digestSize) * (1 / pebble_cache.JanitorCutoffThreshold))) // account for .9 evictor cutoff
				rootDir := testfs.MakeTempDir(t)
				atimeUpdateThreshold := time.Duration(0) // update atime on every access
				atimeBufferSize := 0                     // blocking channel of atime updates
				minEvictionAge := time.Duration(0)       // no min eviction age
				opts := &pebble_cache.Options{
					RootDirectory:               rootDir,
					MaxSizeBytes:                maxSizeBytes,
					AtimeUpdateThreshold:        &atimeUpdateThreshold,
					AtimeBufferSize:             &atimeBufferSize,
					MinEvictionAge:              &minEvictionAge,
					MinBytesAutoZstdCompression: maxSizeBytes,
					MaxInlineFileSizeBytes:      tc.maxInlineFileSizeBytes,
					AverageChunkSizeBytes:       tc.averageChunkSizeBytes,
				}
				pc, err := pebble_cache.NewPebbleCache(te, opts)
				require.NoError(t, err)
				err = pc.Start()
				require.NoError(t, err)

				quartile := numDigests / 4

				resourceKeys := make([]*rspb.ResourceName, numDigests)
				for i := range resourceKeys {
					cacheType := rspb.CacheType_CAS
					if testACEviction && i%2 == 0 {
						cacheType = rspb.CacheType_AC
					}
					r, buf := newResourceAndBuf(t, tc.digestSize, cacheType, "")
					resourceKeys[i] = r
					err := pc.Set(ctx, r, buf)
					require.NoError(t, err)
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
					cacheType := rspb.CacheType_CAS
					if testACEviction && i%2 == 0 {
						cacheType = rspb.CacheType_AC
					}
					r, buf := newResourceAndBuf(t, tc.digestSize, cacheType, "")
					resourceKeys = append(resourceKeys, r)
					err := pc.Set(ctx, r, buf)
					require.NoError(t, err)
				}

				pc.Stop()
				pc, err = pebble_cache.NewPebbleCache(te, opts)
				require.NoError(t, err)
				err = pc.Start()
				require.NoError(t, err)

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
					log.Infof("Evicted %d keys in quartile: %d (%s)", count, quartile, sample)
				}

				// None of the files "used" just before adding more should have been
				// evicted.
				require.Equal(t, 0, len(evictionsByQuartile[0]))

				// None of the most recently added files should have been evicted.
				require.Equal(t, 0, len(evictionsByQuartile[4]))

				// Relax the conditions a little to de-flake the tests.
				require.LessOrEqual(t, len(evictionsByQuartile[1]), len(evictionsByQuartile[2])+1)
				require.LessOrEqual(t, len(evictionsByQuartile[2]), len(evictionsByQuartile[3])+1)
				require.Greater(t, len(evictionsByQuartile[3]), 0)
			})
		}
	}
}

func TestStartupScan(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(emptyUserMap))
	ctx := getAnonContext(t, te)
	maxSizeBytes := int64(1_000_000_000) // 1GB

	testCases := []struct {
		desc                   string
		averageChunkSizeBytes  int
		maxInlineFileSizeBytes int64
		digestSize             int64
	}{
		{
			desc:                  "chunking_on_single_chunk",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            63,
		},
		{
			desc:                  "chunking_on_multiple_chunks",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            2 * 1024,
		},
		{
			desc:                   "chunking_off_inline",
			maxInlineFileSizeBytes: 1000,
			digestSize:             100,
		},
		{
			desc:                   "chunking_off_disk",
			maxInlineFileSizeBytes: 1000,
			digestSize:             2000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rootDir := testfs.MakeTempDir(t)
			options := &pebble_cache.Options{
				RootDirectory:          rootDir,
				MaxSizeBytes:           maxSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, options)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)
			resources := make([]*rspb.ResourceName, 0)
			for i := 0; i < 1000; i++ {
				remoteInstanceName := fmt.Sprintf("remote-instance-%d", i)
				r, buf := newResourceAndBuf(t, tc.digestSize, rspb.CacheType_AC, remoteInstanceName)
				err = pc.Set(ctx, r, buf)
				require.NoError(t, err)
				resources = append(resources, r)
			}
			log.Printf("Wrote %d digests", len(resources))

			time.Sleep(pebble_cache.JanitorCheckPeriod)
			pc.TestingWaitForGC()
			pc.Stop()

			pc2, err := pebble_cache.NewPebbleCache(te, options)
			require.NoError(t, err)

			err = pc2.Start()
			require.NoError(t, err)
			defer pc2.Stop()
			for _, r := range resources {
				rbuf, err := pc2.Get(ctx, r)
				require.NoError(t, err)

				// Compute a digest for the bytes returned.
				d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
				require.NoError(t, err)
				require.Equal(t, r.GetDigest().GetHash(), d2.GetHash())
			}
		})
	}
}

type digestAndType struct {
	cacheType rspb.CacheType
	digest    *repb.Digest
}

func TestDeleteOrphans(t *testing.T) {
	flags.Set(t, "cache.pebble.scan_for_orphaned_files", true)
	flags.Set(t, "cache.pebble.scan_for_missing_files", true)
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
		r, buf := newResourceAndBuf(t, 10000, rspb.CacheType_CAS, "remoteInstanceName")
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		digests[r.GetDigest().GetHash()] = &digestAndType{rspb.CacheType_CAS, r.GetDigest()}
	}
	for i := 0; i < 1000; i++ {
		r, buf := newResourceAndBuf(t, 10000, rspb.CacheType_AC, "remoteInstanceName")
		err = pc.Set(ctx, r, buf)
		require.NoError(t, err)
		digests[r.GetDigest().GetHash()] = &digestAndType{rspb.CacheType_AC, r.GetDigest()}
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
		rn := digest.NewResourceName(dt.digest, "remoteInstanceName", dt.cacheType, repb.DigestFunction_SHA256).ToProto()
		_, err := pc2.Get(ctx, rn)
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
		rn := digest.NewResourceName(dt.digest, "remoteInstanceName", dt.cacheType, repb.DigestFunction_SHA256).ToProto()
		_, err = pc2.Get(ctx, rn)
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
		r, buf := newResourceAndBuf(t, 10000, rspb.CacheType_CAS, "remoteInstanceName")
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
			r, buf := newResourceAndBuf(t, 1000, rspb.CacheType_CAS, remoteInstanceName)
			err = pc.Set(ctx, r, buf)
			require.NoError(t, err)
			digests = append(digests, r.GetDigest())
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
			resourceName := digest.NewResourceName(d, remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()

			exists, err := pc2.Contains(ctx, resourceName)
			require.NoError(t, err)
			require.True(t, exists)

			rbuf, err := pc2.Get(ctx, resourceName)
			require.NoError(t, err)

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf), repb.DigestFunction_SHA256)
			require.NoError(t, err)
			if d.GetHash() != d2.GetHash() {
				t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
			}
		}
	}
}

func generateKMSKey(t *testing.T, kmsDir string, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(kmsDir, id), key, 0644)
	require.NoError(t, err)
	return "local-insecure-kms://" + id
}

func createKey(t *testing.T, env environment.Env, keyID, groupID, groupKeyURI string) (*tables.EncryptionKey, *tables.EncryptionKeyVersion) {
	kmsClient := env.GetKMS()

	masterKeyPart := make([]byte, 32)
	_, err := rand.Read(masterKeyPart)
	require.NoError(t, err)
	groupKeyPart := make([]byte, 32)
	_, err = rand.Read(groupKeyPart)
	require.NoError(t, err)

	masterAEAD, err := kmsClient.FetchMasterKey()
	require.NoError(t, err)
	encMasterKeyPart, err := masterAEAD.Encrypt(masterKeyPart, []byte(groupID))
	require.NoError(t, err)

	groupAEAD, err := kmsClient.FetchKey(groupKeyURI)
	require.NoError(t, err)
	encGroupKeyPart, err := groupAEAD.Encrypt(groupKeyPart, []byte(groupID))
	require.NoError(t, err)

	key := &tables.EncryptionKey{
		EncryptionKeyID: keyID,
		GroupID:         groupID,
	}
	keyVersion := &tables.EncryptionKeyVersion{
		EncryptionKeyID:    keyID,
		Version:            1,
		MasterEncryptedKey: encMasterKeyPart,
		GroupKeyURI:        groupKeyURI,
		GroupEncryptedKey:  encGroupKeyPart,
	}
	return key, keyVersion
}

func getCrypterEnv(t *testing.T) (*testenv.TestEnv, string) {
	rand.Seed(time.Now().UnixMicro())

	kmsDir := testfs.MakeTempDir(t)
	masterKeyURI := generateKMSKey(t, kmsDir, "masterKey")

	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	flags.Set(t, "keystore.master_key_uri", masterKeyURI)
	env := testenv.GetTestEnv(t)
	err := kms.Register(env)
	require.NoError(t, err)
	err = crypter_service.Register(env)
	require.NoError(t, err)
	return env, kmsDir
}

func TestEncryption(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	testCases := []struct {
		desc                   string
		averageChunkSizeBytes  int
		maxInlineFileSizeBytes int64
		digestSize             int64
	}{
		{
			desc:                  "chunking_on_single_chunk",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            63,
		},
		{
			desc:                  "chunking_on_multiple_chunks",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            2 * 1024,
		},
		{
			desc:                   "chunking_off_inline",
			maxInlineFileSizeBytes: 200,
			digestSize:             100,
		},
		{
			desc:                   "chunking_off_disk",
			maxInlineFileSizeBytes: 100,
			digestSize:             1000,
		},
	}

	withKey := []bool{false, true}

	for _, tc := range testCases {
		for _, isKeyAvailable := range withKey {
			desc := fmt.Sprintf("%s_is_key_available_%t", tc.desc, isKeyAvailable)
			t.Run(desc, func(t *testing.T) {
				te, kmsDir := getCrypterEnv(t)
				rootDir := testfs.MakeTempDir(t)

				userID := "US123"
				groupID := "GR123"
				groupKeyID := "EK123"
				user := testauth.User(userID, groupID)
				user.CacheEncryptionEnabled = true
				users := map[string]interfaces.UserInfo{userID: user}
				auther := testauth.NewTestAuthenticator(users)
				te.SetAuthenticator(auther)

				ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
				require.NoError(t, err)

				if isKeyAvailable {
					group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
					key, keyVersion := createKey(t, te, groupKeyID, groupID, group1KeyURI)
					err = te.GetDBHandle().DB(ctx).Create(key).Error
					require.NoError(t, err)
					err = te.GetDBHandle().DB(ctx).Create(keyVersion).Error
					require.NoError(t, err)
				}

				opts := &pebble_cache.Options{
					RootDirectory: rootDir,
					Partitions: []disk.Partition{{
						ID: pebble_cache.DefaultPartitionID, MaxSizeBytes: maxSizeBytes,
					}},
				}
				pc, err := pebble_cache.NewPebbleCache(te, opts)
				require.NoError(t, err)
				err = pc.Start()
				require.NoError(t, err)

				rn, buf := newCASResourceBuf(t, tc.digestSize)
				err = pc.Set(ctx, rn, buf)
				if !isKeyAvailable {
					require.ErrorContains(t, err, "no key available")
				} else {
					readBuf, err := pc.Get(ctx, rn)
					require.NoError(t, err)
					require.Equal(t, buf, readBuf)
				}

				err = pc.Stop()
				require.NoError(t, err)
			})
		}
	}
}

func TestReadEncryptedWrongDigestSize(t *testing.T) {
	maxSizeBytes := int64(1_000_000_000) // 1GB
	te, kmsDir := getCrypterEnv(t)
	rootDir := testfs.MakeTempDir(t)

	userID := "US123"
	groupID := "GR123"
	groupKeyID := "EK123"
	user := testauth.User(userID, groupID)
	user.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{userID: user}
	auther := testauth.NewTestAuthenticator(users)
	te.SetAuthenticator(auther)

	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
	key, keyVersion := createKey(t, te, groupKeyID, groupID, group1KeyURI)
	err = te.GetDBHandle().DB(ctx).Create(key).Error
	require.NoError(t, err)
	err = te.GetDBHandle().DB(ctx).Create(keyVersion).Error
	require.NoError(t, err)

	opts := &pebble_cache.Options{
		RootDirectory: rootDir,
		Partitions: []disk.Partition{{
			ID: pebble_cache.DefaultPartitionID, MaxSizeBytes: maxSizeBytes,
		}},
	}
	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)

	rn, buf := newCASResourceBuf(t, 100)
	err = pc.Set(ctx, rn, buf)
	require.NoError(t, err)

	// Pass a different digest size to Get. It should not affect Get.
	rn.Digest.SizeBytes = 1
	readBuf, err := pc.Get(ctx, rn)
	require.NoError(t, err)
	require.Equal(t, buf, readBuf)

	err = pc.Stop()
	require.NoError(t, err)
}

// The same digest encrypted/unencrypted should be able to co-exist in the
// cache.
func TestEncryptedUnencryptedSameDigest(t *testing.T) {
	flags.Set(t, "cache.pebble.active_key_version", 3)

	testCases := []struct {
		desc                   string
		averageChunkSizeBytes  int
		maxInlineFileSizeBytes int64
		digestSize             int64
	}{
		{
			desc:                  "chunking_on_single_chunk",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            63,
		},
		{
			desc:                  "chunking_on_multiple_chunks",
			averageChunkSizeBytes: 64 * 4,
			digestSize:            2 * 1024,
		},
		{
			desc:                   "chunking_off_inline",
			maxInlineFileSizeBytes: 200,
			digestSize:             100,
		},
		{
			desc:                   "chunking_off_disk",
			maxInlineFileSizeBytes: 100,
			digestSize:             1000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			te, kmsDir := getCrypterEnv(t)

			userID := "US123"
			groupID := "GR123"
			user := testauth.User(userID, groupID)
			user.CacheEncryptionEnabled = true
			users := map[string]interfaces.UserInfo{userID: user}
			auther := testauth.NewTestAuthenticator(users)
			te.SetAuthenticator(auther)

			rootDir := testfs.MakeTempDir(t)
			maxSizeBytes := int64(1_000_000_000) // 1GB
			ctx := context.Background()

			groupKeyID := "EK456"
			group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
			key, keyVersion := createKey(t, te, groupKeyID, groupID, group1KeyURI)
			err := te.GetDBHandle().DB(ctx).Create(key).Error
			require.NoError(t, err)
			err = te.GetDBHandle().DB(ctx).Create(keyVersion).Error
			require.NoError(t, err)

			opts := &pebble_cache.Options{
				RootDirectory: rootDir,
				Partitions: []disk.Partition{{
					ID: pebble_cache.DefaultPartitionID, MaxSizeBytes: maxSizeBytes,
				}},
				MaxInlineFileSizeBytes: tc.maxInlineFileSizeBytes,
				AverageChunkSizeBytes:  tc.averageChunkSizeBytes,
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)

			userCtx, err := auther.WithAuthenticatedUser(context.Background(), userID)
			require.NoError(t, err)
			anonCtx := getAnonContext(t, te)

			rn, buf := newCASResourceBuf(t, tc.digestSize)
			err = pc.Set(anonCtx, rn, buf)
			require.NoError(t, err)
			err = pc.Set(userCtx, rn, buf)
			require.NoError(t, err)

			readBuf, err := pc.Get(userCtx, rn)
			require.NoError(t, err)
			if !bytes.Equal(buf, readBuf) {
				require.FailNow(t, "original text and decrypted text didn't match")
			}

			readBuf, err = pc.Get(anonCtx, rn)
			require.NoError(t, err)
			if !bytes.Equal(buf, readBuf) {
				require.FailNow(t, "original text and read text didn't match")
			}
			err = pc.Stop()
			require.NoError(t, err)

		})
	}
}

func TestEncryptionAndCompression(t *testing.T) {
	maxInlineFileSizeBytes := int64(1024)
	averageChunkSizeBytes := 64 * 4

	te, kmsDir := getCrypterEnv(t)

	userID := "US123"
	groupID := "GR123"
	groupKeyID := "EK123"
	user := testauth.User(userID, groupID)
	user.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{userID: user}
	auther := testauth.NewTestAuthenticator(users)
	te.SetAuthenticator(auther)

	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
	key, keyVersion := createKey(t, te, groupKeyID, groupID, group1KeyURI)
	err = te.GetDBHandle().DB(ctx).Create(key).Error
	require.NoError(t, err)
	err = te.GetDBHandle().DB(ctx).Create(keyVersion).Error
	require.NoError(t, err)

	testParams := []struct {
		desc                  string
		blobSize              int
		averageChunkSizeBytes int
	}{
		{
			desc:     "disk_multiple_compression_chunk",
			blobSize: pebble_cache.CompressorBufSizeBytes + 1,
		},
		{
			desc:     "inline",
			blobSize: int(maxInlineFileSizeBytes) - 100,
		},
		{
			desc:                  "chunking_on_multiple_cdc_chunks",
			blobSize:              pebble_cache.CompressorBufSizeBytes + 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
		},
		{
			desc:                  "chunking_on_multiple_cdc_chunks_single_compression_chunk",
			blobSize:              pebble_cache.CompressorBufSizeBytes - 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
		},
		{
			desc:                  "chunking_on_single_chunk",
			blobSize:              averageChunkSizeBytes/4 - 1,
			averageChunkSizeBytes: averageChunkSizeBytes,
		},
	}

	testCases := []struct {
		name              string
		isWriteCompressed bool
		isReadCompressed  bool
		testOffset        bool
	}{
		{
			name:              "write_compressed_read_compressed",
			isWriteCompressed: true,
			isReadCompressed:  true,
		},
		{
			name:              "write_compressed_read_decompressed",
			isWriteCompressed: true,
			isReadCompressed:  false,
		},
		{
			name:              "write_compressed_read_decompressed_offset",
			isWriteCompressed: true,
			isReadCompressed:  false,
		},
		{
			name:              "write_uncompressed_read_compressed",
			isWriteCompressed: false,
			isReadCompressed:  true,
		},
		{
			name:              "write_uncompressed_read_decompressed",
			isWriteCompressed: false,
			isReadCompressed:  false,
		},
		{
			name:              "write_uncompressed_read_decompressed_offset",
			isWriteCompressed: false,
			isReadCompressed:  false,
		},
	}

	for _, tp := range testParams {
		rootDir := testfs.MakeTempDir(t)
		maxSizeBytes := int64(1_000_000_000) // 1GB
		opts := &pebble_cache.Options{
			RootDirectory: rootDir,
			Partitions: []disk.Partition{{
				ID: pebble_cache.DefaultPartitionID, MaxSizeBytes: maxSizeBytes,
			}},
		}
		pc, err := pebble_cache.NewPebbleCache(te, opts)
		require.NoError(t, err)
		err = pc.Start()
		require.NoError(t, err)

		// Make blob big enough to require multiple chunks to compress
		blob := compressibleBlobOfSize(tp.blobSize)
		compressedBuf := compression.CompressZstd(nil, blob)

		// Note: Digest is of uncompressed contents
		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)

		decompressedRN := digest.NewResourceName(d, "" /*instanceName*/, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()
		compressedRN := proto.Clone(decompressedRN).(*rspb.ResourceName)
		compressedRN.Compressor = repb.Compressor_ZSTD

		for _, tc := range testCases {
			desc := fmt.Sprintf("%s_%s", tc.name, tp.desc)
			t.Run(desc, func(t *testing.T) {
				var dataToWrite []byte
				var rnToWrite, rnToRead *rspb.ResourceName
				if tc.isWriteCompressed {
					dataToWrite = compressedBuf
					rnToWrite = compressedRN
				} else {
					dataToWrite = blob
					rnToWrite = decompressedRN
				}
				if tc.isReadCompressed {
					rnToRead = compressedRN
				} else {
					rnToRead = decompressedRN
				}
				writeResource(t, ctx, pc, rnToWrite, dataToWrite)

				// Read data
				readOffset := int64(0)
				readLimit := int64(0)
				if tc.testOffset {
					readOffset = int64(tp.blobSize) - 30
					readLimit = 20
				}
				data := readResource(t, ctx, pc, rnToRead, readOffset, readLimit)
				if tc.isReadCompressed {
					data, err = compression.DecompressZstd(nil, data)
					require.NoError(t, err)
				}
				expectedReadData := blob
				if tc.testOffset {
					expectedReadData = blob[readOffset : readOffset+readLimit]
				}
				require.Equal(t, expectedReadData, data)
			})
		}

		err = pc.Stop()
		require.NoError(t, err)
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
		r, buf := newResourceAndBuf(b, 1000, rspb.CacheType_CAS, "" /*instanceName*/)
		digestKeys = append(digestKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			b.Fatalf("Error setting %q in cache: %s", r.GetDigest().GetHash(), err.Error())
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
		r, buf := newCASResourceBuf(b, 1000)
		digestKeys = append(digestKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			b.Fatalf("Error setting %q in cache: %s", r.GetDigest().GetHash(), err.Error())
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
		r, buf := newCASResourceBuf(b, 1000)
		digestKeys = append(digestKeys, r)
		if err := pc.Set(ctx, r, buf); err != nil {
			b.Fatalf("Error setting %q in cache: %s", r.GetDigest().GetHash(), err.Error())
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
		r, buf := newCASResourceBuf(b, 1000)
		b.StartTimer()
		err := pc.Set(ctx, r, buf)
		b.StopTimer()
		if err != nil {
			b.Fatalf("Error setting %q in cache: %s", r.GetDigest().GetHash(), err.Error())
		}
	}
}

func TestSupportsEncryption(t *testing.T) {
	te := testenv.GetTestEnv(t)
	apiKey1 := "AK2222"
	group1 := "GR7890"
	apiKey2 := "AK3333"
	group2 := "GR1111"
	testUsers := testauth.TestUsers(apiKey1, group1, apiKey2, group2)
	te.SetAuthenticator(testauth.NewTestAuthenticator(testUsers))

	maxSizeBytes := int64(1_000_000_000) // 1GB
	rootDir := testfs.MakeTempDir(t)
	group1PartitionID := "user1part"
	group2PartitionID := "user2part"
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
				ID:           group1PartitionID,
				MaxSizeBytes: maxSizeBytes,
			},
			{
				ID:                  group2PartitionID,
				MaxSizeBytes:        maxSizeBytes,
				EncryptionSupported: true,
			},
		},
		PartitionMappings: []disk.PartitionMapping{
			{
				GroupID:     group1,
				PartitionID: group1PartitionID,
			},
			{
				GroupID:     group2,
				PartitionID: group2PartitionID,
			},
		},
	}

	pc, err := pebble_cache.NewPebbleCache(te, opts)
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)

	// Anon write should go to the default partition which doesn't support
	// encryption.
	ctx := getAnonContext(t, te)
	require.False(t, pc.SupportsEncryption(ctx))

	// First group is mapped to a partition that does not have encryption
	// support.
	ctx = te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), apiKey1)
	require.False(t, pc.SupportsEncryption(ctx))

	// Second user should be able to use encryption.
	ctx = te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), apiKey2)
	require.True(t, pc.SupportsEncryption(ctx))
}

func TestSampling(t *testing.T) {
	flags.Set(t, "cache.pebble.active_key_version", 3)

	te, kmsDir := getCrypterEnv(t)

	userID := "US123"
	groupID := "GR123"
	groupKeyID := "EK123"
	user := testauth.User(userID, groupID)
	user.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{userID: user}
	auther := testauth.NewTestAuthenticator(users)
	te.SetAuthenticator(auther)

	ctx, err := auther.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	group1KeyURI := generateKMSKey(t, kmsDir, "group1Key")
	key, keyVersion := createKey(t, te, groupKeyID, groupID, group1KeyURI)
	err = te.GetDBHandle().DB(ctx).Create(key).Error
	require.NoError(t, err)
	err = te.GetDBHandle().DB(ctx).Create(keyVersion).Error
	require.NoError(t, err)

	testCases := []struct {
		desc                  string
		averageChunkSizeBytes int
	}{
		{
			desc:                  "chunking_on",
			averageChunkSizeBytes: 64 * 4,
		},
		{
			desc:                  "chunking_off",
			averageChunkSizeBytes: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rootDir := testfs.MakeTempDir(t)
			minEvictionAge := 1 * time.Hour
			clock := clockwork.NewFakeClock()

			opts := &pebble_cache.Options{
				RootDirectory: rootDir,
				Partitions: []disk.Partition{{
					ID: pebble_cache.DefaultPartitionID,
					// Force all entries to be evicted as soon as they pass the minimum
					// eviction age.
					MaxSizeBytes: 2,
				}},
				MinEvictionAge:        &minEvictionAge,
				AverageChunkSizeBytes: tc.averageChunkSizeBytes,
				Clock:                 clock,
			}
			pc, err := pebble_cache.NewPebbleCache(te, opts)
			require.NoError(t, err)
			err = pc.Start()
			require.NoError(t, err)

			rn, buf := newCASResourceBuf(t, 100)
			anonCtx := getAnonContext(t, te)
			err = pc.Set(anonCtx, rn, buf)
			require.NoError(t, err)

			// Advance time (to force newer atime) and write the same digest
			// encrypted. The encrypted key should have the same digest prefix as the
			// unencrypted key and come before the unencrypted key in lexicographical f
			// order.
			clock.Advance(5 * time.Minute)
			err = pc.Set(ctx, rn, buf)
			require.NoError(t, err)

			// Write some random digests as well.
			var randomResources []*rspb.ResourceName
			for i := 0; i < 100; i++ {
				rn, buf := newCASResourceBuf(t, 100)
				anonCtx := getAnonContext(t, te)
				err = pc.Set(anonCtx, rn, buf)
				require.NoError(t, err)
				randomResources = append(randomResources, rn)
			}

			// Now advance the clock past the min eviction age to allow eviction to
			// kick in. The unencrypted test digest should be evicted.
			clock.Advance(minEvictionAge - 1*time.Minute)

			for i := 0; i < 8; i++ {
				if exists, err := pc.Contains(anonCtx, rn); err == nil && !exists {
					log.Infof("i = %d: unencrypted test digest is evicted", i)
					break
				}
				time.Sleep(500 * time.Millisecond)
			}

			// The unencrypted key should no longer exist.
			unencryptedExists, err := pc.Contains(anonCtx, rn)
			require.NoError(t, err)
			require.False(t, unencryptedExists)

			// The encrypted key should still exist.
			encryptedExists, err := pc.Contains(ctx, rn)
			require.NoError(t, err)
			require.True(t, encryptedExists)

			// The other random digests should also exist.
			for _, rr := range randomResources {
				exists, err := pc.Contains(anonCtx, rr)
				require.NoError(t, err)
				require.True(t, exists)
			}

			err = pc.Stop()
			require.NoError(t, err)
		})
	}
}
