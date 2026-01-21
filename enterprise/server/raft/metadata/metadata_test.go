package metadata_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/metadata"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagetracker"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/quarantine"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	guuid "github.com/google/uuid"
)

var (
	userMap = testauth.TestUsers("user1", "group1", "user2", "group2")
)

type testConfig struct {
	env    *testenv.TestEnv
	ta     *testauth.TestAuthenticator
	config *metadata.Config
}

func getTestConfigs(t *testing.T, n int) []testConfig {
	res := make([]testConfig, 0, n)
	for i := 0; i < n; i++ {
		c := testConfig{
			ta:     testauth.NewTestAuthenticator(t, userMap),
			env:    testenv.GetTestEnv(t),
			config: getCacheConfig(t),
		}
		c.env.SetAuthenticator(c.ta)
		res = append(res, c)
	}
	return res
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func getCacheConfig(t *testing.T) *metadata.Config {
	id, err := guuid.NewRandom()
	require.NoError(t, err)
	return &metadata.Config{
		NHID:            id.String(),
		RootDir:         testfs.MakeTempDir(t),
		Hostname:        "127.0.0.1",
		ListenAddr:      "127.0.0.1",
		HTTPPort:        testport.FindFree(t),
		GRPCPort:        testport.FindFree(t),
		LogDBConfigType: store.SmallMemLogDBConfigType,
		Partitions: []disk.Partition{
			{
				ID:           constants.DefaultPartitionID,
				MaxSizeBytes: 10_000_000_000, /* 10 GB */
				NumRanges:    1,
			}},
	}
}

func allHealthy(caches ...*metadata.Server) bool {
	eg := errgroup.Group{}
	for _, cache := range caches {
		cache := cache
		eg.Go(func() error {
			return cache.Check(context.Background())
		})
	}
	err := eg.Wait()
	return err == nil
}

func parallelShutdown(caches ...*metadata.Server) {
	eg := errgroup.Group{}
	ctx := context.Background()
	for _, cache := range caches {
		cache := cache
		eg.Go(func() error {
			cache.Stop(ctx)
			return nil
		})
	}
	eg.Wait()
}

func waitForHealthy(t *testing.T, caches ...*metadata.Server) {
	log.Infof("wait for healthy")
	start := time.Now()
	timeout := 30 * time.Second
	done := make(chan struct{})
	go func() {
		for {
			if allHealthy(caches...) {
				close(done)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		log.Infof("%d caches became healthy in %s", len(caches), time.Since(start))
	case <-time.After(timeout):
		require.Failf(t, "caches not healthy", "Caches [%d] did not become healthy after %s, %+v", len(caches), timeout, caches[0])
	}
}

func waitForShutdown(t *testing.T, caches ...*metadata.Server) {
	timeout := 30 * time.Second
	done := make(chan struct{})
	go func() {
		parallelShutdown(caches...)
		close(done)
	}()

	select {
	case <-done:
		break
	case <-time.After(timeout):
		require.Failf(t, "not shutdown", "Caches [%d] did not shutdown after %s", len(caches), timeout)
	}
}

func startNodes(t *testing.T, configs []testConfig) []*metadata.Server {
	eg := errgroup.Group{}
	n := len(configs)
	caches := make([]*metadata.Server, n)

	joinList := make([]string, 0, n)
	for i := 0; i < n; i++ {
		joinList = append(joinList, localAddr(t))
	}

	for i, config := range configs {
		i := i
		lN := joinList[i]
		joinList := joinList
		gs, err := gossip.NewWithArgs(config.config.NHID, lN, joinList)
		require.NoError(t, err)
		config.config.GossipManager = gs
		eg.Go(func() error {
			n, err := metadata.New(config.env, config.config)
			if err != nil {
				return err
			}
			caches[i] = n
			return nil
		})
	}
	require.NoError(t, eg.Wait())

	// wait for them all to become healthy
	waitForHealthy(t, caches...)

	t.Cleanup(func() {
		waitForShutdown(t, caches...)
	})
	return caches
}

var filestorer = filestore.New()

func randomFileMetadata(t testing.TB, sizeBytes int64, groupID string) *sgpb.FileMetadata {
	t.Helper()

	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	iw := filestorer.InlineWriter(context.TODO(), int64(len(buf)))
	bytesWritten, err := io.Copy(iw, bytes.NewReader(buf))
	require.NoError(t, err)

	rn := digest.ResourceNameFromProto(r)
	require.NoError(t, rn.Validate())

	now := time.Now().UnixMicro()
	md := &sgpb.FileMetadata{
		FileRecord: &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:          rn.GetCacheType(),
				RemoteInstanceName: rn.GetInstanceName(),
				PartitionId:        "default",
				GroupId:            groupID,
			},
			Digest:         rn.GetDigest(),
			DigestFunction: rn.GetDigestFunction(),
			Compressor:     rn.GetCompressor(),
			Encryption:     nil,
		},
		StorageMetadata:    iw.Metadata(),
		EncryptionMetadata: nil,
		StoredSizeBytes:    bytesWritten,
		LastAccessUsec:     now,
		LastModifyUsec:     now,
		FileType:           sgpb.FileMetadata_COMPLETE_FILE_TYPE,
	}
	return md
}

func TestAutoBringup(t *testing.T) {
	configs := getTestConfigs(t, 3)
	startNodes(t, configs)
}

func TestGetAndSet(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNodes(t, configs)
	rc1 := caches[0]

	ta := configs[0].ta
	ctxUser1, err := ta.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctxUser2, err := ta.WithAuthenticatedUser(context.Background(), "user2")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		md := randomFileMetadata(t, 100, "group1")

		// Should be able to Set a record.
		_, err := rc1.Set(ctxUser1, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		require.NoError(t, err, i)

		// User 1 should be able to fetch the record just set.
		getRsp, err := rc1.Get(ctxUser1, &mdpb.GetRequest{
			FileRecords: []*sgpb.FileRecord{md.GetFileRecord()},
		})
		require.NoError(t, err, i)
		require.Equal(t, 1, len(getRsp.GetFileMetadatas()))
		assert.True(t, proto.Equal(md, getRsp.GetFileMetadatas()[0]))

		// User2 should not be able to fetch the record just set.
		_, err = rc1.Get(ctxUser2, &mdpb.GetRequest{
			FileRecords: []*sgpb.FileRecord{md.GetFileRecord()},
		})
		require.Error(t, err)
		require.True(t, status.IsUnauthenticatedError(err), "is unauthenticated")

		// User 2 should not be able to fetch User 1's record even when setting to
		// its own group id.
		fr2 := md.GetFileRecord().CloneVT()
		fr2.GetIsolation().GroupId = "group2"
		getRsp, err = rc1.Get(ctxUser2, &mdpb.GetRequest{
			FileRecords: []*sgpb.FileRecord{fr2},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(getRsp.GetFileMetadatas()))
		require.Nil(t, nil, getRsp.GetFileMetadatas()[0])

		// User 1 should be able to lookup (check existance) of the record.
		findRsp, err := rc1.Find(ctxUser1, &mdpb.FindRequest{
			FileRecords: []*sgpb.FileRecord{md.GetFileRecord()},
		})
		require.NoError(t, err, i)
		require.Equal(t, 1, len(findRsp.GetFindResponses()))
		assert.True(t, findRsp.GetFindResponses()[0].GetPresent())

		// User 2 should not be able to lookup (check existance) of the record.
		_, err = rc1.Find(ctxUser2, &mdpb.FindRequest{
			FileRecords: []*sgpb.FileRecord{md.GetFileRecord()},
		})
		require.Error(t, err)
		require.True(t, status.IsUnauthenticatedError(err), "is unauthenticated")

		// User 2 should be able to check existance; but should not find it.
		findRsp, err = rc1.Find(ctxUser2, &mdpb.FindRequest{
			FileRecords: []*sgpb.FileRecord{fr2},
		})
		require.NoError(t, err, i)
		require.Equal(t, 1, len(findRsp.GetFindResponses()))
		assert.False(t, findRsp.GetFindResponses()[0].GetPresent())

		// User 2 should not be able to delete the record.
		_, err = rc1.Delete(ctxUser2, &mdpb.DeleteRequest{
			DeleteOperations: []*mdpb.DeleteRequest_DeleteOperation{{
				FileRecord: md.GetFileRecord(),
			}},
		})
		require.Error(t, err)
		require.True(t, status.IsUnauthenticatedError(err), "is unauthenticated")

		// User 1 should be able to delete the record.
		_, err = rc1.Delete(ctxUser1, &mdpb.DeleteRequest{
			DeleteOperations: []*mdpb.DeleteRequest_DeleteOperation{{
				FileRecord: md.GetFileRecord(),
			}},
		})
		require.NoError(t, err, i)

		// Record should no longer be found.
		findRsp, err = rc1.Find(ctxUser1, &mdpb.FindRequest{
			FileRecords: []*sgpb.FileRecord{md.GetFileRecord()},
		})
		require.NoError(t, err, i)
		require.Equal(t, 1, len(findRsp.GetFindResponses()))
		assert.False(t, findRsp.GetFindResponses()[0].GetPresent())
	}
}

func TestCacheShutdown(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNodes(t, configs)
	rc1 := caches[0]
	rc2 := caches[1]

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env.GetAuthenticator())
	require.NoError(t, err)

	cacheRPCTimeout := 5 * time.Second
	recordsWritten := make([]*sgpb.FileRecord, 0)
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()

		md := randomFileMetadata(t, 100, interfaces.AuthAnonymousUser)
		_, err := rc1.Set(ctx, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		require.NoError(t, err)
		recordsWritten = append(recordsWritten, md.GetFileRecord())
	}

	// shutdown one node
	waitForShutdown(t, caches[len(caches)-1])

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(ctx, cacheRPCTimeout)
		defer cancel()
		md := randomFileMetadata(t, 100, interfaces.AuthAnonymousUser)
		_, err := rc2.Set(ctx, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		require.NoError(t, err)
		recordsWritten = append(recordsWritten, md.GetFileRecord())
	}

	findRsp, err := rc1.Find(ctx, &mdpb.FindRequest{
		FileRecords: recordsWritten,
	})
	require.NoError(t, err)
	require.Equal(t, len(recordsWritten), len(findRsp.GetFindResponses()))
	for _, rsp := range findRsp.GetFindResponses() {
		assert.True(t, rsp.GetPresent())
	}

}

func TestDistributedRanges(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNodes(t, configs)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env.GetAuthenticator())
	require.NoError(t, err)

	wrote := make([]*sgpb.FileMetadata, 0)
	for i := 0; i < 10; i++ {
		rc := caches[rand.Intn(len(caches))]

		md := randomFileMetadata(t, 100, interfaces.AuthAnonymousUser)
		_, err := rc.Set(ctx, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		require.NoError(t, err)
		wrote = append(wrote, md)
	}

	victim := caches[0]
	caches = caches[1:]
	waitForShutdown(t, victim)

	for _, md := range wrote {
		rc := caches[rand.Intn(len(caches))]

		getRsp, err := rc.Get(ctx, &mdpb.GetRequest{
			FileRecords: []*sgpb.FileRecord{md.GetFileRecord()},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(getRsp.GetFileMetadatas()))
		assert.True(t, proto.Equal(md, getRsp.GetFileMetadatas()[0]))
	}
}

func TestFindMissingMetadata(t *testing.T) {
	configs := getTestConfigs(t, 3)
	caches := startNodes(t, configs)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env.GetAuthenticator())
	require.NoError(t, err)

	rc1 := caches[0]

	recordsWritten := make([]*sgpb.FileRecord, 0)
	setReq := &mdpb.SetRequest{}
	for i := 0; i < 10; i++ {
		md := randomFileMetadata(t, 100, interfaces.AuthAnonymousUser)
		setReq.SetOperations = append(setReq.SetOperations, &mdpb.SetRequest_SetOperation{
			FileMetadata: md,
		})
		recordsWritten = append(recordsWritten, md.GetFileRecord())
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = rc1.Set(ctx, setReq)
	require.NoError(t, err)

	recordsToLookFor := recordsWritten
	// Look for some additional records which have not been written to the
	// metadata server. They should not be found.
	for i := 0; i < 5; i++ {
		md := randomFileMetadata(t, 100, interfaces.AuthAnonymousUser)
		recordsToLookFor = append(recordsToLookFor, md.GetFileRecord())
	}

	findRsp, err := rc1.Find(ctx, &mdpb.FindRequest{
		FileRecords: recordsToLookFor,
	})
	require.NoError(t, err)
	require.Equal(t, len(recordsToLookFor), len(findRsp.GetFindResponses()))

	for i, rsp := range findRsp.GetFindResponses() {
		if i < len(recordsWritten) {
			assert.True(t, rsp.GetPresent())
		} else {
			assert.False(t, rsp.GetPresent())
		}
	}
}

func TestLRU(t *testing.T) {
	quarantine.SkipQuarantinedTest(t)

	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.atime_update_threshold", 10*time.Second)
	flags.Set(t, "cache.raft.atime_write_batch_size", 1)
	flags.Set(t, "cache.raft.min_eviction_age", 0)
	flags.Set(t, "cache.raft.samples_per_batch", 50)
	flags.Set(t, "cache.raft.sample_pool_size", 10)
	flags.Set(t, "cache.raft.eviction_batch_size", 1)
	flags.Set(t, "cache.raft.local_size_update_period", 100*time.Millisecond)
	flags.Set(t, "cache.raft.partition_usage_delta_bytes_threshold", 100)

	digestSize := int64(1000)
	numDigests := 25
	maxSizeBytes := int64(math.Ceil(14022 * (1 / usagetracker.EvictionCutoffThreshold))) // account for .9 evictor cutoff

	configs := getTestConfigs(t, 1)

	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), configs[0].env.GetAuthenticator())
	require.NoError(t, err)

	clock := clockwork.NewFakeClock()
	for _, c := range configs {
		c.env.SetClock(clock)
		c.config.Partitions = []disk.Partition{
			{
				ID:           "default",
				MaxSizeBytes: maxSizeBytes,
				NumRanges:    1,
			},
		}
	}

	caches := startNodes(t, configs)
	rc1 := caches[0]
	quartile := numDigests / 4
	lastUsed := make(map[*sgpb.FileRecord]time.Time, numDigests)
	resourceKeys := make([]*sgpb.FileRecord, 0)
	for i := 0; i < numDigests; i++ {
		md := randomFileMetadata(t, digestSize, interfaces.AuthAnonymousUser)
		_, err := rc1.Set(ctx, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		require.NoError(t, err)
		lastUsed[md.GetFileRecord()] = clock.Now()
		resourceKeys = append(resourceKeys, md.GetFileRecord())
	}

	rc1.TestingFlush()

	clock.Advance(5 * time.Minute)
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

			_, err = rc1.Find(ctx, &mdpb.FindRequest{
				FileRecords: []*sgpb.FileRecord{r},
			})
			require.NoError(t, err)
			lastUsed[r] = clock.Now()
		}
		clock.Advance(5 * time.Minute)
	}

	// Write more data
	for i := 0; i < quartile; i++ {
		md := randomFileMetadata(t, digestSize, interfaces.AuthAnonymousUser)
		_, err := rc1.Set(ctx, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		require.NoError(t, err)
		lastUsed[md.GetFileRecord()] = clock.Now()
		resourceKeys = append(resourceKeys, md.GetFileRecord())
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = rc1.TestingWaitForGC(ctx)
	require.NoError(t, err)
	waitForShutdown(t, caches...)

	caches = startNodes(t, configs)
	rc1 = caches[0]

	perfectLRUEvictees := make(map[*sgpb.FileRecord]struct{})
	sort.Slice(resourceKeys, func(i, j int) bool {
		return lastUsed[resourceKeys[i]].Before(lastUsed[resourceKeys[j]])
	})
	for _, r := range resourceKeys[:quartile] {
		perfectLRUEvictees[r] = struct{}{}
	}
	log.Infof("perfectLRUEvictees num: %d", len(perfectLRUEvictees))
	// We expect no more than x keys to have been evicted
	// We expect *most* of the keys evicted to be older
	evictedCount := 0
	perfectEvictionCount := 0
	evictedAgeTotal := time.Duration(0)

	keptCount := 0
	keptAgeTotal := time.Duration(0)

	now := clock.Now()
	for r, usedAt := range lastUsed {
		findRsp, err := rc1.Find(ctx, &mdpb.FindRequest{
			FileRecords: []*sgpb.FileRecord{r},
		})
		evicted := false
		if err == nil && len(findRsp.GetFindResponses()) == 1 {
			if !findRsp.GetFindResponses()[0].GetPresent() {
				evicted = true
			}
		}

		age := now.Sub(usedAt)
		if evicted {
			evictedCount++
			evictedAgeTotal += age
			if _, ok := perfectLRUEvictees[r]; ok {
				perfectEvictionCount++
			}
		} else {
			keptCount++
			keptAgeTotal += age
		}
	}

	require.Greater(t, evictedCount, 0)

	avgEvictedAgeSeconds := evictedAgeTotal.Seconds() / float64(evictedCount)
	avgKeptAgeSeconds := keptAgeTotal.Seconds() / float64(keptCount)

	log.Printf("evictedCount: %d [%d perfect], keptCount: %d, quartile: %d", evictedCount, perfectEvictionCount, keptCount, quartile)
	log.Printf("evictedAgeTotal: %s, keptAgeTotal: %s", evictedAgeTotal, keptAgeTotal)
	log.Printf("avg evictedAge: %f, avg keptAge: %f", avgEvictedAgeSeconds, avgKeptAgeSeconds)

	// Check that mostly (80%) of evictions were perfect:
	// Note: perfectEvictionCount <= len(perfectLRUEvictees) is always true.
	// Therefore, perfectEvictionCount >= 0.8 * evictedCount is only true when
	// 0.8 * evictedCount < len(perfectLRUEvictees). When 0.8 * evictedCount >=
	// len(perfectLRUEvictees), we have
	// perfectEvictionCount == len(perfectLRUEvictees).
	require.GreaterOrEqual(t, float64(perfectEvictionCount), math.Min(math.Floor(.80*float64(evictedCount)), float64(len(perfectLRUEvictees))))
	// Check that total number of evictions was < quartile*2, so not too much
	// good stuff was evicted.
	require.LessOrEqual(t, evictedCount, quartile*2)
	// Check that the avg age of evicted items is older than avg age of kept items.
	require.Greater(t, avgEvictedAgeSeconds, avgKeptAgeSeconds)
}
