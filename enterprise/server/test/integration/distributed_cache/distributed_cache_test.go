package distributed_cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	digestFunction = repb.DigestFunction_BLAKE3

	clusterSize = 8

	minFileSizeBytes = 100
	maxFileSizeBytes = minFileSizeBytes + 10_000
	numWrites        = 100
	numReads         = numWrites * 10
	doReads          = false
	concurrency      = 100

	shutdownDuringWrites = true
	shutdownDuringReads  = true
)

func TestDistributedCache(t *testing.T) {
	cluster := buildbuddy_enterprise.CreateCluster(t)

	var distributedCachePorts []int
	for i := 0; i < clusterSize; i++ {
		distributedCachePorts = append(distributedCachePorts, testport.FindFree(t))
	}
	var wg sync.WaitGroup
	for i := 0; i < clusterSize; i++ {
		flags := []string{
			"--cache.disk.root_directory=",
			"--cache.zstd_transcoding_enabled=true",
			"--cache.directory_sizes_enabled=true",
			"--cache.pebble.root_directory=" + testfs.MakeTempDir(t),
			"--cache.pebble.active_key_version=5",
			"--cache.distributed_cache.replication_factor=3",
			"--cache.distributed_cache.cluster_size=" + fmt.Sprint(clusterSize),
			"--cache.distributed_cache.listen_addr=" + fmt.Sprintf("localhost:%d", distributedCachePorts[i]),
			"--app.log_level=info", // DO NOT SUBMIT (switch to warn)
			"--zone_override=test",
		}
		for _, port := range distributedCachePorts {
			flags = append(flags, "--cache.distributed_cache.nodes="+fmt.Sprintf("localhost:%d", port))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cluster.RunApp(flags...)
		}()
	}
	wg.Wait()
	log.Infof("Test: all servers are ready")

	// Wait a little bit for all apps to discover each other.
	// TODO: do this explicitly somehow?
	time.Sleep(1000 * time.Millisecond)

	conn := cluster.LB.Dial()
	bsc := bspb.NewByteStreamClient(conn)

	ctx := context.Background()

	eg := errgroup.Group{}
	eg.SetLimit(100)
	// Do several concurrent writes (some compressed, some uncompressed)
	var digests []*repb.Digest
	log.Infof("Test: starting %d writes...", numWrites)
	for i := 1; i <= numWrites; i++ {
		i := i
		time.Sleep(1 * time.Millisecond)
		eg.Go(func() error {
			gen := digest.RandomGenerator(int64(i))
			size := minFileSizeBytes + rand.Int63n(maxFileSizeBytes)
			d, b, err := gen.RandomDigestBuf(size)
			require.NoError(t, err)
			digests = append(digests, d)

			rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
			if rand.Intn(2) == 0 {
				rn.SetCompressor(repb.Compressor_ZSTD)
			}

			if shutdownDuringWrites && i == numWrites/2 {
				go func() {
					t.Log("Shutting down app")
					cluster.ShutdownApp(rand.Intn(len(cluster.Apps)))
				}()
			}

			_, err = cachetools.UploadFromReader(ctx, bsc, rn, bytes.NewReader(b))
			require.NoError(t, err, "Write")

			return nil
		})
	}
	eg.Wait()

	// Do several concurrent reads of the digests we just wrote.
	// Halfway through, shut down a random app - the test should survive this.
	log.Infof("Test: starting %d reads...", numReads)
	for i := 1; i <= numReads; i++ {
		i := i
		time.Sleep(1 * time.Millisecond)
		eg.Go(func() error {
			d := digests[rand.Intn(len(digests))]

			rn := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
			if rand.Intn(2) == 0 {
				rn.SetCompressor(repb.Compressor_ZSTD)
			}

			for attempt := 1; attempt <= 10; attempt++ {
				err := cachetools.GetBlob(ctx, bsc, rn, io.Discard)
				if err == nil {
					return nil
				}
				// TODO: we should never get these "connection reset by peer"
				// errors if the testgrpc proxy is working properly. Fix!
				if status.IsUnavailableError(err) && strings.Contains(err.Error(), "connection reset by peer") {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				require.NoError(t, err, "Read %d of %d", i, numReads)
			}
			return nil
		})
		if shutdownDuringReads && i == numReads/2 {
			log.Infof("Test: completed read %d; shutting down app", i)
			cluster.ShutdownApp(rand.Intn(len(cluster.Apps)))
		}
	}
	eg.Wait()
}
