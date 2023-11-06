package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget  = flag.String("cache_target", "grpc://localhost:1985", "Cache target to connect to.")
	writeQPS     = flag.Uint("write_qps", 100, "How many queries per second to attempt to write.")
	readQPS      = flag.Uint("read_qps", 1000, "How many queries per second to attempt to read.")
	instanceName = flag.String("instance_name", "loadtest", "An optional Remote Instance name.")
	apiKey       = flag.String("api_key", "", "An optional API key to use when reading / writing data.")
	qpsAvgWindow = flag.Duration("qps_avg_window", 5*time.Second, "QPS averaging window")

	blobSize        = flag.Int64("blob_size", -1, "Num bytes (max) of blob to send/read. If -1, realistic blob sizes are used.")
	readCompressed  = flag.Bool("read_compressed", false, "Whether to request compressed blobs when reading.")
	readBatch       = flag.Bool("read_batch", false, "Whether to use BatchReadBlobs for reads.")
	writeCompressed = flag.Bool("write_compressed", false, "Whether to write compressed blobs.")
	recycleRate     = flag.Float64("recycle_rate", .10, "If true, re-queue digests for read after reading")
	timeout         = flag.Duration("timeout", 10*time.Second, "Use this timeout as the context timeout for rpc calls")
	keepGoing       = flag.Bool("keep_going", false, "If true, warn on errors but continue running")
	listen          = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	monitoringPort  = flag.Int("monitoring_port", 0, "The port to listen for monitoring traffic on")

	setRequestMetadata = flag.Bool("set_request_metadata", false, "Whether to set a simulated bazel request metadata header.")
)

const (
	byteStreamRead   = "google.bytestream.ByteStream/Read"
	byteStreamWrite  = "google.bytestream.ByteStream/Write"
	findMissingBlobs = "build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs"
)

var (
	digestGenerator *digest.Generator
	mu              sync.Mutex
)

var (
	// Data computed by sampling stored cache blob sizes.
	histBuckets     = []int{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000}
	histCounts      = []int{23, 33611, 33498, 20473, 10036, 3265, 504, 62}
	histCountsTotal int

	CacheloadErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "buildbuddy",
		Subsystem: "cacheload",
		Name:      "error_count",
		Help:      "The total number of cacheload errors.",
	}, []string{
		metrics.StatusHumanReadableLabel,
	})
)

func init() {
	for _, c := range histCounts {
		histCountsTotal += c
	}
}

func randRange(low, high int) int64 {
	i := int64(rand.Intn(high-low+1) + low)
	return i
}

func randomBlobSize() int64 {
	if *blobSize >= 0 {
		return *blobSize
	}
	n := rand.Intn(histCountsTotal)
	var sumTotal, low, high int
	for i, c := range histCounts {
		sumTotal += c
		high = histBuckets[i+1]
		if n < sumTotal {
			return randRange(low, high)
		}
		low = histBuckets[i+1]
	}
	return randRange(histBuckets[len(histBuckets)-2], histBuckets[len(histBuckets)-1])
}

func newRandomDigestBuf(sizeBytes int64) (*repb.Digest, []byte) {
	d, buf, err := digestGenerator.RandomDigestBuf(sizeBytes)
	if err != nil {
		log.Fatalf("Error generating digest: %s", err)
	}
	return d, buf
}

func incrementPromErrorMetric(err error) {
	if err == nil {
		return
	}
	CacheloadErrorCount.With(prometheus.Labels{
		metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
	}).Inc()
}

func writeBlob(ctx context.Context, client bspb.ByteStreamClient) (*repb.Digest, error) {
	d, buf := newRandomDigestBuf(randomBlobSize())
	resourceName := digest.NewResourceName(d, *instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	if *writeCompressed {
		resourceName.SetCompressor(repb.Compressor_ZSTD)
	}
	retrier := retry.DefaultWithContext(ctx)
	var err error
	for retrier.Next() {
		_, err = cachetools.UploadFromReader(ctx, client, resourceName, bytes.NewReader(buf))
		incrementPromErrorMetric(err)
		if err == nil {
			return d, nil
		} else if status.IsUnavailableError(err) {
			continue
		}
		return nil, err
	}
	return nil, err
}

func readBlob(ctx context.Context, client bspb.ByteStreamClient, casClient repb.ContentAddressableStorageClient, d *repb.Digest) error {
	resourceName := digest.NewResourceName(d, *instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	if *readCompressed {
		resourceName.SetCompressor(repb.Compressor_ZSTD)
	}
	retrier := retry.DefaultWithContext(ctx)
	var err error
	for retrier.Next() {
		if *readBatch {
			_, err = batchReadSingleBlob(ctx, casClient, resourceName)
		} else {
			err = cachetools.GetBlob(ctx, client, resourceName, io.Discard)
		}
		incrementPromErrorMetric(err)
		if err == nil {
			return nil
		} else if status.IsUnavailableError(err) {
			continue
		}
		return err
	}
	return err
}

func batchReadSingleBlob(ctx context.Context, casClient repb.ContentAddressableStorageClient, rn *digest.ResourceName) ([]byte, error) {
	responses, err := cachetools.BatchReadBlobs(ctx, casClient, &repb.BatchReadBlobsRequest{
		InstanceName:          rn.GetInstanceName(),
		AcceptableCompressors: []repb.Compressor_Value{rn.GetCompressor()},
		DigestFunction:        rn.GetDigestFunction(),
		Digests:               []*repb.Digest{rn.GetDigest()},
	})
	if err != nil {
		return nil, err
	}
	return responses[0].Data, responses[0].Err
}

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}

	digestGenerator = digest.RandomGenerator(time.Now().Unix())
	ctx := context.Background()

	if *writeQPS == 0 {
		log.Fatalf("Write QPS cannot be 0 -- data must be written before it can be read")
	}
	blobSizeDesc := fmt.Sprintf("size %d bytes", *blobSize)
	if *blobSize < 0 {
		blobSizeDesc = "simulating real blob sizes."
	}
	log.Printf("Cache loadtesting target %q", *cacheTarget)
	log.Printf("Planned load W: %d / R: %d [QPS], blob size: %s", *writeQPS, *readQPS, blobSizeDesc)

	if *monitoringPort > 0 {
		monitoring.StartMonitoringHandler(fmt.Sprintf("%s:%d", *listen, *monitoringPort))
	}

	conn, err := grpc_client.DialSimple(*cacheTarget, grpc.WithBlock(), grpc.WithTimeout(*timeout))
	if err != nil {
		log.Fatalf("Unable to connect to target '%s': %s", *cacheTarget, err)
	}
	log.Printf("Connected to target: %q", *cacheTarget)

	bsClient := bspb.NewByteStreamClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}
	if *setRequestMetadata {
		var err error
		ctx, err = bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
			ToolInvocationId: "0fa77149-7bbc-404a-94b6-71a326999893",
			ToolDetails: &repb.ToolDetails{
				ToolName:    "bazel",
				ToolVersion: "6.3.1",
			},
		})
		if err != nil {
			log.Fatalf("Failed to set request metadata: %s", err.Error())
		}
	}
	eg, gctx := errgroup.WithContext(ctx)

	numWriters := int(1 + (*writeQPS / 100))
	numReaders := int(1 + (*readQPS / 100))

	writtenDigests := make(chan *repb.Digest, 10000)
	readsPerWrite := int(math.Ceil(float64(*readQPS) / float64(*writeQPS)))

	writeQPSCounter := qps.NewCounter(*qpsAvgWindow)
	defer writeQPSCounter.Stop()
	readQPSCounter := qps.NewCounter(*qpsAvgWindow)
	defer readQPSCounter.Stop()

	writeLimiter := rate.NewLimiter(rate.Limit(*writeQPS), 1)
	readLimiter := rate.NewLimiter(rate.Limit(*readQPS), 1)

	eg.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-time.After(time.Second):
				log.Printf("Write: %.1f, Read: %.1f QPS (%s avg)", writeQPSCounter.Get(), readQPSCounter.Get(), *qpsAvgWindow)
			}
		}
	})

	for i := 0; i < numWriters; i++ {
		eg.Go(func() error {
			for {
				if err := writeLimiter.Wait(gctx); err != nil {
					return err
				}
				ctx, cancel := context.WithTimeout(gctx, *timeout)
				d, err := writeBlob(ctx, bsClient)
				cancel()
				if err != nil {
					log.Errorf("Write err: %s", err)
					if *keepGoing {
						return nil
					}
					return err
				}
				writeQPSCounter.Inc()

				if *readQPS == 0 {
					continue
				}

				select {
				case writtenDigests <- d:
					break
				default:
					log.Warningf("Digest Q is full, maybe increase read QPS")
				}
			}
		})
	}

	for i := 0; i < numReaders; i++ {
		eg.Go(func() error {
			for {
				var d *repb.Digest
				select {
				case d = <-writtenDigests:
					break
				case <-gctx.Done():
					return nil
				}

				for i := 0; i < readsPerWrite; i++ {
					if err := readLimiter.Wait(gctx); err != nil {
						return err
					}
					ctx, cancel := context.WithTimeout(gctx, *timeout)
					err := readBlob(ctx, bsClient, casClient, d)
					cancel()
					if err != nil {
						log.Errorf("Read err: %s", err)
						if *keepGoing {
							continue
						}
						return err
					}
					readQPSCounter.Inc()
				}
				if rand.Intn(10) < int(*recycleRate*10) {
					writtenDigests <- d
				}
			}
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatalf("Error during run: %s", err)
	}
}
