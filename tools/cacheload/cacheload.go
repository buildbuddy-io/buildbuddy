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
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget  = flag.String("cache_target", "localhost:1985", "Cache target to connect to.")
	writeQPS     = flag.Uint("write_qps", 100, "How many queries per second to attempt to write.")
	readQPS      = flag.Uint("read_qps", 1000, "How many queries per second to attempt to read.")
	instanceName = flag.String("instance_name", "loadtest", "An optional Remote Instance name.")
	apiKey       = flag.String("api_key", "", "An optional API key to use when reading / writing data.")

	blobSize    = flag.Int64("blob_size", -1, "Num bytes (max) of blob to send/read. If -1, realistic blob sizes are used.")
	recycleRate = flag.Float64("recycle_rate", .10, "If true, re-queue digests for read after reading")
	timeout     = flag.Duration("timeout", 10*time.Second, "Use this timeout as the context timeout for rpc calls")
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
)

func init() {
	for _, c := range histCounts {
		histCountsTotal += c
	}
}

type Counter struct {
	c uint64
}

func (c *Counter) Get() uint64 {
	return atomic.LoadUint64(&c.c)
}
func (c *Counter) Inc() uint64 {
	return atomic.AddUint64(&c.c, 1)
}

func randRange(low, high int) int64 {
	i := int64(rand.Intn(high-low+1) + low)
	return i
}

func randomBlobSize() int64 {
	if *blobSize > 0 {
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

func writeBlob(ctx context.Context, client bspb.ByteStreamClient) (*repb.Digest, error) {
	d, buf := newRandomDigestBuf(randomBlobSize())
	retrier := retry.DefaultWithContext(ctx)
	var err error
	for retrier.Next() {
		_, err = cachetools.UploadBlob(ctx, client, *instanceName, bytes.NewReader(buf))
		if err == nil {
			return d, nil
		} else if status.IsUnavailableError(err) {
			continue
		}
		return nil, err
	}
	return nil, status.InternalErrorf("Error writing digest: %s/%d: %s", d.GetHash(), d.GetSizeBytes(), err)
}

func readBlob(ctx context.Context, client bspb.ByteStreamClient, d *repb.Digest) error {
	resourceName := digest.NewResourceName(d, *instanceName)
	if err := cachetools.GetBlob(ctx, client, resourceName, io.Discard); err != nil {
		return status.InternalErrorf("Error reading digest: %s/%d: %s", resourceName.GetDigest().GetHash(), resourceName.GetDigest().GetSizeBytes(), err)
	}
	return nil
}

func main() {
	flag.Parse()

	digestGenerator = digest.RandomGenerator(time.Now().Unix())
	ctx := context.Background()

	blobSizeDesc := fmt.Sprintf("size %d bytes", *blobSize)
	if *blobSize < 0 {
		blobSizeDesc = "simulating real blob sizes."
	}
	log.Printf("Applying load to cache W: %d / R: %d [QPS], blob size: %s", *writeQPS, *readQPS, blobSizeDesc)

	conn, err := grpc_client.DialTargetWithOptions(*cacheTarget, false, grpc.WithBlock(), grpc.WithTimeout(*timeout))
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", *cacheTarget, err)
	}
	log.Printf("Connected to cache: %q", *cacheTarget)

	bsClient := bspb.NewByteStreamClient(conn)
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	eg, gctx := errgroup.WithContext(ctx)

	numWriters := int(1 + (*writeQPS / 100))
	numReaders := int(1 + (*readQPS / 100))

	writtenDigests := make(chan *repb.Digest, 10000)
	readsPerWrite := int(math.Ceil(float64(*readQPS) / float64(*writeQPS)))

	writeQPSCounter := &Counter{}
	readQPSCounter := &Counter{}

	writeLimiter := rate.NewLimiter(rate.Limit(*writeQPS), 1)
	readLimiter := rate.NewLimiter(rate.Limit(*readQPS), 1)

	eg.Go(func() error {
		lastWriteCount := writeQPSCounter.Get()
		lastReadCount := readQPSCounter.Get()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-time.After(time.Second):
				writeCount := writeQPSCounter.Get()
				readCount := readQPSCounter.Get()
				log.Printf("Write: %d, Read: %d QPS", writeCount-lastWriteCount, readCount-lastReadCount)
				lastWriteCount = writeCount
				lastReadCount = readCount
			}
		}
		return nil
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
					return err
				}
				writeQPSCounter.Inc()

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
				d := <-writtenDigests
				for i := 0; i < readsPerWrite; i++ {
					if err := readLimiter.Wait(gctx); err != nil {
						return err
					}
					ctx, cancel := context.WithTimeout(gctx, *timeout)
					err := readBlob(ctx, bsClient, d)
					cancel()
					if err != nil {
						log.Errorf("Read err: %s", err)
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
