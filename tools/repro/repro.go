package main

import (
	"context"
	"flag"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget = flag.String("cache_target", "localhost:1985", "Cache target to connect to.")

	concurrency    = flag.Int("concurrency", 10, "Number of concurrent workers to use")
	sizeBytes      = flag.Int64("size_bytes", 1e8, "Size of blob to upload")
	instanceName   = flag.String("instance_name", "loadtest", "An optional Remote Instance name.")
	randomSeed     = flag.Int64("random_seed", 0, "Random seed.")
	headers        = flagtypes.Slice("headers", []string{}, "A list of headers to set (format: 'key=val'")
	dialPerThread  = flag.Bool("dial_per_thread", false, "Whether to create a separate connection per worker")
	reportInterval = flag.Duration("report_interval", 5*time.Second, "How often to print perf stats.")
)

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val & 0xff)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}
}

func streamBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, cb func(n int)) error {
	req := &bspb.ReadRequest{
		ResourceName: r.DownloadString(),
	}
	stream, err := bsClient.Read(ctx, req)
	if err != nil {
		if gstatus.Code(err) == gcodes.NotFound {
			return digest.MissingDigestError(r.GetDigest())
		}
		return err
	}

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		cb(len(rsp.Data))
	}
	return nil
}

type worker struct {
	mu       sync.Mutex
	received int
	total    int
}

func (w *worker) Start(ctx context.Context, r *digest.ResourceName, quitChan chan struct{}, bsClient bspb.ByteStreamClient) error {
	for {
		bsClient := bsClient
		if *dialPerThread {
			conn, err := grpc_client.DialTarget(*cacheTarget)
			if err != nil {
				log.Fatalf("Error dialing: %s", err)
			}
			bsClient = bspb.NewByteStreamClient(conn)
		}
		select {
		case <-quitChan:
			return nil
		default:
		}
		err := streamBlob(ctx, bsClient, r, func(n int) {
			w.mu.Lock()
			w.received += n
			w.total += n
			w.mu.Unlock()
		})
		if err != nil {
			return err
		}
	}
}

func main() {
	flag.Parse()

	seed := *randomSeed
	if seed == 0 {
		seed = time.Now().Unix()
	}
	randomSrc := &randomDataMaker{rand.NewSource(seed)}
	ctx := context.Background()

	headersToSet := make([]string, 0)
	for _, header := range *headers {
		pair := strings.SplitN(header, "=", 2)
		if len(pair) != 2 {
			log.Fatalf("Target headers must be of form key=val, got: %q", *headers)
		}
		headersToSet = append(headersToSet, pair[0])
		headersToSet = append(headersToSet, pair[1])
	}
	if len(headersToSet) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, headersToSet...)
	}

	conn, err := grpc_client.DialTarget(*cacheTarget)
	if err != nil {
		log.Fatalf("Error dialing: %s", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)

	f, err := os.CreateTemp("", "example")
	if err != nil {
		log.Fatalf("Error making tempdir: %s", err)
	}
	defer func() {
		os.Remove(f.Name())
		log.Printf("Cleaned up file %q", f.Name())
	}()

	n, err := io.CopyN(f, randomSrc, *sizeBytes)
	if err != nil {
		log.Fatalf("Error copying bytes to file: %s", err)
	}
	if err := f.Close(); err != nil {
		log.Fatalf("Error closing file: %s", err)
	}
	log.Printf("Wrote %d random bytes to temp file: %q", n, f.Name())

	d, err := cachetools.UploadFile(ctx, bsClient, *instanceName, f.Name())
	if err != nil {
		log.Fatalf("Error uploading file: %s", err)
	}
	log.Printf("Uploaded %q to cache. Digest %s/%d", f.Name(), d.GetHash(), d.GetSizeBytes())

	quitChan := make(chan struct{})
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		close(quitChan)
	}()

	var workers []*worker
	r := digest.NewResourceName(d, *instanceName)
	eg := errgroup.Group{}
	for i := 0; i < *concurrency; i++ {
		worker := &worker{}
		workers = append(workers, worker)
		eg.Go(func() error {
			return worker.Start(ctx, r, quitChan, bsClient)
		})
	}
	eg.Go(func() error {
		for {
			start := time.Now()
			time.Sleep(*reportInterval)
			dur := time.Since(start)
			tot := 0
			for i, w := range workers {
				w.mu.Lock()
				tot += w.received
				tput := (float64(w.received) / 1e6) / float64(dur.Seconds())
				w.received = 0
				workerTotal := float64(w.total) / 1e6
				w.mu.Unlock()
				log.Infof("Worker %d throughput %f MB/sec (total transferred: %f MB)", i, tput, workerTotal)
			}
			tput := (float64(tot) / 1e6) / float64(dur.Seconds())
			log.Infof("Total throughput %f MB/sec)", tput)
		}
	})
	if err := eg.Wait(); err != nil {
		log.Fatalf("WaitGroup err: %s", err)
	}
}
