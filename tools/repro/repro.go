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
	"google.golang.org/grpc/metadata"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"	
)

var (
	cacheTarget  = flag.String("cache_target", "localhost:1985", "Cache target to connect to.")

	concurrency  = flag.Int("concurrency", 10, "Number of concurrent workers to use")
	sizeBytes    = flag.Int64("size_bytes", 1e8, "Size of blob to upload")
	instanceName = flag.String("instance_name", "loadtest", "An optional Remote Instance name.")
	randomSeed         = flag.Int64("random_seed", 0, "Random seed.")
	headers            = flagtypes.Slice("headers", []string{}, "A list of headers to set (format: 'key=val'")
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
	go func(){
		<-c
		close(quitChan)
	}()

	mu := sync.Mutex{}
	r := digest.NewResourceName(d, *instanceName)
	eg := errgroup.Group{}
	for i := 0; i < *concurrency; i++ {
		i := i
		eg.Go(func() error {
			for {
				select {
				case <-quitChan:
					return nil
				default:
				}
				start := time.Now()
				if err := cachetools.GetBlob(ctx, bsClient, r, io.Discard); err != nil {
					return err
				}
				mu.Lock()

				dur := time.Since(start)
				tput := (float64(d.GetSizeBytes())/1e6) / float64(dur.Seconds())
				log.Printf("Thread %d downloaded %d bytes in %s (%f MB/sec)", i, d.GetSizeBytes(), dur, tput)
				mu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		log.Fatalf("WaitGroup err: %s", err)
	}
}

