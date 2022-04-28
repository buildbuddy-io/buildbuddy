package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget  = flag.String("cache_target", "localhost:1985", "Cache target to connect to.")
	method       = flag.String("method", "google.bytestream.ByteStream/Write", "One of google.bytestream.ByteStream/{Read,Write}.")
	rps          = flag.Uint("rps", 1000, "How many requests per second to attempt.")
	testDuration = flag.Duration("test_duration", 10*time.Second, "The duration of the loadtest.")
	concurrency  = flag.Uint("concurrency", 10, "Number of concurrent workers to use")
	instanceName = flag.String("instance_name", "loadtest", "An optional Remote Instance name.")
	apiKey       = flag.String("api_key", "", "An optional API key to use when reading / writing data.")

	randomSeed         = flag.Int64("random_seed", 0, "Random seed.")
	realisticBlobSizes = flag.Bool("realistic_blob_sizes", true, "If true, use realistic blob sizes, ignoring blob_size flag.")
	ssl                = flag.Bool("ssl", false, "If true, use ssl.")
	blobSize           = flag.Int64("blob_size", 100000, "Num bytes (max) of blob to send/read.")
	htmlOutputFile     = flag.String("html_output_file", "", "If set, results will be written to this file in HTML format")
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

var (
	randomSrc         io.Reader
	mu                sync.Mutex
	preWrittenDigests []*repb.Digest
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

func randRange(low, high int) int64 {
	i := int64(rand.Intn(high-low+1) + low)
	return i
}

func randomBlobSize() int64 {
	if !*realisticBlobSizes {
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

func writeBlobsForReading() []*repb.Digest {
	log.Print("Pre-writing blobs for read test.")
	prefix := "grpc://"
	if *ssl {
		prefix = "grpcs://"
	}
	conn, err := grpc_client.DialTarget(prefix + *cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", *cacheTarget, err)
	}
	bsClient := bspb.NewByteStreamClient(conn)
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	digests := make([]*repb.Digest, 0)
	for i := 0; uint(i) < *concurrency; i++ {
		d, buf := newRandomDigestBuf(randomBlobSize())
		_, err := cachetools.UploadBlob(ctx, bsClient, *instanceName, bytes.NewReader(buf))
		if err != nil {
			log.Fatalf("Error pre-writing blob %q: %s", d.GetHash(), err)
		}
		digests = append(digests, d)
	}
	return digests
}

func newRandomDigestBuf(sizeBytes int64) (*repb.Digest, []byte) {
	buf := new(bytes.Buffer)
	mu.Lock()
	io.CopyN(buf, randomSrc, sizeBytes)
	mu.Unlock()
	readSeeker := bytes.NewReader(buf.Bytes())

	// Compute a digest for the random bytes.
	d, err := digest.Compute(readSeeker)
	if err != nil {
		log.Fatalf("Error computing digest: %s", err)
	}
	return d, buf.Bytes()
}

func writeDataFunc(mtd *desc.MethodDescriptor, cd *runner.CallData) []byte {
	d, buf := newRandomDigestBuf(randomBlobSize())
	resourceName, err := digest.NewResourceName(d, *instanceName).UploadString()
	if err != nil {
		log.Fatalf("Error computing upload resource name: %s", err)
	}
	wr := &bspb.WriteRequest{
		ResourceName: resourceName,
		WriteOffset:  0,
		Data:         buf,
		FinishWrite:  true,
	}
	binData, err := proto.Marshal(wr)
	if err != nil {
		log.Fatalf("Error marshalling write: %s", err)
	}
	return binData
}

func readDataFunc(mtd *desc.MethodDescriptor, cd *runner.CallData) []byte {
	randomDigest := preWrittenDigests[rand.Intn(len(preWrittenDigests))]

	resourceName := digest.NewResourceName(randomDigest, *instanceName).DownloadString()
	rr := &bspb.ReadRequest{
		ResourceName: resourceName,
		ReadOffset:   0,
		ReadLimit:    0,
	}
	binData, err := proto.Marshal(rr)
	if err != nil {
		log.Fatalf("Error marshalling read: %s", err)
	}
	return binData
}

func dataFunc(mtd *desc.MethodDescriptor, cd *runner.CallData) []byte {
	switch *method {
	case "google.bytestream.ByteStream/Read":
		return readDataFunc(mtd, cd)
	case "google.bytestream.ByteStream/Write":
		return writeDataFunc(mtd, cd)
	default:
		log.Fatalf("Unknown bytestream method: %q", *method)
	}
	return nil
}
func main() {
	flag.Parse()

	seed := *randomSeed
	if seed == 0 {
		seed = time.Now().Unix()
	}
	randomSrc = &randomDataMaker{rand.NewSource(seed)}

	// Figure out where our runfiles (static content bundled with the binary) live.
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		log.Fatalf("Error figuring out runfiles: %s", err)
	}
	protosetFile := filepath.Join(rfp, "/enterprise/server/cmd/smash/bspb.protoset")

	if *method == "google.bytestream.ByteStream/Read" {
		preWrittenDigests = writeBlobsForReading()
	}

	blobSizeDesc := fmt.Sprintf("size %d bytes", *blobSize)
	if *realisticBlobSizes {
		blobSizeDesc = "simulating real blob sizes."
	}

	md := make(map[string]string)
	if *apiKey != "" {
		md["x-buildbuddy-api-key"] = *apiKey
	}
	log.Printf("Running a %s test @ %d r/sec, concurrency: %d, %s", *testDuration, *rps, *concurrency, blobSizeDesc)
	report, err := runner.Run(
		*method,
		*cacheTarget,
		runner.WithConcurrency(*concurrency),
		runner.WithRPS(*rps),
		runner.WithRunDuration(*testDuration),
		runner.WithProtoset(protosetFile),
		runner.WithInsecure(!*ssl),
		runner.WithBinaryDataFunc(dataFunc),
		runner.WithMetadata(md),
	)

	if err != nil {
		log.Fatal(err.Error())
	}

	printer := printer.ReportPrinter{
		Out:    os.Stdout,
		Report: report,
	}
	printer.Print("summary")

	if *htmlOutputFile != "" {
		f, err := os.Create(*htmlOutputFile)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer f.Close()
		printer.Out = f
		if err := printer.Print("html"); err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("Wrote results to f: %+v", f.Name())
	}

}
