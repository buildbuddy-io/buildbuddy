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
	"sort"
	"sync"
	"time"

	"github.com/DataDog/zstd"
	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"google.golang.org/grpc/metadata"

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

	ssl            = flag.Bool("ssl", false, "If true, use ssl.")
	htmlOutputFile = flag.String("html_output_file", "", "If set, results will be written to this file in HTML format")

	// NOTE: Blobs in blob_dir are not expected to change while this tool is
	// running, so be sure not to run against a local server if this is pointed at
	// a local cache dir (make a copy of your local cache dir instead). Only
	// top-level regular files in blob_dir are considered.
	//
	// If blob_dir is unspecified, random blobs are used instead.

	blobDir       = flag.String("blob_dir", "", "If set, specifies a directory containing blobs to use as test data.")
	blobSaltBytes = flag.Int("blob_salt_bytes", 4, "Replace this many bytes at the end of every blob in blob_dir with random bytes, to avoid cache hits.")
	compressBlobs = flag.Bool("compress_blobs", false, "If set, compress the blobs in blob_dir with zstd before uploading, and decompress when reading.")
	showBlobStats = flag.Bool("show_blob_stats", false, "If set, compute blob stats for the blobs in blob_dir.")

	// "Random" mode options (with realistic lengths)

	randomSeed         = flag.Int64("random_seed", 0, "Random seed.")
	realisticBlobSizes = flag.Bool("realistic_blob_sizes", true, "If true, use realistic blob sizes, ignoring blob_size flag.")
	blobSize           = flag.Int64("blob_size", 100000, "Num bytes (max) of blob to send/read.")
)

var (
	// blobs read from blobDir, if set.
	blobs []*blobFile
)

type blobFile struct {
	Digest  *repb.Digest
	RelPath string
}

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

func getCompression() repb.Compressor_Value {
	if *compressBlobs {
		return repb.Compressor_ZSTD
	}
	return repb.Compressor_IDENTITY
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
		blob := randomBlob()
		_, err := cachetools.UploadPreCompressedFromReader(ctx, bsClient, blob.ResourceName, bytes.NewReader(blob.Data))
		if err != nil {
			log.Fatalf("Error pre-writing blob %q: %s", blob.ResourceName.GetHash(), err)
		}
		digests = append(digests, blob.ResourceName.Digest)
	}
	return digests
}

type Blob struct {
	// ResourceName uniquely identifies the blob in the cache.
	ResourceName *digest.ResourceName
	// Data is the compressed data.
	Data []byte
}

func randomBlob() *Blob {
	if len(blobs) == 0 {
		d, b := newRandomDigestBuf(randomBlobSize())
		return &Blob{
			ResourceName: &digest.ResourceName{
				InstanceNameDigest: digest.NewInstanceNameDigest(d, *instanceName),
				Compression:        repb.Compressor_IDENTITY,
			},
			Data: b,
		}
	}

	blob := blobs[rand.Intn(len(blobs))]
	f, err := os.Open(filepath.Join(*blobDir, blob.RelPath))
	if err != nil {
		log.Fatalf("Failed to open %q: %s", f.Name(), err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("Failed to read %q: %s", f.Name(), err)
	}
	d := blob.Digest
	// Apply salt if needed and re-compute digest
	if *blobSaltBytes > 0 {
		saltLen := *blobSaltBytes
		if saltLen > len(b) {
			saltLen = len(b)
		}
		salt := b[len(b)-saltLen:]
		_, err := randomSrc.Read(salt)
		if err != nil {
			log.Fatalf("Failed to salt")
		}
		d, err = digest.Compute(bytes.NewReader(b))
		if err != nil {
			log.Fatalf("Failed to compute post-salt digest: %s", err)
		}
	}
	// Apply compression but keep uncompressed digest (per protocol)
	compression := repb.Compressor_IDENTITY
	if *compressBlobs {
		compressedData, err := zstd.Compress(nil, b)
		if err != nil {
			log.Fatalf("Failed to compress blob: %s", err)
		}
		b = compressedData
		compression = repb.Compressor_ZSTD
	}

	return &Blob{
		ResourceName: &digest.ResourceName{
			InstanceNameDigest: digest.NewInstanceNameDigest(d, *instanceName),
			Compression:        compression,
		},
		Data: b,
	}
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
	blob := randomBlob()
	uploadName, err := digest.UploadResourceName(blob.ResourceName)
	if err != nil {
		log.Fatalf("Error computing upload resource name: %s", err)
	}
	wr := &bspb.WriteRequest{
		ResourceName: uploadName,
		WriteOffset:  0,
		Data:         blob.Data,
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

	resourceName := digest.DownloadResourceName(
		&digest.ResourceName{
			InstanceNameDigest: digest.NewInstanceNameDigest(randomDigest, *instanceName),
			Compression:        getCompression(),
		},
	)
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

func streamRecvFunc(msg *dynamic.Message, err error) error {
	if err != nil {
		return err
	}
	if !*compressBlobs {
		return nil
	}

	// Decompress read responses to simulate decompression costs when reading
	// from cache.
	switch *method {
	case "google.bytestream.ByteStream/Read":
		res := &bspb.ReadResponse{}
		if err := msg.ConvertTo(res); err != nil {
			return err
		}
		_, err := zstd.Decompress(nil, res.Data)
		return err
	default:
		return nil
	}
}

// readBlobDir reads the blob dir and returns a list of blob files that are
// directly underneath it.
func readBlobDir(path string) []*blobFile {
	entries, err := os.ReadDir(path)
	if err != nil {
		log.Fatalf("Failed to read blob dir: %s", err)
	}
	out := []*blobFile{}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			// Don't recurse into dirs or follow symlinks.
			continue
		}
		f, err := os.Open(filepath.Join(path, entry.Name()))
		if err != nil {
			log.Fatalf("Failed to open blob %q: %s", entry.Name(), err)
		}
		defer f.Close()
		d, err := digest.Compute(f)
		if err != nil {
			log.Fatalf("Failed to compute digest for file %q", f.Name())
		}
		out = append(out, &blobFile{Digest: d, RelPath: entry.Name()})
	}
	return out
}

func printBlobStats(blobs []*blobFile) {
	compressionRatios := make([]float64, len(blobs))
	maxSize := int64(0)
	maxCompressedSize := int64(0)
	totalSize := int64(0)
	totalCompressedSize := int64(0)

	for i, blob := range blobs {
		f, err := os.Open(filepath.Join(*blobDir, blob.RelPath))
		if err != nil {
			log.Fatalf("Failed to open blob file: %s", f.Name())
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			log.Fatalf("Failed to read blob file: %s", f.Name())
		}
		b, err = zstd.Compress(nil, b)
		if err != nil {
			log.Fatalf("Failed to compress blob: %s", err)
		}

		compressionRatios[i] = float64(len(b)) / float64(blob.Digest.SizeBytes)
		totalSize += blob.Digest.SizeBytes
		if blob.Digest.SizeBytes > maxSize {
			maxSize = blob.Digest.SizeBytes
		}
		totalCompressedSize += int64(len(b))
		if int64(len(b)) > maxCompressedSize {
			maxCompressedSize = int64(len(b))
		}
	}
	fmt.Printf("Blob dir stats:\n")
	fmt.Printf("  Total size:         %d MB\n", totalSize/1e6)
	fmt.Printf("  Total size (compr): %d MB\n", totalCompressedSize/1e6)
	fmt.Printf("  Max size:           %d MB\n", maxSize/1e6)
	fmt.Printf("  Max size (compr):   %d MB\n", maxCompressedSize/1e6)
	fmt.Printf("  Compression ratio quantiles:\n")
	sort.Slice(compressionRatios, func(i, j int) bool {
		return compressionRatios[i] < compressionRatios[j]
	})
	for _, q := range []float64{0, 0.05, 0.10, 0.15, 0.2, 0.25, 0.3, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.9, 0.95, 0.99, 0.999} {
		i := int(q * float64(len(compressionRatios)))
		fmt.Printf("    q=%.3f:  r=%.2f%%\n", q, compressionRatios[i]*100)
	}
}

func main() {
	flag.Parse()
	monitoring.StartMonitoringHandler("localhost:8888")

	seed := *randomSeed
	if seed == 0 {
		seed = time.Now().Unix()
	}
	randomSrc = &randomDataMaker{rand.NewSource(seed)}

	if *blobDir != "" {
		log.Infof("Reading blobs from %q ...", *blobDir)
		blobs = readBlobDir(*blobDir)
		if len(blobs) == 0 {
			log.Fatalf("No blobs found in blob dir %q", *blobDir)
		}
		log.Infof("Computing blob stats ...")
		if *showBlobStats {
			printBlobStats(blobs)
		}
	}

	// Figure out where our runfiles (static content bundled with the binary) live.
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		log.Fatalf("Error figuring out runfiles: %s", err)
	}
	protosetFile := filepath.Join(rfp, "/enterprise/server/cmd/smash/bspb.protoset")

	if *method == "google.bytestream.ByteStream/Read" {
		preWrittenDigests = writeBlobsForReading()
	}

	blobsDesc := fmt.Sprintf("random blobs of fixed size %d bytes", *blobSize)
	if *realisticBlobSizes {
		blobsDesc = "random blobs, simulating real blob sizes"
	}
	if len(blobs) != 0 {
		blobsDesc = fmt.Sprintf("using blobs from %q", *blobDir)
	}

	md := make(map[string]string)
	if *apiKey != "" {
		md["x-buildbuddy-api-key"] = *apiKey
	}
	log.Printf("Running a %s test @ %d r/sec, concurrency: %d, %s", *testDuration, *rps, *concurrency, blobsDesc)
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
		// Make workers decompress the responses if they are compressed, for a more
		// fair simulation in the case of compressed blobs. Note that this is not
		// included in the reported gRPC latency, but does affect E2E throughput,
		// since workers cannot move on to the next task until they run their stream
		// interceptor.
		runner.WithStreamRecvMsgIntercept(streamRecvFunc),
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
