package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	target       = flag.String("target", "grpc://localhost:1970", "Cache target to connect to.")
	writeQPS     = flag.Uint("write_qps", 1000, "How many queries per second to attempt to write.")
	readQPS      = flag.Uint("read_qps", 1000, "How many queries per second to attempt to read.")
	instanceName = flag.String("instance_name", "loadtest", "An optional Remote Instance name.")
	apiKey       = flag.String("api_key", "", "An optional API key to use when reading / writing data.")
	qpsAvgWindow = flag.Duration("qps_avg_window", 5*time.Second, "QPS averaging window")

	blobSize       = flag.Int64("blob_size", 100, "Num bytes (max) of blob to send/read")
	recycleRate    = flag.Float64("recycle_rate", .10, "If true, re-queue digests for read after reading")
	timeout        = flag.Duration("timeout", 60*time.Second, "Use this timeout as the context timeout for rpc calls")
	keepGoing      = flag.Bool("keep_going", false, "If true, warn on errors but continue running")
	monitoringAddr = flag.String("listen", "", "The interface to listen on, like 0.0.0.0:9090 (default: disabled)")
	partitions     = flag.Slice("partitions", []Partition{}, "")
)

var (
	digestGenerator *digest.Generator
	mu              sync.Mutex
	filestorer      = filestore.New()
	partitionIDs    []string
	cumulatives     []int
)

const (
	methodLabel = "method"
)

var (
	MDLoadTotalErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "buildbuddy",
		Subsystem: "mdload",
		Name:      "total_error_count",
		Help:      "The total number of mdload errors, including errors in retry",
	}, []string{
		methodLabel,
		metrics.StatusHumanReadableLabel,
	})

	MDLoadFinalErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "buildbuddy",
		Subsystem: "mdload",
		Name:      "final_error_count",
		Help:      "The total number of mdload errors after retries",
	}, []string{
		methodLabel,
		metrics.StatusHumanReadableLabel,
	})
)

type Partition struct {
	ID      string `yaml:"id" json:"id" usage:"The ID of the partition."`
	Percent int    `yaml:"percent" json:"percent" usage:"The percentage of requests to route to this partition (0-100)"`
}

func newRandomDigestBuf(sizeBytes int64) (*repb.Digest, []byte) {
	d, buf, err := digestGenerator.RandomDigestBuf(sizeBytes)
	if err != nil {
		log.Fatalf("Error generating digest: %s", err)
	}
	return d, buf
}

func selectPartitionID() string {
	if len(partitionIDs) == 0 {
		return "default"
	}

	randNum := rand.Intn(100)
	for i, cumulative := range cumulatives {
		if randNum < cumulative {
			return partitionIDs[i]
		}
	}
	return partitionIDs[len(partitionIDs)-1]
}

func randomFileMetadata(sizeBytes int64) *sgpb.FileMetadata {
	d, buf := newRandomDigestBuf(sizeBytes)
	rn := digest.NewResourceName(d, *instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)

	iw := filestorer.InlineWriter(context.TODO(), int64(len(buf)))
	bytesWritten, err := io.Copy(iw, bytes.NewReader(buf))
	if err != nil {
		log.Fatalf("Error writing buffer: %s", err)
	}

	now := time.Now().UnixMicro()
	md := &sgpb.FileMetadata{
		FileRecord: &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:          rn.GetCacheType(),
				RemoteInstanceName: rn.GetInstanceName(),
				PartitionId:        selectPartitionID(),
				GroupId:            interfaces.AuthAnonymousUser,
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

func incrementPromTotalErrorMetric(method string, err error) {
	if err == nil {
		return
	}
	MDLoadTotalErrorCount.With(prometheus.Labels{
		methodLabel:                      method,
		metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
	}).Inc()
}

func incrementPromFinalErrorMetric(method string, err error) {
	if err == nil {
		return
	}
	MDLoadFinalErrorCount.With(prometheus.Labels{
		methodLabel:                      method,
		metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
	}).Inc()
}

func writeBlob(ctx context.Context, client mdspb.MetadataServiceClient) (*sgpb.FileRecord, error) {
	md := randomFileMetadata(*blobSize)
	return retry.Do(ctx, retry.DefaultOptions(), func(ctx context.Context) (*sgpb.FileRecord, error) {
		_, err := client.Set(ctx, &mdpb.SetRequest{
			SetOperations: []*mdpb.SetRequest_SetOperation{{
				FileMetadata: md,
			}},
		})
		incrementPromTotalErrorMetric("write", err)
		if err == nil {
			return md.GetFileRecord(), nil
		} else if status.IsUnavailableError(err) {
			return nil, err
		}
		return nil, retry.NonRetryableError(err)
	})
}

func readBlob(ctx context.Context, client mdspb.MetadataServiceClient, fr *sgpb.FileRecord) error {
	return retry.DoVoid(ctx, retry.DefaultOptions(), func(ctx context.Context) error {
		rsp, err := client.Get(ctx, &mdpb.GetRequest{
			FileRecords: []*sgpb.FileRecord{fr},
		})
		incrementPromTotalErrorMetric("read", err)
		if err == nil {
			if !proto.Equal(rsp.GetFileMetadatas()[0].GetFileRecord(), fr) {
				log.Fatalf("returned md did not match request")
			}
			return nil
		} else if status.IsUnavailableError(err) {
			return err
		}
		return retry.NonRetryableError(err)
	})
}

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}

	// Validate that partition percentages sum to 100 and build cumulative distribution
	if len(*partitions) > 0 {
		var totalPercent int
		for _, partition := range *partitions {
			totalPercent += partition.Percent
		}
		if totalPercent != 100 {
			log.Fatalf("Partition percentages must sum to 100, got %d", totalPercent)
		}

		// Pre-compute cumulative distribution for efficient partition selection
		partitionIDs = make([]string, len(*partitions))
		cumulatives = make([]int, len(*partitions))
		cumulative := 0
		for i, partition := range *partitions {
			partitionIDs[i] = partition.ID
			cumulative += partition.Percent
			cumulatives[i] = cumulative
		}
	}

	digestGenerator = digest.RandomGenerator(time.Now().Unix())
	env := real_environment.NewBatchEnv()
	ctx := context.Background()

	if *writeQPS == 0 {
		log.Fatalf("Write QPS cannot be 0 -- data must be written before it can be read")
	}
	blobSizeDesc := fmt.Sprintf("size %d bytes", *blobSize)

	log.Infof("MDLoad testing target %q", *target)
	log.Infof("Planned load W: %d / R: %d [QPS], blob size: %s", *writeQPS, *readQPS, blobSizeDesc)

	if *monitoringAddr != "" {
		monitoring.StartMonitoringHandler(env, *monitoringAddr)
	}

	conn, err := grpc_client.DialSimple(*target, grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		log.Fatalf("Unable to connect to target '%s': %s", *target, err)
	}
	log.Infof("Connected to target: %q", *target)

	mdClient := mdspb.NewMetadataServiceClient(conn)
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	eg, gctx := errgroup.WithContext(ctx)

	writtenDigests := make(chan *sgpb.FileRecord, 1_000_000)
	writeQPSCounter := qps.NewCounter(*qpsAvgWindow, clockwork.NewRealClock())
	defer writeQPSCounter.Stop()
	readQPSCounter := qps.NewCounter(*qpsAvgWindow, clockwork.NewRealClock())
	defer readQPSCounter.Stop()

	readsPerWrite := int(math.Ceil(float64(*readQPS) / float64(*writeQPS)))

	// Periodically print read and write QPS.
	eg.Go(func() error {
		log.Infof("Starting printer!")
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-gctx.Done():
				log.Errorf("exiting")
				return nil
			case <-ticker.C:
				log.Infof("Write: %.1f, Read: %.1f QPS (%s avg)", writeQPSCounter.Get(), readQPSCounter.Get(), *qpsAvgWindow)
			}
		}
	})

	writeOnce := func() {
		eg.Go(func() error {
			ctx, cancel := context.WithTimeout(gctx, *timeout)
			d, err := writeBlob(ctx, mdClient)
			cancel()
			if err != nil {
				log.Errorf("Write err: %s", err)
				incrementPromFinalErrorMetric("write", err)
				if *keepGoing {
					return nil
				}
				return err
			}
			writeQPSCounter.Inc()

			if *readQPS > 0 {
				for i := 0; i < readsPerWrite; i++ {
					select {
					case writtenDigests <- d:
					default:
					}
				}
			}
			return nil
		})
	}

	eg.Go(func() error {
		ticker := time.NewTicker(time.Second / time.Duration(*writeQPS))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				writeOnce()
			case <-gctx.Done():
				return nil
			}
		}
	})

	readOnce := func() {
		eg.Go(func() error {
			var d *sgpb.FileRecord
			select {
			case d = <-writtenDigests:
				break
			case <-gctx.Done():
				return nil
			}

			ctx, cancel := context.WithTimeout(gctx, *timeout)
			err := readBlob(ctx, mdClient, d)
			cancel()
			if err != nil {
				log.Errorf("Read err: %s", err)
				incrementPromFinalErrorMetric("read", err)
				if *keepGoing {
					return nil
				}
				return err
			}
			readQPSCounter.Inc()
			if rand.Intn(10) < int(*recycleRate*10) {
				writtenDigests <- d
			}
			return nil
		})
	}

	eg.Go(func() error {
		if *readQPS <= 0 {
			return nil
		}

		ticker := time.NewTicker(time.Second / time.Duration(*readQPS))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				readOnce()
			case <-gctx.Done():
				return nil
			}
		}
	})

	if err := eg.Wait(); err != nil {
		log.Fatalf("Error during run: %s", err)
	}
}
