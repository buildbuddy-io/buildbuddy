// bazelcache is a prober that tests remote cache services by writing and reading random bytes.
// This prober tests ByteStream, ActionCache, and ContentAddressableStorage services.
package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget  = flag.String("cache_target", "", "Cache grpc target (required)")
	instanceName = flag.String("instance_name", "", "Remote instance name")
	apiKey       = flag.String("api_key", "", "API key for authentication")
	blobSize     = flag.Int64("blob_size", 100_000, "Size of test blobs in bytes")

	// Test infrastructure
	conn      *grpc_client.ClientConnPool
	ctx       context.Context
	bsClient  bspb.ByteStreamClient
	acClient  repb.ActionCacheClient
	casClient repb.ContentAddressableStorageClient
)

func TestByteStream(t *testing.T) {
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	d, buf, err := digestGenerator.RandomDigestBuf(*blobSize)
	require.NoError(t, err, "failed to generate random data")

	scenarios := []struct {
		name       string
		compressor repb.Compressor_Value
	}{
		{"uncompressed", repb.Compressor_IDENTITY},
		{"zstd", repb.Compressor_ZSTD},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			resourceName := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
			resourceName.SetCompressor(scenario.compressor)

			reader := bytes.NewReader(buf)
			_, bytesUploaded, err := cachetools.UploadFromReader(ctx, bsClient, resourceName, reader)
			require.NoError(t, err, "failed to upload blob")

			var downloadBuf bytes.Buffer
			err = cachetools.GetBlob(ctx, bsClient, resourceName, &downloadBuf)
			require.NoError(t, err, "failed to download blob")

			require.Equal(t, buf, downloadBuf.Bytes(), "downloaded data doesn't match uploaded data")
		})
	}
}

func TestActionCache(t *testing.T) {
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	actionDigest, _, err := digestGenerator.RandomDigestBuf(100)
	require.NoError(t, err, "failed to generate random digest")

	actionResult := &repb.ActionResult{
		ExitCode:  0,
		StdoutRaw: []byte("test stdout"),
		StderrRaw: []byte("test stderr"),
	}

	updateReq := &repb.UpdateActionResultRequest{
		InstanceName:   *instanceName,
		ActionDigest:   actionDigest,
		ActionResult:   actionResult,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err = acClient.UpdateActionResult(ctx, updateReq)
	require.NoError(t, err, "failed to update action result")

	getReq := &repb.GetActionResultRequest{
		ActionDigest:   actionDigest,
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	retrievedResult, err := acClient.GetActionResult(ctx, getReq)
	require.NoError(t, err, "failed to get action result")

	require.Equal(t, actionResult.GetExitCode(), retrievedResult.GetExitCode(), "exit code mismatch")
	require.Equal(t, actionResult.GetStdoutRaw(), retrievedResult.GetStdoutRaw(), "stdout mismatch")
	require.Equal(t, actionResult.GetStderrRaw(), retrievedResult.GetStderrRaw(), "stderr mismatch")
}

func TestCAS(t *testing.T) {
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	numBlobs := 3

	scenarios := []struct {
		name       string
		compressor repb.Compressor_Value
	}{
		{"uncompressed", repb.Compressor_IDENTITY},
		{"zstd", repb.Compressor_ZSTD},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			var digests []*repb.Digest
			var blobs [][]byte
			for i := 0; i < numBlobs; i++ {
				d, buf, err := digestGenerator.RandomDigestBuf(1024)
				require.NoError(t, err, "failed to generate random data")
				digests = append(digests, d)
				blobs = append(blobs, buf)
			}

			expectedBlobs := make(map[string][]byte, numBlobs)
			for i, d := range digests {
				expectedBlobs[d.GetHash()] = blobs[i]
			}

			findReq := &repb.FindMissingBlobsRequest{
				InstanceName:   *instanceName,
				DigestFunction: repb.DigestFunction_SHA256,
				BlobDigests:    digests,
			}
			findResp, err := casClient.FindMissingBlobs(ctx, findReq)
			require.NoError(t, err, "failed to find missing blobs")

			var requests []*repb.BatchUpdateBlobsRequest_Request
			for i, d := range digests {
				data := blobs[i]
				if scenario.compressor != repb.Compressor_IDENTITY {
					data = compression.CompressZstd(nil, blobs[i])
				}
				requests = append(requests, &repb.BatchUpdateBlobsRequest_Request{
					Digest:     d,
					Data:       data,
					Compressor: scenario.compressor,
				})
			}

			batchUpdateReq := &repb.BatchUpdateBlobsRequest{
				InstanceName:   *instanceName,
				DigestFunction: repb.DigestFunction_SHA256,
				Requests:       requests,
			}
			batchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, batchUpdateReq)
			require.NoError(t, err, "failed to batch update blobs")

			for _, resp := range batchUpdateResp.GetResponses() {
				require.Equal(t, int32(0), resp.GetStatus().GetCode(), "blob upload failed: %s", resp.GetStatus().GetMessage())
			}

			findResp, err = casClient.FindMissingBlobs(ctx, findReq)
			require.NoError(t, err, "failed to find missing blobs after upload")
			require.Equal(t, 0, len(findResp.GetMissingBlobDigests()), "expected 0 missing blobs after upload")

			batchReadReq := &repb.BatchReadBlobsRequest{
				InstanceName:   *instanceName,
				DigestFunction: repb.DigestFunction_SHA256,
				Digests:        digests,
			}
			if scenario.compressor != repb.Compressor_IDENTITY {
				batchReadReq.AcceptableCompressors = []repb.Compressor_Value{
					repb.Compressor_IDENTITY,
					scenario.compressor,
				}
			}

			batchReadResp, err := cachetools.BatchReadBlobs(ctx, casClient, batchReadReq)
			require.NoError(t, err, "failed to batch read blobs")
			require.Equal(t, numBlobs, len(batchReadResp), "unexpected number of responses")

			// Verify data integrity
			for _, resp := range batchReadResp {
				require.NoError(t, resp.Err, "blob download failed")

				expectedData, ok := expectedBlobs[resp.Digest.GetHash()]
				require.True(t, ok, "unexpected blob with hash %s", resp.Digest.GetHash())
				require.Equal(t, expectedData, resp.Data, "blob %s data mismatch", resp.Digest.GetHash())
			}
		})
	}
}

func TestMain(m *testing.M) {
	if *cacheTarget == "" {
		log.Fatalf("--cache_target is required")
	}

	log.Infof("Connecting to cache at %s", *cacheTarget)
	var err error
	conn, err = grpc_client.DialSimple(*cacheTarget)
	if err != nil {
		log.Fatalf("failed to connect to cache: %s", err)
	}
	defer conn.Close()

	bsClient = bspb.NewByteStreamClient(conn)
	acClient = repb.NewActionCacheClient(conn)
	casClient = repb.NewContentAddressableStorageClient(conn)

	ctx = context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	os.Exit(m.Run())
}

func main() {
	flag.Parse()

	m := testing.MainStart(&testDeps{}, []testing.InternalTest{
		{Name: "TestByteStream", F: TestByteStream},
		{Name: "TestActionCache", F: TestActionCache},
		{Name: "TestCAS", F: TestCAS},
	}, nil, nil, nil)

	TestMain(m)
}

// testDeps implements testing.testDeps interface
type testDeps struct{}

func (testDeps) MatchString(pat, str string) (bool, error)       { return true, nil }
func (testDeps) StartCPUProfile(w io.Writer) error               { return nil }
func (testDeps) StopCPUProfile()                                 {}
func (testDeps) WriteProfileTo(string, io.Writer, int) error     { return nil }
func (testDeps) ImportPath() string                              { return "" }
func (testDeps) StartTestLog(io.Writer)                          {}
func (testDeps) StopTestLog() error                              { return nil }
func (testDeps) SetPanicOnExit0(bool)                            {}
func (testDeps) CheckCorpus([]any, []reflect.Type) error         { return nil }
func (testDeps) ResetCoverage()                                  {}
func (testDeps) SnapshotCoverage()                               {}
func (testDeps) InitRuntimeCoverage() (mode string, tearDown func(string, string) (string, error), snapcov func() float64) {
	return
}
func (testDeps) CoordinateFuzzing(time.Duration, int64, time.Duration, int64, int, []corpusEntry, []reflect.Type, string, string) error {
	return nil
}
func (testDeps) RunFuzzWorker(func(corpusEntry) error) error { return nil }
func (testDeps) ReadCorpus(string, []reflect.Type) ([]corpusEntry, error) {
	return nil, nil
}

type corpusEntry = struct {
	Parent     string
	Path       string
	Data       []byte
	Values     []any
	Generation int
	IsSeed     bool
}
