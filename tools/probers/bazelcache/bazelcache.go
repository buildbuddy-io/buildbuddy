// bazelcache is a prober that tests remote cache services by writing and reading random bytes.
// This prober tests ByteStream, ActionCache, and ContentAddressableStorage services.
package main

import (
	"bytes"
	"context"
	"flag"
	"reflect"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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

// Assertion helpers

func assertNoError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func assertEqual(expected, actual interface{}, msg string) {
	if !reflect.DeepEqual(expected, actual) {
		log.Fatalf("%s: values not equal", msg)
	}
}

func assertTrue(condition bool, msg string) {
	if !condition {
		log.Fatalf("%s", msg)
	}
}

func TestByteStream() {
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	d, buf, err := digestGenerator.RandomDigestBuf(*blobSize)
	assertNoError(err, "failed to generate random data")

	scenarios := []struct {
		name       string
		compressor repb.Compressor_Value
	}{
		{"uncompressed", repb.Compressor_IDENTITY},
		{"zstd", repb.Compressor_ZSTD},
	}

	for _, scenario := range scenarios {
		log.Infof("[ByteStream] Testing %s", scenario.name)

		resourceName := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
		resourceName.SetCompressor(scenario.compressor)

		reader := bytes.NewReader(buf)
		_, _, err := cachetools.UploadFromReader(ctx, bsClient, resourceName, reader)
		assertNoError(err, "failed to upload blob")

		var downloadBuf bytes.Buffer
		err = cachetools.GetBlob(ctx, bsClient, resourceName, &downloadBuf)
		assertNoError(err, "failed to download blob")

		assertEqual(buf, downloadBuf.Bytes(), "downloaded data doesn't match uploaded data")
		log.Infof("[ByteStream] %s: PASS", scenario.name)
	}
}

func TestActionCache() {
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	actionDigest, _, err := digestGenerator.RandomDigestBuf(100)
	assertNoError(err, "failed to generate random digest")

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
	assertNoError(err, "failed to update action result")

	getReq := &repb.GetActionResultRequest{
		ActionDigest:   actionDigest,
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	retrievedResult, err := acClient.GetActionResult(ctx, getReq)
	assertNoError(err, "failed to get action result")

	assertEqual(actionResult.GetExitCode(), retrievedResult.GetExitCode(), "exit code mismatch")
	assertEqual(actionResult.GetStdoutRaw(), retrievedResult.GetStdoutRaw(), "stdout mismatch")
	assertEqual(actionResult.GetStderrRaw(), retrievedResult.GetStderrRaw(), "stderr mismatch")
}

func TestCAS() {
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
		log.Infof("[CAS] Testing %s", scenario.name)

		var digests []*repb.Digest
		var blobs [][]byte
		for i := 0; i < numBlobs; i++ {
			d, buf, err := digestGenerator.RandomDigestBuf(1024)
			assertNoError(err, "failed to generate random data")
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
		assertNoError(err, "failed to find missing blobs")
		_ = findResp // Used later

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
		assertNoError(err, "failed to batch update blobs")

		for _, resp := range batchUpdateResp.GetResponses() {
			assertEqual(int32(0), resp.GetStatus().GetCode(), "blob upload failed")
		}

		findResp, err = casClient.FindMissingBlobs(ctx, findReq)
		assertNoError(err, "failed to find missing blobs after upload")
		assertEqual(0, len(findResp.GetMissingBlobDigests()), "expected 0 missing blobs after upload")

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
		assertNoError(err, "failed to batch read blobs")
		assertEqual(numBlobs, len(batchReadResp), "unexpected number of responses")

		// Verify data integrity
		for _, resp := range batchReadResp {
			assertNoError(resp.Err, "blob download failed")

			expectedData, ok := expectedBlobs[resp.Digest.GetHash()]
			assertTrue(ok, "unexpected blob")
			assertEqual(expectedData, resp.Data, "blob data mismatch")
		}

		log.Infof("[CAS] %s: PASS", scenario.name)
	}
}

func main() {
	flag.Parse()

	if *cacheTarget == "" {
		log.Fatalf("--cache_target is required")
	}

	// Setup connection and clients
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

	// Run tests
	TestByteStream()
	log.Infof("TestByteStream: PASS")

	TestActionCache()
	log.Infof("TestActionCache: PASS")

	TestCAS()
	log.Infof("TestCAS: PASS")

	log.Infof("All tests passed")
}
