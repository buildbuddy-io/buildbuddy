// bazelcache is a prober that tests remote cache services by writing and reading random bytes.
// This prober tests ByteStream, ActionCache, and ContentAddressableStorage services.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget  = flag.String("cache_target", "", "Cache grpc target (required)")
	instanceName = flag.String("instance_name", "", "Remote instance name")
	apiKey       = flag.String("api_key", "", "API key for authentication")
	blobSize     = flag.Int64("blob_size", 100_000, "Size of test blobs in bytes")
)

func verifyBlobIntegrity(downloaded, expected []byte) error {
	if !bytes.Equal(downloaded, expected) {
		return status.DataLossError("downloaded data doesn't match uploaded data")
	}
	return nil
}

func verifyBatchUploadSuccess(responses []*repb.BatchUpdateBlobsResponse_Response) error {
	for _, resp := range responses {
		if resp.GetStatus().GetCode() != 0 {
			return status.UnknownErrorf("blob upload failed: %s", resp.GetStatus().GetMessage())
		}
	}
	return nil
}

func generateTestBlobs(gen *digest.Generator, count int, size int64) ([]*repb.Digest, [][]byte, error) {
	var digests []*repb.Digest
	var blobs [][]byte

	for i := 0; i < count; i++ {
		d, buf, err := gen.RandomDigestBuf(size)
		if err != nil {
			return nil, nil, status.UnknownErrorf("failed to generate random data: %s", err)
		}
		digests = append(digests, d)
		blobs = append(blobs, buf)
	}
	return digests, blobs, nil
}

func probeByteStream(ctx context.Context, bsClient bspb.ByteStreamClient) error {
	log.Infof("[ByteStream] Starting probe...")

	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	d, buf, err := digestGenerator.RandomDigestBuf(*blobSize)
	if err != nil {
		return status.UnknownErrorf("failed to generate random data: %s", err)
	}

	scenarios := []struct {
		name       string
		compressor repb.Compressor_Value
	}{
		{"uncompressed", repb.Compressor_IDENTITY},
		{"zstd compressed", repb.Compressor_ZSTD},
	}

	for _, scenario := range scenarios {
		log.Infof("[ByteStream] Testing %s...", scenario.name)

		resourceName := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
		resourceName.SetCompressor(scenario.compressor)

		log.Infof("[ByteStream] Uploading %d bytes (digest: %s/%d)", len(buf), d.GetHash(), d.GetSizeBytes())
		reader := bytes.NewReader(buf)
		_, bytesUploaded, err := cachetools.UploadFromReader(ctx, bsClient, resourceName, reader)
		if err != nil {
			return status.UnknownErrorf("failed to upload %s blob: %s", scenario.name, err)
		}
		log.Infof("[ByteStream] Upload successful: %d bytes", bytesUploaded)

		log.Infof("[ByteStream] Downloading blob...")
		var downloadBuf bytes.Buffer
		if err := cachetools.GetBlob(ctx, bsClient, resourceName, &downloadBuf); err != nil {
			return status.UnknownErrorf("failed to download %s blob: %s", scenario.name, err)
		}

		if err := verifyBlobIntegrity(downloadBuf.Bytes(), buf); err != nil {
			return status.UnknownErrorf("%s: %s", scenario.name, err)
		}
		log.Infof("[ByteStream] Download successful and verified: %d bytes", downloadBuf.Len())
	}

	return nil
}

func probeActionCache(ctx context.Context, acClient repb.ActionCacheClient) error {
	log.Infof("[ActionCache] Starting probe...")

	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	actionDigest, _, err := digestGenerator.RandomDigestBuf(100)
	if err != nil {
		return status.UnknownErrorf("failed to generate random digest: %s", err)
	}

	actionResult := &repb.ActionResult{
		ExitCode:  0,
		StdoutRaw: []byte("test stdout"),
		StderrRaw: []byte("test stderr"),
	}

	log.Infof("[ActionCache] Storing ActionResult (action digest: %s/%d)", actionDigest.GetHash(), actionDigest.GetSizeBytes())
	updateReq := &repb.UpdateActionResultRequest{
		InstanceName:   *instanceName,
		ActionDigest:   actionDigest,
		ActionResult:   actionResult,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err = acClient.UpdateActionResult(ctx, updateReq)
	if err != nil {
		return status.UnknownErrorf("failed to update action result: %s", err)
	}
	log.Infof("[ActionCache] ActionResult stored successfully")

	log.Infof("[ActionCache] Retrieving ActionResult...")
	getReq := &repb.GetActionResultRequest{
		ActionDigest:   actionDigest,
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	retrievedResult, err := acClient.GetActionResult(ctx, getReq)
	if err != nil {
		return status.UnknownErrorf("failed to get action result: %s", err)
	}

	if retrievedResult.GetExitCode() != actionResult.GetExitCode() {
		return status.DataLossError("exit code mismatch")
	}
	if !bytes.Equal(retrievedResult.GetStdoutRaw(), actionResult.GetStdoutRaw()) {
		return status.DataLossError("stdout mismatch")
	}
	if !bytes.Equal(retrievedResult.GetStderrRaw(), actionResult.GetStderrRaw()) {
		return status.DataLossError("stderr mismatch")
	}
	log.Infof("[ActionCache] ActionResult retrieved and verified successfully")

	return nil
}

func testCASBatchOperations(ctx context.Context, casClient repb.ContentAddressableStorageClient, gen *digest.Generator, numBlobs int, compressor repb.Compressor_Value) error {
	compressorName := "uncompressed"
	if compressor != repb.Compressor_IDENTITY {
		compressorName = compressor.String()
	}

	digests, blobs, err := generateTestBlobs(gen, numBlobs, 1024)
	if err != nil {
		return err
	}
	log.Infof("[CAS] Generated %d test blobs for %s test", numBlobs, compressorName)

	expectedBlobs := make(map[string][]byte, numBlobs)
	for i, d := range digests {
		expectedBlobs[d.GetHash()] = blobs[i]
	}

	log.Infof("[CAS] Checking for missing blobs (%s)...", compressorName)
	findReq := &repb.FindMissingBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		BlobDigests:    digests,
	}
	findResp, err := casClient.FindMissingBlobs(ctx, findReq)
	if err != nil {
		return status.UnknownErrorf("failed to find missing blobs: %s", err)
	}
	log.Infof("[CAS] Missing blobs before upload: %d", len(findResp.GetMissingBlobDigests()))

	var requests []*repb.BatchUpdateBlobsRequest_Request
	for i, d := range digests {
		data := blobs[i]
		if compressor != repb.Compressor_IDENTITY {
			data = compression.CompressZstd(nil, blobs[i])
		}
		requests = append(requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     d,
			Data:       data,
			Compressor: compressor,
		})
	}

	log.Infof("[CAS] Uploading blobs via BatchUpdateBlobs (%s)...", compressorName)
	batchUpdateReq := &repb.BatchUpdateBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		Requests:       requests,
	}
	batchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, batchUpdateReq)
	if err != nil {
		return status.UnknownErrorf("failed to batch update blobs: %s", err)
	}

	// Check all uploads succeeded
	if err := verifyBatchUploadSuccess(batchUpdateResp.GetResponses()); err != nil {
		return err
	}
	log.Infof("[CAS] Uploaded %d blobs successfully", len(batchUpdateResp.GetResponses()))

	// Verify upload with FindMissingBlobs (should report none missing)
	log.Infof("[CAS] Verifying blobs are no longer missing...")
	findResp, err = casClient.FindMissingBlobs(ctx, findReq)
	if err != nil {
		return status.UnknownErrorf("failed to find missing blobs after upload: %s", err)
	}
	if len(findResp.GetMissingBlobDigests()) != 0 {
		return status.DataLossError(fmt.Sprintf("expected 0 missing blobs after upload, got %d", len(findResp.GetMissingBlobDigests())))
	}
	log.Infof("[CAS] All blobs present after upload")

	// Download using BatchReadBlobs
	log.Infof("[CAS] Downloading blobs via BatchReadBlobs (%s)...", compressorName)
	batchReadReq := &repb.BatchReadBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		Digests:        digests,
	}
	if compressor != repb.Compressor_IDENTITY {
		batchReadReq.AcceptableCompressors = []repb.Compressor_Value{
			repb.Compressor_IDENTITY,
			compressor,
		}
	}

	batchReadResp, err := cachetools.BatchReadBlobs(ctx, casClient, batchReadReq)
	if err != nil {
		return status.UnknownErrorf("failed to batch read blobs: %s", err)
	}

	if len(batchReadResp) != numBlobs {
		return status.DataLossError(fmt.Sprintf("expected %d responses, got %d", numBlobs, len(batchReadResp)))
	}

	for _, resp := range batchReadResp {
		if resp.Err != nil {
			return status.UnknownErrorf("blob download failed: %s", resp.Err)
		}

		expectedData, ok := expectedBlobs[resp.Digest.GetHash()]
		if !ok {
			return status.DataLossError(fmt.Sprintf("unexpected blob with hash %s", resp.Digest.GetHash()))
		}

		if err := verifyBlobIntegrity(resp.Data, expectedData); err != nil {
			return status.DataLossError(fmt.Sprintf("blob %s: %s", resp.Digest.GetHash(), err))
		}
	}
	log.Infof("[CAS] Downloaded and verified %d blobs successfully (%s)", numBlobs, compressorName)

	return nil
}

func probeCAS(ctx context.Context, casClient repb.ContentAddressableStorageClient) error {
	log.Infof("[CAS] Starting probe...")

	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	numBlobs := 3

	if err := testCASBatchOperations(ctx, casClient, digestGenerator, numBlobs, repb.Compressor_IDENTITY); err != nil {
		return err
	}

	log.Infof("[CAS] Testing with zstd compression...")
	if err := testCASBatchOperations(ctx, casClient, digestGenerator, numBlobs, repb.Compressor_ZSTD); err != nil {
		return err
	}

	return nil
}

func runProbe() error {
	if *cacheTarget == "" {
		return status.InvalidArgumentError("--cache_target is required")
	}

	log.Infof("Connecting to cache at %s", *cacheTarget)
	conn, err := grpc_client.DialSimple(*cacheTarget)
	if err != nil {
		return status.UnavailableErrorf("failed to connect to cache: %s", err)
	}
	defer conn.Close()

	bsClient := bspb.NewByteStreamClient(conn)
	acClient := repb.NewActionCacheClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	if err := probeByteStream(ctx, bsClient); err != nil {
		return err
	}

	if err := probeActionCache(ctx, acClient); err != nil {
		return err
	}

	if err := probeCAS(ctx, casClient); err != nil {
		return err
	}

	log.Infof("All probes passed successfully!")
	return nil
}

func main() {
	flag.Parse()

	if err := runProbe(); err != nil {
		log.Fatalf("Probe failed: %s", err)
	}
}
