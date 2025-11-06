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

func probeByteStream(ctx context.Context, bsClient bspb.ByteStreamClient) error {
	log.Infof("[ByteStream] Starting probe...")

	// Generate random test data
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	d, buf, err := digestGenerator.RandomDigestBuf(*blobSize)
	if err != nil {
		return status.UnknownErrorf("failed to generate random data: %s", err)
	}

	// Upload via ByteStream
	log.Infof("[ByteStream] Uploading %d bytes (digest: %s/%d)", len(buf), d.GetHash(), d.GetSizeBytes())
	resourceName := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
	reader := bytes.NewReader(buf)

	_, bytesUploaded, err := cachetools.UploadFromReader(ctx, bsClient, resourceName, reader)
	if err != nil {
		return status.UnknownErrorf("failed to upload blob: %s", err)
	}
	log.Infof("[ByteStream] Upload successful: %d bytes", bytesUploaded)

	// Download via ByteStream
	log.Infof("[ByteStream] Downloading blob...")
	var downloadBuf bytes.Buffer
	err = cachetools.GetBlob(ctx, bsClient, resourceName, &downloadBuf)
	if err != nil {
		return status.UnknownErrorf("failed to download blob: %s", err)
	}

	// Verify data integrity
	if !bytes.Equal(buf, downloadBuf.Bytes()) {
		return status.DataLossError("downloaded data doesn't match uploaded data")
	}
	log.Infof("[ByteStream] Download successful and verified: %d bytes", downloadBuf.Len())

	// Test with zstd compression
	log.Infof("[ByteStream] Testing with zstd compression...")
	compressedResourceName := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
	compressedResourceName.SetCompressor(repb.Compressor_ZSTD)

	// Upload with compression
	log.Infof("[ByteStream] Uploading %d bytes with zstd compression (digest: %s/%d)", len(buf), d.GetHash(), d.GetSizeBytes())
	compressedReader := bytes.NewReader(buf)
	_, compressedBytesUploaded, err := cachetools.UploadFromReader(ctx, bsClient, compressedResourceName, compressedReader)
	if err != nil {
		return status.UnknownErrorf("failed to upload compressed blob: %s", err)
	}
	log.Infof("[ByteStream] Compressed upload successful: %d bytes sent", compressedBytesUploaded)

	// Download with compression
	log.Infof("[ByteStream] Downloading compressed blob...")
	var compressedDownloadBuf bytes.Buffer
	err = cachetools.GetBlob(ctx, bsClient, compressedResourceName, &compressedDownloadBuf)
	if err != nil {
		return status.UnknownErrorf("failed to download compressed blob: %s", err)
	}

	// Verify data integrity (cachetools.GetBlob auto-decompresses for us)
	if !bytes.Equal(buf, compressedDownloadBuf.Bytes()) {
		return status.DataLossError("compressed round-trip data doesn't match original data")
	}
	log.Infof("[ByteStream] Compressed download successful and verified: %d bytes", compressedDownloadBuf.Len())

	return nil
}

func probeActionCache(ctx context.Context, acClient repb.ActionCacheClient) error {
	log.Infof("[ActionCache] Starting probe...")

	// Generate a random action digest
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	actionDigest, _, err := digestGenerator.RandomDigestBuf(100)
	if err != nil {
		return status.UnknownErrorf("failed to generate random digest: %s", err)
	}

	// Create a simple ActionResult
	actionResult := &repb.ActionResult{
		ExitCode:  0,
		StdoutRaw: []byte("test stdout"),
		StderrRaw: []byte("test stderr"),
	}

	// Store ActionResult
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

	// Retrieve ActionResult
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

	// Verify the retrieved result
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

func probeCAS(ctx context.Context, casClient repb.ContentAddressableStorageClient) error {
	log.Infof("[CAS] Starting probe...")

	// Generate 3 small random blobs for batch operations
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	numBlobs := 3
	var digests []*repb.Digest
	var blobs [][]byte

	for i := 0; i < numBlobs; i++ {
		d, buf, err := digestGenerator.RandomDigestBuf(1024) // 1KB each
		if err != nil {
			return status.UnknownErrorf("failed to generate random data: %s", err)
		}
		digests = append(digests, d)
		blobs = append(blobs, buf)
	}
	log.Infof("[CAS] Generated %d test blobs", numBlobs)

	// Build map of digest hash -> original payload for verification
	expectedBlobs := make(map[string][]byte, numBlobs)
	for i, digest := range digests {
		expectedBlobs[digest.GetHash()] = blobs[i]
	}

	// Test FindMissingBlobs (should report all as missing initially)
	log.Infof("[CAS] Checking for missing blobs...")
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

	// Upload using BatchUpdateBlobs
	log.Infof("[CAS] Uploading blobs via BatchUpdateBlobs...")
	var requests []*repb.BatchUpdateBlobsRequest_Request
	for i, d := range digests {
		requests = append(requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest: d,
			Data:   blobs[i],
		})
	}

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
	for _, resp := range batchUpdateResp.GetResponses() {
		if resp.GetStatus().GetCode() != 0 {
			return status.UnknownErrorf("blob upload failed: %s", resp.GetStatus().GetMessage())
		}
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
	log.Infof("[CAS] Downloading blobs via BatchReadBlobs...")
	batchReadReq := &repb.BatchReadBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		Digests:        digests,
	}

	batchReadResp, err := cachetools.BatchReadBlobs(ctx, casClient, batchReadReq)
	if err != nil {
		return status.UnknownErrorf("failed to batch read blobs: %s", err)
	}

	if len(batchReadResp) != numBlobs {
		return status.DataLossError(fmt.Sprintf("expected %d responses, got %d", numBlobs, len(batchReadResp)))
	}

	// Verify data integrity for each blob using digest hash as key
	for _, resp := range batchReadResp {
		if resp.Err != nil {
			return status.UnknownErrorf("blob download failed: %s", resp.Err)
		}

		expectedData, ok := expectedBlobs[resp.Digest.GetHash()]
		if !ok {
			return status.DataLossError(fmt.Sprintf("unexpected blob with hash %s", resp.Digest.GetHash()))
		}

		if !bytes.Equal(resp.Data, expectedData) {
			return status.DataLossError(fmt.Sprintf("blob %s data mismatch", resp.Digest.GetHash()))
		}
	}
	log.Infof("[CAS] Downloaded and verified %d blobs successfully", numBlobs)

	// Test with zstd compression
	log.Infof("[CAS] Testing with zstd compression...")

	// Generate new test blobs for compression test
	var compressedDigests []*repb.Digest
	var compressedBlobs [][]byte
	var compressedBlobData [][]byte

	for i := 0; i < numBlobs; i++ {
		d, buf, err := digestGenerator.RandomDigestBuf(1024) // 1KB each
		if err != nil {
			return status.UnknownErrorf("failed to generate random data: %s", err)
		}
		compressedBuf := compression.CompressZstd(nil, buf)
		compressedDigests = append(compressedDigests, d)
		compressedBlobs = append(compressedBlobs, buf)                 // Original uncompressed data
		compressedBlobData = append(compressedBlobData, compressedBuf) // Compressed data
	}
	log.Infof("[CAS] Generated %d test blobs for compression", numBlobs)

	// Upload using BatchUpdateBlobs with compression
	log.Infof("[CAS] Uploading compressed blobs via BatchUpdateBlobs...")
	var compressedRequests []*repb.BatchUpdateBlobsRequest_Request
	for i, d := range compressedDigests {
		compressedRequests = append(compressedRequests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     d,
			Data:       compressedBlobData[i],
			Compressor: repb.Compressor_ZSTD,
		})
	}

	compressedBatchUpdateReq := &repb.BatchUpdateBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		Requests:       compressedRequests,
	}
	compressedBatchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, compressedBatchUpdateReq)
	if err != nil {
		return status.UnknownErrorf("failed to batch update compressed blobs: %s", err)
	}

	// Check all uploads succeeded
	for _, resp := range compressedBatchUpdateResp.GetResponses() {
		if resp.GetStatus().GetCode() != 0 {
			return status.UnknownErrorf("compressed blob upload failed: %s", resp.GetStatus().GetMessage())
		}
	}
	log.Infof("[CAS] Uploaded %d compressed blobs successfully", len(compressedBatchUpdateResp.GetResponses()))

	// Download using BatchReadBlobs with compression support
	log.Infof("[CAS] Downloading compressed blobs via BatchReadBlobs...")
	compressedBatchReadReq := &repb.BatchReadBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		Digests:        compressedDigests,
		AcceptableCompressors: []repb.Compressor_Value{
			repb.Compressor_IDENTITY,
			repb.Compressor_ZSTD,
		},
	}

	compressedBatchReadResp, err := cachetools.BatchReadBlobs(ctx, casClient, compressedBatchReadReq)
	if err != nil {
		return status.UnknownErrorf("failed to batch read compressed blobs: %s", err)
	}

	if len(compressedBatchReadResp) != numBlobs {
		return status.DataLossError(fmt.Sprintf("expected %d compressed responses, got %d", numBlobs, len(compressedBatchReadResp)))
	}

	// Verify data integrity using digest hash as key (cachetools.BatchReadBlobs auto-decompresses)
	expectedCompressedBlobs := make(map[string][]byte, numBlobs)
	for i, digest := range compressedDigests {
		expectedCompressedBlobs[digest.GetHash()] = compressedBlobs[i]
	}

	for _, resp := range compressedBatchReadResp {
		if resp.Err != nil {
			return status.UnknownErrorf("compressed blob download failed: %s", resp.Err)
		}

		expectedData, ok := expectedCompressedBlobs[resp.Digest.GetHash()]
		if !ok {
			return status.DataLossError(fmt.Sprintf("unexpected compressed blob with hash %s", resp.Digest.GetHash()))
		}

		if !bytes.Equal(resp.Data, expectedData) {
			return status.DataLossError(fmt.Sprintf("compressed blob %s data mismatch", resp.Digest.GetHash()))
		}
	}
	log.Infof("[CAS] Downloaded and verified %d compressed blobs successfully", numBlobs)

	return nil
}

func runProbe() error {
	if *cacheTarget == "" {
		return status.InvalidArgumentError("--cache_target is required")
	}

	// Connect to cache
	log.Infof("Connecting to cache at %s", *cacheTarget)
	conn, err := grpc_client.DialSimple(*cacheTarget)
	if err != nil {
		return status.UnavailableErrorf("failed to connect to cache: %s", err)
	}
	defer conn.Close()

	// Create clients
	bsClient := bspb.NewByteStreamClient(conn)
	acClient := repb.NewActionCacheClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)

	// Setup context with API key if provided
	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	// Run probes
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

	err := runProbe()
	if err != nil {
		log.Fatalf("Probe failed: %s", err)
	}
}
