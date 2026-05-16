// bazelcache is a prober that tests remote cache services by writing and reading random bytes.
// This prober tests ByteStream, ActionCache, and ContentAddressableStorage services.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cdc"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget       = flag.String("cache_target", "", "Cache grpc target (required)")
	instanceName      = flag.String("instance_name", "", "Remote instance name")
	apiKey            = flag.String("api_key", "", "API key for authentication")
	blobSize          = flag.Int64("blob_size", 100_000, "Size of test blobs in bytes")
	chunkedBlobSizes  byteSizeList
	testProxyChunking = flag.Bool(
		"test_proxy_chunking",
		false,
		"If true, verify large ByteStream writes are chunked by the target. Intended for cache proxy targets.")

	conn      *grpc_client.ClientConnPool
	ctx       context.Context
	bsClient  bspb.ByteStreamClient
	acClient  repb.ActionCacheClient
	casClient repb.ContentAddressableStorageClient
	capClient repb.CapabilitiesClient
)

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

type byteSizeList []int64

func (l *byteSizeList) String() string {
	parts := make([]string, 0, len(*l))
	for _, size := range *l {
		parts = append(parts, strconv.FormatInt(size, 10))
	}
	return strings.Join(parts, ",")
}

func (l *byteSizeList) Set(value string) error {
	for part := range strings.SplitSeq(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		size, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return err
		}
		if size < 0 {
			return fmt.Errorf("byte size must be >= 0, got %d", size)
		}
		if size == 0 {
			continue
		}
		*l = append(*l, size)
	}
	return nil
}

func init() {
	flag.Var(&chunkedBlobSizes, "chunked_blob_size", "Size of a blob to upload and download using chunking APIs. May be repeated or comma-separated. If omitted, chunking API tests are skipped.")
}

func fastCDCParams(ctx context.Context) *repb.FastCdc2020Params {
	rsp, err := capClient.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{InstanceName: *instanceName})
	assertNoError(err, "failed to get cache capabilities")
	params := rsp.GetCacheCapabilities().GetFastCdc_2020Params()
	assertTrue(params.GetAvgChunkSizeBytes() > 0, "server did not advertise FastCDC params")
	return params
}

func TestByteStream() {
	digestGenerator := digest.RandomGenerator(time.Now().UnixNano())
	d, buf, err := digestGenerator.RandomDigestBuf(*blobSize)
	assertNoError(err, "failed to generate random data")

	for _, compressor := range []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD} {
		resourceName := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
		resourceName.SetCompressor(compressor)

		reader := bytes.NewReader(buf)
		_, _, err := cachetools.UploadFromReader(ctx, bsClient, resourceName, reader)
		assertNoError(err, "failed to upload blob")

		var downloadBuf bytes.Buffer
		err = cachetools.GetBlob(ctx, bsClient, resourceName, &downloadBuf)
		assertNoError(err, "failed to download blob")

		assertEqual(buf, downloadBuf.Bytes(), "downloaded data doesn't match uploaded data")
	}
}

func makeChunkedBlob(sizeBytes int64) []byte {
	buf := make([]byte, sizeBytes)
	_, err := rand.Read(buf)
	assertNoError(err, "failed to generate chunked blob")
	return buf
}

func uploadBlobChunked(ctx context.Context, rn *digest.CASResourceName, buf []byte, params *repb.FastCdc2020Params) {
	chunkSizeBytes := max(3*1024*1024, 3*int64(params.GetAvgChunkSizeBytes()))
	assertTrue(chunkSizeBytes > 0, "chunk size must be positive")
	assertTrue(int64(len(buf)) > chunkSizeBytes, "chunked blob size is too small to split into multiple chunks")

	var chunkDigests []*repb.Digest
	for offset := int64(0); offset < int64(len(buf)); offset += chunkSizeBytes {
		end := min(offset+chunkSizeBytes, int64(len(buf)))
		d, err := digest.Compute(bytes.NewReader(buf[offset:end]), rn.GetDigestFunction())
		assertNoError(err, "failed to compute chunk digest")
		chunkDigests = append(chunkDigests, d)
	}
	assertTrue(len(chunkDigests) > 1, "expected chunked blob to split into multiple chunks")

	manifest := &chunking.Manifest{
		BlobDigest:     rn.GetDigest(),
		ChunkDigests:   chunkDigests,
		InstanceName:   rn.GetInstanceName(),
		DigestFunction: rn.GetDigestFunction(),
	}

	// These RPCs operate on chunks, not whole blobs. Tag them so proxies and apps
	// do not try to treat the chunk digests as independently chunkable blobs.
	chunkCtx := cdc.ContextWithChunked(ctx)
	missingResp, err := cachetools.FindMissingBlobs(chunkCtx, casClient, manifest.ToFindMissingBlobsRequest())
	assertNoError(err, "failed to find missing chunked blob chunks")
	missing := make(map[digest.Key]struct{}, len(missingResp.GetMissingBlobDigests()))
	for _, d := range missingResp.GetMissingBlobDigests() {
		missing[digest.NewKey(d)] = struct{}{}
	}

	var offset int64
	for _, d := range chunkDigests {
		if _, ok := missing[digest.NewKey(d)]; ok {
			chunkRN := digest.NewCASResourceName(d, rn.GetInstanceName(), rn.GetDigestFunction())
			chunkRN.SetCompressor(repb.Compressor_ZSTD)
			chunk := bytes.NewReader(buf[offset : offset+d.GetSizeBytes()])
			_, _, err := cachetools.UploadFromReader(chunkCtx, bsClient, chunkRN, chunk)
			assertNoError(err, "failed to upload chunked blob chunk")
		}
		offset += d.GetSizeBytes()
	}

	_, err = casClient.SpliceBlob(ctx, manifest.ToSpliceBlobRequest())
	assertNoError(err, "failed to splice chunked blob")
}

func verifyChunkedBlob(ctx context.Context, rn *digest.CASResourceName, buf []byte) {
	splitResp, err := casClient.SplitBlob(ctx, &repb.SplitBlobRequest{
		BlobDigest:     rn.GetDigest(),
		InstanceName:   rn.GetInstanceName(),
		DigestFunction: rn.GetDigestFunction(),
	})
	assertNoError(err, "failed to split chunked blob")
	assertTrue(len(splitResp.GetChunkDigests()) > 1, "expected chunked blob to split into multiple chunks")

	f, err := os.CreateTemp("", "bazelcache-prober-chunked-*")
	assertNoError(err, "failed to create chunked download temp file")
	defer os.Remove(f.Name())
	defer f.Close()
	err = cachetools.GetBlobChunked(ctx, bsClient, casClient, rn, nil /*=parent*/, f, nil /*=openLocal*/)
	assertNoError(err, "failed to download chunked blob via SplitBlob")
	_, err = f.Seek(0, io.SeekStart)
	assertNoError(err, "failed to rewind chunked download temp file")
	downloadBuf, err := io.ReadAll(f)
	assertNoError(err, "failed to read chunked download temp file")
	assertEqual(buf, downloadBuf, "SplitBlob downloaded data doesn't match uploaded data")

	var bytestreamDownload bytes.Buffer
	err = cachetools.GetBlob(ctx, bsClient, rn, &bytestreamDownload)
	assertNoError(err, "failed to download chunked blob via ByteStream")
	assertEqual(buf, bytestreamDownload.Bytes(), "ByteStream downloaded data doesn't match uploaded data")
}

func TestClientChunking(sizeBytes int64, params *repb.FastCdc2020Params) {
	buf := makeChunkedBlob(sizeBytes)
	d, err := digest.Compute(bytes.NewReader(buf), repb.DigestFunction_SHA256)
	assertNoError(err, "failed to compute chunked blob digest")
	assertTrue(
		chunking.ShouldUploadChunkedWithMax(d, int64(params.GetAvgChunkSizeBytes()), params.GetBuildbuddyMaxChunkedWriteSizeBytes()),
		"chunked blob size is below the chunking threshold",
	)
	rn := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
	rn.SetCompressor(repb.Compressor_ZSTD)

	uploadBlobChunked(ctx, rn, buf, params)
	verifyChunkedBlob(ctx, rn, buf)
}

func TestProxyChunking(sizeBytes int64, params *repb.FastCdc2020Params) {
	buf := makeChunkedBlob(sizeBytes)
	d, err := digest.Compute(bytes.NewReader(buf), repb.DigestFunction_SHA256)
	assertNoError(err, "failed to compute proxy chunked blob digest")
	assertTrue(
		chunking.ShouldUploadChunkedWithMax(d, int64(params.GetAvgChunkSizeBytes()), params.GetBuildbuddyMaxChunkedWriteSizeBytes()),
		"proxy chunked blob size is below the chunking threshold",
	)
	rn := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
	rn.SetCompressor(repb.Compressor_ZSTD)

	_, _, err = cachetools.UploadFromReader(ctx, bsClient, rn, bytes.NewReader(buf))
	assertNoError(err, "failed to upload proxy chunked blob")
	verifyChunkedBlob(ctx, rn, buf)
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

	for _, compressor := range []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD} {
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
		_, err := casClient.FindMissingBlobs(ctx, findReq)
		assertNoError(err, "failed to find missing blobs")

		var requests []*repb.BatchUpdateBlobsRequest_Request
		for i, d := range digests {
			data := blobs[i]
			if compressor == repb.Compressor_ZSTD {
				data = compression.CompressZstd(nil, blobs[i])
			}
			requests = append(requests, &repb.BatchUpdateBlobsRequest_Request{
				Digest:     d,
				Data:       data,
				Compressor: compressor,
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

		findResp, err := casClient.FindMissingBlobs(ctx, findReq)
		assertNoError(err, "failed to find missing blobs after upload")
		assertEqual(0, len(findResp.GetMissingBlobDigests()), "expected 0 missing blobs after upload")

		batchReadReq := &repb.BatchReadBlobsRequest{
			InstanceName:          *instanceName,
			DigestFunction:        repb.DigestFunction_SHA256,
			Digests:               digests,
			AcceptableCompressors: []repb.Compressor_Value{compressor},
		}

		batchReadResp, err := cachetools.BatchReadBlobs(ctx, casClient, batchReadReq)
		assertNoError(err, "failed to batch read blobs")
		assertEqual(numBlobs, len(batchReadResp), "unexpected number of responses")

		for _, resp := range batchReadResp {
			assertNoError(resp.Err, "blob download failed")

			expectedData, ok := expectedBlobs[resp.Digest.GetHash()]
			assertTrue(ok, "unexpected blob")
			assertEqual(expectedData, resp.Data, "blob data mismatch")
		}
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
	capClient = repb.NewCapabilitiesClient(conn)

	ctx = context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	TestByteStream()
	TestActionCache()
	TestCAS()
	if len(chunkedBlobSizes) > 0 {
		params := fastCDCParams(ctx)
		for _, sizeBytes := range chunkedBlobSizes {
			TestClientChunking(sizeBytes, params)
			if *testProxyChunking {
				TestProxyChunking(sizeBytes, params)
			}
		}
	}
}
