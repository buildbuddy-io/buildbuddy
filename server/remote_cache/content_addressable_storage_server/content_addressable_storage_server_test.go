package content_addressable_storage_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/cas"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/cdc"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/fastcdc2020/fastcdc"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bspb "github.com/buildbuddy-io/buildbuddy/proto/bytestream"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func runCASServer(ctx context.Context, t *testing.T, env *testenv.TestEnv, configure ...func(*content_addressable_storage_server.ContentAddressableStorageServer)) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	for _, f := range configure {
		f(casServer)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(t, ctx, lis)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

type evilCache struct {
	interfaces.Cache
}

func (e *evilCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	rsp, err := e.Cache.GetMulti(ctx, resources)
	for d := range rsp {
		rsp[d] = []byte{}
	}
	return rsp, err
}

type casCompressionCache struct {
	interfaces.Cache
}

func (c *casCompressionCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	if r.GetCacheType() != rspb.CacheType_CAS {
		return c.Cache.Get(ctx, r)
	}
	return (&testcompression.CompressionCache{Cache: c.Cache}).Get(ctx, r)
}

func (c *casCompressionCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(resources))
	for _, r := range resources {
		data, err := c.Get(ctx, r)
		if status.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		foundMap[r.GetDigest()] = data
	}
	return foundMap, nil
}

func (c *casCompressionCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	switch compressor {
	case repb.Compressor_IDENTITY, repb.Compressor_ZSTD:
		return true
	default:
		return false
	}
}

func TestBatchUpdateBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	var digests []*repb.Digest
	req := &repb.BatchUpdateBlobsRequest{}
	for range 100 {
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest: rn.GetDigest(),
			Data:   buf,
		})
		digests = append(digests, rn.GetDigest())
	}
	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 100, len(rsp.GetResponses()))
	for _, singleRsp := range rsp.GetResponses() {
		assert.Equal(t, int32(gcodes.OK), singleRsp.GetStatus().GetCode())
	}

	digests = append(digests[:10], append([]*repb.Digest{{Hash: digest.EmptySha256}}, digests[10:]...)...)
	readReq := &repb.BatchReadBlobsRequest{
		Digests: digests,
	}
	_, err = casClient.BatchReadBlobs(ctx, readReq)
	require.NoError(t, err)
}

func TestBlackholeAnonymousCacheRequests(t *testing.T) {
	flags.Set(t, "auth.blackhole_anonymous_cache_requests", false)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	te.SetAuthenticator(auth)
	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)
	bsClient := bspb.NewByteStreamClient(clientConn)

	storedRN, storedData := testdigest.RandomCASResourceBuf(t, 10)
	_, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: storedRN.GetDigest(), Data: storedData}},
	})
	require.NoError(t, err)

	flags.Set(t, "auth.blackhole_anonymous_cache_requests", true)
	emptyDigest := &repb.Digest{Hash: digest.EmptySha256}
	findRsp, err := casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{storedRN.GetDigest(), emptyDigest},
	})
	require.NoError(t, err)
	require.Equal(t, digestStrings(storedRN.GetDigest()), digestStrings(findRsp.GetMissingBlobDigests()...))

	readRsp, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{storedRN.GetDigest(), emptyDigest},
	})
	require.NoError(t, err)
	require.Len(t, readRsp.GetResponses(), 2)
	require.Equal(t, int32(gcodes.NotFound), readRsp.GetResponses()[0].GetStatus().GetCode())
	require.Equal(t, int32(gcodes.OK), readRsp.GetResponses()[1].GetStatus().GetCode())

	batchWriteRN, batchWriteData := testdigest.RandomCASResourceBuf(t, 10)
	updateRsp, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: batchWriteRN.GetDigest(), Data: batchWriteData}},
	})
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.OK), updateRsp.GetResponses()[0].GetStatus().GetCode())

	readStream, err := bsClient.Read(ctx, &bspb.ReadRequest{ResourceName: digest.NewCASResourceName(storedRN.GetDigest(), "", repb.DigestFunction_SHA256).DownloadString()})
	require.NoError(t, err)
	_, err = readStream.Recv()
	require.True(t, status.IsNotFoundError(err), "expected cache miss, got %v", err)

	byteStreamWriteDigest, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte("discard me"))
	require.NoError(t, err)
	_, err = bsClient.QueryWriteStatus(ctx, &bspb.QueryWriteStatusRequest{
		ResourceName: digest.NewCASResourceName(byteStreamWriteDigest, "", repb.DigestFunction_SHA256).NewUploadString(),
	})
	require.True(t, status.IsNotFoundError(err), "expected missing upload, got %v", err)

	treeStream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{RootDigest: storedRN.GetDigest(), DigestFunction: repb.DigestFunction_SHA256})
	require.NoError(t, err)
	_, err = treeStream.Recv()
	require.True(t, status.IsNotFoundError(err), "expected cache miss, got %v", err)

	_, err = casClient.SplitBlob(ctx, &repb.SplitBlobRequest{BlobDigest: storedRN.GetDigest(), DigestFunction: repb.DigestFunction_SHA256})
	require.True(t, status.IsNotFoundError(err), "expected cache miss, got %v", err)
	spliceRsp, err := casClient.SpliceBlob(ctx, &repb.SpliceBlobRequest{BlobDigest: storedRN.GetDigest()})
	require.NoError(t, err)
	require.Equal(t, storedRN.GetDigest(), spliceRsp.GetBlobDigest())

	authCtx, err := auth.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	authenticatedRN, authenticatedData := testdigest.RandomCASResourceBuf(t, 10)
	_, err = casClient.BatchUpdateBlobs(authCtx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: authenticatedRN.GetDigest(), Data: authenticatedData}},
	})
	require.NoError(t, err)
	authenticatedReadRsp, err := casClient.BatchReadBlobs(authCtx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{authenticatedRN.GetDigest()}})
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.OK), authenticatedReadRsp.GetResponses()[0].GetStatus().GetCode())
	require.Equal(t, authenticatedData, authenticatedReadRsp.GetResponses()[0].GetData())

	flags.Set(t, "auth.blackhole_anonymous_cache_requests", false)
	findRsp, err = casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{storedRN.GetDigest(), batchWriteRN.GetDigest(), byteStreamWriteDigest},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, digestStrings(batchWriteRN.GetDigest(), byteStreamWriteDigest), digestStrings(findRsp.GetMissingBlobDigests()...))
}

func TestBatchUpdateAndReadCompressedBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", true)
	flags.Set(t, "cache.detailed_stats_enabled", true)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	te.SetMetricsCollector(mc)
	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := compression.CompressZstd(nil, blob)

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	// FindMissingBlobs should report that the blob is missing, initially.
	missingResp, err := casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{d},
	})

	require.NoError(t, err)
	require.Equal(t, digestStrings(d), digestStrings(missingResp.MissingBlobDigests...))

	// Upload compressed blob via BatchUpdate.
	// Use an invocation context scoped just to this request.
	{
		iid, err := uuid.NewRandom()
		require.NoError(t, err)
		rmd := &repb.RequestMetadata{ToolInvocationId: iid.String(), ActionMnemonic: "GoCompile"}
		ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
		require.NoError(t, err)
		batchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
			Requests: []*repb.BatchUpdateBlobsRequest_Request{
				{Digest: d, Data: compressedBlob, Compressor: repb.Compressor_ZSTD},
			},
		})
		require.NoError(t, err)
		for i, resp := range batchUpdateResp.Responses {
			require.Equal(t, "", resp.Status.Message)
			require.Equal(t, int32(gcodes.OK), resp.Status.Code, "BatchUpdateResponse[%d].Status != OK", i)
		}
		sc := hit_tracker.ScoreCard(ctx, te, iid.String())
		require.Len(t, sc.Results, 1)
		assert.Equal(t, repb.Compressor_ZSTD, sc.Results[0].Compressor)
		assert.Equal(t, int64(len(compressedBlob)), sc.Results[0].TransferredSizeBytes)
	}

	// FindMissingBlobs should not report the blob missing after uploading.
	missingResp, err = casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{d},
	})

	require.NoError(t, err)
	require.Equal(
		t, []string{}, digestStrings(missingResp.MissingBlobDigests...),
		"uncompressed digest should not be missing after uploading compressed blob")

	// Read back the blob we just uploaded, indicating that we accept zstd.
	// After decompressing, should get back the original blob contents.
	// Use a new invocation context to get a new cache scorecard.
	iid, err := uuid.NewRandom()
	require.NoError(t, err)
	rmd := &repb.RequestMetadata{ToolInvocationId: iid.String(), ActionMnemonic: "GoCompile"}
	ctx, err = bazel_request.WithRequestMetadata(ctx, rmd)
	require.NoError(t, err)
	readResp, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests:               []*repb.Digest{d},
		AcceptableCompressors: []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD},
	})

	require.NoError(t, err)
	sc := hit_tracker.ScoreCard(ctx, te, iid.String())
	require.Len(t, sc.Results, len(readResp.Responses))
	decompressedBlobs := make([][]byte, len(readResp.Responses))
	for i, resp := range readResp.Responses {
		require.Equal(t, int32(gcodes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i)
		assert.Equal(t, int64(len(resp.Data)), sc.Results[i].TransferredSizeBytes)
		decompressedBlobs[i] = zstdDecompress(t, resp.Data)
	}
	require.Equal(t, [][]byte{blob}, decompressedBlobs)

	// Now try reading back again, this time not accepting zstd.
	readResp, err = casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests:               []*repb.Digest{d},
		AcceptableCompressors: []repb.Compressor_Value{repb.Compressor_IDENTITY},
	})

	require.NoError(t, err)
	blobs := make([][]byte, len(readResp.Responses))
	for i, resp := range readResp.Responses {
		require.Equal(t, int32(gcodes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i)
		blobs[i] = resp.Data
	}
	require.Equal(t, [][]byte{blob}, blobs)
}

func TestBatchUpdateRejectsCompressedBlobsIfCompressionDisabled(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", false)
	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := compression.CompressZstd(nil, blob)

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	// Upload compressed blob via BatchUpdate.
	batchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{Digest: d, Data: compressedBlob, Compressor: repb.Compressor_ZSTD},
		},
	})
	require.NoError(t, err)
	for i, resp := range batchUpdateResp.Responses {
		require.Equal(t, int32(gcodes.Unimplemented), resp.Status.Code, "BatchUpdateResponse[%d].Status != Unimplemented", i)
	}
}

func TestBatchUpdateRejectCorruptBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	req := &repb.BatchUpdateBlobsRequest{}
	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	buf[0] = ^buf[0] // corrupt the data in buf
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: rn.GetDigest(),
		Data:   buf,
	})

	rn2, buf := testdigest.RandomCASResourceBuf(t, 100)
	rn2.Digest.SizeBytes += 1 // corrupt the payload size of d2
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: rn2.GetDigest(),
		Data:   buf,
	})

	rn3, buf := testdigest.RandomCASResourceBuf(t, 100)
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: rn3.GetDigest(),
		Data:   buf,
	})

	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(rsp.GetResponses()))
	assert.Equal(t, int32(gcodes.InvalidArgument), rsp.GetResponses()[0].GetStatus().GetCode())
	assert.Equal(t, int32(gcodes.InvalidArgument), rsp.GetResponses()[1].GetStatus().GetCode())
	assert.Equal(t, int32(gcodes.OK), rsp.GetResponses()[2].GetStatus().GetCode())
}

func TestBatchUpdateAndRead_CacheHandlesCompression(t *testing.T) {
	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := compression.CompressZstd(nil, blob)

	testCases := []struct {
		name                string
		uploadCompression   repb.Compressor_Value
		downloadCompression repb.Compressor_Value
	}{
		{
			name:                "Write compressed, read compressed",
			uploadCompression:   repb.Compressor_ZSTD,
			downloadCompression: repb.Compressor_ZSTD,
		},
		{
			name:                "Write compressed, read decompressed",
			uploadCompression:   repb.Compressor_ZSTD,
			downloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                "Write decompressed, read decompressed",
			uploadCompression:   repb.Compressor_IDENTITY,
			downloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                "Write decompressed, read compressed",
			uploadCompression:   repb.Compressor_IDENTITY,
			downloadCompression: repb.Compressor_ZSTD,
		},
	}

	for _, tc := range testCases {
		{
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			te.SetCache(&testcompression.CompressionCache{Cache: te.GetCache()})
			flags.Set(t, "cache.zstd_transcoding_enabled", true)
			flags.Set(t, "cache.detailed_stats_enabled", true)
			mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
			require.NoError(t, err)
			te.SetMetricsCollector(mc)
			clientConn := runCASServer(ctx, t, te)
			casClient := repb.NewContentAddressableStorageClient(clientConn)

			uploadBlob := blob
			if tc.uploadCompression == repb.Compressor_ZSTD {
				uploadBlob = compressedBlob
			}
			expectedDownloadBlob := blob
			if tc.downloadCompression == repb.Compressor_ZSTD {
				expectedDownloadBlob = compressedBlob
			}

			// Note: Digest is of uncompressed contents
			d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
			require.NoError(t, err, tc.name)

			// FindMissingBlobs should report that the blob is missing, initially.
			missingResp, err := casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
				BlobDigests: []*repb.Digest{d},
			})
			require.NoError(t, err, tc.name)
			require.Equal(t, digestStrings(d), digestStrings(missingResp.MissingBlobDigests...), tc.name)

			// Upload blob via BatchUpdate.
			// Use an invocation context scoped just to this request.
			{
				iid, err := uuid.NewRandom()
				require.NoError(t, err, tc.name)
				rmd := &repb.RequestMetadata{ToolInvocationId: iid.String(), ActionMnemonic: "GoCompile"}
				ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
				require.NoError(t, err, tc.name)
				batchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
					Requests: []*repb.BatchUpdateBlobsRequest_Request{
						{Digest: d, Data: uploadBlob, Compressor: tc.uploadCompression},
					},
				})
				require.NoError(t, err, tc.name)
				for i, resp := range batchUpdateResp.Responses {
					require.Equal(t, "", resp.Status.Message, tc.name)
					require.Equal(t, int32(gcodes.OK), resp.Status.Code, "BatchUpdateResponse[%d].Status != OK", i, tc.name)
				}
				sc := hit_tracker.ScoreCard(ctx, te, iid.String())
				require.Len(t, sc.Results, 1, tc.name)
				assert.Equal(t, tc.uploadCompression, sc.Results[0].Compressor, tc.name)
				assert.Equal(t, int64(len(uploadBlob)), sc.Results[0].TransferredSizeBytes, tc.name)
			}

			// Read back the blob we just uploaded
			// Use a new invocation context to get a new cache scorecard.
			iid, err := uuid.NewRandom()
			require.NoError(t, err, tc.name)
			rmd := &repb.RequestMetadata{ToolInvocationId: iid.String(), ActionMnemonic: "GoCompile"}
			ctx, err = bazel_request.WithRequestMetadata(ctx, rmd)
			require.NoError(t, err, tc.name)
			readResp, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
				Digests:               []*repb.Digest{d},
				AcceptableCompressors: []repb.Compressor_Value{tc.downloadCompression},
			})

			require.NoError(t, err, tc.name)
			sc := hit_tracker.ScoreCard(ctx, te, iid.String())
			require.Len(t, sc.Results, len(readResp.Responses), tc.name)
			downloadedBlobs := make([][]byte, len(readResp.Responses))
			for i, resp := range readResp.Responses {
				require.Equal(t, int32(gcodes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i, tc.name)
				assert.Equal(t, int64(len(resp.Data)), sc.Results[i].TransferredSizeBytes, tc.name)
				downloadedBlobs[i] = resp.Data
			}
			require.Equal(t, [][]byte{expectedDownloadBlob}, downloadedBlobs, tc.name)
		}
	}
}

func TestMalevolentCache(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	c, err := memory_cache.NewMemoryCache(1000000)
	if err != nil {
		t.Fatal(err)
	}
	te.SetCache(&evilCache{c})
	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	set, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{
				Digest: rn.GetDigest(),
				Data:   buf,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(set.GetResponses()))
	assert.Equal(t, rn.GetDigest().GetHash(), set.GetResponses()[0].GetDigest().GetHash())
	assert.Equal(t, int32(gcodes.OK), set.GetResponses()[0].GetStatus().GetCode())
}

func digestStrings(digests ...*repb.Digest) []string {
	out := make([]string, len(digests))
	for i, d := range digests {
		out[i] = fmt.Sprintf("%s/%d", d.Hash, d.SizeBytes)
	}
	return out
}

func zstdDecompress(t *testing.T, b []byte) []byte {
	out, err := compression.DecompressZstd(nil, b)
	require.NoError(t, err, "failed to decompress blob")
	return out
}

func TestGetTree(t *testing.T) {
	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	// Upload a dir containing fileCount files, and return the file
	// names and directory digest.
	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return cas.MakeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, child1Files := uploadDirWithFiles(2, 1)
	child2Digest, child2Files := uploadDirWithFiles(2, 1)

	// Upload a root directory containing both child directories.
	rootDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "child1",
				Digest: child1Digest,
			},
			{
				Name:   "child2",
				Digest: child2Digest,
			},
		},
	}
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, rootDir)
	assert.Nil(t, err)

	allFiles := append(child1Files, child2Files...)
	allFiles = append(allFiles, "child1", "child2")
	treeFiles := cas.ReadTree(ctx, t, casClient, instanceName, rootDigest)
	assert.ElementsMatch(t, allFiles, treeFiles)
}

func TestGetTreeCaching(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return cas.MakeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, child1Files := uploadDirWithFiles(10, 2)
	child2Digest, child2Files := uploadDirWithFiles(10, 2)
	child3Digest, child3Files := uploadDirWithFiles(1, 1)

	// Upload a root directory containing both child directories.
	rootDir1 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "child1",
				Digest: child1Digest,
			},
			{
				Name:   "child2",
				Digest: child2Digest,
			},
		},
	}
	rootDigest1, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, rootDir1)
	assert.Nil(t, err)

	rootDir2 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "child2",
				Digest: child2Digest,
			},
			{
				Name:   "child3",
				Digest: child3Digest,
			},
		},
	}
	rootDigest2, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, rootDir2)
	assert.Nil(t, err)

	uploadedFiles1 := append(child1Files, child2Files...)
	uploadedFiles1 = append(uploadedFiles1, "child1", "child2")

	start := time.Now()
	treeFiles1 := cas.ReadTree(ctx, t, casClient, instanceName, rootDigest1)
	fetch1Time := time.Since(start)

	assert.ElementsMatch(t, uploadedFiles1, treeFiles1)

	uploadedFiles2 := append(child2Files, child3Files...)
	uploadedFiles2 = append(uploadedFiles2, "child2", "child3")
	start = time.Now()
	treeFiles2 := cas.ReadTree(ctx, t, casClient, instanceName, rootDigest2)
	fetch2Time := time.Since(start)

	assert.ElementsMatch(t, uploadedFiles2, treeFiles2)
	assert.Less(t, fetch2Time, fetch1Time/2)
}

func NestForTest(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, dirToNest *repb.Directory, prefix string, levels int) (*repb.Digest, []string) {
	outFiles := make([]string, 0)
	rootDir := dirToNest
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, rootDir)
	assert.Nil(t, err)
	for i := range levels {
		name := fmt.Sprintf("%s-%d", prefix, i)
		outFiles = append(outFiles, name)
		rootDir = &repb.Directory{
			Directories: []*repb.DirectoryNode{
				{
					Name:   name,
					Digest: rootDigest,
				},
			},
		}
		rootDigest, err = cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, rootDir)
		assert.Nil(t, err)
	}
	return rootDigest, outFiles
}

func TestGetTreeCachingWithSplitting(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	flags.Set(t, "cache.tree_cache_splitting_min_size", 1000)

	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return cas.MakeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, child1Files := uploadDirWithFiles(10, 2)
	nodeModulesDigest, nodeModulesFiles := uploadDirWithFiles(10, 2)
	child3Digest, child3Files := uploadDirWithFiles(1, 1)

	// Upload a root directory containing both child directories.
	rootDir1 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "child1",
				Digest: child1Digest,
			},
			{
				Name:   "node_modules",
				Digest: nodeModulesDigest,
			},
		},
	}
	rootDigest1, extraFiles1 := NestForTest(t, ctx, bsClient, instanceName, rootDir1, "dir1", 5)

	rootDir2 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "node_modules",
				Digest: nodeModulesDigest,
			},
			{
				Name:   "child3",
				Digest: child3Digest,
			},
		},
	}
	rootDigest2, extraFiles2 := NestForTest(t, ctx, bsClient, instanceName, rootDir2, "dir2", 5)

	uploadedFiles1 := append(child1Files, nodeModulesFiles...)
	uploadedFiles1 = append(uploadedFiles1, "child1", "node_modules")
	uploadedFiles1 = append(uploadedFiles1, extraFiles1...)

	start := time.Now()
	treeFiles1 := cas.ReadTree(ctx, t, casClient, instanceName, rootDigest1)
	fetch1Time := time.Since(start)

	assert.ElementsMatch(t, uploadedFiles1, treeFiles1)

	uploadedFiles2 := append(nodeModulesFiles, child3Files...)
	uploadedFiles2 = append(uploadedFiles2, "node_modules", "child3")
	uploadedFiles2 = append(uploadedFiles2, extraFiles2...)
	start = time.Now()
	treeFiles2 := cas.ReadTree(ctx, t, casClient, instanceName, rootDigest2)
	fetch2Time := time.Since(start)

	assert.ElementsMatch(t, uploadedFiles2, treeFiles2)
	assert.Less(t, fetch2Time, fetch1Time/2)
}

func TestGetTreeWithSubtrees(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	flags.Set(t, "cache.get_tree_subtree_support", true)

	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return cas.MakeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, child1Files := uploadDirWithFiles(10, 2)
	nodeModulesDigest, nodeModulesFiles := uploadDirWithFiles(10, 2)
	child3Digest, child3Files := uploadDirWithFiles(1, 1)

	// Upload a root directory containing both child directories.
	rootDir1 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "child1",
				Digest: child1Digest,
			},
			{
				Name:   "node_modules",
				Digest: nodeModulesDigest,
			},
		},
	}
	rootDigest1, extraFiles1 := NestForTest(t, ctx, bsClient, instanceName, rootDir1, "dir1", 5)

	rootDir2 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "node_modules",
				Digest: nodeModulesDigest,
			},
			{
				Name:   "child3",
				Digest: child3Digest,
			},
		},
	}
	rootDigest2, extraFiles2 := NestForTest(t, ctx, bsClient, instanceName, rootDir2, "dir2", 5)

	uploadedFiles1 := append(child1Files, nodeModulesFiles...)
	uploadedFiles1 = append(uploadedFiles1, "child1", "node_modules")
	uploadedFiles1 = append(uploadedFiles1, extraFiles1...)

	// Stuff cache.
	treeFiles1 := cas.ReadTree(ctx, t, casClient, instanceName, rootDigest1)

	assert.ElementsMatch(t, uploadedFiles1, treeFiles1)

	// Now read with subtrees..
	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
		InstanceName:             instanceName,
		RootDigest:               rootDigest2,
		SendCachedSubtreeDigests: true,
	})
	assert.Nil(t, err)

	treeFiles2 := make([]string, 0)
	subtrees := make([]*repb.SubtreeResourceName, 0)
	directoryCount := 0

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		directoryCount += len(rsp.GetDirectories())
		for _, dir := range rsp.GetDirectories() {
			for _, file := range dir.GetFiles() {
				treeFiles2 = append(treeFiles2, file.GetName())
			}
			for _, subdir := range dir.GetDirectories() {
				treeFiles2 = append(treeFiles2, subdir.GetName())
			}
		}
		subtrees = append(subtrees, rsp.GetSubtrees()...)
	}

	assert.Equal(t, 8, directoryCount)
	assert.Equal(t, 1, len(subtrees))

	subtree := &capb.TreeCache{}
	rn := digest.NewCASResourceName(subtrees[0].GetDigest(), instanceName, subtrees[0].GetDigestFunction())
	rn.SetCompressor(subtrees[0].GetCompressor())
	err = cachetools.GetBlobAsProto(ctx, bsClient, rn, subtree)
	assert.NoError(t, err)

	uploadedFiles2 := append(nodeModulesFiles, child3Files...)
	uploadedFiles2 = append(uploadedFiles2, "node_modules", "child3")
	uploadedFiles2 = append(uploadedFiles2, extraFiles2...)
	assert.Equal(t, 2047, len(subtree.GetChildren()))
	for _, child := range subtree.GetChildren() {
		for _, file := range child.GetDirectory().GetFiles() {
			treeFiles2 = append(treeFiles2, file.GetName())
		}
		for _, subdir := range child.GetDirectory().GetDirectories() {
			treeFiles2 = append(treeFiles2, subdir.GetName())
		}
	}

	assert.ElementsMatch(t, uploadedFiles2, treeFiles2)
}

func hasMissingDigestError(err error) bool {
	st := gstatus.Convert(err)
	for _, detail := range st.Details() {
		switch detail := detail.(type) {
		case *errdetails.PreconditionFailure:
			if len(detail.Violations) > 0 && detail.Violations[0].GetType() == "MISSING" {
				return true
			}
		}
	}
	return false
}

func TestGetTreeMissingRoot(t *testing.T) {
	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	// Upload a dir containing fileCount files, and return the file
	// names and directory digest.
	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return cas.MakeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, _ := uploadDirWithFiles(2, 1)
	child2Digest, _ := uploadDirWithFiles(2, 1)

	// Upload a root directory containing both child directories.
	rootDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{
				Name:   "child11",
				Digest: child1Digest,
			},
			{
				Name:   "child2",
				Digest: child2Digest,
			},
		},
	}
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, repb.DigestFunction_SHA256, rootDir)
	assert.Nil(t, err)

	rootRN := digest.NewResourceName(rootDigest, instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	require.NoError(t, te.GetCache().Delete(ctx, rootRN.ToProto()))

	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
		InstanceName: instanceName,
		RootDigest:   rootDigest,
	})
	assert.Nil(t, err)

	_, err = stream.Recv()
	require.Error(t, err)
	require.True(t, hasMissingDigestError(err))
}

func TestSpliceAndSplitBlob(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	testFile := "testdata/server_notification.a"
	fileData, err := os.ReadFile(testFile)
	require.NoError(t, err)
	require.Greater(t, len(fileData), 0)

	avgChunkSize := 64 << 10 // 64KB
	chunker, err := fastcdc.NewChunker(bytes.NewReader(fileData), avgChunkSize)
	require.NoError(t, err)

	var chunks [][]byte
	var chunkDigests []*repb.Digest

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		chunkData := make([]byte, len(chunk.Data))
		copy(chunkData, chunk.Data)
		chunks = append(chunks, chunkData)

		chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), repb.DigestFunction_BLAKE3)
		require.NoError(t, err)
		chunkDigests = append(chunkDigests, chunkDigest)
	}

	require.Equal(t, len(chunks), 10)
	batchReq := &repb.BatchUpdateBlobsRequest{
		Requests:       make([]*repb.BatchUpdateBlobsRequest_Request, len(chunks)),
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	for i, chunk := range chunks {
		batchReq.Requests[i] = &repb.BatchUpdateBlobsRequest_Request{
			Digest: chunkDigests[i],
			Data:   chunk,
		}
	}

	blobDigest, err := digest.Compute(bytes.NewReader(fileData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	spliceReq := &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		DigestFunction: repb.DigestFunction_BLAKE3,
	}

	// First, show that SpliceBlob fails if the chunks are not yet uploaded.
	_, err = casClient.SpliceBlob(ctx, spliceReq)
	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err))

	// Upload the chunks, then show successful SpliceBlob and SplitBlob.
	_, err = casClient.BatchUpdateBlobs(ctx, batchReq)
	require.NoError(t, err)

	spliceResp, err := casClient.SpliceBlob(ctx, spliceReq)
	require.NoError(t, err)
	require.Equal(t, blobDigest.Hash, spliceResp.BlobDigest.Hash)
	require.Equal(t, blobDigest.SizeBytes, spliceResp.BlobDigest.SizeBytes)

	splitReq := &repb.SplitBlobRequest{
		BlobDigest:     blobDigest,
		DigestFunction: repb.DigestFunction_BLAKE3,
	}

	splitResp, err := casClient.SplitBlob(ctx, splitReq)
	require.NoError(t, err)
	require.Equal(t, len(chunkDigests), len(splitResp.ChunkDigests))

	for i, expectedDigest := range chunkDigests {
		actualDigest := splitResp.ChunkDigests[i]
		assert.Equal(t, expectedDigest.Hash, actualDigest.Hash)
		assert.Equal(t, expectedDigest.SizeBytes, actualDigest.SizeBytes)
	}
}

func TestSpliceBlobWithoutValidation(t *testing.T) {
	for _, tc := range []struct {
		name              string
		groupID           string
		clientIdentity    string
		noIdentityService bool
		sendHeader        bool
		missingChunk      bool
		wrongBlobSize     bool
		wantSuccess       bool
		wantValidation    string
	}{
		{
			name:           "trusted executor",
			groupID:        "GR1",
			clientIdentity: interfaces.ClientIdentityExecutor,
			sendHeader:     true,
			wantSuccess:    true,
			wantValidation: metrics.SpliceBlobValidationSkipped,
		},
		{
			name:           "trusted cache proxy forwards header",
			groupID:        "GR1",
			clientIdentity: interfaces.ClientIdentityCacheProxy,
			sendHeader:     true,
			wantSuccess:    true,
			wantValidation: metrics.SpliceBlobValidationSkipped,
		},
		{
			name:           "trusted executor without header",
			groupID:        "GR1",
			clientIdentity: interfaces.ClientIdentityExecutor,
			wantValidation: metrics.SpliceBlobValidationFull,
		},
		{
			name:           "trusted executor cannot reference a missing chunk",
			groupID:        "GR1",
			clientIdentity: interfaces.ClientIdentityExecutor,
			sendHeader:     true,
			missingChunk:   true,
			wantValidation: metrics.SpliceBlobValidationSkipped,
		},
		{
			name:           "trusted executor cannot claim wrong blob size",
			groupID:        "GR1",
			clientIdentity: interfaces.ClientIdentityExecutor,
			sendHeader:     true,
			wrongBlobSize:  true,
			wantValidation: metrics.SpliceBlobValidationSkipped,
		},
		{
			name:              "server without client identity service",
			groupID:           "GR1",
			noIdentityService: true,
			sendHeader:        true,
			wantValidation:    metrics.SpliceBlobValidationFull,
		},
		{
			name:           "untrusted caller cannot spoof header",
			groupID:        "GR1",
			sendHeader:     true,
			wantValidation: metrics.SpliceBlobValidationFull,
		},
		{
			name:           "app identity cannot skip validation",
			groupID:        "GR1",
			clientIdentity: interfaces.ClientIdentityApp,
			sendHeader:     true,
			wantValidation: metrics.SpliceBlobValidationFull,
		},
		{
			name:           "signed unknown client cannot spoof header",
			groupID:        "GR1",
			clientIdentity: "unknown-client",
			sendHeader:     true,
			wantValidation: metrics.SpliceBlobValidationFull,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			user := testauth.User("US1", tc.groupID)
			env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
				user.GetUserID(): user,
			}))

			var identityService *clientidentity.Service
			if !tc.noIdentityService {
				flags.Set(t, "app.client_identity.key", "test-client-identity-key")
				var err error
				identityService, err = clientidentity.New(env.GetClock())
				require.NoError(t, err)
				env.SetClientIdentityService(identityService)
			}

			ctx := testauth.WithAuthenticatedUserInfo(t.Context(), user)
			ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
			require.NoError(t, err)
			if tc.sendHeader {
				ctx = cdc.ContextWithSpliceWithoutValidation(ctx)
			}
			if tc.clientIdentity != "" {
				identityHeader, err := identityService.NewIdentityHeader(&interfaces.ClientIdentity{
					Origin: interfaces.ClientIdentityInternalOrigin,
					Client: tc.clientIdentity,
				}, clientidentity.DefaultExpiration)
				require.NoError(t, err)
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, identityHeader)
			}

			chunks := [][]byte{[]byte("chunk one"), []byte("chunk two")}
			chunkDigests := make([]*repb.Digest, 0, len(chunks))
			var blobSize int
			for _, chunk := range chunks {
				d, err := digest.Compute(bytes.NewReader(chunk), repb.DigestFunction_SHA256)
				require.NoError(t, err)
				chunkDigests = append(chunkDigests, d)
				blobSize += len(chunk)
				if !tc.missingChunk {
					rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
					require.NoError(t, env.GetCache().Set(ctx, rn.ToProto(), chunk))
				}
			}
			if tc.wrongBlobSize {
				blobSize++
			}
			blobDigest, err := digest.Compute(strings.NewReader(strings.Repeat("x", blobSize)), repb.DigestFunction_SHA256)
			require.NoError(t, err)

			clientConn := runCASServer(ctx, t, env)
			t.Cleanup(func() { clientConn.Close() })
			client := repb.NewContentAddressableStorageClient(clientConn)
			metrics.SpliceBlobCount.Reset()
			_, err = client.SpliceBlob(ctx, &repb.SpliceBlobRequest{
				BlobDigest:     blobDigest,
				ChunkDigests:   chunkDigests,
				DigestFunction: repb.DigestFunction_SHA256,
			})
			assert.Equal(t, float64(1), testmetrics.CounterValueForLabels(t, metrics.SpliceBlobCount, prometheus.Labels{
				metrics.SpliceBlobValidation: tc.wantValidation,
				metrics.GroupID:              tc.groupID,
			}))
			if !tc.wantSuccess {
				require.Error(t, err)
				require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgumentError, got %v", err)
				return
			}
			require.NoError(t, err)

			manifest, err := chunking.LoadManifest(ctx, env.GetCache(), blobDigest, "", repb.DigestFunction_SHA256)
			require.NoError(t, err)
			require.Equal(t, chunkDigests, manifest.ChunkDigests)
		})
	}
}

func TestSplitBlobNotFound(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	// Create a digest for a blob that has never been spliced
	blobDigest := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 12345,
	}

	splitReq := &repb.SplitBlobRequest{
		BlobDigest:     blobDigest,
		DigestFunction: repb.DigestFunction_BLAKE3,
	}

	_, err = casClient.SplitBlob(ctx, splitReq)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)
}

func TestSplitBlobRejectsLayeredManifest(t *testing.T) {
	flags.Set(t, "cache.avg_chunk_size_bytes", 1024*1024)
	flags.Set(t, "cache.min_chunked_read_fallback_size_bytes", 2*1024*1024)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)
	cache := te.GetCache()

	leaf1RN, leaf1 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	leaf2RN, leaf2 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	leaf3RN, leaf3 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	nestedData := bytes.Join([][]byte{leaf1, leaf2, leaf3}, nil)
	nestedDigest, err := digest.Compute(bytes.NewReader(nestedData), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	for i, leaf := range []struct {
		rn   *rspb.ResourceName
		data []byte
	}{
		{leaf1RN, leaf1},
		{leaf2RN, leaf2},
		{leaf3RN, leaf3},
	} {
		require.NoError(t, cache.Set(ctx, leaf.rn, leaf.data), "store leaf %d", i)
	}
	require.NoError(t, (&chunking.Manifest{
		BlobDigest:     nestedDigest,
		ChunkDigests:   []*repb.Digest{leaf1RN.GetDigest(), leaf2RN.GetDigest(), leaf3RN.GetDigest()},
		DigestFunction: repb.DigestFunction_SHA256,
	}).Store(ctx, cache))

	directRN, directData := testdigest.RandomCASResourceBuf(t, 2*1024*1024)
	require.NoError(t, cache.Set(ctx, directRN, directData))
	parentData := bytes.Join([][]byte{nestedData, directData}, nil)
	parentDigest, err := digest.Compute(bytes.NewReader(parentData), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	// Inject a legacy layered manifest directly. Normal manifest storage rejects
	// this because nestedDigest is not itself present in the CAS.
	require.NoError(t, (&chunking.Manifest{
		BlobDigest:     parentDigest,
		ChunkDigests:   []*repb.Digest{nestedDigest, directRN.GetDigest()},
		DigestFunction: repb.DigestFunction_SHA256,
	}).StoreWithoutVerification(ctx, cache))

	_, err = casClient.SplitBlob(ctx, &repb.SplitBlobRequest{
		BlobDigest:     parentDigest,
		DigestFunction: repb.DigestFunction_SHA256,
	})
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got: %v", err)
}

func TestSpliceBlobSingleChunk(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	// Upload a single chunk. This is not supported by the server.
	chunkData := []byte("this is a single chunk of data")
	chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	batchReq := &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{
				Digest: chunkDigest,
				Data:   chunkData,
			},
		},
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	_, err = casClient.BatchUpdateBlobs(ctx, batchReq)
	require.NoError(t, err)

	blobDigest := chunkDigest

	spliceReq := &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunkDigest},
		DigestFunction: repb.DigestFunction_BLAKE3,
	}

	_, err = casClient.SpliceBlob(ctx, spliceReq)
	require.Error(t, err)
	require.True(t, status.IsUnimplementedError(err), "expected UnimplementedError, got: %v", err)
}

func TestFindMissingBlobsWithChunkedBlob(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)
	cache := te.GetCache()

	chunk1RN, chunk1 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk2RN, chunk2 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk3RN, chunk3 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk4RN, chunk4 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk5RN, chunk5 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	fullBlob := bytes.Join([][]byte{chunk1, chunk2, chunk3, chunk4, chunk5}, nil)

	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	require.NoError(t, cache.Set(ctx, chunk1RN, chunk1))
	require.NoError(t, cache.Set(ctx, chunk2RN, chunk2))
	require.NoError(t, cache.Set(ctx, chunk3RN, chunk3))
	require.NoError(t, cache.Set(ctx, chunk4RN, chunk4))
	require.NoError(t, cache.Set(ctx, chunk5RN, chunk5))

	manifest := &chunking.Manifest{
		BlobDigest: blobDigest,
		ChunkDigests: []*repb.Digest{
			chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest(),
			chunk4RN.GetDigest(), chunk5RN.GetDigest(),
		},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}
	require.NoError(t, manifest.Store(ctx, cache))

	regularBlob := []byte("small")
	regularDigest, err := digest.Compute(bytes.NewReader(regularBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	rsp, err := casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{blobDigest, regularDigest},
	})
	require.NoError(t, err)

	require.Len(t, rsp.MissingBlobDigests, 1)
	require.Equal(t, regularDigest.GetHash(), rsp.MissingBlobDigests[0].GetHash())

	rsp, err = casClient.FindMissingBlobs(cdc.ContextWithChunked(ctx), &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{blobDigest, regularDigest},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, digestStrings(blobDigest, regularDigest), digestStrings(rsp.MissingBlobDigests...))
}

func TestChunkedBlobAtCurrentWriteThresholdIsMissingButReadable(t *testing.T) {
	flags.Set(t, "cache.avg_chunk_size_bytes", 1024*1024)
	flags.Set(t, "cache.min_chunked_read_fallback_size_bytes", 2*1024*1024)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)
	bsClient := bspb.NewByteStreamClient(clientConn)
	cache := te.GetCache()

	chunk1RN, chunk1 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk2RN, chunk2 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk3RN, chunk3 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk4RN, chunk4 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	fullBlob := bytes.Join([][]byte{chunk1, chunk2, chunk3, chunk4}, nil)

	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	require.NoError(t, cache.Set(ctx, chunk1RN, chunk1))
	require.NoError(t, cache.Set(ctx, chunk2RN, chunk2))
	require.NoError(t, cache.Set(ctx, chunk3RN, chunk3))
	require.NoError(t, cache.Set(ctx, chunk4RN, chunk4))

	manifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest(), chunk4RN.GetDigest()},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}
	require.NoError(t, manifest.Store(ctx, cache))

	rsp, err := casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{blobDigest},
	})
	require.NoError(t, err)
	require.Len(t, rsp.MissingBlobDigests, 1)
	require.Equal(t, blobDigest.GetHash(), rsp.MissingBlobDigests[0].GetHash())

	var downloaded bytes.Buffer
	rn := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_SHA256)
	require.NoError(t, cachetools.GetBlob(ctx, bsClient, rn, &downloaded))
	require.Equal(t, fullBlob, downloaded.Bytes())
}

func TestBatchReadBlobsWithChunkedBlob(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		acceptableCompressors []repb.Compressor_Value
		wantCompressor        repb.Compressor_Value
		useCompressionCache   bool
	}{
		{
			name:           "Identity",
			wantCompressor: repb.Compressor_IDENTITY,
		},
		{
			name:                  "Zstd",
			acceptableCompressors: []repb.Compressor_Value{repb.Compressor_ZSTD},
			wantCompressor:        repb.Compressor_ZSTD,
			useCompressionCache:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			if tc.useCompressionCache {
				flags.Set(t, "cache.zstd_transcoding_enabled", true)
				te.SetCache(&casCompressionCache{Cache: te.GetCache()})
			}

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
			require.NoError(t, err)

			clientConn := runCASServer(ctx, t, te)
			casClient := repb.NewContentAddressableStorageClient(clientConn)
			cache := te.GetCache()

			regularBlob := []byte("regular blob")
			regularDigest, err := digest.Compute(bytes.NewReader(regularBlob), repb.DigestFunction_SHA256)
			require.NoError(t, err)
			require.NoError(t, cache.Set(ctx, digest.NewCASResourceName(regularDigest, "", repb.DigestFunction_SHA256).ToProto(), regularBlob))

			chunk1RN, chunk1 := testdigest.RandomCASResourceBuf(t, 1024*1024)
			chunk2RN, chunk2 := testdigest.RandomCASResourceBuf(t, 1024*1024)
			chunk3RN, chunk3 := testdigest.RandomCASResourceBuf(t, 1024*1024)
			chunkedBlob := append(append(chunk1, chunk2...), chunk3...)

			chunkedBlobDigest, err := digest.Compute(bytes.NewReader(chunkedBlob), repb.DigestFunction_SHA256)
			require.NoError(t, err)

			require.NoError(t, cache.Set(ctx, chunk1RN, chunk1))
			require.NoError(t, cache.Set(ctx, chunk2RN, chunk2))
			require.NoError(t, cache.Set(ctx, chunk3RN, chunk3))

			manifest := &chunking.Manifest{
				BlobDigest:     chunkedBlobDigest,
				ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest()},
				InstanceName:   "",
				DigestFunction: repb.DigestFunction_SHA256,
			}
			require.NoError(t, manifest.Store(ctx, cache))

			wantByDigest := map[string][]byte{
				regularDigest.GetHash():     regularBlob,
				chunkedBlobDigest.GetHash(): chunkedBlob,
			}

			readResp, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
				Digests:               []*repb.Digest{regularDigest, chunkedBlobDigest},
				AcceptableCompressors: tc.acceptableCompressors,
			})
			require.NoError(t, err)
			require.Len(t, readResp.GetResponses(), len(wantByDigest))

			for _, resp := range readResp.GetResponses() {
				require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode())
				require.Equal(t, tc.wantCompressor, resp.GetCompressor())

				wantBlob, ok := wantByDigest[resp.GetDigest().GetHash()]
				require.True(t, ok, "unexpected digest %s", resp.GetDigest().GetHash())
				if tc.wantCompressor == repb.Compressor_ZSTD {
					require.Equal(t, wantBlob, zstdDecompress(t, resp.GetData()))
				} else {
					require.Equal(t, wantBlob, resp.GetData())
				}
			}
		})
	}
}

func TestBatchReadBlobsWithMismatchedChunkedManifest(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)
	cache := te.GetCache()

	chunkRN, chunk := testdigest.RandomCASResourceBuf(t, 1024)
	require.NoError(t, cache.Set(ctx, chunkRN, chunk))

	corruptDigest, _ := testdigest.NewReader(t, 3*1024*1024)
	manifest := &chunking.Manifest{
		BlobDigest:     corruptDigest,
		ChunkDigests:   []*repb.Digest{chunkRN.GetDigest()},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}
	require.NoError(t, manifest.StoreWithoutVerification(ctx, cache))

	readResp, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{corruptDigest},
	})
	require.NoError(t, err)
	require.Len(t, readResp.GetResponses(), 1)
	require.Equal(t, int32(gcodes.NotFound), readResp.GetResponses()[0].GetStatus().GetCode())
}

func TestBatchReadBlobsRejectsOversizedRequest(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	_, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{
			{Hash: strings.Repeat("a", 64), SizeBytes: 3 * 1000 * 1000},
			{Hash: strings.Repeat("b", 64), SizeBytes: 2 * 1000 * 1000},
		},
	})
	require.Error(t, err)
	require.Equal(t, gcodes.InvalidArgument, gstatus.Code(err))
}

func TestSpliceBlobReadOnlyKey(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	readOnlyUser := &testauth.TestUser{
		UserID:       "US1",
		GroupID:      "GR1",
		Capabilities: []cappb.Capability{},
	}
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{readOnlyUser.UserID: readOnlyUser})
	te.SetAuthenticator(ta)

	ctx = testauth.WithAuthenticatedUserInfo(ctx, readOnlyUser)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	spliceReq := &repb.SpliceBlobRequest{
		BlobDigest:     &repb.Digest{Hash: "abc123", SizeBytes: 100},
		ChunkDigests:   []*repb.Digest{{Hash: "chunk1", SizeBytes: 50}, {Hash: "chunk2", SizeBytes: 50}},
		DigestFunction: repb.DigestFunction_BLAKE3,
	}

	_, err := casClient.SpliceBlob(ctx, spliceReq)
	require.NoError(t, err)
}

func TestStreamingChunkMappingRPCsUnimplemented(t *testing.T) {
	ctx := context.Background()
	clientConn := runCASServer(ctx, t, testenv.GetTestEnv(t))
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	getStream, err := casClient.GetChunkMapping(ctx, &repb.GetChunkMappingRequest{})
	require.NoError(t, err)
	_, err = getStream.Recv()
	require.Equal(t, gcodes.Unimplemented, gstatus.Code(err))

	registerStream, err := casClient.RegisterChunkMapping(ctx)
	require.NoError(t, err)
	_, err = registerStream.CloseAndRecv()
	require.Equal(t, gcodes.Unimplemented, gstatus.Code(err))
}

// treeCacheSpy records tree cache traffic. Tree cache entries are identified
// by their instance name.
type treeCacheSpy struct {
	interfaces.Cache
	stripPointerMetadata bool
	// If set, tree cache writes block until it is closed.
	gate chan struct{}

	mu           sync.Mutex
	blobGets     int
	findMissing  int
	pointers     []*repb.ActionResult // in write order
	pointerRNs   []*rspb.ResourceName
	blobWrites   []*capb.TreeCache
	blobWriteRNs []*rspb.ResourceName
}

func isTreeCacheRN(rn *rspb.ResourceName) bool {
	return strings.HasPrefix(rn.GetInstanceName(), digest.TreeCacheRemoteInstanceName)
}

func (c *treeCacheSpy) Get(ctx context.Context, rn *rspb.ResourceName) ([]byte, error) {
	if isTreeCacheRN(rn) && rn.GetCacheType() == rspb.CacheType_CAS {
		c.mu.Lock()
		c.blobGets++
		c.mu.Unlock()
	}
	return c.Cache.Get(ctx, rn)
}

func (c *treeCacheSpy) FindMissing(ctx context.Context, rns []*rspb.ResourceName) ([]*repb.Digest, error) {
	c.mu.Lock()
	c.findMissing++
	c.mu.Unlock()
	return c.Cache.FindMissing(ctx, rns)
}

func (c *treeCacheSpy) Set(ctx context.Context, rn *rspb.ResourceName, data []byte) error {
	if !isTreeCacheRN(rn) {
		return c.Cache.Set(ctx, rn, data)
	}
	if c.gate != nil {
		<-c.gate
	}
	ar := &repb.ActionResult{}
	tc := &capb.TreeCache{}
	isPointer := rn.GetCacheType() == rspb.CacheType_AC
	if isPointer {
		c.mu.Lock()
		strip := c.stripPointerMetadata
		c.mu.Unlock()
		if err := proto.Unmarshal(data, ar); err == nil && strip {
			ar.ExecutionMetadata = nil
			data, _ = proto.Marshal(ar)
		}
	} else {
		_ = proto.Unmarshal(data, tc)
	}
	if err := c.Cache.Set(ctx, rn, data); err != nil {
		return err
	}
	// Recorded only once readable so that waitForPointers is a reliable signal.
	c.mu.Lock()
	defer c.mu.Unlock()
	if isPointer {
		c.pointers = append(c.pointers, ar)
		c.pointerRNs = append(c.pointerRNs, rn)
	} else {
		c.blobWrites = append(c.blobWrites, tc)
		c.blobWriteRNs = append(c.blobWriteRNs, rn)
	}
	return nil
}

func (c *treeCacheSpy) resetCounts() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blobGets = 0
	c.findMissing = 0
}

func (c *treeCacheSpy) counts() (blobGets, findMissing int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blobGets, c.findMissing
}

func (c *treeCacheSpy) waitForPointers(t *testing.T, n int) {
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.pointers) >= n
	}, 30*time.Second, 10*time.Millisecond)
}

func (c *treeCacheSpy) blobRN(i int) *rspb.ResourceName {
	c.mu.Lock()
	defer c.mu.Unlock()
	rn := c.pointerRNs[i]
	return digest.NewResourceName(c.pointers[i].GetOutputFiles()[0].GetDigest(), rn.GetInstanceName(), rspb.CacheType_CAS, rn.GetDigestFunction()).ToProto()
}

type treeResult struct {
	dirs     map[string]bool // digests of directories sent inline
	subtrees map[string]bool // digests of cached subtree blobs sent
}

func getTreeWithSubtrees(ctx context.Context, t *testing.T, casClient repb.ContentAddressableStorageClient, rootDigest *repb.Digest) treeResult {
	return getTree(ctx, t, casClient, rootDigest, true)
}

func getTree(ctx context.Context, t *testing.T, casClient repb.ContentAddressableStorageClient, rootDigest *repb.Digest, sendSubtrees bool) treeResult {
	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
		RootDigest:               rootDigest,
		SendCachedSubtreeDigests: sendSubtrees,
	})
	require.NoError(t, err)
	res := treeResult{dirs: map[string]bool{}, subtrees: map[string]bool{}}
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			return res
		}
		require.NoError(t, err)
		for _, dir := range rsp.GetDirectories() {
			d, err := digest.ComputeForMessage(dir, repb.DigestFunction_SHA256)
			require.NoError(t, err)
			res.dirs[d.GetHash()] = true
		}
		for _, st := range rsp.GetSubtrees() {
			res.subtrees[st.GetDigest().GetHash()] = true
		}
	}
}

// subtreeDirCount returns the dir count recorded in the pointer to the tree
// cache blob with the given hash.
func (c *treeCacheSpy) subtreeDirCount(t *testing.T, blobHash string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ar := range c.pointers {
		if ar.GetOutputFiles()[0].GetDigest().GetHash() != blobHash {
			continue
		}
		meta := &capb.TreeCachePointerMetadata{}
		require.NoError(t, ar.GetExecutionMetadata().GetAuxiliaryMetadata()[0].UnmarshalTo(meta))
		return int(meta.GetDirCount())
	}
	require.FailNow(t, "no pointer for subtree", blobHash)
	return 0
}

// assertCompleteTree checks that got, together with the subtrees it points to,
// covers exactly the directories of the full tree under root.
func (f *skipFetchFixture) assertCompleteTree(t *testing.T, root *repb.Digest, got treeResult) {
	full := getTree(f.ctx, t, f.casClient, root, false)
	require.Empty(t, full.subtrees)
	n := len(got.dirs)
	for h := range got.subtrees {
		n += f.spy.subtreeDirCount(t, h)
	}
	assert.Equal(t, len(full.dirs), n)
	for d := range got.dirs {
		assert.Contains(t, full.dirs, d)
	}
}

type skipFetchFixture struct {
	ctx       context.Context
	spy       *treeCacheSpy
	cache     interfaces.Cache
	casClient repb.ContentAddressableStorageClient
	// Reading rootDigest1 caches child1 and child2; rootDigest2 contains a
	// cached child2 and an uncached child3.
	rootDigest1, rootDigest2 *repb.Digest
	// Contains rootDigest2.
	nestedRoot *repb.Digest

	// For the splitting fixture: the cached subtree whose root TreeCache
	// references a split child, and that split child.
	splitRootBlob, splitChildBlob *rspb.ResourceName
}

func setupSkipFetch(t *testing.T, stripPointerMetadata bool) *skipFetchFixture {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	// Only the roots of the depth-10 trees are large enough to be cached.
	flags.Set(t, "cache.tree_cache_min_descendents", 2000)
	flags.Set(t, "cache.get_tree_subtree_support", true)

	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	c, err := memory_cache.NewMemoryCache(1000000000)
	require.NoError(t, err)
	spy := &treeCacheSpy{Cache: c, stripPointerMetadata: stripPointerMetadata}
	te.SetCache(spy)
	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	child1, _ := cas.MakeTree(ctx, t, bsClient, "", 10, 2)
	child2, _ := cas.MakeTree(ctx, t, bsClient, "", 10, 2)
	child3, _ := cas.MakeTree(ctx, t, bsClient, "", 1, 1)
	rootDigest1, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, &repb.Directory{
		Directories: []*repb.DirectoryNode{{Name: "child1", Digest: child1}, {Name: "child2", Digest: child2}},
	})
	require.NoError(t, err)
	rootDigest2, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, &repb.Directory{
		Directories: []*repb.DirectoryNode{{Name: "child2", Digest: child2}, {Name: "child3", Digest: child3}},
	})
	require.NoError(t, err)

	nestedRoot, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, &repb.Directory{
		Directories: []*repb.DirectoryNode{{Name: "nested", Digest: rootDigest2}},
	})
	require.NoError(t, err)

	cas.ReadTree(ctx, t, casClient, "", rootDigest1)
	spy.waitForPointers(t, 2)
	// From here on, only read; don't write tree cache entries.
	flags.Set(t, "cache.tree_cache_write_probability", 0.0)
	return &skipFetchFixture{ctx: ctx, spy: spy, cache: c, casClient: casClient, rootDigest1: rootDigest1, rootDigest2: rootDigest2, nestedRoot: nestedRoot}
}

func TestGetTreeSkipCachedSubtreeFetch(t *testing.T) {
	f := setupSkipFetch(t, false)

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", false)
	f.spy.resetCounts()
	want := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	gets, findMissing := f.spy.counts()
	require.Len(t, want.subtrees, 1)
	assert.Positive(t, gets)
	assert.Zero(t, findMissing)

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)
	f.spy.resetCounts()
	got := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	gets, findMissing = f.spy.counts()
	assert.Equal(t, want, got)
	assert.Zero(t, gets)
	assert.Positive(t, findMissing)
}

func TestGetTreeSkipCachedSubtreeFetch_SubtreeBelowMinDirCountIsSentInline(t *testing.T) {
	f := setupSkipFetch(t, false)
	flags.Set(t, "cache.get_tree_subtree_min_dir_count", 100000)

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", false)
	want := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	require.Empty(t, want.subtrees)

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)
	f.spy.resetCounts()
	got := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	gets, _ := f.spy.counts()
	assert.Equal(t, want, got)
	assert.Positive(t, gets)
	f.assertCompleteTree(t, f.rootDigest2, got)
}

func TestGetTreeSkipCachedSubtreeFetch_OldFormatPointer(t *testing.T) {
	f := setupSkipFetch(t, true)
	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", false)
	want := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	require.Len(t, want.subtrees, 1)
	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)

	f.spy.resetCounts()
	got := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	gets, findMissing := f.spy.counts()
	assert.Equal(t, want, got)
	assert.Positive(t, gets)
	assert.Zero(t, findMissing)
}

func TestGetTreeSkipCachedSubtreeFetch_MissingBlobFallsBackToWalk(t *testing.T) {
	f := setupSkipFetch(t, false)
	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", false)
	full := getTree(f.ctx, t, f.casClient, f.rootDigest2, false)

	// Evict the blobs of both cached subtrees (child1 and child2).
	for i := range 2 {
		require.NoError(t, f.cache.Delete(f.ctx, f.spy.blobRN(i)))
	}

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)
	got := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	assert.Empty(t, got.subtrees)
	assert.Equal(t, full.dirs, got.dirs)
}

func TestGetTreeSkipCachedSubtreeFetch_MissingSplitChildFallsBackToWalk(t *testing.T) {
	flags.Set(t, "cache.tree_cache_splitting_min_size", 1000)
	f := setupSplitSkipFetch(t)

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)
	f.spy.resetCounts()
	got := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	gets, findMissing := f.spy.counts()
	require.Contains(t, got.subtrees, f.splitRootBlob.GetDigest().GetHash())
	assert.Zero(t, gets)
	assert.Positive(t, findMissing)
	f.assertCompleteTree(t, f.rootDigest2, got)

	require.NoError(t, f.cache.Delete(f.ctx, f.splitChildBlob))
	got = getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	assert.NotContains(t, got.subtrees, f.splitRootBlob.GetDigest().GetHash())
	f.assertCompleteTree(t, f.rootDigest2, got)
}

func TestGetTreeSkipCachedSubtreeFetch_WriteAboveCachedSubtreeIncludesContents(t *testing.T) {
	f := setupSkipFetch(t, false)
	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)

	// The directory at level 1 contains the cached child2 and child3, so it is written.
	f.spy.mu.Lock()
	before := len(f.spy.blobWrites)
	f.spy.mu.Unlock()
	f.spy.resetCounts()
	getTreeWithSubtrees(f.ctx, t, f.casClient, f.nestedRoot)
	gets, _ := f.spy.counts()
	assert.Positive(t, gets, "contents of the cached subtree are needed for the write")

	require.Eventually(t, func() bool {
		f.spy.mu.Lock()
		defer f.spy.mu.Unlock()
		for _, tc := range f.spy.blobWrites[before:] {
			// child2 (2047 dirs) + child3 (2 dirs) + the nested dir itself.
			if len(tc.GetChildren()) >= 2050 {
				return true
			}
		}
		return false
	}, 30*time.Second, 10*time.Millisecond)
}

func setupSplitSkipFetch(t *testing.T) *skipFetchFixture {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	flags.Set(t, "cache.tree_cache_min_descendents", 2000)
	flags.Set(t, "cache.get_tree_subtree_support", true)
	const seed = "split-skip-fetch-test"
	flags.Set(t, "cache.tree_cache_seed", seed)

	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	c, err := memory_cache.NewMemoryCache(1000000000)
	require.NoError(t, err)
	spy := &treeCacheSpy{Cache: c}
	te.SetCache(spy)
	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	nodeModules, _ := cas.MakeTree(ctx, t, bsClient, "", 10, 2)
	child3, _ := cas.MakeTree(ctx, t, bsClient, "", 1, 1)
	// Distinct from child3 so that no directory appears both inline and in a subtree.
	child4, _ := cas.MakeTree(ctx, t, bsClient, "", 1, 1)
	upload := func(d *repb.Directory) *repb.Digest {
		rn, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, d)
		require.NoError(t, err)
		return rn
	}
	inner := upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "node_modules", Digest: nodeModules}, {Name: "child3", Digest: child3}}})
	w := upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "inner", Digest: inner}}})
	x := upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "w", Digest: w}}})
	y := upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "w", Digest: w}, {Name: "child4", Digest: child4}}})

	// Caches w, inner and node_modules.
	cas.ReadTree(ctx, t, casClient, "", x)
	spy.waitForPointers(t, 3)
	flags.Set(t, "cache.tree_cache_write_probability", 0.0)

	f := &skipFetchFixture{ctx: ctx, spy: spy, cache: c, casClient: casClient, rootDigest2: y}
	// The split entry is w's: it is the one reached from y as a cached subtree.
	wPointer := treeCachePointerHash(t, w.GetHash(), seed)
	spy.mu.Lock()
	idx := slices.IndexFunc(spy.pointerRNs, func(rn *rspb.ResourceName) bool {
		return rn.GetCacheType() == rspb.CacheType_AC && rn.GetDigest().GetHash() == wPointer
	})
	var wAR *repb.ActionResult
	if idx >= 0 {
		wAR = spy.pointers[idx]
	}
	spy.mu.Unlock()
	require.GreaterOrEqual(t, idx, 0, "expected a tree cache pointer for w")
	meta := &capb.TreeCachePointerMetadata{}
	require.NoError(t, wAR.GetExecutionMetadata().GetAuxiliaryMetadata()[0].UnmarshalTo(meta))
	require.NotEmpty(t, meta.GetTreeCacheChildren(), "expected w's entry to be split")
	f.splitRootBlob = spy.blobRN(idx)
	f.splitChildBlob = meta.GetTreeCacheChildren()[0]
	return f
}

// treeCachePointerHash returns the hash of the tree cache pointer key for a
// directory.
func treeCachePointerHash(t *testing.T, dirHash, seed string) string {
	d, err := digest.Compute(strings.NewReader(dirHash+seed), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	return d.GetHash()
}

func (c *treeCacheSpy) setStripPointerMetadata(strip bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stripPointerMetadata = strip
}

// lastPointerMetadata returns the metadata of the latest pointer written for
// the directory with the given digest.
func (c *treeCacheSpy) lastPointerMetadata(t *testing.T, dirHash, seed string) *capb.TreeCachePointerMetadata {
	ptr := treeCachePointerHash(t, dirHash, seed)
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, ar := range slices.Backward(c.pointers) {
		if c.pointerRNs[i].GetDigest().GetHash() != ptr {
			continue
		}
		meta := &capb.TreeCachePointerMetadata{}
		require.NotEmpty(t, ar.GetExecutionMetadata().GetAuxiliaryMetadata(), "pointer has no metadata")
		require.NoError(t, ar.GetExecutionMetadata().GetAuxiliaryMetadata()[0].UnmarshalTo(meta))
		return meta
	}
	require.FailNow(t, "no pointer for directory", dirHash)
	return nil
}

// waitForPointerWrites waits until at least n pointers have been written for
// the directory with the given digest.
func (c *treeCacheSpy) waitForPointerWrites(t *testing.T, dirHash, seed string, n int) {
	ptr := treeCachePointerHash(t, dirHash, seed)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		writes := 0
		for _, rn := range c.pointerRNs {
			if rn.GetDigest().GetHash() == ptr {
				writes++
			}
		}
		return writes >= n
	}, 30*time.Second, 10*time.Millisecond)
}

func (c *treeCacheSpy) pointerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pointers)
}

// countTreeDirs returns the number of directories in a GetTree response,
// counting duplicates.
func countTreeDirs(ctx context.Context, t *testing.T, casClient repb.ContentAddressableStorageClient, rootDigest *repb.Digest) int {
	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{RootDigest: rootDigest})
	require.NoError(t, err)
	n := 0
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			return n
		}
		require.NoError(t, err)
		n += len(rsp.GetDirectories())
	}
}

func TestGetTreeSkipCachedSubtreeFetch_BackfillsOldFormatPointer(t *testing.T) {
	f := setupSkipFetch(t, true)
	f.spy.setStripPointerMetadata(false)
	flags.Set(t, "cache.tree_cache_pointer_backfill_probability", 1.0)
	n := f.spy.pointerCount()
	want := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	require.Len(t, want.subtrees, 1)
	f.spy.waitForPointers(t, n+1)

	flags.Set(t, "cache.get_tree_skip_cached_subtree_fetch", true)
	f.spy.resetCounts()
	got := getTreeWithSubtrees(f.ctx, t, f.casClient, f.rootDigest2)
	gets, findMissing := f.spy.counts()
	assert.Equal(t, want, got)
	assert.Zero(t, gets)
	assert.Positive(t, findMissing)
}

// setupSharedSplitTree reads a tree whose cached directory "top" contains the
// same split node_modules under two paths, and returns the root and top.
func setupSharedSplitTree(t *testing.T, stripPointerMetadata bool) (ctx context.Context, spy *treeCacheSpy, casClient repb.ContentAddressableStorageClient, root, top *repb.Digest, seed string) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	flags.Set(t, "cache.tree_cache_min_descendents", 3000)
	flags.Set(t, "cache.tree_cache_splitting_min_size", 1000)
	flags.Set(t, "cache.get_tree_subtree_support", true)
	seed = "shared-split-test"
	flags.Set(t, "cache.tree_cache_seed", seed)

	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	c, err := memory_cache.NewMemoryCache(1000000000)
	require.NoError(t, err)
	spy = &treeCacheSpy{Cache: c, stripPointerMetadata: stripPointerMetadata}
	te.SetCache(spy)
	clientConn := runCASServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient = repb.NewContentAddressableStorageClient(clientConn)

	nodeModules, _ := cas.MakeTree(ctx, t, bsClient, "", 10, 2)
	child3, _ := cas.MakeTree(ctx, t, bsClient, "", 1, 1)
	child4, _ := cas.MakeTree(ctx, t, bsClient, "", 1, 1)
	upload := func(d *repb.Directory) *repb.Digest {
		rn, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, d)
		require.NoError(t, err)
		return rn
	}
	a := upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "node_modules", Digest: nodeModules}, {Name: "child3", Digest: child3}}})
	b := upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "node_modules", Digest: nodeModules}, {Name: "child4", Digest: child4}}})
	top = upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "a", Digest: a}, {Name: "b", Digest: b}}})
	root = upload(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "top", Digest: top}}})

	// Only top (~4100 dirs) is big enough to cache, so its pointer write is the
	// last background write; a, b and node_modules (~2050 each) are not cached.
	cas.ReadTree(ctx, t, casClient, "", root)
	spy.waitForPointerWrites(t, top.GetHash(), seed, 1)
	flags.Set(t, "cache.tree_cache_write_probability", 0.0)
	return
}

func TestTreeCachePointerDirCount_SharedSplitChildCountedOnce(t *testing.T) {
	ctx, spy, casClient, root, top, seed := setupSharedSplitTree(t, false)

	meta := spy.lastPointerMetadata(t, top.GetHash(), seed)
	require.Len(t, meta.GetTreeCacheChildren(), 1, "the split child appears under two paths but is stored once")
	// The root directory is not part of the cached entry.
	assert.Equal(t, countTreeDirs(ctx, t, casClient, root)-1, int(meta.GetDirCount()))
}

func TestTreeCachePointerDirCount_BackfillMatchesRead(t *testing.T) {
	ctx, spy, casClient, root, top, seed := setupSharedSplitTree(t, true)
	spy.setStripPointerMetadata(false)
	flags.Set(t, "cache.tree_cache_pointer_backfill_probability", 1.0)
	read := countTreeDirs(ctx, t, casClient, root) - 1
	spy.waitForPointerWrites(t, top.GetHash(), seed, 2)

	meta := spy.lastPointerMetadata(t, top.GetHash(), seed)
	assert.Equal(t, read, int(meta.GetDirCount()))
	require.Len(t, meta.GetTreeCacheChildren(), 1)
}

// replayHookTree uploads deterministic directories, so that the same tree can
// be rebuilt in a fresh cache.
type replayHookTree struct {
	ctx context.Context
	t   *testing.T
	bs  bspb.ByteStreamClient
}

func (b *replayHookTree) dir(d *repb.Directory) *repb.Digest {
	rn, err := cachetools.UploadProto(b.ctx, b.bs, "", repb.DigestFunction_SHA256, d)
	require.NoError(b.t, err)
	return rn
}

// mid returns a directory holding two files-only leaves.
func (b *replayHookTree) mid(tag string) *repb.Digest {
	leaf := func(name string) *repb.DirectoryNode {
		d := b.dir(&repb.Directory{Files: []*repb.FileNode{{Name: name, Digest: &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1}}}})
		return dirNode(name, d)
	}
	return b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{leaf(tag + "-0"), leaf(tag + "-1")}})
}

func dirNode(name string, d *repb.Digest) *repb.DirectoryNode {
	return &repb.DirectoryNode{Name: name, Digest: d}
}

func setupReplayHookTest(t *testing.T, configure func(*content_addressable_storage_server.ContentAddressableStorageServer)) (*replayHookTree, *treeCacheSpy, repb.ContentAddressableStorageClient) {
	flags.Set(t, "cache.tree_cache_seed", "replay-hooks-test")
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	c, err := memory_cache.NewMemoryCache(1000000000)
	require.NoError(t, err)
	spy := &treeCacheSpy{Cache: c}
	te.SetCache(spy)

	conn := runCASServer(ctx, t, te, configure)
	return &replayHookTree{ctx: ctx, t: t, bs: bspb.NewByteStreamClient(conn)}, spy, repb.NewContentAddressableStorageClient(conn)
}

// treeCacheKeys returns the key of every tree cache Set, sorted, one entry per
// call.
func (c *treeCacheSpy) treeCacheKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var keys []string
	for _, rn := range c.pointerRNs {
		keys = append(keys, "ac/"+rn.GetDigest().GetHash())
	}
	for _, rn := range c.blobWriteRNs {
		keys = append(keys, "cas/"+rn.GetDigest().GetHash())
	}
	slices.Sort(keys)
	return keys
}

func (c *treeCacheSpy) pointerKeys() []string {
	return slices.DeleteFunc(c.treeCacheKeys(), func(k string) bool { return !strings.HasPrefix(k, "ac/") })
}

func replayHookPointerKey(t *testing.T, dirHash string) string {
	return "ac/" + treeCachePointerHash(t, dirHash, "replay-hooks-test")
}

// replayHookFixture builds root -> top -> {x, y}, where x and y each have two
// leaf directories. top, x and y are cacheable.
func replayHookFixture(b *replayHookTree) (root, top, midX, midY *repb.Digest) {
	midX, midY = b.mid("x"), b.mid("y")
	top = b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("x", midX), dirNode("y", midY)}})
	root = b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("top", top)}})
	return
}

func TestGetTreeReplayHooks_RollDecidesWrites(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 0.5)
	var midX *repb.Digest
	b, spy, casClient := setupReplayHookTest(t, func(s *content_addressable_storage_server.ContentAddressableStorageServer) {
		s.SetSynchronousTreeCacheWrites(true)
		s.SetTreeCacheWriteRoll(func(ctx context.Context, dir *rspb.ResourceName) float64 {
			if dir.GetDigest().GetHash() == midX.GetHash() {
				return 0
			}
			return 1
		})
	})
	var root *repb.Digest
	root, _, midX, _ = replayHookFixture(b)

	getTree(b.ctx, t, casClient, root, false)

	require.Equal(t, []string{replayHookPointerKey(t, midX.GetHash())}, spy.pointerKeys())
}

func TestGetTreeReplayHooks_RollIsCalledOncePerCacheableDirectory(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 0.5)
	var mu sync.Mutex
	var rolled []string
	b, _, casClient := setupReplayHookTest(t, func(s *content_addressable_storage_server.ContentAddressableStorageServer) {
		s.SetTreeCacheWriteRoll(func(ctx context.Context, dir *rspb.ResourceName) float64 {
			mu.Lock()
			defer mu.Unlock()
			rolled = append(rolled, dir.GetDigest().GetHash())
			return 1
		})
	})
	root, top, midX, midY := replayHookFixture(b)

	getTree(b.ctx, t, casClient, root, false)

	// The root is below the min level, and leaves have no subdirectories.
	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []string{top.GetHash(), midX.GetHash(), midY.GetHash()}, rolled)
}

func TestGetTreeReplayHooks_SynchronousWritesAreDoneWhenGetTreeReturns(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	b, spy, casClient := setupReplayHookTest(t, func(s *content_addressable_storage_server.ContentAddressableStorageServer) {
		s.SetSynchronousTreeCacheWrites(true)
	})
	root, top, midX, midY := replayHookFixture(b)
	gate := make(chan struct{})
	spy.gate = gate
	var closeGate sync.Once
	t.Cleanup(func() { closeGate.Do(func() { close(gate) }) })

	stream, err := casClient.GetTree(b.ctx, &repb.GetTreeRequest{RootDigest: root})
	require.NoError(t, err)
	// root, top, x, y and their four leaves.
	for n := 0; n < 8; {
		rsp, err := stream.Recv()
		require.NoError(t, err)
		n += len(rsp.GetDirectories())
	}
	// Synchronous writes only start after the last Send, so they hold up the
	// end of the stream, not the directories. Background writes would still be
	// held by the gate here and complete after the walk.
	closeGate.Do(func() { close(gate) })
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)

	want := []string{
		replayHookPointerKey(t, top.GetHash()),
		replayHookPointerKey(t, midX.GetHash()),
		replayHookPointerKey(t, midY.GetHash()),
	}
	require.ElementsMatch(t, want, spy.pointerKeys())
}

func TestGetTreeReplayHooks_RequestDoesNotHitItsOwnWrites(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	var top *repb.Digest
	b, spy, casClient := setupReplayHookTest(t, func(s *content_addressable_storage_server.ContentAddressableStorageServer) {
		s.SetSynchronousTreeCacheWrites(true)
		// Leave top uncached so that the second request looks up d.
		s.SetTreeCacheWriteRoll(func(ctx context.Context, dir *rspb.ResourceName) float64 {
			if dir.GetDigest().GetHash() == top.GetHash() {
				return 2
			}
			return 0
		})
	})
	d := b.mid("d")
	top = b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("a", d), dirNode("b", d)}})
	root := b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("top", top)}})

	getTree(b.ctx, t, casClient, root, false)
	gets, _ := spy.counts()
	require.Zero(t, gets, "first request must not read tree cache blobs")
	require.Equal(t, []string{replayHookPointerKey(t, d.GetHash())}, spy.pointerKeys(), "d is written once although it is seen twice, and top is not written")

	getTree(b.ctx, t, casClient, root, false)
	gets, _ = spy.counts()
	require.Equal(t, 2, gets, "second request hits d under both paths")
}

func TestGetTreeReplayHooks_SynchronousWritesAreDeterministic(t *testing.T) {
	flags.Set(t, "cache.tree_cache_write_probability", 1.0)
	flags.Set(t, "cache.tree_cache_splitting_min_size", 2)

	run := func(t *testing.T) []string {
		var top *repb.Digest
		b, spy, casClient := setupReplayHookTest(t, func(s *content_addressable_storage_server.ContentAddressableStorageServer) {
			s.SetSynchronousTreeCacheWrites(true)
			// A split child shared by two paths of one cached directory is
			// written racily, so keep top out of the cache.
			s.SetTreeCacheWriteRoll(func(ctx context.Context, dir *rspb.ResourceName) float64 {
				if dir.GetDigest().GetHash() == top.GetHash() {
					return 2
				}
				return 0
			})
		})
		nodeModules := b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{
			dirNode("m0", b.mid("m0")), dirNode("m1", b.mid("m1")), dirNode("m2", b.mid("m2")),
		}})
		a := b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("node_modules", nodeModules), dirNode("c", b.mid("c"))}})
		bb := b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("node_modules", nodeModules), dirNode("d", b.mid("d"))}})
		top = b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("a", a), dirNode("b", bb)}})
		root := b.dir(&repb.Directory{Directories: []*repb.DirectoryNode{dirNode("top", top)}})

		getTree(b.ctx, t, casClient, root, false)
		return spy.treeCacheKeys()
	}

	first := run(t)
	require.NotEmpty(t, first)
	// Split children are blobs without a pointer.
	pointers := 0
	for _, k := range first {
		if strings.HasPrefix(k, "ac/") {
			pointers++
		}
	}
	require.Greater(t, len(first)-pointers, pointers, "expected a split child blob")
	for range 5 {
		require.Equal(t, first, run(t))
	}
}
