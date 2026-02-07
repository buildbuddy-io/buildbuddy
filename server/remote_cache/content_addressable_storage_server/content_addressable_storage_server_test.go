package content_addressable_storage_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/fastcdc2020/fastcdc"
	"github.com/google/uuid"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func runCASServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
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
	for i := 0; i < 100; i++ {
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
	flags.Set(t, "cache.tree_cache_splitting", true)
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
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.split_splice_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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

func TestSplitBlobNotFound(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.split_splice_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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

func TestSpliceBlobSingleChunk(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.split_splice_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache.split_splice_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	flags.Set(t, "cache.max_chunk_size_bytes", 100)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runCASServer(ctx, t, te)
	casClient := repb.NewContentAddressableStorageClient(clientConn)
	cache := te.GetCache()

	chunk1 := []byte("This is the first chunk of data. ")
	chunk2 := []byte("This is the second chunk of data. ")
	chunk3 := []byte("This is the third and final chunk.")
	fullBlob := append(append(chunk1, chunk2...), chunk3...)

	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	chunk1Digest, err := digest.Compute(bytes.NewReader(chunk1), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	chunk2Digest, err := digest.Compute(bytes.NewReader(chunk2), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	chunk3Digest, err := digest.Compute(bytes.NewReader(chunk3), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	chunk1RN := digest.NewCASResourceName(chunk1Digest, "", repb.DigestFunction_SHA256)
	chunk2RN := digest.NewCASResourceName(chunk2Digest, "", repb.DigestFunction_SHA256)
	chunk3RN := digest.NewCASResourceName(chunk3Digest, "", repb.DigestFunction_SHA256)

	require.NoError(t, cache.Set(ctx, chunk1RN.ToProto(), chunk1))
	require.NoError(t, cache.Set(ctx, chunk2RN.ToProto(), chunk2))
	require.NoError(t, cache.Set(ctx, chunk3RN.ToProto(), chunk3))

	manifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1Digest, chunk2Digest, chunk3Digest},
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
}

func TestSpliceBlobReadOnlyKey(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))

	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	te.SetExperimentFlagProvider(fp)

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

	_, err = casClient.SpliceBlob(ctx, spliceReq)
	require.NoError(t, err)
}
