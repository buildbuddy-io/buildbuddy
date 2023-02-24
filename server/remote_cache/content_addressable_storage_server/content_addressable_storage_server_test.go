package content_addressable_storage_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
)

func runCASServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

type evilCache struct {
	interfaces.Cache
}

func (e *evilCache) GetMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	rsp, err := e.Cache.GetMulti(ctx, resources)
	for d := range rsp {
		rsp[d] = []byte{}
	}
	return rsp, err
}

func TestBatchUpdateBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	req := &repb.BatchUpdateBlobsRequest{}
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest: d,
			Data:   buf,
		})
	}
	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 100, len(rsp.GetResponses()))
	for _, singleRsp := range rsp.GetResponses() {
		assert.Equal(t, int32(gcodes.OK), singleRsp.GetStatus().GetCode())
	}
}

func TestBatchUpdateAndReadCompressedBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", true)
	flags.Set(t, "cache.detailed_stats_enabled", true)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	te.SetMetricsCollector(mc)
	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := compression.CompressZstd(nil, blob)

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob))
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
			require.Equal(t, int32(codes.OK), resp.Status.Code, "BatchUpdateResponse[%d].Status != OK", i)
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
		require.Equal(t, int32(codes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i)
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
		require.Equal(t, int32(codes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i)
		blobs[i] = resp.Data
	}
	require.Equal(t, [][]byte{blob}, blobs)
}

func TestBatchUpdateRejectsCompressedBlobsIfCompressionDisabled(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", false)
	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := compression.CompressZstd(nil, blob)

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob))
	require.NoError(t, err)

	// Upload compressed blob via BatchUpdate.
	batchUpdateResp, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{Digest: d, Data: compressedBlob, Compressor: repb.Compressor_ZSTD},
		},
	})
	require.NoError(t, err)
	for i, resp := range batchUpdateResp.Responses {
		require.Equal(t, int32(codes.Unimplemented), resp.Status.Code, "BatchUpdateResponse[%d].Status != Unimplemented", i)
	}
}

func TestBatchUpdateRejectCorruptBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	req := &repb.BatchUpdateBlobsRequest{}
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	buf[0] = ^buf[0] // corrupt the data in buf
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d,
		Data:   buf,
	})

	d2, buf := testdigest.NewRandomDigestBuf(t, 100)
	d2.SizeBytes += 1 // corrupt the payload size of d2
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d2,
		Data:   buf,
	})

	d3, buf := testdigest.NewRandomDigestBuf(t, 100)
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d3,
		Data:   buf,
	})

	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(rsp.GetResponses()))
	assert.Equal(t, int32(gcodes.DataLoss), rsp.GetResponses()[0].GetStatus().GetCode())
	assert.Equal(t, int32(gcodes.DataLoss), rsp.GetResponses()[1].GetStatus().GetCode())
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
			clientConn := runCASServer(ctx, te, t)
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
			d, err := digest.Compute(bytes.NewReader(blob))
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
					require.Equal(t, int32(codes.OK), resp.Status.Code, "BatchUpdateResponse[%d].Status != OK", i, tc.name)
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
				require.Equal(t, int32(codes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i, tc.name)
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	c, err := memory_cache.NewMemoryCache(1000000)
	if err != nil {
		t.Fatal(err)
	}
	te.SetCache(&evilCache{c})
	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	set, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{
				Digest: d,
				Data:   buf,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(set.GetResponses()))
	assert.Equal(t, d.GetHash(), set.GetResponses()[0].GetDigest().GetHash())
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

func makeTree(ctx context.Context, t *testing.T, bsClient bspb.ByteStreamClient, instanceName string, depth, branchingFactor int) (*repb.Digest, []string) {
	numFiles := int(math.Pow(float64(branchingFactor), float64(depth)))
	fileNames := make([]string, 0, numFiles)
	var leafNodes []*repb.DirectoryNode

	for d := depth; d > 0; d-- {
		numNodes := int(math.Pow(float64(branchingFactor), float64(d)))
		nextLeafNodes := make([]*repb.DirectoryNode, 0, numNodes)
		for n := 0; n < numNodes; n++ {
			subdir := &repb.Directory{}
			if d == depth {
				d, buf := testdigest.NewRandomDigestBuf(t, 100)
				_, err := cachetools.UploadBlob(ctx, bsClient, instanceName, bytes.NewReader(buf))
				require.NoError(t, err)
				fileName := fmt.Sprintf("leaf-file-%s-%d", d.GetHash(), n)
				fileNames = append(fileNames, fileName)
				subdir.Files = append(subdir.Files, &repb.FileNode{
					Name:   fileName,
					Digest: d,
				})
			} else {
				start := n * branchingFactor
				end := branchingFactor + start
				subdir.Directories = append(subdir.Directories, leafNodes[start:end]...)
			}

			subdirDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, subdir)
			require.NoError(t, err)
			dirName := fmt.Sprintf("node-%s-depth-%d-node-%d", subdirDigest.GetHash(), d, n)
			fileNames = append(fileNames, dirName)
			nextLeafNodes = append(nextLeafNodes, &repb.DirectoryNode{
				Name:   dirName,
				Digest: subdirDigest,
			})
		}
		leafNodes = nextLeafNodes
	}

	parentDir := &repb.Directory{
		Directories: leafNodes,
	}
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, parentDir)
	require.NoError(t, err)
	return rootDigest, fileNames
}

func readTree(ctx context.Context, t *testing.T, casClient repb.ContentAddressableStorageClient, instanceName string, rootDigest *repb.Digest) []string {
	// Fetch the tree, and return contents.
	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
		InstanceName: instanceName,
		RootDigest:   rootDigest,
	})
	assert.Nil(t, err)

	names := make([]string, 0)

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, dir := range rsp.GetDirectories() {
			for _, file := range dir.GetFiles() {
				names = append(names, file.GetName())
			}
			for _, subdir := range dir.GetDirectories() {
				names = append(names, subdir.GetName())
			}
		}
	}
	return names
}

func TestGetTree(t *testing.T) {
	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	// Upload a dir containing fileCount files, and return the file
	// names and directory digest.
	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return makeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, child1Files := uploadDirWithFiles(2, 1)
	child2Digest, child2Files := uploadDirWithFiles(2, 1)

	// Upload a root directory containing both child directories.
	rootDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			&repb.DirectoryNode{
				Name:   "child1",
				Digest: child1Digest,
			},
			&repb.DirectoryNode{
				Name:   "child2",
				Digest: child2Digest,
			},
		},
	}
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, rootDir)
	assert.Nil(t, err)

	allFiles := append(child1Files, child2Files...)
	allFiles = append(allFiles, "child1", "child2")
	treeFiles := readTree(ctx, t, casClient, instanceName, rootDigest)
	assert.ElementsMatch(t, allFiles, treeFiles)
}

func TestGetTreeCaching(t *testing.T) {
	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	uploadDirWithFiles := func(depth, branchingFactor int) (*repb.Digest, []string) {
		return makeTree(ctx, t, bsClient, instanceName, depth, branchingFactor)
	}

	child1Digest, child1Files := uploadDirWithFiles(10, 2)
	child2Digest, child2Files := uploadDirWithFiles(10, 2)
	child3Digest, child3Files := uploadDirWithFiles(1, 1)

	// Upload a root directory containing both child directories.
	rootDir1 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			&repb.DirectoryNode{
				Name:   "child1",
				Digest: child1Digest,
			},
			&repb.DirectoryNode{
				Name:   "child2",
				Digest: child2Digest,
			},
		},
	}
	rootDigest1, err := cachetools.UploadProto(ctx, bsClient, instanceName, rootDir1)
	assert.Nil(t, err)

	rootDir2 := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			&repb.DirectoryNode{
				Name:   "child2",
				Digest: child2Digest,
			},
			&repb.DirectoryNode{
				Name:   "child3",
				Digest: child3Digest,
			},
		},
	}
	rootDigest2, err := cachetools.UploadProto(ctx, bsClient, instanceName, rootDir2)
	assert.Nil(t, err)

	uploadedFiles1 := append(child1Files, child2Files...)
	uploadedFiles1 = append(uploadedFiles1, "child1", "child2")

	start := time.Now()
	treeFiles1 := readTree(ctx, t, casClient, instanceName, rootDigest1)
	fetch1Time := time.Since(start)

	assert.ElementsMatch(t, uploadedFiles1, treeFiles1)

	uploadedFiles2 := append(child2Files, child3Files...)
	uploadedFiles2 = append(uploadedFiles2, "child2", "child3")
	start = time.Now()
	treeFiles2 := readTree(ctx, t, casClient, instanceName, rootDigest2)
	fetch2Time := time.Since(start)

	assert.ElementsMatch(t, uploadedFiles2, treeFiles2)
	assert.Less(t, fetch2Time, fetch1Time/2)
}
