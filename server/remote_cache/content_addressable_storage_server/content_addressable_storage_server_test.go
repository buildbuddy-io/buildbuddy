package content_addressable_storage_server_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcodes "google.golang.org/grpc/codes"
)

func runCASServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)

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

func (e *evilCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	rsp, err := e.Cache.GetMulti(ctx, digests)
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
	flags.Set(t, "cache.zstd_transcoding_enabled", "true")
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
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
	readResp, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests:               []*repb.Digest{d},
		AcceptableCompressors: []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD},
	})

	require.NoError(t, err)
	decompressedBlobs := make([][]byte, len(readResp.Responses))
	for i, resp := range readResp.Responses {
		require.Equal(t, int32(codes.OK), resp.Status.Code, "BatchReadResponse[%d].Status != OK", i)
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
	flags.Set(t, "cache.zstd_transcoding_enabled", "false")
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
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
