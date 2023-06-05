package byte_stream_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	guuid "github.com/google/uuid"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultBazelVersion = "6.0.0"
)

func runByteStreamServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	byteStreamServer, err := NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	go runFunc()

	// TODO(vadim): can we remove the MsgSize override from the default options?
	clientConn, err := env.LocalGRPCConn(ctx, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func readBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, out io.Writer, offset int64) error {
	downloadString, err := r.DownloadString()
	if err != nil {
		return err
	}
	req := &bspb.ReadRequest{
		ResourceName: downloadString,
		ReadOffset:   offset,
		ReadLimit:    r.GetDigest().GetSizeBytes(),
	}
	stream, err := bsClient.Read(ctx, req)
	if err != nil {
		return err
	}

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		out.Write(rsp.Data)
	}
	return nil
}

func TestRPCRead(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	randStr := func(i int) string {
		rstr, err := random.RandomString(i)
		if err != nil {
			t.Error(err)
		}
		return rstr
	}
	cases := []struct {
		wantError    error
		resourceName *digest.ResourceName
		wantData     string
		offset       int64
	}{
		{ // Simple Read
			resourceName: digest.NewResourceName(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256),

			wantData:  randStr(1234),
			wantError: nil,
			offset:    0,
		},
		{ // Large Read
			resourceName: digest.NewResourceName(&repb.Digest{
				Hash:      "ffd14ebb6c1b2701ac793ea1aff6dddf8540e734bd6d051ac2a24aa3ec062781",
				SizeBytes: 1000 * 1000 * 100,
			}, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256),

			wantData:  randStr(1000 * 1000 * 100),
			wantError: nil,
			offset:    0,
		},
		{ // 0 length read
			resourceName: digest.NewResourceName(&repb.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256),

			wantData:  "",
			wantError: nil,
			offset:    0,
		},
		{ // Offset
			resourceName: digest.NewResourceName(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256),

			wantData:  randStr(1234),
			wantError: nil,
			offset:    1,
		},
		{ // Max offset
			resourceName: digest.NewResourceName(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256),

			wantData:  randStr(1234),
			wantError: nil,
			offset:    1234,
		},
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	for _, tc := range cases {
		// Set the value in the cache.
		if err := te.GetCache().Set(ctx, tc.resourceName.ToProto(), []byte(tc.wantData)); err != nil {
			t.Fatal(err)
		}

		// Now read it back with the bytestream API.
		var buf bytes.Buffer
		gotErr := readBlob(ctx, bsClient, tc.resourceName, &buf, tc.offset)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("got %v; want %v", gotErr, tc.wantError)
			//			continue
		}
		got := string(buf.Bytes())
		if got != tc.wantData[tc.offset:] {
			t.Errorf("got %.100s; want %.100s", got, tc.wantData)
		}
	}
}

func TestRPCWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a regular bytestream upload works.
	d, readSeeker := testdigest.NewRandomDigestReader(t, 1000)
	instanceNameDigest := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCMalformedWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a malformed upload (incorrect digest) is rejected.
	instanceNameDigest, buf := testdigest.RandomCASResourceBuf(t, 1000)
	buf[0] = ^buf[0] // flip bits in byte to corrupt digest.

	readSeeker := bytes.NewReader(buf)
	_, err := cachetools.UploadFromReader(ctx, bsClient, digest.ResourceNameFromProto(instanceNameDigest), readSeeker)
	if !status.IsDataLossError(err) {
		t.Fatalf("Expected data loss error but got %s", err)
	}
}

func TestRPCTooLongWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a malformed upload (wrong bytesize) is rejected.
	rnProto, buf := testdigest.RandomCASResourceBuf(t, 1000)
	rnProto.Digest.SizeBytes += 1 // increment expected byte count by 1 to trigger mismatch.
	instanceNameDigest := digest.ResourceNameFromProto(rnProto)

	readSeeker := bytes.NewReader(buf)
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if !status.IsDataLossError(err) {
		t.Fatalf("Expected data loss error but got %s", err)
	}
}

// Tests Read/Write of a blob that exceeds the default gRPC message size.
func TestRPCReadWriteLargeBlob(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	blob, err := random.RandomString(10_000_000)
	require.NoError(t, err)
	d, err := digest.Compute(strings.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	instanceNameDigest := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)

	// Write
	_, err = cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, strings.NewReader(blob))
	require.NoError(t, err)

	// Read
	var buf bytes.Buffer
	err = readBlob(ctx, bsClient, instanceNameDigest, &buf, 0)
	require.NoError(t, err)
	require.Equal(t, blob, string(buf.Bytes()))
}

func TestRPCWriteAndReadCompressed(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", true)
	flags.Set(t, "cache.detailed_stats_enabled", true)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	te.SetMetricsCollector(mc)

	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	for _, blobSize := range []int{1, 1e2, 1e4, 1e6, 8e6} {
		blob := compressibleBlobOfSize(blobSize)
		compressedBlob := compression.CompressZstd(nil, blob)
		require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

		// Note: Digest is of uncompressed contents
		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)

		// ByteStream.Read should return NOT_FOUND initially.
		resourceName := fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
		readStream, err := bsClient.Read(ctx, &bspb.ReadRequest{ResourceName: resourceName})
		require.NoError(t, err)
		_, err = readStream.Recv()

		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "error code should be NOT_FOUND")

		// Upload compressed blob with metadata.
		// Use an invocation context scoped just to this upload.
		{
			rmd := &repb.RequestMetadata{ToolInvocationId: newUUID(t), ActionMnemonic: "GoCompile"}
			ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
			require.NoError(t, err)
			uploadResourceName := fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes)

			mustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, true)

			sc := hit_tracker.ScoreCard(ctx, te, rmd.ToolInvocationId)
			require.Len(t, sc.Results, 1)
			assert.Equal(t, d.GetHash(), sc.Results[0].Digest.Hash)
			assert.Equal(t, repb.Compressor_ZSTD, sc.Results[0].Compressor)
			assert.Equal(t, int64(len(compressedBlob)), sc.Results[0].TransferredSizeBytes)
		}

		// Read back the compressed blob we just uploaded, using a new invocation
		// context. After decompressing, should get back the original blob contents.
		rmd := &repb.RequestMetadata{ToolInvocationId: newUUID(t), ActionMnemonic: "GoCompile"}
		ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
		require.NoError(t, err)
		downloadResourceName := fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
		downloadBuf := []byte{}
		downloadStream, err := bsClient.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadResourceName,
		})
		require.NoError(t, err)
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			downloadBuf = append(downloadBuf, res.Data...)
		}
		decompressedBlob := zstdDecompress(t, downloadBuf)
		require.Equal(t, blob, decompressedBlob)
		sc := hit_tracker.ScoreCard(ctx, te, rmd.ToolInvocationId)
		require.Len(t, sc.Results, 1)
		assert.Equal(t, d.GetHash(), sc.Results[0].Digest.Hash)
		assert.Equal(t, repb.Compressor_ZSTD, sc.Results[0].Compressor)
		assert.Equal(t, int64(len(downloadBuf)), sc.Results[0].TransferredSizeBytes)
	}
}

func TestRPCWriteCompressedReadUncompressed(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Note: Some larger blob sizes are included here so that we have a better
	// chance of exercising the scenario where the gRPC client sends a burst of
	// write requests at once without waiting to see if the server sends back
	// a response (this is a common scenario in client-streaming uploads).
	for _, blobSize := range []int{1, 1e2, 1e4, 1e6, 8e6, 16e6} {
		blob := compressibleBlobOfSize(blobSize)

		compressedBlob := compression.CompressZstd(nil, blob)
		require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

		// Note: Digest is of uncompressed contents
		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)

		// ByteStream.Read should return NOT_FOUND initially.
		resourceName := fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
		readStream, err := bsClient.Read(ctx, &bspb.ReadRequest{ResourceName: resourceName})
		require.NoError(t, err)
		_, err = readStream.Recv()

		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "error code should be NOT_FOUND")

		// Upload the compressed blob.
		uploadResourceName := fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes)
		mustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, true)

		// Read back the compressed blob we just uploaded, but reference the
		// decompressed resource name. Server should decompress it for us.
		downloadResourceName := fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes)
		downloadBuf := []byte{}
		downloadStream, err := bsClient.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadResourceName,
		})
		require.NoError(t, err)
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			downloadBuf = append(downloadBuf, res.Data...)
		}
		require.Equal(t, blob, downloadBuf)

		// Now try uploading a duplicate. The duplicate upload should not fail,
		// and we should still be able to read the blob.
		mustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, false)

		downloadBuf = []byte{}
		downloadStream, err = bsClient.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadResourceName,
		})
		require.NoError(t, err)
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			downloadBuf = append(downloadBuf, res.Data...)
		}
		require.Equal(t, blob, downloadBuf)
	}
}

func TestRPCWriteUncompressedReadCompressed(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	for _, blobSize := range []int{1, 1e2, 1e4, 1e6, 8e6} {
		blob := compressibleBlobOfSize(blobSize)

		// Note: Digest is of uncompressed contents
		d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
		require.NoError(t, err)

		// Upload uncompressed via bytestream.
		uploadResourceName := fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes)
		mustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, blob, true)

		// Read back the blob we just uploaded, but reference the compressed resource
		// name. Server should serve it back compressed since there is no overhead
		// (zstd storage is enabled).
		downloadResourceName := fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
		downloadBuf := []byte{}
		downloadStream, err := bsClient.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadResourceName,
		})
		require.NoError(t, err)
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			downloadBuf = append(downloadBuf, res.Data...)
		}
		decompressedBlob := zstdDecompress(t, downloadBuf)
		require.Equal(t, blob, decompressedBlob)

	}
}

func Test_CacheHandlesCompression(t *testing.T) {
	// Make blob big enough to require multiple chunks to upload
	blob := compressibleBlobOfSize(5e6)
	compressedBlob := compression.CompressZstd(nil, blob)
	require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	testCases := []struct {
		name                        string
		uploadResourceName          string
		uploadBlob                  []byte
		downloadResourceName        string
		expectedDownloadCompression repb.Compressor_Value
		bazelVersion                string
	}{
		{
			name:                        "Write compressed, read compressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
		{
			name:                        "Write compressed, read decompressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                        "Write decompressed, read decompressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  blob,
			downloadResourceName:        fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                        "Write decompressed, read compressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  blob,
			downloadResourceName:        fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
	}
	for _, tc := range testCases {
		run := func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			ctx := context.Background()
			ctx = withBazelVersion(t, ctx, tc.bazelVersion)

			// Enable compression
			flags.Set(t, "cache.zstd_transcoding_enabled", true)
			te.SetCache(&testcompression.CompressionCache{Cache: te.GetCache()})

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
			require.NoError(t, err)
			clientConn := runByteStreamServer(ctx, te, t)
			bsClient := bspb.NewByteStreamClient(clientConn)

			// Upload the blob
			mustUploadChunked(t, ctx, bsClient, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, true)

			// Read back the blob we just uploaded
			downloadBuf := []byte{}
			downloadStream, err := bsClient.Read(ctx, &bspb.ReadRequest{
				ResourceName: tc.downloadResourceName,
			})
			require.NoError(t, err, tc.name)
			for {
				res, err := downloadStream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, tc.name)
				downloadBuf = append(downloadBuf, res.Data...)
			}

			if tc.expectedDownloadCompression == repb.Compressor_IDENTITY {
				require.Equal(t, blob, downloadBuf, tc.name)
			} else if tc.expectedDownloadCompression == repb.Compressor_ZSTD {
				decompressedDownloadBuf, err := compression.DecompressZstd(nil, downloadBuf)
				require.NoError(t, err, tc.name)
				require.Equal(t, blob, decompressedDownloadBuf, tc.name)
			}

			// Now try uploading a duplicate. The duplicate upload should not fail,
			// and we should still be able to read the blob.
			mustUploadChunked(t, ctx, bsClient, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, false)

			downloadBuf = []byte{}
			downloadStream, err = bsClient.Read(ctx, &bspb.ReadRequest{
				ResourceName: tc.downloadResourceName,
			})
			require.NoError(t, err, tc.name)
			for {
				res, err := downloadStream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, tc.name)
				downloadBuf = append(downloadBuf, res.Data...)
			}

			if tc.expectedDownloadCompression == repb.Compressor_IDENTITY {
				require.Equal(t, blob, downloadBuf, tc.name)
			} else if tc.expectedDownloadCompression == repb.Compressor_ZSTD {
				decompressedDownloadBuf, err := compression.DecompressZstd(nil, downloadBuf)
				require.NoError(t, err, tc.name)
				require.Equal(t, blob, decompressedDownloadBuf, tc.name)
			}
		}

		// Run all tests for both bazel 5.0.0 (which introduced compression) and
		// 5.1.0 (which added support for short-circuiting duplicate compressed
		// uploads)
		tc.bazelVersion = "5.0.0"
		t.Run(tc.name+", bazel "+tc.bazelVersion, run)

		tc.bazelVersion = "5.1.0"
		t.Run(tc.name+", bazel "+tc.bazelVersion, run)
	}
}

func compressibleBlobOfSize(sizeBytes int) []byte {
	out := make([]byte, 0, sizeBytes)
	for len(out) < sizeBytes {
		runEnd := len(out) + 100 + rand.Intn(100)
		if runEnd > sizeBytes {
			runEnd = sizeBytes
		}

		runChar := byte(rand.Intn('Z'-'A'+1)) + 'A'
		for len(out) < runEnd {
			out = append(out, runChar)
		}
	}
	return out
}

func withBazelVersion(t *testing.T, ctx context.Context, version string) context.Context {
	rmd := &repb.RequestMetadata{
		ToolDetails: &repb.ToolDetails{
			ToolName:    "bazel",
			ToolVersion: version,
		},
	}
	ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
	require.NoError(t, err)
	return ctx
}

func mustUploadChunked(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, bazelVersion string, uploadResourceName string, blob []byte, isFirstAttempt bool) {
	ctx = withBazelVersion(t, ctx, bazelVersion)

	uploadStream, err := bsClient.Write(ctx)
	require.NoError(t, err)

	remaining := blob
	for len(remaining) > 0 {
		chunkSize := 1_000_000
		if chunkSize > len(remaining) {
			chunkSize = len(remaining)
		}
		err = uploadStream.Send(&bspb.WriteRequest{
			ResourceName: uploadResourceName,
			WriteOffset:  int64(len(blob) - len(remaining)),
			Data:         remaining[:chunkSize],
			FinishWrite:  chunkSize == len(remaining),
		})
		if err != io.EOF {
			require.NoError(t, err)
		}
		remaining = remaining[chunkSize:]
		if err == io.EOF {
			// Server sent back a WriteResponse, which we will receive in the
			// following CloseAndRecv call. Note that this response may have been sent
			// in response to a WriteRequest sent in a previous loop iteration, since
			// the gRPC client does not wait for the server to process each request
			// before sending subsequent requests.
			break
		}
	}
	res, err := uploadStream.CloseAndRecv()
	require.NoError(t, err)

	rn, err := digest.ParseUploadResourceName(uploadResourceName)
	require.NoError(t, err)
	isCompressed := rn.GetCompressor() != repb.Compressor_IDENTITY

	// If this is a duplicate write, we expect the upload to be short-circuited.
	shouldShortCircuit := !isFirstAttempt

	// Note: Bazel pre-5.1.0 doesn't support short-circuiting compressed writes.
	// Instead, the server should allow the client to upload the full stream,
	// but just discard the uploaded stream.
	// See https://github.com/bazelbuild/bazel/issues/14654
	bazel5_1_0 := bazel_request.MustParseVersion("5.1.0")
	if v := bazel_request.MustParseVersion(bazelVersion); isCompressed && !v.IsAtLeast(bazel5_1_0) {
		shouldShortCircuit = false
	}

	if shouldShortCircuit {
		// When short-circuiting, we expect committed size to be -1 for
		// compressed blobs, since the committed size can vary depending on
		// things like compression level.
		if isCompressed {
			require.Equal(t, int64(-1), res.CommittedSize)
		} else {
			require.Equal(t, rn.GetDigest().GetSizeBytes(), res.CommittedSize)
		}
		return
	}

	require.Equal(t, int64(len(blob)), res.CommittedSize)
	require.Len(t, remaining, 0, "not all bytes were uploaded")
}

func zstdDecompress(t *testing.T, b []byte) []byte {
	out, err := compression.DecompressZstd(nil, b)
	require.NoError(t, err, "failed to decompress blob")
	return out
}

func newUUID(t *testing.T) string {
	uuid, err := guuid.NewRandom()
	require.NoError(t, err)
	return uuid.String()
}
