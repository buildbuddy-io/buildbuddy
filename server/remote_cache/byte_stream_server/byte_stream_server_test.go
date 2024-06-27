package byte_stream_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
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

	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	go runFunc()

	// TODO(vadim): can we remove the MsgSize override from the default options?
	clientConn, err := testenv.LocalGRPCConn(ctx, env, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	if err != nil {
		t.Error(err)
	}

	return clientConn
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
		gotErr := byte_stream.ReadBlob(ctx, bsClient, tc.resourceName, &buf, tc.offset)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("got %v; want %v", gotErr, tc.wantError)
			//			continue
		}
		got := buf.String()
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

func TestRPCWriteWithDirectWrite(t *testing.T) {
	flags.Set(t, "cache.max_direct_write_size_bytes", 1024)
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// This file is small enough that we'll skip the Contains check.
	d, readSeeker := testdigest.NewRandomDigestReader(t, 1000)
	instanceNameDigest := digest.NewResourceName(d, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
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
	err = byte_stream.ReadBlob(ctx, bsClient, instanceNameDigest, &buf, 0)
	require.NoError(t, err)
	require.Equal(t, blob, buf.String())
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

	for _, blobSize := range []int64{1, 1e2, 1e4, 1e6, 8e6} {
		rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, blobSize, "" /*instanceName*/)
		compressedBlob := compression.CompressZstd(nil, blob)
		require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

		// Note: Digest is of uncompressed contents
		d := rn.GetDigest()

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

			byte_stream.MustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, true)

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
	for _, blobSize := range []int64{1, 1e2, 1e4, 1e6, 8e6, 16e6} {
		rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, blobSize, "" /*instanceName*/)

		compressedBlob := compression.CompressZstd(nil, blob)
		require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

		// Note: Digest is of uncompressed contents
		d := rn.GetDigest()

		// ByteStream.Read should return NOT_FOUND initially.
		resourceName := fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
		readStream, err := bsClient.Read(ctx, &bspb.ReadRequest{ResourceName: resourceName})
		require.NoError(t, err)
		_, err = readStream.Recv()

		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "error code should be NOT_FOUND")

		// Upload the compressed blob.
		uploadResourceName := fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes)
		byte_stream.MustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, true)

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
		byte_stream.MustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, false)

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

	for _, blobSize := range []int64{1, 1e2, 1e4, 1e6, 8e6} {
		rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, blobSize, "" /*instanceName*/)

		// Note: Digest is of uncompressed contents
		d := rn.GetDigest()

		// Upload uncompressed via bytestream.
		uploadResourceName := fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes)
		byte_stream.MustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, blob, true)

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
	rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, 5e6, "" /*instanceName*/)
	compressedBlob := compression.CompressZstd(nil, blob)
	require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

	// Note: Digest is of uncompressed contents
	d := rn.GetDigest()

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
			ctx = byte_stream.WithBazelVersion(t, ctx, tc.bazelVersion)

			// Enable compression
			flags.Set(t, "cache.zstd_transcoding_enabled", true)
			te.SetCache(&testcompression.CompressionCache{Cache: te.GetCache()})

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
			require.NoError(t, err)
			clientConn := runByteStreamServer(ctx, te, t)
			bsClient := bspb.NewByteStreamClient(clientConn)

			// Upload the blob
			byte_stream.MustUploadChunked(t, ctx, bsClient, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, true)

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
			byte_stream.MustUploadChunked(t, ctx, bsClient, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, false)

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
