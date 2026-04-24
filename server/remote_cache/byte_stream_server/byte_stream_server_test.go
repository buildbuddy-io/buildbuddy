package byte_stream_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	guuid "github.com/google/uuid"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultBazelVersion = "6.0.0"
)

func runByteStreamServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	byteStreamServer, err := NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	go runFunc()

	// TODO(vadim): can we remove the MsgSize override from the default options?
	clientConn, err := testenv.LocalGRPCConn(ctx, lis, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

type gatedReadCache struct {
	interfaces.Cache

	release  chan struct{}
	started  chan struct{}
	gateSize int64

	mu         sync.Mutex
	activeRead int
	maxRead    int
}

func (c *gatedReadCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	if r.GetDigest().GetSizeBytes() != c.gateSize {
		return c.Cache.Reader(ctx, r, uncompressedOffset, limit)
	}
	c.mu.Lock()
	c.activeRead++
	c.maxRead = max(c.maxRead, c.activeRead)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.activeRead--
		c.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.started <- struct{}{}:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
	}
	return c.Cache.Reader(ctx, r, uncompressedOffset, limit)
}

func (c *gatedReadCache) maxConcurrentReads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxRead
}

type closeBlockingReadCloser struct {
	started     chan struct{}
	unblock     <-chan struct{}
	startedOnce sync.Once
}

func (r *closeBlockingReadCloser) Read(_ []byte) (int, error) {
	r.startedOnce.Do(func() {
		r.started <- struct{}{}
	})
	<-r.unblock
	return 0, io.EOF
}

func (r *closeBlockingReadCloser) Close() error {
	return nil
}

type readerFuncCache struct {
	interfaces.Cache
	reader func(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error)
}

func (c *readerFuncCache) Reader(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	return c.reader(ctx, r, offset, limit)
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

func (c *casCompressionCache) Reader(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	if r.GetCacheType() != rspb.CacheType_CAS {
		return c.Cache.Reader(ctx, r, offset, limit)
	}
	return (&testcompression.CompressionCache{Cache: c.Cache}).Reader(ctx, r, offset, limit)
}

func (c *casCompressionCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	switch compressor {
	case repb.Compressor_IDENTITY, repb.Compressor_ZSTD:
		return true
	default:
		return false
	}
}

func TestRPCRead(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, t, te)
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
		resourceName *digest.CASResourceName
		wantData     string
		offset       int64
	}{
		{ // Simple Read
			resourceName: digest.NewCASResourceName(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, "", repb.DigestFunction_SHA256),

			wantData:  randStr(1234),
			wantError: nil,
			offset:    0,
		},
		{ // Large Read
			resourceName: digest.NewCASResourceName(&repb.Digest{
				Hash:      "ffd14ebb6c1b2701ac793ea1aff6dddf8540e734bd6d051ac2a24aa3ec062781",
				SizeBytes: 1000 * 1000 * 100,
			}, "", repb.DigestFunction_SHA256),

			wantData:  randStr(1000 * 1000 * 100),
			wantError: nil,
			offset:    0,
		},
		{ // 0 length read
			resourceName: digest.NewCASResourceName(&repb.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}, "", repb.DigestFunction_SHA256),

			wantData:  "",
			wantError: nil,
			offset:    0,
		},
		{ // Offset
			resourceName: digest.NewCASResourceName(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, "", repb.DigestFunction_SHA256),

			wantData:  randStr(1234),
			wantError: nil,
			offset:    1,
		},
		{ // Max offset
			resourceName: digest.NewCASResourceName(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, "", repb.DigestFunction_SHA256),

			wantData:  randStr(1234),
			wantError: nil,
			offset:    1234,
		},
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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
	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a regular bytestream upload works.
	d, readSeeker := testdigest.NewReader(t, 1000)
	instanceNameDigest := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCWriteWithDirectWrite(t *testing.T) {
	flags.Set(t, "cache.max_direct_write_size_bytes", 1024)
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// This file is small enough that we'll skip the Contains check.
	d, readSeeker := testdigest.NewReader(t, 1000)
	instanceNameDigest := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCMalformedWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a malformed upload (incorrect digest) is rejected.
	instanceNameDigest, buf := testdigest.RandomCASResourceBuf(t, 1000)
	buf[0] = ^buf[0] // flip bits in byte to corrupt digest.

	readSeeker := bytes.NewReader(buf)
	rn, err := digest.CASResourceNameFromProto(instanceNameDigest)
	if err != nil {
		t.Fatalf("failed to create resource name: %v", err)
	}
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, rn, readSeeker)
	if !status.IsInvalidArgumentError(err) {
		t.Fatalf("Expected invalid argument error but got %s", err)
	}
}

func TestRPCTooLongWrite(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Test that a malformed upload (wrong bytesize) is rejected.
	rnProto, buf := testdigest.RandomCASResourceBuf(t, 1000)
	rnProto.Digest.SizeBytes += 1 // increment expected byte count by 1 to trigger mismatch.
	instanceNameDigest, err := digest.CASResourceNameFromProto(rnProto)
	if err != nil {
		t.Fatalf("failed to create resource name: %v", err)
	}

	readSeeker := bytes.NewReader(buf)
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
	if !status.IsInvalidArgumentError(err) {
		t.Fatalf("Expected invalid argument error but got %s", err)
	}
}

// Tests Read/Write of a blob that exceeds the default gRPC message size.
func TestRPCReadWriteLargeBlob(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	blob, err := random.RandomString(10_000_000)
	require.NoError(t, err)
	d, err := digest.Compute(strings.NewReader(blob), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	instanceNameDigest := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)

	// Write
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, strings.NewReader(blob))
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

	clientConn := runByteStreamServer(ctx, t, te)
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

	directWriteSizeBytes := int64(512)
	flags.Set(t, "cache.max_direct_write_size_bytes",
		directWriteSizeBytes)

	clientConn := runByteStreamServer(ctx, t, te)
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
		byte_stream.MustUploadChunked(t, ctx, bsClient, defaultBazelVersion, uploadResourceName, compressedBlob, blobSize < directWriteSizeBytes)

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

	clientConn := runByteStreamServer(ctx, t, te)
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

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
			require.NoError(t, err)
			clientConn := runByteStreamServer(ctx, t, te)
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

func TestReadChunked(t *testing.T) {
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
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	chunk1RN, chunk1 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk2RN, chunk2 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk3RN, chunk3 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	fullBlob := append(append(chunk1, chunk2...), chunk3...)

	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	require.NoError(t, te.GetCache().Set(ctx, chunk1RN, chunk1))
	require.NoError(t, te.GetCache().Set(ctx, chunk2RN, chunk2))
	require.NoError(t, te.GetCache().Set(ctx, chunk3RN, chunk3))

	manifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest()},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}
	require.NoError(t, manifest.Store(ctx, te.GetCache()))

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_SHA256)
	var buf bytes.Buffer
	err = cachetools.GetBlob(ctx, bsClient, blobRN, &buf)
	require.NoError(t, err)
	require.Equal(t, fullBlob, buf.Bytes())
}

func TestReadChunked_NonZeroOffset(t *testing.T) {
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
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	chunkSize := int64(1024 * 1024)
	chunk1RN, chunk1 := testdigest.RandomCASResourceBuf(t, chunkSize)
	chunk2RN, chunk2 := testdigest.RandomCASResourceBuf(t, chunkSize)
	chunk3RN, chunk3 := testdigest.RandomCASResourceBuf(t, chunkSize)
	fullBlob := append(append(chunk1, chunk2...), chunk3...)

	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	require.NoError(t, te.GetCache().Set(ctx, chunk1RN, chunk1))
	require.NoError(t, te.GetCache().Set(ctx, chunk2RN, chunk2))
	require.NoError(t, te.GetCache().Set(ctx, chunk3RN, chunk3))

	manifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1RN.GetDigest(), chunk2RN.GetDigest(), chunk3RN.GetDigest()},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_SHA256,
	}
	require.NoError(t, manifest.Store(ctx, te.GetCache()))

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_SHA256)

	for _, tc := range []struct {
		name   string
		offset int64
	}{
		{name: "mid_first_chunk", offset: chunkSize / 2},
		{name: "exact_chunk_boundary", offset: chunkSize},
		{name: "mid_second_chunk", offset: chunkSize + chunkSize/2},
		{name: "last_chunk", offset: 2 * chunkSize},
		{name: "mid_last_chunk", offset: 2*chunkSize + chunkSize/2},
		{name: "at_end", offset: 3 * chunkSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readReq := &bspb.ReadRequest{
				ResourceName: blobRN.DownloadString(),
				ReadOffset:   tc.offset,
			}
			stream, err := bsClient.Read(ctx, readReq)
			require.NoError(t, err)
			var buf bytes.Buffer
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				buf.Write(resp.GetData())
			}
			expected := fullBlob[tc.offset:]
			got := buf.Bytes()
			if len(expected) == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, expected, got)
			}
		})
	}
}

func TestReadChunked_NonZeroOffset_ZstdBLAKE3(t *testing.T) {
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
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))
	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	chunkSize := int64(1024 * 1024)
	_, chunk1 := testdigest.RandomCASResourceBuf(t, chunkSize)
	_, chunk2 := testdigest.RandomCASResourceBuf(t, chunkSize)
	_, chunk3 := testdigest.RandomCASResourceBuf(t, chunkSize)
	fullBlob := append(append(chunk1, chunk2...), chunk3...)

	chunk1Digest, err := digest.Compute(bytes.NewReader(chunk1), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	chunk2Digest, err := digest.Compute(bytes.NewReader(chunk2), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	chunk3Digest, err := digest.Compute(bytes.NewReader(chunk3), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	chunk1RN := digest.NewCASResourceName(chunk1Digest, "", repb.DigestFunction_BLAKE3)
	chunk2RN := digest.NewCASResourceName(chunk2Digest, "", repb.DigestFunction_BLAKE3)
	chunk3RN := digest.NewCASResourceName(chunk3Digest, "", repb.DigestFunction_BLAKE3)
	require.NoError(t, te.GetCache().Set(ctx, chunk1RN.ToProto(), chunk1))
	require.NoError(t, te.GetCache().Set(ctx, chunk2RN.ToProto(), chunk2))
	require.NoError(t, te.GetCache().Set(ctx, chunk3RN.ToProto(), chunk3))

	manifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1Digest, chunk2Digest, chunk3Digest},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	require.NoError(t, manifest.Store(ctx, te.GetCache()))

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	blobRN.SetCompressor(repb.Compressor_ZSTD)

	offset := chunkSize + chunkSize/2
	stream, err := bsClient.Read(ctx, &bspb.ReadRequest{
		ResourceName: blobRN.DownloadString(),
		ReadOffset:   offset,
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf.Write(resp.GetData())
	}
	require.Equal(t, fullBlob[offset:], zstdDecompress(t, buf.Bytes()))
}

func TestChunkedBlobReaderReadChunk(t *testing.T) {
	t.Run("identity returns exact bytes", func(t *testing.T) {
		data := []byte("hello, world")
		d, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_BLAKE3)
		require.NoError(t, err)
		rn := digest.NewCASResourceName(d, "", repb.DigestFunction_BLAKE3).ToProto()
		r := &chunkedBlobReader{
			ctx: context.Background(),
			cache: &readerFuncCache{
				reader: func(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(data[offset:])), nil
				},
			},
			bufferPool: bytebufferpool.VariableSize(len(data)),
		}

		buf := r.bufferPool.Get(int64(len(data)))
		result := r.readChunk(rn, 0, buf)
		require.NoError(t, result.err)
		require.Equal(t, data, result.data)
	})

	t.Run("identity rejects short reads", func(t *testing.T) {
		data := []byte("hello, world")
		d, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_BLAKE3)
		require.NoError(t, err)
		rn := digest.NewCASResourceName(d, "", repb.DigestFunction_BLAKE3).ToProto()
		r := &chunkedBlobReader{
			ctx: context.Background(),
			cache: &readerFuncCache{
				reader: func(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(data[:len(data)-1])), nil
				},
			},
			bufferPool: bytebufferpool.VariableSize(len(data)),
		}

		buf := r.bufferPool.Get(int64(len(data)))
		result := r.readChunk(rn, 0, buf)
		require.ErrorIs(t, result.err, io.ErrUnexpectedEOF)
	})

	t.Run("zstd allows compression overhead", func(t *testing.T) {
		data := []byte("abc")
		compressed := compression.CompressZstd(nil, data)
		require.Greater(t, len(compressed), len(data))

		d, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_BLAKE3)
		require.NoError(t, err)
		rn := digest.NewCASResourceName(d, "", repb.DigestFunction_BLAKE3)
		rn.SetCompressor(repb.Compressor_ZSTD)
		r := &chunkedBlobReader{
			ctx: context.Background(),
			cache: &readerFuncCache{
				reader: func(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(compressed)), nil
				},
			},
			bufferPool: bytebufferpool.VariableSize(int(compression.ZstdCompressBound(int64(len(data))))),
		}

		buf := r.bufferPool.Get(compression.ZstdCompressBound(int64(len(data))))
		result := r.readChunk(rn.ToProto(), 0, buf)
		require.NoError(t, result.err)
		require.Equal(t, compressed, result.data)
	})
}

func TestChunkedBlobReader_ReadReturnsCurrentChunk(t *testing.T) {
	bufferPool := bytebufferpool.VariableSize(8)
	buf := bufferPool.Get(5)
	copy(buf, "hello")
	r := &chunkedBlobReader{
		bufferPool: bufferPool,
		currentBuf: buf,
	}

	out := make([]byte, 5)
	n, err := r.Read(out)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(out[:n]))
	require.Nil(t, r.currentBuf)
	require.Equal(t, 0, r.currentOffset)
}

func TestChunkedBlobReader_CloseAfterErrorDoesNotBlock(t *testing.T) {
	chunk1 := []byte("hello")
	chunk2 := []byte("world")
	chunk1Digest, err := digest.Compute(bytes.NewReader(chunk1), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	chunk2Digest, err := digest.Compute(bytes.NewReader(chunk2), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	rn1 := digest.NewCASResourceName(chunk1Digest, "", repb.DigestFunction_BLAKE3).ToProto()
	rn2 := digest.NewCASResourceName(chunk2Digest, "", repb.DigestFunction_BLAKE3).ToProto()
	reader := newChunkedBlobReader(context.Background(), &readerFuncCache{
		reader: func(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
			switch r.GetDigest().GetHash() {
			case chunk1Digest.GetHash():
				return io.NopCloser(bytes.NewReader(chunk1)), nil
			case chunk2Digest.GetHash():
				return io.NopCloser(bytes.NewReader(chunk2[:len(chunk2)-1])), nil
			default:
				return nil, status.NotFoundErrorf("unexpected digest %q", r.GetDigest().GetHash())
			}
		},
	}, bytebufferpool.VariableSize(64), []*rspb.ResourceName{rn1, rn2}, 0, "false")

	got, err := io.ReadAll(reader)
	require.Equal(t, chunk1, got)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	done := make(chan struct{})
	go func() {
		_ = reader.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close() hung after a chunk read error")
	}
}

func TestChunkedBlobReader_CloseDoesNotWaitForActiveReads(t *testing.T) {
	started := make(chan struct{}, 2)
	unblock := make(chan struct{})
	chunk1Digest, err := digest.Compute(bytes.NewReader([]byte("a")), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	chunk2Digest, err := digest.Compute(bytes.NewReader([]byte("b")), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	rn1 := digest.NewCASResourceName(chunk1Digest, "", repb.DigestFunction_BLAKE3).ToProto()
	rn2 := digest.NewCASResourceName(chunk2Digest, "", repb.DigestFunction_BLAKE3).ToProto()
	reader := newChunkedBlobReader(context.Background(), &readerFuncCache{
		reader: func(ctx context.Context, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
			return &closeBlockingReadCloser{
				started: started,
				unblock: unblock,
			}, nil
		},
	}, bytebufferpool.VariableSize(8), []*rspb.ResourceName{rn1, rn2}, 0, "false")

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("chunk read did not start")
		}
	}

	done := make(chan struct{})
	go func() {
		_ = reader.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close() waited for active reads")
	}
	close(unblock)
}

func TestNewByteStreamServer_BufferPoolAccommodatesCompressedChunkOverhead(t *testing.T) {
	te := testenv.GetTestEnv(t)
	s, err := NewByteStreamServer(te)
	require.NoError(t, err)

	want := compression.ZstdCompressBound(chunking.MaxChunkSizeBytes())
	buf := s.bufferPool.Get(want)
	defer s.bufferPool.Put(buf)

	require.Equal(t, want, int64(len(buf)))
}

func TestReadChunked_ZstdBLAKE3_PassthroughIncompressible(t *testing.T) {
	chunkSize := int64(1024 * 1024)
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
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))
	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)
	te.SetCache(&casCompressionCache{Cache: te.GetCache()})

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// Random chunks compress to slightly more than the uncompressed size, so
	// this exercises the zstd compress-bound headroom in the chunk read path.
	_, chunk1 := testdigest.RandomCASResourceBuf(t, chunkSize)
	_, chunk2 := testdigest.RandomCASResourceBuf(t, chunkSize)
	_, chunk3 := testdigest.RandomCASResourceBuf(t, chunkSize)
	fullBlob := append(append(chunk1, chunk2...), chunk3...)

	chunk1Digest, err := digest.Compute(bytes.NewReader(chunk1), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	chunk2Digest, err := digest.Compute(bytes.NewReader(chunk2), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	chunk3Digest, err := digest.Compute(bytes.NewReader(chunk3), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	baseCache := te.GetCache().(*casCompressionCache).Cache
	chunk1RN := digest.NewCASResourceName(chunk1Digest, "", repb.DigestFunction_BLAKE3)
	chunk2RN := digest.NewCASResourceName(chunk2Digest, "", repb.DigestFunction_BLAKE3)
	chunk3RN := digest.NewCASResourceName(chunk3Digest, "", repb.DigestFunction_BLAKE3)
	require.NoError(t, baseCache.Set(ctx, chunk1RN.ToProto(), chunk1))
	require.NoError(t, baseCache.Set(ctx, chunk2RN.ToProto(), chunk2))
	require.NoError(t, baseCache.Set(ctx, chunk3RN.ToProto(), chunk3))

	manifest := &chunking.Manifest{
		BlobDigest:     blobDigest,
		ChunkDigests:   []*repb.Digest{chunk1Digest, chunk2Digest, chunk3Digest},
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	require.NoError(t, manifest.Store(ctx, baseCache))

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	blobRN.SetCompressor(repb.Compressor_ZSTD)

	stream, err := bsClient.Read(ctx, &bspb.ReadRequest{
		ResourceName: blobRN.DownloadString(),
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf.Write(resp.GetData())
	}
	require.Equal(t, fullBlob, zstdDecompress(t, buf.Bytes()))
}

func TestReadChunked_MissingManifest(t *testing.T) {
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
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetExperimentFlagProvider(fp)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	clientConn := runByteStreamServer(ctx, t, te)
	bsClient := bspb.NewByteStreamClient(clientConn)

	chunk1RN, chunk1 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk2RN, chunk2 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	chunk3RN, chunk3 := testdigest.RandomCASResourceBuf(t, 1024*1024)
	fullBlob := append(append(chunk1, chunk2...), chunk3...)

	blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
	require.NoError(t, err)

	require.NoError(t, te.GetCache().Set(ctx, chunk1RN, chunk1))
	require.NoError(t, te.GetCache().Set(ctx, chunk2RN, chunk2))
	require.NoError(t, te.GetCache().Set(ctx, chunk3RN, chunk3))

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_SHA256)
	var buf bytes.Buffer
	err = cachetools.GetBlob(ctx, bsClient, blobRN, &buf)

	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, gstatus.Code(err),
		"expected FailedPrecondition, got %s: %s", gstatus.Code(err), err)

	st := gstatus.Convert(err)
	expectedSubject := fmt.Sprintf("blobs/%s/%d", blobDigest.GetHash(), blobDigest.GetSizeBytes())
	var found bool
	for _, detail := range st.Details() {
		if pf, ok := detail.(*errdetails.PreconditionFailure); ok {
			for _, v := range pf.GetViolations() {
				if v.GetType() == "MISSING" && v.GetSubject() == expectedSubject {
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected MISSING violation with subject %q, got: %s", expectedSubject, err)
}

func TestReadChunked_UsesParallelReads(t *testing.T) {
	for _, tc := range []struct {
		name       string
		chunkCount int
		want       int
		compressor repb.Compressor_Value
	}{
		{name: "identity_few_chunks", chunkCount: 3, want: 3, compressor: repb.Compressor_IDENTITY},
		{name: "identity_many_chunks", chunkCount: 2 * defaultChunkedReadMaxInFlight, want: defaultChunkedReadMaxInFlight, compressor: repb.Compressor_IDENTITY},
		{name: "zstd_many_chunks", chunkCount: 2 * defaultChunkedReadMaxInFlight, want: defaultChunkedReadMaxInFlight, compressor: repb.Compressor_ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

			fp, err := experiments.NewFlagProvider(t.Name())
			require.NoError(t, err)
			if tc.compressor == repb.Compressor_ZSTD {
				flags.Set(t, "cache.zstd_transcoding_enabled", true)
			}

			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			te.SetExperimentFlagProvider(fp)

			baseCache := te.GetCache()
			if tc.compressor == repb.Compressor_ZSTD {
				baseCache = &casCompressionCache{Cache: baseCache}
			}
			cache := &gatedReadCache{
				Cache:    baseCache,
				release:  make(chan struct{}),
				started:  make(chan struct{}, tc.chunkCount),
				gateSize: 1024 * 1024,
			}
			te.SetCache(cache)

			ctx, err = prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
			require.NoError(t, err)

			clientConn := runByteStreamServer(ctx, t, te)
			bsClient := bspb.NewByteStreamClient(clientConn)

			chunkRN, chunkData := testdigest.RandomCASResourceBuf(t, 1024*1024)
			require.NoError(t, cache.Set(ctx, chunkRN, chunkData))

			fullBlob := bytes.Repeat(chunkData, tc.chunkCount)
			blobDigest, err := digest.Compute(bytes.NewReader(fullBlob), repb.DigestFunction_SHA256)
			require.NoError(t, err)

			chunkDigests := make([]*repb.Digest, tc.chunkCount)
			for i := range chunkDigests {
				chunkDigests[i] = chunkRN.GetDigest()
			}
			manifest := &chunking.Manifest{
				BlobDigest:     blobDigest,
				ChunkDigests:   chunkDigests,
				InstanceName:   "",
				DigestFunction: repb.DigestFunction_SHA256,
			}
			require.NoError(t, manifest.Store(ctx, cache))

			blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_SHA256)
			errCh := make(chan error, 1)
			go func() {
				if tc.compressor == repb.Compressor_ZSTD {
					blobRN.SetCompressor(repb.Compressor_ZSTD)
				}
				errCh <- cachetools.GetBlob(ctx, bsClient, blobRN, io.Discard)
			}()
			for i := 0; i < tc.want; i++ {
				select {
				case <-cache.started:
				case <-time.After(time.Second):
					t.Fatalf("timed out waiting for chunk read %d of %d", i+1, tc.want)
				}
			}
			require.Equal(t, tc.want, cache.maxConcurrentReads())
			close(cache.release)
			require.NoError(t, <-errCh)
		})
	}
}
