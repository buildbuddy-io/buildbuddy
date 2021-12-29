package byte_stream_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	guuid "github.com/google/uuid"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
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

func readBlob(ctx context.Context, bsClient bspb.ByteStreamClient, d *digest.InstanceNameDigest, out io.Writer, offset int64) error {
	req := &bspb.ReadRequest{
		ResourceName: digest.DownloadResourceName(&digest.ResourceName{InstanceNameDigest: d}),
		ReadOffset:   offset,
		ReadLimit:    d.GetSizeBytes(),
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
		wantError          error
		instanceNameDigest *digest.InstanceNameDigest
		wantData           string
		offset             int64
	}{
		{ // Simple Read
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, ""),
			wantData:  randStr(1234),
			wantError: nil,
			offset:    0,
		},
		{ // Large Read
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "ffd14ebb6c1b2701ac793ea1aff6dddf8540e734bd6d051ac2a24aa3ec062781",
				SizeBytes: 1000 * 1000 * 100,
			}, ""),
			wantData:  randStr(1000 * 1000 * 100),
			wantError: nil,
			offset:    0,
		},
		{ // 0 length read
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			}, ""),
			wantData:  "",
			wantError: nil,
			offset:    0,
		},
		{ // Offset
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, ""),
			wantData:  randStr(1234),
			wantError: nil,
			offset:    1,
		},
		{ // Max offset
			instanceNameDigest: digest.NewInstanceNameDigest(&repb.Digest{
				Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
				SizeBytes: 1234,
			}, ""),
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
		if err := te.GetCache().Set(ctx, tc.instanceNameDigest.Digest, []byte(tc.wantData)); err != nil {
			t.Fatal(err)
		}

		// Now read it back with the bytestream API.
		var buf bytes.Buffer
		gotErr := readBlob(ctx, bsClient, tc.instanceNameDigest, &buf, tc.offset)
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
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")
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
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")
	buf[0] = ^buf[0] // flip bits in byte to corrupt digest.

	readSeeker := bytes.NewReader(buf)
	_, err := cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, readSeeker)
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
	d, buf := testdigest.NewRandomDigestBuf(t, 1000)
	d.SizeBytes += 1 // increment expected byte count by 1 to trigger mismatch.
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")

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
	d, err := digest.Compute(strings.NewReader(blob))
	require.NoError(t, err)
	instanceNameDigest := digest.NewInstanceNameDigest(d, "")

	// Write
	_, err = cachetools.UploadFromReader(ctx, bsClient, instanceNameDigest, strings.NewReader(blob))
	require.NoError(t, err)

	// Read
	var buf bytes.Buffer
	err = readBlob(ctx, bsClient, instanceNameDigest, &buf, 0)
	require.NoError(t, err)
	require.Equal(t, blob, string(buf.Bytes()))
}

func TestRPCReadAndWriteCompressed(t *testing.T) {
	flags.Set(t, "cache.zstd_capability_enabled", "true")
	flags.Set(t, "cache.zstd_storage_enabled", "true")
	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := zstdCompress(t, blob)
	require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob))
	require.NoError(t, err)

	// ByteStream.Read should return NOT_FOUND initially.
	resourceName := fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
	readStream, err := bsClient.Read(ctx, &bspb.ReadRequest{ResourceName: resourceName})
	require.NoError(t, err)
	_, err = readStream.Recv()

	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err), "error code should be NOT_FOUND")

	// Upload compressed blob.
	uploadResourceName := fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes)
	mustUploadChunked(t, ctx, bsClient, uploadResourceName, compressedBlob)

	// Read back the compressed blob we just uploaded. After decompressing, should
	// get back the original blob contents.
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

func TestRPCWriteCompressedReadUncompressed(t *testing.T) {
	flags.Set(t, "cache.zstd_capability_enabled", "true")
	flags.Set(t, "cache.zstd_storage_enabled", "true")
	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")
	compressedBlob := zstdCompress(t, blob)
	require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob))
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
	mustUploadChunked(t, ctx, bsClient, uploadResourceName, compressedBlob)

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
}

func TestRPCWriteUncompressedReadCompressed(t *testing.T) {
	flags.Set(t, "cache.zstd_capability_enabled", "true")
	flags.Set(t, "cache.zstd_storage_enabled", "true")
	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	clientConn := runByteStreamServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	blob := []byte("AAAAAAAAAAAAAAAAAAAAAAAAA")

	// Note: Digest is of uncompressed contents
	d, err := digest.Compute(bytes.NewReader(blob))
	require.NoError(t, err)

	// Upload uncompressed via bytestream.
	uploadResourceName := fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes)
	mustUploadChunked(t, ctx, bsClient, uploadResourceName, blob)

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

func mustUploadChunked(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, uploadResourceName string, blob []byte) {
	uploadStream, err := bsClient.Write(ctx)
	require.NoError(t, err)
	blobChunk1 := blob[:len(blob)/2]
	err = uploadStream.Send(&bspb.WriteRequest{
		ResourceName: uploadResourceName,
		Data:         blobChunk1,
	})
	require.NoError(t, err)
	blobChunk2 := blob[len(blobChunk1):]
	err = uploadStream.Send(&bspb.WriteRequest{
		ResourceName: uploadResourceName,
		// Per protocol, write offset refers to the *compressed* stream.
		WriteOffset: int64(len(blobChunk1)),
		Data:        blobChunk2,
		FinishWrite: true,
	})
	require.NoError(t, err)
	_, err = uploadStream.CloseAndRecv()
	require.NoError(t, err)
}

func zstdCompress(t *testing.T, b []byte) []byte {
	out, err := zstd.Compress(nil, b)
	require.NoError(t, err)
	return out
}

func zstdDecompress(t *testing.T, b []byte) []byte {
	out, err := zstd.Decompress(nil, b)
	require.NoError(t, err, "failed to decompress blob")
	return out
}

func newUUID(t *testing.T) string {
	uuid, err := guuid.NewRandom()
	require.NoError(t, err)
	return uuid.String()
}
