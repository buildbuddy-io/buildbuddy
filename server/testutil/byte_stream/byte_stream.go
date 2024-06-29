package byte_stream

import (
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func WithBazelVersion(t *testing.T, ctx context.Context, version string) context.Context {
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

func ReadBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, out io.Writer, offset int64) error {
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

func MustUploadChunked(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, bazelVersion string, uploadResourceName string, blob []byte, isFirstAttempt bool) {
	ctx = WithBazelVersion(t, ctx, bazelVersion)

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
