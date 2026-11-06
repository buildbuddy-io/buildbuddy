package byte_stream

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

func ReadBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName, out io.Writer, offset int64) error {
	req := &bspb.ReadRequest{
		ResourceName: r.DownloadString(),
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
		chunkSize := min(1_000_000, len(remaining))
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

// ShortCircuitMode selects how ShortCircuitFakeWriteServer ends a Write
// after the first WriteRequest.
type ShortCircuitMode int

const (
	// ShortCircuitWithResponse sends a WriteResponse immediately after the
	// first WriteRequest.
	ShortCircuitWithResponse ShortCircuitMode = iota
	// ShortCircuitWithStatus ends the stream with the configured Status.
	ShortCircuitWithStatus
)

func (m ShortCircuitMode) String() string {
	switch m {
	case ShortCircuitWithResponse:
		return "response"
	case ShortCircuitWithStatus:
		return "status"
	}
	return "unknown"
}

// ShortCircuitFakeWriteServer is a ByteStream server that ends the Write stream
// simulating an "already exists" message. The mode decides how that response
// is communicated to the client.
type ShortCircuitFakeWriteServer struct {
	Mode ShortCircuitMode
	// Status ends the stream in ShortCircuitWithStatus mode. It must be
	// non-nil in that mode.
	Status error
	// Writes counts Write calls, which shows whether a client retried.
	Writes atomic.Int32
	// Received counts request bytes the server consumed, which is the first
	// request of each Write.
	Received atomic.Int64
}

func (s *ShortCircuitFakeWriteServer) Write(stream bspb.ByteStream_WriteServer) error {
	s.Writes.Add(1)
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.Received.Add(int64(len(req.GetData())))
	if s.Mode == ShortCircuitWithStatus {
		return s.Status
	}
	rn, err := digest.ParseUploadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	committed := rn.GetDigest().GetSizeBytes()
	if rn.GetCompressor() != repb.Compressor_IDENTITY {
		committed = -1
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: committed})
}

func (s *ShortCircuitFakeWriteServer) Read(*bspb.ReadRequest, bspb.ByteStream_ReadServer) error {
	return status.UnimplementedError("ShortCircuitFakeWriteServer only implements Write")
}

func (s *ShortCircuitFakeWriteServer) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.UnimplementedError("ShortCircuitFakeWriteServer only implements Write")
}
