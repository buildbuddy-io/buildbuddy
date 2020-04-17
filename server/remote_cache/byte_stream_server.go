package byte_stream_server

import (
	"context"
	"fmt"
	"io"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Keep under the limit of ~4MB (1024 * 1024 * 4).
	readBufSizeBytes = (1024 * 1024 * 4) - 100
)

var (
	// Matches:
	// - "blobs/469db13020c60f8bdf9c89aa4e9a449914db23139b53a24d064f967a51057868/39120"
	// - "uploads/2042a8f9-eade-4271-ae58-f5f6f5a32555/blobs/8afb02ca7aace3ae5cd8748ac589e2e33022b1a4bfd22d5d234c5887e270fe9c/17997850"
	uploadRegex = regexp.MustCompile("^.*blobs/([a-f0-9]{64})/.*$")
)

type ByteStreamServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func NewByteStreamServer(env environment.Env) (*ByteStreamServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ByteStreamServer")
	}
	return &ByteStreamServer{
		env:   env,
		cache: cache,
	}, nil
}

func extractHash(resourceName string) (string, error) {
	parts := uploadRegex.FindStringSubmatch(resourceName)
	if len(parts) != 2 {
		return "", fmt.Errorf("Unparsable resource name: %s", resourceName)
	}
	if parts[1] == "" {
		return "", fmt.Errorf("Unparsable resource name (empty hash?): %s", resourceName)
	}
	return parts[1], nil
}

func checkReadPreconditions(req *bspb.ReadRequest) error {
	if req.ResourceName == "" {
		return status.InvalidArgumentError("Missing resource name")
	}
	if req.ReadOffset < 0 {
		return status.OutOfRangeError("ReadOffset out of range")
	}
	if req.ReadLimit < 0 {
		return status.OutOfRangeError("ReadLimit out of range")
	}
	return nil
}

// `Read()` is used to retrieve the contents of a resource as a sequence
// of bytes. The bytes are returned in a sequence of responses, and the
// responses are delivered as the results of a server-side streaming FUNC (S *BYTESTREAMSERVER).
func (s *ByteStreamServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	if err := checkReadPreconditions(req); err != nil {
		return err
	}
	hash, err := extractHash(req.ResourceName)
	if err != nil {
		return err
	}
	ck, err := perms.UserPrefixCacheKey(stream.Context(), s.env, hash)
	if err != nil {
		return err
	}
	reader, err := s.cache.Reader(stream.Context(), ck, req.ReadOffset, req.ReadLimit)
	if err != nil {
		return err
	}

	buf := make([]byte, readBufSizeBytes)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		rsp := &bspb.ReadResponse{
			Data: buf[:n],
		}
		stream.Send(rsp)

		if err == io.EOF {
			break
		}
	}
	return nil
}

// `Write()` is used to send the contents of a resource as a sequence of
// bytes. The bytes are sent in a sequence of request protos of a client-side
// streaming FUNC (S *BYTESTREAMSERVER).
//
// A `Write()` action is resumable. If there is an error or the connection is
// broken during the `Write()`, the client should check the status of the
// `Write()` by calling `QueryWriteStatus()` and continue writing from the
// returned `committed_size`. This may be less than the amount of data the
// client previously sent.
//
// Calling `Write()` on a resource name that was previously written and
// finalized could cause an error, depending on whether the underlying service
// allows over-writing of previously written resources.
//
// When the client closes the request channel, the service will respond with
// a `WriteResponse`. The service will not view the resource as `complete`
// until the client has sent a `WriteRequest` with `finish_write` set to
// `true`. Sending any requests on a stream after sending a request with
// `finish_write` set to `true` will cause an error. The client **should**
// check the `WriteResponse` it receives to determine how much data the
// service was able to commit and whether the service views the resource as
// `complete` or not.

type writeState struct {
	activeResourceName string
	hash               string
	writer             io.WriteCloser
	bytesWritten       int64
}

func checkInitialPreconditions(req *bspb.WriteRequest) error {
	if req.ResourceName == "" {
		return status.InvalidArgumentError("Initial ResourceName must not be null")
	}
	if req.WriteOffset != 0 {
		return status.InvalidArgumentError("Initial WriteOffset should be 0")
	}
	return nil
}

func checkSubsequentPreconditions(req *bspb.WriteRequest, ws *writeState) error {
	if req.ResourceName != "" {
		if req.ResourceName != ws.activeResourceName {
			return status.InvalidArgumentError(fmt.Sprintf("ResourceName '%s' does not match initial ResourceName: '%s'", req.ResourceName, ws.activeResourceName))
		}
	}
	if req.WriteOffset != ws.bytesWritten {
		return status.InvalidArgumentError(fmt.Sprintf("Incorrect WriteOffset. Expected %d, got %d", ws.bytesWritten, req.WriteOffset))
	}
	return nil
}

func (s *ByteStreamServer) initStreamState(ctx context.Context, req *bspb.WriteRequest) (*writeState, error) {
	hash, err := extractHash(req.ResourceName)
	if err != nil {
		return nil, err
	}
	ck, err := perms.UserPrefixCacheKey(ctx, s.env, hash)
	if err != nil {
		return nil, err
	}
	// The protocol says it is optional to allow overwriting. Skip it for now.
	// exists, err := s.cache.Contains(ctx, ck)
	// if err != nil {
	// 	return nil, err
	// }
	// if exists {
	// 	return nil, status.FailedPreconditionError(fmt.Sprintf("File %s already exists (ck: %s)", hash, ck))
	// }
	wc, err := s.cache.Writer(ctx, ck)
	if err != nil {
		return nil, err
	}
	return &writeState{
		activeResourceName: req.ResourceName,
		writer:             wc,
		bytesWritten:       0,
		hash:               hash,
	}, nil
}

func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	var streamState *writeState
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if streamState == nil { // First message
			if err := checkInitialPreconditions(req); err != nil {
				return err
			}
			streamState, err = s.initStreamState(stream.Context(), req)
			if err != nil {
				return err
			}
		} else { // Subsequent messages
			checkSubsequentPreconditions(req, streamState)
		}

		n, err := streamState.writer.Write(req.Data)
		if err != nil {
			return err
		}
		streamState.bytesWritten += int64(n)
		if req.FinishWrite {
			if err := streamState.writer.Close(); err != nil {
				return err
			}
			stream.SendAndClose(&bspb.WriteResponse{
				CommittedSize: streamState.bytesWritten,
			})
			break
		}
	}
	return nil
}

// `QueryWriteStatus()` is used to find the `committed_size` for a resource
// that is being written, which can then be used as the `write_offset` for
// the next `Write()` call.
//
// If the resource does not exist (i.e., the resource has been deleted, or the
// first `Write()` has not yet reached the service), this method  the
// error `NOT_FOUND`.
//
// The client **may** call `QueryWriteStatus()` at any time to determine how
// much data has been processed for this resource. This is useful if the
// client is buffering data and needs to know which data can be safely
// evicted. For any sequence of `QueryWriteStatus()` calls for a given
// resource name, the sequence of returned `committed_size` values will be
// non-decreasing.
func (s *ByteStreamServer) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	// For now, just tell the client that the entire write failed and let
	// them retry it.
	return &bspb.QueryWriteStatusResponse{
		CommittedSize: 0,
		Complete:      false,
	}, nil
}
