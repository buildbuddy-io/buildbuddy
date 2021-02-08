package byte_stream_server

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Keep under the limit of ~4MB (1024 * 1024 * 4).
	readBufSizeBytes = (1024 * 1024 * 4) - 100
)

type ByteStreamServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func NewByteStreamServer(env environment.Env) (*ByteStreamServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, status.FailedPreconditionError("A cache is required to enable the ByteStreamServer")
	}
	return &ByteStreamServer{
		env:   env,
		cache: cache,
	}, nil
}

func (s *ByteStreamServer) getCache(instanceName string) interfaces.Cache {
	return namespace.CASCache(s.cache, instanceName)
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
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

type streamWriter struct {
	stream bspb.ByteStream_ReadServer
}

func (w *streamWriter) Write(buf []byte) (int, error) {
	err := w.stream.Send(&bspb.ReadResponse{
		Data: buf,
	})
	return len(buf), err
}

// `Read()` is used to retrieve the contents of a resource as a sequence
// of bytes. The bytes are returned in a sequence of responses, and the
// responses are delivered as the results of a server-side streaming FUNC (S *BYTESTREAMSERVER).
func (s *ByteStreamServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	if err := checkReadPreconditions(req); err != nil {
		return err
	}
	instanceName, d, err := digest.ExtractDigestFromDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	cache := s.getCache(instanceName)
	if d.GetHash() == digest.EmptySha256 {
		ht.TrackEmptyHit()
		return nil
	}
	reader, err := cache.Reader(ctx, d, req.ReadOffset)
	if err != nil {
		ht.TrackMiss(d)
		return err
	}

	downloadTracker := ht.TrackDownload(d)
	_, err = io.Copy(&streamWriter{stream}, reader)
	if err == nil {
		downloadTracker.Close()
	}
	return err
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
	d                  *repb.Digest
	writer             io.WriteCloser
	bytesWritten       int64
	alreadyExists      bool
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
			return status.InvalidArgumentErrorf("ResourceName '%s' does not match initial ResourceName: '%s'", req.ResourceName, ws.activeResourceName)
		}
	}
	if req.WriteOffset != ws.bytesWritten {
		return status.InvalidArgumentErrorf("Incorrect WriteOffset. Expected %d, got %d", ws.bytesWritten, req.WriteOffset)
	}
	return nil
}

// A writer that drops anything written to it.
// Useful when you need an io.Writer but don't intend
// to actually write bytes to it.
type discardWriteCloser struct {
	io.Writer
}

func NewDiscardWriteCloser() *discardWriteCloser {
	return &discardWriteCloser{
		ioutil.Discard,
	}
}
func (discardWriteCloser) Close() error {
	return nil
}

func (s *ByteStreamServer) initStreamState(ctx context.Context, req *bspb.WriteRequest) (*writeState, error) {
	instanceName, d, err := digest.ExtractDigestFromUploadResourceName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	cache := s.getCache(instanceName)

	ws := &writeState{
		activeResourceName: req.ResourceName,
		d:                  d,
	}

	// The protocol says it is *optional* to allow overwriting, but does
	// not specify what errors should be returned in that case. We would
	// like to return an "AlreadyExists" error here, but it causes errors
	// with parallel actions during remote execution.
	//
	// Protocol does say that if another parallel write had finished while
	// this one was ongoing, we can immediately return a response with the
	// committed size, so we'll just do that.
	exists, err := cache.Contains(ctx, d)
	if err != nil {
		return nil, err
	}
	var wc io.WriteCloser
	if d.GetHash() != digest.EmptySha256 && !exists {
		wc, err = cache.Writer(ctx, d)
		if err != nil {
			return nil, err
		}
	} else {
		wc = NewDiscardWriteCloser()
	}
	ws.writer = wc
	ws.alreadyExists = exists
	if exists {
		ws.bytesWritten = d.GetSizeBytes()
	} else {
		ws.bytesWritten = 0
	}
	return ws, nil

}

func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	ctx := stream.Context()

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY)
	if err != nil {
		return err
	}
	// If the API key is read-only, pretend the object already exists.
	if !canWrite {
		_, d, err := digest.ExtractDigestFromUploadResourceName(req.ResourceName)
		if err != nil {
			return err
		}
		return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: d.GetSizeBytes()})
	}

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
			streamState, err = s.initStreamState(ctx, req)
			if err != nil {
				return err
			}
			if streamState.alreadyExists {
				return stream.SendAndClose(&bspb.WriteResponse{
					CommittedSize: streamState.bytesWritten,
				})
			}
			ht := hit_tracker.NewHitTracker(ctx, s.env, false)
			uploadTracker := ht.TrackUpload(streamState.d)
			defer uploadTracker.Close()
		} else { // Subsequent messages
			if err := checkSubsequentPreconditions(req, streamState); err != nil {
				return err
			}
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
			return stream.SendAndClose(&bspb.WriteResponse{
				CommittedSize: streamState.bytesWritten,
			})
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
