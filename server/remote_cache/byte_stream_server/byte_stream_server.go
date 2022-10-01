package byte_stream_server

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"hash"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/devnull"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Keep under the limit of ~4MB (save 256KB).
	readBufSizeBytes = (1024 * 1024 * 4) - (1024 * 256)
)

var (
	enableQueryWriteStatusCacheCheck = flag.Bool("cache.enable_query_write_status_cache_check", false, "If enabled, QueryWriteStatus ByteStream RPC will check whether digest is present in the cache.")
)

type ByteStreamServer struct {
	env        environment.Env
	cache      interfaces.Cache
	bufferPool *bytebufferpool.Pool
}

func Register(env environment.Env) error {
	// OPTIONAL CACHE API -- only enable if configured.
	if env.GetCache() == nil {
		return nil
	}
	byteStreamServer, err := NewByteStreamServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServer: %s", err)
	}
	env.SetByteStreamServer(byteStreamServer)
	return nil
}

func NewByteStreamServer(env environment.Env) (*ByteStreamServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, status.FailedPreconditionError("A cache is required to enable the ByteStreamServer")
	}
	return &ByteStreamServer{
		env:        env,
		cache:      cache,
		bufferPool: bytebufferpool.New(readBufSizeBytes),
	}, nil
}

func (s *ByteStreamServer) getCache(ctx context.Context, instanceName string) (interfaces.Cache, error) {
	return namespace.CASCache(ctx, s.cache, instanceName)
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
	r, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	if !s.supportsCompressor(r.GetCompressor()) {
		return status.UnimplementedErrorf("Unsupported compressor %s", r.GetCompressor())
	}
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	cache, err := s.getCache(ctx, r.GetInstanceName())
	if err != nil {
		return err
	}
	if r.GetDigest().GetHash() == digest.EmptySha256 {
		ht.TrackEmptyHit()
		return nil
	}
	reader, err := cache.Reader(ctx, r.GetDigest(), req.ReadOffset, req.ReadLimit)
	if err != nil {
		ht.TrackMiss(r.GetDigest())
		return err
	}
	defer reader.Close()

	bufSize := int64(readBufSizeBytes)
	if r.GetDigest().GetSizeBytes() > 0 && r.GetDigest().GetSizeBytes() < bufSize {
		bufSize = r.GetDigest().GetSizeBytes()
	}

	if r.GetCompressor() == repb.Compressor_ZSTD {
		rbuf := s.bufferPool.Get(bufSize)
		defer s.bufferPool.Put(rbuf)
		cbuf := s.bufferPool.Get(bufSize)
		defer s.bufferPool.Put(cbuf)
		reader, err = compression.NewZstdChunkingCompressor(reader, rbuf[:bufSize], cbuf[:bufSize])
		if err != nil {
			return status.InternalErrorf("Failed to compress blob: %s", err)
		}
		defer reader.Close()
	}

	downloadTracker := ht.TrackDownload(r.GetDigest())

	copyBuf := s.bufferPool.Get(bufSize)
	defer s.bufferPool.Put(copyBuf)
	n, err := io.CopyBuffer(&streamWriter{stream}, reader, copyBuf[:bufSize])
	downloadTracker.CloseWithBytesTransferred(n, r.GetCompressor())
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
	// Top-level writer that handles incoming bytes.
	writer io.Writer

	decompressorCloser io.Closer
	cacheCloser        io.Closer

	checksum           *Checksum
	resourceName       *digest.ResourceName
	resourceNameString string
	offset             int64
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
		if req.ResourceName != ws.resourceNameString {
			return status.InvalidArgumentErrorf("ResourceName '%s' does not match initial ResourceName: '%s'", req.ResourceName, ws.resourceNameString)
		}
	}
	if req.WriteOffset != ws.offset {
		return status.InvalidArgumentErrorf("Incorrect WriteOffset. Expected %d, got %d", ws.offset, req.WriteOffset)
	}
	return nil
}

func (s *ByteStreamServer) initStreamState(ctx context.Context, req *bspb.WriteRequest) (*writeState, error) {
	r, err := digest.ParseUploadResourceName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	if !s.supportsCompressor(r.GetCompressor()) {
		return nil, status.UnimplementedErrorf("Unsupported compressor %s", r.GetCompressor())
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	cache, err := s.getCache(ctx, r.GetInstanceName())
	if err != nil {
		return nil, err
	}

	ws := &writeState{
		resourceName:       r,
		resourceNameString: req.ResourceName,
	}

	// The protocol says it is *optional* to allow overwriting, but does
	// not specify what errors should be returned in that case. We would
	// like to return an "AlreadyExists" error here, but it causes errors
	// with parallel actions during remote execution.
	//
	// Protocol does say that if another parallel write had finished while
	// this one was ongoing, we can immediately return a response with the
	// committed size, so we'll just do that.
	exists, err := s.cache.Contains(ctx, &resource.ResourceName{
		Digest:       r.GetDigest(),
		InstanceName: r.GetInstanceName(),
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    resource.CacheType_CAS,
	})
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, status.AlreadyExistsError("Already exists")
	}

	ws.checksum = NewChecksum()
	ws.writer = ws.checksum
	cacheWriteCloser := devnull.NewWriteCloser()
	if r.GetDigest().GetHash() != digest.EmptySha256 && !exists {
		cacheWriteCloser, err = cache.Writer(ctx, r.GetDigest())
		if err != nil {
			return nil, err
		}
	}
	ws.cacheCloser = cacheWriteCloser
	ws.writer = io.MultiWriter(ws.checksum, cacheWriteCloser)
	if r.GetCompressor() == repb.Compressor_ZSTD {
		decompressor, err := compression.NewZstdDecompressor(ws.writer)
		if err != nil {
			return nil, err
		}
		ws.writer = decompressor
		ws.decompressorCloser = decompressor
	}
	return ws, nil
}

func (w *writeState) Write(buf []byte) error {
	n, err := w.writer.Write(buf)
	w.offset += int64(n)
	return err
}

func (w *writeState) Close() error {
	if w.decompressorCloser != nil {
		defer func() {
			w.decompressorCloser = nil
		}()
		// Close the decompressor, flushing any currently buffered bytes to the
		// checksum+cache multi-writer. If this fails, don't bother computing the
		// checksum or commiting the file to cache, since the incoming data is
		// likely corrupt anyway.
		if err := w.decompressorCloser.Close(); err != nil {
			log.Warning(err.Error())
			return err
		}
	}

	return nil
}

func (w *writeState) Commit() error {
	// Verify the checksum. If it does not match, note that the cache writer is
	// not closed, since that commits the file to cache.
	if err := w.checksum.Check(w.resourceName.GetDigest()); err != nil {
		return err
	}

	return w.cacheCloser.Close()
}

func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	ctx := stream.Context()

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY|akpb.ApiKey_CAS_WRITE_CAPABILITY)
	if err != nil {
		return err
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

			// If the API key is read-only, pretend the object already exists.
			if !canWrite {
				return s.handleAlreadyExists(ctx, stream, req)
			}
			streamState, err = s.initStreamState(ctx, req)
			if status.IsAlreadyExistsError(err) {
				return s.handleAlreadyExists(ctx, stream, req)
			}
			if err != nil {
				return err
			}
			defer func() {
				if err := streamState.Close(); err != nil {
					log.Error(err.Error())
				}
			}()
			ht := hit_tracker.NewHitTracker(ctx, s.env, false)
			uploadTracker := ht.TrackUpload(streamState.resourceName.GetDigest())
			defer func() {
				uploadTracker.CloseWithBytesTransferred(streamState.offset, streamState.resourceName.GetCompressor())
			}()
		} else { // Subsequent messages
			if err := checkSubsequentPreconditions(req, streamState); err != nil {
				return err
			}
		}
		if err := streamState.Write(req.Data); err != nil {
			return err
		}

		if req.FinishWrite {
			// Note: Need to Close before committing, since this flushes any currently
			// buffered bytes from the decompressor to the cache writer.
			if err := streamState.Close(); err != nil {
				return err
			}
			if err := streamState.Commit(); err != nil {
				return err
			}
			return stream.SendAndClose(&bspb.WriteResponse{
				CommittedSize: streamState.offset,
			})
		}
	}
	return nil
}

func (s *ByteStreamServer) supportsCompressor(compression repb.Compressor_Value) bool {
	return compression == repb.Compressor_IDENTITY ||
		compression == repb.Compressor_ZSTD && remote_cache_config.ZstdTranscodingEnabled()
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
	if !*enableQueryWriteStatusCacheCheck {
		// If the data has not been committed to the cache, then just tell the
		//client that we don't have anything and let them retry it.
		return &bspb.QueryWriteStatusResponse{
			CommittedSize: 0,
			Complete:      false,
		}, nil
	}

	rn, err := digest.ParseUploadResourceName(req.GetResourceName())
	if err != nil {
		// If we can't parse the resource name, then tell the client we
		// don't know anything about the write and let them retry it.
		return &bspb.QueryWriteStatusResponse{
			CommittedSize: 0,
			Complete:      false,
		}, nil
	}
	if rn.GetCompressor() != repb.Compressor_IDENTITY {
		// When the upload is compressed, we don't know what value to return,
		// since the committed size depends on how the client compressed the
		// contents. So return 0 here and let it retry the upload.
		// TODO(bduffany): For Bazel versions 5.1 and later, we can return
		// {CommittedSize: -1, Complete: true}.
		// See https://github.com/bazelbuild/bazel/issues/14654 for context.
		return &bspb.QueryWriteStatusResponse{
			CommittedSize: 0,
			Complete:      false,
		}, nil
	}

	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	cache, err := s.getCache(ctx, rn.GetInstanceName())
	if err != nil {
		return nil, err
	}

	md, err := cache.Metadata(ctx, rn.GetDigest())
	if err != nil {
		// If the data has not been committed to the cache, then just tell the
		// client that we don't have anything and let them retry it.
		if status.IsNotFoundError(err) {
			return &bspb.QueryWriteStatusResponse{
				CommittedSize: 0,
				Complete:      false,
			}, nil
		}
		return nil, err
	}

	return &bspb.QueryWriteStatusResponse{
		CommittedSize: md.SizeBytes,
		Complete:      true,
	}, nil
}

func (s *ByteStreamServer) handleAlreadyExists(ctx context.Context, stream bspb.ByteStream_WriteServer, firstRequest *bspb.WriteRequest) error {
	r, err := digest.ParseUploadResourceName(firstRequest.ResourceName)
	if err != nil {
		return err
	}
	// Bazel 5.0.0 effectively requires that the committed_size match the total
	// length of the *uploaded* payload. For compressed payloads, the uploaded
	// payload length is not yet known, because it depends on the compression
	// parameters used by the client. So we need to read the whole stream from the
	// client in the compressed case.
	//
	// See https://github.com/bazelbuild/bazel/issues/14654 for context.
	committedSize := r.GetDigest().GetSizeBytes()
	if r.GetCompressor() != repb.Compressor_IDENTITY {
		remainingSize := int64(0)
		if !firstRequest.FinishWrite {
			// In the case where we read the full stream in order to determine its
			// size, count it as an upload.
			ht := hit_tracker.NewHitTracker(ctx, s.env, false)
			uploadTracker := ht.TrackUpload(r.GetDigest())
			remainingSize, err = s.recvAll(stream)
			uploadTracker.CloseWithBytesTransferred(int64(len(firstRequest.Data))+remainingSize, r.GetCompressor())
			if err != nil {
				return err
			}
		}
		committedSize = int64(len(firstRequest.Data)) + remainingSize
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: committedSize})
}

// recvAll receives the remaining write requests from the client, and returns
// the total (compressed) size of the uploaded bytes.
func (s *ByteStreamServer) recvAll(stream bspb.ByteStream_WriteServer) (int64, error) {
	size := int64(0)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return size, nil
		}
		if err != nil {
			return size, err
		}
		size += int64(len(req.Data))
	}
}

type Checksum struct {
	hash         hash.Hash
	bytesWritten int64
}

func NewChecksum() *Checksum {
	return &Checksum{
		hash:         sha256.New(),
		bytesWritten: 0,
	}
}

func (s *Checksum) BytesWritten() int64 {
	return s.bytesWritten
}

func (s *Checksum) Write(p []byte) (int, error) {
	n, err := s.hash.Write(p)
	if err != nil {
		log.Errorf("Failed to update checksum: %s", err)
		return n, err
	}
	s.bytesWritten += int64(n)
	return n, nil
}

func (s *Checksum) Check(d *repb.Digest) error {
	computedDigest := fmt.Sprintf("%x", s.hash.Sum(nil))
	if computedDigest != d.GetHash() {
		return status.DataLossErrorf("Uploaded bytes sha256 hash (%q) did not match digest (%q).", computedDigest, d.GetHash())
	}
	if s.BytesWritten() != d.GetSizeBytes() {
		return status.DataLossErrorf("Uploaded bytes length (%d bytes) did not match digest (%d).", s.BytesWritten(), d.GetSizeBytes())
	}
	return nil
}
