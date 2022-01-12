package byte_stream_server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/DataDog/zstd"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/devnull"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Keep under the limit of ~4MB (save 256KB).
	readBufSizeBytes = (1024 * 1024 * 4) - (1024 * 256)
)

type ByteStreamServer struct {
	env        environment.Env
	cache      interfaces.Cache
	bufferPool *bytebufferpool.Pool
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
	reader, err := cache.Reader(ctx, r.GetDigest(), req.ReadOffset)
	if err != nil {
		ht.TrackMiss(r.GetDigest())
		return err
	}
	defer reader.Close()

	if r.GetCompressor() == repb.Compressor_ZSTD {
		reader, err = NewZstdCompressor(reader, r.GetDigest().GetSizeBytes())
		if err != nil {
			return status.InternalErrorf("Failed to compress blob: %s", err)
		}
		defer reader.Close()
	}

	downloadTracker := ht.TrackDownload(r.GetDigest())

	bufSize := int64(readBufSizeBytes)
	if r.GetDigest().GetSizeBytes() > 0 && r.GetDigest().GetSizeBytes() < bufSize {
		bufSize = r.GetDigest().GetSizeBytes()
	}
	copyBuf := s.bufferPool.Get(bufSize)
	_, err = io.CopyBuffer(&streamWriter{stream}, reader, copyBuf[:bufSize])
	s.bufferPool.Put(copyBuf)
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
	// Top-level writer that handles incoming bytes.
	writer io.Writer

	decompressorCloser io.Closer
	cacheCloser        io.Closer

	checksum           *Checksum
	d                  *repb.Digest
	activeResourceName string
	offset             int64
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
		activeResourceName: req.ResourceName,
		d:                  r.GetDigest(),
	}

	// The protocol says it is *optional* to allow overwriting, but does
	// not specify what errors should be returned in that case. We would
	// like to return an "AlreadyExists" error here, but it causes errors
	// with parallel actions during remote execution.
	//
	// Protocol does say that if another parallel write had finished while
	// this one was ongoing, we can immediately return a response with the
	// committed size, so we'll just do that.
	exists, err := cache.Contains(ctx, r.GetDigest())
	if err != nil {
		return nil, err
	}
	ws.alreadyExists = exists

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
		decompressor := NewZstdDecompressor(ws.writer)
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
	if err := w.checksum.Check(w.d); err != nil {
		return err
	}

	return w.cacheCloser.Close()
}

func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	ctx := stream.Context()

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY)
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
				r, err := digest.ParseUploadResourceName(req.ResourceName)
				if err != nil {
					return err
				}
				return stream.SendAndClose(&bspb.WriteResponse{
					CommittedSize: r.GetDigest().GetSizeBytes(),
				})
			}

			streamState, err = s.initStreamState(ctx, req)
			if err != nil {
				return err
			}
			defer func() {
				if err := streamState.Close(); err != nil {
					log.Error(err.Error())
				}
			}()
			if streamState.alreadyExists {
				return stream.SendAndClose(&bspb.WriteResponse{
					CommittedSize: streamState.d.GetSizeBytes(),
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
		compression == repb.Compressor_ZSTD && s.env.GetConfigurator().GetCacheZstdTranscodingEnabled()
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

type zstdDecompressor struct {
	pw         *io.PipeWriter
	zstdReader io.ReadCloser
	done       chan error
}

// NewZstdDecompressor returns a WriteCloser that accepts zstd-compressed bytes,
// and streams the decompressed bytes to the given writer.
//
// Note that writes are not matched one-to-one, since the compression scheme may
// require more than one chunk of compressed data in order to write a single
// chunk of decompressed data.
func NewZstdDecompressor(writer io.Writer) io.WriteCloser {
	pr, pw := io.Pipe()
	d := &zstdDecompressor{
		pw:         pw,
		zstdReader: zstd.NewReader(pr),
		done:       make(chan error, 1),
	}
	go func() {
		defer pr.Close()
		_, err := io.Copy(writer, d.zstdReader)
		d.done <- err
		close(d.done)
	}()
	return d
}

func (d *zstdDecompressor) Write(p []byte) (int, error) {
	return d.pw.Write(p)
}

func (d *zstdDecompressor) Close() error {
	var lastErr error
	if err := d.pw.Close(); err != nil {
		lastErr = err
	}
	defer func() {
		if err := d.zstdReader.Close(); err != nil {
			log.Errorf("Failed to close decompressor; some objects in the zstd library may not be freed properly: %s", err)
		}
	}()
	// Wait for the remaining bytes to be decompressed by the goroutine. Note that
	// since the write-end of the pipe is closed, reads from the decompressor
	// should no longer be blocking, and it should return EOF as soon as the
	// remaining decompressed bytes are drained.
	err, ok := <-d.done
	if ok {
		lastErr = err
	}
	return lastErr
}

func NewZstdCompressor(reader io.Reader, decompressedLength int64) (io.ReadCloser, error) {
	// Optimization: If we can send the whole response in one message,
	// decompress all at once, instead of Read() then Close(), which may issue two
	// separate responses since Close() may flush some buffered data.
	if decompressedLength <= readBufSizeBytes {
		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, reader); err != nil {
			return nil, err
		}
		out, err := zstd.Compress(make([]byte, 0, int(decompressedLength)), buf.Bytes())
		if err != nil {
			return nil, err
		}
		return cachetools.NewBytesReadSeekCloser(out), nil
	}

	pr, pw := io.Pipe()
	go func() {
		compressor := zstd.NewWriter(pw)
		_, err := io.Copy(compressor, reader)
		defer func(err error) {
			// Pipe writer needs to be closed after the compressor is closed, since
			// closing the compressor may result in more bytes written to the pipe.
			pw.CloseWithError(err)
		}(err)
		if err := compressor.Close(); err != nil {
			// ErrClosePipe means the compressor tried to flush its remaining data to
			// the pipe, but the pipe was closed since the client canceled their
			// request or some other error occurred. This can be safely ignored;
			// the compressor will still clean up properly in this case.
			if err == io.ErrClosedPipe {
				return
			}
			log.Errorf("Failed to close zstd writer: %s", err)
		}
	}()
	return pr, nil
}
