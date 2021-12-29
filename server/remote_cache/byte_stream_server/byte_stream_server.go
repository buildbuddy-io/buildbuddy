package byte_stream_server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/DataDog/zstd"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/cachelog"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
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
	resource, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	if !s.supportsCompression(resource.Compression) {
		return status.InvalidArgumentError("Unsupported compression type")
	}
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	cache, err := s.getCache(ctx, resource.GetInstanceName())
	if err != nil {
		return err
	}
	if resource.GetHash() == digest.EmptySha256 {
		ht.TrackEmptyHit()
		return nil
	}
	reader, err := cache.Reader(ctx, resource.Digest, req.ReadOffset)
	if err != nil {
		ht.TrackMiss(resource.Digest)
		return err
	}
	defer reader.Close()

	// TODO(DO NOT MERGE): Decide whether we want to keep this decompressive transcoding.
	if resource.Compression == repb.Compressor_IDENTITY && s.env.GetConfigurator().GetCacheZstdStorageEnabled() {
		reader = zstd.NewReader(reader)
		defer reader.Close()
	}
	// TODO(DO NOT MERGE): Decide whether we want compressive transcoding.

	downloadTracker := ht.TrackDownload(resource.Digest)

	bufSize := int64(readBufSizeBytes)
	if resource.GetSizeBytes() > 0 && resource.GetSizeBytes() < bufSize {
		bufSize = resource.GetSizeBytes()
	}
	copyBuf := s.bufferPool.Get(bufSize)
	_, err = io.CopyBuffer(&streamWriter{stream}, reader, copyBuf[:bufSize])
	s.bufferPool.Put(copyBuf)
	if err == nil {
		cachelog.Log(ctx, cachelog.ByteStreamRead, resource.Digest.Hash, resource.Digest.SizeBytes)
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
	writer                  io.WriteCloser
	checksum                *Checksum
	nextExpectedWriteOffset int64
	d                       *repb.Digest
	activeResourceName      string
	alreadyExists           bool
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
	if req.WriteOffset != ws.nextExpectedWriteOffset {
		return status.InvalidArgumentErrorf("Incorrect WriteOffset. Expected %d, got %d", ws.nextExpectedWriteOffset, req.WriteOffset)
	}
	return nil
}

func (s *ByteStreamServer) initStreamState(ctx context.Context, req *bspb.WriteRequest) (*writeState, error) {
	resource, err := digest.ParseUploadResourceName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	if !s.supportsCompression(resource.Compression) {
		return nil, status.InvalidArgumentError("Unsupported compression type")
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	cache, err := s.getCache(ctx, resource.GetInstanceName())
	if err != nil {
		return nil, err
	}

	ws := &writeState{
		activeResourceName: req.ResourceName,
		d:                  resource.Digest,
	}

	// The protocol says it is *optional* to allow overwriting, but does
	// not specify what errors should be returned in that case. We would
	// like to return an "AlreadyExists" error here, but it causes errors
	// with parallel actions during remote execution.
	//
	// Protocol does say that if another parallel write had finished while
	// this one was ongoing, we can immediately return a response with the
	// committed size, so we'll just do that.
	exists, err := cache.Contains(ctx, resource.Digest)
	if err != nil {
		return nil, err
	}
	var cacheWriter io.WriteCloser
	if resource.GetHash() != digest.EmptySha256 && !exists {
		cacheWriter, err = cache.Writer(ctx, resource.Digest)
		if err != nil {
			return nil, err
		}
	}

	ws.checksum = NewChecksum(resource.Digest)
	// Set up the stream processing pipeline, starting with the checksum as the
	// final stage and adding more pre-processing stages if applicable.
	//
	// NOTE: We don't close any WriteClosers in this function because closing
	// needs to happen at the end of the Write. When closing, we only close the
	// WriteCloser at the beginning of the pipeline. So, to ensure that all
	// WriteClosers are closed, each WriteCloser has the semantics that any
	// "wrapped" WriteClosers must be closed when calling Close.
	//
	// TODO(DO NOT MERGE): Probably simpler to append each closer to a list
	// and loop over all closers, rather than having this recursive closing
	// behavior which might be error prone. It would also avoid the need for
	// MultiWriteCloser.
	var wc io.WriteCloser = ws.checksum
	// If the cache stores blobs uncompressed, then write to cache before
	// computing the checksum, since the checksum operates on uncompressed
	// bytes.
	if cacheWriter != nil && !s.env.GetConfigurator().GetCacheZstdStorageEnabled() {
		wc = MultiWriteCloser(cacheWriter, wc)
	}
	// If the incoming data is compressed, feed it through a decompressor before
	// it gets to the final stage. Decompression is always required in this case
	// because we need to compute the checksum over the decompressed bytes.
	if resource.Compression == repb.Compressor_ZSTD {
		wc = NewZstdDecompressor(ctx, wc)
	}
	// If the incoming data is compressed and we want to store data compressed,
	// tee it into the cache before it gets to the decompression stage.
	if resource.Compression == repb.Compressor_ZSTD && s.env.GetConfigurator().GetCacheZstdStorageEnabled() {
		wc = MultiWriteCloser(cacheWriter, wc)
	}
	// If the incoming data is *not* compressed, but we only want to store
	// compressed data, then we need a compression stage as well.
	if cacheWriter != nil && resource.Compression == repb.Compressor_IDENTITY && s.env.GetConfigurator().GetCacheZstdStorageEnabled() {
		compressor := NewZstdCompressor(cacheWriter)
		wc = MultiWriteCloser(compressor, wc)
	}

	// Final picture:
	//
	//    data >> 1 >> [[compressor] >> cache]
	//            2 >> [decompressor] >> 1 >> [cache]
	//                                   2 >> checksum
	//
	// Components in [square brackets] are conditionally present, `>>` indicates
	// data flow via calls to Write(), and a numbered list indicates an ordered
	// sequence of writes, where data propagates to the end of each step before
	// moving on to the next step.

	ws.writer = wc
	ws.alreadyExists = exists
	return ws, nil
}

func (w *writeState) BytesWritten() int64 {
	if w.alreadyExists {
		return w.d.GetSizeBytes()
	}
	return w.checksum.BytesWritten()
}

func (w *writeState) Write(buf []byte) error {
	n, err := w.writer.Write(buf)
	if err != nil {
		return err
	}
	// Note: Protocol says the next expected write offset refers to the
	// *compressed* byte stream. buf here is compressed, so we update the next
	// expected write offset by the number of bytes written from buf.
	w.nextExpectedWriteOffset += int64(n)
	return err
}

func (w *writeState) Close() error {
	return w.writer.Close()
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
				resource, err := digest.ParseUploadResourceName(req.ResourceName)
				if err != nil {
					return err
				}
				if !s.supportsCompression(resource.Compression) {
					return status.InvalidArgumentError("Unsupported compression type")
				}
				return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: resource.GetSizeBytes()})
			}

			streamState, err = s.initStreamState(ctx, req)
			if err != nil {
				return err
			}
			if streamState.alreadyExists {
				return stream.SendAndClose(&bspb.WriteResponse{
					CommittedSize: streamState.BytesWritten(),
				})
			}
			ht := hit_tracker.NewHitTracker(ctx, s.env, false)
			uploadTracker := ht.TrackUpload(streamState.d)
			defer uploadTracker.Close()
			cachelog.Log(ctx, cachelog.ByteStreamWrite, streamState.d.Hash, streamState.d.SizeBytes)
		} else { // Subsequent messages
			if err := checkSubsequentPreconditions(req, streamState); err != nil {
				return err
			}
		}
		if err := streamState.Write(req.Data); err != nil {
			return err
		}

		if req.FinishWrite {
			if err := streamState.Close(); err != nil {
				return err
			}
			return stream.SendAndClose(&bspb.WriteResponse{
				CommittedSize: streamState.BytesWritten(),
			})
		}
	}
	return nil
}

func (s *ByteStreamServer) supportsCompression(compression repb.Compressor_Value) bool {
	return compression == repb.Compressor_IDENTITY ||
		compression == repb.Compressor_ZSTD && s.env.GetConfigurator().GetCacheZstdAccepted()
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

	d *repb.Digest
}

func NewChecksum(d *repb.Digest) *Checksum {
	return &Checksum{
		hash:         sha256.New(),
		bytesWritten: 0,
		d:            d,
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

func (s *Checksum) Close() error {
	// When closing, verify digest and length.
	computedDigest := fmt.Sprintf("%x", s.hash.Sum(nil))
	if computedDigest != s.d.GetHash() {
		return status.DataLossErrorf("Uploaded bytes checksum (%q) did not match digest (%q).", computedDigest, s.d.GetHash())
	}
	if s.bytesWritten != s.d.GetSizeBytes() {
		return status.DataLossErrorf("%d bytes were uploaded but %d were expected.", s.bytesWritten, s.d.GetSizeBytes())
	}
	return nil
}

type zstdCompressor struct {
	*zstd.Writer
	sink io.WriteCloser
}

// NewZstdCompressor returns a WriteCloser that writes the zstd-compressed bytes
// to the given WriteCloser. When closed, it also closes the given WriteCloser.
func NewZstdCompressor(sink io.WriteCloser) io.WriteCloser {
	return &zstdCompressor{
		Writer: zstd.NewWriter(sink),
		sink:   sink,
	}
}

func (c *zstdCompressor) Close() error {
	var out error
	if err := c.Writer.Close(); err != nil {
		out = err
	}
	if err := c.sink.Close(); err != nil {
		out = err
	}
	return out
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
func NewZstdDecompressor(ctx context.Context, wc io.WriteCloser) io.WriteCloser {
	pr, pw := io.Pipe()
	d := &zstdDecompressor{
		pw:         pw,
		zstdReader: zstd.NewReader(pr),
		done:       make(chan error, 1),
	}
	// TODO(bduffany): Consider buffering write operations in a channel to avoid
	// blocking writes on decompression.
	go func() {
		defer pr.Close()
		defer close(d.done)
		buf := make([]byte, 512)
		for {
			select {
			case <-ctx.Done():
				return
			default: // fallthrough
			}
			n, err := d.zstdReader.Read(buf)
			if n > 0 {
				if _, err := wc.Write(buf[:n]); err != nil {
					d.done <- err
					return
				}
			}
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Errorf("Failed to decompress bytes for checksum: %s", err)
				return
			}
		}
	}()
	return d
}

func (d *zstdDecompressor) Write(p []byte) (int, error) {
	return d.pw.Write(p)
}

func (d *zstdDecompressor) Close() error {
	d.pw.Close()
	defer func() {
		if err := d.zstdReader.Close(); err != nil {
			log.Errorf("Failed to close decompressor; some objects in the zstd library may not be freed properly: %s", err)
		}
	}()
	// Wait for the remaining bytes to be decompressed by the goroutine. Note that
	// since the write-end of the pipe is closed, reads from the decompressor should
	// no longer be blocking, and it should return EOF as soon as the remaining decompressed
	// bytes are drained.
	err, ok := <-d.done
	if ok {
		return err
	}
	return nil
}

type multiWriteCloser struct {
	writeClosers []io.WriteCloser
}

// MultiWriteCloser creates a WriteCloser that duplicates its write and close
// operations to all the provided WriteClosers, similar to the Unix tee(1)
// command.
//
// Each write is written to each listed writer, one at a time. If a listed
// writer returns an error, that overall write operation stops and returns the
// error; it does not continue down the list.
//
// The close operation is also applied to each listed closer, one at a time. If
// a listed closer returns an error, the remaining closers are still closed. If
// an error is returned from one or more close operations, the last non-nil
// error is returned.
func MultiWriteCloser(wcs ...io.WriteCloser) io.WriteCloser {
	allWriteClosers := make([]io.WriteCloser, 0, len(wcs))
	for _, wc := range wcs {
		// Flatten child MultiWriteClosers, consistent with io.MultiWriter implementation.
		if mwc, ok := wc.(*multiWriteCloser); ok {
			allWriteClosers = append(allWriteClosers, mwc.writeClosers...)
		} else {
			allWriteClosers = append(allWriteClosers, wc)
		}
	}
	return &multiWriteCloser{allWriteClosers}
}

func (m *multiWriteCloser) Write(p []byte) (int, error) {
	for _, wc := range m.writeClosers {
		n, err := wc.Write(p)
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
	}
	return len(p), nil
}

func (m *multiWriteCloser) Close() error {
	var out error
	for _, wc := range m.writeClosers {
		if err := wc.Close(); err != nil {
			// TODO: aggregate errors instead of only returning the last one.
			out = err
		}
	}
	return out
}
