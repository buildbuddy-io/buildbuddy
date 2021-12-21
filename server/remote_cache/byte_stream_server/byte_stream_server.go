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
	writer                   io.WriteCloser
	checksum                 hash.Hash
	checksumWriteCloser      io.WriteCloser
	uncompressedBytesWritten int64
	nextExpectedWriteOffset  int64
	d                        *repb.Digest
	activeResourceName       string
	alreadyExists            bool
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
	var wc io.WriteCloser
	if resource.GetHash() != digest.EmptySha256 && !exists {
		wc, err = cache.Writer(ctx, resource.Digest)
		if err != nil {
			return nil, err
		}
	} else {
		wc = devnull.NewWriteCloser()
	}
	ws.checksum = sha256.New()
	if resource.Compression == repb.Compressor_ZSTD {
		// Pipeline:
		//     compressed chunks >> pipe: pw,pr >> decompressor >> checksum goroutine
		pr, pw := io.Pipe()
		decompressor := zstd.NewReader(pr)
		done := make(chan struct{})
		// TODO(bduffany): This goroutine is blocking, because reads are not placed
		// on a queue. Test to see how this affects performance.
		go func() {
			defer close(done)
			buf := make([]byte, 512)
			for {
				select {
				case <-ctx.Done():
					return
				default: // fallthrough
				}
				n, err := decompressor.Read(buf)
				if n > 0 {
					if _, err := ws.checksum.Write(buf[:n]); err != nil {
						log.Errorf("Failed to update checksum: %s", err)
						return
					}
					// Update with the number of *uncompressed* bytes written.
					ws.uncompressedBytesWritten += int64(n)
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
		close := func() error {
			pw.Close()
			<-done
			return decompressor.Close()
		}
		ws.checksumWriteCloser = &writeCloser{pw.Write, close}
	} else {
		write := func(p []byte) (int, error) {
			n, err := ws.checksum.Write(p)
			ws.uncompressedBytesWritten += int64(n)
			return n, err
		}
		ws.checksumWriteCloser = &writeCloser{write, nopCloseFn}
	}
	ws.writer = wc
	ws.alreadyExists = exists
	if exists {
		ws.uncompressedBytesWritten = resource.GetSizeBytes()
	} else {
		ws.uncompressedBytesWritten = 0
	}
	return ws, nil
}

func (w *writeState) Write(buf []byte) error {
	_, err := w.writer.Write(buf)
	if err != nil {
		return err
	}
	n, err := w.checksumWriteCloser.Write(buf)
	// Note: Protocol says the next expected write offset refers to the
	// *compressed* byte stream. buf here is compressed, so we update the next
	// expected write offset by the number of bytes written from buf.
	w.nextExpectedWriteOffset += int64(n)
	return err
}

func (w *writeState) Close() error {
	// Verify that digest length and hash match.
	if err := w.checksumWriteCloser.Close(); err != nil {
		log.Errorf("Failed to close checksum writer: %s", err)
	}
	computedDigest := fmt.Sprintf("%x", w.checksum.Sum(nil))
	//fmt.Println("█ ByteStreamServer: Checksum=", computedDigest)
	if computedDigest != w.d.GetHash() {
		return status.DataLossErrorf("Uploaded bytes checksum (%q) did not match digest (%q).", computedDigest, w.d.GetHash())
	}
	if w.uncompressedBytesWritten != w.d.GetSizeBytes() {
		return status.DataLossErrorf("%d bytes were uploaded but %d were expected.", w.uncompressedBytesWritten, w.d.GetSizeBytes())
	}
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
		//fmt.Println("█ ByteStreamServer: Write:", req, err)
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
					CommittedSize: streamState.uncompressedBytesWritten,
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
				CommittedSize: streamState.uncompressedBytesWritten,
			})
		}
	}
	return nil
}

func (s *ByteStreamServer) supportsCompression(compression repb.Compressor_Value) bool {
	// DO NOT MERGE: This impl is for local benchmarking only to ensure we are
	// only testing one type of compression at a time.
	if s.env.GetConfigurator().GetCacheZstdEnabled() {
		return compression == repb.Compressor_ZSTD
	}
	return compression == repb.Compressor_IDENTITY

	// CORRECT IMPL:
	// return compression == repb.Compressor_IDENTITY || compression == repb.Compressor_ZSTD && s.env.GetConfigurator().GetCacheZstdEnabled()
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

var nopCloseFn = func() error { return nil }

type writeCloser struct {
	WriteFn func(p []byte) (int, error)
	CloseFn func() error
}

func (wc *writeCloser) Write(p []byte) (int, error) { return wc.WriteFn(p) }
func (wc *writeCloser) Close() error                { return wc.CloseFn() }
