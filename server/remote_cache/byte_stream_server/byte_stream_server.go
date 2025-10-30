package byte_stream_server

import (
	"context"
	"encoding/hex"
	"hash"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_deprecation"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/peer"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// readBufSizeBytes controls the buffer size used for reading from the
	// cache. Benchmarks show that 256KiB or 512KiB perform similarly,
	// but experiments in dev show that 256KiB is better. Values smaller than
	// 256KiB or larger than 1MiB are both slower and allocate more bytes.
	readBufSizeBytes = 256 * 1024
)

var (
	bazel5_1_0              = bazel_request.MustParseVersion("5.1.0")
	maxDirectWriteSizeBytes = flag.Int64("cache.max_direct_write_size_bytes", 16384, "For bytestream requests smaller than this size, write straight to the cache without checking if the entry already exists.")
)

type ByteStreamServer struct {
	env        environment.Env
	cache      interfaces.Cache
	bufferPool *bytebufferpool.VariableSizePool
	warner     *bazel_deprecation.Warner
}

func Register(env *real_environment.RealEnv) error {
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
		bufferPool: bytebufferpool.VariableSize(readBufSizeBytes),
		warner:     bazel_deprecation.NewWarner(env),
	}, nil
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

func rpcPeerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

// `Read()` is used to retrieve the contents of a resource as a sequence
// of bytes. The bytes are returned in a sequence of responses, and the
// responses are delivered as the results of a server-side streaming FUNC (S *BYTESTREAMSERVER).
func (s *ByteStreamServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	if err := checkReadPreconditions(req); err != nil {
		return err
	}
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	return s.ReadCASResource(stream.Context(), rn, req.GetReadOffset(), req.GetReadLimit(), stream)
}

// This version of Read accepts the parameters of a ReadRequest directly so it
// can be called by ByteStreamServerProxy to avoid re-parsing resource names.
func (s *ByteStreamServer) ReadCASResource(ctx context.Context, r *digest.CASResourceName, offset, limit int64, stream bspb.ByteStream_ReadServer) error {
	if !s.supportsCompressor(r.GetCompressor()) {
		return status.UnimplementedErrorf("Unsupported compressor %s", r.GetCompressor())
	}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return err
	}

	ht := s.env.GetHitTrackerFactory().NewCASHitTracker(ctx, bazel_request.GetRequestMetadata(ctx))
	if r.IsEmpty() {
		dt := ht.TrackDownload(r.GetDigest())
		if err := dt.CloseWithBytesTransferred(0, 0, r.GetCompressor(), "byte_stream_server"); err != nil {
			log.Debugf("ByteStream Read: hit tracker TrackEmptyHit error: %s", err)
		}
		return nil
	}
	downloadTracker := ht.TrackDownload(r.GetDigest())

	cacheRN := digest.NewCASResourceName(r.GetDigest(), r.GetInstanceName(), r.GetDigestFunction())
	passthroughCompressionEnabled := s.cache.SupportsCompressor(r.GetCompressor()) && offset == 0 && limit == 0
	if passthroughCompressionEnabled {
		cacheRN.SetCompressor(r.GetCompressor())
	}
	reader, err := s.cache.Reader(ctx, cacheRN.ToProto(), offset, limit)
	if err != nil {
		if err := ht.TrackMiss(r.GetDigest()); err != nil {
			log.Debugf("ByteStream Read: hit tracker TrackMiss error: %s", err)
		}
		return err
	}
	defer reader.Close()

	bufSize := int64(readBufSizeBytes)
	if r.GetDigest().GetSizeBytes() > 0 && r.GetDigest().GetSizeBytes() < bufSize {
		bufSize = r.GetDigest().GetSizeBytes()
	}

	// If the cache doesn't support the requested compression, it will cache decompressed bytes and the server
	// is in charge of compressing it
	var counter *ioutil.Counter
	if r.GetCompressor() == repb.Compressor_ZSTD && !passthroughCompressionEnabled {
		// Counter for the number of bytes from the original reader containing decompressed bytes
		counter = &ioutil.Counter{}
		rc := io.NopCloser(io.TeeReader(reader, counter))

		reader, err = compression.NewBufferedCompressionReader(rc, s.bufferPool, bufSize)
		if err != nil {
			return status.InternalErrorf("Failed to compress blob: %s", err)
		}
		defer reader.Close()
	}

	copyBuf := s.bufferPool.Get(bufSize)
	defer s.bufferPool.Put(copyBuf)

	bytesTransferredToClient := 0
	for {
		n, err := ioutil.ReadTryFillBuffer(reader, copyBuf)
		bytesTransferredToClient += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&bspb.ReadResponse{Data: copyBuf[:n]}); err != nil {
			return err
		}
	}
	// If the reader was not passed through the compressor above, the data will be sent
	// as is from the cache to the client, and the number of bytes will be equal
	bytesFromCache := int64(bytesTransferredToClient)
	if counter != nil {
		bytesFromCache = counter.Count()
	}

	if err := downloadTracker.CloseWithBytesTransferred(bytesFromCache, int64(bytesTransferredToClient), r.GetCompressor(), "byte_stream_server"); err != nil {
		log.Debugf("ByteStream Read: downloadTracker.CloseWithBytesTransferred error: %s", err)
	}
	return err
}

// writeHandler enapsulates an on-going ByteStream write to a cache,
// freeing the caller of having to manage writing and committing-to the cache
// tracking cache hits, verifying checksums, etc. Here is how it must be used:
//   - A new WriteHandler may be obtained by providing the first frame of the
//     stream to the ByteStreamServer.beginWrite() function. This function will
//     return a new writeHandler, or an error.
//   - If a writeHandler is returned from beginWrite(),
//     writeHandler.Close() must be called to free system resources
//     when the write is finished.
//   - Each subsequent frame should be passed to
//     writeHandler.Write(), which will return an error on error
//     (note: io.EOF indicates the cache believes the write is finished), or an
//     optional WriteResponse that should be sent to the client if the client
//     indicated the write is finished. This function will return (nil, nil) if
//     the frame was processed successfully, but the write is not finished yet.
type writeHandler struct {
	// Top-level writer that handles incoming bytes.
	writer io.Writer

	bytesUploadedFromClient int
	transferTimer           interfaces.TransferTimer

	bufioCloser    io.Closer
	cacheCommitter interfaces.Committer
	cacheCloser    io.Closer

	checksum           *Checksum
	resourceName       *digest.CASResourceName
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

func checkSubsequentPreconditions(req *bspb.WriteRequest, ws *writeHandler) error {
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

func (s *ByteStreamServer) beginWrite(ctx context.Context, req *bspb.WriteRequest) (*writeHandler, error) {
	if err := checkInitialPreconditions(req); err != nil {
		return nil, err
	}

	r, err := digest.ParseUploadResourceName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	if !s.supportsCompressor(r.GetCompressor()) {
		return nil, status.UnimplementedErrorf("Unsupported compressor %s", r.GetCompressor())
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}

	hitTracker := s.env.GetHitTrackerFactory().NewCASHitTracker(ctx, bazel_request.GetRequestMetadata(ctx))
	ws := &writeHandler{
		transferTimer:      hitTracker.TrackUpload(r.GetDigest()),
		resourceName:       r,
		resourceNameString: req.ResourceName,
	}
	canWrite, err := capabilities.IsGranted(ctx, s.env.GetAuthenticator(), cappb.Capability_CACHE_WRITE|cappb.Capability_CAS_WRITE)
	if err != nil {
		return nil, err
	}
	if !canWrite {
		// Return already-exists error if the API key may not write so that
		// higher-level code can detect and short-circuit this case.
		return nil, status.AlreadyExistsError("The provided API Key does not have permission to write to the cache")
	}

	casRN := digest.NewCASResourceName(r.GetDigest(), r.GetInstanceName(), r.GetDigestFunction())
	if s.cache.SupportsCompressor(r.GetCompressor()) {
		casRN.SetCompressor(r.GetCompressor())
	}
	compressData := false
	if casRN.GetCompressor() == repb.Compressor_IDENTITY && s.cache.SupportsCompressor(repb.Compressor_ZSTD) && r.GetDigest().GetSizeBytes() >= 100 {
		casRN.SetCompressor(repb.Compressor_ZSTD)
		compressData = true
	}

	if r.GetDigest().GetSizeBytes() >= *maxDirectWriteSizeBytes {
		// The protocol says it is *optional* to allow overwriting, but does
		// not specify what errors should be returned in that case. We would
		// like to return an "AlreadyExists" error here, but it causes errors
		// with parallel actions during remote execution.
		//
		// Protocol does say that if another parallel write had finished while
		// this one was ongoing, we can immediately return a response with the
		// committed size, so we'll just do that.
		exists, err := s.cache.Contains(ctx, casRN.ToProto())
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, status.AlreadyExistsError("Already exists")
		}
	}

	var committedWriteCloser interfaces.CommittedWriteCloser
	if r.IsEmpty() {
		committedWriteCloser = ioutil.DiscardWriteCloser()
	} else {
		cacheWriter, err := s.cache.Writer(ctx, casRN.ToProto())
		if err != nil {
			return nil, err
		}
		committedWriteCloser = cacheWriter
	}
	ws.cacheCommitter = committedWriteCloser
	ws.cacheCloser = committedWriteCloser

	hasher, err := digest.HashForDigestType(r.GetDigestFunction())
	if err != nil {
		return nil, err
	}
	ws.checksum = NewChecksum(hasher, r.GetDigestFunction())
	// Write to the cache first. This gives more time between the final cache
	// write and the commit, reducing the amount of time that the commit has to
	// wait to flush writes.
	ws.writer = io.MultiWriter(committedWriteCloser, ws.checksum)

	if r.GetCompressor() == repb.Compressor_ZSTD {
		// The incoming data is compressed.
		if s.cache.SupportsCompressor(r.GetCompressor()) {
			// If the cache supports compression, write the already compressed
			// bytes to the cache with committedWriteCloser but wrap the
			// checksum in a decompressor to validate the decompressed data.
			decompressingChecksum, err := compression.NewZstdDecompressor(ws.checksum)
			if err != nil {
				return nil, err
			}
			ws.writer = io.MultiWriter(committedWriteCloser, decompressingChecksum)
			ws.bufioCloser = decompressingChecksum
		} else {
			// If the cache doesn't support compression, wrap both the checksum and cache writer in a decompressor
			decompressor, err := compression.NewZstdDecompressor(ws.writer)
			if err != nil {
				return nil, err
			}
			ws.writer = decompressor
			ws.bufioCloser = decompressor
		}
	} else if compressData {
		// If the cache supports compression but the request isn't compressed,
		// wrap the cache writer in a compressor. This is faster than sending
		// uncompressed data to the cache and letting it compress it.
		compressor, err := compression.NewZstdCompressingWriter(committedWriteCloser, r.GetDigest().GetSizeBytes())
		if err != nil {
			return nil, err
		}
		ws.writer = io.MultiWriter(compressor, ws.checksum)
		ws.bufioCloser = compressor
	}

	return ws, nil
}

func (w *writeHandler) Write(req *bspb.WriteRequest) (*bspb.WriteResponse, error) {
	if err := checkSubsequentPreconditions(req, w); err != nil {
		return nil, err
	}

	n, err := w.writer.Write(req.Data)
	if err != nil {
		return nil, err
	}
	w.bytesUploadedFromClient += len(req.Data)
	w.offset += int64(n)
	if req.FinishWrite {
		if err := w.commit(); err != nil {
			return nil, err
		}
		return &bspb.WriteResponse{CommittedSize: w.offset}, nil
	}
	return nil, nil
}

func (w *writeHandler) commit() error {
	if w.bufioCloser != nil {
		defer func() {
			w.bufioCloser = nil
		}()
		// Close the decompressor or compressor, flushing any currently buffered
		// bytes. If this fails, don't bother computing the checksum or
		// commiting the file to cache, since the incoming data is likely
		// corrupt anyway.
		if err := w.bufioCloser.Close(); err != nil {
			log.Warning(err.Error())
			return err
		}
	}

	// Verify the checksum. If it does not match, note that the cache writer is
	// not committed, since that persists the file to cache.
	if err := w.checksum.Check(w.resourceName); err != nil {
		return err
	}

	return w.cacheCommitter.Commit()
}

func (w *writeHandler) Close() error {
	if err := w.transferTimer.CloseWithBytesTransferred(w.offset, int64(w.bytesUploadedFromClient), w.resourceName.GetCompressor(), "byte_stream_server"); err != nil {
		log.Debugf("ByteStream Write: uploadTracker.CloseWithBytesTransferred error: %s", err)
	}

	if w.bufioCloser != nil {
		w.bufioCloser.Close()
	}
	return w.cacheCloser.Close()
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
func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	ctx := stream.Context()
	var streamState *writeHandler
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if streamState == nil {
			streamState, err = s.beginWrite(ctx, req)
			if status.IsAlreadyExistsError(err) {
				hitTracker := s.env.GetHitTrackerFactory().NewCASHitTracker(ctx, bazel_request.GetRequestMetadata(ctx))
				return s.handleAlreadyExists(ctx, hitTracker, stream, req)
			}
			if err != nil {
				return err
			}
			defer func() {
				if err := streamState.Close(); err != nil {
					log.Error(err.Error())
				}
			}()
		}

		resp, err := streamState.Write(req)
		if err != nil {
			return err
		}
		if resp != nil {
			// Warn after the write has completed.
			if err := s.warner.Warn(ctx); err != nil {
				return err
			}
			return stream.SendAndClose(resp)
		}
	}
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
	// If the data has not been committed to the cache, then just tell the
	//client that we don't have anything and let them retry it.
	return &bspb.QueryWriteStatusResponse{
		CommittedSize: 0,
		Complete:      false,
	}, nil
}

func (s *ByteStreamServer) handleAlreadyExists(ctx context.Context, ht interfaces.HitTracker, stream bspb.ByteStream_WriteServer, firstRequest *bspb.WriteRequest) error {
	r, err := digest.ParseUploadResourceName(firstRequest.ResourceName)
	if err != nil {
		return err
	}
	clientUploadedBytes := int64(len(firstRequest.Data))

	uploadTracker := ht.TrackUpload(r.GetDigest())
	defer func() {
		if err := uploadTracker.CloseWithBytesTransferred(0, clientUploadedBytes, r.GetCompressor(), "byte_stream_server"); err != nil {
			log.Debugf("handleAlreadyExists: uploadTracker.CloseWithBytesTransferred error: %s", err)
		}
	}()

	// Uncompressed uploads can always short-circuit, returning
	// the digest size as the committed size.
	if r.GetCompressor() == repb.Compressor_IDENTITY {
		return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: r.GetDigest().GetSizeBytes()})
	}

	// Bazel pre-5.1.0 doesn't support short-circuiting compressed uploads.
	// Instead, it expects the committed size to match the size of the
	// compressed stream. Since there is no unique compressed representation,
	// the only way to get the compressed stream size is to read the full
	// stream.
	if v := bazel_request.GetVersion(ctx); v != nil && !v.IsAtLeast(bazel5_1_0) {
		if !firstRequest.FinishWrite {
			n, err := s.recvAll(stream)
			clientUploadedBytes += n
			if err != nil {
				return err
			}
		}
		return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: clientUploadedBytes})
	}

	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: -1})
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
	digestFunction repb.DigestFunction_Value
	hash           hash.Hash
	bytesWritten   int64
}

func NewChecksum(h hash.Hash, digestFunction repb.DigestFunction_Value) *Checksum {
	return &Checksum{
		digestFunction: digestFunction,
		hash:           h,
		bytesWritten:   0,
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

func (s *Checksum) Check(r *digest.CASResourceName) error {
	d := r.GetDigest()
	computedDigest := hex.EncodeToString(s.hash.Sum(nil))
	if computedDigest != d.GetHash() {
		return status.DataLossErrorf("Hash of uploaded bytes %q [%s] did not match provided digest: %q [%s].", computedDigest, s.digestFunction, d.GetHash(), r.GetDigestFunction())
	}
	if s.BytesWritten() != d.GetSizeBytes() {
		return status.DataLossErrorf("Uploaded bytes length (%d bytes) did not match digest (%d).", s.BytesWritten(), d.GetSizeBytes())
	}
	return nil
}
