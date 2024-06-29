package byte_stream_server

import (
	"context"
	"fmt"
	"hash"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Keep under the limit of ~4MB (save 256KB).
	readBufSizeBytes = (1024 * 1024 * 4) - (1024 * 256)
)

var (
	bazel5_1_0              = bazel_request.MustParseVersion("5.1.0")
	maxDirectWriteSizeBytes = flag.Int64("cache.max_direct_write_size_bytes", 0, "For bytestream requests smaller than this size, write straight to the cache without checking if the entry already exists.")
)

type ByteStreamServer struct {
	env        environment.Env
	cache      interfaces.Cache
	bufferPool *bytebufferpool.VariableSizePool
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
	if !SupportsCompressor(r.GetCompressor()) {
		return status.UnimplementedErrorf("Unsupported compressor %s", r.GetCompressor())
	}
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, false /*=ac*/)
	if r.IsEmpty() {
		if err := ht.TrackEmptyHit(); err != nil {
			log.Debugf("ByteStream Read: hit tracker TrackEmptyHit error: %s", err)
		}
		return nil
	}
	downloadTracker := ht.TrackDownload(r.GetDigest())

	cacheRN := digest.NewResourceName(r.GetDigest(), r.GetInstanceName(), rspb.CacheType_CAS, r.GetDigestFunction())
	passthroughCompressionEnabled := s.cache.SupportsCompressor(r.GetCompressor()) && req.ReadOffset == 0 && req.ReadLimit == 0
	if passthroughCompressionEnabled {
		cacheRN.SetCompressor(r.GetCompressor())
	}
	reader, err := s.cache.Reader(ctx, cacheRN.ToProto(), req.ReadOffset, req.ReadLimit)
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
		rbuf := s.bufferPool.Get(bufSize)
		defer s.bufferPool.Put(rbuf)
		cbuf := s.bufferPool.Get(bufSize)
		defer s.bufferPool.Put(cbuf)

		// Counter for the number of bytes from the original reader containing decompressed bytes
		counter = &ioutil.Counter{}
		reader, err = compression.NewZstdCompressingReader(io.NopCloser(io.TeeReader(reader, counter)), rbuf, cbuf)
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
	cacheCommitter     interfaces.Committer
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
	if !SupportsCompressor(r.GetCompressor()) {
		return nil, status.UnimplementedErrorf("Unsupported compressor %s", r.GetCompressor())
	}
	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}

	ws := &writeState{
		resourceName:       r,
		resourceNameString: req.ResourceName,
	}

	casRN := digest.NewResourceName(r.GetDigest(), r.GetInstanceName(), rspb.CacheType_CAS, r.GetDigestFunction())
	if s.cache.SupportsCompressor(r.GetCompressor()) {
		casRN.SetCompressor(r.GetCompressor())
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
	ws.writer = io.MultiWriter(ws.checksum, committedWriteCloser)

	if r.GetCompressor() == repb.Compressor_ZSTD {
		if s.cache.SupportsCompressor(r.GetCompressor()) {
			// If the cache supports compression, write compressed bytes to the cache with committedWriteCloser
			// but wrap the checksum in a decompressor to validate the decompressed data
			decompressingChecksum, err := compression.NewZstdDecompressor(ws.checksum)
			if err != nil {
				return nil, err
			}
			ws.writer = io.MultiWriter(decompressingChecksum, committedWriteCloser)
			ws.decompressorCloser = decompressingChecksum
		} else {
			// If the cache doesn't support compression, wrap both the checksum and cache writer in a decompressor
			decompressor, err := compression.NewZstdDecompressor(ws.writer)
			if err != nil {
				return nil, err
			}
			ws.writer = decompressor
			ws.decompressorCloser = decompressor
		}
	}

	return ws, nil
}

func (w *writeState) Write(buf []byte) error {
	n, err := w.writer.Write(buf)
	w.offset += int64(n)
	return err
}

func (w *writeState) Flush() error {
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
	// not committed, since that persists the file to cache.
	if err := w.checksum.Check(w.resourceName); err != nil {
		return err
	}

	return w.cacheCommitter.Commit()
}

func (w *writeState) Close() error {
	return w.cacheCloser.Close()
}

type streamLike interface {
	Context() context.Context
	SendAndClose(*bspb.WriteResponse) error
	Recv() (*bspb.WriteRequest, error)
}

func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	return s.WriteDirect(stream)
}

func (s *ByteStreamServer) WriteDirect(stream streamLike) error {
	ctx := stream.Context()

	canWrite, err := capabilities.IsGranted(ctx, s.env, akpb.ApiKey_CACHE_WRITE_CAPABILITY|akpb.ApiKey_CAS_WRITE_CAPABILITY)
	if err != nil {
		return err
	}

	var streamState *writeState
	bytesUploadedFromClient := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		bytesUploadedFromClient += len(req.Data)
		if streamState == nil { // First message
			if err := checkInitialPreconditions(req); err != nil {
				return err
			}

			ht := hit_tracker.NewHitTracker(ctx, s.env, false)

			// If the API key is read-only, pretend the object already exists.
			if !canWrite {
				return s.handleAlreadyExists(ctx, ht, stream, req)
			}

			streamState, err = s.initStreamState(ctx, req)
			if status.IsAlreadyExistsError(err) {
				return s.handleAlreadyExists(ctx, ht, stream, req)
			}
			if err != nil {
				return err
			}
			defer func() {
				if err := streamState.Close(); err != nil {
					log.Error(err.Error())
				}
			}()
			uploadTracker := ht.TrackUpload(streamState.resourceName.GetDigest())
			defer func() {
				if err := uploadTracker.CloseWithBytesTransferred(streamState.offset, int64(bytesUploadedFromClient), streamState.resourceName.GetCompressor(), "byte_stream_server"); err != nil {
					log.Debugf("ByteStream Write: uploadTracker.CloseWithBytesTransferred error: %s", err)
				}
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
			// Note: Need to Flush before committing, since this
			// flushes any currently buffered bytes from the
			// decompressor to the cache writer.
			if err := streamState.Flush(); err != nil {
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

func SupportsCompressor(compression repb.Compressor_Value) bool {
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

func (s *ByteStreamServer) handleAlreadyExists(ctx context.Context, ht *hit_tracker.HitTracker, stream streamLike, firstRequest *bspb.WriteRequest) error {
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
func (s *ByteStreamServer) recvAll(stream streamLike) (int64, error) {
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

func (s *Checksum) Check(r *digest.ResourceName) error {
	d := r.GetDigest()
	computedDigest := fmt.Sprintf("%x", s.hash.Sum(nil))
	if computedDigest != d.GetHash() {
		return status.DataLossErrorf("Hash of uploaded bytes %q [%s] did not match provided digest: %q [%s].", computedDigest, s.digestFunction, d.GetHash(), r.GetDigestFunction())
	}
	if s.BytesWritten() != d.GetSizeBytes() {
		return status.DataLossErrorf("Uploaded bytes length (%d bytes) did not match digest (%d).", s.BytesWritten(), d.GetSizeBytes())
	}
	return nil
}
