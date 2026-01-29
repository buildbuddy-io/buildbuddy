package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunked_manifest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	maxChunkSizeBytes      = flag.Int64("cache_proxy.max_chunk_size_bytes", 2<<20, "Only blobs larger (non-inclusive) than this threshold will be chunked (default 2MB). This is also the maximum size of a chunk. The average chunk size will be 1/4 of this value, and the minimum will be 1/16 of this value.")
	chunkUploadConcurrency = flag.Int("cache_proxy.chunk_upload_concurrency", 8, "Maximum number of concurrent chunk uploads when uploading missing chunks to remote cache.")
)

type ByteStreamServerProxy struct {
	supportsEncryption func(context.Context) bool
	authenticator      interfaces.Authenticator
	local              interfaces.ByteStreamServer
	remote             bspb.ByteStreamClient
	efp                interfaces.ExperimentFlagProvider
	localCache         interfaces.Cache
	remoteCAS          repb.ContentAddressableStorageClient
	compressBufPool    *bytebufferpool.VariableSizePool
}

func Register(env *real_environment.RealEnv) error {
	proxy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServerProxy: %s", err)
	}
	env.SetByteStreamServer(proxy)
	return nil
}

func New(env environment.Env) (*ByteStreamServerProxy, error) {
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, fmt.Errorf("An Authenticator is required to enable ByteStreamServerProxy")
	}
	remote := env.GetByteStreamClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	local := env.GetLocalByteStreamServer()
	if local == nil {
		return nil, fmt.Errorf("A local ByteStreamServer is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		supportsEncryption: remote_crypter.SupportsEncryption(env),
		authenticator:      authenticator,
		local:              local,
		remote:             remote,
		efp:                env.GetExperimentFlagProvider(),
		localCache:         env.GetCache(),
		remoteCAS:          env.GetContentAddressableStorageClient(),
		compressBufPool:    bytebufferpool.VariableSize(int(*maxChunkSizeBytes)),
	}, nil
}

// Wrapper around a ByteStream_ReadServer that counts the number of frames
// and bytes read through it.
type meteredReadServerStream struct {
	bytes  int64
	frames int64
	bspb.ByteStream_ReadServer
}

func (s *meteredReadServerStream) Send(message *bspb.ReadResponse) error {
	s.bytes += int64(len(message.GetData()))
	s.frames++
	return s.ByteStream_ReadServer.Send(message)
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(ctx)
	meteredStream := &meteredReadServerStream{ByteStream_ReadServer: stream}
	stream = meteredStream
	readMetrics, err := s.read(ctx, req, meteredStream)
	bsm := byteStreamMetrics{
		requestType:  requestTypeLabel,
		compressor:   readMetrics.compressor,
		err:          err,
		bytes:        meteredStream.bytes,
		chunked:      readMetrics.chunked,
		blobBytes:    readMetrics.blobBytes,
		chunksTotal:  readMetrics.chunksTotal,
		chunksLocal:  readMetrics.chunksLocal,
		chunksRemote: readMetrics.chunksRemote,
	}
	recordReadMetrics(bsm, readMetrics.cacheStatus)
	return err
}

type readMetrics struct {
	cacheStatus  string // should be metrics.HitStatusLabel/MissStatusLabel
	compressor   string
	chunked      bool
	blobBytes    int64
	chunksTotal  int
	chunksLocal  int
	chunksRemote int
}

func (s *ByteStreamServerProxy) read(ctx context.Context, req *bspb.ReadRequest, stream *meteredReadServerStream) (readMetrics, error) {
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return readMetrics{
			cacheStatus: metrics.HitStatusLabel,
			compressor:  "unknown",
		}, err
	}

	if authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx) {
		return readMetrics{
			cacheStatus: metrics.UncacheableStatusLabel,
			compressor:  rn.GetCompressor().String(),
		}, s.readRemoteOnly(ctx, req, stream)
	}

	// Store auth headers in context so they can be reused between the
	// atime_updater and the hit_tracker_client.
	ctx = authutil.ContextWithCachedAuthHeaders(ctx, s.authenticator)

	if proxy_util.SkipRemote(ctx) {
		if err := s.readLocalOnly(req, stream); err != nil {
			log.CtxInfof(ctx, "Error reading local: %v", err)
			return readMetrics{
				cacheStatus: metrics.MissStatusLabel,
				compressor:  rn.GetCompressor().String(),
			}, err
		}
		return readMetrics{
			cacheStatus: metrics.HitStatusLabel,
			compressor:  rn.GetCompressor().String(),
		}, nil
	}

	localErr := s.local.ReadCASResource(ctx, rn, req.GetReadOffset(), req.GetReadLimit(), stream)
	// If some responses were streamed to the client, just return the
	// error. Otherwise, fall-back to remote. We might be able to continue
	// streaming to the client by doing an offset read from the remote
	// cache, but keep it simple for now.
	if localErr != nil && stream.frames == 0 {
		// Recover from local error if no frames have been sent

		// If the blob does not exist locally, it might exist in chunks.
		// If chunking is enabled and the blob is large enough, try to read the chunks.
		// TODO(buildbuddy-internal#6426): Once most large blobs are chunked, consider
		// attempting the chunked read first.
		if s.shouldReadChunked(ctx, req, rn) {
			chunkMetrics, chunkedErr := s.readChunked(ctx, req, stream, rn)
			if chunkedErr == nil {
				return readMetrics{
					cacheStatus:  metrics.MissStatusLabel,
					compressor:   rn.GetCompressor().String(),
					chunked:      true,
					blobBytes:    rn.GetDigest().GetSizeBytes(),
					chunksTotal:  chunkMetrics.chunksLocal + chunkMetrics.chunksRemote,
					chunksLocal:  chunkMetrics.chunksLocal,
					chunksRemote: chunkMetrics.chunksRemote,
				}, nil
			} else if !status.IsNotFoundError(chunkedErr) {
				log.CtxWarningf(ctx, "Error reading chunked blob %s: %s", rn.DownloadString(), chunkedErr)
			}
		}

		return readMetrics{
			cacheStatus: metrics.MissStatusLabel,
			compressor:  rn.GetCompressor().String(),
		}, s.readRemoteWriteLocal(req, stream)
	} else {
		return readMetrics{
			cacheStatus: metrics.HitStatusLabel,
			compressor:  rn.GetCompressor().String(),
		}, localErr
	}
}

func (s *ByteStreamServerProxy) shouldReadChunked(ctx context.Context, req *bspb.ReadRequest, rn *digest.CASResourceName) bool {
	if authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx) {
		// TODO(buildbuddy-internal#6426): Read and write chunked blobs for encrypted requests.
		return false
	}
	return s.chunkingEnabled(ctx) &&
		rn.GetDigest().GetSizeBytes() > *maxChunkSizeBytes &&
		req.GetReadOffset() == 0 && req.GetReadLimit() == 0
}

type chunkedReadMetrics struct {
	chunksLocal  int
	chunksRemote int
}

func (s *ByteStreamServerProxy) readChunked(ctx context.Context, req *bspb.ReadRequest, stream *meteredReadServerStream, rn *digest.CASResourceName) (chunkedReadMetrics, error) {
	var m chunkedReadMetrics
	if s.localCache == nil || s.remoteCAS == nil {
		return m, status.UnimplementedError("chunked reads not available")
	}

	// Using the interface.Cache requires namespacing the AC keys by group ID.
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.authenticator)
	if err != nil {
		return m, err
	}

	instanceName := rn.GetInstanceName()
	digestFunction := rn.GetDigestFunction()

	splitReq := &repb.SplitBlobRequest{
		BlobDigest:     rn.GetDigest(),
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}
	splitResp, err := s.remoteCAS.SplitBlob(ctx, splitReq)
	if err != nil {
		return m, err
	}
	for _, chunkDigest := range splitResp.GetChunkDigests() {
		chunkRN := digest.NewCASResourceName(chunkDigest, instanceName, digestFunction)
		chunkRN.SetCompressor(rn.GetCompressor())
		if err := s.local.ReadCASResource(ctx, chunkRN, 0, 0, stream); status.IsNotFoundError(err) {
			if err := s.readRemoteWriteLocal(&bspb.ReadRequest{ResourceName: chunkRN.DownloadString()}, stream); err != nil {
				return m, err
			}
			m.chunksRemote++
		} else if err != nil {
			return m, err
		} else {
			m.chunksLocal++
		}
	}
	return m, nil
}

func (s *ByteStreamServerProxy) readRemoteOnly(ctx context.Context, req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}
	return flushToClient(ctx, remoteReadStream, stream)
}

func flushToClient(ctx context.Context, remoteReadStream bspb.ByteStream_ReadClient, stream bspb.ByteStream_ReadServer) error {
	for {
		message, err := remoteReadStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.CtxInfof(ctx, "Error streaming from remote for read: %s", err)
			return err
		}
		if err = stream.Send(message); err != nil {
			return err
		}
	}
}

func (s *ByteStreamServerProxy) readLocalOnly(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	return s.local.Read(req, stream)
}

func writeRequest(resourceName string, data []byte, offset int64, finishWrite bool) *bspb.WriteRequest {
	return &bspb.WriteRequest{
		ResourceName: resourceName,
		Data:         data,
		WriteOffset:  offset,
		FinishWrite:  finishWrite,
	}
}

type readThroughCacheStream struct {
	// This is just here so we can pass this to local.Write
	bspb.ByteStream_WriteServer

	ctx      context.Context
	remote   bspb.ByteStream_ReadClient
	client   bspb.ByteStream_ReadServer
	uploadRN string

	localWriteOffset   int64
	remoteRecvErr      error
	clientSendErr      error
	localWriteFinished bool
}

// Recv turns a ReadResponse from a remote read into a WriteRequest that gets
// returned to a local write.
func (r *readThroughCacheStream) Recv() (*bspb.WriteRequest, error) {
	resp, err := r.remote.Recv()
	if err != nil {
		r.remoteRecvErr = err
		if err == io.EOF {
			return writeRequest(r.uploadRN, nil, r.localWriteOffset, true), nil
		}
		return nil, err
	}

	if err := r.client.Send(resp); err != nil {
		// If the client isn't listening any more, quit here and don't write to
		// local
		r.clientSendErr = err
		return nil, err
	}
	req := writeRequest(r.uploadRN, resp.GetData(), r.localWriteOffset, false)
	r.localWriteOffset += int64(len(resp.GetData()))
	return req, nil
}

func (r *readThroughCacheStream) SendAndClose(resp *bspb.WriteResponse) error {
	// Ignore the local write response
	r.localWriteFinished = true
	return nil
}

func (r *readThroughCacheStream) Context() context.Context { return r.ctx }

func (s *ByteStreamServerProxy) readRemoteWriteLocal(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	// Rewrite the resource name so we can write to the local server
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}

	readThrough := &readThroughCacheStream{
		ctx:      ctx,
		remote:   remoteReadStream,
		client:   stream,
		uploadRN: rn.NewUploadString(),
	}
	localErr := s.local.Write(readThrough)

	if localErr != nil {
		log.CtxDebugf(ctx, "Error writing to local ByteStreamServer: %v", err)
	}
	if readThrough.clientSendErr != nil {
		return readThrough.clientSendErr
	}
	if readThrough.remoteRecvErr == io.EOF {
		return nil
	} else if readThrough.remoteRecvErr != nil {
		log.CtxInfof(ctx, "Error streaming from remote for read through: %s", err)
		return readThrough.remoteRecvErr
	}
	// The local write returned but remoteRecvErr != EOF, which means the read
	// isn't done, so flush the rest.
	if err := flushToClient(ctx, remoteReadStream, stream); err != nil {
		return err
	}
	if localErr == nil && !readThrough.localWriteFinished {
		// Only log this if the local write didn't fail.
		log.CtxInfo(ctx, "remote read done but local write is not")
	}
	return nil
}

type byteStreamMetrics struct {
	requestType       string
	compressor        string
	err               error
	bytes             int64
	chunked           bool
	blobBytes         int64
	chunksTotal       int
	chunksLocal       int
	chunksRemote      int
	chunksDeduped     int
	chunkBytesTotal   int64
	chunkBytesDeduped int64
}

func recordReadMetrics(bsm byteStreamMetrics, cacheStatus string) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           status.MetricsLabel(bsm.err),
		metrics.CacheHitMissStatus:    cacheStatus,
		metrics.CacheProxyRequestType: bsm.requestType,
		metrics.CompressionType:       bsm.compressor,
		metrics.ChunkedLabel:          strconv.FormatBool(bsm.chunked),
	}
	metrics.ByteStreamProxiedReadRequests.With(labels).Inc()
	if bsm.bytes > 0 {
		metrics.ByteStreamProxiedReadBytes.With(labels).Add(float64(bsm.bytes))
	}

	if bsm.chunked && bsm.chunksTotal > 0 {
		chunkedLabels := prometheus.Labels{
			metrics.StatusLabel:     status.MetricsLabel(bsm.err),
			metrics.CompressionType: bsm.compressor,
		}
		metrics.ByteStreamChunkedReadRequests.With(chunkedLabels).Inc()
		if bsm.blobBytes > 0 {
			metrics.ByteStreamChunkedReadBlobBytes.With(chunkedLabels).Add(float64(bsm.blobBytes))
		}
		metrics.ByteStreamChunkedReadChunksTotal.With(chunkedLabels).Add(float64(bsm.chunksTotal))
		if bsm.chunksLocal > 0 {
			metrics.ByteStreamChunkedReadChunksLocal.With(chunkedLabels).Add(float64(bsm.chunksLocal))
		}
		if bsm.chunksRemote > 0 {
			metrics.ByteStreamChunkedReadChunksRemote.With(chunkedLabels).Add(float64(bsm.chunksRemote))
		}
	}
}

// Wrapper around a ByteStream_WriteServer that counts the number of bytes
// written through it.
type meteredServerSideClientStream struct {
	compressor string
	bytes      int64
	bspb.ByteStream_WriteServer
}

func (s *meteredServerSideClientStream) Recv() (*bspb.WriteRequest, error) {
	message, err := s.ByteStream_WriteServer.Recv()
	if err == nil {
		s.detectCompressor(message)
	}
	s.bytes += int64(len(message.GetData()))
	return message, err
}

func (s *meteredServerSideClientStream) detectCompressor(msg *bspb.WriteRequest) {
	if s.compressor != "unknown" {
		return
	}
	if rn, err := digest.ParseUploadResourceName(msg.GetResourceName()); err == nil {
		s.compressor = rn.GetCompressor().String()
	}
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(stream.Context())
	meteredStream := &meteredServerSideClientStream{compressor: "unknown", ByteStream_WriteServer: stream}
	stream = meteredStream

	// TODO(buildbuddy-internal#6426): Read and write chunked blobs for encrypted requests.
	encryptionRequested := authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx)
	if s.chunkingEnabled(ctx) && !encryptionRequested && !proxy_util.SkipRemote(ctx) {
		result, err := s.writeChunked(ctx, stream)
		if err == nil {
			recordWriteMetrics(byteStreamMetrics{
				requestType:       requestTypeLabel,
				compressor:        meteredStream.compressor,
				err:               nil,
				bytes:             meteredStream.bytes,
				chunked:           true,
				blobBytes:         result.blobBytes,
				chunksTotal:       result.chunksTotal,
				chunksDeduped:     result.chunksDeduped,
				chunkBytesTotal:   result.chunkBytesTotal,
				chunkBytesDeduped: result.chunkBytesDeduped,
			})
			return nil
		}

		// If firstReq is nil, more than one request was consumed and we can't fall back.
		if result.firstReq == nil {
			return err
		}

		stream = &replayableWriteStream{
			ByteStream_WriteServer: stream,
			firstReq:               result.firstReq,
		}
	}

	var err error
	if encryptionRequested {
		err = s.writeRemoteOnly(ctx, stream)
	} else if proxy_util.SkipRemote(ctx) {
		err = s.writeLocalOnly(stream)
	} else {
		err = s.dualWrite(ctx, stream)
	}
	bsm := byteStreamMetrics{
		requestType: requestTypeLabel,
		compressor:  meteredStream.compressor,
		err:         err,
		bytes:       meteredStream.bytes,
	}
	recordWriteMetrics(bsm)
	return err
}

func (s *ByteStreamServerProxy) writeRemoteOnly(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}
	if err := flushToRemote(stream, remoteStream); err != nil {
		return err
	}
	resp, err := remoteStream.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

func (s *ByteStreamServerProxy) writeLocalOnly(stream bspb.ByteStream_WriteServer) error {
	return s.local.Write(stream)
}

type forwardingWriteStream struct {
	bspb.ByteStream_WriteServer
	remote          bspb.ByteStream_WriteClient
	recvErr         error
	remoteSendErr   error
	finishWriteSent bool
	localFinished   bool
}

func (s *forwardingWriteStream) Recv() (*bspb.WriteRequest, error) {
	req, err := s.ByteStream_WriteServer.Recv()
	if err != nil {
		s.recvErr = err
		return nil, err
	}
	s.remoteSendErr = s.remote.Send(req)
	if s.remoteSendErr != nil {
		// Tell the local server to abandon the request since the remote request
		// finished or failed. We could instead let local finish when remote
		// finishes early (with EOF). This might take longer, but we would have
		// the blob locally. For now, we rely on the fact that if someone needs
		// this blob, we'll then read it from remote and write it locally.
		return nil, io.EOF
	}
	if req.GetFinishWrite() {
		s.finishWriteSent = true
	}
	return req, nil
}

func (s *forwardingWriteStream) SendAndClose(resp *bspb.WriteResponse) error {
	// Ignore the local response
	s.localFinished = true
	return nil
}

func (s *ByteStreamServerProxy) dualWrite(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}
	forwarding := &forwardingWriteStream{ByteStream_WriteServer: stream, remote: remoteStream}
	localErr := s.local.Write(forwarding)

	if forwarding.recvErr != nil {
		return forwarding.recvErr
	}
	if localErr != nil {
		log.CtxInfof(ctx, "error writing to local bytestream server for write: %s", localErr)
	}
	if forwarding.remoteSendErr != nil && forwarding.remoteSendErr != io.EOF {
		// Remote failed, so fail the whole request
		return forwarding.remoteSendErr
	}
	if !forwarding.finishWriteSent && forwarding.remoteSendErr == nil {
		// Local write returned, but remoteSendErr != EOF so we need to forward
		// the rest.
		if localErr == nil {
			log.CtxInfo(ctx, "local write done but remote write is not")
		}
		if err := flushToRemote(stream, remoteStream); err != nil {
			return err
		}
	}
	if localErr == nil && !forwarding.localFinished {
		// Only log this if the local write didn't fail.
		log.CtxInfo(ctx, "remote write done but local write is not")
	}
	resp, err := remoteStream.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

func flushToRemote(stream bspb.ByteStream_WriteServer, remoteStream bspb.ByteStream_WriteClient) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = remoteStream.Send(req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetFinishWrite() {
			return nil
		}
	}
}

func recordWriteMetrics(bsm byteStreamMetrics) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           status.MetricsLabel(bsm.err),
		metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
		metrics.CacheProxyRequestType: bsm.requestType,
		metrics.CompressionType:       bsm.compressor,
		metrics.ChunkedLabel:          strconv.FormatBool(bsm.chunked),
	}
	metrics.ByteStreamProxiedWriteRequests.With(labels).Inc()
	if bsm.bytes > 0 {
		metrics.ByteStreamProxiedWriteBytes.With(labels).Add(float64(bsm.bytes))
	}
	if bsm.chunked && bsm.blobBytes > 0 {
		chunkedLabels := prometheus.Labels{
			metrics.StatusLabel:     status.MetricsLabel(bsm.err),
			metrics.CompressionType: bsm.compressor,
		}
		metrics.ByteStreamChunkedWriteBlobBytes.With(chunkedLabels).Add(float64(bsm.blobBytes))
		if bsm.chunksTotal > 0 {
			metrics.ByteStreamChunkedWriteChunksTotal.With(chunkedLabels).Add(float64(bsm.chunksTotal))
		}
		if bsm.chunksDeduped > 0 {
			metrics.ByteStreamChunkedWriteChunksDeduped.With(chunkedLabels).Add(float64(bsm.chunksDeduped))
		}
		if bsm.chunkBytesTotal > 0 {
			metrics.ByteStreamChunkedWriteChunkBytes.With(chunkedLabels).Add(float64(bsm.chunkBytesTotal))
		}
		if bsm.chunkBytesDeduped > 0 {
			metrics.ByteStreamChunkedWriteDedupedChunkBytes.With(chunkedLabels).Add(float64(bsm.chunkBytesDeduped))
		}
	}
}

// replayableWriteStream is a wrapper around a ByteStream_WriteServer that
// replays the first request if it is the only one that has been received.
// This is required to inspect the resource name and size of the first request.
// It is not thread-safe.
type replayableWriteStream struct {
	bspb.ByteStream_WriteServer
	firstReq      *bspb.WriteRequest
	replayedFirst bool
}

func (s *replayableWriteStream) Recv() (*bspb.WriteRequest, error) {
	if !s.replayedFirst {
		s.replayedFirst = true
		return s.firstReq, nil
	}
	return s.ByteStream_WriteServer.Recv()
}

func (s *ByteStreamServerProxy) chunkingEnabled(ctx context.Context) bool {
	return s.localCache != nil && s.remoteCAS != nil && s.efp != nil &&
		s.efp.Boolean(ctx, "cache.chunking_enabled", false) &&
		s.efp.Boolean(ctx, "cache.split_splice_enabled", false)
}

type writeChunkedResult struct {
	firstReq          *bspb.WriteRequest
	blobBytes         int64
	chunksTotal       int
	chunksDeduped     int
	chunkBytesTotal   int64
	chunkBytesDeduped int64
}

func (s *ByteStreamServerProxy) writeChunked(ctx context.Context, stream bspb.ByteStream_WriteServer) (writeChunkedResult, error) {
	firstReq, err := stream.Recv()
	if err != nil {
		return writeChunkedResult{}, status.WrapErrorf(err, "receive first request")
	}

	rn, err := digest.ParseUploadResourceName(firstReq.GetResourceName())
	if err != nil {
		return writeChunkedResult{}, status.InvalidArgumentErrorf("parse resource name: %s", err)
	}

	blobSize := rn.GetDigest().GetSizeBytes()
	if blobSize <= *maxChunkSizeBytes {
		return writeChunkedResult{firstReq: firstReq}, status.UnimplementedError("blob too small for chunking")
	}

	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.authenticator)
	if err != nil {
		return writeChunkedResult{}, err
	}

	var chunkDigests []*repb.Digest

	digestFunction := rn.GetDigestFunction()
	instanceName := rn.GetInstanceName()
	compressor := rn.GetCompressor()

	// Get a buffer from the pool for compression. We reuse this buffer across
	// all chunk compressions to avoid allocating for each chunk.
	compressBuf := s.compressBufPool.Get(*maxChunkSizeBytes)
	defer s.compressBufPool.Put(compressBuf)

	// chunkWriteFn is called on each new chunk once it's available through the chunker's pipe.
	// We write chunks to local first, then use FindMissingBlobs + upload for remote.
	// Chunks are stored and read compressed with ZSTD.
	chunkWriteFn := func(chunkData []byte) error {
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), digestFunction)
		if err != nil {
			return status.InternalErrorf("computing chunked digest for Write: %s", err)
		}
		chunkDigests = append(chunkDigests, chunkDigest)

		chunkRN := digest.NewCASResourceName(chunkDigest, instanceName, digestFunction)
		chunkRN.SetCompressor(repb.Compressor_ZSTD)
		compressedData := compression.CompressZstd(compressBuf, chunkData)
		stream := &rawWriteStream{
			ctx:          ctx,
			resourceName: chunkRN.NewUploadString(),
			data:         compressedData,
		}
		if err := s.local.Write(stream); err != nil {
			return status.InternalErrorf("writing chunk %s to local: %s", chunkRN.DownloadString(), err)
		}
		return nil
	}

	chunker, err := chunker.New(ctx, int(*maxChunkSizeBytes/4), chunkWriteFn)
	if err != nil {
		return writeChunkedResult{}, status.InternalErrorf("creating chunker: %s", err)
	}
	defer chunker.Close()

	var chunkInput io.Writer = chunker
	var decompressor io.WriteCloser
	if compressor == repb.Compressor_ZSTD {
		decompressor, err = compression.NewZstdDecompressor(chunker)
		if err != nil {
			return writeChunkedResult{}, status.InternalErrorf("creating decompressor: %s", err)
		}
		defer decompressor.Close()
		chunkInput = decompressor
	}

	req := firstReq
	bytesReceived := int64(0)
	for {
		if _, err := chunkInput.Write(req.GetData()); err != nil {
			return writeChunkedResult{}, status.InternalErrorf("writing data to chunker: %s", err)
		}
		bytesReceived += int64(len(req.GetData()))
		if req.GetFinishWrite() {
			break
		}

		req, err = stream.Recv()
		if err == io.EOF {
			return writeChunkedResult{}, status.InvalidArgumentErrorf("received EOF before FinishWrite; stream cannot be recovered")
		}
		if err != nil {
			return writeChunkedResult{}, status.InternalErrorf("receiving request after partially consuming stream; stream cannot be recovered: %s", err)
		}
	}

	if decompressor != nil {
		if err := decompressor.Close(); err != nil {
			return writeChunkedResult{}, status.InternalErrorf("closing decompressor: %s", err)
		}
	}

	// Close blocks until all chunk writes complete, ensuring chunkDigests is fully populated.
	if err := chunker.Close(); err != nil {
		return writeChunkedResult{}, status.InternalErrorf("closing chunker: %s", err)
	}

	var chunkBytesTotal int64
	for _, d := range chunkDigests {
		chunkBytesTotal += d.GetSizeBytes()
	}

	result := writeChunkedResult{
		blobBytes:       blobSize,
		chunksTotal:     len(chunkDigests),
		chunkBytesTotal: chunkBytesTotal,
	}

	// If there's only 1 chunk, the chunking threshold is misconfigured.
	// It should be set higher than the max chunk size to avoid overlap.
	if len(chunkDigests) == 1 {
		return result, status.InternalErrorf("chunking produced only 1 chunk; only chunked blobs larger than max_chunk_size_bytes are supported")
	}

	manifest := &chunked_manifest.ChunkedManifest{
		BlobDigest:     rn.GetDigest(),
		ChunkDigests:   chunkDigests,
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}

	if err := manifest.Store(ctx, s.localCache); err != nil {
		return writeChunkedResult{}, status.InternalErrorf("storing manifest locally: %s", err)
	}

	missingBlobs, err := s.remoteCAS.FindMissingBlobs(ctx, manifest.ToFindMissingBlobsRequest())
	if err != nil {
		return writeChunkedResult{}, status.InternalErrorf("finding missing blobs on remote: %s", err)
	}
	missingDigests := missingBlobs.GetMissingBlobDigests()
	missingSet := make(set.Set[string], len(missingDigests))
	for _, d := range missingDigests {
		missingSet.Add(d.GetHash())
	}

	// Deduped chunks are ones that already exist on remote.
	for _, d := range chunkDigests {
		if !missingSet.Contains(d.GetHash()) {
			result.chunksDeduped++
			result.chunkBytesDeduped += d.GetSizeBytes()
		}
	}
	if len(missingDigests) > 0 {
		if err := s.uploadMissingChunks(ctx, missingDigests, instanceName, digestFunction); err != nil {
			return writeChunkedResult{}, status.InternalErrorf("uploading missing chunks to remote: %s", err)
		}
	}

	if _, err := s.remoteCAS.SpliceBlob(ctx, manifest.ToSpliceBlobRequest()); err != nil {
		return writeChunkedResult{}, status.InternalErrorf("splice blob on remote: %s", err)
	}

	return result, stream.SendAndClose(&bspb.WriteResponse{CommittedSize: bytesReceived})
}

func (s *ByteStreamServerProxy) uploadMissingChunks(ctx context.Context, missingDigests []*repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(*chunkUploadConcurrency)
	for _, chunkDigest := range missingDigests {
		chunkRN := digest.NewCASResourceName(chunkDigest, instanceName, digestFunction)
		g.Go(func() error {
			return s.uploadChunk(gCtx, chunkRN)
		})
	}
	return g.Wait()
}

func (s *ByteStreamServerProxy) uploadChunk(ctx context.Context, rn *digest.CASResourceName) error {
	rnCopy, err := digest.CASResourceNameFromProto(rn.ToProto())
	if err != nil {
		return err
	}
	rnCopy.SetCompressor(repb.Compressor_ZSTD)
	reader, err := s.localCache.Reader(ctx, rnCopy.ToProto(), 0, 0)
	if err != nil {
		return status.InternalErrorf("reading chunk %s from local cache: %s", rn.DownloadString(), err)
	}
	defer reader.Close()

	_, _, err = cachetools.UploadFromReaderWithCompression(ctx, s.remote, rnCopy, reader, repb.Compressor_ZSTD)
	if err != nil {
		return status.InternalErrorf("uploading chunk %s to remote: %s", rn.DownloadString(), err)
	}
	return nil
}

// rawWriteStream implements ByteStream_WriteServer to provide raw data from
// memory for a Write call. It is not thread-safe.
type rawWriteStream struct {
	bspb.ByteStream_WriteServer

	ctx          context.Context
	resourceName string
	data         []byte
	dataSent     bool
}

func (s *rawWriteStream) Recv() (*bspb.WriteRequest, error) {
	if s.dataSent {
		return nil, io.EOF
	}
	s.dataSent = true
	return &bspb.WriteRequest{
		ResourceName: s.resourceName,
		Data:         s.data,
		WriteOffset:  0,
		FinishWrite:  true,
	}, nil
}

func (s *rawWriteStream) SendAndClose(resp *bspb.WriteResponse) error {
	return nil // No client to send response to
}

func (s *rawWriteStream) Context() context.Context { return s.ctx }

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedError("Skip remote not implemented")
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return s.remote.QueryWriteStatus(ctx, req)
}
