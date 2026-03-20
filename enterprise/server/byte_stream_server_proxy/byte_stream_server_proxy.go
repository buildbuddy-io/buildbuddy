package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/cdc"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	chunkUploadConcurrency = flag.Int("cache_proxy.chunk_upload_concurrency", 8, "Maximum number of concurrent chunk uploads when uploading missing chunks to remote cache.")
)

func groupIDForMetrics(ctx context.Context) string {
	c, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return c.GroupID
}

type ByteStreamServerProxy struct {
	supportsEncryption func(context.Context) bool
	authenticator      interfaces.Authenticator
	local              interfaces.ByteStreamServer
	remote             bspb.ByteStreamClient
	efp                interfaces.ExperimentFlagProvider
	localCache         interfaces.Cache
	remoteCAS          repb.ContentAddressableStorageClient
	bufPool            *bytebufferpool.VariableSizePool
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
		bufPool:            bytebufferpool.VariableSize(int(chunking.MaxChunkSizeBytes())),
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

// bufferedReadStream collects ReadResponse messages in memory instead of
// sending them to the client. This allows the caller to discard the buffer
// on failure without having sent partial data.
type bufferedReadStream struct {
	bspb.ByteStream_ReadServer
	pool     *bytebufferpool.VariableSizePool
	messages []*bspb.ReadResponse
	bufs     [][]byte
}

func (b *bufferedReadStream) Send(message *bspb.ReadResponse) error {
	data := message.GetData()
	buf := b.pool.Get(int64(len(data)))
	copy(buf, data)
	b.messages = append(b.messages, &bspb.ReadResponse{Data: buf[:len(data)]})
	b.bufs = append(b.bufs, buf)
	return nil
}

func (b *bufferedReadStream) flushTo(stream bspb.ByteStream_ReadServer) error {
	for _, msg := range b.messages {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func (b *bufferedReadStream) release() {
	for _, buf := range b.bufs {
		b.pool.Put(buf)
	}
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
		var chunkedErr error
		if s.shouldReadChunked(ctx, req, rn) {
			chunkMetrics, err := s.readChunked(ctx, req, stream, rn)
			chunkedErr = err
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
			}

			if stream.frames > 0 {
				return readMetrics{
					cacheStatus: metrics.MissStatusLabel,
					compressor:  rn.GetCompressor().String(),
				}, chunkedErr
			}
		}

		remoteErr := s.readRemoteWriteLocal(req, stream)
		if remoteErr != nil && chunkedErr != nil {
			// TODO(buildbuddy-internal#6426): Remove once root cause for missing digests is found.
			log.CtxWarningf(ctx, "Proxy chunked read failed for %s (remote fallback also failed): chunkedErr=%s remoteErr=%s", rn.DownloadString(), chunkedErr, remoteErr)
		}
		return readMetrics{
			cacheStatus: metrics.MissStatusLabel,
			compressor:  rn.GetCompressor().String(),
		}, remoteErr
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
	return s.localCache != nil && s.remoteCAS != nil &&
		chunking.ShouldReadChunkedOnProxy(ctx, s.efp, rn.GetDigest().GetSizeBytes(), req.GetReadOffset(), req.GetReadLimit())
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
		metrics.ByteStreamProxyChunkedReadFailures.With(prometheus.Labels{
			metrics.ChunkedFailureReasonLabel: "auth_error",
			metrics.StatusHumanReadableLabel:  status.MetricsLabel(err),
		}).Inc()
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
		metrics.ByteStreamProxyChunkedReadFailures.With(prometheus.Labels{
			metrics.ChunkedFailureReasonLabel: "split_blob_error",
			metrics.StatusHumanReadableLabel:  status.MetricsLabel(err),
		}).Inc()
		return m, err
	}

	offset := req.GetReadOffset()
	for _, chunkDigest := range splitResp.GetChunkDigests() {
		chunkSize := chunkDigest.GetSizeBytes()
		if offset >= chunkSize {
			offset -= chunkSize
			continue
		}

		chunkRN := digest.NewCASResourceName(chunkDigest, instanceName, digestFunction)
		chunkRN.SetCompressor(rn.GetCompressor())
		intraChunkOffset := offset
		offset = 0

		// If we're midway through a chunk (non-zero offset),
		// read remote only, since we can't cache a partial chunk locally.
		if intraChunkOffset > 0 {
			remoteReq := &bspb.ReadRequest{
				ResourceName: chunkRN.DownloadString(),
				ReadOffset:   intraChunkOffset,
			}
			if err := s.readRemoteOnly(ctx, remoteReq, stream); err != nil {
				metrics.ByteStreamProxyChunkedReadFailures.With(prometheus.Labels{
					metrics.ChunkedFailureReasonLabel: "chunk_remote_fetch_error_partial",
					metrics.StatusHumanReadableLabel:  status.MetricsLabel(err),
				}).Inc()
				return m, err
			}
			m.chunksRemote++
			continue
		}

		// Buffer the local read so that if it fails, no partial data
		// has been sent to the client and we can cleanly fall back to remote.
		buf := &bufferedReadStream{ByteStream_ReadServer: stream, pool: s.bufPool}
		localErr := s.local.ReadCASResource(ctx, chunkRN, 0, 0, buf)
		if localErr == nil {
			err := buf.flushTo(stream)
			buf.release()
			if err != nil {
				return m, err
			}
			m.chunksLocal++
			continue
		}
		buf.release()

		metrics.ByteStreamProxyChunkedReadFailures.With(prometheus.Labels{
			metrics.ChunkedFailureReasonLabel: "chunk_local_read_error",
			metrics.StatusHumanReadableLabel:  status.MetricsLabel(localErr),
		}).Inc()
		if err := s.readRemoteWriteLocal(&bspb.ReadRequest{ResourceName: chunkRN.DownloadString()}, stream); err != nil {
			metrics.ByteStreamProxyChunkedReadFailures.With(prometheus.Labels{
				metrics.ChunkedFailureReasonLabel: "chunk_remote_fetch_error",
				metrics.StatusHumanReadableLabel:  status.MetricsLabel(err),
			}).Inc()
			return m, err
		}
		m.chunksRemote++
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
		log.CtxInfof(ctx, "Error streaming from remote for read through: %v", readThrough.remoteRecvErr)
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
	groupID           string
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
	chunkingDuration  time.Duration
	remoteDuration    time.Duration
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
	if s.writeChunkingEnabled(ctx) && !encryptionRequested && !proxy_util.SkipRemote(ctx) {
		result, err := s.writeChunked(ctx, stream)
		if err == nil {
			recordWriteMetrics(byteStreamMetrics{
				requestType:       requestTypeLabel,
				compressor:        meteredStream.compressor,
				groupID:           groupIDForMetrics(ctx),
				err:               nil,
				bytes:             meteredStream.bytes,
				chunked:           true,
				blobBytes:         result.blobBytes,
				chunksTotal:       result.chunksTotal,
				chunksDeduped:     result.chunksDeduped,
				chunkBytesTotal:   result.chunkBytesTotal,
				chunkBytesDeduped: result.chunkBytesDeduped,
				chunkingDuration:  result.chunkingDuration,
				remoteDuration:    result.remoteDuration,
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
		chunkedLabelsWithGroup := prometheus.Labels{
			metrics.StatusLabel:     status.MetricsLabel(bsm.err),
			metrics.CompressionType: bsm.compressor,
			metrics.GroupID:         bsm.groupID,
		}
		metrics.ByteStreamChunkedWriteBlobBytes.With(chunkedLabels).Add(float64(bsm.blobBytes))
		if bsm.chunksTotal > 0 {
			metrics.ByteStreamChunkedWriteChunksTotal.With(chunkedLabelsWithGroup).Add(float64(bsm.chunksTotal))
		}
		if bsm.chunksDeduped > 0 {
			metrics.ByteStreamChunkedWriteChunksDeduped.With(chunkedLabelsWithGroup).Add(float64(bsm.chunksDeduped))
		}
		if bsm.chunkBytesTotal > 0 {
			metrics.ByteStreamChunkedWriteChunkBytes.With(chunkedLabels).Add(float64(bsm.chunkBytesTotal))
		}
		if bsm.chunkBytesDeduped > 0 {
			metrics.ByteStreamChunkedWriteDedupedChunkBytes.With(chunkedLabels).Add(float64(bsm.chunkBytesDeduped))
		}
		totalDuration := bsm.chunkingDuration + bsm.remoteDuration
		metrics.ByteStreamChunkedWriteDurationUsec.With(chunkedLabels).Observe(float64(totalDuration.Microseconds()))
		metrics.ByteStreamChunkedWriteChunkingDurationUsec.With(chunkedLabels).Observe(float64(bsm.chunkingDuration.Microseconds()))
		metrics.ByteStreamChunkedWriteRemoteDurationUsec.With(chunkedLabels).Observe(float64(bsm.remoteDuration.Microseconds()))
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

func (s *ByteStreamServerProxy) writeChunkingEnabled(ctx context.Context) bool {
	if s.localCache == nil || s.remoteCAS == nil {
		return false
	}
	if cdc.EnabledViaHeader(ctx) {
		return true
	}
	return chunking.Enabled(ctx, s.efp) &&
		s.efp.Boolean(ctx, "cache_proxy.intercept_and_chunk_large_writes", false)
}

type writeChunkedResult struct {
	firstReq          *bspb.WriteRequest
	blobBytes         int64
	chunksTotal       int
	chunksDeduped     int
	chunkBytesTotal   int64
	chunkBytesDeduped int64
	chunkingDuration  time.Duration
	remoteDuration    time.Duration
}

func (s *ByteStreamServerProxy) writeChunked(ctx context.Context, stream bspb.ByteStream_WriteServer) (writeChunkedResult, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	firstReq, err := stream.Recv()
	if err != nil {
		return writeChunkedResult{}, status.WrapErrorf(err, "receive first request")
	}

	rn, err := digest.ParseUploadResourceName(firstReq.GetResourceName())
	if err != nil {
		return writeChunkedResult{}, status.InvalidArgumentErrorf("parse resource name: %s", err)
	}

	blobSize := rn.GetDigest().GetSizeBytes()
	if blobSize <= chunking.MaxChunkSizeBytes() {
		return writeChunkedResult{firstReq: firstReq}, status.UnimplementedError("blob too small for chunking")
	}

	if spn.IsRecording() {
		spn.SetAttributes(
			attribute.Int64("blob_size", blobSize),
			attribute.String("compressor", rn.GetCompressor().String()),
		)
	}

	ctx, err = prefix.AttachUserPrefixToContext(ctx, s.authenticator)
	if err != nil {
		return writeChunkedResult{}, err
	}

	var chunkDigests []*repb.Digest

	digestFunction := rn.GetDigestFunction()
	instanceName := rn.GetInstanceName()
	compressor := rn.GetCompressor()
	useBatchParallelUploads := s.efp != nil && s.efp.Boolean(ctx, "cache_proxy.enable_batch_parallel_uploads", false)
	if spn.IsRecording() {
		spn.SetAttributes(attribute.Bool("batch_parallel_uploads", useBatchParallelUploads))
	}

	var uploader *chunkUploader
	if useBatchParallelUploads {
		uploader, err = newChunkUploader(ctx, s, instanceName, digestFunction)
		if err != nil {
			return writeChunkedResult{}, err
		}
	}

	// chunkWriteFn is called on each new chunk once it's available through the chunker's pipe.
	// Chunks are stored and read compressed with ZSTD.
	chunkWriteFn := func(chunkData []byte) error {
		chunkCtx, chunkSpn := tracing.StartNamedSpan(ctx, "chunkWriteFn")
		defer chunkSpn.End()

		chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), digestFunction)
		if err != nil {
			return status.InternalErrorf("computing chunked digest for Write: %s", err)
		}
		chunkDigests = append(chunkDigests, chunkDigest)

		chunkRN := digest.NewCASResourceName(chunkDigest, instanceName, digestFunction)
		chunkRN.SetCompressor(repb.Compressor_ZSTD)

		poolBuf := s.bufPool.Get(int64(len(chunkData)))
		_, compressSpn := tracing.StartNamedSpan(chunkCtx, "CompressZstd")
		compressedData := compression.CompressZstd(poolBuf, chunkData)
		compressSpn.End()

		if chunkSpn.IsRecording() {
			chunkSpn.SetAttributes(
				attribute.Int("chunk_size", len(chunkData)),
				attribute.Int("compressed_size", len(compressedData)),
			)
		}

		localWriteCtx, localWriteSpn := tracing.StartNamedSpan(chunkCtx, "localChunkWrite")
		defer localWriteSpn.End()
		rawStream := &rawWriteStream{
			ctx:          localWriteCtx,
			resourceName: chunkRN.NewUploadString(),
			data:         compressedData,
		}
		if err := s.local.Write(rawStream); err != nil {
			s.bufPool.Put(poolBuf)
			return status.InternalErrorf("writing chunk %s to local: %s", chunkRN.DownloadString(), err)
		}
		if uploader != nil {
			uploader.addChunk(compressedData, poolBuf, chunkDigest)
		} else {
			s.bufPool.Put(poolBuf)
		}
		return nil
	}

	chunker, err := chunking.NewChunker(ctx, int(chunking.AvgChunkSizeBytes()), chunkWriteFn)
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

	chunkingStart := time.Now()
	req := firstReq
	bytesReceived := int64(0)
	for {
		// Backpressure: chunkInputWrite / streamRecv (compression/local-write is bottleneck).
		// Forward pressure: streamRecv / chunkInputWrite (client send is bottleneck).
		_, pipeWriteSpn := tracing.StartNamedSpan(ctx, "chunkInputWrite")
		_, err := chunkInput.Write(req.GetData())
		pipeWriteSpn.End()
		if err != nil {
			return writeChunkedResult{}, status.InternalErrorf("writing data to chunker: %s", err)
		}
		bytesReceived += int64(len(req.GetData()))
		if req.GetFinishWrite() {
			break
		}

		_, recvSpn := tracing.StartNamedSpan(ctx, "streamRecv")
		req, err = stream.Recv()
		recvSpn.End()
		if err == io.EOF {
			return writeChunkedResult{}, status.InvalidArgumentErrorf("received EOF before FinishWrite; stream cannot be recovered")
		}
		if err != nil {
			return writeChunkedResult{}, status.InternalErrorf("receiving request after partially consuming stream; stream cannot be recovered: %s", err)
		}
	}

	if decompressor != nil {
		_, closeSpn := tracing.StartNamedSpan(ctx, "closeDecompressor")
		err := decompressor.Close()
		closeSpn.End()
		if err != nil {
			return writeChunkedResult{}, status.InternalErrorf("closing decompressor: %s", err)
		}
	}

	// Close blocks until all chunk writes complete, ensuring chunkDigests is fully populated.
	_, closeChunkerSpn := tracing.StartNamedSpan(ctx, "closeChunker")
	err = chunker.Close()
	closeChunkerSpn.End()
	if err != nil {
		return writeChunkedResult{}, status.InternalErrorf("closing chunker: %s", err)
	}
	chunkingDuration := time.Since(chunkingStart)

	var chunkBytesTotal int64
	for _, d := range chunkDigests {
		chunkBytesTotal += d.GetSizeBytes()
	}

	result := writeChunkedResult{
		blobBytes:        blobSize,
		chunksTotal:      len(chunkDigests),
		chunkBytesTotal:  chunkBytesTotal,
		chunkingDuration: chunkingDuration,
	}

	if spn.IsRecording() {
		spn.SetAttributes(
			attribute.Int("chunks_total", result.chunksTotal),
			attribute.Int64("chunk_bytes_total", result.chunkBytesTotal),
		)
	}

	// If there's only 1 chunk, the chunking threshold is misconfigured.
	// It should be set higher than the max chunk size to avoid overlap.
	if len(chunkDigests) == 1 {
		return result, status.InternalErrorf("chunking produced only 1 chunk; only chunked blobs larger than 4x avg_chunk_size_bytes are supported")
	}

	manifest := &chunking.Manifest{
		BlobDigest:     rn.GetDigest(),
		ChunkDigests:   chunkDigests,
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}

	remoteStart := time.Now()

	if uploader != nil {
		_, flushSpn := tracing.StartNamedSpan(ctx, "flushChunkUploads")
		if err := uploader.flush(); err != nil {
			flushSpn.End()
			return writeChunkedResult{}, status.WrapErrorf(err, "uploading missing chunks to remote")
		}
		flushSpn.End()
		result.chunksDeduped = int(uploader.dedupedChunks.Load())
		result.chunkBytesDeduped = uploader.dedupedChunkBytes.Load()
	} else {
		fmbCtx, fmbSpn := tracing.StartNamedSpan(ctx, "remote.FindMissingBlobs")
		missingBlobs, err := s.remoteCAS.FindMissingBlobs(fmbCtx, manifest.ToFindMissingBlobsRequest())
		fmbSpn.End()
		if err != nil {
			return writeChunkedResult{}, status.WrapErrorf(err, "finding missing blobs on remote")
		}
		missingDigests := missingBlobs.GetMissingBlobDigests()
		missingSet := make(map[string]struct{}, len(missingDigests))
		for _, d := range missingDigests {
			missingSet[d.GetHash()] = struct{}{}
		}

		// Deduped chunks are ones that already exist on remote.
		for _, d := range chunkDigests {
			if _, ok := missingSet[d.GetHash()]; !ok {
				result.chunksDeduped++
				result.chunkBytesDeduped += d.GetSizeBytes()
			}
		}
		if len(missingDigests) > 0 {
			if err := s.uploadMissingChunks(ctx, missingDigests, instanceName, digestFunction); err != nil {
				return writeChunkedResult{}, status.WrapErrorf(err, "uploading missing chunks to remote")
			}
		}
	}

	spliceCtx, spliceSpn := tracing.StartNamedSpan(ctx, "remote.SpliceBlob")
	_, err = s.remoteCAS.SpliceBlob(spliceCtx, manifest.ToSpliceBlobRequest())
	spliceSpn.End()
	if err != nil {
		return writeChunkedResult{}, status.WrapErrorf(err, "splice blob on remote")
	}

	result.remoteDuration = time.Since(remoteStart)
	return result, stream.SendAndClose(&bspb.WriteResponse{CommittedSize: bytesReceived})
}

func (s *ByteStreamServerProxy) uploadMissingChunks(ctx context.Context, missingDigests []*repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) error {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if spn.IsRecording() {
		spn.SetAttributes(attribute.Int("missing_chunks", len(missingDigests)))
	}
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
		return status.WrapErrorf(err, "uploading chunk %s to remote", rn.DownloadString())
	}
	return nil
}

type pendingChunk struct {
	digest         *repb.Digest
	compressedData []byte
	poolBuf        []byte
}

type chunkUploader struct {
	s              *ByteStreamServerProxy
	instanceName   string
	digestFunction repb.DigestFunction_Value

	fmbG     *errgroup.Group
	fmbCtx   context.Context
	batchG   *errgroup.Group
	batchCtx context.Context

	pendingFMB []pendingChunk

	mu                     sync.Mutex
	seen                   map[digest.Key]int
	pendingBatchUpload     []pendingChunk
	pendingBatchUploadSize int64
	dedupedChunks          atomic.Int64
	dedupedChunkBytes      atomic.Int64
}

// newChunkUploader batches chunks into FMB groups of up to
// chunkUploadConcurrency digests and upload requests of up to 2 MiB.
// With one FMB group in flight and upload concurrency 8, it uses roughly
// 50 MiB of compressed chunk data, plus overhead.
func newChunkUploader(ctx context.Context, s *ByteStreamServerProxy, instanceName string, digestFunction repb.DigestFunction_Value) (*chunkUploader, error) {
	if *chunkUploadConcurrency <= 0 {
		return nil, status.FailedPreconditionErrorf("cache_proxy.chunk_upload_concurrency must be > 0")
	}
	fmbG, fmbCtx := errgroup.WithContext(ctx)
	fmbG.SetLimit(1)
	batchG, batchCtx := errgroup.WithContext(ctx)
	batchG.SetLimit(*chunkUploadConcurrency)
	return &chunkUploader{
		s:              s,
		instanceName:   instanceName,
		digestFunction: digestFunction,
		fmbG:           fmbG,
		fmbCtx:         fmbCtx,
		batchG:         batchG,
		batchCtx:       batchCtx,
		seen:           make(map[digest.Key]int),
	}, nil
}

// addChunk transfers ownership of poolBuf to the uploader. The uploader returns
// it to the pool once the chunk is deduped or its upload completes.
func (c *chunkUploader) addChunk(compressedData []byte, poolBuf []byte, d *repb.Digest) {
	chunk := pendingChunk{
		digest:         d,
		compressedData: compressedData,
		poolBuf:        poolBuf,
	}
	dk := digest.NewKey(d)

	c.mu.Lock()
	c.seen[dk]++
	seenCount := c.seen[dk]
	c.mu.Unlock()
	if seenCount > 1 {
		c.s.bufPool.Put(chunk.poolBuf)
		return
	}

	c.pendingFMB = append(c.pendingFMB, chunk)
	if len(c.pendingFMB) >= *chunkUploadConcurrency {
		c.flushPendingFMB()
	}
}

func (c *chunkUploader) flush() error {
	c.flushPendingFMB()
	fmbErr := c.fmbG.Wait()
	if fmbErr == nil {
		c.flushPendingBatchUpload()
	} else {
		c.mu.Lock()
		for _, chunk := range c.pendingBatchUpload {
			c.s.bufPool.Put(chunk.poolBuf)
		}
		c.pendingBatchUpload = nil
		c.pendingBatchUploadSize = 0
		c.mu.Unlock()
	}
	batchErr := c.batchG.Wait()
	if fmbErr != nil {
		return fmbErr
	}
	return batchErr
}

func (c *chunkUploader) flushPendingFMB() {
	if len(c.pendingFMB) == 0 {
		return
	}
	group := c.pendingFMB
	c.pendingFMB = nil
	c.fmbG.Go(func() error {
		return c.processFMBGroup(group)
	})
}

func (c *chunkUploader) processFMBGroup(group []pendingChunk) error {
	_, fmbSpn := tracing.StartNamedSpan(c.fmbCtx, "remote.FindMissingBlobs")
	digests := make([]*repb.Digest, 0, len(group))
	for _, chunk := range group {
		digests = append(digests, chunk.digest)
	}
	fmbResp, err := c.s.remoteCAS.FindMissingBlobs(c.fmbCtx, &repb.FindMissingBlobsRequest{
		InstanceName:   c.instanceName,
		BlobDigests:    digests,
		DigestFunction: c.digestFunction,
	})
	fmbSpn.End()
	if err != nil {
		for _, chunk := range group {
			c.s.bufPool.Put(chunk.poolBuf)
		}
		return err
	}

	missingSet := make(set.Set[string], len(fmbResp.GetMissingBlobDigests()))
	for _, d := range fmbResp.GetMissingBlobDigests() {
		missingSet.Add(d.GetHash())
	}

	for _, chunk := range group {
		if missingSet.Contains(chunk.digest.GetHash()) {
			c.queueUploadChunk(chunk)
			continue
		}
		c.recordDedupedChunk(chunk.digest)
		c.s.bufPool.Put(chunk.poolBuf)
	}
	return nil
}

func (c *chunkUploader) queueUploadChunk(chunk pendingChunk) {
	chunkSize := int64(len(chunk.compressedData))
	var batch []pendingChunk
	c.mu.Lock()
	if c.pendingBatchUploadSize+chunkSize > cachetools.BatchUploadLimitBytes && len(c.pendingBatchUpload) > 0 {
		batch = c.pendingBatchUpload
		c.pendingBatchUpload = nil
		c.pendingBatchUploadSize = 0
	}
	c.pendingBatchUpload = append(c.pendingBatchUpload, chunk)
	c.pendingBatchUploadSize += chunkSize
	c.mu.Unlock()

	if len(batch) > 0 {
		c.batchG.Go(func() error {
			return c.uploadBatch(batch)
		})
	}
}

func (c *chunkUploader) flushPendingBatchUpload() {
	c.mu.Lock()
	batch := c.pendingBatchUpload
	c.pendingBatchUpload = nil
	c.pendingBatchUploadSize = 0
	c.mu.Unlock()
	if len(batch) > 0 {
		c.batchG.Go(func() error {
			return c.uploadBatch(batch)
		})
	}
}

func (c *chunkUploader) uploadBatch(batch []pendingChunk) error {
	defer func() {
		for _, chunk := range batch {
			c.s.bufPool.Put(chunk.poolBuf)
		}
	}()

	req := &repb.BatchUpdateBlobsRequest{
		InstanceName:   c.instanceName,
		DigestFunction: c.digestFunction,
	}
	uploadSizeBytes := int64(0)
	for _, chunk := range batch {
		req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     chunk.digest,
			Data:       chunk.compressedData,
			Compressor: repb.Compressor_ZSTD,
		})
		uploadSizeBytes += int64(len(chunk.compressedData))
	}
	err := c.s.sendBatchUpload(c.batchCtx, req)
	metrics.ByteStreamChunkedWriteUploadSizeBytes.With(prometheus.Labels{
		metrics.StatusLabel: status.MetricsLabel(err),
	}).Observe(float64(uploadSizeBytes))
	return err
}

func (c *chunkUploader) recordDedupedChunk(d *repb.Digest) {
	dk := digest.NewKey(d)
	c.mu.Lock()
	count := c.seen[dk]
	c.mu.Unlock()
	if count == 0 {
		count = 1
	}
	c.dedupedChunks.Add(int64(count))
	c.dedupedChunkBytes.Add(int64(count) * d.GetSizeBytes())
}

func (s *ByteStreamServerProxy) sendBatchUpload(ctx context.Context, req *repb.BatchUpdateBlobsRequest) error {
	_, uploadSpn := tracing.StartNamedSpan(ctx, "remote.BatchUpdateBlobs")
	defer uploadSpn.End()

	rsp, err := s.remoteCAS.BatchUpdateBlobs(ctx, req)
	if err != nil {
		return status.WrapErrorf(err, "batch uploading chunks to remote")
	}
	for _, r := range rsp.GetResponses() {
		if c := r.GetStatus().GetCode(); c != 0 {
			return gstatus.Errorf(codes.Code(c), "batch upload chunk %s: %s", r.GetDigest().GetHash(), r.GetStatus().GetMessage())
		}
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
