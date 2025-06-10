package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

type ByteStreamServerProxy struct {
	atimeUpdater  interfaces.AtimeUpdater
	authenticator interfaces.Authenticator
	local         bspb.ByteStreamServer
	remote        bspb.ByteStreamClient
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
	atimeUpdater := env.GetAtimeUpdater()
	if atimeUpdater == nil {
		return nil, fmt.Errorf("An AtimeUpdater is required to enable ByteStreamServerProxy")
	}
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
		atimeUpdater:  atimeUpdater,
		authenticator: authenticator,
		local:         local,
		remote:        remote,
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
	cacheStatus, err := s.read(ctx, req, meteredStream)
	recordReadMetrics(cacheStatus, requestTypeLabel, err, int(meteredStream.bytes))
	return err
}

func (s *ByteStreamServerProxy) read(ctx context.Context, req *bspb.ReadRequest, stream *meteredReadServerStream) (string, error) {
	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		return metrics.UncacheableStatusLabel, s.readRemoteOnly(ctx, req, stream)
	}
	if proxy_util.SkipRemote(ctx) {
		if err := s.readLocalOnly(req, stream); err != nil {
			log.CtxInfof(ctx, "Error reading local: %v", err)
			return metrics.MissStatusLabel, err
		}
		return metrics.HitStatusLabel, nil
	}

	localErr := s.local.Read(req, stream)
	// If some responses were streamed to the client, just return the
	// error. Otherwise, fall-back to remote. We might be able to continue
	// streaming to the client by doing an offset read from the remote
	// cache, but keep it simple for now.
	if localErr != nil && stream.frames == 0 {
		// Recover from local error if no frames have been sent
		return metrics.MissStatusLabel, s.readRemoteWriteLocal(req, stream)
	} else {
		s.atimeUpdater.EnqueueByResourceName(ctx, req.ResourceName)
		return metrics.HitStatusLabel, localErr
	}
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

func recordReadMetrics(cacheStatus string, proxyRequestType string, err error, bytesRead int) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           strconv.Itoa(int(gstatus.Code(err))),
		metrics.CacheHitMissStatus:    cacheStatus,
		metrics.CacheProxyRequestType: proxyRequestType,
	}
	metrics.ByteStreamProxiedReadRequests.With(labels).Inc()
	metrics.ByteStreamProxiedReadBytes.With(labels).Add(float64(bytesRead))
}

// Wrapper around a ByteStream_WriteServer that counts the number of bytes
// written through it.
type meteredServerSideClientStream struct {
	bytes int64
	bspb.ByteStream_WriteServer
}

func (s *meteredServerSideClientStream) Recv() (*bspb.WriteRequest, error) {
	message, err := s.ByteStream_WriteServer.Recv()
	s.bytes += int64(len(message.GetData()))
	return message, err
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(stream.Context())
	meteredStream := &meteredServerSideClientStream{ByteStream_WriteServer: stream}
	stream = meteredStream
	var err error
	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		err = s.writeRemoteOnly(ctx, stream)
	} else if proxy_util.SkipRemote(ctx) {
		err = s.writeLocalOnly(stream)
	} else {
		err = s.dualWrite(ctx, stream)
	}
	recordWriteMetrics(meteredStream.bytes, err, requestTypeLabel)
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
		log.CtxInfof(ctx, "error writing to local bytestream server for write: %s", err)
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

func recordWriteMetrics(bytesWritten int64, err error, proxyRequestType string) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           strconv.Itoa(int(gstatus.Code(err))),
		metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
		metrics.CacheProxyRequestType: proxyRequestType,
	}
	metrics.ByteStreamProxiedWriteRequests.With(labels).Inc()
	if bytesWritten > 0 {
		metrics.ByteStreamProxiedWriteBytes.With(labels).Add(float64(bytesWritten))
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedError("Skip remote not implemented")
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return s.remote.QueryWriteStatus(ctx, req)
}
