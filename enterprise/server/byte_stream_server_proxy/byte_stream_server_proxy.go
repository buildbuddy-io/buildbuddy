package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

type ByteStreamServerProxy struct {
	atimeUpdater  interfaces.AtimeUpdater
	authenticator interfaces.Authenticator
	local         interfaces.ByteStreamServer
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
	bytes  *atomic.Int64
	frames *atomic.Int64
	proxy  bspb.ByteStream_ReadServer
}

func (s *meteredReadServerStream) GetByteCount() int64 {
	return s.bytes.Load()
}

func (s *meteredReadServerStream) GetFrameCount() int64 {
	return s.frames.Load()
}

func (s meteredReadServerStream) Send(message *bspb.ReadResponse) error {
	s.bytes.Add(int64(proto.Size(message)))
	s.frames.Add(1)
	return s.proxy.Send(message)
}

func (s meteredReadServerStream) SetHeader(header metadata.MD) error {
	return s.proxy.SetHeader(header)
}

func (s meteredReadServerStream) SendHeader(header metadata.MD) error {
	return s.proxy.SendHeader(header)
}

func (s meteredReadServerStream) SetTrailer(trailer metadata.MD) {
	s.proxy.SetTrailer(trailer)
}

func (s meteredReadServerStream) Context() context.Context {
	return s.proxy.Context()
}

func (s meteredReadServerStream) SendMsg(message any) error {
	return s.proxy.SendMsg(message)
}

func (s meteredReadServerStream) RecvMsg(message any) error {
	return s.proxy.RecvMsg(message)
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	cacheStatus := "unknown"
	var err error
	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(ctx)
	meteredStream := &meteredReadServerStream{
		bytes:  &atomic.Int64{},
		frames: &atomic.Int64{},
		proxy:  stream,
	}
	stream = nil // use meteredStream

	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		cacheStatus = metrics.UncacheableStatusLabel
		err = s.readRemoteOnly(ctx, req, meteredStream)
	} else if proxy_util.SkipRemote(ctx) {
		err = s.readLocalOnly(req, meteredStream)
		cacheStatus = metrics.MissStatusLabel
		if err == nil {
			cacheStatus = metrics.HitStatusLabel
		}
	} else {
		localErr := s.local.Read(req, meteredStream)
		if localErr != nil && meteredStream.GetFrameCount() == 0 {
			// Recover from local error if no frames have been sent
			cacheStatus = metrics.MissStatusLabel
			err = s.readRemoteWriteLocal(req, meteredStream)
		} else {
			cacheStatus = metrics.HitStatusLabel
			s.atimeUpdater.EnqueueByResourceName(ctx, req.ResourceName)
		}
	}

	recordReadMetrics(cacheStatus, requestTypeLabel, err, int(meteredStream.GetByteCount()))
	return err
}

func (s *ByteStreamServerProxy) readRemoteOnly(ctx context.Context, req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		return err
	}
	for {
		message, err := remoteReadStream.Recv()
		if err != nil {
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

func (s *ByteStreamServerProxy) readRemoteWriteLocal(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}

	uploadRN := ""
	localWriteOffset := req.ReadOffset
	var localWriter interfaces.ByteStreamWriteHandler

	for {
		rsp, err := remoteReadStream.Recv()

		if err == io.EOF {
			if localWriter != nil {
				_, err = localWriter.Write(writeRequest(uploadRN, []byte{}, localWriteOffset, true))
				if err != nil {
					log.CtxDebugf(ctx, "Error writing to local ByteStreamServer: %v", err)
				}
			}
			return nil
		}
		if err != nil {
			log.CtxInfof(ctx, "Error streaming from remote for read through: %s", err)
			return err
		}

		if uploadRN == "" {
			// Rewrite the resource name so we can write to the local server.
			rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
			if err != nil {
				return err
			}
			uploadRN = rn.NewUploadString()
		}

		// Initialize local cache writer if necessary
		if localWriteOffset == 0 && localWriter == nil {
			localWriter, err = s.local.BeginWrite(ctx, writeRequest(uploadRN, rsp.GetData(), localWriteOffset, false))
			if err != nil {
				log.CtxDebugf(ctx, "Error opening local ByteStreamServer write stream: %v", err)
			} else {
				defer localWriter.Close()
			}
		}

		// Write to local cache
		if localWriter != nil {
			_, err = localWriter.Write(writeRequest(uploadRN, rsp.GetData(), localWriteOffset, false))
			if err != nil {
				log.CtxDebugf(ctx, "Error writing to local ByteStreamServer: %v", err)
				localWriter = nil
			}
		}

		// Send frame to client
		localWriteOffset += int64(len(rsp.GetData()))
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
}

func recordReadMetrics(cacheStatus string, proxyRequestType string, err error, bytesRead int) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus:    cacheStatus,
		metrics.CacheProxyRequestType: proxyRequestType,
	}
	metrics.ByteStreamProxiedReadRequests.With(labels).Inc()
	metrics.ByteStreamProxiedReadBytes.With(labels).Add(float64(bytesRead))
}

// Wrapper around a ByteStream_WriteServer that counts the number of bytes
// written through it.
type meteredServerSideClientStream struct {
	bytes *atomic.Int64
	proxy bspb.ByteStream_WriteServer
}

func (s *meteredServerSideClientStream) GetByteCount() int64 {
	return s.bytes.Load()
}

func (s meteredServerSideClientStream) SendAndClose(message *bspb.WriteResponse) error {
	return s.proxy.SendAndClose(message)
}

func (s meteredServerSideClientStream) Recv() (*bspb.WriteRequest, error) {
	message, err := s.proxy.Recv()
	s.bytes.Add(int64(proto.Size(message)))
	return message, err
}

func (s meteredServerSideClientStream) SetHeader(header metadata.MD) error {
	return s.proxy.SetHeader(header)
}

func (s meteredServerSideClientStream) SendHeader(header metadata.MD) error {
	return s.proxy.SendHeader(header)
}

func (s meteredServerSideClientStream) SetTrailer(trailer metadata.MD) {
	s.proxy.SetTrailer(trailer)
}

func (s meteredServerSideClientStream) Context() context.Context {
	return s.proxy.Context()
}

func (s meteredServerSideClientStream) SendMsg(message any) error {
	return s.proxy.SendMsg(message)
}

func (s meteredServerSideClientStream) RecvMsg(message any) error {
	return s.proxy.RecvMsg(message)
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(stream.Context())
	meteredStream := &meteredServerSideClientStream{
		bytes: &atomic.Int64{},
		proxy: stream,
	}
	stream = nil // use meteredStream
	var err error
	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		err = s.writeRemoteOnly(ctx, meteredStream)
	} else if proxy_util.SkipRemote(ctx) {
		err = s.writeLocalOnly(meteredStream)
	} else {
		err = s.dualWrite(ctx, meteredStream)
	}
	recordWriteMetrics(meteredStream.GetByteCount(), err, requestTypeLabel)
	return err
}

func (s *ByteStreamServerProxy) writeRemoteOnly(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err = remoteStream.Send(req); err != nil {
			return err
		}
		if req.GetFinishWrite() {
			break
		}
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

// TODO(iain): investigate performance of making the local write async
func (s *ByteStreamServerProxy) dualWrite(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	// Grab the first frame from the client so the local writer can be created
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	localWriteStream, err := s.local.BeginWrite(ctx, req)
	if err == nil {
		defer localWriteStream.Close()
	} else {
		log.CtxDebugf(ctx, "Error opening local write stream: %v", err)
	}

	remoteWriteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}

	for {
		// Send to the local ByteStreamServer (if available)
		localDone := req.GetFinishWrite()
		if localWriteStream != nil {
			if _, err := localWriteStream.Write(req); err != nil {
				localWriteStream = nil
				if err == io.EOF {
					localDone = true
				} else {
					log.CtxInfof(ctx, "error writing to local bytestream server for write: %s", err)
				}
			}
		}

		// Send to the remote ByteStreamServer
		remoteDone := req.GetFinishWrite()
		if err := remoteWriteStream.Send(req); err != nil {
			if err == io.EOF {
				remoteDone = true
			} else {
				return err
			}
		}

		// Handle stream-finished cases
		if remoteDone {
			if localWriteStream != nil && !localDone {
				log.CtxDebug(ctx, "remote write done but local write is not")
			}
			resp, err := remoteWriteStream.CloseAndRecv()
			if err != nil {
				return err
			}
			err = stream.SendAndClose(resp)
			return err
		} else if localDone {
			log.CtxDebug(ctx, "local write done but remote write is not")
		}

		// Finally, receive the next frame from the client
		req, err = stream.Recv()
		if err != nil {
			return err
		}
	}
}

func recordWriteMetrics(bytesWritten int64, err error, proxyRequestType string) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
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
