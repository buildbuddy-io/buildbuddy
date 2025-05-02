package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

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

func (s *ByteStreamServerProxy) readRemoteWriteLocal(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}

	// Retrieve first frame from read stream
	rsp, err := remoteReadStream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.CtxInfof(ctx, "Error streaming from remote for read-remote-write-local: %s", err)
		return err
	}

	// Rewrite the resource name so we can write to the local server
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	uploadRN := rn.NewUploadString()

	// Open the local writer (if possible).
	localWriteOffset := req.GetReadOffset()
	var localWriter interfaces.ByteStreamWriteHandler
	if req.GetReadOffset() == 0 {
		localWriter, err = s.local.BeginWrite(ctx, writeRequest(uploadRN, rsp.GetData(), localWriteOffset, false))
		if err != nil {
			log.CtxDebugf(ctx, "Error opening local ByteStreamServer write stream: %v", err)
		} else {
			defer localWriter.Close()
		}
	}

	for {
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

		// Retreive the next frame and handle errors
		rsp, err = remoteReadStream.Recv()
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
	if err == io.EOF {
		log.CtxInfof(ctx, "Unexpected EOF reading first frame: %v", err)
		return nil
	}
	if err != nil {
		return err
	}

	localWriteStream, err := s.local.BeginWrite(ctx, req)
	if err == nil {
		defer localWriteStream.Close()
	} else {
		log.CtxWarningf(ctx, "Error opening local write stream: %v", err)
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
				log.CtxInfo(ctx, "remote write done but local write is not")
			}
			resp, err := remoteWriteStream.CloseAndRecv()
			if err != nil {
				return err
			}
			err = stream.SendAndClose(resp)
			return err
		} else if localDone {
			log.CtxInfo(ctx, "local write done but remote write is not")
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
