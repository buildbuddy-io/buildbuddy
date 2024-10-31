package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

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
)

type ByteStreamServerProxy struct {
	atimeUpdater  interfaces.AtimeUpdater
	authenticator interfaces.Authenticator
	local         bspb.ByteStreamClient
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
	local := env.GetLocalByteStreamClient()
	if local == nil {
		return nil, fmt.Errorf("A local ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	remote := env.GetByteStreamClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		atimeUpdater:  atimeUpdater,
		authenticator: authenticator,
		local:         local,
		remote:        remote,
	}, nil
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	if authutil.EncryptionEnabled(ctx, s.authenticator) {
		metrics.ByteStreamProxyReads.With(
			prometheus.Labels{metrics.CacheHitMissStatus: "remote-only"}).Inc()
		return s.readRemote(req, stream)
	}

	localReadStream, err := s.local.Read(ctx, req)
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.CtxInfof(ctx, "Error reading from local bytestream client: %s", err)
		}
		metrics.ByteStreamProxyReads.With(
			prometheus.Labels{metrics.CacheHitMissStatus: "miss"}).Inc()
		return s.readRemote(req, stream)
	}

	s.atimeUpdater.EnqueueByResourceName(ctx, req.ResourceName)

	responseSent := false
	for {
		rsp, err := localReadStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If some responses were streamed to the client, just return the
			// error. Otherwise, fall-back to remote. We might be able to
			// continue streaming to the client by doing an offset read from
			// the remote cache, but keep it simple for now.
			if responseSent {
				log.CtxInfof(ctx, "error midstream of local read: %s", err)
				return err
			} else {
				metrics.ByteStreamProxyReads.With(
					prometheus.Labels{metrics.CacheHitMissStatus: "miss"}).Inc()
				return s.readRemote(req, stream)
			}
		}

		if err := stream.Send(rsp); err != nil {
			metrics.ByteStreamProxyReads.With(
				prometheus.Labels{metrics.CacheHitMissStatus: "hit"}).Inc()
			return err
		}
		responseSent = true
	}
	metrics.ByteStreamProxyReads.With(
		prometheus.Labels{metrics.CacheHitMissStatus: "hit"}).Inc()
	return nil
}

// The Write() RPC requires the client keep track of some state. The
// implementations of this interface take care of that.
type localWriter interface {
	send(data []byte) error
	commit() error
}

// A localWriter that discards everything sent to it.
type discardingLocalWriter struct {
}

func (s *discardingLocalWriter) send(data []byte) error {
	return nil
}

func (s *discardingLocalWriter) commit() error {
	return nil
}

// A localWriter that writes data to a local ByteStream_WriteClient.
type realLocalWriter struct {
	ctx          context.Context
	local        bspb.ByteStream_WriteClient
	resourceName string
	initialized  bool
	offset       int64
}

func (s *realLocalWriter) send(data []byte) error {
	req := &bspb.WriteRequest{WriteOffset: s.offset, Data: data}
	if !s.initialized {
		// Rewrite the resource name so we can write to the local server.
		rn, err := digest.ParseDownloadResourceName(s.resourceName)
		if err != nil {
			return err
		}
		urn, err := rn.UploadString()
		if err != nil {
			return err
		}
		req.ResourceName = urn
		s.initialized = true
	}
	s.offset += int64(len(data))
	return s.local.Send(req)
}

func (s *realLocalWriter) commit() error {
	if err := s.local.Send(&bspb.WriteRequest{WriteOffset: s.offset, FinishWrite: true}); err != nil {
		return err
	}
	// Ignore the local response (but not the error)
	_, err := s.local.CloseAndRecv()
	return err
}

func (s *ByteStreamServerProxy) readRemote(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}

	var localWriteStream localWriter = &discardingLocalWriter{}
	if req.ReadOffset == 0 && !authutil.EncryptionEnabled(ctx, s.authenticator) {
		localStream, err := s.local.Write(ctx)
		if err == nil {
			localWriteStream = &realLocalWriter{
				ctx:          ctx,
				local:        localStream,
				resourceName: req.ResourceName,
				initialized:  false,
				offset:       int64(0),
			}
		} else {
			log.CtxInfof(ctx, "error opening local bytestream write stream for read through: %s", err)
		}
	}

	for {
		rsp, err := remoteReadStream.Recv()
		if err != nil {
			if err == io.EOF {
				if err := localWriteStream.commit(); err != nil {
					log.CtxInfof(ctx, "error committing local write: %s", err)
				}
				break
			}
			log.CtxInfof(ctx, "error streaming from remote for read through: %s", err)
			return err
		}

		if err := localWriteStream.send(rsp.Data); err != nil {
			log.CtxInfof(ctx, "Error writing locally for read through: %s", err)
			localWriteStream = &discardingLocalWriter{}
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	var local bspb.ByteStream_WriteClient
	if !authutil.EncryptionEnabled(ctx, s.authenticator) {
		localWriter, err := s.local.Write(ctx)
		if err != nil {
			log.CtxInfof(ctx, "error opening local bytestream write stream for write: %s", err)
			localWriter = nil
		}
		local = localWriter
	}
	remote, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// Send to the local ByteStreamServer (if it hasn't errored)
		localDone := req.GetFinishWrite()
		if local != nil {
			if err := local.Send(req); err != nil {
				if err == io.EOF {
					localDone = true
				} else {
					log.CtxInfof(ctx, "error writing to local bytestream server for write: %s", err)
				}
				local = nil
			}
		}

		// Send to the remote ByteStreamServer
		done := req.GetFinishWrite()
		if err := remote.Send(req); err != nil {
			if err == io.EOF {
				done = true
			} else {
				return err
			}
		}

		// If the client or the remote server told us the write is done, send the
		// response to the client.
		if done {
			if local != nil {
				if !localDone {
					log.CtxInfo(ctx, "remote write done but local write is not")
				}
				if _, err := local.CloseAndRecv(); err != nil {
					log.CtxInfof(ctx, "error closing local write stream: %s", err)
				}
			}
			resp, err := remote.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(resp)
		} else if localDone {
			log.CtxInfo(ctx, "local write done but remote write is not")
		}
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return s.remote.QueryWriteStatus(ctx, req)
}
