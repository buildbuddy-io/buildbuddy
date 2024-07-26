package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type ByteStreamServerProxy struct {
	env    environment.Env
	local  bspb.ByteStreamClient
	remote bspb.ByteStreamClient
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
	local := env.GetLocalByteStreamClient()
	if local == nil {
		return nil, fmt.Errorf("A local ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	remote := env.GetByteStreamClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		env:    env,
		local:  local,
		remote: remote,
	}, nil
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	localReadStream, err := s.local.Read(stream.Context(), req)
	if err != nil {
		return s.readRemote(req, stream)
	}
	responseSent := false
	for {
		rsp, err := localReadStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If some responses were streamed to the client, just return the
			// error. Otherwise, fall-back to remote.
			if responseSent {
				return err
			} else {
				return s.readRemote(req, stream)
			}
		}

		if err := stream.Send(rsp); err != nil {
			return err
		}
		responseSent = true
	}
	return nil
}

// The Write() RPC requires the client keep track of some state. This struct
// and its methods take care of that.
type localWriter struct {
	ctx          context.Context
	local        bspb.ByteStream_WriteClient
	resourceName string
	initialized  bool
	offset       int64
}

func (s *localWriter) send(data []byte) error {
	req := &bspb.WriteRequest{WriteOffset: s.offset, Data: data}
	if !s.initialized {
		// Read resources have a different name format than written resources
		// Re-write the resource name with a fake execution ID so it passes
		// the write regex.
		req.ResourceName = "/uploads/00000000-0000-0000-0000-000000000000" + s.resourceName
		s.initialized = true
	}
	s.offset += int64(len(data))
	return s.local.Send(req)
}

func (s *localWriter) close() error {
	if err := s.local.Send(&bspb.WriteRequest{WriteOffset: s.offset, FinishWrite: true}); err != nil {
		return err
	}
	// Ignore the local response (but not the error)
	_, err := s.local.CloseAndRecv()
	return err
}

func (s *ByteStreamServerProxy) readRemote(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteReadStream, err := s.remote.Read(stream.Context(), req)
	if err != nil {
		return err
	}

	var localWriteStream *localWriter = nil
	if req.ReadOffset == 0 {
		localStream, err := s.local.Write(stream.Context())
		if err != nil {
			log.Warningf("error writing to local bytestream server: %s", err)
		}
		localWriteStream = &localWriter{
			ctx:          stream.Context(),
			local:        localStream,
			resourceName: req.ResourceName,
			initialized:  false,
			offset:       int64(0),
		}
	}

	for {
		rsp, err := remoteReadStream.Recv()
		if err != nil {
			if localWriteStream != nil {
				localWriteStream.close()
			}
			if err == io.EOF {
				break
			}
			return err
		}

		if localWriteStream != nil {
			if err := localWriteStream.send(rsp.Data); err != nil {
				log.Debugf("Error writing locally: %s", err)
				localWriteStream = nil
			}
		}
		if err = stream.Send(rsp); err != nil {
			if localWriteStream != nil {
				if err := localWriteStream.close(); err != nil {
					log.Debugf("Error closing local write stream: %s", err)
				}
			}
			return err
		}
	}
	return nil
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	local, err := s.local.Write(stream.Context())
	if err != nil {
		local = nil
	}
	remote, err := s.remote.Write(stream.Context())
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
					log.Infof("error writing to local bytestream server: %s", err)
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
					log.Info("remote write done but local write is not")
				}
				_, err := local.CloseAndRecv()
				if err != nil {
					log.Infof("error closing local write stream: %s", err)
				}
			}
			resp, err := remote.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(resp)
		} else if localDone {
			log.Info("local write done but remote write is not")
		}
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return s.remote.QueryWriteStatus(ctx, req)
}
