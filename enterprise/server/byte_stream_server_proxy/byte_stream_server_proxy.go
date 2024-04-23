package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type ByteStreamServer struct {
	env          environment.Env
	local_cache  interfaces.Cache
	remote_cache bspb.ByteStreamClient
}

func Register(env *real_environment.RealEnv) error {
	byteStreamServer, err := NewByteStreamServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServerProxy: %s", err)
	}
	env.SetByteStreamServer(byteStreamServer)
	return nil
}

func NewByteStreamServer(env environment.Env) (*ByteStreamServer, error) {
	local_cache := env.GetCache()
	if local_cache == nil {
		return nil, status.FailedPreconditionError("A cache is required to enable the ByteStreamServerProxy")
	}
	remote_cache := env.GetByteStreamClient()
	if remote_cache == nil {
		return nil, fmt.Errorf("A ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServer{
		env:          env,
		local_cache:  local_cache,
		remote_cache: remote_cache,
	}, nil
}

func (s *ByteStreamServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteStream, err := s.remote_cache.Read(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		stream.Send(rsp)
	}
	return nil
}

func (s *ByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	remote_stream, err := s.remote_cache.Write(stream.Context())
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		writeDone := req.GetFinishWrite()
		if err := remote_stream.Send(req); err != nil {
			if err == io.EOF {
				writeDone = true
			} else {
				return err
			}
		}
		if writeDone {
			lastRsp, err := remote_stream.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(lastRsp)
		}
	}
}

func (s *ByteStreamServer) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return s.remote_cache.QueryWriteStatus(ctx, req)
}
