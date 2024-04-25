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

type ByteStreamServerProxy struct {
	env         environment.Env
	localCache  interfaces.Cache
	remoteCache bspb.ByteStreamClient
}

func Register(env *real_environment.RealEnv) error {
	byteStreamServer, err := NewByteStreamServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServerProxy: %s", err)
	}
	env.SetByteStreamServer(byteStreamServer)
	return nil
}

func NewByteStreamServerProxy(env environment.Env) (*ByteStreamServerProxy, error) {
	localCache := env.GetCache()
	if localCache == nil {
		return nil, status.FailedPreconditionError("A cache is required to enable the ByteStreamServerProxy")
	}
	remoteCache := env.GetByteStreamClient()
	if remoteCache == nil {
		return nil, fmt.Errorf("A ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		env:         env,
		localCache:  localCache,
		remoteCache: remoteCache,
	}, nil
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteStream, err := s.remoteCache.Read(stream.Context(), req)
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
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remoteCache.Write(stream.Context())
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		writeDone := req.GetFinishWrite()
		if err := remoteStream.Send(req); err != nil {
			if err == io.EOF {
				writeDone = true
			} else {
				return err
			}
		}
		if writeDone {
			lastRsp, err := remoteStream.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(lastRsp)
		}
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return s.remoteCache.QueryWriteStatus(ctx, req)
}
