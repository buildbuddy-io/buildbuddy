package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Channel size to use for tee-ing writes to the local bytestream server.
	// This value should be somewhat large to prevent blocking remote writes if
	// the local server is slow for some reason. Note that this channel size is
	// request-scoped, not global scoped, so there will be this many channel
	// places per write request.
	localWriteChannelSize = 512
)

type ByteStreamServerProxy struct {
	env    environment.Env
	local  byte_stream_server.ByteStreamServer
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
	local, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		return nil, err
	}
	remote := env.GetByteStreamClient()
	if remote == nil {
		return nil, fmt.Errorf("A ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		env:    env,
		local:  *local,
		remote: remote,
	}, nil
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	err := s.local.Read(req, stream)
	if status.IsNotFoundError(err) {
		return s.readRemote(req, stream)
	}
	return err
}

type localWriteStream struct {
	ctx     context.Context
	channel chan *bspb.WriteRequest
}

func (s localWriteStream) Context() context.Context {
	return s.ctx
}
func (s localWriteStream) SendAndClose(resp *bspb.WriteResponse) error {
	return nil
}
func (s localWriteStream) Recv() (*bspb.WriteRequest, error) {
	req := <-s.channel
	return req, nil
}

func (s *ByteStreamServerProxy) readRemote(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteStream, err := s.remote.Read(stream.Context(), req)
	if err != nil {
		return err
	}

	var localStream *localWriteStream = nil
	localStreamInitialized := false
	localStreamOffset := int64(0)
	if req.ReadOffset == 0 {
		localStream = &localWriteStream{
			ctx:     stream.Context(),
			channel: make(chan *bspb.WriteRequest, localWriteChannelSize),
		}
		localStreamOffset = req.ReadOffset
		go func() {
			if err := s.local.WriteDirect(localStream); err != nil {
				log.Warningf("Error writing to local cache: %s", err)
			}
		}()
	}

	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			if localStream != nil {
				localStream.channel <- &bspb.WriteRequest{WriteOffset: localStreamOffset, FinishWrite: true}
			}
			break
		}
		if err != nil {
			return err
		}
		if localStream != nil {
			localReq := &bspb.WriteRequest{WriteOffset: localStreamOffset, Data: rsp.Data}
			if !localStreamInitialized {
				// Resources that are read have a different format than those
				// being written. Re-write the resource name with a fake
				// execution ID so it passes the write regex.
				localReq.ResourceName = "/uploads/00000000-0000-0000-0000-000000000000" + req.ResourceName
				localStreamInitialized = true
			}
			localStreamOffset += int64(len(rsp.Data))
			localStream.channel <- localReq
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	localStream := localWriteStream{
		ctx:     stream.Context(),
		channel: make(chan *bspb.WriteRequest, localWriteChannelSize),
	}
	go func() {
		if err := s.local.WriteDirect(localStream); err != nil {
			log.Warningf("Error writing to local bytestream server: %s", err)
		}
	}()

	remoteStream, err := s.remote.Write(stream.Context())
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		localStream.channel <- req
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
	return s.remote.QueryWriteStatus(ctx, req)
}
