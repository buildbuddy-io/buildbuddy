package grpc_stream_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_stream"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"

	"fmt"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	framesToReturn    = 10
	dataBytesPerFrame = 10
)

type TestServer struct {
	client bspb.ByteStreamClient
}

func (s *TestServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	for i := 0; i < framesToReturn; i++ {
		data := strings.Repeat(fmt.Sprintf("%d", i), dataBytesPerFrame)
		resp := &bspb.ReadResponse{Data: []byte(data)}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (s *TestServer) Write(stream bspb.ByteStream_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if req.GetFinishWrite() {
			return stream.SendAndClose(&bspb.WriteResponse{})
		}
	}
}

func (s *TestServer) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return &bspb.QueryWriteStatusResponse{}, nil
}

func startServer(t *testing.T, env environment.Env) *TestServer {
	port := testport.FindFree(t)
	server, err := grpc_server.New(env, port, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	testServer := TestServer{}
	bspb.RegisterByteStreamServer(server.GetServer(), &testServer)
	require.NoError(t, server.Start())
	conn, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", port))
	require.NoError(t, err)
	testServer.client = bspb.NewByteStreamClient(conn)
	return &testServer
}

func TestByteCountingServerStream(t *testing.T) {
	server := startServer(t, testenv.GetTestEnv(t))
	grpcStream, err := server.client.Read(t.Context(), &bspb.ReadRequest{})
	require.NoError(t, err)
	stream := grpc_stream.NewByteCountingServerStream(grpcStream)
	require.Equal(t, int64(0), stream.GetByteCount())
	for i := 0; i < 2*framesToReturn; i++ {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, int64(120), stream.GetByteCount())
}

func TestByteCountingClientStream(t *testing.T) {
	server := startServer(t, testenv.GetTestEnv(t))
	grpcStream, err := server.client.Write(t.Context())
	require.NoError(t, err)
	stream := grpc_stream.NewByteCountingClientStream(grpcStream)
	require.NotNil(t, stream)
	bytesSent := 0
	require.Equal(t, int64(0), stream.GetByteCount())
	for i := 0; i < 10; i++ {
		req := &bspb.WriteRequest{Data: []byte(strings.Repeat("a", 10))}
		bytesSent += proto.Size(req)
		require.NoError(t, stream.Send(req))
		require.Equal(t, int64(bytesSent), stream.GetByteCount())
	}
}
