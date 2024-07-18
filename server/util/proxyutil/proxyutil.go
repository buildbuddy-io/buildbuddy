package proxyutil

import (
	"context"
	"net"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// TODO(iain): make this configurable
	bufferSizeBytes = 10 * 1024 * 1024
)

// StartBufConnServer starts a gRPC server that listens to a bufconn (in-memory
// connection), and registers the provided ByteStreamServer to that gRPC
// server. It returns a grpc.ClientConn to the newly-started gRPC server.
// The server is shutdown when the provided context is canelled.
//
// TODO(iain): refactor to support registering other types of servers too.
func StartBufConnServer(ctx context.Context, localBSS bspb.ByteStreamServer) (*grpc.ClientConn, error) {
	listener := bufconn.Listen(bufferSizeBytes)
	localGRPCServer := grpc.NewServer()
	bspb.RegisterByteStreamServer(localGRPCServer, localBSS)
	go func() {
		if err := localGRPCServer.Serve(listener); err != nil {
			log.Errorf("error serving locally: %s", err.Error())
		}
		listener.Close()
		localGRPCServer.Stop()
	}()

	conn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return conn, nil
}
