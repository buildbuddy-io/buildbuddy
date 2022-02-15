package api

import (
	"context"
	"net"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

type Server struct {
	fileDir string
	store   *store.Store

	grpcServer *grpc.Server
}

func NewServer(fileDir string, store *store.Store) (*Server, error) {
	s := &Server{
		fileDir: fileDir,
		store:   store,
	}
	return s, nil
}

func (s *Server) Start(grpcAddress string) error {
	// grpcServer is responsible for presenting an API to manage raft nodes
	// on each host, as well as an API to shuffle data around between nodes,
	// outside of raft.
	s.grpcServer = grpc.NewServer()
	reflection.Register(s.grpcServer)
	rfspb.RegisterApiServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return err
	}
	go func() {
		s.grpcServer.Serve(lis)
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return grpc_server.GRPCShutdown(ctx, s.grpcServer)
}

func (s *Server) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	return s.store.StartCluster(ctx, req)
}

func (s *Server) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	return s.store.RemoveData(ctx, req)
}

func (s *Server) Read(req *rfpb.ReadRequest, stream rfspb.Api_ReadServer) error {
	return s.store.Read(req, stream)
}

func (s *Server) Write(stream rfspb.Api_WriteServer) error {
	return s.store.Write(stream)
}

func (s *Server) FindMissing(ctx context.Context, req *rfpb.FindMissingRequest) (*rfpb.FindMissingResponse, error) {
	return s.store.FindMissing(ctx, req)
}

func (s *Server) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	return s.store.SyncPropose(ctx, req)
}

func (s *Server) SyncRead(ctx context.Context, req *rfpb.SyncReadRequest) (*rfpb.SyncReadResponse, error) {
	return s.store.SyncRead(ctx, req)
}
