package main

import (
	"context"
	"flag"
	"net"
	"os"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/casfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmcasfs"
	libVsock "github.com/mdlayher/vsock"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	port = flag.Uint("port", vsock.VMCASFSPort, "The vsock port number to listen on")
)

type casfsServer struct {
	cfs      *casfs.CASFS
	bsClient bspb.ByteStreamClient

	mu sync.Mutex
	// Context & cancel func for CAS RPS.
	remoteRPCCtx       context.Context
	cancelRemoteRPCCtx context.CancelFunc
}

func NewServer() (*casfsServer, error) {
	if err := os.Mkdir("/casfs", 0755); err != nil {
		return nil, err
	}
	cfs := casfs.New("/workspace", "/casfs/", &casfs.Options{})
	if err := cfs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount CASFS: %s", err)
	}

	vsockDialer := func(ctx context.Context, s string) (net.Conn, error) {
		conn, err := libVsock.Dial(libVsock.Host, vsock.HostByteStreamProxyPort)
		return conn, err
	}
	conn, err := grpc.Dial("vsock", grpc.WithContextDialer(vsockDialer), grpc.WithInsecure())
	if err != nil {
		return nil, status.InternalErrorf("Could not dial host: %s", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)

	return &casfsServer{
		cfs:      cfs,
		bsClient: bsClient,
	}, nil
}

func (s *casfsServer) Prepare(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	s.mu.Lock()
	if s.cancelRemoteRPCCtx != nil {
		s.cancelRemoteRPCCtx()
	}
	rpcCtx, cancel := context.WithCancel(context.Background())
	s.remoteRPCCtx = rpcCtx
	s.cancelRemoteRPCCtx = cancel
	s.mu.Unlock()

	layout := req.GetFileSystemLayout()
	ff := dirtools.NewBatchFileFetcher(s.remoteRPCCtx, layout.GetRemoteInstanceName(), nil /* =fileCache */, s.bsClient, nil /* casClient= */)
	if err := s.cfs.PrepareForTask(ctx, ff, "fc" /* =taskID */, &container.FileSystemLayout{Inputs: layout.GetInputs()}); err != nil {
		return nil, err
	}

	return &vmfspb.PrepareResponse{}, nil
}

func (s *casfsServer) Sync(ctx context.Context, request *vmfspb.SyncRequest) (*vmfspb.SyncResponse, error) {
	s.mu.Lock()
	if s.cancelRemoteRPCCtx != nil {
		s.cancelRemoteRPCCtx()
	}
	s.mu.Unlock()
	// TODO(vadim): implement
	return &vmfspb.SyncResponse{}, nil
}

func main() {
	flag.Parse()

	ctx := context.Background()
	listener, err := vsock.NewGuestListener(ctx, uint32(*port))
	if err != nil {
		log.Fatalf("Error listening on vsock port: %s", err)
	}
	log.Infof("Starting VM CASFS listener on vsock port: %d", *port)
	server := grpc.NewServer()
	vmService, err := NewServer()
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}
	vmfspb.RegisterFileSystemServer(server, vmService)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Serve failed: %s", err)
	}
}
