package main

import (
	"context"
	"flag"
	"net"
	"os"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/casfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmcasfs"
	libVsock "github.com/mdlayher/vsock"
)

const (
	// this should match guestCASFSMountDir in firecracker.go
	mountDir = "/casfs"
)

type casfsServer struct {
	cfs       *casfs.CASFS
	vfsClient vfspb.FileSystemClient

	mu sync.Mutex
	// Context & cancel func for VFS RPCs.
	remoteCtx        context.Context
	cancelRemoteFunc context.CancelFunc
}

func NewServer() (*casfsServer, error) {
	if err := os.Mkdir(mountDir, 0755); err != nil {
		return nil, err
	}

	vsockDialer := func(ctx context.Context, s string) (net.Conn, error) {
		conn, err := libVsock.Dial(libVsock.Host, vsock.HostByteStreamProxyPort)
		return conn, err
	}
	conn, err := grpc.Dial("vsock", grpc.WithContextDialer(vsockDialer), grpc.WithInsecure())
	if err != nil {
		return nil, status.InternalErrorf("Could not dial host: %s", err)
	}

	vfsClient := vfspb.NewFileSystemClient(conn)

	cfs := casfs.New(vfsClient, mountDir, &casfs.Options{})
	if err := cfs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount CASFS: %s", err)
	}

	return &casfsServer{
		cfs:       cfs,
		vfsClient: vfsClient,
	}, nil
}

func (s *casfsServer) Prepare(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	reqLayout := req.GetFileSystemLayout()
	// TODO(vadim): get rid of this struct and use a common proto throughout
	layout := &container.FileSystemLayout{
		Inputs: reqLayout.GetInputs(),
	}

	// This is the context that is used to make RPCs to the host.
	// It needs to stay alive as long as there's an active command on the VM.
	rpcCtx, cancel := context.WithCancel(context.Background())
	s.remoteCtx = rpcCtx
	s.cancelRemoteFunc = cancel
	s.mu.Unlock()

	if err := s.cfs.PrepareForTask(s.remoteCtx, "fc" /* =taskID */, layout); err != nil {
		return nil, err
	}

	return &vmfspb.PrepareResponse{}, nil
}

func (s *casfsServer) Finish(ctx context.Context, request *vmfspb.FinishRequest) (*vmfspb.FinishResponse, error) {
	s.mu.Lock()
	if s.cancelRemoteFunc != nil {
		s.cancelRemoteFunc()
	}
	s.mu.Unlock()
	return &vmfspb.FinishResponse{}, nil
}

func main() {
	flag.Parse()

	ctx := context.Background()
	listener, err := vsock.NewGuestListener(ctx, vsock.VMCASFSPort)
	if err != nil {
		log.Fatalf("Error listening on vsock port: %s", err)
	}
	log.Infof("Starting VM CASFS listener on vsock port: %d", vsock.VMCASFSPort)
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
