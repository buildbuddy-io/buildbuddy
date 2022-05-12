package main

import (
	"context"
	"flag"
	"net"
	"os"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	libVsock "github.com/mdlayher/vsock"
)

const (
	// this should match guestVFSMountDir in firecracker.go
	mountDir = "/vfs"
)

type vfsServer struct {
	vfs       *vfs.VFS
	vfsClient vfspb.FileSystemClient

	mu sync.Mutex
	// Context & cancel func for VFS RPCs.
	remoteCtx        context.Context
	cancelRemoteFunc context.CancelFunc
}

func NewServer() (*vfsServer, error) {
	if err := os.Mkdir(mountDir, 0755); err != nil {
		return nil, err
	}

	vsockDialer := func(ctx context.Context, s string) (net.Conn, error) {
		conn, err := libVsock.Dial(libVsock.Host, vsock.HostVFSServerPort, &libVsock.Config{})
		return conn, err
	}
	conn, err := grpc.Dial("vsock", grpc.WithContextDialer(vsockDialer), grpc.WithInsecure())
	if err != nil {
		return nil, status.InternalErrorf("Could not dial host: %s", err)
	}

	vfsClient := vfspb.NewFileSystemClient(conn)

	fs := vfs.New(vfsClient, mountDir, &vfs.Options{})
	if err := fs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount VFS: %s", err)
	}

	return &vfsServer{
		vfs:       fs,
		vfsClient: vfsClient,
	}, nil
}

func (s *vfsServer) Prepare(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	// This is the context that is used to make RPCs to the host.
	// It needs to stay alive as long as there's an active command on the VM.
	rpcCtx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.remoteCtx = rpcCtx
	s.cancelRemoteFunc = cancel
	s.mu.Unlock()

	if err := s.vfs.PrepareForTask(s.remoteCtx, "fc" /* =taskID */); err != nil {
		return nil, err
	}

	return &vmfspb.PrepareResponse{}, nil
}

func (s *vfsServer) Finish(ctx context.Context, request *vmfspb.FinishRequest) (*vmfspb.FinishResponse, error) {
	s.mu.Lock()
	if s.cancelRemoteFunc != nil {
		s.cancelRemoteFunc()
	}
	s.mu.Unlock()

	err := s.vfs.FinishTask()
	if err != nil {
		return nil, err
	}

	return &vmfspb.FinishResponse{}, nil
}

func main() {
	flag.Parse()

	ctx := context.Background()
	listener, err := vsock.NewGuestListener(ctx, vsock.VMVFSPort)
	if err != nil {
		log.Fatalf("Error listening on vsock port: %s", err)
	}
	log.Infof("Starting VM VFS listener on vsock port: %d", vsock.VMVFSPort)
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
