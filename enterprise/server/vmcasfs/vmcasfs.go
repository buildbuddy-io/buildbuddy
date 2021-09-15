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
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmcasfs"
	libVsock "github.com/mdlayher/vsock"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// this should match guestCASFSMountDir in firecracker.go
	mountDir = "/casfs"
)

type casfsServer struct {
	env environment.Env
	cfs *casfs.CASFS

	mu        sync.Mutex
	vfsClient vfspb.FileSystemClient
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
	bsClient := bspb.NewByteStreamClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)

	vfsClient := vfspb.NewFileSystemClient(conn)

	cfs := casfs.New(vfsClient, mountDir, &casfs.Options{})
	if err := cfs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount CASFS: %s", err)
	}

	configurator, err := config.NewConfigurator("")
	if err != nil {
		return nil, err
	}
	healthChecker := healthcheck.NewHealthChecker("vmexec")
	env := real_environment.NewRealEnv(configurator, healthChecker)
	env.SetByteStreamClient(bsClient)
	env.SetContentAddressableStorageClient(casClient)

	return &casfsServer{
		env:       env,
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
	if err := s.cfs.PrepareForTask(context.Background(), "fc" /* =taskID */, layout); err != nil {
		return nil, err
	}

	return &vmfspb.PrepareResponse{}, nil
}

func (s *casfsServer) Finish(ctx context.Context, request *vmfspb.FinishRequest) (*vmfspb.FinishResponse, error) {
	err := s.cfs.FinishTask()
	if err != nil {
		return nil, err
	}

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
