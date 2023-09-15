package nbdserver

import (
	"context"
	"net"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	nbdpb "github.com/buildbuddy-io/buildbuddy/proto/nbd"
)

// Device is a block store exported by a server along with its associated
// metadata.
type Device struct {
	*copy_on_write.COWStore
	Metadata *nbdpb.DeviceMetadata
}

func NewExt4Device(store *copy_on_write.COWStore, name string) (*Device, error) {
	return &Device{
		COWStore: store,
		Metadata: &nbdpb.DeviceMetadata{
			Name:           name,
			FilesystemType: nbdpb.FilesystemType_EXT4_FILESYSTEM_TYPE,
		},
	}, nil
}

// Server runs on the host and serves block device contents for
// a ClientDevice to read and write.
type Server struct {
	env    environment.Env
	server *grpc.Server

	mu      sync.RWMutex
	devices map[string]*Device
}

// DeviceServer implements nbdpb.BlockDeviceServer
var _ nbdpb.BlockDeviceServer = (*Server)(nil)

func New(ctx context.Context, env environment.Env, devices ...*Device) (*Server, error) {
	m := make(map[string]*Device, len(devices))
	for _, d := range devices {
		name := d.Metadata.GetName()
		if _, ok := m[name]; ok {
			return nil, status.InvalidArgumentErrorf("duplicate device label %s", name)
		}
		m[name] = d
	}
	return &Server{
		env:     env,
		devices: m,
	}, nil
}

func (s *Server) getDevice(name string) *Device {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.devices[name]
}

func (s *Server) SetDevice(name string, device *Device) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.devices[name] = device
}

// Start starts the device server.
// lis will be closed when calling Stop().
func (s *Server) Start(lis net.Listener) error {
	// Intentionally using a minimal set of server options here here since the
	// common interceptors add overhead to each disk I/O operation and also add
	// noise to the logs.
	s.server = grpc.NewServer(grpc_server.KeepaliveEnforcementPolicy())
	nbdpb.RegisterBlockDeviceServer(s.server, s)
	go func() {
		_ = s.server.Serve(lis)
	}()
	return nil
}

func (s *Server) Stop() error {
	s.server.Stop()
	return nil
}

func (s *Server) Metadata(ctx context.Context, req *nbdpb.MetadataRequest) (*nbdpb.MetadataResponse, error) {
	d := s.getDevice(req.GetName())
	if d == nil {
		return nil, status.NotFoundErrorf("device %q not found", req.GetName())
	}
	// Compute size lazily (only when client requests it).
	size, err := d.SizeBytes()
	if err != nil {
		return nil, err
	}
	// Attach size to existing metadata.
	md := proto.Clone(d.Metadata).(*nbdpb.DeviceMetadata)
	md.SizeBytes = size
	return &nbdpb.MetadataResponse{DeviceMetadata: md}, nil
}

func (s *Server) Read(ctx context.Context, req *nbdpb.ReadRequest) (*nbdpb.ReadResponse, error) {
	name := req.GetName()
	d := s.getDevice(name)
	if d == nil {
		return nil, status.NotFoundErrorf("device %q not found", name)
	}
	b := make([]byte, int(req.GetLength()))
	if _, err := d.ReadAt(b, int64(req.GetOffset())); err != nil {
		return nil, status.WrapErrorf(err, "failed Read at offset 0x%x, length 0x%x", req.GetOffset(), req.GetLength())
	}
	return &nbdpb.ReadResponse{Data: b}, nil
}

func (s *Server) Write(ctx context.Context, req *nbdpb.WriteRequest) (*nbdpb.WriteResponse, error) {
	name := req.GetName()
	d := s.getDevice(name)
	if d == nil {
		return nil, status.NotFoundErrorf("device %q not found", name)
	}
	if _, err := d.WriteAt(req.Data, int64(req.Offset)); err != nil {
		return nil, status.WrapErrorf(err, "failed Write at offset 0x%x, length 0x%x", req.GetOffset(), len(req.GetData()))
	}
	return &nbdpb.WriteResponse{}, nil
}
