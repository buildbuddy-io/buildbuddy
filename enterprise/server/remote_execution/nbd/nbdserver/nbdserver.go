package nbdserver

import (
	"context"
	"net"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdio"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	nbdpb "github.com/buildbuddy-io/buildbuddy/proto/nbd"
)

// Device encapsulates the IO interface with the device metadata. This struct is
// used by the host to serve the device.
type Device struct {
	nbdio.BlockDevice
	Metadata *nbdpb.DeviceMetadata
}

func NewExt4Device(f *os.File, deviceID uint64, label string) (*Device, error) {
	return &Device{
		BlockDevice: &nbdio.File{File: f},
		Metadata: &nbdpb.DeviceMetadata{
			DeviceId:       deviceID,
			Label:          label,
			FilesystemType: "ext4",
			// SizeBytes is populated when the guest requests it.
		},
	}, nil
}

// Server runs on the host and serves block device contents for
// a ClientDevice to read and write.
type Server struct {
	server         *grpc.Server
	devicesByID    map[uint64]*Device
	devicesByLabel map[string]*Device
}

// DeviceServer implements nbdpb.BlockDeviceServer
var _ nbdpb.BlockDeviceServer = (*Server)(nil)

func New(ctx context.Context, env environment.Env, devices []*Device) (*Server, error) {
	devicesByID := make(map[uint64]*Device, len(devices))
	devicesByLabel := make(map[string]*Device, len(devices))
	for _, d := range devices {
		id := d.Metadata.GetDeviceId()
		if _, ok := devicesByID[id]; ok {
			return nil, status.InvalidArgumentErrorf("duplicate device ID %d", id)
		}
		devicesByID[id] = d
		label := d.Metadata.GetLabel()
		if _, ok := devicesByLabel[label]; ok {
			return nil, status.InvalidArgumentErrorf("duplicate device label %s", label)
		}
		devicesByLabel[label] = d
	}
	return &Server{
		devicesByID:    devicesByID,
		devicesByLabel: devicesByLabel,
	}, nil
}

// Start starts the device server.
// lis will be closed when calling Stop().
func (s *Server) Start(lis net.Listener) error {
	s.server = grpc.NewServer()
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
	d := s.devicesByLabel[req.GetLabel()]
	if d == nil {
		return nil, status.NotFoundErrorf("device %q not found", req.GetLabel())
	}
	// Compute size lazily (only when client requests it).
	size, err := d.Size()
	if err != nil {
		return nil, err
	}
	md := proto.Clone(d.Metadata).(*nbdpb.DeviceMetadata)
	md.SizeBytes = uint64(size)
	return &nbdpb.MetadataResponse{DeviceMetadata: md}, nil
}

func (s *Server) Read(ctx context.Context, req *nbdpb.ReadRequest) (*nbdpb.ReadResponse, error) {
	id := req.GetDeviceId()
	d := s.devicesByID[id]
	if d == nil {
		return nil, status.NotFoundErrorf("device %d not found", id)
	}
	b := make([]byte, int(req.GetLength()))
	if _, err := d.ReadAt(b, int64(req.GetOffset())); err != nil {
		return nil, status.WrapErrorf(err, "failed Read at offset 0x%x, length 0x%x", req.GetOffset(), req.GetLength())
	}
	return &nbdpb.ReadResponse{Data: b}, nil
}

func (s *Server) Write(ctx context.Context, req *nbdpb.WriteRequest) (*nbdpb.WriteResponse, error) {
	id := req.GetDeviceId()
	d := s.devicesByID[id]
	if d == nil {
		return nil, status.NotFoundErrorf("device %d not found", id)
	}
	if _, err := d.WriteAt(req.Data, int64(req.Offset)); err != nil {
		return nil, status.WrapErrorf(err, "failed Write at offset 0x%x, length 0x%x", req.GetOffset(), len(req.GetData()))
	}
	return &nbdpb.WriteResponse{}, nil
}

func (s *Server) Sync(ctx context.Context, req *nbdpb.SyncRequest) (*nbdpb.SyncResponse, error) {
	id := req.GetDeviceId()
	d := s.devicesByID[id]
	if d == nil {
		return nil, status.NotFoundErrorf("device %d not found", id)
	}
	if err := d.Sync(); err != nil {
		return nil, status.WrapError(err, "failed Sync")
	}
	return &nbdpb.SyncResponse{}, nil
}
