package nbdclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Merovius/nbd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	nbdpb "github.com/buildbuddy-io/buildbuddy/proto/nbd"
	libvsock "github.com/mdlayher/vsock"
)

const (
	// How long to wait for an NBD to become ready after it is connected.
	readyCheckTimeout = 1 * time.Second

	// How often to poll for an NBD to become ready.
	readyCheckPollInterval = 500 * time.Microsecond
)

type device struct {
	// Index is the nbd index assigned to the device.
	// For example, 0 means that the device path is "/dev/nbd0"
	Index uint32
	// Cancel cancels the device context, which should disconnect it.
	cancel context.CancelFunc
	// Wait waits for the device to be disconnected.
	wait func() error
}

func (d *device) Disconnect() error {
	d.cancel()
	return d.wait()
}

// ClientDevice implements the nbd.Device interface by forwarding read and write
// requests to a remote target.
type ClientDevice struct {
	ctx       context.Context
	client    nbdpb.BlockDeviceClient
	conn      *grpc.ClientConn
	metadata  *nbdpb.DeviceMetadata
	device    *device
	mountPath string
}

func dialHost() (*grpc.ClientConn, error) {
	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return libvsock.Dial(libvsock.Host, vsock.HostBlockDeviceServerPort, &libvsock.Config{})
	}
	return grpc.Dial("vsock", grpc.WithContextDialer(dialer), grpc.WithInsecure())
}

// NewClientDevice returns a client to the host device with the given name.
func NewClientDevice(ctx context.Context, name string) (*ClientDevice, error) {
	conn, err := dialHost()
	if err != nil {
		return nil, err
	}
	client := nbdpb.NewBlockDeviceClient(conn)
	return &ClientDevice{
		ctx:    ctx,
		conn:   conn,
		client: client,
		// Note: only the name metadata is populated initially; the rest of the
		// metadata is fetched from the host before mounting.
		metadata: &nbdpb.DeviceMetadata{Name: name},
	}, nil
}

// DevicePath returns the /dev/nbd* path for this device, such as "/dev/nbd0"
func (d *ClientDevice) DevicePath() string {
	if d.device == nil {
		fatalf("called DevicePath() before device is created")
	}
	return fmt.Sprintf("/dev/nbd%d", d.device.Index)
}

func (d *ClientDevice) updateMetadata(ctx context.Context) error {
	return d.doWithRedial(func() error {
		res, err := d.client.Metadata(ctx, &nbdpb.MetadataRequest{
			Name: d.metadata.GetName(),
		})
		if err != nil {
			return status.WrapError(err, "failed to get device metadata from host")
		}
		d.metadata = res.DeviceMetadata
		return nil
	})
}

// createDevice connects a free network block device such as "/dev/nbd0" to this
// client device and waits for the device to become ready.
//
// Note that the kernel pre-allocates a fixed number of inactive nbd devices
// according to the kernel config. "Creating" the device here just means that we
// are activating one of these existing devices, connecting it to this
// ClientDevice instance.
func (d *ClientDevice) createDevice() error {
	if d.device != nil {
		return status.FailedPreconditionError("device is already created")
	}

	start := time.Now()

	ctx := d.ctx
	if err := d.updateMetadata(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	idx, wait, err := nbd.Loopback(ctx, d, uint64(d.metadata.GetSizeBytes()))
	if err != nil {
		cancel()
		return status.WrapErrorf(err, "failed to create device %q", d.metadata.GetName())
	}
	d.device = &device{
		Index:  idx,
		cancel: cancel,
		wait:   wait,
	}
	if err := d.waitForReady(ctx); err != nil {
		return status.WrapError(err, "ready check poll failed")
	}

	log.Infof("Created network block device %q at %s in %s", d.metadata.GetName(), d.DevicePath(), time.Since(start))
	return nil
}

// waitForReady waits for the NBD device to become ready.
func (d *ClientDevice) waitForReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
	defer cancel()

	f, err := os.Open(fmt.Sprintf("/sys/block/nbd%d/size", d.device.Index))
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		rsize, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(strings.TrimSpace(string(rsize)), 10, 64)
		if err != nil {
			return err
		}
		if size > 0 {
			return nil
		}
		select {
		case <-time.After(readyCheckPollInterval):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Mount connects the NBD device and mounts it to the given directory path. The
// directory must already exist.
func (d *ClientDevice) Mount(path string) error {
	if d.mountPath != "" {
		return status.FailedPreconditionError("already mounted")
	}
	if err := d.createDevice(); err != nil {
		return status.WrapError(err, "failed to create device")
	}
	fstype := filesystemTypeString(d.metadata.GetFilesystemType())
	if err := syscall.Mount(d.DevicePath(), path, fstype, syscall.MS_RELATIME, "" /*=data*/); err != nil {
		return status.InternalErrorf("failed to mount %s (type %s) to %s: %s", d.DevicePath(), fstype, path, err)
	}
	d.mountPath = path
	log.Infof("Mounted %s (type %s) to %s", d.DevicePath(), d.metadata.GetName(), path)
	return nil
}

// Unmount unmounts the device from its current mount path and disconnects the
// device.
func (d *ClientDevice) Unmount() error {
	if d.mountPath == "" {
		return status.FailedPreconditionError("not mounted")
	}
	if err := syscall.Unmount(d.mountPath, 0); err != nil {
		return status.InternalErrorf("unmount %s (%s): %s", d.mountPath, d.metadata.GetName(), err)
	}
	log.Infof("Unmounted %s", d.mountPath)
	d.mountPath = ""
	// Also disconnect the device since the host may be swapping out the backing
	// file, and it's simpler to disconnect and reconnect rather than try to
	// reconfigure a connected device.
	if err := d.device.Disconnect(); err != nil {
		return status.InternalErrorf("disconnect %s (%s): %s", d.DevicePath(), d.metadata.GetName(), err)
	}
	log.Infof("Disconnected %s", d.DevicePath())
	d.device = nil
	return nil
}

func (d *ClientDevice) ReadAt(p []byte, off int64) (int, error) {
	var res *nbdpb.ReadResponse
	err := d.doWithRedial(func() error {
		var err error
		res, err = d.client.Read(d.ctx, &nbdpb.ReadRequest{
			Name:   d.metadata.GetName(),
			Offset: off,
			Length: int64(len(p)),
		})
		return err
	})
	if err != nil {
		fatalf("%s: read failed: %s", d.DevicePath(), err)
	}
	if len(res.Data) > len(p) {
		fatalf(
			"block device server returned too many bytes (0x%x) for read at offset=0x%x length=0x%x",
			len(res.Data), len(p), off)
	}
	n := copy(p, res.Data)
	return n, nil
}

func (d *ClientDevice) WriteAt(p []byte, off int64) (int, error) {
	err := d.doWithRedial(func() error {
		_, err := d.client.Write(d.ctx, &nbdpb.WriteRequest{
			Name:   d.metadata.GetName(),
			Offset: off,
			Data:   p,
		})
		return err
	})
	if err != nil {
		fatalf("%s: write failed: %s", d.DevicePath(), err)
	}
	return len(p), nil
}

func (d *ClientDevice) Sync() error {
	// Do nothing for now since we don't buffer reads/writes and instead pass
	// them through directly to the host executor. The executor is responsible
	// for guaranteeing that reads and writes are persisted.
	return nil
}

// doWithRedial invokes a function that performs an RPC to the host server,
// and re-dials the host server if the function fails on the first attempt.
func (d *ClientDevice) doWithRedial(f func() error) error {
	var lastErr error
	for i := 1; i <= 2; i++ {
		err := f()
		if err == nil || !status.IsUnavailableError(err) {
			return err
		}
		d.conn.Close()
		log.Infof("%s: host device server is unavailable; reconnecting.", d.DevicePath())
		d.conn, err = dialHost()
		if err != nil {
			return status.WrapErrorf(lastErr, "failed to re-dial host (got error %s) after initial UNAVAILABLE error", err)
		}
		d.client = nbdpb.NewBlockDeviceClient(d.conn)
		lastErr = err
	}
	return lastErr
}

func filesystemTypeString(t nbdpb.FilesystemType) string {
	switch t {
	case nbdpb.FilesystemType_EXT4_FILESYSTEM_TYPE:
		return "ext4"
	default:
		return ""
	}
}

// fatalf crashes the VM. It is used in cases where a read or write from the
// host fails and we do not want to return the error back to the NBD device
// driver, since otherwise users would see an application-specific IO
// error/exception in their actions (or worse, they would not see an error
// but have an inconsistent disk state).
func fatalf(format string, args ...any) {
	// NOTE: do not change this "die: " prefix. We rely on it to parse the fatal
	// error from the firecracker machine logs and return it back to the user.
	log.Fatalf("die: "+format, args...)
}
