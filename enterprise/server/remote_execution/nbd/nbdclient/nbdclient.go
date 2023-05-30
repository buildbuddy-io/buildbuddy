package nbdclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/samalba/buse-go/buse"
	"google.golang.org/grpc"

	nbdpb "github.com/buildbuddy-io/buildbuddy/proto/nbd"
	libvsock "github.com/mdlayher/vsock"
)

const (
	// How long to wait for an NBD to become ready after it is created.
	readyCheckTimeout = 1 * time.Second

	// How often to poll for an NBD to become ready.
	readyCheckPollInterval = 1 * time.Millisecond
)

// ClientDevice implements the BUSE interface by forwarding read and write
// requests to a remote target.
type ClientDevice struct {
	ctx       context.Context
	client    nbdpb.BlockDeviceClient
	conn      *grpc.ClientConn
	metadata  *nbdpb.DeviceMetadata
	device    *buse.BuseDevice
	mountPath string
}

// ClientDevice implements buse.BuseInterface
var _ buse.BuseInterface = (*ClientDevice)(nil)

func dialHost() (*grpc.ClientConn, error) {
	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return libvsock.Dial(libvsock.Host, vsock.HostBlockDeviceServerPort, &libvsock.Config{})
	}
	return grpc.Dial("vsock", grpc.WithContextDialer(dialer), grpc.WithInsecure())
}

// NewClientDevice returns a client to the host device with the given label.
func NewClientDevice(ctx context.Context, label string) (*ClientDevice, error) {
	conn, err := dialHost()
	if err != nil {
		return nil, err
	}
	client := nbdpb.NewBlockDeviceClient(conn)
	return &ClientDevice{
		ctx:    ctx,
		conn:   conn,
		client: client,
		// Note: only the label metadata is populated initially; the rest of the
		// metadata is fetched from the host before mounting.
		metadata: &nbdpb.DeviceMetadata{Label: label},
	}, nil
}

// DevicePath returns the /dev/nbd* path for this device, such as "/dev/nbd0"
func (d *ClientDevice) DevicePath() string {
	return fmt.Sprintf("/dev/nbd%d", d.metadata.GetDeviceId())
}

func (d *ClientDevice) updateMetadata(ctx context.Context) error {
	return d.doWithRedial(func() error {
		res, err := d.client.Metadata(ctx, &nbdpb.MetadataRequest{Label: d.metadata.GetLabel()})
		if err != nil {
			return status.WrapError(err, "failed to get device metadata from host")
		}
		d.metadata = res.DeviceMetadata
		return nil
	})
}

// CreateDevice connects the network block device such as "/dev/nbd0" to this
// client device.
//
// Note that the kernel pre-allocates a fixed number of inactive nbd devices
// according to the kernel config. "Creating" the device here just means that we
// are activating one of these existing devices, connecting it to this
// ClientDevice instance.
func (d *ClientDevice) createDevice(ctx context.Context) error {
	start := time.Now()

	if err := d.updateMetadata(ctx); err != nil {
		return err
	}

	path := d.DevicePath()
	device, err := buse.CreateDevice(path, uint(d.metadata.GetSizeBytes()), d)
	if err != nil {
		return err
	}
	d.device = device
	go func() {
		err := device.Connect()
		log.Fatalf("die: %s unexpectedly disconnected: %s", d.DevicePath(), err)
	}()

	if err := d.waitForReady(ctx); err != nil {
		return status.WrapError(err, "ready check poll failed")
	}

	log.Infof("Created network block device %q at %s in %s", d.metadata.GetLabel(), path, time.Since(start))
	return nil
}

// waitForReady waits for the device file to become ready.
func (d *ClientDevice) waitForReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
	defer cancel()

	f, err := os.Open(fmt.Sprintf("/sys/block/%s/size", filepath.Base(d.DevicePath())))
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

// Mount connects the NBD device if not already connected, updates the device
// size parameter to match its current backing file size (according to the host
// server), and mounts the client device to the given directory path. The
// directory must already exist.
func (d *ClientDevice) Mount(path string) error {
	if d.mountPath != "" {
		return status.FailedPreconditionError("already mounted")
	}
	d.mountPath = path
	if d.device == nil {
		if err := d.createDevice(context.TODO()); err != nil {
			return status.WrapError(err, "failed to create device")
		}
	} else {
		// If we're re-mounting the same device but with a new image, need to
		// update the block device size so that it matches the image size,
		// otherwise the mount will fail due to the block size mismatch.
		if err := d.updateMetadata(context.TODO()); err != nil {
			return status.WrapError(err, "failed to fetch device metadata")
		}
		d.device.SetSize(uint(d.metadata.GetSizeBytes()))
	}
	if err := syscall.Mount(d.DevicePath(), path, d.metadata.GetFilesystemType(), syscall.MS_RELATIME, "" /*=data*/); err != nil {
		return status.InternalErrorf("failed to mount %s (type %s) to %s: %s", d.DevicePath(), d.metadata.GetFilesystemType(), path, err)
	}
	log.Infof("Mounted %s (type %s) to %s", d.DevicePath(), d.metadata.GetLabel(), path)
	return nil
}

// Unmount unmounts the device from its current mount path.
func (d *ClientDevice) Unmount() error {
	if d.mountPath == "" {
		return status.FailedPreconditionError("not mounted")
	}
	_, err := os.Stat(d.mountPath)
	if os.IsNotExist(err) {
		log.Warningf("Unmount %s: %s", d.mountPath, err)
		d.mountPath = ""
		return nil // not mounted
	}
	if err := syscall.Unmount(d.mountPath, 0); err != nil {
		return status.InternalErrorf("unmount %s (%s): %s", d.mountPath, d.metadata.GetLabel(), err)
	}
	log.Infof("Unmounted %s (%s)", d.mountPath, d.DevicePath())
	d.mountPath = ""
	return nil
}

func (d *ClientDevice) ReadAt(p []byte, off uint) error {
	cancel := canary.Start(1 * time.Second)
	defer cancel()
	var res *nbdpb.ReadResponse
	err := d.doWithRedial(func() error {
		var err error
		res, err = d.client.Read(d.ctx, &nbdpb.ReadRequest{
			DeviceId: d.metadata.GetDeviceId(),
			Offset:   uint64(off),
			Length:   uint64(len(p)),
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
	copy(p, res.Data)
	return nil
}

func (d *ClientDevice) WriteAt(p []byte, off uint) error {
	err := d.doWithRedial(func() error {
		_, err := d.client.Write(d.ctx, &nbdpb.WriteRequest{
			DeviceId: d.metadata.GetDeviceId(),
			Offset:   uint64(off),
			Data:     p,
		})
		return err
	})
	if err != nil {
		fatalf("%s: write failed: %s", d.DevicePath(), err)
	}
	return nil
}

func (d *ClientDevice) Disconnect() {
	log.Errorf("%s unexpected disconnect", d.DevicePath())
}

func (d *ClientDevice) Flush() error {
	err := d.doWithRedial(func() error {
		_, err := d.client.Sync(d.ctx, &nbdpb.SyncRequest{
			DeviceId: d.metadata.GetDeviceId(),
		})
		return err
	})
	if err != nil {
		fatalf("%s: flush failed: %s", d.DevicePath(), err)
	}
	return nil
}

// doWithRedial invokes a function that performs an RPC to the host server,
// and re-dials the host server if the function fails on the first attempt.
func (d *ClientDevice) doWithRedial(f func() error) error {
	var lastErr error
	for i := 1; i <= 2; i++ {
		err := f()
		if status.IsUnavailableError(err) {
			d.conn.Close()
			log.Infof("%s: host device server is unavailable; reconnecting.", d.DevicePath())
			d.conn, err = dialHost()
			if err != nil {
				return status.WrapErrorf(lastErr, "failed to re-dial host (got error %s) after UNAVAILABLE error", err)
			}
			d.client = nbdpb.NewBlockDeviceClient(d.conn)
			lastErr = err
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}
	return lastErr
}

func (d *ClientDevice) Trim(off, length uint) error {
	return status.UnimplementedError("device does not support TRIM operation")
}

func fatalf(format string, args ...any) {
	// NOTE: do not change this "die: " prefix. We rely on it to parse the fatal
	// error from the firecracker machine logs and return it back to the user.
	log.Fatalf("die: "+format, args...)
}
