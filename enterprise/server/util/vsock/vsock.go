package vsock

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"syscall"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"

	libVsock "github.com/mdlayher/vsock"
)

const (
	// Corresponds to VHOST_VSOCK_SET_GUEST_CID in vhost.h
	ioctlVsockSetGuestCID = uintptr(0x4008AF60)

	// 0, 1 and 2 are reserved CIDs, see http://man7.org/linux/man-pages/man7/vsock.7.html
	minCID = 3
	maxCID = math.MaxUint32

	DefaultPort = 25415
)

// GetContextID returns ths next available vsock context ID.
// Inspired by https://github.com/firecracker-microvm/firecracker-containerd/
func GetContextID(ctx context.Context) (uint32, error) {
	file, err := os.OpenFile("/dev/vhost-vsock", syscall.O_RDWR, 0600)
	if err != nil {
		return 0, status.FailedPreconditionErrorf("failed to open vsock device: %s", err)
	}
	defer file.Close()

	for contextID := minCID; contextID < maxCID; contextID++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			cid := contextID
			_, _, err = syscall.Syscall(unix.SYS_IOCTL, file.Fd(), ioctlVsockSetGuestCID, uintptr(unsafe.Pointer(&cid)))
			switch err {
			case unix.Errno(0):
				return uint32(contextID), nil
			case unix.EADDRINUSE:
				continue // ID in use
			default:
				return 0, status.InternalErrorf("unexpected error: %s", err)
			}
		}
	}
	return 0, status.UnavailableError("Unable to determine next vsock context ID")
}

type vListener struct {
	net.Listener
	ctx context.Context
}

// NewGuestListener returns a new net.Listener that listens on the guest VSock
// on the specified port.
func NewGuestListener(ctx context.Context, port uint32) (net.Listener, error) {
	l, err := libVsock.Listen(port)
	if err != nil {
		return nil, err
	}

	return &vListener{
		ctx:      ctx,
		Listener: l,
	}, nil
}

func (l *vListener) Accept() (net.Conn, error) {
	for {
		select {
		case <-l.ctx.Done():
			return nil, l.ctx.Err()
		default:
			conn, err := l.Listener.Accept()
			if err == nil {
				return conn, err
			} else if isTemporary(err) {
				continue
			} else {
				return nil, err
			}
		}
	}
}

// isTemporary returns true if the error is non-nil and implements the
// Temporary() bool interface and is Temporary().
func isTemporary(err error) bool {
	tempErr, ok := err.(interface {
		Temporary() bool
	})

	return err != nil && ok && tempErr.Temporary()
}

// DialHostToGuest connects to the specified VSock socketPath and port and returns a
// new net.Conn or error if unable to connect.
func DialHostToGuest(ctx context.Context, socketPath string, port uint32) (net.Conn, error) {
	var d net.Dialer
	raddr := net.UnixAddr{Name: socketPath, Net: "unix"}
	conn, err := d.DialContext(ctx, "unix", raddr.String())
	if err != nil {
		return nil, err
	}

	// https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md#host-initiated-connections
	fcConnectString := fmt.Sprintf("CONNECT %d\n", port)
	n, err := conn.Write([]byte(fcConnectString))
	if err != nil {
		return nil, err
	}
	if n != len(fcConnectString) {
		return nil, status.InternalErrorf("HostDial failed: wrote %d bytes, expected %d", n, len(fcConnectString))
	}
	rsp, err := bufio.NewReaderSize(conn, 32).ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(rsp, "OK ") {
		return nil, status.InternalErrorf("HostDial failed: didn't receive 'OK' after CONNECT, got %q", rsp)
	}
	return conn, nil
}
