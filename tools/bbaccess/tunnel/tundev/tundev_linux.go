//go:build linux && !android

// Package tundev opens the TUN device the interceptor reads from.
package tundev

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
	"golang.zx2c4.com/wireguard/tun"
)

// Open attaches to the persistent TUN interface created by `bbaccess tunnel install`.
// The channel is never closed: the interface outlives the daemon.
//
// The interface is created once, as root, and owned by the developer's user, so
// the daemon itself runs unprivileged.
func Open(name string) (tun.Device, <-chan struct{}, error) {
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("opening /dev/net/tun: %w (is the tunnel installed? run: sudo bbaccess tunnel install)", err)
	}

	var ifr struct {
		name  [unix.IFNAMSIZ]byte
		flags uint16
		_     [22]byte
	}
	if len(name) >= unix.IFNAMSIZ {
		unix.Close(fd)
		return nil, nil, fmt.Errorf("interface name %q is too long", name)
	}
	copy(ifr.name[:], name)
	// No IFF_VNET_HDR: the persistent device was created without it, and
	// requesting it here fails with EINVAL.
	ifr.flags = unix.IFF_TUN | unix.IFF_NO_PI

	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), uintptr(unix.TUNSETIFF), uintptr(unsafe.Pointer(&ifr))); errno != 0 {
		unix.Close(fd)
		if errno == unix.EPERM {
			return nil, nil, fmt.Errorf("permission denied attaching to %s: the interface must exist and be owned by this user (run: sudo bbaccess tunnel install)", name)
		}
		return nil, nil, fmt.Errorf("attaching to %s: %w", name, errno)
	}

	// CreateUnmonitoredTUNFromFD, not CreateTUNFromFile: the latter sets the
	// MTU, which needs CAP_NET_ADMIN. The installer sets it instead.
	dev, _, err := tun.CreateUnmonitoredTUNFromFD(fd)
	if err != nil {
		unix.Close(fd)
		return nil, nil, fmt.Errorf("wrapping %s: %w", name, err)
	}
	return dev, nil, nil
}

// Describe reports the interface the daemon would attach to, for status.
func Describe(configured string) string { return configured }
