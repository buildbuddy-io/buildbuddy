//go:build darwin && !ios

package tundev

import (
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"golang.org/x/sys/unix"
	"golang.zx2c4.com/wireguard/tun"
)

// Open attaches to the utun interface the privileged tun device helper holds.
// The returned channel closes if the helper goes away.
func Open(name string) (tun.Device, <-chan struct{}, error) {
	// Talk to the privileged tun helper to get an fd for the tun device.
	fd, ifname, conn, err := tunhelperutil.Receive()
	if err != nil {
		return nil, nil, fmt.Errorf("attaching to the tunnel device: %w (is the tunnel installed? run: sudo bbaccess tunnel install)", err)
	}
	// Non-blocking before os.NewFile, so that reads use the poller and Close
	// interrupts them.
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		conn.Close()
		return nil, nil, err
	}
	unix.CloseOnExec(fd)
	// MTU 0: the helper set it, and changing it needs root.
	dev, err := tun.CreateTUNFromFile(os.NewFile(uintptr(fd), ifname), 0)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("wrapping %s: %w", ifname, err)
	}
	lost := make(chan struct{})
	go func() {
		conn.Read(make([]byte, 1))
		close(lost)
	}()
	return dev, lost, nil
}

// Describe reports the interface the daemon would attach to, for status.
func Describe(configured string) string {
	fd, name, conn, err := tunhelperutil.Receive()
	if err != nil {
		return fmt.Sprintf("unavailable (%s)", err)
	}
	unix.Close(fd)
	conn.Close()
	return name + ", held by the device helper"
}
