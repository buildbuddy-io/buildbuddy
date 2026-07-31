//go:build linux

package networking

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

const ethernetProtocolAll = 0x0003

type linuxPacketCapture struct {
	fd        int
	closed    atomic.Bool
	closeOnce sync.Once
	done      chan struct{}
}

func startPacketCapture(interfaceName string, observe func([]byte)) (packetCapture, error) {
	iface, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return nil, fmt.Errorf("look up network interface %q: %w", interfaceName, err)
	}
	protocol := hostToNetworkShort(ethernetProtocolAll)
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, int(protocol))
	if err != nil {
		return nil, fmt.Errorf("open packet observer socket: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrLinklayer{Protocol: protocol, Ifindex: iface.Index}); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("bind packet observer to %q: %w", interfaceName, err)
	}
	c := &linuxPacketCapture{fd: fd, done: make(chan struct{})}
	go c.run(observe)
	return c, nil
}

func (c *linuxPacketCapture) run(observe func([]byte)) {
	defer close(c.done)
	buffer := make([]byte, 64<<10)
	pollFDs := []unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLIN}}
	for !c.closed.Load() {
		_, err := unix.Poll(pollFDs, 250)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return
		}
		for {
			n, _, err := unix.Recvfrom(c.fd, buffer, unix.MSG_DONTWAIT)
			if err != nil {
				if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
					break
				}
				return
			}
			if n > 0 {
				observe(buffer[:n])
			}
		}
	}
}

func (c *linuxPacketCapture) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		closeErr = unix.Close(c.fd)
		<-c.done
	})
	return closeErr
}

func hostToNetworkShort(value uint16) uint16 {
	return value<<8 | value>>8
}
