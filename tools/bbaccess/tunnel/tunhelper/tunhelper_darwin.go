//go:build darwin && !ios

package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"golang.org/x/sys/unix"
)

const (
	utunControl     = "com.apple.net.utun_control"
	sysprotoControl = 2 // SYSPROTO_CONTROL
	utunOptIfname   = 2 // UTUN_OPT_IFNAME
)

// up creates a utun interface, configures it for cidr and hands it to uid's
// daemon on tunhelperutil.SocketPath until signaled. macOS names the device.
func up(uid int, cidr, _ string) error {
	// Validate before creating a device.
	if _, _, _, err := pointToPoint(cidr); err != nil {
		return err
	}
	fd, name, err := createUTUN()
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	// If the tunhelper restarts, the tun interface will not go away until the
	// bbaccess tunnel daemon stops referencing the tun file descriptor. The
	// daemon should notice that the tunhelper exited and exit as well so we
	// wait here to allow time for the daemon to exit.
	for attempt := 0; ; attempt++ {
		err := configure(name, cidr)
		if err == nil {
			break
		}
		if attempt%30 == 0 {
			log.Printf("Configuring %s failed, retrying: %s", name, err)
		}
		time.Sleep(time.Second)
	}
	path := tunhelperutil.SocketPath
	os.Remove(path) // left behind by a previous run
	l, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", path, err)
	}
	// Leave the socket file to the next run, which may already be listening.
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	// Only the owner may connect; Serve checks the peer too.
	if err := os.Chown(path, uid, -1); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	log.Printf("Version %d holding %s (%s) for uid %d on %s", tunhelperutil.Version, name, cidr, uid, path)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() { <-stop; l.Close() }()
	tunhelperutil.Serve(l, fd, name, uid)
	log.Printf("Shutting down; %s goes away with it", name)
	return nil
}

// down is not needed: the device goes away when launchd stops the helper.
func down(string) error {
	return errors.New("--down is not used on macOS: stop the launchd job instead")
}

// createUTUN asks the kernel for the next free utun, which only root may do,
// and returns its descriptor and name.
func createUTUN() (int, string, error) {
	fd, err := unix.Socket(unix.AF_SYSTEM, unix.SOCK_DGRAM, sysprotoControl)
	if err != nil {
		return 0, "", fmt.Errorf("opening the utun control: %w", err)
	}
	info := &unix.CtlInfo{}
	copy(info.Name[:], utunControl)
	if err := unix.IoctlCtlInfo(fd, info); err != nil {
		unix.Close(fd)
		return 0, "", fmt.Errorf("looking up %s: %w", utunControl, err)
	}
	if err := unix.Connect(fd, &unix.SockaddrCtl{ID: info.Id, Unit: 0}); err != nil {
		unix.Close(fd)
		return 0, "", fmt.Errorf("creating a utun interface: %w", err)
	}
	name, err := unix.GetsockoptString(fd, sysprotoControl, utunOptIfname)
	if err != nil {
		unix.Close(fd)
		return 0, "", fmt.Errorf("reading the utun name: %w", err)
	}
	return fd, name, nil
}

// configure gives the interface the first two addresses of cidr and routes
// the range to it. On Mac the tun interface is point-to-point and requires
// two addresses to be specified, even though we never actually used the second
// one.
func configure(name, cidr string) error {
	local, peer, mask, err := pointToPoint(cidr)
	if err != nil {
		return err
	}
	for _, args := range [][]string{
		{name, "mtu", strconv.Itoa(mtu)},
		{name, local, peer, "netmask", mask, "up"},
	} {
		if out, err := exec.Command("ifconfig", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("ifconfig %v: %w: %s", args, err, out)
		}
	}
	return addRoute(name, cidr)
}

// addRoute routes cidr to the interface.
func addRoute(name, cidr string) error {
	if out, err := exec.Command("route", "-n", "add", "-net", cidr, "-interface", name).CombinedOutput(); err != nil {
		return fmt.Errorf("route add %s: %w: %s", cidr, err, out)
	}
	return nil
}

// pointToPoint derives the local address, peer address and netmask for the
// interface serving cidr. On mac, a tun interface is a point-to-point interface
// and requires two IPs to be assigned for the two "ends". The peer address
// is configured on the interface but is not used for anything.
func pointToPoint(cidr string) (local, peer, mask string, err error) {
	addr, err := tunhelperutil.LocalAddr(cidr)
	if err != nil {
		return "", "", "", err
	}
	m := net.CIDRMask(netip.MustParsePrefix(cidr).Bits(), 32)
	return addr.String(), addr.Next().String(), net.IP(m).String(), nil
}
