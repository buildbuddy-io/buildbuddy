//go:build linux && !android

package main

import (
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

// up creates the persistent TUN device dev, owned by uid, and routes cidr to
// it. The device outlives the helper, so the daemon attaches unprivileged.
func up(uid int, cidr, dev string) error {
	if dev == "" {
		return errors.New("--dev is required on Linux")
	}
	addr, err := tunhelperutil.LocalAddr(cidr)
	if err != nil {
		return err
	}
	// Start over, so that the owner and flags are the ones asked for.
	if err := down(dev); err != nil {
		return err
	}
	if err := createPersistent(dev, uid); err != nil {
		return err
	}
	link, err := netlink.LinkByName(dev)
	if err != nil {
		return err
	}
	if err := netlink.LinkSetMTU(link, mtu); err != nil {
		return fmt.Errorf("setting the MTU of %s: %w", dev, err)
	}
	if err := netlink.LinkSetUp(link); err != nil {
		return fmt.Errorf("bringing %s up: %w", dev, err)
	}
	// The kernel routes the range to the device along with the address.
	ipnet := &net.IPNet{IP: addr.AsSlice(), Mask: net.CIDRMask(netip.MustParsePrefix(cidr).Bits(), 32)}
	if err := netlink.AddrAdd(link, &netlink.Addr{IPNet: ipnet}); err != nil {
		return fmt.Errorf("adding %s to %s: %w", ipnet, dev, err)
	}
	return nil
}

// createPersistent creates a TUN device that stays after we close it, which
// uid may attach to without privileges.
func createPersistent(dev string, uid int) error {
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("opening /dev/net/tun: %w", err)
	}
	defer unix.Close(fd)
	ifr, err := unix.NewIfreq(dev)
	if err != nil {
		return err
	}
	// The daemon attaches with the same flags: no IFF_VNET_HDR.
	ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		return fmt.Errorf("creating %s: %w", dev, err)
	}
	if err := unix.IoctlSetInt(fd, unix.TUNSETOWNER, uid); err != nil {
		return fmt.Errorf("setting the owner of %s: %w", dev, err)
	}
	if err := unix.IoctlSetInt(fd, unix.TUNSETPERSIST, 1); err != nil {
		return fmt.Errorf("making %s persistent: %w", dev, err)
	}
	return nil
}

// down removes the device, if it exists.
func down(dev string) error {
	if dev == "" {
		return errors.New("--dev is required on Linux")
	}
	link, err := netlink.LinkByName(dev)
	if errors.As(err, &netlink.LinkNotFoundError{}) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := netlink.LinkDel(link); err != nil {
		return fmt.Errorf("removing %s: %w", dev, err)
	}
	return nil
}
